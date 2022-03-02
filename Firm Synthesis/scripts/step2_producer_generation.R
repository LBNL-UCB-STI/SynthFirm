################ part 2 ######################
# at the beginning of this part, the input io has 302 rows and 387 variables
#-----------------------------------------------------------------------------------
#Create producers/suppliers database
#-----------------------------------------------------------------------------------
rm(list = ls())
path2code <- '/Users/xiaodanxu/Documents/GitHub/SynthFirm/Firm Synthesis/scripts/'
source(paste0(path2code, 'step0_SynthFirm_starter.R')) # load packages
source(paste0(path2code, 'scenario/scenario_variables.R'))  # load environmental variable

path2file <-
  "/Users/xiaodanxu/Documents/SynthFirm"
setwd(path2file)

c_n6_n6io_sctg <-
  data.table::fread("./inputs/corresp_naics6_n6io_sctg.csv", h = T)
firms <- data.table::fread("./outputs/synthetic_firms.csv", h = T)
for_prod <-
  data.table::fread("./inputs/data_foreign_prod.csv", h = T)
io <- data.table::fread("./inputs/data_2017io.csv", h = T)
unitcost <- data.table::fread("./inputs/data_unitcost.csv", h = T)
# prefweights <-
#   data.table::fread("./inputs/data_firm_pref_weights.csv", h = T)


print("Creating Producers Database")

# Creation of Producers database
# All agents that produce some SCTG commodity become potential producers
# wholesales are dealt with separately below
producers <-
  firms[Commodity_SCTG > 0 &
        substr(Industry_NAICS6_Make, 1, 2) != "42",] # 860,528 producers, exclude wholesale

# Create a table of production values and employment by NAICS code to calculate production value

#TODO change to use melt.data.table here to simplify
setnames(io, names(io), sub("X", "", names(io))) # Strip out the leading 'X' in the column names
io <-
  data.table(melt(io, id.vars = "Industry_NAICS6_MakeUse")) # Melt into long format and turn back into a data.table 
# 116,572 rows
setnames(
  io,
  c("Industry_NAICS6_MakeUse", "variable", "value"),
  c("Industry_NAICS6_Make", "Industry_NAICS6_Use", "ProVal") #make = from, use = to, use similar definition below
)
io[, Industry_NAICS6_Use := as.character(Industry_NAICS6_Use)]
write.csv(io, './outputs/io_summary.csv')
# io_sum <- copy(io) #keep for summaries

#Wholesalers: grouped into 42000 in IO tables, but 6 digit NAICS codes in employment data
#Distribute the IO table production and consumption
# to_wholesale -- amount of commodity GOES TO wholesale

to_wholesale <- # commodity flow goes TO wholesale
  io[substr(Industry_NAICS6_Use, 1, 2) == "42" &
       ProVal > 0 &
       Industry_NAICS6_Make %in% producers$Industry_NAICS6_Make] # 136 rows, 3 variables

wholesale_by_sctg <- data.table(
  SCTG = 1:40,
  NAICS_whl = c(
    rep("424500", 4),
    rep("424400", 3),
    "424800",
    "424400",
    rep("423300", 3),
    rep("423500", 2),
    "423700",
    rep("424700", 4),
    "424600",
    "424200",
    rep("424600", 2),
    "423400",
    rep("423300", 2),
    rep("424100", 3),
    "424300",
    rep("423500", 2),
    "423700",
    "423800",
    "425100",
    rep("423100", 2),
    "425100",
    "423200",
    "423900"
  )
)  # define wholesale commodity lookup table
#TODO note that "424900" also SCTG=40

# based on CFS 2017, essentially need to update this table
wholesale_by_sctg <- rbind(wholesale_by_sctg, list(35, "423600"))

wholesale_by_sctg <- rbind(wholesale_by_sctg, list(3, "424900"), list(4, "424900"), list(9, "424900"), list(22, "424900"), 
                           list(23, "424900"), list(29, "424900"), list(40, "424900"))

naics_by_sctg_fration <- unique(c_n6_n6io_sctg[, list(SCTG = Commodity_SCTG, Industry_NAICS6_Make, Proportion)])
#proportion of commodity by maker's industry
wholesale_by_sctg <-
  merge(wholesale_by_sctg, naics_by_sctg_fration, by =
          "SCTG") # size = 395 * 4

wholesale_by_sctg[, Proportion := Proportion / sum(Proportion), by = Industry_NAICS6_Make] 
# Re-distribution total production by wholesale segment and commodity

to_wholsale_by_sctg <-
  merge(wholesale_by_sctg, to_wholesale[, list(Industry_NAICS6_Make, ProVal)], by = "Industry_NAICS6_Make") # 227 * 5
to_wholsale_by_sctg[, ProVal := ProVal * Proportion] 

# at this point, 'to_wholsale_by_sctg' refers to the amount of commodity from various industry TO wholesale, size = 178 * 5

from_wholesale <- io[substr(Industry_NAICS6_Make, 1, 2) == "42" &
                       ProVal > 0] # commodity from wholesale, size = 385 * 3
setnames(from_wholesale, "ProVal", "ProValFromWhl")
from_wholesale_to_user <-
  merge(from_wholesale[, list(Industry_NAICS6_Use, ProValFromWhl)],
        io[ProVal > 0], by = "Industry_NAICS6_Use", all.x = TRUE) #29282 * 4
setnames(from_wholesale_to_user, "ProVal", "ProValUse")

# at this point, 'from_wholesale_to_user' refers to the amount of commodity from wholesale to each end users (ProVal), 
# and the amount of commodity that end user needs (ProValUse)

#which of those commodities can actually be sourced from wholesalers?
#TODO improve the naming here to be more explicit (e.g., fromwhl not a good name)
wholesale_flow <-
  merge(from_wholesale_to_user, to_wholsale_by_sctg, by = "Industry_NAICS6_Make", allow.cartesian = TRUE) #size = 30843 * 8

# at this point, fromwhl defines amount of commodity from wholesale and to wholesale
wholesale_flow[, ProValUse := ProValUse * Proportion]
wholesale_flow[, ProValPctUse := ProValUse / sum(ProValUse), by = Industry_NAICS6_Use]

#allocate out the ProValFromWhl by weighting both input to wholesalers
#first need to scale production amount to consumption
whlcons <-
  sum(unique(wholesale_flow[, list(Industry_NAICS6_Use, ProValFromWhl)])$ProValFromWhl)
whlprod <-
  sum(unique(wholesale_flow[, list(Industry_NAICS6_Make, ProVal)])$ProVal)
wholesale_flow[, ProValFact := ProVal * whlcons / whlprod]
wholesale_flow[, CellValue := ProValFact * ProValPctUse]

#matrix of producers to consumers
#row totals are production amounts
#column totals are consumption amounts
#initial cell values are the proportion of consumption that comes from each producers by industry
#Then need to IPF to adjust cell values such that row and column totals are conserved


for (i in 1:10) {
  wholesale_flow[, ColTotal := sum(CellValue), by = Industry_NAICS6_Use]
  wholesale_flow[, ColWeight := ProValFromWhl / ColTotal]
  wholesale_flow[, CellValue := CellValue * ColWeight]
  wholesale_flow[, RowTotal := sum(CellValue), by = Industry_NAICS6_Make]
  wholesale_flow[, RowWeight := ProValFact / RowTotal]
  wholesale_flow[, CellValue := CellValue * RowWeight]
}

wholesale_flow[, CellValue := round(CellValue)]
wholesale_flow <-
  wholesale_flow[CellValue > 0, list(Industry_NAICS6_Make,
                                     Industry_NAICS6_Use,
                                     SCTG,
                                     NAICS_whl,
                                     ProValWhl = CellValue)]
iowhl <-
  wholesale_flow[, list(ProValWhl = sum(ProValWhl)), by = list(Industry_NAICS6_Make, Industry_NAICS6_Use)]

#remove wholesale records from io table
io_no_wholesale <-
  io[substr(Industry_NAICS6_Use, 1, 2) != "42" &
       substr(Industry_NAICS6_Make, 1, 2) != "42"]

#replace wholesale records by adding the make-use value back to the io table
#as if it was direct from producer to consumer and not via whl
#so that the production amounts and consumption amounts will be correct
io_no_wholesale <-
  merge(
    io_no_wholesale,
    iowhl,
    by = c("Industry_NAICS6_Make", "Industry_NAICS6_Use"),
    all.x = TRUE
  )
io_no_wholesale[is.na(ProValWhl), ProValWhl := 0]
io_no_wholesale[, ProVal := ProVal + ProValWhl]
io_no_wholesale[, ProValWhl := NULL]

#add the wholesales with the correct capacities in value and tons
#to both producer and consumer tables
wholesalers <- firms[substr(Industry_NAICS6_Make, 1, 2) == "42",] # 407,124 wholesaler simulated
whlval <- wholesale_flow[, list(ProVal = sum(ProValWhl)), by = NAICS_whl]
setnames(whlval, "NAICS_whl", "Industry_NAICS6_Make")
whlval <-
  merge(whlval, wholesalers[, list(Emp = sum(Emp)), by = Industry_NAICS6_Make], "Industry_NAICS6_Make")
whlval[, ValEmp := ProVal / Emp] #production value per employee

wholesalers_with_value <-
  merge(whlval[, list(Industry_NAICS6_Make, ValEmp)], wholesalers, "Industry_NAICS6_Make") #merge the value per employee back on to businesses


wholesalers_with_value[, ProdVal := Emp * ValEmp] #calculate production value for each establishment



#======= this markes as the end of wholesaler generation ============#

#issues - need to be marked as a wholesales using the NAICS code
#but need to be tagged with the correct make/use commodity seperate from their NAICS code
#this should be easy for the consumer side, check possible for the producers side

prodval <-
  merge(io_no_wholesale[, list(ProVal = sum(ProVal)), by = Industry_NAICS6_Make], 
        producers[, list(Emp = sum(Emp)), by = Industry_NAICS6_Make], "Industry_NAICS6_Make")
prodval[, ValEmp := ProVal / Emp] #production value per employee (in Million of Dollars)
# write.csv(prodval, 'production_value_per_emp.csv')
producers <-
  merge(prodval[, list(Industry_NAICS6_Make, ValEmp)], producers, "Industry_NAICS6_Make") #merge the value per employee back on to producers

# NAICS 331314 (Secondary Smelting and Alloying of Aluminum) is excluded in this process as it is not in I/O table
producers[, ProdVal := Emp * ValEmp] #calculate production value for each establishment (in Million of Dollars)


# Add foreign producers - one agent per country per commodity
# add in the NAICS_Make code, group, and calculate employment requirements to support that production
################ part 2 END######################


################ part 3 ######################


setnames(for_prod, "Commodity_NAICS6", "Industry_NAICS6_CBP") # 28470 * 6 
for_prod <-
  merge(for_prod, c_n6_n6io_sctg[, list(Industry_NAICS6_CBP, Industry_NAICS6_Make, Commodity_SCTG)], by =
          "Industry_NAICS6_CBP") #Merge in the I/O NAICS codes and SCTG codes, size = 27786 * 8
for_prod <-
  for_prod[Commodity_SCTG > 0, list(ProdVal = sum(USImpVal) / 1000000), by =
             list(Industry_NAICS6_Make, CBPZONE, FAFZONE, Commodity_SCTG)] # 20,416 foreign forms that provide commodities in scope
for_prod <-
  merge(for_prod, prodval[, list(Industry_NAICS6_Make, ValEmp)], by = "Industry_NAICS6_Make", all.x =
          TRUE) # no firm loss in this step, good
#TODO check on these missing commodities/commodites without value in the IO table -- should there be US production too? should it have value?
for_prod <- for_prod[!is.na(ValEmp) & ValEmp != 0] # 20,392 firms remaining
#estimate employment and size category
#update ValEmp using foreign producer adjustment

for_prod[, ValEmp := ValEmp * foreignprodcostfactor] #same adjustment applied to unitcost, so assumption is that quantity per employee is the same as domestic production
for_prod[, Emp := round(ProdVal / ValEmp)]
for_prod[, esizecat := findInterval(Emp, c(0, 20, 100, 250, 500, 1000, 2500, 5000))]
#producers' output in POUNDS (units costs converted to pounds)
unitcost[, UnitCost := UnitCost / 2000]
for_prod <- merge(for_prod, unitcost, "Commodity_SCTG")
#update unit cost using foreign producer adjustment
for_prod[, UnitCost := UnitCost * foreignprodcostfactor]
for_prod[, ProdCap := ProdVal * 1000000 / UnitCost] # ProdVal was in $M, ProdCap in pound
### -------------------------------------------------------------------------------------
## -- Heither, 03-11-2016: enumerate large foreign consumers into multiple firms (threshold reduced)
for_prod[ProdCap <= 500000000, est := 1] # max production capability as 500 million pound
for_prod[ProdCap > 500000000, est := ceiling(ProdCap / 500000000)]		## number of fims to create
for_prod[est > 1, ProdVal := ProdVal / est]								## update ProdVal for multiple firms
for_prod[est > 1, ProdCap := ProdCap / est]								## update ProdCap for multiple firms
for_prod <-
  for_prod[rep(seq_len(for_prod[, .N]), est),]				## Enumerates the foreign producers using the est variable.
for_prod[, est := NULL]

for_prod[, MESOZONE := CBPZONE + 30000L]
for_prod[, BusID := max(firms$BusID) + .I] # size = 25186 * 11
### -------------------------------------------------------------------------------------
# calculate other fields required in producers tables

# Calculate producers' output in POUNDS (units costs converted to pounds)
producers <- merge(producers, unitcost, "Commodity_SCTG")
producers[, ProdCap := ProdVal * 1000000 / UnitCost] # ProdVal was in $M
# Calculate wholesalers output in POUNDS
wholesalers_with_value <- merge(wholesalers_with_value, unitcost, "Commodity_SCTG")
# factor up unitcost to reflect wholesalers margin
wholesalers_with_value[, UnitCost := UnitCost * wholesalecostfactor]
wholesalers_with_value[, ProdCap := ProdVal * 1000000 / UnitCost] # ProdVal was in $M

# combine domestic and foreign producers
producers[, c("V1") := NULL] 
producers <- rbind(producers, for_prod, use.names = TRUE) # 885,604 PRODUCERS
rm(for_prod)

# Prepare for Writing out a producers file for each NAICS, with each firm represented by:
# SellerID (BusID)  Zone (MESOZONE)	NAICS (NAICS6_Make)	Size (Emp)	OutputCommodity (SCTG_Make)	OutputCapacityTons (ProdCap)	NonTransportUnitCost (UnitCost)
producers[, c("FAFZONE", "CBPZONE", "esizecat", "ProdVal", "ValEmp") := NULL]
setnames(
  producers,
  c(
    "BusID",
    "MESOZONE",
    "Industry_NAICS6_Make",
    "Emp",
    "ProdCap",
    "UnitCost"
  ),
  c(
    "SellerID",
    "Zone",
    "NAICS",
    "Size",
    "OutputCapacityTons",
    "NonTransportUnitCost"
  )
)
producers[, OutputCommodity := NAICS] # size = 316275 * 12

#Add in wholesalers to producers
wholesalers_with_value[, c("FAFZONE", "CBPZONE", "esizecat", "ProdVal", "ValEmp") :=
              NULL]
setnames(
  wholesalers_with_value,
  c(
    "BusID",
    "MESOZONE",
    "Industry_NAICS6_Make",
    "Emp",
    "ProdCap",
    "UnitCost"
  ),
  c(
    "SellerID",
    "Zone",
    "NAICS",
    "Size",
    "OutputCapacityTons",
    "NonTransportUnitCost"
  )
)
#simulate the single specific NAICS commodity that the wholesaler deals in to simplify
#(wholesale NAICS are one to many NAICS commodities)
#each wholesale firm is identified with a specific NAICS and SCTG
#need probabilities for the match with NAICS commodity
whlnaics <-
  wholesale_flow[, list(ProValWhl = sum(ProValWhl)), by = list(Industry_NAICS6_Make, SCTG, NAICS =
                                                                 NAICS_whl)]
whlnaics[, ProbProValWhl := ProValWhl / sum(ProValWhl), by = list(SCTG, NAICS)]
setkey(whlnaics, NAICS, SCTG)
whlnaics[, CumProValWhl := cumsum(ProbProValWhl), by = list(SCTG, NAICS)]
whlnaicscombs <- unique(whlnaics[, list(NAICS, SCTG)])
wholesalers_with_value[, temprand := runif(.N)]
for (i in 1:nrow(whlnaicscombs)) {
  whlnaicsi <-
    whlnaics[NAICS == whlnaicscombs$NAICS[i] &
               SCTG == whlnaicscombs$SCTG[i]]
  wholesalers_with_value[NAICS == whlnaicscombs$NAICS[i] &
                Commodity_SCTG == whlnaicscombs$SCTG[i],
              OutputCommodity := whlnaicsi$Industry_NAICS6_Make[1 + findInterval(temprand, whlnaicsi$CumProValWhl)]]
}
#TODO - clean up correspondences to avoid no matches here
wholesalers_with_value[, temprand := NULL]
wholesalers_with_value <- wholesalers_with_value[!is.na(OutputCommodity)]
wholesalers_with_value[, c("V1") := NULL] 
producers <- rbind(producers, wholesalers_with_value) # size = 622764 * 8
setkey(producers, OutputCommodity)

write.csv(producers, './outputs/synthetic_producers.csv')
#writing out done below once sampling identified
################ part 3 END######################