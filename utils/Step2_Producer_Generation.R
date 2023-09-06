################ part 2 ######################
# at the beginning of this part, the input io has 302 rows and 387 variables
#-----------------------------------------------------------------------------------
#Create producers/suppliers database
#-----------------------------------------------------------------------------------
# rm(list = ls())
# path2code <- '/Users/xiaodanxu/Documents/GitHub/SynthFirm/utils/'
# source(paste0(path2code, 'step0_SynthFirm_starter.R')) # load packages
#source(paste0(path2code, 'scenario/scenario_variables.R'))  # load environmental variable
#source('config.R')  # load input settings 

# path2file <-
#   "/Users/xiaodanxu/Documents/SynthFirm.nosync"
# setwd(path2file)
# try update file name
print("Generating synthetic producers...")

output_dir <- paste0(path2file, "/outputs_", out_scenario)

firms <- data.table::fread(paste0(output_dir, '/', synthetic_firms_no_location_file), h = T) # 8,396, 679 FIRMS
mesozone_faf_lookup <- data.table::fread(paste0(path2file, "/inputs_", scenario, '/', zonal_id_file), h = T)

c_n6_n6io_sctg <-
  data.table::fread(paste0(path2file, "/SynthFirm_parameters/", c_n6_n6io_sctg_file), h = T)
for_prod <-
  data.table::fread(paste0(path2file, "/SynthFirm_parameters/", foreign_prod_file), h = T)

io <- data.table::fread(paste0(path2file, "/SynthFirm_parameters/", BEA_io_2017_file), h = T)
unitcost <- data.table::fread(paste0(path2file, "/SynthFirm_parameters/", agg_unit_cost_file), h = T)

producer_value_fraction_by_location <- 
  data.table::fread(paste0(path2file, "/SynthFirm_parameters/", prod_by_zone_file), h = T)

sctg_lookup <- data.table::fread(paste0(path2file, "/SynthFirm_parameters/", SCTG_group_file), h = T)

# Creation of Producers database
# All agents that produce some SCTG commodity become potential producers
# wholesales are dealt with separately below
producers <-
  firms[Commodity_SCTG > 0 &
        substr(Industry_NAICS6_Make, 1, 2) != "42",] # 423,944 producers, exclude wholesale

# Create a table of production values and employment by NAICS code to calculate production value

#TODO change to use melt.data.table here to simplify
setnames(io, names(io), sub("X", "", names(io))) # Strip out the leading 'X' in the column names
io <-
  data.table(melt(io, id.vars = "make")) # Melt into long format and turn back into a data.table 
# 140,998 rows
setnames(
  io,
  c("make", "variable", "value"),
  c("Industry_NAICS6_Make", "Industry_NAICS6_Use", "ProVal") #make = from, use = to, use similar definition below
)
io[, Industry_NAICS6_Use := as.character(Industry_NAICS6_Use)]
io <- io[io$ProVal>0,]
write.csv(io, paste0(path2file, "/SynthFirm_parameters/", io_summary_file), row.names=FALSE) 
# write cleaned I-O data for future use


to_wholesale <- # commodity flow goes TO wholesale
  io[substr(Industry_NAICS6_Use, 1, 2) == "42" &
       ProVal > 0 ] # 1383 rows, 3 variables

naics_by_sctg_fration <- unique(c_n6_n6io_sctg[, list(SCTG = Commodity_SCTG, Industry_NAICS6_Make, Proportion)])
naics_by_sctg_fration <-
  naics_by_sctg_fration[SCTG > 0,]
#proportion of commodity by maker's industry
# wholesale_by_sctg <-
#   naics_by_sctg_fration[substr(Industry_NAICS6_Make, 1, 2) == "42" & SCTG > 0] # size = 395 * 4

naics_by_sctg_fration[, Proportion := Proportion / sum(Proportion), by = Industry_NAICS6_Make] 
# Re-distribution total production by wholesale segment and commodity

to_wholsale_by_sctg <-
  merge(naics_by_sctg_fration, to_wholesale, by = "Industry_NAICS6_Make") # 954 * 5
to_wholsale_by_sctg[, ProVal := ProVal * Proportion] 


# at this point, 'to_wholsale_by_sctg' refers to the amount of commodity from various industry TO wholesale, size = 954 * 5

from_wholesale <- io[substr(Industry_NAICS6_Make, 1, 2) == "42" &
                       ProVal > 0] # 859 rows
setnames(from_wholesale, "ProVal", "ProValFromWhl")


# at this point, 'from_wholesale_to_user' refers to the amount of commodity from wholesale to each end users (ProValFromWhl)

#which of those commodities can actually be sourced from wholesalers?
#TODO improve the naming here to be more explicit (e.g., fromwhl not a good name)
wholesale_flow <-
  merge(from_wholesale, to_wholsale_by_sctg, 
        by.x = 'Industry_NAICS6_Make', 
        by.y = 'Industry_NAICS6_Use', allow.cartesian = TRUE) #size = 81048 * 8

# at this point, fromwhl defines amount of commodity from wholesale and to wholesale

wholesale_flow[, ProValPctUse := ProVal / sum(ProVal), by = c('Industry_NAICS6_Make', 'Industry_NAICS6_Use')] 
# split among source production industry (to wholesale)

#allocate out the ProValFromWhl by weighting both input to wholesalers
#first need to scale production amount to consumption

wholesale_flow[, CellValue := ProValFromWhl * ProValPctUse]
whlprod <-
  sum(unique(wholesale_flow[, list(Industry_NAICS6_Make, ProVal)])$ProVal)
whlcons <-
  sum(wholesale_flow$CellValue)
wholesale_flow[, CellValue := CellValue * whlprod / whlcons]

#matrix of producers to consumers
#row totals are production amounts
#column totals are consumption amounts
#initial cell values are the proportion of consumption that comes from each producers by industry
#Then need to IPF to adjust cell values such that row and column totals are conserved


# for (i in 1:10) { # IPF method, col sum matches total wholesale production from manufacturers, col sum matches total consumption to end users
# 
#   wholesale_flow[, ColTotal := sum(CellValue), by = c('Industry_NAICS6_Make', 'Industry_NAICS6_Make.y')]
#   wholesale_flow[, ColWeight := ProVal / ColTotal]
#   wholesale_flow[, CellValue := CellValue * ColWeight]
#   wholesale_flow[, RowTotal := sum(CellValue), by = c('Industry_NAICS6_Make', 'Industry_NAICS6_Use')]
#   wholesale_flow[, RowWeight :=  ProValFromWhl/ RowTotal]
#   wholesale_flow[, CellValue := CellValue * RowWeight]
# }

wholesale_flow[, CellValue := round(CellValue)]
wholesale_flow <-
  wholesale_flow[CellValue > 0, list(Industry_NAICS6_Make = Industry_NAICS6_Make.y,
                                     Industry_NAICS6_Use,
                                     SCTG,
                                     NAICS_whl = Industry_NAICS6_Make,
                                     ProValWhl = CellValue)] #28747 * 5 after removing zero transactions
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
whlval <- wholesale_flow[, list(ProVal = sum(ProValWhl)), by = c('NAICS_whl', 'SCTG')]
setnames(whlval, "NAICS_whl", "Industry_NAICS6_Make")
setnames(whlval, "SCTG", "Commodity_SCTG")

list_of_wholesaler_naics <- unique(whlval$Industry_NAICS6_Make)
wholesalers[, Commodity_SCTG := NULL]
wholesalers_with_sctg <- NULL
for (whl_naics in list_of_wholesaler_naics){
  # print(whl_naics)
  con_firms <- wholesalers[Industry_NAICS6_Make == whl_naics,]
  sample_size <- nrow(con_firms)
  con_io <- whlval[Industry_NAICS6_Make == whl_naics,]
  con_io[, probability := ProVal/sum(ProVal)]
  sample_naics_make = sample_n(con_io, size = sample_size, weight = probability, replace = TRUE) %>% as_tibble()
  sample_naics_make <- as.data.table(sample_naics_make)
  con_firms <- cbind(con_firms, sample_naics_make[, 'Commodity_SCTG'])
  wholesalers_with_sctg <- rbind(wholesalers_with_sctg, con_firms)
  # break
} #301,825 wholesalers with sctg

wholesale_emp <- wholesalers_with_sctg[, list(Emp = sum(Emp)), by = c('Industry_NAICS6_Make', 'Commodity_SCTG', 'FAFZONE')] #5135

whlval_with_loc <-
  merge(wholesale_emp, whlval, 
  by = c("Industry_NAICS6_Make", 'Commodity_SCTG'))
whlval_with_loc <- merge(whlval_with_loc, 
                         producer_value_fraction_by_location[, list(Commodity_SCTG, FAFZONE = FAF, value_fraction)],
                         by = c("Commodity_SCTG", "FAFZONE")) # 31,656
whlval_with_loc[, value_fraction := value_fraction / sum(value_fraction), by = c('Industry_NAICS6_Make', 'Commodity_SCTG')] #production value per employee
whlval_with_loc[, ProVal := ProVal * value_fraction]
whlval_with_loc[, ValEmp := ProVal / Emp] #production value per employee
#wholesalers[, Commodity_SCTG := NULL]
wholesalers_with_value <-
  merge(wholesalers_with_sctg, whlval_with_loc[, list(Industry_NAICS6_Make, ValEmp, Commodity_SCTG, FAFZONE)], 
        by = c('Industry_NAICS6_Make', "Commodity_SCTG", 'FAFZONE')) #merge the value per employee back on to businesses

wholesalers_with_value[, ProdVal := Emp * ValEmp] #calculate production value for each establishment, 
# some firms are removed as there is no I-O data + SCTG information for that NAICS code
# 299,526 wholesaler, some were dropped as no matching production value found


#======= this markes as the end of wholesaler generation ============#

#issues - need to be marked as a wholesales using the NAICS code
#but need to be tagged with the correct make/use commodity seperate from their NAICS code
#this should be easy for the consumer side, check possible for the producers side
producer_emp = producers[, list(Emp = sum(Emp)), by = c('Industry_NAICS6_Make', 'Commodity_SCTG', 'FAFZONE')]

prodval <-
  merge(producer_emp, io_no_wholesale[, list(ProVal = sum(ProVal)), by = Industry_NAICS6_Make], "Industry_NAICS6_Make")

prodval_with_loc <- merge(prodval, 
                          producer_value_fraction_by_location[, list(Commodity_SCTG, FAFZONE = FAF, value_fraction)],
                          by = c("Commodity_SCTG", "FAFZONE")) # 31,656
prodval_with_loc[, value_fraction := value_fraction / sum(value_fraction), by = c('Industry_NAICS6_Make', 'Commodity_SCTG')] #production value per employee
prodval_with_loc[, ProVal := ProVal * value_fraction]
prodval_with_loc[, ValEmp := ProVal / Emp] #production value per employee (in Million of Dollars)
# write.csv(prodval, 'production_value_per_emp.csv')
producers <-
  merge(producers, prodval_with_loc[, list(Industry_NAICS6_Make, ValEmp, Commodity_SCTG, FAFZONE)], 
        by = c('Industry_NAICS6_Make', 'Commodity_SCTG', 'FAFZONE')) #394,389 producers

# NAICS 331314 (Secondary Smelting and Alloying of Aluminum) is excluded in this process as it is not in I/O table
producers[, ProdVal := Emp * ValEmp] #calculate production value for each establishment (in Million of Dollars)



################ part 2 END######################


################ part 3 ######################
# Add foreign producers - one agent per country per commodity
# add in the NAICS_Make code, group, and calculate employment requirements to support that production
prodval <-
  merge(io[, list(ProVal = sum(ProVal)), by = Industry_NAICS6_Make], 
        producers[, list(Emp = sum(Emp)), by = Industry_NAICS6_Make], "Industry_NAICS6_Make")
prodval[, ValEmp := ProVal / Emp] #production value per employee (in Million of Dollars)

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
for_prod <- for_prod[!is.na(ValEmp) & ValEmp != 0] # 15312 firms remaining
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
for_prod[, BusID := max(firms$BusID) + .I] # size = 20446 * 11
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
#producers[, c("FAFID") := NULL] 
producers <- rbind(producers, for_prod, use.names = TRUE) # 441,376 PRODUCERS
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
    "OutputCapacitylb",
    "NonTransportUnitCost"
  )
)
producers[, OutputCommodity := NAICS] # size = 414835 * 12

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
    "OutputCapacitylb",
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
}  # for each wholesaler, find the only consumer industry within a list of potential consumers
#TODO - clean up correspondences to avoid no matches here
wholesalers_with_value[, temprand := NULL]
wholesalers_with_value <- wholesalers_with_value[!is.na(OutputCommodity)]
# wholesalers_with_value[, c("V1") := NULL] 
producers <- rbind(producers, wholesalers_with_value) # size = 690,436 * 8
setkey(producers, OutputCommodity)

print('Total number of producers:')
print(nrow(producers))

print('Total number of wholesalers (among producers):')
print(nrow(wholesalers_with_value))

write.csv(wholesalers_with_value, paste0(output_dir, '/', synthetic_wholesaler_file), row.names=FALSE)
write.csv(producers, paste0(output_dir, '/', synthetic_producer_file), row.names=FALSE)
write.csv(io_no_wholesale, paste0(output_dir, '/', io_filtered_file), row.names=FALSE)

sctg_lookup <- sctg_lookup %>% select(SCTG_Code, SCTG_Group, SCTG_Name) %>% as_tibble()
producers = producers %>% left_join(sctg_lookup, by = c("Commodity_SCTG" = "SCTG_Code")) %>% as_tibble()

for (i in 1:5) {
  print(paste0("Processing SCTG Group ", i))
  
  g1_prods = producers %>% filter(SCTG_Group == i) %>% select(
    SCTG_Group,
    Commodity_SCTG,
    SellerID,
    Zone,
    NAICS,
    OutputCommodity,
    OutputCapacitylb
  ) %>% as_tibble()
  
  
  fwrite(g1_prods, paste0(output_dir, '/', synthetic_producer_by_sctg_filehead, i, ".csv"))
}  

print('Producer generation is done!')
#writing out done below once sampling identified
################ part 3 END######################