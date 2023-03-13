################ part 4 ######################

#-----------------------------------------------------------------------------------
rm(list = ls())
path2code <- '/Users/xiaodanxu/Documents/GitHub/SynthFirm/Firm Synthesis/scripts/'
source(paste0(path2code, 'step0_SynthFirm_starter.R')) # load packages
source(paste0(path2code, 'scenario/scenario_variables.R'))  # load environmental variable

path2file <-
  "/Users/xiaodanxu/Documents/SynthFirm.nosync"
setwd(path2file)

c_n6_n6io_sctg <-
  data.table::fread("./inputs_national/corresp_naics6_n6io_sctg_revised.csv", h = T)
firms <- data.table::fread("./outputs_national/synthetic_firms.csv", h = T)
unitcost <- data.table::fread("./inputs_national/data_unitcost_cfs2017_calibrated.csv", h = T)
for_cons <-
  data.table::fread("./inputs_national/data_foreign_cons.csv", h = T)
prefweights <-
  data.table::fread("./inputs_national/data_firm_pref_weights.csv", h = T)

wholesalers <- data.table::fread('./outputs_national/synthetic_wholesaler.csv')
producers <- data.table::fread('./outputs_national/synthetic_producers.csv')
io <- data.table::fread('./outputs_national/data_2017io_filtered.csv')

consumer_value_fraction_by_location <- data.table::fread("./inputs/consumer_value_fraction_by_faf.csv", h = T)
mesozone_faf_lookup <- data.table::fread("./inputs/zonal_id_lookup_final.csv", h = T)
#-----------------------------------------------------------------------------------
#Create consumers database
#-----------------------------------------------------------------------------------
print("Creating Consumers Database")

#For each firm generate a list of input commodities that need to be purchased (commodity code, amount)
list_of_supply_industry <- unique(producers$NAICS)
io <-
  io[Industry_NAICS6_Make %in% list_of_supply_industry] #focus on just producers of transported commodities
setkey(io, Industry_NAICS6_Use, ProVal) #sort on NAICS_USe, ProVal
#io <- io[ProVal >0,]

# io[, CumPctProVal := cumsum(ProVal) / sum(ProVal), by = Industry_NAICS6_Use] #cumulative pct value of the consumption inputs
#io <- io[CumPctProVal > 1 - provalthreshold,] #select suppliers including the first above the threshold value


#Calcuate value per employee required
setnames(firms, "Industry_NAICS6_Make", "Industry_NAICS6_Use")  #In the consumers table Use code is that of the consuming firm
list_of_consumer_naics <- unique(io$Industry_NAICS6_Use)

consumers <- NULL
for (con_naics in list_of_consumer_naics){
  print(con_naics)
  con_firms <- firms[Industry_NAICS6_Use == con_naics,]
  sample_size <- nrow(con_firms)
  con_io <- io[Industry_NAICS6_Use == con_naics,]
  con_io[, probability := ProVal/sum(ProVal)]
  sample_naics_make = sample_n(con_io, size = sample_size, weight = probability, replace = TRUE) %>% as_tibble()
  sample_naics_make <- as.data.table(sample_naics_make)
  con_firms <- cbind(con_firms, sample_naics_make[, 'Industry_NAICS6_Make'])
  consumers <- rbind(consumers, con_firms)
  # break
}
setnames(consumers, "Commodity_SCTG", "Buyer.SCTG") #7,670,012 consumers

naics_by_sctg_fration <- unique(c_n6_n6io_sctg[, list(SCTG = Commodity_SCTG, Industry_NAICS6_Make, Proportion)])
naics_by_sctg_fration <-
  naics_by_sctg_fration[SCTG > 0,]

# consumers <-
#   merge(consumers, naics_by_sctg_fration, "Industry_NAICS6_Make", allow.cartesian = TRUE) #merge in the first matching SCTG code
# consumers[, Proportion := Proportion / sum(Proportion), by = BusID]

emp <- consumers[, list(Emp = sum(Emp)), by = c('Industry_NAICS6_Use', 'Industry_NAICS6_Make', 'FAFZONE')]
io <- merge(emp, io, by = c("Industry_NAICS6_Use", "Industry_NAICS6_Make"))
io <- merge(io, naics_by_sctg_fration, "Industry_NAICS6_Make", allow.cartesian = TRUE)
io[, ProVal := ProVal * Proportion]
setnames(io, 'SCTG', 'Commodity_SCTG')
io_with_loc <- merge(io, 
                     consumer_value_fraction_by_location[, list(FAFZONE = FAF, Commodity_SCTG, value_fraction)], 
                     by = c("Commodity_SCTG", 'FAFZONE'))
io_with_loc[, value_fraction := value_fraction / sum(value_fraction), by = c("Industry_NAICS6_Use", 'Industry_NAICS6_Make', 'Commodity_SCTG')] #production value per employee
io_with_loc[, ProVal := ProVal * value_fraction]
io_with_loc[, ValEmp := ProVal / Emp] #production value per employee (in Million of Dollars)


#Merge top k% suppliers with establishment list to create a consumers\buyers dataset
consumers <-
  merge(io_with_loc[, list(Industry_NAICS6_Use, Industry_NAICS6_Make, Commodity_SCTG, FAFZONE, ValEmp)], 
        consumers[, list(MESOZONE, Industry_NAICS6_Use, Industry_NAICS6_Make, Buyer.SCTG, FAFZONE, BusID, Emp)],
        by = c('Industry_NAICS6_Use', 'Industry_NAICS6_Make', 'FAFZONE'), allow.cartesian=TRUE) #8,626,516


consumers[, ConVal := ValEmp * Emp]
# print(consumers$ConVal)



# Calculate the purchase amount and convert to tons needed - this is production value
# consumers[, ConVal := ValEmp * Emp]
# Convert purchase value from $M to POUNDS
unitcost[, UnitCost := UnitCost / 2000]
consumers[, UnitCost := unitcost$UnitCost[match(Commodity_SCTG, unitcost$Commodity_SCTG)]]
consumers[, ConVal := ConVal * 1000000] # Value was in $M
consumers[, PurchaseAmountlb := ConVal / UnitCost]
consumers[, c("ValEmp", "UnitCost") := NULL] # Remove extra fields

#Add foreign consumers to the domestic consumption
#In this case we know US Export Value by commodity and country
#We need to synthesize the types of firms that are buying it (by industry)
#Use the IO data to indicate the industry types that consume the exported commodities
setnames(for_cons, "Commodity_NAICS6", "Industry_NAICS6_CBP")
for_cons <-
  merge(for_cons, unique(c_n6_n6io_sctg[, list(Industry_NAICS6_CBP, Industry_NAICS6_Make)]), by =
          "Industry_NAICS6_CBP") #Merge in the I/O NAICS codes and SCTG codes

setnames(for_cons, 'Industry_NAICS6_Make', 'Industry_NAICS6_Use')

list_of_for_consumer_naics <- unique(for_cons$Industry_NAICS6_Use)

for_consumers <- NULL
for (con_naics in list_of_for_consumer_naics){
  print(con_naics)
  con_firms <- for_cons[Industry_NAICS6_Use == con_naics,]
  sample_size <- nrow(con_firms)
  con_io <- io[Industry_NAICS6_Use == con_naics,]
  con_io[, probability := ProVal/sum(ProVal)]
  sample_naics_make = sample_n(con_io, size = sample_size, weight = probability, replace = TRUE) %>% as_tibble()
  sample_naics_make <- as.data.table(sample_naics_make)
  con_firms <- cbind(con_firms, sample_naics_make[, 'Industry_NAICS6_Make'])
  for_consumers <- rbind(for_consumers, con_firms)
  # break
}

for_consumers <-
  merge(for_consumers, unique(c_n6_n6io_sctg[, list(Industry_NAICS6_Make, Commodity_SCTG, Proportion)]), by =
          "Industry_NAICS6_Make", allow.cartesian = TRUE) #Merge in the I/O NAICS codes and SCTG codes

for_consumers[, USExpVal := USExpVal * Proportion]

for_consumers <-
  for_consumers[Commodity_SCTG > 0, list(ProdVal = sum(USExpVal) / 1000000), by =
             list(Industry_NAICS6_Make, Industry_NAICS6_Use, CBPZONE, FAFZONE, Commodity_SCTG)]


# io[, PctProVal := ProVal / sum(ProVal), by = Industry_NAICS6_Make]
# setkey(io, Industry_NAICS6_Make)
# for_cons <-
#   merge(for_cons, io[, list(Industry_NAICS6_Make, Industry_NAICS6_Use, ProVal, PctProVal)], by =
#           "Industry_NAICS6_Make", allow.cartesian = TRUE)
for_consumers[, ConVal := ProdVal]

# Convert purchase value from $M to POUNDS

for_consumers[, UnitCost := unitcost$UnitCost[match(Commodity_SCTG, unitcost$Commodity_SCTG)]]
for_consumers[, ConVal := ConVal * 1000000] # Value was in $M
for_consumers[, PurchaseAmountlb := ConVal / UnitCost]
### -------------------------------------------------------------------------------------
## -- Heither, 03-11-2016: enumerate large foreign consumers into multiple firms (threshold reduced)
for_consumers[PurchaseAmountlb <= 500000000, est := 1]
for_consumers[PurchaseAmountlb > 500000000, est := ceiling(PurchaseAmountlb /
                                                          500000000)]		## number of fims to create
for_consumers[est > 1, ProdVal := ProdVal / est]														## update ProdVal for multiple firms
for_consumers[est > 1, ConVal := ConVal / est]															## update ConVal for multiple firms
for_consumers[est > 1, PurchaseAmountlb := PurchaseAmountlb / est]									## update PurchaseAmountTons for multiple firms
for_consumers <-
  for_consumers[rep(seq_len(for_consumers[, .N]), est),]										## Enumerates the foreign producers using the est variable.
for_consumers[, est := NULL] #40,843 foreign consumers
### -------------------------------------------------------------------------------------

# calculate other fields required in producers tables, clean table, and rbind with consumers
for_consumers[, MESOZONE := CBPZONE + 100000L] # no duplicate with county fips code
for_consumers[, BusID := max(producers$SellerID) + .I] #add foreign consumers on after the foreign producers
for_consumers[, Buyer.SCTG := 0L] #don't know buyer industry
for_consumers[, Emp := 0L] #don't know employment of buying firm
for_consumers[, c("CBPZONE",
             "FAFZONE",
             "ProdVal",
             "UnitCost") := NULL] # Remove extra fields
consumers[, FAFZONE:= NULL]
consumers <- rbind(consumers, for_consumers) #8,171,491

# Add preference weights
setkey(prefweights, Commodity_SCTG)
consumers <-
  merge(consumers, prefweights[, list(Commodity_SCTG,
                                      CostWeight,
                                      TimeWeight,
                                      SingleSourceMaxFraction)], "Commodity_SCTG")
#consumers[,c("PrefWeight3_AttributeK","PrefWeight4_AttributeL","PrefWeight5_AttributeM","MaxFrac1_SingleSource","MaxFrac2_PortOfOrigin","MaxFrac3_PortOfEntry"):=list(0,0,0,0.8,1.0,1.0)]

# Prepare for writing a file of each NAICS containing the firms that use those goods as inputs (done once sampling figured out below)
#BuyerID (BusID)  Zone (MESOZONE)	NAICS (NAICS6_Use)	Size (Emp)	OutputCommodity (SCTG_Make)	InputCommodity (SCTG_Use)	PurchaseAmountTons	PrefWeight1_UnitCost (CostWeight)	PrefWeight2_ShipTime (TimeWeight)	PrefWeight3_AttributeK	PrefWeight4_AttributeL	PrefWeight5_AttributeM	MaxFrac1_SingleSource	MaxFrac2_PortOfOrigin	MaxFrac3_PortOfEntry
setnames(
  consumers,
  c(
    "BusID",
    "MESOZONE",
    "Industry_NAICS6_Make",
    "Industry_NAICS6_Use",
    "Emp",
    "CostWeight",
    "TimeWeight"
  ),
  c(
    "BuyerID",
    "Zone",
    "InputCommodity",
    "NAICS",
    "Size",
    "PrefWeight1_UnitCost",
    "PrefWeight2_ShipTime"
  )
)
consumers[, OutputCommodity := NAICS]

#Add wholesalers to the consumers
wholesalers[, ConVal := OutputCapacitylb * NonTransportUnitCost / wholesalecostfactor]
wholesalers[, NonTransportUnitCost := NULL]
wholesalers <-
  merge(wholesalers, prefweights[, list(Commodity_SCTG,
                                        CostWeight,
                                        TimeWeight,
                                        SingleSourceMaxFraction)], "Commodity_SCTG")
setnames(
  wholesalers,
  c(
    "SellerID",
    "OutputCommodity",
    "OutputCapacitylb",
    "CostWeight",
    "TimeWeight"
  ),
  c(
    "BuyerID",
    "InputCommodity",
    "PurchaseAmountlb",
    "PrefWeight1_UnitCost",
    "PrefWeight2_ShipTime"
  )
)
wholesalers[, Buyer.SCTG := Commodity_SCTG]
wholesalers[, OutputCommodity := InputCommodity] #size = 249,060 * 11
# wholesalers[, c("V1") := NULL] 
consumers <- rbind(consumers, wholesalers, use.names = TRUE)

sample_consumers <- consumers %>% filter(BuyerID <= 100) %>% as_tibble()
setkey(consumers, InputCommodity)

#data.table::fwrite(producers, "./outputs/producers_all.csv")
data.table::fwrite(consumers, "./outputs_national/synthetic_consumers.csv", row.names=FALSE)
data.table::fwrite(sample_consumers, "./outputs_national/sample_synthetic_consumers.csv", row.names=FALSE)
