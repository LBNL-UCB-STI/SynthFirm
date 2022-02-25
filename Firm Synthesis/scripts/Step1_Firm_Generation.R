#-----------------------------------------------------------------------------------
#Step 1 Firm Synthesis
#-----------------------------------------------------------------------------------


################ part 0 ######################
rm(list = ls())
path2code <- '/Users/xiaodanxu/Documents/GitHub/SynthFirm/Firm Synthesis/scripts/'
source(paste0(path2code, 'step0_SynthFirm_starter.R')) # load packages
source(paste0(path2code, 'scenario/scenario_variables.R'))  # load environmental variable

path2file <-
  "/Users/xiaodanxu/Documents/SynthFirm"
setwd(path2file)

c_n6_n6io_sctg <-
  data.table::fread("./inputs/corresp_naics6_n6io_sctg.csv", h = T)
cbp <- data.table::fread("./inputs/data_emp_cbp.csv", h = T)
mzemp <-
  data.table::fread("./inputs/data_mesozone_emprankings.csv", h = T)
employment_per_firm <- 
  data.table::fread("./inputs/employment_by_firm_size_naics.csv", h = T)
# for_prod <-
#   data.table::fread("./inputs/data_foreign_prod.csv", h = T)
# for_cons <-
#   data.table::fread("./inputs/data_foreign_cons.csv", h = T)
# io <- data.table::fread("./inputs/data_2017io.csv", h = T)
# unitcost <- data.table::fread("./inputs/data_unitcost.csv", h = T)
# prefweights <-
#   data.table::fread("./inputs/data_firm_pref_weights.csv", h = T)

# at the end of this step, the script consumes 2.55 gb of memory

################ part 1 ######################
#-----------------------------------------------------------------------------------
#Enumerating firms and merge with correspondenses
#-----------------------------------------------------------------------------------
print("Enumerating Firms")

#c_n6_n6io_sctg                  <-     as.data.table(corresp_naics6_n6io_sctg)              #Correspondence between NAICS 6-digit, I/O NAICS, and SCTG
#cbp                             <-     as.data.table(data_emp_cbp)              #CBP data file
#for_prod                        <-     as.data.table(data_foreign_prod)         #Foreign producers
#for_cons                        <-     as.data.table(data_foreign_cons)         #foreign consumers
#io                              <-     as.data.table(data_2010io)
#unitcost                        <-     as.data.table(data_unitcost)
#prefweights                     <-     as.data.table(data_firm_pref_weights)
#mzemp                           <-     as.data.table(data_mesozone_emprankings)      #Industry rankings data by mesozone based on employment


#Aggregate the employment data by zones, NAICS, and firm size category
#1='1-19',2='20-99',3='100-249',4='250-499',5='500-999',6='1,000-2,499',7='2,500-4,999',8='Over 5,000'
#cbp <- as.data.table(cbp)
# colnames(cbp) <- c('Industry_NAICS6_CBP', 'FAFZONE',	'CBPZONE',
#                   'employment',	'establishment',	'e1',	'e2',	'e3',	'e4',	'e5',	'e6',	'e7',	'e8')
total_employment = sum(cbp$employment)
print(total_employment)
cbp <-
  cbp[!is.na(CBPZONE) &
        !is.na(FAFZONE) & !is.na(Industry_NAICS6_CBP),
      list(
        e1 = sum(e1),
        e2 = sum(e2),
        e3 = sum(e3),
        e4 = sum(e4),
        e5 = sum(e5),
        e6 = sum(e6),
        e7 = sum(e7),
        e8 = sum(e8)
      ),
      by = list(Industry_NAICS6_CBP, CBPZONE, FAFZONE)] #Remove records with missing zones and NAICS codes
setkey(c_n6_n6io_sctg, Industry_NAICS6_CBP)
cbp <-
  merge(cbp, c_n6_n6io_sctg[, list(Industry_NAICS6_CBP, Industry_NAICS6_Make, Commodity_SCTG)], by =
          "Industry_NAICS6_CBP") #Merge in the I/O NAICS codes and SCTG codes (Remove a few businesses with unknown naics codes from InfoUSA data)
cbp[, c("n2", "n4") := list(substr(Industry_NAICS6_CBP, 1, 2),
                            substr(Industry_NAICS6_CBP, 1, 4))] #add 2 and 4 digit NAICS
cbp <-
  melt(
    cbp,
    measure.vars = paste0("e", 1:8),
    variable.name = "esizecat",
    value.name = "est"
  ) #Melt to create separate rows for each firm size category
cbp <- as.data.table(cbp)
cbp[, esizecat := as.integer(esizecat)] #convert esizecat to an integer (1:8)
# cbp[, Emp := c(10L, 60L, 175L, 375L, 750L, 1750L, 3750L, 7500L)[esizecat]] # Estimate the number of employees
cbp[, Emp := c(3L, 23L, 100L, 250L, 696L, 1750L, 3750L, 7500L)[esizecat]] # employment capped by census total

print('total number of firm is ')
print(sum(cbp$est))

cbp <-
  cbp[rep(seq_len(cbp[, .N]), est),] #Enumerates the agent businesses using the est variable.
cbp[, BusID := .I] #Add an ID

# cbp_emp <- cbp %>% 
#   group_by(Industry_NAICS6_CBP, CBPZONE, FAFZONE) %>% 
#   summarise(total_emp_sim = sum(Emp)) %>% as_tibble()
# 
# 
# cbp_emp <- cbp_emp %>% left_join(cbp, by = c('Industry_NAICS6_CBP', 'CBPZONE', 'FAFZONE'))
# cbp_emp <- cbp_emp %>% mutate(emp_adj = employment/total_emp_sim)
# cbp_emp
# summarise(cbp_emp)
total_employment_est = sum(cbp$Emp)
#-----------------------------------------------------------------------------------
# Allocating specific commodity and location for each establishment
#-----------------------------------------------------------------------------------
print("Allocating commodities and locations to establishments")

#TODO: are these still appropriate correspodences given the new I/O data?
#This section identifies producers who make 2+ commodities (especially wholesalers) and
#simulates a specific commodity for them based on probability thresholds for multiple commodities
set.seed(151)
cbp[, temprand := runif(.N)]
#For all the NAICS which may produce more than one SCTG commodity, simulate one SCTG commodity using set probability thresholds
setkey(cbp, Industry_NAICS6_CBP)
cbp[list(211111), Commodity_SCTG := c(16L, 19L)[1 + findInterval(temprand, c(0.45))]]
cbp[list(324110), Commodity_SCTG := c(17L, 18L, 19L)[1 + findInterval(temprand, c(0.25, 0.50))]]

setkey(cbp, n4)
cbp["4245", Commodity_SCTG := c(1L, 2L, 3L, 4L)[1 + findInterval(temprand, c(0.25, 0.50, 0.75))]] #Farm Product Raw Material Merchant Wholesalers
cbp["4244", Commodity_SCTG := c(5L, 6L, 7L, 9L)[1 + findInterval(temprand, c(0.25, 0.50, 0.75))]] #Grocery and Related Product Wholesalers
cbp["4248", Commodity_SCTG := 8L] #Beer, Wine, and Distilled Alcoholic Beverage Merchant Wholesalers
cbp["4233", Commodity_SCTG := c(10L, 11L, 12L, 25L, 26L)[1 + findInterval(temprand, c(0.10, 0.20, 0.80, 0.90))]] #Lumber and Other Construction Materials Merchant Wholesalers
cbp["4235", Commodity_SCTG := c(13L, 14L, 31L, 32L)[1 + findInterval(temprand, c(0.25, 0.50, 0.75))]] #Metal and Mineral (except Petroleum) Merchant Wholesalers
cbp["4237", Commodity_SCTG := c(15L, 33L)[1 + findInterval(temprand, c(0.50))]] #Hardware, and Plumbing and Heating Equipment and Supplies Merchant Wholesalers
cbp["4247", Commodity_SCTG := c(16L, 17L, 18L, 19L)[1 + findInterval(temprand, c(0.25, 0.50, 0.75))]] #Petroleum and Petroleum Products Merchant Wholesalers
cbp["4246", Commodity_SCTG := c(20L, 21L, 22L, 23L)[1 + findInterval(temprand, c(0.25, 0.50, 0.75))]] #Chemical and Allied Products Merchant Wholesalers
cbp["4242", Commodity_SCTG := 21L] #Drugs and Druggists Sundries Merchant Wholesalers
cbp["4234", Commodity_SCTG := 24L] #Professional and Commercial Equipment and Supplies Merchant Wholesalers
cbp["4241", Commodity_SCTG := c(27L, 28L, 29L)[1 + findInterval(temprand, c(0.33, 0.67))]] #Paper and Paper Product Merchant Wholesalers
cbp["4243", Commodity_SCTG := 30L] #Apparel, Piece Goods, and Notions Merchant Wholesalers
cbp["4238", Commodity_SCTG := 34L] #Machinery, Equipment, and Supplies Merchant Wholesalers
cbp["4251", Commodity_SCTG := c(35L, 38L)[1 + findInterval(temprand, c(0.50))]] #Wholesale Electronic Markets and Agents and Brokers
cbp["4236", Commodity_SCTG := c(35L, 38L)[1 + findInterval(temprand, c(0.50))]] #Electrical and Electronic Goods Merchant Wholesalers
cbp["4231", Commodity_SCTG := c(36L, 37L)[1 + findInterval(temprand, c(0.50))]] #Motor Vehicle and Motor Vehicle Parts and Supplies Merchant Wholesalers
cbp["4232", Commodity_SCTG := 39L] #Furniture and Home Furnishing Merchant Wholesalers
cbp["4239", Commodity_SCTG := 40L] #Miscellaneous Durable Goods Merchant Wholesalers
cbp["4249", Commodity_SCTG := 40L] #Miscellaneous Nondurable Goods Merchant Wholesalers
cbp[n2 == "42", Industry_NAICS6_Make := paste0(n4, "00")]

#Assign firms within study areas 
cbpc <- cbp[CBPZONE > 999, list(CBPZONE, BusID, n2, Emp)]
#Assign specific NAICS categories which would be used to locate businesses to tazs
cbpc[n2 %in% c("31", "32", "33"), n2 := "3133"]
cbpc[n2 %in% c("44", "45"), n2 := "4445"]
cbpc[n2 %in% c("48", "49"), n2 := "4849"]
cbpc[n2 %in% c("92"), n2 := "92"] # there is no public administration info from CBP --- this line can be removed

mzemp <- data.table(melt(mzemp, id.vars = c("COUNTY", "MESOZONE")))
mzemp[, n2 := sub("rank", "", as.character(variable))]
setnames(mzemp, c("COUNTY", "value"), c("CBPZONE", "EmpRank"))
cbpc <-
  merge(cbpc,
        mzemp,
        c("CBPZONE", "n2"),
        allow.cartesian = TRUE,
        all.x = TRUE) #Merge the rankings dataset to the firms database based on county
#Select candidate tazs based on the industry of the firm, firm size, and ranking of that particular industry in a taz
cbpc[, candidate := 0L]
cbpc[Emp > 5000 & EmpRank %in% c(9, 10), candidate := 1L]
cbpc[Emp > 2000 &
       Emp <= 5000 & EmpRank %in% c(7:10), candidate := 1L]
cbpc[Emp > 500 &
       Emp <= 2000 & EmpRank %in% c(5:10), candidate := 1L]
cbpc[Emp > 100 & Emp <= 500 & EmpRank %in% c(4:10), candidate := 1L]
cbpc[Emp > 20 & Emp <= 100 & EmpRank %in% c(2:10), candidate := 1L]
cbpc[Emp <= 20 & EmpRank %in% c(1:10), candidate := 1L]
#small number of businesses that did not get a candiate TAZ - allow those to have some candidates (small error is better than omitting the businesses)
ZeroCand <- cbpc[, sum(candidate), by = BusID][V1 == 0]
cbpc[BusID %in% ZeroCand$BusID, candidate := 1L]
cbpc <- cbpc[candidate == 1,] #remove non-candidate TAZs
cbpc[, u := runif(.N)] #Generate a random number based on which one of the candidate tazs would be selected
cbpc <-
  cbpc[cbpc[, .I[which.max(u)], by = BusID]$V1,] #Assign the taz for which the random number generated is the highest among all candidate tazs

#Assign MESOZONES for all firms
cbp[CBPZONE <= 132, MESOZONE := CBPZONE + 20000L]
#cbp[CBPZONE %in% 801:808, MESOZONE:= CBPZONE]
setkey(cbp, BusID)
setkey(cbpc, BusID)
cbp[CBPZONE > 999, MESOZONE := cbpc$MESOZONE]
#Cleanup
rm(mzemp, ZeroCand, cbpc)
cbp[, c("Industry_NAICS6_CBP", "n2", "n4", "est", "temprand") := NULL] #Revome extra fields,

# by end of this part, the output variable 'cbp' has 7,071,215 rows and 8 variables, used 7.77gb of memory



