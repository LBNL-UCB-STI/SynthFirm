#-----------------------------------------------------------------------------------
#Step 1 Firm Synthesis
#-----------------------------------------------------------------------------------

rm(list = ls())
path2code <- '/Users/xiaodanxu/Documents/GitHub/SynthFirm/Firm Synthesis/scripts/'
source(paste0(path2code, 'step0_SynthFirm_starter.R')) # load packages
source(paste0(path2code, 'scenario/scenario_variables.R'))  # load environmental variable

path2file <-
  "/Users/xiaodanxu/Documents/SynthFirm.nosync"
setwd(path2file)

c_n6_n6io_sctg <-
  data.table::fread("./inputs/corresp_naics6_n6io_sctg_revised.csv", h = T)
cbp <- data.table::fread("./inputs/data_emp_cbp_imputed.csv", h = T)
mzemp <-
  data.table::fread("./inputs/data_mesozone_emprankings.csv", h = T)
employment_per_firm <- 
  data.table::fread("./inputs/employment_by_firm_size_naics.csv", h = T)
employment_per_firm_gapfill <- 
  data.table::fread("./inputs/employment_by_firm_size_gapfill.csv", h = T)
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
#1 = '1-19',2 = '20-99',3 ='100-499',4 = '500-999',5 = '1,000-2,499',6 = '2,500-4,999',7 = 'Over 5,000'
#cbp <- as.data.table(cbp)
# colnames(cbp) <- c('Industry_NAICS6_CBP', 'FAFZONE',	'CBPZONE',
#                   'employment',	'establishment',	'e1',	'e2',	'e3',	'e4',	'e5',	'e6',	'e7',	'e8')
total_employment = sum(cbp$employment)
print(total_employment)
cbp_by_location <-
  cbp[!is.na(CBPZONE) &
        !is.na(FAFZONE) & !is.na(Industry_NAICS6_CBP),
      list(
        e1 = sum(e1),
        e2 = sum(e2),
        e3 = sum(e3),
        e4 = sum(e4),
        e5 = sum(e5),
        e6 = sum(e6),
        e7 = sum(e7)
      ),
      by = list(Industry_NAICS6_CBP, CBPZONE, FAFZONE)] #Remove records with missing zones and NAICS codes
setkey(c_n6_n6io_sctg, Industry_NAICS6_CBP)
cbp_by_industry <-
  merge(cbp_by_location, c_n6_n6io_sctg[, list(Industry_NAICS6_CBP, Industry_NAICS6_Make, Commodity_SCTG)], by =
          "Industry_NAICS6_CBP") #Merge in the I/O NAICS codes and SCTG codes
cbp_by_industry[, c("n2", "n4") := list(substr(Industry_NAICS6_CBP, 1, 2),
                                  substr(Industry_NAICS6_CBP, 1, 4))] #add 2 and 4 digit NAICS
cbp_long <-
  data.table::melt(
    cbp_by_industry,
    measure.vars = paste0("e", 1:7),
    variable.name = "esizecat",
    value.name = "est"
  ) #Melt to create separate rows for each firm size category
cbp_long <- as.data.table(cbp_long)
cbp_long[, esizecat := as.integer(esizecat)] #convert esizecat to an integer (1:8)
cbp_long <- cbp_long %>% filter(est > 0) %>% as_tibble()

# assign employment size per category
# cbp[, Emp := c(10L, 60L, 175L, 375L, 750L, 1750L, 3750L, 7500L)[esizecat]] # Estimate the number of employees
employment_per_firm <- employment_per_firm %>% select(NAICS, size_group, emp_per_est) %>% as_tibble()
cbp_long <- merge(cbp_long, employment_per_firm, by.x = c('Industry_NAICS6_CBP', 'esizecat'),
                  by.y = c('NAICS', 'size_group'), all.x = TRUE)

emp_gap_fill <- pull(employment_per_firm_gapfill, 'emp_per_est')
cbp_long <- cbp_long %>% mutate(emp_per_est = ifelse(is.na(emp_per_est), ifelse(esizecat ==1, emp_gap_fill[1],
                                                                         ifelse(esizecat ==2, emp_gap_fill[2], 
                                                                         ifelse(esizecat ==3, emp_gap_fill[3], 
                                                                         ifelse(esizecat ==4, emp_gap_fill[4], 
                                                                         ifelse(esizecat ==5, emp_gap_fill[5], 
                                                                         ifelse(esizecat ==6, emp_gap_fill[6], emp_gap_fill[7])))))), 
                                                     emp_per_est)) %>% as_tibble()


print('total number of firm is ')
print(sum(cbp_long$est))

cbp_long <- as.data.table(cbp_long)
firms <-
  cbp_long[rep(seq_len(cbp_long[, .N]), est),] #Enumerates the agent businesses using the est variable.
firms[, BusID := .I] #Add an ID

# cbp_emp <- cbp %>% 
#   group_by(Industry_NAICS6_CBP, CBPZONE, FAFZONE) %>% 
#   summarise(total_emp_sim = sum(Emp)) %>% as_tibble()
# 
# 
# cbp_emp <- cbp_emp %>% left_join(cbp, by = c('Industry_NAICS6_CBP', 'CBPZONE', 'FAFZONE'))
# cbp_emp <- cbp_emp %>% mutate(emp_adj = employment/total_emp_sim)
# cbp_emp
# summarise(cbp_emp)
total_employment_est = sum(firms$emp_per_est)
#-----------------------------------------------------------------------------------
# Allocating specific commodity and location for each establishment
#-----------------------------------------------------------------------------------
# print("Allocating commodities and locations to establishments")

#TODO: are these still appropriate correspodences given the new I/O data?
#This section identifies producers who make 2+ commodities (especially wholesalers) and
#simulates a specific commodity for them based on probability thresholds for multiple commodities
# set.seed(151)
# firms[, temprand := runif(.N)]
# #For all the NAICS which may produce more than one SCTG commodity, simulate one SCTG commodity using set probability thresholds
# setkey(firms, Industry_NAICS6_CBP)
# firms[list(211111), Commodity_SCTG := c(16L, 19L)[1 + findInterval(temprand, c(0.45))]]
# firms[list(324110), Commodity_SCTG := c(17L, 18L, 19L)[1 + findInterval(temprand, c(0.25, 0.50))]]
# 
# setkey(firms, n4)
# firms["4245", Commodity_SCTG := c(1L, 2L, 3L, 4L)[1 + findInterval(temprand, c(0.25, 0.50, 0.75))]] #Farm Product Raw Material Merchant Wholesalers
# firms["4244", Commodity_SCTG := c(5L, 6L, 7L, 9L)[1 + findInterval(temprand, c(0.25, 0.50, 0.75))]] #Grocery and Related Product Wholesalers
# firms["4248", Commodity_SCTG := 8L] #Beer, Wine, and Distilled Alcoholic Beverage Merchant Wholesalers
# firms["4233", Commodity_SCTG := c(10L, 11L, 12L, 25L, 26L)[1 + findInterval(temprand, c(0.10, 0.20, 0.80, 0.90))]] #Lumber and Other Construction Materials Merchant Wholesalers
# firms["4235", Commodity_SCTG := c(13L, 14L, 31L, 32L)[1 + findInterval(temprand, c(0.25, 0.50, 0.75))]] #Metal and Mineral (except Petroleum) Merchant Wholesalers
# firms["4237", Commodity_SCTG := c(15L, 33L)[1 + findInterval(temprand, c(0.50))]] #Hardware, and Plumbing and Heating Equipment and Supplies Merchant Wholesalers
# firms["4247", Commodity_SCTG := c(16L, 17L, 18L, 19L)[1 + findInterval(temprand, c(0.25, 0.50, 0.75))]] #Petroleum and Petroleum Products Merchant Wholesalers
# firms["4246", Commodity_SCTG := c(20L, 21L, 22L, 23L)[1 + findInterval(temprand, c(0.25, 0.50, 0.75))]] #Chemical and Allied Products Merchant Wholesalers
# firms["4242", Commodity_SCTG := 21L] #Drugs and Druggists Sundries Merchant Wholesalers
# firms["4234", Commodity_SCTG := 24L] #Professional and Commercial Equipment and Supplies Merchant Wholesalers
# firms["4241", Commodity_SCTG := c(27L, 28L, 29L)[1 + findInterval(temprand, c(0.33, 0.67))]] #Paper and Paper Product Merchant Wholesalers
# firms["4243", Commodity_SCTG := 30L] #Apparel, Piece Goods, and Notions Merchant Wholesalers
# firms["4238", Commodity_SCTG := 34L] #Machinery, Equipment, and Supplies Merchant Wholesalers
# firms["4251", Commodity_SCTG := c(35L, 38L)[1 + findInterval(temprand, c(0.50))]] #Wholesale Electronic Markets and Agents and Brokers
# firms["4236", Commodity_SCTG := c(35L, 38L)[1 + findInterval(temprand, c(0.50))]] #Electrical and Electronic Goods Merchant Wholesalers
# firms["4231", Commodity_SCTG := c(36L, 37L)[1 + findInterval(temprand, c(0.50))]] #Motor Vehicle and Motor Vehicle Parts and Supplies Merchant Wholesalers
# firms["4232", Commodity_SCTG := 39L] #Furniture and Home Furnishing Merchant Wholesalers
# firms["4239", Commodity_SCTG := 40L] #Miscellaneous Durable Goods Merchant Wholesalers
# firms["4249", Commodity_SCTG := 40L] #Miscellaneous Nondurable Goods Merchant Wholesalers
# firms[n2 == "42", Industry_NAICS6_Make := paste0(n4, "00")]

#Assign firms within study areas 
firms_in_boundary <- firms[CBPZONE > 999, list(CBPZONE, BusID, n2, emp_per_est)]
#Assign specific NAICS categories which would be used to locate businesses to tazs
firms_in_boundary[n2 %in% c("31", "32", "33"), n2 := "3133"]
firms_in_boundary[n2 %in% c("44", "45"), n2 := "4445"]
firms_in_boundary[n2 %in% c("48", "49"), n2 := "4849"]
firms_in_boundary[n2 %in% c("92"), n2 := "92"] 

emp_ranking_in_boundary <- data.table(melt(mzemp, id.vars = c("COUNTY", "MESOZONE")))
emp_ranking_in_boundary[, n2 := sub("rank", "", as.character(variable))]
setnames(emp_ranking_in_boundary, c("COUNTY", "value"), c("CBPZONE", "EmpRank"))
firms_in_boundary <-
  merge(firms_in_boundary,
        emp_ranking_in_boundary,
        c("CBPZONE", "n2"),
        allow.cartesian = TRUE,
        all.x = TRUE) #Merge the rankings dataset to the firms database based on county
#Select candidate tazs based on the industry of the firm, firm size, and ranking of that particular industry in a taz
firms_in_boundary[, candidate := 0L]
firms_in_boundary[emp_per_est > 5000 & EmpRank %in% c(9, 10), candidate := 1L]
firms_in_boundary[emp_per_est > 2000 &
                    emp_per_est <= 5000 & EmpRank %in% c(7:10), candidate := 1L]
firms_in_boundary[emp_per_est > 500 &
                    emp_per_est <= 2000 & EmpRank %in% c(5:10), candidate := 1L]
firms_in_boundary[emp_per_est > 100 & emp_per_est <= 500 & EmpRank %in% c(4:10), candidate := 1L]
firms_in_boundary[emp_per_est > 20 & emp_per_est <= 100 & EmpRank %in% c(2:10), candidate := 1L]
firms_in_boundary[emp_per_est <= 20 & EmpRank %in% c(1:10), candidate := 1L]
#small number of businesses that did not get a candiate TAZ - allow those to have some candidates (small error is better than omitting the businesses)
ZeroCand <- firms_in_boundary[, sum(candidate), by = BusID][V1 == 0]
firms_in_boundary[BusID %in% ZeroCand$BusID, candidate := 1L]
firms_in_boundary <- firms_in_boundary[candidate == 1,] #remove non-candidate TAZs
firms_in_boundary[, u := runif(.N)] #Generate a random number based on which one of the candidate tazs would be selected
firms_in_boundary <-
  firms_in_boundary[firms_in_boundary[, .I[which.max(u)], by = BusID]$V1,] #Assign the taz for which the random number generated is the highest among all candidate tazs

#Assign MESOZONES for all firms
firms[CBPZONE <= 132, MESOZONE := CBPZONE + 20000L]
#cbp[CBPZONE %in% 801:808, MESOZONE:= CBPZONE]
setkey(firms, BusID)
setkey(firms_in_boundary, BusID)
firms[CBPZONE > 999, MESOZONE := firms_in_boundary$MESOZONE]
#Cleanup
rm(mzemp, ZeroCand, firms_in_boundary)
firms[, c("Industry_NAICS6_CBP", "n2", "n4", "est") := NULL] #Revome extra fields,
setnames(firms, "emp_per_est", "Emp")
write.csv(firms, './outputs/synthetic_firms.csv', row.names=FALSE)
# by end of this part, the output variable 'cbp' has 7,071,215 rows and 8 variables, used 7.77gb of memory



