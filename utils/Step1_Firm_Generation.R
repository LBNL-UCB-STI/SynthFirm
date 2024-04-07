#-----------------------------------------------------------------------------------
#Step 1 Firm Synthesis
#-----------------------------------------------------------------------------------

# rm(list = ls())
#path2code <- '/Users/xiaodanxu/Documents/GitHub/SynthFirm/utils/'
# source('step0_SynthFirm_starter.R') # load packages
# # source('scenario/scenario_variables.R')  # load environmental variable
# source('config.R')
# path2file <-
#   "/Users/xiaodanxu/Documents/SynthFirm.nosync"
# setwd(path2file)
#
print("Start synthetic firm generation...")

# cbp <- data.table::fread(paste0(path2file, "/inputs_", scenario, '/', cbp_file), h = T)
# mzemp <-
#   data.table::fread(paste0(path2file, "/inputs_", scenario, '/', mzemp_file), h = T)
#
# c_n6_n6io_sctg <-
#   data.table::fread(paste0(path2file, "/SynthFirm_parameters/", c_n6_n6io_sctg_file), h = T)
# employment_per_firm <-
#   data.table::fread(paste0(path2file, "/SynthFirm_parameters/", employment_per_firm_file), h = T)
# employment_per_firm_gapfill <-
#   data.table::fread(paste0(path2file, "/SynthFirm_parameters/", employment_per_firm_gapfill_file), h = T)

output_dir <- paste0(path2file, "/outputs_", out_scenario)
if (!dir.exists(output_dir)){
  dir.create(output_dir)
}else{
  print("output directory exists")
}

################ part 1 ######################
#-----------------------------------------------------------------------------------
#Enumerating firms and merge with correspondenses
#-----------------------------------------------------------------------------------
print("Enumerating Firms")


#Aggregate the employment data by zones, NAICS, and firm size category
#1 = '1-19',2 = '20-99',3 ='100-499',4 = '500-999',5 = '1,000-2,499',6 = '2,500-4,999',7 = 'Over 5,000'

cbp <- cbp %>% mutate(employment = ifelse(employment < establishment, establishment, employment)) %>% as_tibble()
total_employment = sum(cbp$employment)

cbp <- as.data.table(cbp)
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
cbp_long[, esizecat := as.integer(esizecat)] #convert esizecat to an integer (1:7)
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

cbp_long <- as.data.table(cbp_long)
firms <-
  cbp_long[rep(seq_len(cbp_long[, .N]), est),] #Enumerates the agent businesses using the est variable.
firms[, BusID := .I] #Add an ID

emp_obs <- cbp %>%
  group_by(Industry_NAICS6_CBP, CBPZONE, FAFZONE) %>%
  summarise(total_emp_obs = sum(employment)) %>% as_tibble()

emp_sim <- firms %>%
  group_by(Industry_NAICS6_CBP, CBPZONE, FAFZONE) %>%
  summarise(total_emp_sim = sum(emp_per_est)) %>% as_tibble()

emp_adj <- emp_obs %>% left_join(emp_sim, by = c('Industry_NAICS6_CBP', 'CBPZONE', 'FAFZONE'))
emp_adj <- emp_adj %>% mutate(emp_adj = total_emp_obs/total_emp_sim)

firms <- firms %>% left_join(emp_adj, by = c('Industry_NAICS6_CBP', 'CBPZONE', 'FAFZONE')) %>% as_tibble()
firms <- firms %>% mutate(emp_per_est = emp_per_est * emp_adj)

# summarise(cbp_emp)
total_employment_est = sum(firms$emp_per_est)

print('total number of firm is ')
print(sum(cbp_long$est))
print('total number of employment is ')
print(total_employment_est)
#-----------------------------------------------------------------------------------
# Allocating specific commodity and location for each establishment
#-----------------------------------------------------------------------------------
# print("Allocating commodities and locations to establishments")



#Assign firms within study areas 
firms <- as.data.table(firms)
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
        all.x = TRUE) # Merge the rankings dataset to the firms database based on county

#Select candidate tazs based on the industry of the firm, firm size, and ranking of that particular industry in a census block group
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
firms[CBPZONE <= 999, MESOZONE := CBPZONE + 20000L]
#cbp[CBPZONE %in% 801:808, MESOZONE:= CBPZONE]
setkey(firms, BusID)
setkey(firms_in_boundary, BusID)
firms[CBPZONE > 999, MESOZONE := firms_in_boundary$MESOZONE]
#Cleanup
rm(mzemp, ZeroCand, firms_in_boundary)
firms[, c("Industry_NAICS6_CBP", "total_emp_obs", "total_emp_sim", "emp_adj", "n2", "n4", "est") := NULL] #Revome extra fields,
setnames(firms, "emp_per_est", "Emp")
write.csv(firms, paste0(output_dir, '/', synthetic_firms_no_location_file), row.names=FALSE)
print('synthetic firm generation is done!')




