#################################################################################
rm(list = ls())
options(scipen = '10')
list.of.packages <-
  c("dplyr",
    "data.table",
    "sf",
    "tmap",
    "tmaptools",
    "bit64")
new.packages <-
  list.of.packages[!(list.of.packages %in% installed.packages()[, "Package"])]
if (length(new.packages))
  install.packages(new.packages)
lapply(list.of.packages, require, character = TRUE)
#################################################################################
#install_github("f1kidd/fmlogit")
path2file <-
  "/Users/xiaodanxu/Documents/SynthFirm/BayArea_GIS"
setwd(path2file)

# link: https://www2.census.gov/programs-surveys/susb/datasets/2016/us_state_naics_detailedsizes_2016.txt

employment_by_firm_size = data.table::fread("us_state_naics_detailedsizes_2016.txt", h = T)
employment_by_firm_size <- employment_by_firm_size %>% filter(NAICS != '--')
employment_by_firm_size <- employment_by_firm_size %>% mutate(NAICS = as.integer(NAICS))
employment_by_firm_size = employment_by_firm_size %>% na.exclude()
employment_by_firm_size <- employment_by_firm_size %>% mutate(naics_code_size = floor(log10(NAICS)) + 1)

employment_by_6digit_naics <- employment_by_firm_size %>% filter(naics_code_size == 6)
employment_by_6digit_naics <- employment_by_6digit_naics %>% filter(!ENTRSIZE %in% c(1, 6, 19))
# employment_by_6digit_naics <- employment_by_6digit_naics %>% mutate(emp_per_est  = EMPL/ESTB)
#1 = '1-19',2 = '20-99',3 ='100-499',4 = '500-999',5 = '1,000-2,499',6 = '2,500-4,999',7 = 'Over 5,000'
employment_by_6digit_naics <- employment_by_6digit_naics %>% mutate(size_group = ifelse(ENTRSIZE %in% c(2,3,4,5), 1,
                                                                                 ifelse(ENTRSIZE %in% c(7,8,9,10,11,12,13), 2,
                                                                                 ifelse(ENTRSIZE %in% c(14,15,16,17,18), 3,
                                                                                 ifelse(ENTRSIZE %in% c(20,21), 4,
                                                                                 ifelse(ENTRSIZE %in% c(22,23,24), 5,
                                                                                 ifelse(ENTRSIZE %in% c(25), 6, 7)))))))

employment_by_6digit_naics <- employment_by_6digit_naics%>% filter(EMPL > 0)
employment_by_size_group <- employment_by_6digit_naics %>% 
  group_by(NAICS, size_group) %>% 
  summarise(total_emp = sum(EMPL), total_est = sum(ESTB), total_firm = sum(FIRM))

employment_by_size_group <- employment_by_size_group %>% mutate(emp_per_est  = total_emp/total_est, est_per_firm = total_est/total_firm)

employment_by_size_group_filtered <- employment_by_size_group %>% filter(est_per_firm < 1.5)
write.csv(employment_by_size_group_filtered, 'employment_by_firm_size_naics.csv')

employment_by_size_group_no_naics <- employment_by_size_group_filtered %>% 
  group_by(size_group) %>% 
  summarise(total_emp = sum(total_emp), total_est = sum(total_est))

employment_by_size_group_no_naics <- employment_by_size_group_no_naics %>% mutate(emp_per_est  = total_emp/total_est)
write.csv(employment_by_size_group_no_naics, 'employment_by_firm_size_gapfill.csv')
