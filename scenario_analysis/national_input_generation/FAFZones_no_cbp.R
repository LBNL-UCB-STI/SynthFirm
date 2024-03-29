#################################################################################
rm(list = ls())
options(scipen = '10')
list.of.packages <-
  c("dplyr",
    "tidyr",
    "data.table",
    "sf",
    "tmap",
    "tmaptools",
    "base",
    "bit64")
new.packages <-
  list.of.packages[!(list.of.packages %in% installed.packages()[, "Package"])]
if (length(new.packages))
  install.packages(new.packages)
lapply(list.of.packages, require, character = TRUE)
#################################################################################
#install_github("f1kidd/fmlogit")
path2file <-
  "/Users/xiaodanxu/Documents/SynthFirm.nosync"
setwd(path2file)

region_name = 'national'
naics_code <-
  data.table::fread('national_GIS/corresp_naics6_n6io_sctg_revised.csv', h = T)
cbp_data <-
  data.table::fread("national_GIS/data_emp_cbp.csv", h = T)
region_file_name = paste0('national_GIS/', region_name, '_FAFCNTY.csv')
faf_county_lookup <-
  data.table::fread(region_file_name, h = T)

list_of_naics_in_cbp <- unique(cbp_data$Industry_NAICS6_CBP)
naics_code_to_fill <-naics_code %>% filter(!Industry_NAICS6_CBP %in% list_of_naics_in_cbp) %>% as_tibble()

list_of_naics_to_add <- as.character(unique(naics_code_to_fill$Industry_NAICS6_CBP))

#download from U.S. department of labor, QCEW by industry, https://www.bls.gov/cew/downloadable-data-files.htm
list_of_qcew_files <- list.files('BayArea_GIS/2017_annual_by_industry')
qcew_files <- as.data.table(list_of_qcew_files)
qcew_files_sep <- separate(qcew_files, sep = ' ', col = c('list_of_qcew_files'),
                           into = c('s1', 's2', 's3', 's4', 's5', 's6'))

qcew_files_sep <- cbind(qcew_files_sep, qcew_files)
qcew_files_to_add <- qcew_files_sep %>% filter(s2 %in% list_of_naics_to_add) %>% as_tibble() # match with 6 digit

cbp_filled_out <- NULL
for (row in 1:nrow(qcew_files_to_add)) {
  print(qcew_files_to_add[row, 's2'])
  link <- paste0('BayArea_GIS/2017_annual_by_industry/', qcew_files_to_add[row, 'list_of_qcew_files'])
  firm_emp_by_naics <- data.table::fread(link, h = T)
  firm_emp_by_naics <- firm_emp_by_naics %>% filter(agglvl_code == 78, annual_avg_estabs_count > 0) %>% as_tibble()
  firm_emp_by_naics_summary <- firm_emp_by_naics %>%
    group_by(area_fips, industry_code) %>%
    summarise(employment = sum(annual_avg_emplvl), establishment = sum(annual_avg_estabs_count))
  firm_emp_by_naics_summary <- firm_emp_by_naics_summary %>% mutate(ST_CNTY = as.integer(area_fips))
  firm_emp_by_naics_summary <- merge(firm_emp_by_naics_summary, faf_county_lookup, by = 'ST_CNTY', all.x = TRUE, all.y = FALSE)
  firm_emp_by_naics_out <- firm_emp_by_naics_summary %>%
    group_by(industry_code, FAFID, CBPZONE1) %>%
    summarise(emp = sum(employment), est = sum(establishment))
  firm_emp_by_naics_out <- firm_emp_by_naics_out %>% mutate(emp = ifelse(emp == 0, est, emp)) # if not employment found, fill with 1 as a ghost worker (self-employment)
  firm_emp_by_naics_out <- firm_emp_by_naics_out %>% mutate(firm_size = emp/est)
  firm_emp_by_naics_out <- firm_emp_by_naics_out %>% na.exclude()
  
  # assign firm size indicator 
  #1 = '1-19',2 = '20-99',3 ='100-499',4 = '500-999',5 = '1,000-2,499',6 = '2,500-4,999',7 = 'Over 5,000'
  firm_emp_by_naics_out <- firm_emp_by_naics_out %>% mutate(e1 = ifelse(firm_size < 20, est, 0))
  firm_emp_by_naics_out <- firm_emp_by_naics_out %>% mutate(e2 = ifelse((firm_size < 100) & (firm_size>=20), est, 0))
  firm_emp_by_naics_out <- firm_emp_by_naics_out %>% mutate(e3 = ifelse((firm_size < 500) & (firm_size>=100), est, 0))
  firm_emp_by_naics_out <- firm_emp_by_naics_out %>% mutate(e4 = ifelse((firm_size < 1000) & (firm_size>=500), est, 0))
  firm_emp_by_naics_out <- firm_emp_by_naics_out %>% mutate(e5 = ifelse((firm_size < 2500) & (firm_size>=1000), est, 0))
  firm_emp_by_naics_out <- firm_emp_by_naics_out %>% mutate(e6 = ifelse((firm_size < 5000) & (firm_size>=2500), est, 0))
  firm_emp_by_naics_out <- firm_emp_by_naics_out %>% mutate(e7 = ifelse(firm_size >= 5000, est, 0))
  firm_emp_by_naics_out <- firm_emp_by_naics_out %>% select(industry_code,
                                                            FAFID,
                                                            CBPZONE1,
                                                            emp,
                                                            est,
                                                            e1,
                                                            e2,
                                                            e3,
                                                            e4,
                                                            e5,
                                                            e6,
                                                            e7)
  colnames(firm_emp_by_naics_out) <- c('Industry_NAICS6_CBP', 'FAFZONE',	'CBPZONE',
                    'employment',	'establishment',	'e1',	'e2',	'e3',	'e4',	'e5',	'e6',	'e7')
  cbp_filled_out <- rbind(cbp_filled_out, firm_emp_by_naics_out)
  # break
}

industry_code_added <- unique(cbp_filled_out$Industry_NAICS6_CBP)
# naics_code_to_fill <-naics_code_to_fill %>% mutate(processed_industry = ifelse(Industry_NAICS6_CBP %in% industry_code_added, Industry_NAICS6_CBP,
#                                                                                floor(Industry_NAICS6_CBP/10)))  %>% as_tibble()

naics_code_to_fill_remaining <- naics_code_to_fill[(! naics_code_to_fill$Industry_NAICS6_CBP %in% industry_code_added),]
# list_of_naics_to_add_5digit <- unique(naics_code_to_fill_remaining$processed_industry)
# qcew_files_remaining <- qcew_files_sep %>% filter(! s2 %in% industry_code_added) %>% as_tibble()
# qcew_files_to_add_5digit <- qcew_files_sep %>% filter(s2 %in% list_of_naics_to_add_5digit) %>% as_tibble() # match with 6 digit

additional_cbp_filled_out <- NULL
list_of_naics_with_data <- unique(as.numeric(qcew_files_sep$s2))
for (row in 1:nrow(naics_code_to_fill_remaining)) {
  raw_naics <- as.numeric(naics_code_to_fill_remaining[row, 'Industry_NAICS6_CBP'])
  print(raw_naics)
  # find closest level of aggregation to fetch the employment data
  if (raw_naics %in% list_of_naics_with_data){ # 6digit
    s2_value <- as.character(raw_naics)
    agg_lev_code <- 78
  } else if (floor(raw_naics/10) %in% list_of_naics_with_data){ # 5digit
    s2_value <- as.character(floor(raw_naics/10))
    agg_lev_code <- 77
  }else if (floor(raw_naics/100) %in% list_of_naics_with_data){ # 4digit
    s2_value <- as.character(floor(raw_naics/100))
    agg_lev_code <- 76
  }else if (floor(raw_naics/1000) %in% list_of_naics_with_data){ # 3digit
    s2_value <- as.character(floor(raw_naics/1000))
    agg_lev_code <- 75
  }else if (floor(raw_naics/10000) %in% list_of_naics_with_data){ # 2digit
    s2_value <- as.character(floor(raw_naics/10000))
    agg_lev_code <- 74
  }else{
    print(paste(raw_naics, 'file not found!'))
    next}
  file_name <- qcew_files_sep[qcew_files_sep$s2 == s2_value, 'list_of_qcew_files']
  selected_naics <- qcew_files_sep[qcew_files_sep$s2 == s2_value, 's2']
  # print(qcew_files_to_add[row, 's2'])
  link <- paste0('BayArea_GIS/2017_annual_by_industry/', file_name)
  firm_emp_by_naics <- data.table::fread(link, h = T)
  firm_emp_by_naics <- firm_emp_by_naics %>% filter(agglvl_code == agg_lev_code, annual_avg_estabs_count > 0) %>% as_tibble()
  firm_emp_by_naics_summary <- firm_emp_by_naics %>%
    group_by(area_fips, industry_code) %>%
    summarise(employment = sum(annual_avg_emplvl), establishment = sum(annual_avg_estabs_count))
  firm_emp_by_naics_summary <- firm_emp_by_naics_summary %>% mutate(ST_CNTY = as.integer(area_fips))
  firm_emp_by_naics_summary <- merge(firm_emp_by_naics_summary, faf_county_lookup, by = 'ST_CNTY', all.x = TRUE, all.y = FALSE)
  firm_emp_by_naics_out <- firm_emp_by_naics_summary %>%
    group_by(industry_code, FAFID, CBPZONE1) %>%
    summarise(emp = sum(employment), est = sum(establishment))
  firm_emp_by_naics_out <- firm_emp_by_naics_out %>% mutate(emp = ifelse(emp == 0, est, emp)) # if not employment found, fill with 1 as a ghost worker (self-employment)
  firm_emp_by_naics_out <- firm_emp_by_naics_out %>% mutate(firm_size = emp/est)
  firm_emp_by_naics_out <- firm_emp_by_naics_out %>% na.exclude()
  
  # assign firm size indicator 
  #1 = '1-19',2 = '20-99',3 ='100-499',4 = '500-999',5 = '1,000-2,499',6 = '2,500-4,999',7 = 'Over 5,000'
  firm_emp_by_naics_out <- firm_emp_by_naics_out %>% mutate(e1 = ifelse(firm_size < 20, est, 0))
  firm_emp_by_naics_out <- firm_emp_by_naics_out %>% mutate(e2 = ifelse((firm_size < 100) & (firm_size>=20), est, 0))
  firm_emp_by_naics_out <- firm_emp_by_naics_out %>% mutate(e3 = ifelse((firm_size < 500) & (firm_size>=100), est, 0))
  firm_emp_by_naics_out <- firm_emp_by_naics_out %>% mutate(e4 = ifelse((firm_size < 1000) & (firm_size>=500), est, 0))
  firm_emp_by_naics_out <- firm_emp_by_naics_out %>% mutate(e5 = ifelse((firm_size < 2500) & (firm_size>=1000), est, 0))
  firm_emp_by_naics_out <- firm_emp_by_naics_out %>% mutate(e6 = ifelse((firm_size < 5000) & (firm_size>=2500), est, 0))
  firm_emp_by_naics_out <- firm_emp_by_naics_out %>% mutate(e7 = ifelse(firm_size >= 5000, est, 0))
  firm_emp_by_naics_out <- firm_emp_by_naics_out %>% select(industry_code,
                                                            FAFID,
                                                            CBPZONE1,
                                                            emp,
                                                            est,
                                                            e1,
                                                            e2,
                                                            e3,
                                                            e4,
                                                            e5,
                                                            e6,
                                                            e7)
  colnames(firm_emp_by_naics_out) <- c('Industry_NAICS_selected', 'FAFZONE',	'CBPZONE',
                                       'employment',	'establishment',	'e1',	'e2',	'e3',	'e4',	'e5',	'e6',	'e7')
  firm_emp_by_naics_out$Industry_NAICS6_CBP = raw_naics
  additional_cbp_filled_out <- rbind(additional_cbp_filled_out, firm_emp_by_naics_out)
  # break
}
write.csv(additional_cbp_filled_out, 'national_GIS/gap_filling_cbp_data.csv')

additional_cbp_out <- additional_cbp_filled_out %>% 
  ungroup() %>% 
  select(Industry_NAICS6_CBP, FAFZONE,	CBPZONE, employment,	establishment,	e1,	e2,	e3,	e4,	e5,	e6,	e7)

final_cbp_with_gap_filling <- rbind(cbp_data, cbp_filled_out, additional_cbp_out)
write.csv(final_cbp_with_gap_filling, 'national_GIS/data_emp_cbp_imputed.csv')
total_emp <- sum(final_cbp_with_gap_filling$employment)
total_est <- sum(final_cbp_with_gap_filling$establishment)


old_emp <- sum(cbp_data$employment)
old_est <- sum(cbp_data$establishment)
