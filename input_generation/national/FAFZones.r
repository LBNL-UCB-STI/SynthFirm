#################################################################################
rm(list = ls())
options(scipen = '10')
list.of.packages <-
  c("dplyr",
    "data.table",
    "sf",
    "tmap",
    "tmaptools",
    "bit64", 
    "tidycensus", 
    "tigris", 
    "tidyr")
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

# define inputs
region_name = 'Seattle'
state_name = 'WA'
analysis_year = 2016 # cbp year
# this is a critical step in selecting study area boundary
study_area_faf <- c(531, 532, 539, 411) # Seattle 
# study_area_faf <- c(62, 64, 65, 69) #sf ones
# study_area_faf <- c(481, 488, 489) #austin ones

regional_analysis = 0 # 0 - national analysis, 1 - regional analysis
# Users can refer to 'CFS_FAF_LOOKUP.csv' for description of FAF zones and select zone IDs that covers study area

cfs_df = sf::st_read("RawData/CFS2017_Areas.geojson")
cfs_lookup = fread("RawData/CFS_FAF_LOOKUP.csv",
                   colClasses = list(character = c("STFIPS", "CFSMA", "FAF")),
                   h = T)

cfs_df = cfs_df %>% mutate(ST_MA = paste0(ANSI_ST, "-", CFS17_AREA))

cfs_faf = cfs_df %>% left_join(cfs_lookup, by = c("ST_MA"))
cfs_faf = cfs_faf %>% select(ANSI_ST, ANSI_CNTY, ST_MA, FAF, CFSAREANAM)
faf_zones = cfs_faf %>% group_by(FAF) %>% summarize()

#sf::st_write(cfs_faf, "cfs_faf_lookup.geojson")
# sf::st_write(faf_zones, "RawData/FAF5Zones.geojson")

#require(ggplot2)
#ggplot() + geom_sf(data=faf_zones, aes(fill = factor(FAF)))


faf_cnty = cfs_faf %>% group_by(FAF, ANSI_ST, ANSI_CNTY) %>% summarize(count = n())

faf_cnty = faf_cnty %>% mutate(FAFID = as.numeric(FAF))

faf_df = as.data.frame(faf_cnty) %>% select(FAFID, ANSI_ST, ANSI_CNTY)

if (regional_analysis == 0){ # generate attributes for national-level run
  faf_all = faf_df %>% mutate(CBPZONE  = paste0(ANSI_ST, ANSI_CNTY))
  faf_all = faf_all %>% mutate(CBPZONE1 = as.numeric(CBPZONE))
  faf_all = faf_all %>% mutate(ST_CNTY = paste0(ANSI_ST, ANSI_CNTY))
}

# this is needed for region-specific input
if (regional_analysis == 1){
  faf_df1 = faf_df %>% filter(FAFID %in% study_area_faf) # select FAF zones within study area
  faf_df2 = faf_df %>% anti_join(faf_df1, by = c("FAFID")) %>% arrange(FAFID) # select FAF zones outside study area
  
  faf_df1 = faf_df1 %>% mutate(CBPZONE = paste0(ANSI_ST, ANSI_CNTY)) # 5-digit fips code
  
  faf_df3 = faf_df2 %>% group_by(FAFID) %>% summarize(count = n()) %>% mutate(CBPZONE = seq(1, length(FAFID)))
  
  faf_df2 = faf_df2 %>% left_join(faf_df3, by = c("FAFID"))
  
  faf_external = faf_df2 %>% select(FAFID, ANSI_ST, ANSI_CNTY, CBPZONE) %>% mutate(CBPZONE = as.character(CBPZONE))
  faf_internal = faf_df1
  faf_all = bind_rows(faf_internal, faf_external) %>% mutate(CBPZONE1 = as.numeric(CBPZONE))
  faf_all = faf_all %>% mutate(ST_CNTY = paste0(ANSI_ST, ANSI_CNTY))
}
#library(tigris)
us <- unique(fips_codes$state)[1:51]
# this function relies on tigris
combined_counties <- rbind_tigris(
  lapply(us, function(x) {
    counties(x, cb = TRUE, year = analysis_year)
  })
)
combined_counties <- combined_counties %>% st_drop_geometry()
combined_counties <- combined_counties %>% select(STATEFP,GEOID, NAME)
states = states(year = analysis_year)
states <- states %>% st_drop_geometry()
states <- states %>% select(STATEFP, STUSPS)
combined_counties <- combined_counties %>%
  merge(states, by = c('STATEFP'), all.x=TRUE)

faf_all <- faf_all %>% 
  merge(combined_counties, by.x = 'ST_CNTY', by.y = 'GEOID', all.x = TRUE)

file_name = paste0('inputs_', region_name, '/', region_name, '_FAFCNTY.csv')
data.table::fwrite(faf_all, file_name) 

# load CBP cpmplete county file from Census.gov, example: https://www.census.gov/data/datasets/2017/econ/cbp/2017-cbp.html
# load county-level cbp
f1 = data.table::fread("RawData/CBP/cbp16co.txt",
                       colClasses = list(
                         character = c("fipstate", "fipscty", "naics", "censtate",
                                       "cencty")
                       ),
                       h = T) # the 2017 cbp data has some unmatching total issues

f1$naics6 = as.numeric(f1$naics)
f2 = f1 %>% filter(!is.na(naics6)) %>% mutate(ST_CNTY = paste0(fipstate, fipscty)) %>% as_tibble()


f2$n1_4 = as.numeric(f2$n1_4)
f2$n5_9 = as.numeric(f2$n5_9)
f2$n10_19 = as.numeric(f2$n10_19)
f2$n20_49 = as.numeric(f2$n20_49)
f2$n50_99 = as.numeric(f2$n50_99)
f2$n100_249 = as.numeric(f2$n100_249)
f2$n250_499 = as.numeric(f2$n250_499)
f2$n500_999 = as.numeric(f2$n500_999)
f2$n1000 = as.numeric(f2$n1000)
f2$n1000_1 = as.numeric(f2$n1000_1)
f2$n1000_2 = as.numeric(f2$n1000_2)
f2$n1000_3 = as.numeric(f2$n1000_3)
f2$n1000_4 = as.numeric(f2$n1000_4)

f2[is.na(f2)] <- 0

#  employment size group for firms
#1 = '1-19',2 = '20-99',3 ='100-499',4 = '500-999',5 = '1,000-2,499',6 = '2,500-4,999',7 = 'Over 5,000'

f2 = f2 %>% mutate(
  e1 =  n1_4 + n5_9 + n10_19,
  e2 = n20_49 + n50_99,
  e3 = n100_249 + n250_499,
  e4 = n500_999,
  e5 = n1000_1 + n1000_2,
  e6 = n1000_3,
  e7 = n1000_4
)

f3 = f2 %>% select(naics6, ST_CNTY, empflag, emp, est, e1, e2, e3, e4, e5, e6, e7)

# for national implementation, using zip-code level cbp data
f1_zip = data.table::fread("RawData/CBP/zbp16detail.txt", colClasses = list(
                            character = c("zip", "naics")), h = T)
f1_zip$naics6 = as.numeric(f1_zip$naics)
f1_zip = f1_zip %>% filter(!is.na(naics6)) 


#%>% mutate(ST_CNTY = paste0(fipstate, fipscty)) %>% as_tibble()
# https://www2.census.gov/programs-surveys/rhfs/cbp/technical%20documentation/2015_record_layouts/county_layout_2015.txt
f1_zip$n1_4 = as.numeric(f1_zip$n1_4)
f1_zip$n5_9 = as.numeric(f1_zip$n5_9)
f1_zip$n10_19 = as.numeric(f1_zip$n10_19)
f1_zip$n20_49 = as.numeric(f1_zip$n20_49)
f1_zip$n50_99 = as.numeric(f1_zip$n50_99)
f1_zip$n100_249 = as.numeric(f1_zip$n100_249)
f1_zip$n250_499 = as.numeric(f1_zip$n250_499)
f1_zip$n500_999 = as.numeric(f1_zip$n500_999)
f1_zip$n1000 = as.numeric(f1_zip$n1000)
f1_zip$n1000_1 = as.numeric(f1_zip$n1000_1)
f1_zip$n1000_2 = as.numeric(f1_zip$n1000_2)
f1_zip$n1000_3 = as.numeric(f1_zip$n1000_3)
f1_zip$n1000_4 = as.numeric(f1_zip$n1000_4)

f1_zip[is.na(f1_zip)] <- 0
f1_zip <- f1_zip %>%
  mutate(emp_e =  2*n1_4 + 7 * n5_9 + 15 *n10_19 + 35* n20_49 + 75 * n50_99 + 175 * n100_249+
           375*n250_499 + 750 *n500_999 + 1250 * n1000_1 + 2000 * n1000_2 + 3750 * n1000_3 + 5000 * n1000_4)

#  employment size group for firms
#1 = '1-19',2 = '20-99',3 ='100-499',4 = '500-999',5 = '1,000-2,499',6 = '2,500-4,999',7 = 'Over 5,000'

f1_zip = f1_zip %>% mutate(
  e1 =  n1_4 + n5_9 + n10_19,
  e2 = n20_49 + n50_99,
  e3 = n100_249 + n250_499,
  e4 = n500_999,
  e5 = n1000_1 + n1000_2,
  e6 = n1000_3,
  e7 = n1000_4
)

f1_zip = f1_zip %>% select(zip, naics6, emp_e, est, e1, e2, e3, e4, e5, e6, e7)

print('total firms from cbp:')
print(sum(f3$est))
print('total firms from zbp:')
print(sum(f1_zip$est))

print('total employment from cbp:')
print(sum(f3$emp))
print('total employment scale factor from zbp:')
print(sum(f1_zip$emp_e))

# load additional cbp file to generate zip-county crosswalk 
cbp_summary = data.table::fread("RawData/CBP/zbp16totals.txt", colClasses = list(
  character = c("zip")), h = T)
cbp_summary <- cbp_summary %>%
  select(zip, stabbr, cty_name)

combined_counties <- combined_counties %>% mutate(NAME = toupper(NAME)) %>%
  mutate(NAME = gsub("-", " ", NAME))

spatial_crosswalk <- cbp_summary %>% merge(combined_counties, 
                                           by.x = c('stabbr', 'cty_name'), 
                                           by.y=c('STUSPS','NAME'), all.x = TRUE)
spatial_crosswalk_nona <- spatial_crosswalk %>% filter(! is.na(GEOID)) %>% select(zip, stabbr, cty_name, GEOID)
spatial_crosswalk_na <- spatial_crosswalk %>% filter(is.na(GEOID)) %>%select(zip, stabbr, cty_name)

zip_ct_file = data.table::fread("RawData/CBP/ZIP_COUNTY_LOOKUP_2016_clean.csv",colClasses = list(
   character = c("zip", "geoid")), h = T)
zip_ct_file <- zip_ct_file %>% select(zip, geoid)

us_geoids <- unique(combined_counties$GEOID)
zip_ct_file <- zip_ct_file %>% filter(geoid %in% us_geoids)

spatial_crosswalk_na <- spatial_crosswalk_na %>% 
  merge(zip_ct_file, by = 'zip', all.x = TRUE) %>% rename(GEOID = geoid)

spatial_crosswalk <- rbind(spatial_crosswalk_nona, spatial_crosswalk_na)

spatial_crosswalk <- spatial_crosswalk %>%
  merge(combined_counties, by = c('GEOID'), all.x = TRUE)

spatial_crosswalk <- spatial_crosswalk %>% distinct(zip, .keep_all = TRUE)

# spatial_crosswalk <- spatial_crosswalk %>% 
#   mutate(multi_ct = ifelse(duplicated(zip), 1, 0))
# save zip code to county crosswalk
file_name = paste0('SynthFirm_parameters/', 'zip_to_county.csv')
data.table::fwrite(spatial_crosswalk, file_name) 

spatial_crosswalk_na <- spatial_crosswalk %>% filter(is.na(GEOID))

# assign county to zbp
cbp_by_zip_and_county <- f1_zip %>% 
  merge(spatial_crosswalk, by = c('zip'), all.x = TRUE) %>% rename(geoid = GEOID)

# fill missing geoid for zip != 99999 (using geoid of nearby zip code)
cbp_by_zip_and_county <- cbp_by_zip_and_county %>% arrange(zip) %>%
  fill(geoid, .direction = "down") %>%
  mutate(geoid = ifelse(zip == '99999', NA, geoid))

# fill missing geoid for zip == 99999 (using geoid from the same industry)
cbp_by_zip_and_county <- cbp_by_zip_and_county %>% arrange(naics6) %>%
  fill(geoid, .direction = "down")
  
cbp_by_county <- f3 %>% select(naics6, ST_CNTY, emp) %>%rename(geoid=ST_CNTY)

cbp_by_zip_and_county <- cbp_by_zip_and_county %>%
  merge(cbp_by_county, by = c('naics6', 'geoid'), all.x = TRUE)
missing_zipcode <- unique(cbp_by_zip_and_county[is.na(cbp_by_zip_and_county$geoid), 'zip'])

# the missing value in joint table is caused by missing records under CBP, 
# which might be attributed to mismatch between zip code and county
print(sum(is.na(cbp_by_zip_and_county$emp)))
cbp_by_zip_and_county <- cbp_by_zip_and_county %>% 
  mutate(emp = ifelse(is.na(emp), 0, emp))

print(sum(is.na(cbp_by_zip_and_county$emp)))
# print(sum(cbp_by_zip_and_county$emp))
cbp_by_zip_and_county <- cbp_by_zip_and_county %>%
  group_by(naics6, geoid) %>%
  mutate(emp_frac = emp_e / sum(emp_e)) %>%
  mutate(emp_frac = ifelse(is.na(emp_frac), 0, emp_frac))

cbp_by_zip_and_county <- cbp_by_zip_and_county %>%
  mutate(emp = as.numeric(emp) * emp_frac)
print(sum(cbp_by_zip_and_county$emp))

# adjust employment count

emp_sim <- cbp_by_zip_and_county %>%
  group_by(naics6, geoid) %>%
  summarise(emp_sim = sum(emp)) 

emp_adj <- cbp_by_county %>% merge(emp_sim, by = c('naics6', 'geoid'), all = TRUE)
emp_adj <- emp_adj %>% 
  mutate(emp_sim = ifelse(is.na(emp_sim), 0, emp_sim)) %>% 
  mutate(emp_adj = emp/emp_sim) %>% 
  mutate(emp_adj = ifelse(emp == 0, 1, emp_adj))

industry_to_replace <- emp_adj %>% filter(emp_adj > 5) %>% select(naics6, geoid)

# replace ZIP data with NA counties using CBP
f4 <- f3 %>% rename(geoid = ST_CNTY)
f4 <- f4 %>% merge(industry_to_replace, all.x = FALSE)

industry_to_replace <- industry_to_replace %>% mutate(to_drop = 1)

cbp_by_zip_and_county <- cbp_by_zip_and_county %>% merge(industry_to_replace, all.x = TRUE) %>%
  mutate(to_drop = ifelse(is.na(to_drop), 0, 1))

cbp_by_zip_and_county <- cbp_by_zip_and_county %>% filter(to_drop != 1)

cbp_by_zip_and_county <- cbp_by_zip_and_county %>% select(naics6, geoid, zip,
                                                          emp, est,
                                                          e1,e2,e3,e4,e5,e6,e7)

f4 <- f4 %>% select(naics6, geoid, 
                    emp, est,
                    e1,e2,e3,e4,e5,e6,e7)
f4 <- f4 %>% mutate(zip = '99999')
cbp_by_zip_and_county = rbind(cbp_by_zip_and_county, f4)

zip_ct_file <- zip_ct_file %>% mutate(matched = 1)
cbp_by_zip_and_county <- cbp_by_zip_and_county %>%
  merge(zip_ct_file, all.x = TRUE) %>%
  mutate(matched = ifelse(is.na(matched), 0, matched))

# assign unmatched zip code to NA (='99999')
cbp_by_zip_and_county <- cbp_by_zip_and_county %>%
  mutate(zip = ifelse(matched == 0, '99999', zip)) %>% rename(ST_CNTY = geoid)


# merge results with synthfirm spatial unit definition
f5 = cbp_by_zip_and_county %>% left_join(faf_all, by = c("ST_CNTY"))
f5 = f5 %>% select(naics6, FAFID, CBPZONE, ST_CNTY, zip,
                   emp,est,
                   e1,e2,e3,e4,e5,e6,e7)
f5 = f5 %>% na.exclude()

# f6 = f5 %>% group_by(naics6, FAFID, CBPZONE, ST_CNTY, zip) %>% summarize(
#   emp = sum(emp),
#   est = sum(est),
#   e1 = sum(e1),
#   e2 = sum(e2),
#   e3 = sum(e3),
#   e4 = sum(e4),
#   e5 = sum(e5),
#   e6 = sum(e6),
#   e7 = sum(e7)
# )

colnames(f5) <- c('Industry_NAICS6_CBP', 'FAFZONE',	'CBPZONE', 'COUNTY', 'ZIPCODE',
                  'employment',	'establishment',	'e1',	'e2',	'e3',	'e4',	'e5',	'e6',	'e7')

output_path <- paste0('inputs_', region_name, '/', 'data_emp_cbp.csv')
data.table::fwrite(f5, output_path)

f7 = f5 %>% group_by(COUNTY, CBPZONE, FAFZONE, ZIPCODE) %>% summarize(
  count=n()
)

output_path_2 <- paste0('inputs_', region_name, '/', 'data_cbp_lookup.csv')
data.table::fwrite(as_tibble(f7), output_path_2)

