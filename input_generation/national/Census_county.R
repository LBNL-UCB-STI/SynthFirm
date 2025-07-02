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
require(tidycensus)
require(reshape2) # this package is deprecated
require(tidyverse)
library(lehdr) # to download LEHD data from FTP
library(stringr)
#################################################################################
#install_github("f1kidd/fmlogit")
path2file <-
  "/Users/xiaodanxu/Documents/SynthFirm.nosync"
setwd(path2file)

# define inputs

#output_state = c('CA')
output_state = 'US'
selected_year = 2022
region_name = 'national'

CBG20_TO_COUNTY10 <- data.table::fread('RawData/nhgis_bg2020_co2010/nhgis_bg2020_co2010.csv', h = T)
# REF: https://www.nhgis.org/geographic-crosswalks#download-2020-2010
# formatting
CBG20_TO_COUNTY10 <- CBG20_TO_COUNTY10 %>% 
  select(bg2020ge, co2010ge, parea) %>%
  mutate(bg2020ge = str_pad(bg2020ge, width = 12, pad = "0"),
         co2010ge = str_pad(co2010ge, width = 5, pad = "0"))

CBG20_TO_COUNTY10_nodup <- CBG20_TO_COUNTY10 %>%
  group_by(bg2020ge) %>%
  slice_max(parea, n = 1, with_ties = FALSE)

CBG20_TO_COUNTY10_nodup <- CBG20_TO_COUNTY10_nodup %>% 
  rename(GEOID = bg2020ge, CBPZONE = co2010ge)

if (output_state =='US'){
  selected_state = unique(fips_codes$state)[1:56]
  # Workplace area characteristics (latest data from AK is 2016)
  selected_state <- selected_state[!selected_state %in%  c("AS", "GU", "MP", "PR", "UM", "AK", "MS")]
  if (selected_year > 2021){
  selected_state <- selected_state[!selected_state %in%  c("MI")]  
  }
}else{
  # user needs to define the state here
  selected_state = output_state
}

####### BEGINNING OF CENSUS DATA PROCESSES ######
state_wac <- grab_lodes(selected_state, 
                    selected_year,
                  version = 'LODES8',
                  lodes_type = "wac",
                  job_type = "JT00", #all jobs combined
                  segment = "S000", # select total jobs
                  agg_geo = "bg")
head(state_wac)

# Use older data for AK and MS
if (output_state =='US'){
  state_wac_ak <- grab_lodes('AK', 
                          2016,
                          version = 'LODES8',
                          lodes_type = "wac",
                          job_type = "JT00", #all jobs combined
                          segment = "S000", # select total jobs
                          agg_geo = "bg")
  
  state_wac_ms <- grab_lodes('MS', 
                             2018,
                             version = 'LODES8',
                             lodes_type = "wac",
                             job_type = "JT00", #all jobs combined
                             segment = "S000", # select total jobs
                             agg_geo = "bg")
  state_wac = rbind(state_wac, state_wac_ak, state_wac_ms)
  if (selected_year >2021){
    state_wac_mi <- grab_lodes('MI', 
                               2021,
                               version = 'LODES8',
                               lodes_type = "wac",
                               job_type = "JT00", #all jobs combined
                               segment = "S000", # select total jobs
                               agg_geo = "bg")
    state_wac = rbind(state_wac, state_wac_mi)
  }
}


naics = c(
  "n11",
  "n21",
  "n22",
  "n23",
  "n3133",
  "n42",
  "n4445",
  "n4849",
  "n51",
  "n52",
  "n53",
  "n54",
  "n55",
  "n56",
  "n61",
  "n62",
  "n71",
  "n72",
  "n81",
  "n92"
)

state_wac <- state_wac %>% 
  rename(GEOID = w_bg) %>% 
  mutate(
         n11 = CNS01, 
         n21 = CNS02, 
         n22 = CNS03,
         n23 = CNS04, 
         n3133 = CNS05,
         n42 = CNS06,
         n4445 = CNS07,
         n4849 = CNS08,
         n51 = CNS09,
         n52 = CNS10,
         n53 = CNS11,
         n54 = CNS12,
         n55 = CNS13,
         n56 = CNS14,
         n61 = CNS15,
         n62 = CNS16,
         n71 = CNS17,
         n72 = CNS18,
         n81 = CNS19,
         n92 = CNS20
         )

state_wac = state_wac %>% 
  select(GEOID, all_of(naics)) # note: metalayer id is not used in following analysis

state_wac_to_ct <- merge(state_wac, CBG20_TO_COUNTY10_nodup, 
                         by = "GEOID", all.x = TRUE, all.y = FALSE)

state_wac_to_ct <- state_wac_to_ct %>%
  group_by(CBPZONE) %>%
  summarize(across(all_of(naics), sum, .names = "{.col}"))


output_name = paste0('inputs_', region_name, '/', region_name, '_', selected_year, '_naics_county.csv')
data.table::fwrite(state_wac_to_ct, output_name)






