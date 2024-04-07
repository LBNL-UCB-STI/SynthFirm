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
#################################################################################
#install_github("f1kidd/fmlogit")
path2file <-
  "/Users/xiaodanxu/Documents/SynthFirm.nosync"
setwd(path2file)

# define inputs
selected_state = c('WA', 'OR')
output_state = 'WA'
selected_year = 2017
region_name = 'Seattle'


####### BEGINNING OF CENSUS DATA PROCESSES ######
state_wac <- grab_lodes(selected_state, 
                    selected_year,
                  version = 'LODES7',
                  lodes_type = "wac",
                  job_type = "JT00", #all jobs combined
                  segment = "S000", # select total jobs
                  agg_geo = "bg")
head(state_wac)

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
  mutate(metalayer_id = substr(GEOID, 1, 8),
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
  select(GEOID, metalayer_id, all_of(naics)) # note: metalayer id is not used in following analysis


state_bg_df = get_acs(
  geography = "block group",
  year = selected_year,
  variables = c('B01003_001'),
  state = selected_state,
  geometry = TRUE
)

state_bg_df_filtered <- state_bg_df %>% filter(! grepl('Block Group 0', NAME)) # with population
list_of_geoid <- unique(state_bg_df_filtered$GEOID)
bg_name = paste0('inputs_', region_name, '/', output_state, '_bg.geojson')
sf::st_write(state_bg_df_filtered, bg_name)

state_wac_filtered <- state_wac %>% filter(GEOID %in% list_of_geoid)
output_name = paste0('inputs_', region_name, '/', output_state, '_naics.csv')
data.table::fwrite(state_wac_filtered, output_name)



##########################
#### previous version ####
##########################

# census_api_key("d49f1c9b81751571b083252dfbb8ac14ae8b63b7", install = TRUE, overwrite=TRUE) # using this at the first time of running SynthFirm
# readRenviron("~/.Renviron")
# male_naics = c(
#   "C24030_004",
#   "C24030_005",
#   "C24030_006",
#   "C24030_007",
#   "C24030_008",
#   "C24030_009",
#   "C24030_011",
#   "C24030_012",
#   "C24030_013",
#   "C24030_015",
#   "C24030_016",
#   "C24030_018",
#   "C24030_019",
#   "C24030_020",
#   "C24030_022",
#   "C24030_023",
#   "C24030_025",
#   "C24030_026",
#   "C24030_027",
#   "C24030_028"
# )
# 
# state_df1 <-
#   get_acs(
#     geography = "block group",
#     year = selected_year, # ACS 5-year estimate, using later years such as 2018/2019 will cause technical issues
#     variables = male_naics,
#     state = selected_state
#   )
# 
# fem_naics = c(
#   "C24030_031",
#   "C24030_032",
#   "C24030_033",
#   "C24030_034",
#   "C24030_035",
#   "C24030_036",
#   "C24030_038",
#   "C24030_039",
#   "C24030_040",
#   "C24030_042",
#   "C24030_043",
#   "C24030_045",
#   "C24030_046",
#   "C24030_047",
#   "C24030_049",
#   "C24030_050",
#   "C24030_052",
#   "C24030_053",
#   "C24030_054",
#   "C24030_055"
# )
# 
# state_df2 <-
#   get_acs(
#     geography = "block group",
#     year = selected_year,
#     variables = fem_naics,
#     state = selected_state
#   )
# 
# 
# state_acs1 <-  state_df1 %>% 
#   select(GEOID, NAME, variable, estimate)  %>% 
#   pivot_wider(names_from = variable, values_from = estimate)
# 
# state_acs2 <-  state_df2 %>% 
#   select(GEOID, NAME, variable, estimate)  %>% 
#   pivot_wider(names_from = variable, values_from = estimate)
# 
# # state_acs1 = state_df1 %>% dcast(GEOID + NAME ~ variable, value.var = "estimate") # dcast function has deprecated
# # state_acs2 = state_df2 %>% dcast(GEOID + NAME ~ variable, value.var = "estimate")
# 
# naics_m = c(
#   "n11_m",
#   "n21_m",
#   "n23_m",
#   "n3133_m",
#   "n42_m",
#   "n4445_m",
#   "n4849_m",
#   "n22_m",
#   "n51_m",
#   "n52_m",
#   "n53_m",
#   "n54_m",
#   "n55_m",
#   "n56_m",
#   "n61_m",
#   "n62_m",
#   "n71_m",
#   "n72_m",
#   "n81_m",
#   "n92_m"
# )
# naics_f = c(
#   "n11_f",
#   "n21_f",
#   "n23_f",
#   "n3133_f",
#   "n42_f",
#   "n4445_f",
#   "n4849_f",
#   "n22_f",
#   "n51_f",
#   "n52_f",
#   "n53_f",
#   "n54_f",
#   "n55_f",
#   "n56_f",
#   "n61_f",
#   "n62_f",
#   "n71_f",
#   "n72_f",
#   "n81_f",
#   "n92_f"
# )
# 
# 
# names(state_acs1) = c("GEOID", "NAME", naics_m)
# names(state_acs2) = c("GEOID", "NAME", naics_f)
# 
# state_acs = state_acs1 %>% left_join(state_acs2, by = c("GEOID", "NAME")) %>% mutate(
#   n11 = n11_m + n11_f,
#   n21 = n21_m + n21_f,
#   n22 = n22_m + n22_f,
#   n23 = n23_m + n23_f,
#   n3133 = n3133_m + n3133_f,
#   n42 = n42_m + n42_f,
#   n4445 = n4445_m + n4445_f,
#   n4849 = n4849_m + n4849_f,
#   n51 = n51_m + n51_f,
#   n52 = n52_m + n52_f,
#   n53 = n53_m + n53_f,
#   n54 = n54_m + n54_f,
#   n55 = n55_m + n55_f,
#   n56 = n56_m + n56_f,
#   n61 = n61_m + n61_f,
#   n62 = n62_m + n62_f,
#   n71 = n71_m + n71_f,
#   n72 = n72_m + n72_f,
#   n81 = n81_m + n81_f,
#   n92 = n92_m + n92_f
# )
# 
# state_acs = state_acs %>% select(GEOID, NAME, all_of(naics)) %>% mutate(metalayer_id = substr(GEOID, 1, 8)) %>%
#   select(GEOID, metalayer_id, all_of(naics)) # note: metalayer id is not used in following analysis
# 
# 
# v19 <- load_variables(2019, "acs5", cache = TRUE)
# View(v19)



