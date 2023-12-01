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
  "/Users/xiaodanxu/Documents/SynthFirm.nosync"
setwd(path2file)

# define inputs
region_name = 'Seattle'
state_name = 'WA'
study_area_faf <- c(531, 532, 539, 411) # this is a critical step in selecting study area boundary
regional_analysis = 1 # 0 - national analysis, 1 - regional analysis
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
sf::st_write(faf_zones, "RawData/FAF5Zones.geojson")

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


file_name = paste0('inputs_', region_name, '/', region_name, '_FAFCNTY.csv')
data.table::fwrite(faf_all, file_name) 

# load CBP cpmplete county file from Census.gov, example: https://www.census.gov/data/datasets/2017/econ/cbp/2017-cbp.html
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

f4 = f3 %>% left_join(faf_all, by = c("ST_CNTY"))
f5 = f4 %>% select(ST_CNTY,
                   FAFID,
                   CBPZONE1,
                   naics6,
                   emp,
                   est,
                   e1,
                   e2,
                   e3,
                   e4,
                   e5,
                   e6,
                   e7)
f5 = f5 %>% na.exclude()

f6 = f5 %>% group_by(naics6, FAFID, CBPZONE1) %>% summarize(
  emp = sum(emp),
  est = sum(est),
  e1 = sum(e1),
  e2 = sum(e2),
  e3 = sum(e3),
  e4 = sum(e4),
  e5 = sum(e5),
  e6 = sum(e6),
  e7 = sum(e7)
)

colnames(f6) <- c('Industry_NAICS6_CBP', 'FAFZONE',	'CBPZONE',
                  'employment',	'establishment',	'e1',	'e2',	'e3',	'e4',	'e5',	'e6',	'e7')

output_path <- paste0('inputs_', region_name, '/', 'data_emp_cbp.csv')
data.table::fwrite(f6, output_path)

f7 = f5 %>% group_by(ST_CNTY, CBPZONE1, FAFID) %>% summarize(
  count=n()
)

output_path_2 <- paste0('inputs_', region_name, '/', 'data_cbp_lookup.csv')
data.table::fwrite(as_tibble(f7), output_path_2)

