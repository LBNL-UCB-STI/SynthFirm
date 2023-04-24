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

# data_to_check <- st_read('sfbay_freight.geojson')
region_name = 'SFBay'
state_name = 'CA'
cfs_df = sf::st_read(dsn = "CFS_Areas.geojson")
cfs_lookup = fread("CFS_FAF_LOOKUP.csv",
                   colClasses = list(character = c("STFIPS", "CFSMA", "FAF")),
                   h = T)

cfs_df = cfs_df %>% mutate(ST_MA = paste0(ANSI_ST, "-", CFS12_AREA))

cfs_faf = cfs_df %>% left_join(cfs_lookup, by = c("ST_MA"))
cfs_faf = cfs_faf %>% select(ANSI_ST, ANSI_CNTY, ST_MA, FAF, CFSAREANAM)
faf_zones = cfs_faf %>% group_by(FAF) %>% summarize()

sf::st_write(cfs_faf, "cfs_faf_lookup.geojson")
sf::st_write(faf_zones, "FAF4Zones.geojson")

#require(ggplot2)
#ggplot() + geom_sf(data=faf_zones, aes(fill = factor(FAF)))


faf_cnty = cfs_faf %>% group_by(FAF, ANSI_ST, ANSI_CNTY) %>% summarize(count =
                                                                         n())

faf_cnty = faf_cnty %>% mutate(FAFID = as.numeric(FAF))

faf_df = as.data.frame(faf_cnty) %>% select(FAFID, ANSI_ST, ANSI_CNTY)

study_area_faf <- c(64, 62, 65, 69)  # need to make this token a global input
faf_df1 = faf_df %>% filter(FAFID %in% study_area_faf) # select FAF zones within study area
faf_df2 = faf_df %>% anti_join(faf_df1, by = c("FAFID")) %>% arrange(FAFID) # select FAF zones outside study area

faf_df1 = faf_df1 %>% mutate(CBPZONE = paste0(ANSI_ST, ANSI_CNTY)) # 5-digit fips code

faf_df3 = faf_df2 %>% group_by(FAFID) %>% summarize(count = n()) %>% mutate(CBPZONE = seq(1, length(FAFID)))

faf_df2 = faf_df2 %>% left_join(faf_df3, by = c("FAFID"))

faf_external = faf_df2 %>% select(FAFID, ANSI_ST, ANSI_CNTY, CBPZONE) %>% mutate(CBPZONE = as.character(CBPZONE))
faf_internal = faf_df1
faf_all = bind_rows(faf_internal, faf_external) %>% mutate(CBPZONE1 = as.numeric(CBPZONE))
faf_all = faf_all %>% mutate(ST_CNTY = paste0(ANSI_ST, ANSI_CNTY))

file_name = paste0(region_name, '_FAFCNTY.csv')
data.table::fwrite(faf_all, file_name) # use token to replace 'SFBay'

# load CBP cpmplete county file from Census.gov, example: https://www.census.gov/data/datasets/2017/econ/cbp/2017-cbp.html
f1 = data.table::fread("cbp16co.txt",
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
data.table::fwrite(f6, "data_emp_cbp.csv")

f7 = f5 %>% group_by(ST_CNTY, CBPZONE1, FAFID) %>% summarize(
  count=n()
)


data.table::fwrite(as_tibble(f7), "data_cbp_lookup.csv")


# archived scripts for CBP_EMPRANKINGS.R, use the other file instead

# naics_df = data.table::fread("ca_naics.csv",colClasses = list(character=c("GEOID","metalayer_id")),h=T)
# naics_df1 = naics_df %>% group_by(metalayer_id) %>% 
#             summarize(n11=sum(n11),n21=sum(n21),n22=sum(n22),n23=sum(n23),n3133=sum(n3133),n42=sum(n42),
#                       n4445=sum(n4445),n4849=sum(n4849),n51=sum(n51),n52=sum(n52),n53=sum(n53),n54=sum(n54),
#                       n55=sum(n55),n56=sum(n56),n61=sum(n61),n62=sum(n62),n71=sum(n71),n72=sum(n72),n81=sum(n81),
#                       n92=sum(n92))
# 
# 
# 
# naics_long = reshape2::melt(naics_df1, id.vars=c("metalayer_id"))
# 
# naics_df2 = naics_long %>% group_by(metalayer_id) %>% summarize(totemp = sum(value))
# 
# naics_df3 = naics_long %>% left_join(naics_df2, by=c("metalayer_id")) %>% 
#             mutate(pct = 100*value/totemp) %>% arrange(metalayer_id,desc(pct))
# 
# naics_df3 = naics_df3 %>% group_by(metalayer_id) %>% mutate(percrank = floor(10*rank(pct)/length(pct)))
# 
# naics_df4 = reshape2::dcast(naics_df3, metalayer_id ~ variable, value.var = "percrank")
# naics_df4[naics_df4==0] <- NA
# 
# sfbay_df = sf::st_read("sf_metalayer.geojson")
# 
# sfbay_df = sfbay_df %>% rename(metalayer_id = tract_id)
# sfbay_counties = unique(sfbay_df$cnty_id)
# 
# naics_df4 = naics_df4 %>% mutate(cnty_id = substr(metalayer_id,1,5))
# naics_df5 = naics_df4 %>% filter(cnty_id %in% sfbay_counties)
# naics_df5$n99 = floor(runif(nrow(naics_df5),1,10))
# naics_df5 = naics_df5 %>% rename(MESOZONE = metalayer_id,
#                                  COUNTY = cnty_id)
# 
# naics = c(
#   "n11",
#   "n21",
#   "n23",
#   "n3133",
#   "n42",
#   "n4445",
#   "n4849",
#   "n22",
#   "n51",
#   "n52",
#   "n53",
#   "n54",
#   "n55",
#   "n56",
#   "n61",
#   "n62",
#   "n71",
#   "n72",
#   "n81",
#   "n92",
#   "n99"
# )
# 
# rank_vars = c(
#   "rank11",
#   "rank21",
#   "rank23",
#   "rank3133",
#   "rank42",
#   "rank4445",
#   "rank4849",
#   "rank22",
#   "rank51",
#   "rank52",
#   "rank53",
#   "rank54",
#   "rank55",
#   "rank56",
#   "rank61",
#   "rank62",
#   "rank71",
#   "rank72",
#   "rank81",
#   "rank92",
#   "rank99"
# )
# 
# naics_df6 = naics_df5 %>% select(COUNTY, MESOZONE, naics)
# names(naics_df6)[3:23] = rank_vars
# naics_df6 = naics_df6 %>% arrange(COUNTY, MESOZONE)
# naics_df6$MAZ = seq(1,nrow(naics_df6),1)
# naics_df7 = naics_df6 %>% select(COUNTY, MAZ, rank_vars) %>% rename(MESOZONE = MAZ)
# 
# data.table::fwrite(naics_df7, "data_mesozone_emprankings.csv")
