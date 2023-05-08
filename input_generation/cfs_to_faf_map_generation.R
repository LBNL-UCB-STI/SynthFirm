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
    "stringr")
new.packages <-
  list.of.packages[!(list.of.packages %in% installed.packages()[, "Package"])]
if (length(new.packages))
  install.packages(new.packages)
lapply(list.of.packages, require, character = TRUE)

path2file <-
  "/Users/xiaodanxu/Documents/SynthFirm.nosync"
setwd(path2file)

cfs2012_template <- st_read('RawData/CFS_Areas.geojson') # load CFS2012 results as a template

# cfs county file
cfs_county <- st_read('RawData/CFS Shapefiles/tl_2017_us_county.shp') 
plot(st_geometry(cfs_county)) # long run time, do it if absolutely necessary
# downloaded from https://www.census.gov/programs-surveys/cfs/technical-documentation/geographies.html

# cfs county to metro area
cfs_county_to_area <- fread('RawData/CFS_area_2017.csv') # csv version of list2017.xlsx
cfs_county_to_area <- cfs_county_to_area %>%
  mutate(ST = str_pad(ST, 2, pad = "0"), CNTY = str_pad(CNTY, 3, pad = "0"))
# downloaded from https://www.census.gov/programs-surveys/cfs/technical-documentation/geographies.html

# cfs metro area to faf zone
cfs_area_to_faf <- fread('RawData/CFS_FAF_LOOKUP.csv') # csv version of CFS area code - FAF5 zone id.xlsx
cfs_area_to_faf <- cfs_area_to_faf %>%
  mutate(STFIPS = str_pad(STFIPS, 2, pad = "0"))
# downloaded from https://faf.ornl.gov/faf5/

cfs_county_with_area <- merge(cfs_county, cfs_county_to_area, 
                              by.x = c('STATEFP', 'COUNTYFP'), 
                              by.y = c('ST', 'CNTY'), all.x = TRUE)

cfs_county_with_area <- merge(cfs_county_with_area, cfs_area_to_faf, 
                              by.x = c('STATEFP', 'CFS17_AREA'), 
                              by.y = c('STFIPS', 'CFSMA'), all.x = TRUE)

cfs_county_with_area <- cfs_county_with_area %>%
  select(STATEFP, COUNTYFP, GEOID, CNTY_NAME, CFS07_AREA, CFS12_AREA, CFS17_AREA,
         CFS07_TYPE, CFS12_TYPE, CFS17_TYPE, CFS07_GEOID, CFS12_GEOID, CFS17_GEOID,
         CFS07_NAME, CFS12_NAME, CFS17_NAME, FAF, geometry)

colnames(cfs_county_with_area) <- c("ANSI_ST", "ANSI_CNTY", "ANSI_ST_CO", "CNTY_NAME",  "CFS07_AREA", "CFS12_AREA", "CFS17_AREA",
                                    "CFS07_TYPE", "CFS12_TYPE", "CFS17_TYPE", "CFS07_GEOID", "CFS12_GEOID", "CFS17_GEOID", 
                                    "CFS07_NAME", "CFS12_NAME", "CFS17_NAME", "FAFZONE", "geometry")

st_write(cfs_county_with_area, 'RawData/CFS2017_areas.geojson')
