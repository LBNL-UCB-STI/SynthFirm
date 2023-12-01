#################################################################################
rm(list = ls())
options(scipen = '10')
list.of.packages <-
  c("dplyr",
    "data.table",
    "sf",
    "mapview",
    "dtplyr")
new.packages <-
  list.of.packages[!(list.of.packages %in% installed.packages()[, "Package"])]
if (length(new.packages))
  install.packages(new.packages)
lapply(list.of.packages, require, character = TRUE)
#################################################################################
#install_github("f1kidd/fmlogit")
set.seed(0)
path2file <-
  "/Users/xiaodanxu/Documents/SynthFirm.nosync"
setwd(path2file)

# load inputs
state_name = 'WA'
region_name = 'Seattle'
file_name = paste0('inputs_', region_name, '/', state_name, '_bg.geojson')
state_df = sf::st_read(file_name)

region_bg = data.table::fread(paste0('inputs_', region_name,"/MESOZONE_GEOID_LOOKUP.csv"), 
                              colClasses = list(character=c("GEOID")))

faf1 = sf::st_read("RawData/FAF5Zones.geojson")
lookup_file = paste0('inputs_', region_name, '/', region_name, '_FAFCNTY.csv')
faf_lookup = data.table::fread(lookup_file,h=T)

region_bg = state_df %>% left_join(region_bg, by=c("GEOID"))
region_bg = region_bg %>% select(GEOID, CBPZONE1,MESOZONE) %>% na.exclude()

#ca_bg1 = ca_bg %>% group_by(GEOID,CBPZONE1,MESOZONE) %>% summarize()
# out_file_name = paste0(region_name, "_cbg.geojson")
# sf::st_write(region_bg, out_file_name)


f1_in_study_area <- faf_lookup %>% filter(CBPZONE>=1000) %>% as_tibble()
study_area_faf <- unique(f1_in_study_area$FAFID)

faf_lookup <- faf_lookup %>% filter(! FAFID %in% study_area_faf) %>% as_tibble()

faf_lookup <- faf_lookup %>% select(FAFID, CBPZONE1)
faf_lookup_unique <- faf_lookup %>% distinct()
faf_lookup_unique <- faf_lookup_unique %>% mutate(MESOZONE = 20000 + CBPZONE1)
faf_lookup_unique = faf_lookup_unique %>% rename(FAF = FAFID)
faf1 = faf1 %>% mutate(FAF = as.integer(FAF))

faf2 <- faf1 %>% filter(! FAF %in% study_area_faf)
faf2 <- faf2 %>% left_join(faf_lookup_unique, by=c("FAF"))

faf2 = faf2 %>% rename(GEOID = FAF)

# generate mesozone shapefile
region_bg = region_bg %>% rename(CBPZONE = CBPZONE1)
faf2 = faf2 %>% rename(CBPZONE = CBPZONE1)
faf2 <- faf2 %>% mutate(GEOID = as.character(GEOID))
bg_df = bind_rows(region_bg,faf2)


box = c(xmin = -179.23109, ymin = 10, xmax = -66.96466, ymax = 71.365162) # U.S. boundary for plot, no remote island
bg_df <- st_crop(bg_df, box)
plot(st_geometry(bg_df))
region_map_out <- paste0('inputs_', region_name, '/', region_name, '_freight.geojson')
sf::st_write(bg_df, region_map_out)

# generate centroid file


freight_centroid <- st_centroid(bg_df)
plot(st_geometry(freight_centroid), col = 'red', add = TRUE)
centroid_map_out <- paste0('inputs_', region_name, '/', region_name, '_freight_centroids.geojson')
sf::st_write(freight_centroid, centroid_map_out)
