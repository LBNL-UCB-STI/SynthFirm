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
  "/Users/srinath/OneDrive - LBNL/Projects/SMART-2.0/Task-3 BAMOS/BayArea_GIS"
setwd(path2file)

ca_df = sf::st_read("ca_bg.geojson")

sf_bg = data.table::fread("MESOZONE_GEOID_LOOKUP.csv", colClasses = list(character=c("GEOID")))

ca_bg = ca_df %>% left_join(sf_bg, by=c("GEOID"))
ca_bg = ca_bg %>% select(GEOID, CBPZONE1,MESOZONE) %>% na.exclude()

ca_bg1 = ca_bg %>% group_by(GEOID,CBPZONE1,MESOZONE) %>% summarize()
sf::st_write(ca_bg1, "sfbay1_cbg.geojson")


faf1 = sf::st_read("FAF4Zones.geojson")
firms = data.table::fread("FIRMS_LOOKUP.csv",h=T)
firms = firms %>% mutate(FAF = ifelse(nchar(FAFZONE)==2,paste0("0",FAFZONE),FAFZONE)) %>% as_tibble()
firms = firms %>% filter(MESOZONE > 10617) %>% select(FAF,CBPZONE,MESOZONE)

faf2 = faf1 %>% left_join(firms,by=c("FAF")) %>% na.exclude()

faf2 = faf2 %>% rename(GEOID = FAF)

ca_bg1 = ca_bg1 %>% rename(CBPZONE = CBPZONE1)

bg_df = bind_rows(ca_bg1,faf2)

sf::st_write(bg_df, "sfbay_freight.geojson")

