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
  "/Users/xiaodanxu/Documents/SynthFirm/BayArea_GIS"
setwd(path2file)

ca_df = sf::st_read("CA_bg.geojson")

sf_bg = data.table::fread("MESOZONE_GEOID_LOOKUP.csv", colClasses = list(character=c("GEOID")))

ca_bg = ca_df %>% left_join(sf_bg, by=c("GEOID"))
ca_bg = ca_bg %>% select(GEOID, CBPZONE1,MESOZONE) %>% na.exclude()

#ca_bg1 = ca_bg %>% group_by(GEOID,CBPZONE1,MESOZONE) %>% summarize()
sf::st_write(ca_bg1, "sfbay1_cbg.geojson")


faf1 = sf::st_read("FAF4Zones.geojson")
faf_lookup = data.table::fread("SFBay_FAFCNTY.csv",h=T)
study_area_faf <- c(62, 64, 65, 69)  # need to make this token a global input
faf_lookup <- faf_lookup %>% filter(! FAFID %in% study_area_faf) %>% as_tibble()

faf_lookup <- faf_lookup %>% select(FAFID, CBPZONE1)
faf_lookup_unique <- faf_lookup %>% distinct()
faf_lookup_unique <- faf_lookup_unique %>% mutate(MESOZONE = 20000 + CBPZONE1)
faf_lookup_unique = faf_lookup_unique %>% rename(FAF = FAFID)
faf1 = faf1 %>% mutate(FAF = as.integer(FAF))
# firms = firms %>% mutate(FAF = ifelse(nchar(FAFZONE)==2,paste0("0",FAFZONE),FAFZONE)) %>% as_tibble()
# firms = firms %>% filter(MESOZONE > 10617) %>% select(FAF,CBPZONE,MESOZONE)
faf2 <- faf1 %>% filter(! FAF %in% study_area_faf)
faf2 <- faf2 %>% left_join(faf_lookup_unique, by=c("FAF"))

faf2 = faf2 %>% rename(GEOID = FAF)

ca_bg = ca_bg %>% rename(CBPZONE = CBPZONE1)
faf2 <- faf2 %>% mutate(GEOID = as.character(GEOID))
bg_df = bind_rows(ca_bg,faf2)
plot(st_geometry(bg_df))
sf::st_write(bg_df, "sfbay_freight.geojson")

