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
  "/Users/srinath/OneDrive - LBNL/Projects/SMART-2.0/Task-3 BAMOS/BayArea_GIS"
setwd(path2file)

naics = c(
  "n11",
  "n21",
  "n23",
  "n3133",
  "n42",
  "n4445",
  "n4849",
  "n22",
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
  "n92",
  "n99"
)

rank_vars = c(
  "rank11",
  "rank21",
  "rank23",
  "rank3133",
  "rank42",
  "rank4445",
  "rank4849",
  "rank22",
  "rank51",
  "rank52",
  "rank53",
  "rank54",
  "rank55",
  "rank56",
  "rank61",
  "rank62",
  "rank71",
  "rank72",
  "rank81",
  "rank92",
  "rank99"
)

f1 = data.table::fread("SFBay_FAFCNTY.csv",colClasses = list(character=c("ANSI_ST","ANSI_CNTY","ST_CNTY")),
                       h=T)

f2 = data.table::fread("ca_naics.csv",colClasses = list(character=c("GEOID","metalayer_id")),h=T)
f2 = f2 %>% select(-c(metalayer_id))

naics_long = reshape2::melt(f2, id.vars=c("GEOID"))
naics_df2 = naics_long %>% group_by(GEOID) %>% summarize(totemp = sum(value))

naics_df3 = naics_long %>% left_join(naics_df2, by=c("GEOID")) %>% 
  mutate(pct = 100*value/totemp) %>% arrange(GEOID,desc(pct))

naics_df3 = naics_df3 %>% group_by(GEOID) %>% mutate(percrank = floor(10*rank(pct)/length(pct)))
naics_df4 = reshape2::dcast(naics_df3, GEOID ~ variable, value.var = "percrank")
naics_df4 = naics_df4 %>% mutate(cnty_id = substr(GEOID, 1, 5))

f3 = f1 %>% filter(FAFID==62 | FAFID==64 | FAFID==65 | FAFID==69) %>% select(ST_CNTY,CBPZONE1)

naics_df5 = naics_df4 %>% left_join(f3, by=c("cnty_id"="ST_CNTY"))
naics_df6 = na.omit(naics_df5)
naics_df6$MESOZONE = seq(1,length(unique(naics_df6$GEOID)))
naics_df6$n99 = floor(runif(nrow(naics_df6),0,10))
naics_df6[naics_df6==0] <- NA
naics_df7 = naics_df6 %>% select(CBPZONE1,MESOZONE,all_of(naics))
names(naics_df7)[3:23] = rank_vars
naics_df7 = naics_df7 %>% rename(COUNTY = CBPZONE1)

data.table::fwrite(naics_df7, "data_mesozone_emprankings.csv")
data.table::fwrite(naics_df6[c("CBPZONE1","GEOID","MESOZONE")],"MESOZONE_GEOID_LOOKUP.csv")




