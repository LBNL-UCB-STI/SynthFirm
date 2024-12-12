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

selected_state = 'TX'
selected_region = 'Austin'

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
  "n92"
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
  "rank92"
)
selected_file = paste0('inputs_', selected_region, '/', selected_region, '_FAFCNTY.csv')
f1 = data.table::fread(selected_file,colClasses = list(character=c("ANSI_ST","ANSI_CNTY","ST_CNTY")), h=T)

f1_in_study_area <- f1 %>% filter(CBPZONE>=1000)
study_area_faf <- unique(f1_in_study_area$FAFID) # VERY IMPORTANT: check if the region generated here is accurate

f3 = f1 %>% filter(FAFID %in% study_area_faf) %>% select(ST_CNTY,CBPZONE1)

naics_file_name = paste0('inputs_', selected_region, '/', selected_state, '_naics.csv') # employment data
f2 = data.table::fread(naics_file_name,colClasses = list(character=c("GEOID","metalayer_id")),h=T)
f2 = f2 %>% select(-c(metalayer_id))

naics_long = reshape2::melt(f2, id.vars=c("GEOID"))
f2_rank = f2

# naics_df4 = f2_rank
f2_rank = f2_rank %>% mutate(cnty_id = substr(GEOID, 1, 5))
f2_rank = f2_rank %>% left_join(f3, by=c("cnty_id"="ST_CNTY")) #employment within study area
f2_rank = na.omit(f2_rank)
f2_rank$MESOZONE = seq(1,length(unique(f2_rank$GEOID)))

naics_df6 = f2_rank
#df_length = as.integer(nrow(f2_rank))
f2_rank[f2_rank == 0] <- NA
f2_rank <- f2_rank %>%
  group_by(cnty_id) %>% 
  mutate(n11 = rank(n11, ties.method = "first",na='keep'),
         n21 = rank(n21, ties.method = "first",na='keep'),
         n22 = rank(n22, ties.method = "first",na='keep'),
         n23 = rank(n23, ties.method = "first",na='keep'),
         n3133 = rank(n3133, ties.method = "first",na='keep'),
         n42 = rank(n42, ties.method = "first",na='keep'),
         n4445 = rank(n4445, ties.method = "first",na='keep'),
         n4849 = rank(n4849, ties.method = "first",na='keep'),
         n51 = rank(n51, ties.method = "first",na='keep'),
         n52 = rank(n52, ties.method = "first",na='keep'),
         n53 = rank(n53, ties.method = "first",na='keep'),
         n54 = rank(n54, ties.method = "first",na='keep'),
         n55 = rank(n55, ties.method = "first",na='keep'),
         n56 = rank(n56, ties.method = "first",na='keep'),
         n61 = rank(n61, ties.method = "first",na='keep'),
         n62 = rank(n62, ties.method = "first",na='keep'),
         n71 = rank(n71, ties.method = "first",na='keep'),
         n72 = rank(n72, ties.method = "first",na='keep'),
         n81 = rank(n81, ties.method = "first",na='keep'),
         n92 = rank(n92, ties.method = "first",na='keep'))

# rank from small to large, with rank = 1 means smallest value within group

# f2_rank <- f2_rank %>%
#   group_by(cnty_id) %>% 
#   mutate(n11 = cut(n11, quantile(n11, probs=0:8/8, na.rm=TRUE), include.lowest=TRUE, labels=FALSE, na.rm=TRUE),
#          n21 = cut(n21, quantile(n21, probs=0:8/8, na.rm=TRUE), include.lowest=TRUE, labels=FALSE, na.rm=TRUE),
#          n22 = cut(n22, quantile(n22, probs=0:8/8, na.rm=TRUE), include.lowest=TRUE, labels=FALSE, na.rm=TRUE),
#          n23 = cut(n23, quantile(n23, probs=0:8/8, na.rm=TRUE), include.lowest=TRUE, labels=FALSE, na.rm=TRUE),
#          n3133 = cut(n3133, quantile(n3133, probs=0:8/8, na.rm=TRUE), include.lowest=TRUE, labels=FALSE, na.rm=TRUE),
#          n42 = cut(n42, quantile(n42, probs=0:8/8, na.rm=TRUE), include.lowest=TRUE, labels=FALSE, na.rm=TRUE),
#          n4445 = cut(n4445, quantile(n4445, probs=0:8/8, na.rm=TRUE), include.lowest=TRUE, labels=FALSE, na.rm=TRUE),
#          n4849 = cut(n4849, quantile(n4849, probs=0:8/8, na.rm=TRUE), include.lowest=TRUE, labels=FALSE, na.rm=TRUE),
#          n51 = cut(n51, quantile(n51, probs=0:8/8, na.rm=TRUE), include.lowest=TRUE, labels=FALSE, na.rm=TRUE),
#          n52 = cut(n52, quantile(n52, probs=0:8/8, na.rm=TRUE), include.lowest=TRUE, labels=FALSE, na.rm=TRUE),
#          n53 = cut(n53, quantile(n53, probs=0:8/8, na.rm=TRUE), include.lowest=TRUE, labels=FALSE, na.rm=TRUE),
#          n54 = cut(n54, quantile(n54, probs=0:8/8, na.rm=TRUE), include.lowest=TRUE, labels=FALSE, na.rm=TRUE),
#          n55 = cut(n55, quantile(n55, probs=0:8/8, na.rm=TRUE), include.lowest=TRUE, labels=FALSE, na.rm=TRUE),
#          n56 = cut(n56, quantile(n56, probs=0:8/8, na.rm=TRUE), include.lowest=TRUE, labels=FALSE, na.rm=TRUE),
#          n61 = cut(n61, quantile(n61, probs=0:8/8, na.rm=TRUE), include.lowest=TRUE, labels=FALSE, na.rm=TRUE),
#          n62 = cut(n62, quantile(n62, probs=0:8/8, na.rm=TRUE), include.lowest=TRUE, labels=FALSE, na.rm=TRUE),
#          n71 = cut(n71, quantile(n71, probs=0:8/8, na.rm=TRUE), include.lowest=TRUE, labels=FALSE, na.rm=TRUE),
#          n72 = cut(n72, quantile(n72, probs=0:8/8, na.rm=TRUE), include.lowest=TRUE, labels=FALSE, na.rm=TRUE),
#          n81 = cut(n81, quantile(n81, probs=0:8/8, na.rm=TRUE), include.lowest=TRUE, labels=FALSE, na.rm=TRUE),
#          n92 = cut(n92, quantile(n92, probs=0:8/8, na.rm=TRUE), include.lowest=TRUE, labels=FALSE, na.rm=TRUE)
#         )  

  
# naics_df2 = naics_long %>% group_by(GEOID) %>% summarize(totemp = sum(value))
# 
# naics_df3 = naics_long %>% left_join(naics_df2, by=c("GEOID")) %>% 
#   mutate(pct = 100*value/totemp) %>% arrange(GEOID,desc(pct))
# 
# naics_df3 = naics_df3 %>% group_by(GEOID) %>% mutate(percrank = floor(10*rank(pct)/length(pct)))
# naics_df4 = reshape2::dcast(naics_df3, GEOID ~ variable, value.var = "percrank")

#naics_df6 = na.omit(naics_df5)
# naics_df5$MESOZONE = seq(1,length(unique(naics_df5$GEOID)))
#naics_df6$n99 = floor(runif(nrow(naics_df6),0,10))
#naics_df6[naics_df6==0] <- NA
naics_df7 = f2_rank %>% select(CBPZONE1,MESOZONE,all_of(naics))
names(naics_df7)[4:23] = rank_vars
naics_df7 = naics_df7 %>% rename(COUNTY = CBPZONE1)

output_path = paste0('inputs_', selected_region, '/data_mesozone_emprankings.csv')
data.table::fwrite(naics_df7, output_path)
output_path_2 = paste0('inputs_', selected_region, '/MESOZONE_GEOID_LOOKUP.csv')
data.table::fwrite(naics_df6[,c("CBPZONE1","GEOID","MESOZONE")], output_path_2)




