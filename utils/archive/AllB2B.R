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

firms_all = data.table::fread("./MTC_Freight/results/synthfirms_all.csv", h = T)
sup_df = data.table::fread("./MTC_Freight/results/producers_all.csv", h = T)
buyer_df = data.table::fread("./MTC_Freight/results/consumers_all.csv", h = T)

sctg_lookup = data.table::fread("./MTC_Freight/results/SCTG_Groups.csv", h = T)

sup_df = sup_df %>% left_join(sctg_lookup, by = c("Commodity_SCTG" = "SCTG_Code"))
buyer_df = buyer_df %>% left_join(sctg_lookup, by = c("Commodity_SCTG" =
                                                        "SCTG_Code"))
gc()

f0 = firms_all %>% filter(MESOZONE>10617) %>% as_tibble()

#f0 = firms_all %>% group_by(MESOZONE,CBPZONE,FAFZONE) %>% summarize(count=n())

data.table::fwrite(as_tibble(f0),"FIRMS_LOOKUP.csv")

### SCTG Group-1 ###
g1_prods = sup_df %>% filter(SCTG_Group == 5) %>% select(SCTG_Group,
                                                         SellerID,
                                                         Zone,
                                                         NAICS,
                                                         OutputCommodity,
                                                         OutputCapacityTons) %>% as_tibble()
g1_consm = buyer_df %>% filter(SCTG_Group == 5) %>% select(
  SCTG_Group,
  BuyerID,
  Zone,
  NAICS,
  InputCommodity,
  PurchaseAmountTons,
  SingleSourceMaxFraction
) %>% as_tibble()

fwrite(g1_prods, "./MTC_Freight/results/prods_sctg5.csv")
fwrite(g1_consm, "./MTC_Freight/results/consumers_sctg5.csv")

firm_list = list()
BusID_List = as_tibble(g1_consm %>% distinct(BuyerID))
TruckShare = 0.80

g1_firm1 = g1_consm %>% filter(BuyerID == BusID_List$BuyerID[1]) %>% as_tibble()
g1_firm2 = full_join(g1_firm1, g1_prods, by=c("InputCommodity"="OutputCommodity")) %>% filter(BuyerID != SellerID)
set.seed(0)
df1 = g1_firm2 %>% sample_n(size = round(runif(1, 5, nrow(g1_firm2))), replace =
                              FALSE)
df1 = df1 %>% mutate(TmpTons = OutputCapacityTons * PurchaseAmountTons /
                       sum(OutputCapacityTons))
df1 = df1 %>% mutate(TruckLoad = round(TruckShare * TmpTons))
df1 = df1 %>% rename(
  BuyerZone = Zone.x,
  BuyerNAICS = NAICS.x,
  SellerZone = Zone.y,
  SellerNAICS = NAICS.y,
  SCTG_Group = SCTG_Group.x
)
df2 = df1 %>% select(
  BuyerID,
  BuyerZone,
  BuyerNAICS,
  SellerID,
  SellerZone,
  SellerNAICS,
  TruckLoad,
  SCTG_Group
)





split_number = rep(nrow(as_tibble(g1_consm))/100,100)

g1_split = split(as_tibble(g1_consm), sample(rep(1:100,times=split_number)))

v1 = g1_split$`1`

g1 = full_join(v1, g1_prods, by=c("InputCommodity"="OutputCommodity")) %>% filter(BuyerID != SellerID)
g1 = g1 %>% filter(BuyerID != SellerID)

firm_list = list()
BusID_List = sample(unique(g1$BuyerID), size = length(unique(g1$BuyerID)))
TruckShare = 0.80

start_time = Sys.time()

for (i in 1:length(BusID_List)) {
  print(paste0("Processing BuyerID ", i))
  g1_firm1 = g1 %>% filter(BuyerID == BusID_List[[i]])
  df1 = g1_firm1 %>% sample_n(size = round(runif(1, 5, nrow(g1_firm1))), replace =
                                FALSE)
  df1 = df1 %>% mutate(TmpTons = OutputCapacityTons * PurchaseAmountTons /
                         sum(OutputCapacityTons))
  df1 = df1 %>% mutate(TruckLoad = round(TruckShare * TmpTons))
  df1 = df1 %>% rename(
    BuyerZone = Zone.x,
    BuyerNAICS = NAICS.x,
    SellerZone = Zone.y,
    SellerNAICS = NAICS.y,
    SCTG_Group = SCTG_Group.x
  )
  df2 = df1 %>% select(
    BuyerID,
    BuyerZone,
    BuyerNAICS,
    SellerID,
    SellerZone,
    SellerNAICS,
    TruckLoad,
    SCTG_Group
  )
  firm_list[[i]] = df2
}

end_time = Sys.time() - start_time
print(end_time)
sctg_od1 = firm_list %>% bind_rows()


