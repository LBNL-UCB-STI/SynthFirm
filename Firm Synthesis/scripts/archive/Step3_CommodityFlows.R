#################################################################################
rm(list = ls())
options(scipen = '10')
list.of.packages <-
  c("dplyr",
    "data.table",
    "sf",
    "mapview",
    "dtplyr",
    "foreach",
    "doParallel")
new.packages <-
  list.of.packages[!(list.of.packages %in% installed.packages()[, "Package"])]
if (length(new.packages))
  install.packages(new.packages)
lapply(list.of.packages, require, character = TRUE)
#################################################################################
#install_github("f1kidd/fmlogit")
set.seed(0)
basedir <-
  "/Users/srinath/OneDrive - LBNL/Projects/SMART-2.0/Task-3 BAMOS/SynthFirm"
setwd(basedir)

sctg_list = seq(1, 5, 1)

for (k in sctg_list) {
  print(paste0("Processing SCTG Group-", k))
  
  g1_prods = data.table::fread(paste0("./outputs/prods_sctg", k, ".csv"), h = T)
  g1_consm = data.table::fread(paste0("./outputs/consumers_sctg", k, ".csv"), h = T)
  
  BusID_List = g1_consm %>% distinct(BuyerID) %>% as_tibble()
  TruckShare = 1
  nsize = 50000
  chunk_size = round(length(BusID_List$BuyerID) / nsize, 0)
  split_number = rep(nsize, chunk_size)
  
  g1_split = split(BusID_List, sample(rep(1:chunk_size, times = split_number)))
  
  start_time = Sys.time()
  
  for (i in 1:length(g1_split)) {
    print(paste0("Processing File Chunk ", i))
    f0 = g1_split[[i]]
    registerDoParallel(cores = 3)
    test0 <-
      foreach(j = 1:nrow(f0), .packages = c("dplyr")) %dopar% {
        g1_firm1 = g1_consm %>% filter(BuyerID == f0$BuyerID[j]) %>% as_tibble()
        #set.seed(0)
        nsample = round(runif(1, min = 10, 50))
        df0 = g1_prods %>% sample_n(size = nsample, replace = FALSE) %>% as_tibble()
        df1 = full_join(g1_firm1,
                        df0,
                        by = c("InputCommodity" = "OutputCommodity")) %>%
          filter(BuyerID != SellerID)
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
        ) %>% filter(TruckLoad > 0)
        return(df2)
        gc()
      }
    
    sctg1_od <- test0 %>% bind_rows()
    saveRDS(sctg1_od, paste0("./outputs/sctg", k, "_od", i, ".rds"))
    rm(sctg1_od)
    rm(test0)
    gc()
  }
  
  end_time = Sys.time() - start_time
  print(end_time)
  
  od_list <- list()
  
  for (i in 1:chunk_size) {
    print(paste0("Reading OD ", i))
    od_list[[i]] = readRDS(paste0("./outputs/sctg", k, "_od", i, ".rds"))
  }
  
  od_df = od_list %>% bind_rows()
  saveRDS(od_df, paste0("./outputs/sctg", k, "_od.rds"))
  sapply(paste0("./outputs/sctg", k, "_od", 1:chunk_size, ".rds"),
         unlink)
}
