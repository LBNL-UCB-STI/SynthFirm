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
    "doParallel",
    'splitstackshape')
new.packages <-
  list.of.packages[!(list.of.packages %in% installed.packages()[, "Package"])]
if (length(new.packages))
  install.packages(new.packages)
lapply(list.of.packages, require, character = TRUE)
#################################################################################
#install_github("f1kidd/fmlogit")
set.seed(0)
basedir <-
  "/Volumes/GoogleDrive/My Drive/BEAM-CORE/Task3 Freight/CodeBase"
setwd(basedir)

sctg_list = seq(5, 5, 1)

####### loading parameters for distance-based stratify sampling ########
TruckShare = 1
mesozone_to_faf_lookup = data.table::fread("./parameters/zonal_id_lookup_final.csv", h = T)
shipment_by_distance_bin_distribution = data.table::fread("./parameters/fraction_of_shipment_count_by_distance_bin.csv", h = T)
shipment_distance_lookup = data.table::fread("./parameters/combined_travel_time_skim.csv", h = T)

shipment_distance_lookup = shipment_distance_lookup[shipment_distance_lookup$Alternative == 'Air', ]

registerDoParallel(6)

for (k in sctg_list) {
  start_time = Sys.time()
  print(paste0("Processing SCTG Group-", k))
  
  g1_prods = data.table::fread(paste0("./outputs/prods_sctg", k, ".csv"), h = T)
  g1_consm = data.table::fread(paste0("./outputs/consumers_sctg", k, ".csv"), h = T)
  
  # assign faf id to producers
  g1_prods <- merge(g1_prods, mesozone_to_faf_lookup, by.x = 'Zone', by.y = 'MESOZONE', all.x = TRUE)
  # split consumer by zone
  list_of_mesozones = unique(g1_consm$Zone)
  
  for (zone in list_of_mesozones) {
    print(paste0("Processing consumers origin from ", zone))
    f0 <- g1_consm %>% filter(Zone == zone) %>% as_tibble()
    
    # filter supplier
    origin_faf <- as.numeric(mesozone_to_faf_lookup[mesozone_to_faf_lookup$MESOZONE == zone, 'FAFID'])
    distance_skim <- shipment_distance_lookup[shipment_distance_lookup$orig_FAFID == origin_faf, ]
    commodity_need <- unique(f0$InputCommodity)
    supplier <- g1_prods %>% filter(OutputCommodity %in% commodity_need)
    supplier <- merge(supplier, distance_skim, by.x = 'FAFID', by.y = 'dest_FAFID', all.x = TRUE)
    supplier <- supplier %>%
      mutate(Distance = if_else(is.na(Distance), 5000, Distance))
    
    # assign distance bins
    nbreaks <- as.numeric(shipment_by_distance_bin_distribution$Cutpoint)
    nbreaks <- c(-1, nbreaks)
    nlabels <- shipment_by_distance_bin_distribution$IDs
    
    supplier$distance_bin <- cut(supplier$Distance, 
                       breaks = nbreaks, 
                       labels = nlabels)
    shipment_by_distance_bin_distribution$probability <- pull(shipment_by_distance_bin_distribution %>% select(k+2) %>% as.data.frame())
    supplier <- merge(supplier, shipment_by_distance_bin_distribution, 
                      by.x = 'distance_bin', by.y = 'IDs', all.x=TRUE)
    list_of_buyers <- unique(f0$BuyerID)
    test0 <-
    foreach (j = 1:length(list_of_buyers), .packages = c("dplyr")) %dopar% {
      buyer = list_of_buyers[j]
      g1_firm1 = f0 %>% filter(BuyerID == buyer) %>% as_tibble()
      firm_need <- unique(g1_firm1$InputCommodity)
      vendor_pool <- NULL
      for (need in firm_need){
        vendor <- supplier %>% filter(OutputCommodity == need)
        # list_bins <- unique(vendor$distance_bin)
        # shipment_by_distance_bin_distribution_used <- shipment_by_distance_bin_distribution %>% 
        #   filter(IDs %in% list_bins) %>% as_tibble()
        sample_size <- round(runif(1, min = 3, 10))
        # shipment_by_distance_bin_distribution_used$probability <- pull(shipment_by_distance_bin_distribution_used %>% select(k+2) %>% as.data.frame())
        # shipment_by_distance_bin_distribution_used <- shipment_by_distance_bin_distribution_used %>% 
        #   mutate(probability = probability/sum(probability))
        # shipment_by_distance_bin_distribution_used <- shipment_by_distance_bin_distribution_used %>% 
        #   filter(probability > 1/sample_size)
        # vendor <- merge(vendor, shipment_by_distance_bin_distribution_used, by.x = 'distance_bin', by.y = 'IDs', all.x=TRUE)
        # probability <- pull(shipment_by_distance_bin_distribution_used %>% select(k+2) %>% as.data.frame())
        # probability_adj <- 1/sum(probability) * probability # scale sum of probability to 1
        # list_of_sample <- round(sample_size * probability_adj)
        # list_of_bins <-pull(shipment_by_distance_bin_distribution_used %>% select(IDs) %>% as.data.frame())
        # probability <- pull(shipment_by_distance_bin_distribution_used%>% select(probability) %>% as.data.frame())

        vendor_selected <- sample_n(vendor, size = sample_size, weight = probability)
        vendor_pool <- rbind(vendor_pool, vendor_selected)
      }
      
      df1 = full_join(g1_firm1,
                      vendor_pool,
                      by = c("InputCommodity" = "OutputCommodity")) %>%
                      filter(BuyerID != SellerID)
      df1 = df1 %>% group_by(InputCommodity)%>%
        mutate(TmpTons = OutputCapacityTons * PurchaseAmountTons /
                     sum(OutputCapacityTons))
     
      df1 = df1 %>% group_by(InputCommodity)%>%
        mutate(TruckLoad = round(TruckShare * TmpTons))
      
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
      # break
    }
      sctg1_od <- test0 %>% bind_rows()
      saveRDS(sctg1_od, paste0("./outputs/sctg", k, "_od", zone, ".rds"))
      rm(sctg1_od)
      rm(test0)
      gc()

    #break
  }
  
  # BusID_List = g1_consm %>% distinct(BuyerID) %>% as_tibble()
  # TruckShare = 1
  # nsize = 50000
  # chunk_size = round(length(BusID_List$BuyerID) / nsize, 0)
  # split_number = rep(nsize, chunk_size)
  # 
  # g1_split = split(BusID_List, sample(rep(1:chunk_size, times = split_number)))
  
  
  
  # for (i in 1:length(g1_split)) {
  #   print(paste0("Processing File Chunk ", i))
  #   f0 = g1_split[[i]]
  #   registerDoParallel(cores = 3)
  #   test0 <-
  #     foreach(j = 1:nrow(f0), .packages = c("dplyr")) %dopar% {
  #       g1_firm1 = g1_consm %>% filter(BuyerID == f0$BuyerID[j]) %>% as_tibble()
  #       #set.seed(0)
  #       nsample = round(runif(1, min = 10, 50))
  #       df0 = g1_prods %>% sample_n(size = nsample, replace = FALSE) %>% as_tibble()
  #       df1 = full_join(g1_firm1,
  #                       df0,
  #                       by = c("InputCommodity" = "OutputCommodity")) %>%
  #         filter(BuyerID != SellerID)
  #       df1 = df1 %>% mutate(TmpTons = OutputCapacityTons * PurchaseAmountTons /
  #                              sum(OutputCapacityTons))
  #       df1 = df1 %>% mutate(TruckLoad = round(TruckShare * TmpTons))
  #       df1 = df1 %>% rename(
  #         BuyerZone = Zone.x,
  #         BuyerNAICS = NAICS.x,
  #         SellerZone = Zone.y,
  #         SellerNAICS = NAICS.y,
  #         SCTG_Group = SCTG_Group.x
  #       )
  #       df2 = df1 %>% select(
  #         BuyerID,
  #         BuyerZone,
  #         BuyerNAICS,
  #         SellerID,
  #         SellerZone,
  #         SellerNAICS,
  #         TruckLoad,
  #         SCTG_Group
  #       ) %>% filter(TruckLoad > 0)
  #       return(df2)
  #       gc()
  #       # break
  #     }
  #   
  #   sctg1_od <- test0 %>% bind_rows()
  #   saveRDS(sctg1_od, paste0("./outputs/sctg", k, "_od", i, ".rds"))
  #   rm(sctg1_od)
  #   rm(test0)
  #   gc()
  #   break
  # }
  # 
  end_time = Sys.time() - start_time
  print(end_time)

  od_list <- list()

  for (i in list_of_mesozones) {
    print(paste0("Reading OD ", i))
    od_list[[i]] = readRDS(paste0("./outputs/sctg", k, "_od", i, ".rds"))
  }

  od_df = od_list %>% bind_rows()
  saveRDS(od_df, paste0("./outputs/sctg", k, "_od.rds"))
  sapply(paste0("./outputs/sctg", k, "_od", list_of_mesozones, ".rds"),
         unlink)
 #break
}
