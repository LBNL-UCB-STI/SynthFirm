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
    "tidyr",
    "doParallel",
    'splitstackshape',
    'gtools')
new.packages <-
  list.of.packages[!(list.of.packages %in% installed.packages()[, "Package"])]
if (length(new.packages))
  install.packages(new.packages)
lapply(list.of.packages, require, character = TRUE)
#################################################################################
#install_github("f1kidd/fmlogit")
set.seed(0)
basedir <-
  "/Users/xiaodanxu/Documents/SynthFirm.nosync"
setwd(basedir)

sctg_list = seq(1, 5, 1)

####### loading parameters for distance-based stratify sampling ########

mesozone_to_faf_lookup = data.table::fread("./inputs/zonal_id_lookup_final.csv", h = T)
shipment_by_distance_bin_distribution = data.table::fread("./skims/fraction_of_shipment_count_by_distance_bin.csv", h = T)
shipment_distance_lookup = data.table::fread("./skims/combined_travel_time_skim.csv", h = T)
producer = data.table::fread("./outputs/synthetic_producers.csv", h = T)

producer <- merge(producer, mesozone_to_faf_lookup, by.x = 'Zone', by.y = 'MESOZONE', all.x = TRUE)
producer <- producer %>% select(SellerID, Zone, NAICS, OutputCommodity, OutputCapacitylb, FAFID) %>% as_tibble()
shipment_distance_lookup = shipment_distance_lookup[shipment_distance_lookup$Alternative == 'Air', ]

# registerDoParallel(6)

for (k in sctg_list) {
  start_time = Sys.time()
  print(paste0("Processing SCTG Group-", k))
  shipment_by_distance_bin_distribution$probability <- pull(shipment_by_distance_bin_distribution %>% select(k+2) %>% as.data.frame())
  
  # g1_prods = data.table::fread(paste0("./outputs/prods_sctg", k, ".csv"), h = T)
  g1_consm = data.table::fread(paste0("./outputs/consumers_sctg", k, ".csv"), h = T)
  g1_consm <- merge(g1_consm, mesozone_to_faf_lookup, by.x = 'Zone', by.y = 'MESOZONE', all.x = TRUE)
  g1_consm <- g1_consm %>% mutate(FAFID = replace_na(FAFID, 0))
  g1_consm <- g1_consm %>% select(BuyerID, Zone, Commodity_SCTG, SCTG_Group, NAICS, InputCommodity, PurchaseAmountlb, FAFID)  %>% as_tibble() 
  # assign faf id to producers
  # g1_prods <- merge(g1_prods, mesozone_to_faf_lookup, by.x = 'Zone', by.y = 'MESOZONE', all.x = TRUE)
  # split consumer by zone
  list_of_mesozones = unique(g1_consm$Zone)
  list_of_commodity = unique(g1_consm$InputCommodity)
  
  for (com in list_of_commodity){
    print(com)
    output_b2b_flow = NULL
    buyer <- g1_consm %>% filter(InputCommodity == com, PurchaseAmountlb > 0) %>% as_tibble()
    cut_off = sort(unique(quantile(buyer$PurchaseAmountlb, probs = seq(0, 1, 0.1), na.rm = FALSE)))
    label = seq(1, length(cut_off)-1, 1)
    buyer$demand_rank = cut(buyer$PurchaseAmountlb, breaks = cut_off, labels = label, right = TRUE, include.lowest = TRUE)
    
    supplier <- producer %>% filter(OutputCommodity == com, OutputCapacitylb > 0) %>% as_tibble()
    supplier$supply_rank = cut(supplier$OutputCapacitylb, breaks = cut_off, labels = label, right = TRUE, include.lowest = TRUE)
    
    level_of_ranks = order(-label)
    
    for (level in level_of_ranks){
      print(level)
      selected_buyer <- buyer %>% filter(as.integer(demand_rank) == level)
      list_of_faf = unique(selected_buyer$FAFID)
      for (faf in list_of_faf){
        # print(faf)
        selected_buyer_by_zone <- selected_buyer %>% filter(FAFID == faf)
        supplier_pool <- supplier %>% filter(as.integer(supply_rank) >= level)   
        distance_skim <- shipment_distance_lookup[shipment_distance_lookup$orig_FAFID == faf, ]
        supplier_pool <- merge(supplier_pool, distance_skim, by.x = 'FAFID', by.y = 'dest_FAFID', all.x = TRUE)
        supplier_pool <- supplier_pool %>%
               mutate(Distance = if_else(is.na(Distance), 5000, Distance))
        nbreaks <- as.numeric(shipment_by_distance_bin_distribution$Cutpoint)
        nbreaks <- c(-1, nbreaks)
        nlabels <- shipment_by_distance_bin_distribution$IDs
        supplier_pool$distance_bin <- cut(supplier_pool$Distance, breaks = nbreaks, labels = nlabels)
        supplier_pool <- merge(supplier_pool, shipment_by_distance_bin_distribution, 
                             by.x = 'distance_bin', by.y = 'IDs', all.x=TRUE)
        supplier_pool <- supplier_pool %>% select(SellerID, Zone, NAICS, OutputCommodity, OutputCapacitylb, supply_rank,
                                                  Distance, probability)
        sample_size = min(2 * nrow(selected_buyer_by_zone), nrow(supplier_pool))  # select max 3 suppliers for each buyer, capped by total suppliers
        selected_supplier <- sample_n(supplier_pool, size = sample_size, weight = probability)
        paired_buyer_supplier <- full_join(selected_buyer_by_zone,
                                           selected_supplier,
                                           by = c("InputCommodity" = "OutputCommodity")) %>%
                                            filter(BuyerID != SellerID)     
        paired_buyer_supplier <- paired_buyer_supplier %>% mutate(ratio = OutputCapacitylb/PurchaseAmountlb)
        paired_buyer_supplier <- paired_buyer_supplier %>%
          group_by(BuyerID) %>%
          arrange(Distance, .by_group = TRUE) %>%
          mutate(cs_ratio = cumsum(ratio))
        paired_buyer_supplier <- paired_buyer_supplier %>% mutate(met_demand = ifelse(cs_ratio>=1, 1, 0))
        paired_buyer_supplier <- paired_buyer_supplier %>%
          group_by(BuyerID) %>%
          mutate(cs_met = cumsum(met_demand))
        selected_b2b_flow <- paired_buyer_supplier %>% filter(cs_met <= 1)
        selected_b2b_flow = selected_b2b_flow %>% group_by(BuyerID)%>%
          mutate(TruckLoad = ratio * PurchaseAmountlb /
                   sum(ratio))
        selected_b2b_flow <- selected_b2b_flow %>% select(InputCommodity, BuyerID, Zone.x, Commodity_SCTG, SCTG_Group, NAICS.x, 
                                                          PurchaseAmountlb, SellerID, Zone.y, NAICS.y, OutputCapacitylb, TruckLoad)
        selected_b2b_flow = selected_b2b_flow %>% rename(
          BuyerZone = Zone.x,
          BuyerNAICS = NAICS.x,
          SellerZone = Zone.y,
          SellerNAICS = NAICS.y
        )
        selected_b2b_flow = selected_b2b_flow %>% select(
          BuyerID,
          BuyerZone,
          BuyerNAICS,
          SellerID,
          SellerZone,
          SellerNAICS,
          TruckLoad,
          SCTG_Group
        )
        output_b2b_flow = rbind(output_b2b_flow, selected_b2b_flow)
        if (nrow(selected_buyer_by_zone) > nrow(selected_b2b_flow)){
          print(paste0('incomplete match for zone ', faf))}
        #break
        gc()
      }

    }

    saveRDS(output_b2b_flow, paste0("./outputs/sctg", k, "_", com, ".rds"))
    
    gc()
#    buyer <- merge(buyer, supplier, by.x = "InputCommodity", by.y = 'OutputCommodity', allow.cartesian = TRUE)

    # break
  }
  od_list <- list()

  for (i in list_of_commodity) {
    print(paste0("Reading OD ", i))
    od_list[[i]] = readRDS(paste0("./outputs/sctg", k, "_", i, ".rds"))
  }

  od_df = od_list %>% bind_rows()
  write.csv(od_df, paste0("./outputs/sctg", k, "_od.csv"))
  sapply(paste0("./outputs/sctg", k, "_", list_of_commodity, ".rds"),
         unlink)

  gc()
    #break
  }
  

