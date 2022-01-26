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
basedir <-
  "/Users/srinath/OneDrive - LBNL/Projects/SMART-2.0/Task-3 BAMOS/SynthFirm"
setwd(basedir)

firms_all = data.table::fread("./outputs/synthfirms_all.csv", h = T)
sup_df = data.table::fread("./outputs/producers_all.csv", h = T)
buyer_df = data.table::fread("./outputs/consumers_all.csv", h = T)
sctg_lookup = data.table::fread("./inputs/sctg_groups.csv", h = T)

sup_df = sup_df %>% left_join(sctg_lookup, by = c("Commodity_SCTG" = "SCTG_Code"))
buyer_df = buyer_df %>% left_join(sctg_lookup, by = c("Commodity_SCTG" =
                                                        "SCTG_Code"))
gc()

### SCTG 5 Groups Processed ###
for (i in 1:5) {
  print(paste0("Processing SCTG Group ", i))
  
  g1_prods = sup_df %>% filter(SCTG_Group == i) %>% select(
    SCTG_Group,
    Commodity_SCTG,
    SellerID,
    Zone,
    NAICS,
    OutputCommodity,
    OutputCapacityTons
  ) %>% as_tibble()
  
  g1_consm = buyer_df %>% filter(SCTG_Group == i) %>% select(
    SCTG_Group,
    Commodity_SCTG,
    BuyerID,
    Zone,
    NAICS,
    InputCommodity,
    PurchaseAmountTons,
    SingleSourceMaxFraction
  ) %>% as_tibble()
  
  fwrite(g1_prods, paste0("./outputs/prods_sctg", i, ".csv"))
  fwrite(g1_consm, paste0("./outputs/consumers_sctg", i, ".csv"))
  gc()
}
