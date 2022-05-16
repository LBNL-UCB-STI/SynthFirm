#################################################################################
rm(list = ls())
options(scipen = '10')
list.of.packages <-
  c("dplyr",
    "data.table",
    "sf",
    "mapview",
    "dtplyr",
    "tidyr",
    "parallel")
new.packages <-
  list.of.packages[!(list.of.packages %in% installed.packages()[, "Package"])]
if (length(new.packages))
  install.packages(new.packages)
lapply(list.of.packages, require, character = TRUE)
#################################################################################
#install_github("f1kidd/fmlogit")
set.seed(0)
path2file <-
  "/Users/xiaodanxu/Documents/SynthFirm.nosync/BayArea_GIS"
setwd(path2file)

tx_registration <- fread('registration/TEXAS_MDHDbybiz.csv')
medium_duty_class <- c(3, 4, 5, 6) # ref: https://afdc.energy.gov/data/10380
heavy_duty_class <- c(7, 8)

carrier_types = unique(tx_registration$carrier_type)
fuel_types = unique(tx_registration$fuel)

