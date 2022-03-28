################ part 5 ######################

#-----------------------------------------------------------------------------------
rm(list = ls())
path2code <- '/Users/xiaodanxu/Documents/GitHub/SynthFirm/Firm Synthesis/scripts/'
source(paste0(path2code, 'step0_SynthFirm_starter.R')) # load packages
source(paste0(path2code, 'scenario/scenario_variables.R'))  # load environmental variable

path2file <-
  "/Users/xiaodanxu/Documents/SynthFirm.nosync"
setwd(path2file)

firms <- data.table::fread("./outputs/synthetic_firms.csv", h = T)
carrier_size_distribution <- data.table::fread("./inputs/fleet_size_distribution.csv", h = T)
cargo_type_distribution <- data.table::fread("./inputs/probability_of_cargo_group.csv", h = T)
mesozone_shapefile <- st_read('./inputs/Austin_freight.geojson')

# clip us map to remove island
box = c(xmin = -179.23109, ymin = 10, xmax = -66.96466, ymax = 71.365162)
mesozone_shapefile_no_island <- st_crop(mesozone_shapefile, box)
st_write(mesozone_shapefile_no_island, './inputs/Austin_freight_noisland.geojson')
list_of_mesozones <- unique(firms$MESOZONE)
firms$lat <- 0
firms$lon <- 0
for (zone in list_of_mesozones){
  print(zone)
  sample_size <- nrow(firms[firms$MESOZONE == zone, ])
  selected_zone <- mesozone_shapefile_no_island %>% filter(MESOZONE == zone)
  sample_points <- st_sample(selected_zone, sample_size)
  sample_point_coordinates <- st_coordinates(sample_points)
  firms[firms$MESOZONE == zone, 'lat'] = sample_point_coordinates[,2]
  firms[firms$MESOZONE == zone, 'lon'] = sample_point_coordinates[,1]
}

write.csv(firms, './outputs/synthetic_firms_with_location.csv', row.names=FALSE)
# zone_id = 20004
# sample_size <- nrow(firms[firms$MESOZONE == zone_id, ])
# selected_zone <- mesozone_shapefile_no_island %>% filter(MESOZONE == zone_id)
# sample_points <- st_sample(selected_zone, sample_size)
# sample_point_coordinates <- st_coordinates(sample_points)
# firms[firms$MESOZONE == zone, 'lat'] = sample_point_coordinates[,2]
# firms[firms$MESOZONE == zone, 'lon'] = sample_point_coordinates[,1]

firms <- data.table::fread('./outputs/synthetic_firms_with_location.csv', h = T)
carrier_industry <- firms %>% filter(Industry_NAICS6_Make %in% c('492000', '484000')) %>% as_tibble()

carrier_size_distribution_short <- carrier_size_distribution %>% select(fleet_size, min_size, avg_truck_per_carrier, 
                                                                        total_truck_std, fraction_of_carrier, percent_sut) %>% as_tibble()
sample_size <- nrow(carrier_industry)

sample_fleet_size <- sample_n(carrier_size_distribution_short, size = sample_size, weight = fraction_of_carrier, replace = TRUE)
sample_fleet_size$n_trucks <- as.integer(rnorm(sample_size, mean = sample_fleet_size$avg_truck_per_carrier, sd = sample_fleet_size$total_truck_std))
sample_fleet_size <- sample_fleet_size %>% mutate(n_trucks = ifelse(n_trucks <= min_size, min_size, n_trucks))

scale_factor <- sum(carrier_size_distribution$total_trucks)/ sum(sample_fleet_size$n_trucks)
sample_fleet_size <- sample_fleet_size %>% mutate(n_trucks = as.integer(scale_factor * n_trucks))
sample_fleet_size <- sample_fleet_size %>% mutate(mdt = round(n_trucks * percent_sut, 0))
sample_fleet_size <- sample_fleet_size %>% mutate(hdt = n_trucks - mdt)
sample_fleet_size <- arrange(sample_fleet_size, n_trucks)
sample_fleet_size_out <- sample_fleet_size %>% select(mdt, hdt)
carrier_industry <- arrange(carrier_industry, Emp)
carrier_industry <- cbind(carrier_industry, sample_fleet_size_out)

for (row in 1:nrow(cargo_type_distribution)) {
  cargo_type = as.character(cargo_type_distribution[row, 'Cargo'])
  fraction = as.numeric(cargo_type_distribution[row, 'probability'])
  carrier_industry[, cargo_type] = rbinom(sample_size, 1, fraction)
}

list_of_cargo_type <- unique(cargo_type_distribution$Cargo)
carrier_industry[, 'cargo_check'] = apply(carrier_industry[, list_of_cargo_type], 1, max)
carrier_industry[carrier_industry$cargo_check == 0, 'other_cargo'] = 1
carrier_industry <- select(carrier_industry, -cargo_check)




# carrier_industry <- data.table::fread("./outputs/synthetic_carriers.csv", h = T)

carrier_industry <- carrier_industry %>% mutate(SCTG1 = ifelse(construction_material == 1, 1, 
                                                        ifelse(farm_product == 1, 1, 
                                                        ifelse(large_equipment == 1, 1, 
                                                        ifelse(other_bulk == 1, 1, 
                                                        ifelse(other_cargo==1, 1, 0)))))) %>% as_tibble()

carrier_industry <- carrier_industry %>% mutate(SCTG2 = ifelse(farm_product == 1, 1, 
                                                        ifelse(chemical == 1, 1, 
                                                        ifelse(other_cargo == 1, 1, 0)))) %>% as_tibble()
carrier_industry <- carrier_industry %>% mutate(SCTG3 = ifelse(food == 1, 1, 
                                                        ifelse(farm_product == 1, 1, 
                                                        ifelse(other_bulk == 1, 1, 
                                                        ifelse(other_cargo==1, 1, 0))))) %>% as_tibble()

carrier_industry <- carrier_industry %>% mutate(SCTG4 = ifelse(construction_material == 1, 1, 
                                                        ifelse(vehicle_home == 1, 1, 
                                                        ifelse(large_equipment == 1, 1, 
                                                        ifelse(other_bulk == 1, 1, 
                                                        ifelse(household ==1, 1,
                                                        ifelse(other_cargo==1, 1, 0))))))) %>% as_tibble()

carrier_industry <- carrier_industry %>% mutate(SCTG5 = ifelse(garbage == 1, 1, 
                                                        ifelse(other_bulk == 1, 1, 
                                                        ifelse(intermodal_container == 1, 1, 
                                                        ifelse(other_cargo==1, 1, 0))))) %>% as_tibble()

write.csv(carrier_industry, './outputs/synthetic_carriers.csv', row.names=FALSE)
