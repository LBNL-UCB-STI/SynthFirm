################ part 5 ######################

#-----------------------------------------------------------------------------------
packages = c(
  "dplyr",
  "dtplyr",
  "data.table",
  "bit64",
  "reshape",
  "reshape2",
  "ggplot2",
  "fastcluster",
  "sf",
  "foreach",
  "doParallel",
  "tidyverse"
)
lapply(packages, require, character = TRUE)


path2file <- "/Users/xiaodanxu/Documents/SynthFirm.nosync" # path to project folder
number_of_cores <- 8 # register number of cores for location generation
registerDoParallel(cores=number_of_cores)
## define scenario name
scenario <- 'SF' # scenario name, the same as input generation process
out_scenario <- 'SF_2040'

synthetic_firms_no_location_file <- "forecasted_firms.csv" # synthetic firm output (before location assignment)
synthetic_firms_with_location_file <- "forecasted_firms_with_location.csv" # synthetic firm output (after location assignment)

output_dir <- paste0(path2file, "/outputs_", out_scenario)
sf_use_s2(FALSE)
firms <- data.table::fread(paste0(output_dir, '/', synthetic_firms_no_location_file), h = T) # 8,396, 679 FIRMS
mesozone_shapefile <- st_read(paste0(path2file, "/inputs_", scenario, '/SFBay_freight.geojson'))

# clip us map to remove island
box = c(xmin = -179.23109, ymin = 10, xmax = -66.96466, ymax = 71.365162)
mesozone_shapefile_no_island <- st_crop(mesozone_shapefile, box)

# st_write(mesozone_shapefile_no_island,
#          paste0(path2file, "/inputs_", scenario, '/', scenario, '_freight_noisland.geojson'), append=FALSE)

# firms <- firms[1:1000] # test it
list_of_mesozones <- unique(firms$MESOZONE)
firms$lat <- 0
firms$lon <- 0
firms <- foreach(i = 1:length(list_of_mesozones), .combine=rbind) %dopar%  {
  zone <- list_of_mesozones[i]
  # print(zone)
  sample_size <- nrow(firms[firms$MESOZONE == zone, ])
  selected_zone <- mesozone_shapefile_no_island %>% filter(MESOZONE == zone)
  sample_points <- st_sample(selected_zone, sample_size)
  sample_point_coordinates <- st_coordinates(sample_points)
  firms_selected <- firms[firms$MESOZONE == zone]
  firms_selected[, 'lat'] = sample_point_coordinates[,2]
  firms_selected[, 'lon'] = sample_point_coordinates[,1]
  
  firms_selected
}

write.csv(firms, paste0(output_dir, '/', synthetic_firms_with_location_file), row.names=FALSE)
