################ part 5 ######################

#-----------------------------------------------------------------------------------
# rm(list = ls())
# path2code <- '/Users/xiaodanxu/Documents/GitHub/SynthFirm/Firm Synthesis/scripts/'
# source(paste0(path2code, 'step0_SynthFirm_starter.R')) # load packages
# source(paste0(path2code, 'scenario/scenario_variables.R'))  # load environmental variable
# library(tidyverse)
# 
# library(foreach)
# library(doParallel)
registerDoParallel(cores=number_of_cores)

# path2file <-
#   "/Users/xiaodanxu/Documents/SynthFirm.nosync"
# setwd(path2file)
output_dir <- paste0(path2file, "/outputs_", scenario)
sf_use_s2(FALSE)
firms <- data.table::fread(paste0(output_dir, '/', synthetic_firms_no_location_file), h = T) # 8,396, 679 FIRMS
mesozone_shapefile <- st_read(paste0(path2file, "/inputs_", scenario, '/', scenario, '_freight.geojson'))

# clip us map to remove island
box = c(xmin = -179.23109, ymin = 10, xmax = -66.96466, ymax = 71.365162)
mesozone_shapefile_no_island <- st_crop(mesozone_shapefile, box)

st_write(mesozone_shapefile_no_island,
         paste0(path2file, "/inputs_", scenario, '/', scenario, '_freight_noisland.geojson'), append=FALSE)

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
