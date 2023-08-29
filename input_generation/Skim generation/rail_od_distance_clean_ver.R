library(GISTools)
library(sf)
library(dplyr)
library(tidyr)
library(sfnetworks)
library(tidygraph)
library(tidyverse)
library(igraph)
library(nngeo)
library(parallel)
library(stplanr)

# load FAF zone and generate centroids
setwd("/Volumes/GoogleDrive/My Drive/BEAM-CORE/Task3 Freight/GIS Analysis")
faf_zone <- st_read('GIS_Data/FAF4Zones.geojson')
freight_centroid <- st_centroid(faf_zone)
plot(st_geometry(freight_centroid))

# load rail lines, nodes and terminals
railway_network <- st_read('North_American_Rail_Lines/North_American_Rail_Lines.shp')
network_crs <- st_crs(railway_network)
freight_centroid <- st_transform(freight_centroid, crs = network_crs)
plot(st_geometry(railway_network))


railway_terminal <- st_read('Intermodal_Freight_Facilities_RailTOFCCOFC/Intermodal_Freight_Facilities_RailTOFCCOFC.shp')
terminal_crs <- st_crs(railway_terminal)
freight_centroid$nearest_terminal <- unlist(st_nn(freight_centroid, railway_terminal, k = 1))  # find nearest terminal for a given freight centroid

railway_node <- st_read('North_American_Rail_Nodes/North_American_Rail_Nodes.shp')
node_crs <- st_crs(railway_node)
plot(st_geometry(railway_node))

# in rail line file, assign start and end nodes for each line (needed for form directional graph)
node_finder <- function(node_input){
  node_output <- which(railway_node$FRANODEID == node_input)
  return(node_output)
}

railway_network$from = mcmapply(node_finder, railway_network$FRFRANODE)
railway_network$to = mcmapply(node_finder, railway_network$TOFRANODE)

# generate directional graph for rail 
railway_network$weight <- railway_network$MILES
railway_net = sfnetwork(railway_node, railway_network, directed = FALSE, force = TRUE)
plot(railway_net)

# clean network to remove looped lines and duplicated lines
railway_network_sfn_simple = railway_net %>%
  activate("edges") %>%
  arrange(edge_length()) %>%
  filter(!edge_is_multiple()) %>%
  filter(!edge_is_loop())


#### GET ROUTE example
start_node = railway_terminal[30,]
end_node = railway_terminal[5,]
paths = st_network_paths(railway_network_sfn_simple, from = start_node, to = end_node)
paths

node_path = paths %>%
  slice(1) %>%
  pull(node_paths) %>%
  unlist()
# plot(highway_network_sfn_simple, col = "grey")

paths_sf = highway_network_sfn_subdivision %>%
  activate("edges") %>%
  slice(unlist(paths$edge_paths)) %>%
  st_as_sf()

od = rbind(start_node, end_node)
plot(st_geometry(od), col = 'red')
plot(st_geometry(paths_sf), add = TRUE)
routed_network <- slice(activate(highway_network_sfn_subdivision, "nodes"), node_path)
plot(routed_network)

routed_distance <- st_network_cost(highway_network_sfn_subdivision, from = start_node, to = end_node)
gcdistance <- st_distance(start_node, end_node) * 0.000621371
routed_distance
gcdistance

#### GENERATE OD DISTANCE #######

rail_distance_generator_and_error_catcher <- function(ozone, dzone){
  start_node <- railway_terminal[railway_terminal$OBJECTID == ozone,]
  end_node <- railway_terminal[railway_terminal$OBJECTID == dzone,]
  output <- tryCatch(st_network_cost(railway_network_sfn_simple, from = start_node, to = end_node), error = function(e) {'route missing'})
  return(output)
}

origin_node <- freight_centroid[, c('FAF', 'nearest_terminal')] %>% st_drop_geometry()
dest_node <- freight_centroid[, c('FAF', 'nearest_terminal')] %>% st_drop_geometry()
colnames(origin_node) <- c('origin_FAF', 'origin_terminal')
colnames(dest_node) <- c('dest_FAF', 'dest_terminal')
od_with_node <- expand_grid(origin_node, dest_node)
# sample_od_with_node <- sample_n(od_with_node, 1000)
od_with_node$rail_distance <- mcmapply(rail_distance_generator_and_error_catcher, 
                                         od_with_node$origin_terminal, od_with_node$dest_terminal)
od_with_node$rail_distance <- od_with_node$rail_distance *0.000621371
write_csv(od_with_node, 'FAF_od_with_rail_distance.csv')



###########generate highway distance to terminal #############
highway_network <- st_read('nhs20210815/National_NHS_2021-08-23.shp')
network_crs <- st_crs(highway_network)
highway_network <- st_transform(highway_network, crs = terminal_crs)
# clean up network
st_geometry(highway_network) = st_geometry(highway_network) %>%
  lapply(function(x) round(x, 4)) %>%
  st_sfc(crs = st_crs(highway_network))


######## sf network method #######
highway_network$weight <- highway_network$LENGTH
highway_network_sfn <- as_sfnetwork(highway_network, directed = FALSE)%>%
  activate("edges") 

highway_network_sfn_simple = highway_network_sfn %>%
  activate("edges") %>%
  arrange(edge_length()) %>%
  filter(!edge_is_multiple()) %>%
  filter(!edge_is_loop())

highway_network_sfn_subdivision = convert(highway_network_sfn_simple, to_spatial_subdivision)

origin_distance_generator_and_error_catcher <- function(ozone, dzone){
  start_node <- freight_centroid[freight_centroid$FAF == ozone,]
  end_node <- railway_terminal[railway_terminal$OBJECTID == dzone,]
  output <- tryCatch(st_network_cost(highway_network_sfn_subdivision, from = start_node, to = end_node), error = function(e) {'route missing'})
  return(output)
}
dest_distance_generator_and_error_catcher <- function(ozone, dzone){
  start_node <- railway_terminal[railway_terminal$OBJECTID == ozone,]
  end_node <- freight_centroid[freight_centroid$FAF == dzone,]
  output <- tryCatch(st_network_cost(highway_network_sfn_subdivision, from = start_node, to = end_node), error = function(e) {'route missing'})
  return(output)
}

freight_centroid$arrive_terminal_dist <- mcmapply(origin_distance_generator_and_error_catcher, 
                                              freight_centroid$FAF, freight_centroid$nearest_terminal)

freight_centroid$depart_terminal_dist <- mcmapply(dest_distance_generator_and_error_catcher, 
                                              freight_centroid$nearest_terminal, freight_centroid$FAF)

origin_distance_generator_osm <- function(ozone, dzone){
  start_node <- st_coordinates(freight_centroid[freight_centroid$FAF == ozone,])
  end_node <- st_coordinates(railway_terminal[railway_terminal$OBJECTID == dzone,])
  route <- route_osrm(start_node, end_node, osrm.profile = "car")
  output <- 0.000621371 * route$distance
  return(output)
}

dest_distance_generator_osm <- function(ozone, dzone){
  start_node <- st_coordinates(railway_terminal[railway_terminal$OBJECTID == ozone,])
  end_node <- st_coordinates(freight_centroid[freight_centroid$FAF == dzone,])
  route <- route_osrm(start_node, end_node, osrm.profile = "car")
  print(route$distance)
  output <- 0.000621371 * route$distance
  return(output)
}

freight_centroid_to_fill <- freight_centroid %>%
  filter(! FAF %in% c('020', '151', '159'), arrive_terminal_dist == Inf)
freight_centroid_nofill <- freight_centroid %>%
  filter(FAF %in% c('020', '151', '159') )
freight_centroid_filled <- freight_centroid %>%
  filter(! FAF %in% c('020', '151', '159'), arrive_terminal_dist != Inf)
freight_centroid_to_fill$arrive_terminal_dist <- mcmapply(origin_distance_generator_osm, 
                                                          freight_centroid_to_fill$FAF, freight_centroid_to_fill$nearest_terminal)
freight_centroid_to_fill$depart_terminal_dist <- mcmapply(dest_distance_generator_osm, 
                                                          freight_centroid_to_fill$nearest_terminal, freight_centroid_to_fill$FAF)

freight_centroid_refilled <- rbind(freight_centroid_to_fill, freight_centroid_nofill, freight_centroid_filled)

rail_distance_matrix <- read.csv('FAF_od_with_rail_distance.csv')
rail_distance_matrix <- rail_distance_matrix %>%
  select(origin_FAF, dest_FAF, rail_distance)
od_with_node$origin_FAF <- as.numeric(od_with_node$origin_FAF)
od_with_node$dest_FAF <- as.numeric(od_with_node$dest_FAF)
rail_distance_matrix <- merge(rail_distance_matrix, od_with_node, by = c("origin_FAF", "dest_FAF")) 
leg1 <- freight_centroid_refilled %>% select(FAF, arrive_terminal_dist)
leg1$FAF <- as.numeric(leg1$FAF)
leg2 <- freight_centroid_refilled %>% select(FAF, depart_terminal_dist)
leg2$FAF <- as.numeric(leg2$FAF)
rail_distance_matrix <- merge(rail_distance_matrix, leg1, by.x = c("origin_FAF"), by.y = c('FAF'))
rail_distance_matrix <- merge(rail_distance_matrix, leg2, by.x = c("dest_FAF"), by.y = c('FAF'))
rail_distance_matrix <- rail_distance_matrix %>%
  mutate(drive_distance = ifelse(origin_FAF == dest_FAF, 0, arrive_terminal_dist + depart_terminal_dist))
rail_distance_matrix <- rail_distance_matrix %>%
  mutate(total_distance = drive_distance + rail_distance)

rail_distance_matrix_out <- rail_distance_matrix %>% 
  select(origin_FAF, dest_FAF, origin_terminal, dest_terminal, rail_distance, 
         arrive_terminal_dist, depart_terminal_dist, drive_distance, total_distance)

write.csv(rail_distance_matrix_out, 'FAF_od_with_rail_distance_and_transfer.csv')
