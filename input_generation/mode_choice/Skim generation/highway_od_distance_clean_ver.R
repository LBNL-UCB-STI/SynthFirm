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

setwd("/Volumes/GoogleDrive/My Drive/BEAM-CORE/Task3 Freight/GIS Analysis")
faf_zone <- st_read('GIS_Data/FAF4Zones.geojson')
freight_centroid <- st_centroid(faf_zone)
# freight_centroid <- st_read('freight_centroids.geojson')
plot(st_geometry(freight_centroid))

highway_network <- st_read('nhs20210815/National_NHS_2021-08-23.shp')
network_crs <- st_crs(highway_network)
freight_centroid <- st_transform(freight_centroid, crs = network_crs)
#plot(st_geometry(highway_network))

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
#highway_network_sfn_smoothed = convert(highway_network_sfn_subdivision, to_spatial_smooth)
#plot(highway_network_sfn_simple)

#### GET ROUTE example
start_node = freight_centroid[1,]
end_node = freight_centroid[2,]
paths = st_network_paths(highway_network_sfn_subdivision, from = start_node, to = end_node)
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

#### GENERATE OD DISTANCE
distance_generator_and_error_catcher <- function(ozone, dzone){
  start_node <- freight_centroid[freight_centroid$FAF == ozone,]
  end_node <- freight_centroid[freight_centroid$FAF == dzone,]
  output <- tryCatch(st_network_cost(highway_network_sfn_subdivision, from = start_node, to = end_node), error = function(e) {'route missing'})
  return(output)
}

origin_node <- freight_centroid[, c('FAF')] %>% st_drop_geometry()
dest_node <- freight_centroid[, c('FAF')] %>% st_drop_geometry()
colnames(origin_node) <- c('origin_FAF')
colnames(dest_node) <- c('dest_FAF')
od_with_node <- expand_grid(origin_node, dest_node)
# sample_od_with_node <- sample_n(od_with_node, 1000)
od_with_node$routed_distance <- mcmapply(distance_generator_and_error_catcher, 
                                         od_with_node$origin_FAF, od_with_node$dest_FAF)

write_csv(od_with_node, 'FAF_od_with_distance.csv')
# fill missing route
#### GENERATE OD DISTANCE
distance_generator_osm <- function(ozone, dzone){
  start_node <- st_coordinates(freight_centroid[freight_centroid$FAF == ozone,])
  end_node <- st_coordinates(freight_centroid[freight_centroid$FAF == dzone,])
  route <- route_osrm(start_node, end_node, osrm.profile = "car")
  output <- 0.000621371 * route$distance
  return(output)
}

od_with_node_to_fill <- od_with_node %>%
  filter(! origin_FAF %in% c('020', '151', '159'), !dest_FAF %in% c('020', '151', '159'), routed_distance == Inf)
od_with_node_nofill <- od_with_node %>%
  filter((origin_FAF %in% c('020', '151', '159') | dest_FAF %in% c('020', '151', '159')))
od_with_node_filled <- od_with_node %>%
  filter(! origin_FAF %in% c('020', '151', '159'), !dest_FAF %in% c('020', '151', '159'), routed_distance != Inf)
od_with_node_to_fill$routed_distance <- mcmapply(distance_generator_osm, 
                                                 od_with_node_to_fill$origin_FAF, od_with_node_to_fill$dest_FAF)
od_with_node_refilled <- rbind(od_with_node_filled, od_with_node_nofill, od_with_node_to_fill)
write_csv(od_with_node_refilled, 'FAF_od_with_distance_refilled.csv')


# test_route = route_osrm(st_coordinates(start_node), st_coordinates(end_node), osrm.profile = "car")
# print(0.000621371 * test_route$distance)
#find origin/destination node
network_nodes <- highway_network_sfn_simple %>%
  activate("nodes") %>%
  st_as_sf()



