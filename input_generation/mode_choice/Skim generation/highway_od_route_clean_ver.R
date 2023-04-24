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

# node_path = paths %>%
#   slice(1) %>%
#   pull(node_paths) %>%
#   unlist()
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

#### GENERATE OD route
origin_node <- freight_centroid[freight_centroid$FAF == '064',]
route_id <- 1
for (row in 1:nrow(freight_centroid)) {
  print(row)
  dest_node <- freight_centroid[row,]
  if (dest_node$FAF %in% c('020', '064', '151', '159')){
    next
  }
  paths = st_network_paths(highway_network_sfn_subdivision, from = origin_node, to = dest_node)
  paths_sf = highway_network_sfn_subdivision %>%
    activate("edges") %>%
    slice(unlist(paths$edge_paths)) %>%
    st_as_sf()
  paths_sf <- st_geometry(paths_sf)
  #print(paths_sf)

  if (length(paths_sf) == 0) {
    print('fill path...')
    start_node_coordinate <- st_coordinates(origin_node)
    end_node_coordinate <- st_coordinates(dest_node)
    paths_sf <- route_osrm(start_node_coordinate, end_node_coordinate, osrm.profile = "car")
    paths_sf <- st_transform(paths_sf, crs = network_crs)
    paths_sf <- paths_sf$geometry
    #break
  }
  paths_sf <- st_sf(paths_sf)
  paths_sf$route_id <- route_id
  paths_sf$origin_faf <- as.numeric(origin_node$FAF)
  paths_sf$dest_faf <- as.numeric(dest_node$FAF)
  
  if (route_id == 1){
    sf_route <- paths_sf
  }else{
    sf_route <- rbind(sf_route, paths_sf)
  }
  route_id <- route_id + 1
  #break
  }
plot(st_geometry(sf_route))
plot(st_geometry(freight_centroid), add = TRUE, col = 'red')

st_write(sf_route, "route/sf_departed_routes_v2.shp")
st_write(freight_centroid, "route/faf_centroids.shp")

st_write(sf_route, "sf_departed_routes.geojson")
st_write(freight_centroid, "faf_centroids.geojson")



