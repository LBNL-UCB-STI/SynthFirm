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
  "/Users/xiaodanxu/Documents/SynthFirm.nosync/inputs_Seattle"
setwd(path2file)

f1 = sf::st_read("Seattle_freight_centroids.geojson")

require(sp)

origin_node <- f1[, c('MESOZONE')] %>% st_drop_geometry()
dest_node <- f1[, c('MESOZONE')] %>% st_drop_geometry()
colnames(origin_node) <- c('origin_mesozone')
colnames(dest_node) <- c('dest_mesozone')
od_with_node <- expand_grid(origin_node, dest_node)


# distance_generator <- function(ozone, dzone){
#   start_node <- f1[f1$MESOZONE == ozone,]
#   end_node <- f1[f1$MESOZONE == dzone,]
#   output <- st_distance(start_node, end_node) * 0.000621371
#   return(output)
# }
# sample_od_with_node <- od_with_node[0:1000,]
# od_with_node$DistMiles <- mcmapply(distance_generator, 
#                                    od_with_node$origin_mesozone, od_with_node$dest_mesozone)

# sample_points <- f1[1:5,]
centroid_distance <- st_distance(f1, f1) * 0.000621371
centroid_distance <- as.data.table(centroid_distance)
colnames(centroid_distance) <- as.character(f1$MESOZONE)
rownames(centroid_distance) <- as.character(f1$MESOZONE)

centroid_distance$id <- as.character(f1$MESOZONE)
centroid_distance_long = reshape2::melt(centroid_distance, id.vars = c("id"))
names(centroid_distance_long) = c("OriginZone","DestinationZone","DistMiles")
# f2 = f2 %>% filter(DistMiles > 0)
#Function to convert X & Y /LAT & LONG coordinates into UTM for distance computations.
# LongLatToUTM <- function(x, y, zone) {
#   xy <- data.frame(ID = 1:length(x),
#                    X = x,
#                    Y = y)
#   coordinates(xy) <- c("X", "Y")
#   proj4string(xy) <- CRS("+proj=longlat +datum=WGS84")
#   res <-
#     spTransform(xy, CRS(paste(
#       "+proj=utm +zone=", zone, " ellps=WGS84", sep = ''
#     )))
#   return(as.data.frame(res))
# }
# 
# centroids <-
#   LongLatToUTM(f1$X_cord, f1$Y_cord, 16) #FOR NEWYORK/NEW JERSEY/PENN regions it is UTM zone 20.
# 
# xxW = dist(cbind(centroids$X, centroids$Y))
# m2miles <- 0.000621371 # meters to miles coversion factor
# xxW = xxW*m2miles
# xxW = as.matrix(xxW)
# rownames(xxW) = f1$MESOZONE
# colnames(xxW) = f1$MESOZONE
# f2 = reshape2::melt(xxW)
# names(f2) = c("OriginZone","DestinationZone","DistMiles")
# f2 = f2 %>% filter(DistMiles > 0)
data.table::fwrite(centroid_distance_long, "Seattle_od_dist.csv")
