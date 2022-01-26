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
set.seed(0)
path2file <-
  "/Users/srinath/OneDrive - LBNL/Projects/SMART-2.0/Task-3 BAMOS/BayArea_GIS"
setwd(path2file)

f1 = sf::st_read("freight_centroids.geojson")

require(sp)
#Function to convert X & Y /LAT & LONG coordinates into UTM for distance computations.
LongLatToUTM <- function(x, y, zone) {
  xy <- data.frame(ID = 1:length(x),
                   X = x,
                   Y = y)
  coordinates(xy) <- c("X", "Y")
  proj4string(xy) <- CRS("+proj=longlat +datum=WGS84")
  res <-
    spTransform(xy, CRS(paste(
      "+proj=utm +zone=", zone, " ellps=WGS84", sep = ''
    )))
  return(as.data.frame(res))
}

centroids <-
  LongLatToUTM(f1$X_cord, f1$Y_cord, 16) #FOR NEWYORK/NEW JERSEY/PENN regions it is UTM zone 20.

xxW = dist(cbind(centroids$X, centroids$Y))
m2miles <- 0.000621371 # meters to miles coversion factor
xxW = xxW*m2miles
xxW = as.matrix(xxW)
rownames(xxW) = f1$MESOZONE
colnames(xxW) = f1$MESOZONE
f2 = reshape2::melt(xxW)
names(f2) = c("OriginZone","DestinationZone","DistMiles")
f2 = f2 %>% filter(DistMiles > 0)
data.table::fwrite(f2, "od_dist.csv")
