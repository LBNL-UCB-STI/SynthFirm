#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 14 10:36:11 2024

@author: xiaodanxu
"""

import pandas as pd
import os
import numpy as np
from pandas import read_csv
import geopandas as gpd
# from multiprocessing import Pool
# import time
from shapely.geometry import box, Point, Polygon
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings("ignore")

print("Generating synthetic firm locations...")

########################################################
#### step 1 - configure environment and load inputs ####
########################################################

scenario_name = 'Seattle'
out_scenario_name = 'Seattle'
file_path = '/Users/xiaodanxu/Documents/SynthFirm.nosync'
parameter_dir = 'SynthFirm_parameters'
input_dir = 'inputs_' + scenario_name
output_dir = 'outputs_' + out_scenario_name

# specifications that has been defined before
synthetic_firms_no_location_file = "synthetic_firms.csv" 

# specifications that are new to this code
synthetic_firms_with_location_file = "synthetic_firms_with_location.csv" # synthetic firm output (after location assignment)
spatial_boundary_file = scenario_name + '_freight.geojson'

# load inputs
firms = read_csv(os.path.join(file_path, output_dir, synthetic_firms_no_location_file), low_memory=False) # 8,396, 679 FIRMS
mesozone_shapefile = gpd.read_file(os.path.join(file_path, input_dir, spatial_boundary_file))

# <codecell>

# remove remote island
polygon = box(-170, 10, -66.96466, 71.365162)
mesozone_shapefile = mesozone_shapefile.clip(polygon)
# mesozone_shapefile.plot()

# <codecell>


def Random_Points_in_Bounds(polygon, number, map_crs):   
    bounds = polygon.bounds
    x = np.random.uniform( float(bounds['minx']), float(bounds['maxx']), number )
    y = np.random.uniform( float(bounds['miny']), float(bounds['maxy']), number )
    df = pd.DataFrame()
    df['geometry'] = list(zip(x,y))
    df['geometry'] = df['geometry'].apply(Point)
    gdf_points = gpd.GeoDataFrame(df, geometry='geometry')
    gdf_points = gdf_points.set_crs(map_crs)
    return gdf_points

list_of_mesozones = firms.MESOZONE.unique()

firms.loc[:, 'lat'] = np.nan
firms.loc[:, 'lon'] = np.nan
firm_with_location = None

map_crs = mesozone_shapefile.crs
counter = 1
for zone in list_of_mesozones:
    if counter % 100 == 0:
        print('processed ' + str(counter) + ' zones')
    counter += 1
    firm_selected = firms.loc[firms['MESOZONE'] == zone]
    sample_size = len(firm_selected)
    polygon = mesozone_shapefile.loc[mesozone_shapefile['MESOZONE'] == zone]
    if zone == 20031:
        gdf_points = Random_Points_in_Bounds(polygon.geometry, 100 * sample_size, map_crs)
    else:
        gdf_points = Random_Points_in_Bounds(polygon.geometry, 5 * sample_size, map_crs)

    Sjoin = gpd.tools.sjoin(gdf_points, polygon, predicate="within", how='left')
    # # Keep points in "myPoly"
    pnts_in_poly = gdf_points[Sjoin.MESOZONE == zone]
    pnts_in_poly = pnts_in_poly.head(sample_size)
    firm_selected = pd.concat([firm_selected.reset_index(), pnts_in_poly.reset_index()], axis = 1)
    firm_with_location = pd.concat([firm_with_location, firm_selected])
    # ax1 = pnts_in_poly.plot(alpha = 0.1, markersize = 0.1)
    # polygon.plot(ax = ax1, facecolor='none', edgecolor='k',linewidth = 0.5)
    # plt.show()
    # break

# <codecell>

# for firms with no location generated, try it again
firm_with_missing = firm_with_location.loc[firm_with_location['geometry'].isna()]
firm_with_missing = firm_with_missing.drop(columns = ['index', 'geometry'])
firm_no_missing = firm_with_location.loc[~firm_with_location['geometry'].isna()]
# try location generation again
list_of_mesozones = firm_with_missing.MESOZONE.unique()

firm_with_missing_location = None

counter = 1
for zone in list_of_mesozones:
    if counter % 100 == 0:
        print('processed ' + str(counter) + ' zones')
    counter += 1
    firm_selected = firm_with_missing.loc[firm_with_missing['MESOZONE'] == zone]
    sample_size = len(firm_selected)
    polygon = mesozone_shapefile.loc[mesozone_shapefile['MESOZONE'] == zone]
    if zone == 20031:
        gdf_points = Random_Points_in_Bounds(polygon.geometry, 500 * sample_size, map_crs)
    else:
        gdf_points = Random_Points_in_Bounds(polygon.geometry, 50 * sample_size, map_crs)

    Sjoin = gpd.tools.sjoin(gdf_points, polygon, predicate="within", how='left')
    # # Keep points in "myPoly"
    pnts_in_poly = gdf_points[Sjoin.MESOZONE == zone]
    pnts_in_poly = pnts_in_poly.head(sample_size)
    firm_selected = pd.concat([firm_selected.reset_index(), pnts_in_poly.reset_index()], axis = 1)
    firm_with_missing_location = pd.concat([firm_with_missing_location, firm_selected])

# <codecell>

firm_with_location = pd.concat([firm_no_missing, firm_with_missing_location])
print(len(firm_with_location))
firm_with_location[['lat', 'lon']] = \
    firm_with_location.apply(lambda p: (p.geometry.y, p.geometry.x), axis=1, 
                             result_type='expand')
    
firm_with_location = firm_with_location.drop(columns = ['index', 'geometry'])

# writing output
zonal_output_file = scenario_name + '_freight_no_island.geojson'
mesozone_shapefile.to_file(os.path.join(file_path, input_dir, zonal_output_file), 
                           driver="GeoJSON")

firm_with_location.to_csv(os.path.join(file_path, output_dir, 
                                       synthetic_firms_with_location_file), 
                          index = False)