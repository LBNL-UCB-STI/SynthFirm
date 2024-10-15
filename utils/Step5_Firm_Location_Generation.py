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



########################################################
#### step 1 - configure environment and load inputs ####
########################################################


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
    
def firm_location_generation(synthetic_firms_no_location_file,
                             synthetic_firms_with_location_file,
                             mesozone_to_faf_file,
                             zonal_output_file,
                             spatial_boundary_file, output_path):
    
    print("Generating synthetic firm locations...")
    # load inputs
    firms = read_csv(synthetic_firms_no_location_file, low_memory=False) # 8,396, 679 FIRMS
    mesozone_shapefile = gpd.read_file(spatial_boundary_file)
    mesozone_to_faf = read_csv(mesozone_to_faf_file)
    
    # <codecell>
    
    # remove remote island
    polygon = box(-170, 10, -66.96466, 71.365162)
    mesozone_shapefile = mesozone_shapefile.clip(polygon)
    # mesozone_shapefile.plot()
    
    # <codecell>
 
    list_of_mesozones = firms.MESOZONE.unique()
    
    firms.loc[:, 'lat'] = np.nan
    firms.loc[:, 'lon'] = np.nan
    firm_with_location = None
    
    map_crs = mesozone_shapefile.crs
    counter = 1
    for zone in list_of_mesozones:
        # print(zone)
        # if zone >= 20000 or zone <=3000:
        #     continue
        if counter % 100 == 0:
            print('processed ' + str(counter) + ' zones')
        region = mesozone_to_faf.loc[mesozone_to_faf['MESOZONE']== zone, 'FAFNAME'].values[0]
        region = str(region)
        # print(zone, region)
        counter += 1
        firm_selected = firms.loc[firms['MESOZONE'] == zone]
        sample_size = len(firm_selected)
        polygon = mesozone_shapefile.loc[mesozone_shapefile['MESOZONE'] == zone]
        if region == 'Rest of HI':
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
    print('number of firms without locations in first round:')
    print(len(firm_with_missing))
    firm_no_missing = firm_with_location.loc[~firm_with_location['geometry'].isna()]
    # try location generation again
    list_of_mesozones = firm_with_missing.MESOZONE.unique()
    
    firm_with_missing_location = None
    
    counter = 1
    for zone in list_of_mesozones:

    
        if counter % 100 == 0:
            print('processed ' + str(counter) + ' zones')
        region = mesozone_to_faf.loc[mesozone_to_faf['MESOZONE']== zone, 'FAFNAME']
        region = str(region)
        counter += 1
        firm_selected = firm_with_missing.loc[firm_with_missing['MESOZONE'] == zone]
        sample_size = len(firm_selected)
        polygon = mesozone_shapefile.loc[mesozone_shapefile['MESOZONE'] == zone]
        if region == 'Rest of HI':
            gdf_points = Random_Points_in_Bounds(polygon.geometry, 5000 * sample_size, map_crs)
        else:
            gdf_points = Random_Points_in_Bounds(polygon.geometry, 500 * sample_size, map_crs)
    
        Sjoin = gpd.tools.sjoin(gdf_points, polygon, predicate="within", how='left')
        # # Keep points in "myPoly"
        pnts_in_poly = gdf_points[Sjoin.MESOZONE == zone]
        pnts_in_poly = pnts_in_poly.head(sample_size)
        firm_selected = pd.concat([firm_selected.reset_index(), pnts_in_poly.reset_index()], axis = 1)
        firm_with_missing_location = pd.concat([firm_with_missing_location, firm_selected])
    
    
    print('missing value after re-generation:')
    print(firm_with_missing_location['geometry'].isna().sum())
    # <codecell>
    
    firm_with_location = pd.concat([firm_no_missing, firm_with_missing_location])
    print(len(firm_with_location))
    firm_with_location[['lat', 'lon']] = \
        firm_with_location.apply(lambda p: (p.geometry.y, p.geometry.x), axis=1, 
                                 result_type='expand')
        
    firm_with_location = firm_with_location.drop(columns = ['index', 'geometry'])
    
    # writing output
    mesozone_shapefile.to_file(zonal_output_file, 
                               driver="GeoJSON")
    
    firm_with_location.to_csv(os.path.join(output_path, 
                                           synthetic_firms_with_location_file), 
                              index = False)