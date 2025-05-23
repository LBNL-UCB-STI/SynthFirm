#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 13 14:26:04 2023

@author: xiaodanxu
"""

import pandas as pd
import os
import numpy as np
from pandas import read_csv
import warnings
import geopandas as gpd
import shapely.wkt
from shapely.geometry import LineString

warnings.filterwarnings("ignore")

data_dir = 'C:/SynthFirm'
os.chdir(data_dir)
scenario_name = 'national'
out_scenario_name = 'national'
file_path = '/Users/xiaodanxu/Documents/SynthFirm.nosync'
parameter_dir = 'SynthFirm_parameters'

input_dir = 'inputs_' + scenario_name
output_dir = 'outputs_' + out_scenario_name
meter_to_mile = 0.000621371
### load input data
season_factor = read_csv(input_dir + '/temporal_distribution/' + 
                         'seasonal_allocation_factor_by_sctg.csv')
day_of_week_factor = read_csv(input_dir + '/temporal_distribution/' + 
                         'CA_time_of_week_distribution.csv')

day_of_week_factor = \
    day_of_week_factor.groupby(['dayofweek'])[['trip_count']].sum()
day_of_week_factor = day_of_week_factor.reset_index()
day_of_week_factor.loc[:, 'fraction'] = \
    day_of_week_factor.loc[:, 'trip_count']/ \
        day_of_week_factor.loc[:, 'trip_count'].sum()
# day_of_week_factor = day_of_week_factor.loc[day_of_week_factor['truck_type'] == 'HDT']

firm_location = read_csv(output_dir + '/synthetic_firms_with_location.csv', sep = ',')
firm_location = firm_location[['BusID', 'lat', 'lon']]
## assumptions
selected_quarter = 1
start_date = '2017-01-17' # tuesday
end_date = '2017-01-17' # thursday
days_of_q1 = pd.date_range(start='1/1/2017', end='3/31/2017', freq = 'D')
days_of_q1 = pd.DataFrame(
    [(day, day.weekday()) for day in days_of_q1], columns=('date', 'weekday'))
days_of_q1 = pd.merge(days_of_q1, day_of_week_factor,
                      left_on = 'weekday',
                      right_on = 'dayofweek', how = 'left')
days_of_q1 = days_of_q1[['date', 'fraction']]

def divide_chunks(l, n):
     
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]
# <codecell>
n = 10
for k in range(4):
    sctg = 'sctg' + str(k + 1)
    print('processing trips from ' + sctg)
    sctg_id = int(sctg[4])

    season_factor_selected = season_factor.loc[season_factor['SCTG_Group'] == sctg_id]
    out_dir_name = sctg + '_truck'
    truck_shipments_files = os.listdir(output_dir + '/' + out_dir_name)
    file_chunk = list(divide_chunks(truck_shipments_files, n))
    chunk_id = 0
    for chunk in file_chunk:
        daily_shipments = None
        chunk_id += 1
        print('process chunk ' + str(chunk_id))
        for file in chunk:
            if file == '.DS_Store':
                continue
            truck_shipments = read_csv(output_dir + '/' + out_dir_name + '/' + file)
            sample_size = len(truck_shipments)
            
            quarter_sample = season_factor_selected.sample(n = sample_size,
                               weights = season_factor_selected['weight_fraction'],
                               replace = True)
            quarter_sample.drop(columns = ['SCTG_Group', 'SCTG_Name'], inplace = True)
            truck_shipments = pd.concat([truck_shipments.reset_index(drop=True), 
                                  quarter_sample.reset_index(drop=True)], axis=1)
            truck_shipments = truck_shipments.drop(columns = 'weight_fraction')
            truck_shipments = truck_shipments.loc[truck_shipments['QUARTER'] == selected_quarter]
            sample_size_2 = len(truck_shipments)
            date_sample = days_of_q1.sample(n = sample_size_2,
                               weights = days_of_q1['fraction'],
                               replace = True)    
            truck_shipments = pd.concat([truck_shipments.reset_index(drop=True), 
                                  date_sample.reset_index(drop=True)], axis=1)  
            truck_shipments = truck_shipments.drop(columns = 'fraction')
            criteria = (truck_shipments['date'] >= start_date) & \
                (truck_shipments['date'] <= end_date)
            truck_shipments = truck_shipments.loc[criteria]
            truck_shipments.loc[:, 'date'] = truck_shipments.loc[:, 'date'].dt.date
            daily_shipments = pd.concat([daily_shipments, truck_shipments])
            
            # break
        # construct O-D desire line
        daily_shipments_with_loc = pd.merge(daily_shipments, firm_location,
                                            left_on = 'SellerID',
                                            right_on = 'BusID', how = 'left')
        daily_shipments_with_loc.loc[:, 'origin'] = \
            gpd.points_from_xy(daily_shipments_with_loc.lon, daily_shipments_with_loc.lat)
        daily_shipments_with_loc = \
            daily_shipments_with_loc.drop(columns=['BusID', 'lat', 'lon'])
        
        daily_shipments_with_loc = pd.merge(daily_shipments_with_loc, firm_location,
                                            left_on = 'BuyerID',
                                            right_on = 'BusID', how = 'left')
        daily_shipments_with_loc.loc[:, 'dest'] = \
            gpd.points_from_xy(daily_shipments_with_loc.lon, daily_shipments_with_loc.lat)
        daily_shipments_with_loc = \
            daily_shipments_with_loc.drop(columns=['BusID', 'lat', 'lon'])
        daily_shipments_with_loc.loc[:,'geometry'] = \
            daily_shipments_with_loc.apply(lambda row: LineString([row['origin'], row['dest']]), axis=1) 
        daily_shipments_with_loc = \
            gpd.GeoDataFrame(daily_shipments_with_loc, crs='epsg:4326')
        daily_shipments_with_loc = daily_shipments_with_loc.to_crs("EPSG:3310") # METER SYSTEM
    # in order to get length in meter, the shapefile need to re-projected to a coordinate system in meters (not required in R)
    # line_by_polygon.loc[:, 'Length'] = line_by_polygon.loc[:, 'geometry'].length
        daily_shipments_with_loc.loc[:, 'length'] = \
            daily_shipments_with_loc.loc[:,'geometry'].length * meter_to_mile # IN MILES
        daily_shipments_with_loc = daily_shipments_with_loc.to_crs("EPSG:4326")
        # print(truck_shipments_files)
        daily_shipments_with_loc.to_csv(output_dir + '/processed_shipment/' + 'daily_shipments_' + sctg + '_' + str(chunk_id) + '.zip',
                                index = False)
        print('total selected shipments ' + str(len(daily_shipments)))
    #     break
    # break

# <codecell>

# combine all output
# combine parquet file
# for state in select_state:
result_dir = output_dir + '/processed_shipment/'
shipment_list = [file for file in os.listdir(result_dir) if file.endswith('.zip')]
shipment_df = pd.concat((pd.read_csv(os.path.join(result_dir, f)) \
                      for f in shipment_list), ignore_index=True)
out_file_name = 'daily_shipment_sample.csv.zip'
shipment_df.to_csv(os.path.join(output_dir, out_file_name))