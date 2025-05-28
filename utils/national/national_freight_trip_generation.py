#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 17 10:59:33 2023

@author: xiaodanxu
"""

import pandas as pd
import os
from os import listdir
# import constants as c
import numpy as np
from pandas import read_csv
import warnings

#define paths
warnings.filterwarnings("ignore")

data_dir = 'C:/SynthFirm'
os.chdir(data_dir)
scenario_name = 'national'
out_scenario_name = 'national'
file_path = '/Users/xiaodanxu/Documents/SynthFirm.nosync'
parameter_dir = 'SynthFirm_parameters'

input_dir = 'inputs_' + scenario_name
output_dir = 'outputs_' + out_scenario_name


#define inputs

##  truck types
truck_allocation_factor =\
    read_csv(os.path.join(input_dir, 'freight_trip', 'veh_assignment_by_ro_bin.csv'))

## payload factor and fraction of empty trips
avg_payload_and_empty_trips = \
    read_csv(os.path.join(input_dir, 'freight_trip', 'avg_payload_and_deadheading.csv'))

## time of day factor
hour_factor = \
    read_csv(os.path.join(input_dir, 'temporal_distribution', 'CA_hour_factor.csv'))
# avg_payload = {'SUT': 4.95, 'CT': 16.11}


## daily shipments
out_file_name = 'daily_shipment_sample.csv.zip'
shipment_input = read_csv(os.path.join(output_dir, out_file_name))

# define model parameters
nbreaks = truck_allocation_factor['Maximum Range'].tolist()
nbreaks = np.insert(nbreaks, 0, -1)
nlabels = truck_allocation_factor['range_bin'].tolist()

truck_mapping = {'HDT tractor': 'HDT',	
                 'HDT vocational': 'HDT',	
                 'MDT vocational': 'MDT'}

truck_cap_level = {'HDT tractor': 3,	
                 'HDT vocational': 2,	
                 'MDT vocational': 1}
                
selected_date = '2017-01-17'

# <codecell>
from sklearn.utils import shuffle
shipment_input = shuffle(shipment_input)

def split_dataframe(df, chunk_size = 10 ** 6): 
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    return chunks

chunk_size = 10 ** 5
chunks_of_shipments = split_dataframe(shipment_input, chunk_size)
cleaned_output = None
q = 1

veh_types = list(truck_mapping.keys())
# assign range bin and vehicle type
for chunk in chunks_of_shipments: 
    print('Processing batch ' + str(q))
    chunk = chunk.loc[chunk['date'] == selected_date]
    chunk.loc[:, 'distance_bin'] = pd.cut(chunk.loc[:, 'length'], 
                                          bins = nbreaks, labels = nlabels,
                                          right = True)
    print('Average shipping distance:')
    print(chunk['length'].mean())
    # assign vehicle type based on distance bin
    shipment_out = None
    
    for label in nlabels:
        shipment_selected = chunk.loc[chunk['distance_bin'] == label].reset_index()  
        sample_size = len(shipment_selected)
        probabilities = \
            truck_allocation_factor.loc[truck_allocation_factor['range_bin'] == label, veh_types].to_numpy()[0]
        # print(probabilities)
        samples = np.random.choice(veh_types, size = sample_size, 
                                    p = probabilities) 
        truck_sample = pd.DataFrame({'veh_class': samples})
        truck_sample.loc[:, 'cap_ranking'] = \
            truck_sample.loc[:, 'veh_class'].map(truck_cap_level)
        truck_sample = truck_sample.sort_values(by = ['cap_ranking'], ascending = True)
        shipment_selected = shipment_selected.sort_values(by = ['TruckLoad'], ascending = True)
        
        truck_sample = truck_sample[['veh_class']]
        
        shipment_selected = pd.concat([shipment_selected.reset_index(drop=True), 
                                      truck_sample.reset_index(drop=True)], axis=1)
        shipment_out = pd.concat([shipment_out, shipment_selected])
    
    # assign payload capacity
    shipment_out = pd.merge(shipment_out, 
                            avg_payload_and_empty_trips,
                            on = ['SCTG_Group', 'veh_class'], 
                            how = 'left')
   
    shipment_out.loc[:, 'n_shipment'] = shipment_out.loc[:, 'TruckLoad'] / \
    shipment_out.loc[:, 'payload']
    shipment_out.loc[:, 'n_shipment'] = np.ceil(shipment_out.loc[:, 'n_shipment'])
    
    # print(shipment_out.n_shipment.sum())
    col_names = shipment_out.columns
    shipment_out = pd.DataFrame(np.repeat(shipment_out.values, shipment_out.n_shipment, axis=0))
    shipment_out.columns = col_names
    print(len(shipment_out))
    
    shipment_out.loc[:, 'TruckLoad'] = \
        shipment_out.loc[:, 'TruckLoad'] / shipment_out.loc[:, 'n_shipment']
    shipment_out.loc[:, 'vc_ratio'] = \
        shipment_out.loc[:, 'TruckLoad'] / shipment_out.loc[:, 'payload']
    shipment_out.drop(columns = ['n_shipment'], inplace = True)
    
    cleaned_output = pd.concat([cleaned_output, shipment_out])
    q+=1
    # break
print('Total shipment after split by payload...')
print(len(cleaned_output))

print('Shipment by class after split by payload...')
print(cleaned_output.groupby('veh_class').size())
# 13.6 million
# <codecell>

# consolidate small shipments with same O-D

threshold_val = 0.8
shipment_to_pair = cleaned_output.loc[cleaned_output['vc_ratio'] < threshold_val]
# 4.0 million
shipment_no_pair = cleaned_output.loc[cleaned_output['vc_ratio'] >= threshold_val]
# 9.6 million

# <codecell>

# create pool of shipments that can be consolidated
shipment_cluster = \
    shipment_to_pair.groupby(['BuyerZone', 'SellerZone', 'SCTG_Group', 'veh_class']).agg({'shipment_id':'count', 
                      'TruckLoad':'sum',  'payload': 'mean'})
shipment_cluster = shipment_cluster.reset_index()
shipment_cluster.loc[:,'cluster_id'] = shipment_cluster.reset_index().index + 1
shipment_cluster = shipment_cluster[['BuyerZone', 'SellerZone', 'SCTG_Group', 'veh_class', 'cluster_id']]

# <codecell>

# fit small shipments into payload
shipment_to_pair = pd.merge(shipment_to_pair,
                            shipment_cluster,
                            on = ['BuyerZone', 'SellerZone', 'SCTG_Group', 'veh_class'],
                            how = 'left')
shipment_to_pair.loc[:, 'vc_ratio'] = shipment_to_pair.loc[:, 'vc_ratio'].astype(float)
shipment_to_pair.loc[:, 'bundle_id'] = \
    shipment_to_pair.groupby(['cluster_id'])['vc_ratio'].cumsum()
shipment_to_pair.loc[:, 'bundle_id'] = np.ceil(shipment_to_pair.loc[:, 'bundle_id'])
shipment_to_pair.loc[:, 'TruckLoad'] = \
    shipment_to_pair.groupby(['cluster_id', 'bundle_id'])['TruckLoad'].transform('sum')

shipment_to_pair = shipment_to_pair.drop_duplicates(subset = ['cluster_id', 'bundle_id'],
                                                    keep = 'first')

shipment_to_pair = shipment_to_pair.drop(columns = ['cluster_id', 'bundle_id'])

print('Number of shipment being consolidated:')
print(len(shipment_to_pair))
# 1.34 million after pairing
# <codecell>
import geopandas as gpd
import shapely.wkt
from shapely.geometry import LineString

shipment_out = pd.concat([shipment_no_pair, shipment_to_pair])
shipment_out.drop(columns = ['index', 'Unnamed: 0'], inplace = True)
print('Number of shipment after consolidation:')
print(len(shipment_out))
shipment_out['geometry'] = shipment_out['geometry'].apply(shapely.wkt.loads)
shipment_out = gpd.GeoDataFrame(shipment_out, geometry='geometry', crs='epsg:4326')
# 10 million

# sample empty trips
shipment_out.loc[:, 'Deadheading'] *=0.01
shipment_out.loc[:, 'rand'] = \
    np.random.uniform(0, 1, size=len(shipment_out))
shipment_out.loc[:, 'empty_ind'] = 0
shipment_out.loc[(shipment_out['rand'] < shipment_out['Deadheading']), 'empty_ind'] = 1

empty_trips = shipment_out.loc[shipment_out['empty_ind'] == 1]
#2.5 million
# <codecell>


empty_trips.rename(columns = {'orig_FAFID': 'dest_FAFID', 
                              'dest_FAFID': 'orig_FAFID',
                              'origin': 'dest', 
                              'dest': 'origin'}, inplace = True)

empty_trips.loc[:, 'TruckLoad'] = 0.0
# recreate line string after flip O-D
empty_trips.loc[:,'geometry'] = \
    empty_trips['geometry'].apply(lambda x: LineString(list(reversed(x.coords)))) 

attr_to_keep = ['BuyerID', 'BuyerZone', 'BuyerNAICS', 'SellerID', 'SellerZone',
       'SellerNAICS', 'TruckLoad', 'Commodity_SCTG', 'SCTG_Group',
       'shipment_id', 'orig_FAFID', 'dest_FAFID', 'mode_choice', 'probability',
       'Distance', 'Travel_time', 'QUARTER', 'date', 'origin', 'dest',
       'geometry', 'length', 'distance_bin', 'veh_class', 'payload', 'empty_ind']

cleaned_output = pd.concat([shipment_out[attr_to_keep], 
                            empty_trips[attr_to_keep]])

# <codecell>
# add departure time

final_output_with_time = None
for vt in veh_types:
    vt_short = truck_mapping[vt]
    hour_factor_selected = hour_factor.loc[hour_factor['veh_type'] == vt_short]
    final_output_seleced = \
        cleaned_output.loc[cleaned_output['veh_class'] == vt].reset_index()
    sample_size = len(final_output_seleced)
    hour_sample = hour_factor_selected.sample(n = sample_size,
                                    weights = hour_factor_selected['fraction'],
                                    replace = True)    
    hour_sample = hour_sample[['start_hour']]
    final_output_seleced = pd.concat([final_output_seleced.reset_index(drop=True), 
                                  hour_sample.reset_index(drop=True)], axis=1)  
    final_output_with_time = pd.concat([final_output_with_time, final_output_seleced])

# 13.5 million
# <codecell>
final_output_with_time = final_output_with_time.drop(columns = ['index', ])
final_output_with_time.to_csv(os.path.join(output_dir, 'daily_trips_by_vehicle_type.csv.zip'), index = False)
# <codecell>
# for file in shipment_list:
#     if file == '.DS_Store':
#         continue
#     print('processing shipment file ' + file)
#     # assign distance bin to each shipment for vehicle assignment
#     shipment_input = read_csv(c.output_dir + 'processed_shipment/' + file, low_memory = False)
#     shipment_input = shipment_input.loc[shipment_input['date'] == selected_date]
#     shipment_input.loc[:, 'distance_bin'] = pd.cut(shipment_input.loc[:, 'length'], 
#                                                    bins = nbreaks, labels = nlabels, 
#                                                    right = True)
    

        
#         # break
    
#     # split large shipments to fit the vehicle capacity
#     shipment_out.loc[:, 'veh_capacity'] = shipment_out.loc[:, 'veh_type'].map(avg_payload)
#     shipment_out.loc[:, 'n_shipment'] = shipment_out.loc[:, 'TruckLoad'] / \
#     shipment_out.loc[:, 'veh_capacity']
#     shipment_out.loc[:, 'n_shipment'] = np.ceil(shipment_out.loc[:, 'n_shipment'])
#     # print(len(shipment_out))
#     # print(shipment_out.n_shipment.sum())
#     col_names = shipment_out.columns
#     shipment_out = pd.DataFrame(np.repeat(shipment_out.values, shipment_out.n_shipment, axis=0))
#     shipment_out.columns = col_names
#     # print(len(shipment_out))
#     shipment_out.loc[:, 'TruckLoad'] = \
#         shipment_out.loc[:, 'TruckLoad'] / shipment_out.loc[:, 'n_shipment']
#     shipment_out.loc[:, 'vc_ratio'] = \
#         shipment_out.loc[:, 'TruckLoad'] / shipment_out.loc[:, 'veh_capacity']
    
#     # consolidate small shipments
#     shipment_to_pair = shipment_out.loc[shipment_out['vc_ratio'] < 0.5]
#     shipment_no_pair = shipment_out.loc[shipment_out['vc_ratio'] >= 0.5]
#     shipment_cluster = \
#         shipment_to_pair.groupby(['BuyerZone', 'SellerZone', 'SCTG_Group', 'veh_type']).agg({'shipment_id':'count', 
#                          'TruckLoad':'sum',  'veh_capacity': 'mean'})
#     shipment_cluster = shipment_cluster.reset_index()
#     shipment_cluster.loc[:,'cluster_id'] = shipment_cluster.reset_index().index + 1
#     shipment_cluster = shipment_cluster[['BuyerZone', 'SellerZone', 'SCTG_Group', 'veh_type', 'cluster_id']]
#     shipment_to_pair = pd.merge(shipment_to_pair,
#                                 shipment_cluster,
#                                 on = ['BuyerZone', 'SellerZone', 'SCTG_Group', 'veh_type'],
#                                 how = 'left')
#     shipment_to_pair.loc[:, 'vc_ratio'] = shipment_to_pair.loc[:, 'vc_ratio'].astype(float)
#     shipment_to_pair.loc[:, 'bundle_id'] = \
#         shipment_to_pair.groupby(['cluster_id'])['vc_ratio'].cumsum()
#     shipment_to_pair.loc[:, 'bundle_id'] = np.ceil(shipment_to_pair.loc[:, 'bundle_id'])
#     shipment_to_pair.loc[:, 'TruckLoad'] = \
#         shipment_to_pair.groupby(['cluster_id', 'bundle_id'])['TruckLoad'].transform('sum')
#     # print(len(shipment_to_pair))
#     shipment_to_pair = shipment_to_pair.drop_duplicates(subset = ['cluster_id', 'bundle_id'],
#                                                         keep = 'first')
#     # print(len(shipment_to_pair))
#     shipment_to_pair = shipment_to_pair.drop(columns = ['cluster_id', 'bundle_id'])
    
#     shipment_out = pd.concat([shipment_no_pair, shipment_to_pair])
#     shipment_out = shipment_out.drop(columns = ['SCTG_Group.1', 'distance_bin', 'n_shipment', 'vc_ratio'])
#     final_output = pd.concat([final_output, shipment_out])
#     # break
# vehicle_types = ['SUT', 'CT']
# final_output_with_time = None
# for vt in vehicle_types:
#     hour_factor_selected = hour_factor.loc[hour_factor['veh_type'] == vt]
#     final_output_seleced = final_output.loc[final_output['veh_type'] == vt].reset_index()
#     sample_size = len(final_output_seleced)
#     hour_sample = hour_factor_selected.sample(n = sample_size,
#                                     weights = hour_factor_selected['fraction'],
#                                     replace = True)    
#     hour_sample = hour_sample[['start_hour']]
#     final_output_seleced = pd.concat([final_output_seleced.reset_index(drop=True), 
#                                   hour_sample.reset_index(drop=True)], axis=1)  
#     final_output_with_time = pd.concat([final_output_with_time, final_output_seleced])

# # <codecell>
# final_output_with_time = final_output_with_time.drop(columns = ['level_0', 'index', ])
# final_output_with_time.to_csv(c.output_dir + 'processed_trips/daily_trips_by_vehicle_type.csv', index = False)