#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 17 10:59:33 2023

@author: xiaodanxu
"""

import pandas as pd
import os
from os import listdir
import constants as c
import numpy as np
from pandas import read_csv

#define input
data_dir = '/Users/xiaodanxu/Documents/SynthFirm.nosync'
os.chdir(data_dir)
truck_allocation_factor = read_csv(c.input_dir + 'truck_allocation_factor.csv')
hour_factor = read_csv(c.input_dir + 'temporal_distribution/CA_hour_factor.csv')
avg_payload = {'SUT': 4.95, 'CT': 16.11}

nbreaks = truck_allocation_factor['Maximum Range'].tolist()
nbreaks = np.insert(nbreaks, 0, -1)
nlabels = truck_allocation_factor['Maximum Range'].tolist()
                
shipment_list = listdir(c.output_dir + 'processed_shipment')
selected_date = '2017-01-17'
final_output = None
for file in shipment_list:
    if file == '.DS_Store':
        continue
    print('processing shipment file ' + file)
    # assign distance bin to each shipment for vehicle assignment
    shipment_input = read_csv(c.output_dir + 'processed_shipment/' + file, low_memory = False)
    shipment_input = shipment_input.loc[shipment_input['date'] == selected_date]
    shipment_input.loc[:, 'distance_bin'] = pd.cut(shipment_input.loc[:, 'length'], 
                                                   bins = nbreaks, labels = nlabels, 
                                                   right = True)
    
    # assign vehicle type based on distance bin
    shipment_out = None
    
    for label in nlabels:
        shipment_selected = shipment_input.loc[shipment_input['distance_bin'] == label].reset_index()  
        sample_size = len(shipment_selected)
        probability_sut = \
            float(truck_allocation_factor.loc[truck_allocation_factor['Maximum Range'] == label, 'SUT'])
        probability_ct = 1 - probability_sut
        shipment_selected.loc[:, 'veh_type'] = \
        pd.Series(np.random.choice(['SUT', 'CT'], size = sample_size, 
                                   p = [probability_sut, probability_ct]) )
        shipment_out = pd.concat([shipment_out, shipment_selected])
        
        # break
    
    # split large shipments to fit the vehicle capacity
    shipment_out.loc[:, 'veh_capacity'] = shipment_out.loc[:, 'veh_type'].map(avg_payload)
    shipment_out.loc[:, 'n_shipment'] = shipment_out.loc[:, 'TruckLoad'] / \
    shipment_out.loc[:, 'veh_capacity']
    shipment_out.loc[:, 'n_shipment'] = np.ceil(shipment_out.loc[:, 'n_shipment'])
    # print(len(shipment_out))
    # print(shipment_out.n_shipment.sum())
    col_names = shipment_out.columns
    shipment_out = pd.DataFrame(np.repeat(shipment_out.values, shipment_out.n_shipment, axis=0))
    shipment_out.columns = col_names
    # print(len(shipment_out))
    shipment_out.loc[:, 'TruckLoad'] = \
        shipment_out.loc[:, 'TruckLoad'] / shipment_out.loc[:, 'n_shipment']
    shipment_out.loc[:, 'vc_ratio'] = \
        shipment_out.loc[:, 'TruckLoad'] / shipment_out.loc[:, 'veh_capacity']
    
    # consolidate small shipments
    shipment_to_pair = shipment_out.loc[shipment_out['vc_ratio'] < 0.5]
    shipment_no_pair = shipment_out.loc[shipment_out['vc_ratio'] >= 0.5]
    shipment_cluster = \
        shipment_to_pair.groupby(['BuyerZone', 'SellerZone', 'SCTG_Group', 'veh_type']).agg({'shipment_id':'count', 
                         'TruckLoad':'sum',  'veh_capacity': 'mean'})
    shipment_cluster = shipment_cluster.reset_index()
    shipment_cluster.loc[:,'cluster_id'] = shipment_cluster.reset_index().index + 1
    shipment_cluster = shipment_cluster[['BuyerZone', 'SellerZone', 'SCTG_Group', 'veh_type', 'cluster_id']]
    shipment_to_pair = pd.merge(shipment_to_pair,
                                shipment_cluster,
                                on = ['BuyerZone', 'SellerZone', 'SCTG_Group', 'veh_type'],
                                how = 'left')
    shipment_to_pair.loc[:, 'vc_ratio'] = shipment_to_pair.loc[:, 'vc_ratio'].astype(float)
    shipment_to_pair.loc[:, 'bundle_id'] = \
        shipment_to_pair.groupby(['cluster_id'])['vc_ratio'].cumsum()
    shipment_to_pair.loc[:, 'bundle_id'] = np.ceil(shipment_to_pair.loc[:, 'bundle_id'])
    shipment_to_pair.loc[:, 'TruckLoad'] = \
        shipment_to_pair.groupby(['cluster_id', 'bundle_id'])['TruckLoad'].transform('sum')
    # print(len(shipment_to_pair))
    shipment_to_pair = shipment_to_pair.drop_duplicates(subset = ['cluster_id', 'bundle_id'],
                                                        keep = 'first')
    # print(len(shipment_to_pair))
    shipment_to_pair = shipment_to_pair.drop(columns = ['cluster_id', 'bundle_id'])
    
    shipment_out = pd.concat([shipment_no_pair, shipment_to_pair])
    shipment_out = shipment_out.drop(columns = ['SCTG_Group.1', 'distance_bin', 'n_shipment', 'vc_ratio'])
    final_output = pd.concat([final_output, shipment_out])
    # break
vehicle_types = ['SUT', 'CT']
final_output_with_time = None
for vt in  vehicle_types:
    hour_factor_selected = hour_factor.loc[hour_factor['veh_type'] == vt]
    final_output_seleced = final_output.loc[final_output['veh_type'] == vt].reset_index()
    sample_size = len(final_output_seleced)
    hour_sample = hour_factor_selected.sample(n = sample_size,
                                    weights = hour_factor_selected['fraction'],
                                    replace = True)    
    hour_sample = hour_sample[['start_hour']]
    final_output_seleced = pd.concat([final_output_seleced.reset_index(drop=True), 
                                  hour_sample.reset_index(drop=True)], axis=1)  
    final_output_with_time = pd.concat([final_output_with_time, final_output_seleced])

# <codecell>
final_output_with_time = final_output_with_time.drop(columns = ['level_0', 'index', ])
final_output_with_time.to_csv(c.output_dir + 'processed_trips/daily_trips_by_vehicle_type.csv', index = False)