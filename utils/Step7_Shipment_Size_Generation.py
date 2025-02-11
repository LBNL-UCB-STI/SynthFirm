#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  4 10:04:44 2022

@author: xiaodanxu
"""

import pandas as pd
import os
# import constants_sf as c
import numpy as np
from pandas import read_csv

# data_dir = '/Users/xiaodanxu/Documents/SynthFirm.nosync'
# os.chdir(data_dir)
# output_dir = 'outputs_SF/'

def split_dataframe(df, chunk_size = 10 ** 6): 
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    return chunks

def model_od_processor(data, mesozone_lookup, region_code):   # assign OD FAF zone ID
    data = pd.merge(data, mesozone_lookup, left_on = 'SellerZone', right_on = 'MESOZONE', how = 'left')
    data = data.rename(columns={"GEOID": "orig_GEOID", "CBPZONE": "orig_CBPZONE", 
                                                "MESOZONE":"orig_MESOZONE", "FAFID":"orig_FAFID",
                                               "FAFNAME":"orig_FAFNAME"})
    data = pd.merge(data, mesozone_lookup, left_on = 'BuyerZone', right_on = 'MESOZONE', how = 'left')
    data = data.rename(columns={"GEOID": "dest_GEOID", "CBPZONE": "dest_CBPZONE", 
                                                "MESOZONE":"dest_MESOZONE", "FAFID":"dest_FAFID",
                                               "FAFNAME":"dest_FAFNAME"})
    data = data.dropna(subset = ['orig_FAFID', 'dest_FAFID']) # remove shipment outside US
    if region_code is None:
        # print('this is a national study')
        data['in_study_area'] = 1
    else:
        data.loc[:, 'in_study_area'] = 0
        data.loc[:, 'in_study_area'] = 1 * (data.loc[:, 'orig_FAFID'].isin(region_code) | \
                                        data.loc[:, 'dest_FAFID'].isin(region_code))
    return(data)

###### processing b2b flow ##########
def shipment_size_generation(mesozone_to_faf_file, max_load_per_shipment_file, 
                             region_code, output_path):
    print('Split B2B flow by shipment size capacity')
    mesozone_lookup = read_csv(mesozone_to_faf_file, sep = ',')
    print(region_code)
    max_load_lookup = read_csv(max_load_per_shipment_file, sep = ',')
    mesozone_lookup['MESOZONE'] = mesozone_lookup['MESOZONE'].astype(np.int64)
    domestic_zones = mesozone_lookup['MESOZONE'].unique()
    output_dir = output_path
    chunk_size = 10 ** 5
    lb_to_ton = 0.0005
    # assign shipment size for each SCTG group
    for k in range(5):
        sctg = 'sctg' + str(k + 1)
        print('generate shipment size for SCTG group ' + sctg)
        # capacity_per_shipment = c.capacity_lookup[sctg]
        # max_ton_per_shipment = c.max_ton_lookup[sctg]
        filelist = [file for file in os.listdir(output_dir) if (file.startswith(sctg) & (file.endswith('.zip')))]
        combined_csv = pd.concat([read_csv(os.path.join(output_dir, f), low_memory=False) for f in filelist ])
        combined_csv = combined_csv.dropna()
        combined_csv['SellerZone'] = combined_csv['SellerZone'].astype(np.int64)
        combined_csv['BuyerZone'] = combined_csv['BuyerZone'].astype(np.int64)
        # print(combined_csv.SellerZone.head(5))
        # print(domestic_zones[0:5])
        if region_code != None:
            combined_csv = combined_csv.loc[combined_csv['SellerZone'].isin(domestic_zones)]
            combined_csv = combined_csv.loc[combined_csv['BuyerZone'].isin(domestic_zones)]

        
        combined_csv = model_od_processor(combined_csv, mesozone_lookup, region_code)
        
        combined_csv.loc[:, "TruckLoad"] *= lb_to_ton
        combined_csv = pd.merge(combined_csv, max_load_lookup, 
                                on = 'Commodity_SCTG', how = 'left')
        # combined_csv.loc[combined_csv["TruckLoad"] >= capacity_per_shipment, "TruckLoad"] = capacity_per_shipment
        combined_csv.loc[:, "ship_count"] = combined_csv.loc[:, "TruckLoad"] * 2000 / \
            combined_csv.loc[:, "SHIPMT_WGHT"] 
        combined_csv.loc[:, "ship_count"] = np.round(combined_csv.loc[:, "ship_count"], 0)
        combined_csv.loc[combined_csv["ship_count"] < 1, "ship_count"] = 1
        combined_csv = combined_csv.loc[combined_csv['in_study_area'] == 1]
        print(combined_csv.head(5))
        # sample_flow = combined_csv.sample(1000)
        chunks_of_flows = split_dataframe(combined_csv, chunk_size)
        q = 1
        
        for chunk in chunks_of_flows: 
            print('processing chunk ' + str(q))
            chunk_dup = pd.DataFrame(np.repeat(chunk.values, chunk.ship_count, axis=0))
            chunk_dup.columns = chunk.columns
            chunk_dup.loc[:, "TruckLoad"] = chunk_dup.loc[:, "TruckLoad"] / chunk_dup.loc[:, "ship_count"]
            chunk_dup = chunk_dup[['BuyerID', 'BuyerZone', 'BuyerNAICS', 'SellerID', 'SellerZone',
               'SellerNAICS', 'TruckLoad', 'SCTG_Group', 'orig_FAFID', 'dest_FAFID', 'Commodity_SCTG', 'UnitCost']]
            # print(chunk_dup.head(5))
            out_file_name = 'shipment_' + sctg + '_od' + str(q) + '.csv.zip'
            chunk_dup.to_csv(os.path.join(output_dir, out_file_name), index = False)
            q += 1
        # break

    print('end of shipping size generation')
    print('-------------------------------')
    
    return
