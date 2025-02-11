#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov  3 10:41:38 2022

@author: xiaodanxu
"""

import os
from pandas import read_csv
import pandas as pd
import geopandas as gps
import matplotlib.pyplot as plt
import seaborn as sns
from os import listdir
import numpy as np
# import constants_sf as c
import warnings
warnings.filterwarnings("ignore")

# os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')
def post_mode_choice(sctg_group_file, mesozone_to_faf_file, 
                     output_path, region_code):
    
    print('post process mode choice results and generate truck flows...')
    sctg_group_lookup = read_csv(sctg_group_file, sep = ',')
    mesozone_lookup = read_csv(mesozone_to_faf_file, sep = ',')

    output_dir = output_path
    truck_mode = ['For-hire Truck', 'Private Truck']
    
    # look up table for sctg group and label
    sctg_group_short = sctg_group_lookup[['SCTG_Group', 'SCTG_Name']]
    sctg_group_short = sctg_group_short.drop_duplicates(keep = 'first')
    # print(sctg_group_short)
    # sctg_def = {'sctg1': 'bulk', 'sctg2': 'fuel_fert', 'sctg3':'interm_food', 'sctg4': 'mfr_goods', 'sctg5': 'other'}
    # <codecell>
    combined_modeled_OD = None
    combined_modeled_OD_mesozone = None
    # mode_choide_by_commodity = None
    # combined_truck_output = None
    for k in range(5):
        sctg = 'sctg' + str(k + 1)
        print('post process mode choice results from ' + sctg)
        b2b_dir = os.path.join(output_dir, sctg)
        list_of_b2b_files = [f for f in os.listdir(b2b_dir) if f.endswith('.zip')]
        if len(list_of_b2b_files) == 0:
            continue
        iterator = 0
    #     cut_off_point = max_ton_lookup[sctg] # shipment capacity for this shipment
        for file in list_of_b2b_files:
            if file == '.DS_Store':
                continue
            # if iterator%10 == 0:
            #     print(iterator)
            modeled_OD_by_sctg = read_csv(os.path.join(b2b_dir, file), sep = ',')
    #         print(modeled_OD_by_sctg.columns)
            list_of_var = ['BuyerID', 'BuyerZone', 'BuyerNAICS', 'SellerID',
               'SellerZone', 'SellerNAICS', 'TruckLoad', 'Commodity_SCTG', 'SCTG_Group', 
               'shipment_id', 'orig_FAFID', 'dest_FAFID', 
               'mode_choice', 'probability', 'Distance', 'Travel_time']
    #         print(modeled_OD_by_sctg.head(5))
            truck_output = modeled_OD_by_sctg.loc[modeled_OD_by_sctg['mode_choice'].isin(truck_mode), list_of_var]
            int_var = ['BuyerID', 'BuyerZone', 'SellerID',
               'SellerZone', 'Commodity_SCTG', 'SCTG_Group', 
               'shipment_id', 'orig_FAFID', 'dest_FAFID']
            truck_output.loc[:, int_var] = truck_output.loc[:, int_var].astype(np.int64)
            truck_only_dir = sctg + '_truck'
            truck_out_path = os.path.join(output_dir, truck_only_dir)
            if not os.path.exists(truck_out_path):
                os.makedirs(truck_out_path)
            truck_output_file = 'truck_only_OD_' + sctg + '_' + str(iterator) + '.csv.zip'
            truck_output.to_csv(os.path.join(truck_out_path, truck_output_file), index = False)
    
            
            ## compute national shipment count and tonmile
            modeled_OD_by_sctg['ShipmentLoad'] = modeled_OD_by_sctg['TruckLoad'] / 1000 # convert to thousand tons
            
            #print(cut_off_point)
    #         modeled_OD_by_sctg.loc[modeled_OD_by_sctg['ShipmentLoad'] > cut_off_point, 'ShipmentLoad'] = cut_off_point
            modeled_OD_by_sctg['tmiles'] = modeled_OD_by_sctg['ShipmentLoad'] * 1000 * modeled_OD_by_sctg['Distance']
            modeled_OD_by_sctg = pd.merge(modeled_OD_by_sctg, mesozone_lookup, 
                                          left_on = ['SellerZone', 'orig_FAFID'], 
                                        right_on = ['MESOZONE', 'FAFID'], how = 'left')
            modeled_OD_by_sctg = modeled_OD_by_sctg.rename(columns={"GEOID": "orig_GEOID",
                                                                    "CBPZONE": "orig_CBPZONE", 
                                                                    "MESOZONE":"orig_MESOZONE", 
                                                                    "FAFNAME":"orig_FAFNAME"})
            modeled_OD_by_sctg = pd.merge(modeled_OD_by_sctg, mesozone_lookup, 
                                          left_on = ['BuyerZone', 'dest_FAFID'], 
                                        right_on = ['MESOZONE', 'FAFID'], how = 'left')
            modeled_OD_by_sctg = modeled_OD_by_sctg.rename(columns={"GEOID": "dest_GEOID", 
                                                                    "CBPZONE": "dest_CBPZONE", 
                                                                    "MESOZONE":"dest_MESOZONE", 
                                                                   "FAFNAME":"dest_FAFNAME"})    
            agg_OD_by_sctg = modeled_OD_by_sctg.groupby(["orig_FAFID", "orig_FAFNAME", 
                                                         "dest_FAFID", "dest_FAFNAME", 
                                                         'Commodity_SCTG', "SCTG_Group", 'mode_choice'])[['tmiles', 'ShipmentLoad']].sum()        
            agg_OD_by_sctg = agg_OD_by_sctg.reset_index()
            agg_count_by_sctg = modeled_OD_by_sctg.groupby(["orig_FAFID", "orig_FAFNAME",
                                                            "dest_FAFID", "dest_FAFNAME",
                                                            'Commodity_SCTG', "SCTG_Group", 'mode_choice'])[['shipment_id']].count() 
            agg_count_by_sctg = agg_count_by_sctg.reset_index()
            agg_OD_by_sctg = pd.merge(agg_OD_by_sctg, agg_count_by_sctg, 
                                      on = ["orig_FAFID", "orig_FAFNAME", "dest_FAFID", "dest_FAFNAME", 
                                            'Commodity_SCTG', "SCTG_Group", 'mode_choice'],
                                      how = 'left')
            agg_OD_by_sctg = pd.merge(agg_OD_by_sctg, sctg_group_short, 
                                      on = "SCTG_Group",
                                      how ='left')
            agg_OD_by_sctg = agg_OD_by_sctg.rename(columns={"shipment_id": "count"})
            # agg_OD_by_sctg.loc[:, 'SCTG_Name'] = sctg_def[sctg]
            # print(iterator)
            # print(agg_OD_by_sctg.head(5))
            
            agg_OD_by_sctg.loc[:, 'chunk_id'] = iterator
            combined_modeled_OD = pd.concat([combined_modeled_OD, agg_OD_by_sctg], sort = False)
            
            # adding mesozone aggregation
            agg_OD_by_sctg = modeled_OD_by_sctg.groupby(['SellerZone', "orig_FAFID", 
                                                         "orig_FAFNAME", 'BuyerZone', 
                                                         "dest_FAFID", "dest_FAFNAME", 
                                                         'Commodity_SCTG', "SCTG_Group", 'mode_choice'])[['tmiles', 'ShipmentLoad']].sum()        
            agg_OD_by_sctg = agg_OD_by_sctg.reset_index()
            agg_count_by_sctg = modeled_OD_by_sctg.groupby(['SellerZone', "orig_FAFID", 
                                                            "orig_FAFNAME", 'BuyerZone', 
                                                            "dest_FAFID", "dest_FAFNAME", 
                                                            'Commodity_SCTG', "SCTG_Group", 'mode_choice'])[['shipment_id']].count() 
            agg_count_by_sctg = agg_count_by_sctg.reset_index()
            agg_OD_by_sctg = pd.merge(agg_OD_by_sctg, agg_count_by_sctg, 
                                  on = ['SellerZone', "orig_FAFID", 
                                        "orig_FAFNAME", 'BuyerZone',
                                        "dest_FAFID", "dest_FAFNAME", 
                                        'Commodity_SCTG', "SCTG_Group", 'mode_choice'],
                                  how = 'left')
            agg_OD_by_sctg = agg_OD_by_sctg.rename(columns={"shipment_id": "count"})
            agg_OD_by_sctg = pd.merge(agg_OD_by_sctg, sctg_group_short, 
                                      on = "SCTG_Group",
                                      how ='left')

            agg_OD_by_sctg.loc[:, 'chunk_id'] = iterator
            combined_modeled_OD_mesozone = pd.concat([combined_modeled_OD_mesozone, agg_OD_by_sctg], sort = False)
            
            iterator += 1 
    #         break        
        # break
    #     combined_truck_output.to_csv(c.input_dir + 'truck_only_OD_' + sctg + '.csv', index = False)
    # combined_modeled_OD = pd.merge(combined_modeled_OD, sctg_group_definition, on = ['SCTG_Group'], how = 'left')
    #combined_modeled_OD.head(10) 
    # <codecell>
    
    combined_modeled_OD_agg = combined_modeled_OD.groupby(["orig_FAFID", "orig_FAFNAME", "dest_FAFID", \
                                                           "dest_FAFNAME", 'Commodity_SCTG', "SCTG_Group", 'SCTG_Name',
                                                           'mode_choice'])[['tmiles', 'ShipmentLoad', 'count']].sum()
    combined_modeled_OD_agg = combined_modeled_OD_agg.reset_index()
    # combined_modeled_OD_agg.head(5)
    print(len(combined_modeled_OD_agg))
    # combined_modeled_OD_agg.loc[:, 'in_study_area'] = 0
    # buffer = combined_modeled_OD_agg.loc[:, 'orig_FAFID'].isin(c.bay_area_region_code) | \
    #         combined_modeled_OD_agg.loc[:, 'dest_FAFID'].isin(c.bay_area_region_code)
    # combined_modeled_OD_agg.loc[buffer, 'in_study_area'] = 1
    if region_code is None:
        combined_modeled_OD_agg.loc[:, 'outbound'] = 1
        combined_modeled_OD_agg.loc[:, 'inbound'] = 1
    else:
        combined_modeled_OD_agg.loc[:, 'outbound'] = 0
        combined_modeled_OD_agg.loc[combined_modeled_OD_agg.loc[:, 'orig_FAFID'].isin(region_code), 'outbound'] = 1       
        combined_modeled_OD_agg.loc[:, 'inbound'] = 0
        combined_modeled_OD_agg.loc[combined_modeled_OD_agg.loc[:, 'dest_FAFID'].isin(region_code), 'inbound'] = 1
    
    combined_modeled_OD_agg.loc[:, 'orig_FAFID'] = combined_modeled_OD_agg.loc[:, 'orig_FAFID'].astype(int)
    combined_modeled_OD_agg.loc[:, 'dest_FAFID'] = combined_modeled_OD_agg.loc[:, 'dest_FAFID'].astype(int)
    combined_modeled_OD_agg.loc[:, 'SCTG_Group'] = combined_modeled_OD_agg.loc[:, 'SCTG_Group'].astype(int)
    combined_modeled_OD_agg.loc[:, 'Distance'] = combined_modeled_OD_agg.loc[:, 'tmiles'] / 1000 / combined_modeled_OD_agg.loc[:, 'ShipmentLoad']
    # print(combined_modeled_OD_agg.head(10))
    
    # add mesozone aggregation
    combined_modeled_OD_mesozone = combined_modeled_OD_mesozone.groupby(['SellerZone', "orig_FAFID", "orig_FAFNAME", 'BuyerZone', 
                                                       "dest_FAFID", "dest_FAFNAME", 'Commodity_SCTG', "SCTG_Group", 'SCTG_Name',
                                                       'mode_choice'])[['tmiles', 'ShipmentLoad', 'count']].sum()
    combined_modeled_OD_mesozone = combined_modeled_OD_mesozone.reset_index()
    # combined_modeled_OD_agg.head(5)
    # combined_modeled_OD_agg.loc[:, 'in_study_area'] = 0
    # buffer = combined_modeled_OD_agg.loc[:, 'orig_FAFID'].isin(c.bay_area_region_code) | \
    #         combined_modeled_OD_agg.loc[:, 'dest_FAFID'].isin(c.bay_area_region_code)
    # combined_modeled_OD_agg.loc[buffer, 'in_study_area'] = 1
    if region_code is None:
        combined_modeled_OD_mesozone.loc[:, 'outbound'] = 1
        combined_modeled_OD_mesozone.loc[:, 'inbound'] = 1
    else:
        combined_modeled_OD_mesozone.loc[:, 'outbound'] = 0
        combined_modeled_OD_mesozone.loc[combined_modeled_OD_mesozone.loc[:, 'orig_FAFID'].isin(region_code), 'outbound'] = 1
        combined_modeled_OD_mesozone.loc[:, 'inbound'] = 0
        combined_modeled_OD_mesozone.loc[combined_modeled_OD_mesozone.loc[:, 'dest_FAFID'].isin(region_code), 'inbound'] = 1
    
    combined_modeled_OD_mesozone.loc[:, 'orig_FAFID'] = combined_modeled_OD_mesozone.loc[:, 'orig_FAFID'].astype(int)
    combined_modeled_OD_mesozone.loc[:, 'dest_FAFID'] = combined_modeled_OD_mesozone.loc[:, 'dest_FAFID'].astype(int)
    combined_modeled_OD_mesozone.loc[:, 'SCTG_Group'] = combined_modeled_OD_mesozone.loc[:, 'SCTG_Group'].astype(int)
    combined_modeled_OD_mesozone.loc[:, 'Distance'] = combined_modeled_OD_mesozone.loc[:, 'tmiles'] / 1000 / combined_modeled_OD_agg.loc[:, 'ShipmentLoad']
    # <codecell>
    combined_modeled_OD_agg.to_csv(os.path.join(output_dir, 'processed_b2b_flow_summary.csv'), sep = ',')
    combined_modeled_OD_mesozone.to_csv(os.path.join(output_dir, 'processed_b2b_flow_summary_mesozone.csv'), sep = ',')
    print('end of post mode choice analysis')
    print('--------------------------------')
    
    return