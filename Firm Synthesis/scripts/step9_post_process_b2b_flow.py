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
import constants as c
import warnings
warnings.filterwarnings("ignore")

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')

sctg_group_lookup = read_csv(c.param_dir + c.sctg_group_lookup_file, sep = ',')
mesozone_lookup = read_csv(c.param_dir + c.mesozone_id_lookup_file, sep = ',')

truck_mode = ['For-hire Truck', 'Private Truck']

# <codecell>
combined_modeled_OD = None
mode_choide_by_commodity = None
combined_truck_output = None
for sctg in c.list_of_sctg_group:
    print(sctg)
    b2b_dir = c.output_dir + sctg
    list_of_b2b_files = listdir(b2b_dir)
    iterator = 0
#     cut_off_point = max_ton_lookup[sctg] # shipment capacity for this shipment
    for file in list_of_b2b_files:
        if file == '.DS_Store':
            continue
        if iterator%10 == 0:
            print(iterator)
        modeled_OD_by_sctg = read_csv(b2b_dir + '/' + file, sep = ',')
#         print(modeled_OD_by_sctg.columns)
        list_of_var = ['BuyerID', 'BuyerZone', 'BuyerNAICS', 'SellerID',
           'SellerZone', 'SellerNAICS', 'TruckLoad', 'SCTG_Group', 
           'shipment_id', 'orig_FAFID', 'dest_FAFID', 
           'mode_choice', 'probability', 'Distance', 'Travel_time']
#         print(modeled_OD_by_sctg.head(5))
        truck_output = modeled_OD_by_sctg.loc[modeled_OD_by_sctg['mode_choice'].isin(truck_mode), list_of_var]
        int_var = ['BuyerID', 'BuyerZone', 'SellerID',
           'SellerZone', 'SCTG_Group', 
           'shipment_id', 'orig_FAFID', 'dest_FAFID']
        truck_output.loc[:, int_var] = truck_output.loc[:, int_var].astype(int)
        truck_output.to_csv(c.output_dir + sctg + '_truck/truck_only_OD_' + sctg + '_' + 
                            str(iterator) + '.csv', index = False)

        
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
        agg_OD_by_sctg = modeled_OD_by_sctg.groupby(["orig_FAFID", "orig_FAFNAME", "dest_FAFID", "dest_FAFNAME", "SCTG_Group", 'mode_choice'])[['tmiles', 'ShipmentLoad']].sum()        
        agg_OD_by_sctg = agg_OD_by_sctg.reset_index()
        agg_count_by_sctg = modeled_OD_by_sctg.groupby(["orig_FAFID", "orig_FAFNAME", "dest_FAFID", "dest_FAFNAME", "SCTG_Group", 'mode_choice'])[['shipment_id']].count() 
        agg_count_by_sctg = agg_count_by_sctg.reset_index()
        agg_OD_by_sctg = pd.merge(agg_OD_by_sctg, agg_count_by_sctg, 
                                  on = ["orig_FAFID", "orig_FAFNAME", "dest_FAFID", "dest_FAFNAME", "SCTG_Group", 'mode_choice'],
                                  how = 'left')
        agg_OD_by_sctg = agg_OD_by_sctg.rename(columns={"shipment_id": "count"})
        agg_OD_by_sctg.loc[:, 'SCTG_Name'] = c.sctg_def[sctg]
        agg_OD_by_sctg.loc[:, 'chunk_id'] = iterator
        combined_modeled_OD = pd.concat([combined_modeled_OD, agg_OD_by_sctg], sort = False)
        iterator += 1 
#         break        
#     break
#     combined_truck_output.to_csv(c.input_dir + 'truck_only_OD_' + sctg + '.csv', index = False)
# combined_modeled_OD = pd.merge(combined_modeled_OD, sctg_group_definition, on = ['SCTG_Group'], how = 'left')
#combined_modeled_OD.head(10) 
# <codecell>

combined_modeled_OD_agg = combined_modeled_OD.groupby(["orig_FAFID", "orig_FAFNAME", "dest_FAFID", \
                                                       "dest_FAFNAME", "SCTG_Group", 'SCTG_Name',
                                                       'mode_choice'])[['tmiles', 'ShipmentLoad', 'count']].sum()
combined_modeled_OD_agg = combined_modeled_OD_agg.reset_index()
combined_modeled_OD_agg.head(5)
# combined_modeled_OD_agg.loc[:, 'in_study_area'] = 0
# buffer = combined_modeled_OD_agg.loc[:, 'orig_FAFID'].isin(c.bay_area_region_code) | \
#         combined_modeled_OD_agg.loc[:, 'dest_FAFID'].isin(c.bay_area_region_code)
# combined_modeled_OD_agg.loc[buffer, 'in_study_area'] = 1

combined_modeled_OD_agg.loc[:, 'outbound'] = 0
combined_modeled_OD_agg.loc[combined_modeled_OD_agg.loc[:, 'orig_FAFID'].isin(c.region_code), 'outbound'] = 1

combined_modeled_OD_agg.loc[:, 'inbound'] = 0
combined_modeled_OD_agg.loc[combined_modeled_OD_agg.loc[:, 'dest_FAFID'].isin(c.region_code), 'inbound'] = 1

combined_modeled_OD_agg.loc[:, 'orig_FAFID'] = combined_modeled_OD_agg.loc[:, 'orig_FAFID'].astype(int)
combined_modeled_OD_agg.loc[:, 'dest_FAFID'] = combined_modeled_OD_agg.loc[:, 'dest_FAFID'].astype(int)
combined_modeled_OD_agg.loc[:, 'SCTG_Group'] = combined_modeled_OD_agg.loc[:, 'SCTG_Group'].astype(int)
combined_modeled_OD_agg.loc[:, 'Distance'] = combined_modeled_OD_agg.loc[:, 'tmiles'] / 1000 / combined_modeled_OD_agg.loc[:, 'ShipmentLoad']
print(combined_modeled_OD_agg.head(10))

# <codecell>
combined_modeled_OD_agg.to_csv(c.output_dir + 'processed_b2b_flow_summary.csv', sep = ',')