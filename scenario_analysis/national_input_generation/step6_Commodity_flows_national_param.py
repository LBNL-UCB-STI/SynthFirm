#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 23 09:41:45 2022

@author: xiaodanxu
"""

from pandas import read_csv
import pandas as pd
import numpy as np
import os
import gc
import warnings
import constants as c

warnings.filterwarnings("ignore")

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')

# load inputs
mesozone_to_faf_lookup = read_csv(c.param_dir + "zonal_id_lookup_final.csv")
shipment_by_distance_bin_distribution = read_csv(c.skim_dir + "fraction_of_shipment_by_distance_bin_V2.csv")
shipment_distance_lookup = read_csv(c.skim_dir + "national_od_dist.csv")
producer = read_csv(c.output_dir + "synthetic_producers.csv", low_memory = False)

cost_by_location = read_csv(c.param_dir + 'data_unitcost_by_zone_cfs2017.csv')
cfs_to_faf = read_csv(c.param_dir + 'CFS_FAF_LOOKUP.csv')
max_load_per_shipment = read_csv(c.param_dir + 'max_load_per_shipment_80percent.csv')
sctg_group = read_csv(c.param_dir + 'SCTG_Groups_revised.csv')
supplier_selection_param = read_csv(c.param_dir + 'supplier_selection_parameter.csv')

# output_dir = 'outputs_national/'


producer.loc[producer['Zone'] == 2270, 'Zone'] = 2158
producer.loc[producer['Zone'] == 46113, 'Zone'] = 46102
producer.loc[producer['Zone'] == 51515, 'Zone'] = 51019

# <codecell>

# format distance matrix
mesozone_to_faf_lookup_short = mesozone_to_faf_lookup[['MESOZONE', 'FAFID']]
shipment_distance_lookup = pd.merge(shipment_distance_lookup, mesozone_to_faf_lookup_short,
                                    left_on = 'OriginZone', right_on = 'MESOZONE', how = 'left')

shipment_distance_lookup = pd.merge(shipment_distance_lookup, mesozone_to_faf_lookup_short,
                                    left_on = 'DestinationZone', right_on = 'MESOZONE', how = 'left')
shipment_distance_lookup = \
    shipment_distance_lookup[['OriginZone', 'DestinationZone', 'DistMiles', 'FAFID_x', 'FAFID_y']]
shipment_distance_lookup.columns = ['orig_zone', 'dest_zone', 'Distance', 'orig_FAFID', 'dest_FAFID']
shipment_distance_lookup = shipment_distance_lookup.dropna()
# <codecell>
cost_by_location_faf = pd.merge(cost_by_location, cfs_to_faf,
                                left_on = 'ORIG_CFS_AREA', right_on = 'ST_MA', how = 'left')
cost_by_location_faf = cost_by_location_faf[['FAF', 'Commodity_SCTG', 'UnitCost']] # cost per ton

producer = pd.merge(producer, mesozone_to_faf_lookup, left_on = 'Zone', right_on = 'MESOZONE', how = 'left')
producer.loc[:, 'FAFID'] = producer.loc[:, 'FAFID'].replace(np.nan, 0)
producer = producer[['SellerID', 'Zone', 'NAICS', 'OutputCommodity', 'Commodity_SCTG', 'OutputCapacitylb', 'FAFID']]
int_vars = ['SellerID', 'Zone', 'OutputCapacitylb', 'FAFID']
producer.loc[:, int_vars] = producer.loc[:, int_vars].astype(int)
# shipment_distance_lookup = shipment_distance_lookup.loc[shipment_distance_lookup['Alternative'] == 'Air']
step_size = 100 # mile
# <codecell>
from sklearn.utils import shuffle

def split_dataframe(df, chunk_size = 10000): 
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    return chunks


for k in range(5):
    sctg = k + 1
    print('process SCTG group ' + str(sctg))
    shipment_by_distance_bin_distribution.loc[:, 'probability'] = shipment_by_distance_bin_distribution.loc[:, str(sctg)]
    g1_consm = read_csv(c.output_dir + "consumers_sctg" + str(sctg) +".csv", low_memory = False)
    g1_consm.loc[g1_consm['Zone'] == 2270, 'Zone'] = 2158
    g1_consm.loc[g1_consm['Zone'] == 46113, 'Zone'] = 46102
    g1_consm.loc[g1_consm['Zone'] == 51515, 'Zone'] = 51019
    g1_consm = pd.merge(g1_consm, mesozone_to_faf_lookup, left_on = 'Zone', right_on = 'MESOZONE', how = 'left')
    g1_consm.loc[:, 'FAFID'] = g1_consm.loc[:, 'FAFID'].replace(np.nan, 0)

    g1_consm = g1_consm[['BuyerID', 'Zone', 'Commodity_SCTG', 'SCTG_Group', 'NAICS', 
                         'InputCommodity', 'PurchaseAmountlb', 'FAFID']] 
    int_vars = ['BuyerID','Zone', 'Commodity_SCTG', 'SCTG_Group', 'PurchaseAmountlb', 'FAFID']
    g1_consm.loc[:, int_vars] = g1_consm.loc[:, int_vars].astype(int)    
    g1_consm = pd.merge(g1_consm, supplier_selection_param,
                        on = 'SCTG_Group', how = 'left') # append supplier selection param
    list_of_commodity = g1_consm.InputCommodity.unique()
    #output dir
 
    
    # process each buyer industry
    for com in list_of_commodity:
        print(com)
        output_b2b_flow = None
        output_file = "sctg" + str(sctg) + '_' + str(com) + ".csv.zip"
        path_to_output = c.output_dir + output_file
        if os.path.exists(path_to_output):
            continue        
        buyer = g1_consm.loc[ (g1_consm['InputCommodity'] == com) & (g1_consm['PurchaseAmountlb'] > 0)]
        list_of_quantiles = list(range(10))
        list_of_quantiles = [x + 1 for x in list_of_quantiles]
        list_of_quantiles = [x * 0.1 for x in list_of_quantiles]
        cut_off = buyer.loc[:, 'PurchaseAmountlb'].quantile(q = list_of_quantiles)
        cut_off = np.sort(cut_off.unique())
        label = list(range(len(cut_off)))
        cut_off = np.insert(cut_off, 0, 0)
        
        buyer.loc[:, 'demand_rank'] = pd.cut(buyer.loc[:, 'PurchaseAmountlb'], 
                                             bins = cut_off, labels = label, right = True, include_lowest = True)
        if sctg == 5:
            supplier = producer.loc[producer['OutputCapacitylb'] > 0] # allow less stringent rule for other commodity
        else:
            supplier = producer.loc[(producer['OutputCommodity'] == com) & (producer['OutputCapacitylb'] > 0)]

        supplier.loc[:, 'supply_rank'] = pd.cut(supplier.loc[:, 'OutputCapacitylb'], 
                                                bins = cut_off, labels = label, right = True, include_lowest = True)
        list_of_zone = buyer.Zone.unique()
        for zone in list_of_zone:
            # print(faf)
            selected_buyer = buyer.loc[buyer['Zone'] == zone]
            available_levels = selected_buyer.demand_rank.unique()
            distance_skim = shipment_distance_lookup.loc[shipment_distance_lookup['dest_zone'] == zone]
            supplier_with_dist = pd.merge(supplier, distance_skim, 
                                     left_on = 'Zone', right_on = 'orig_zone', how = 'left')
            supplier_with_dist.loc[:, 'Distance'] = supplier_with_dist.loc[:, 'Distance'].replace(np.nan, 5000)
            supplier_with_distance_cost = pd.merge(supplier_with_dist, cost_by_location_faf,
                                                   left_on = ['FAFID', 'Commodity_SCTG'],
                                                   right_on = ['FAF', 'Commodity_SCTG'],
                                                   how = 'left')
            
            supplier_with_distance_cost = pd.merge(supplier_with_distance_cost, max_load_per_shipment,
                                                   on = 'Commodity_SCTG',
                                                   how = 'left')
            supplier_with_distance_cost.loc[:, 'UnitCost'] = \
                supplier_with_distance_cost.loc[:,'UnitCost'].fillna(supplier_with_distance_cost.groupby('Commodity_SCTG')['UnitCost'].transform('mean'))
            for level in available_levels:
                # print('processing demand level ' + str(level))
                selected_buyer_by_level = selected_buyer.loc[selected_buyer['demand_rank'] == level]
                # select samples of supplier based on distance
                # selected_buyer_by_zone = selected_buyer.loc[selected_buyer['FAFID'] == faf]
                # chunksize = 5000
                # chunks_of_buyers = split_dataframe(selected_buyer_by_level, chunksize)
                
                supplier_pool = supplier_with_distance_cost.loc[supplier_with_distance_cost['supply_rank'] >= level]
                if len(supplier_pool) == 0:                        
                        supplier_pool = supplier_with_distance_cost.copy()

                nbreaks = shipment_by_distance_bin_distribution.Cutpoint.tolist()
                nbreaks = np.insert(nbreaks, 0, -1)
                nlabels = shipment_by_distance_bin_distribution.IDs.tolist()
                supplier_pool.loc[:, 'distance_bin'] = pd.cut(supplier_pool.loc[:, 'Distance'], 
                                                                  bins = nbreaks, labels = nlabels, right = True)
                supplier_pool = pd.merge(supplier_pool, shipment_by_distance_bin_distribution, 
                                left_on = 'distance_bin', right_on = 'IDs', how = 'left')
                supplier_pool = supplier_pool[['SellerID', 'Zone', 'NAICS', 'OutputCommodity', 'Commodity_SCTG',
                                               'OutputCapacitylb', 'supply_rank', 'Distance', 'UnitCost', 'SHIPMT_WGHT', 'probability']]
                # for chunk in chunks_of_buyers: 
                #     # print(len(chunk))
                #     if len(chunk) == 0:
                #         continue
                sample_size = min(5 * len(selected_buyer_by_level), len(supplier_pool))
                if len(supplier_pool) == 0:
                    continue
                selected_supplier = supplier_pool.sample(n = sample_size, replace = False, weights= 'probability')
                
                # pairing buyer and supplier
                if sctg == 5:
                    paired_buyer_supplier = pd.merge(selected_buyer_by_level, selected_supplier,
                                            on = "Commodity_SCTG", 
                                            how = 'left')    # allow less stringent match for other commodity 
                else:
                    paired_buyer_supplier = pd.merge(selected_buyer_by_level, selected_supplier,
                                                left_on = ["InputCommodity", "Commodity_SCTG"], 
                                                right_on = ["OutputCommodity", "Commodity_SCTG"], 
                                                how = 'left')
           
                paired_buyer_supplier = paired_buyer_supplier.loc[paired_buyer_supplier['BuyerID'] != paired_buyer_supplier['SellerID']]
                
                if len(paired_buyer_supplier) == 0:
                    print('pairing failed for selected buyers ' + str(len(selected_buyer_by_level)))
                    continue
                paired_buyer_supplier.loc[:,'Value'] = paired_buyer_supplier.loc[:,'PurchaseAmountlb'] * paired_buyer_supplier.loc[:,'UnitCost'] / 2000
                criteria1 = (paired_buyer_supplier['PurchaseAmountlb'] >= paired_buyer_supplier['SHIPMT_WGHT'])
                paired_buyer_supplier.loc[criteria1,'Value'] = paired_buyer_supplier.loc[criteria1,'SHIPMT_WGHT'] * \
                    paired_buyer_supplier.loc[criteria1,'UnitCost'] / 2000
                paired_buyer_supplier.loc[:,'Utility'] = \
                    paired_buyer_supplier.loc[:,'dist_const'] * (paired_buyer_supplier.loc[:,'Distance'] > 500) + \
                    paired_buyer_supplier.loc[:,'dist_low'] * paired_buyer_supplier.loc[:,'Distance'] * (paired_buyer_supplier.loc[:,'Distance'] <= 500) + \
                    paired_buyer_supplier.loc[:,'dist_high'] * paired_buyer_supplier.loc[:,'Distance'] * (paired_buyer_supplier.loc[:,'Distance'] > 500) + \
                    paired_buyer_supplier.loc[:,'cost'] * paired_buyer_supplier.loc[:,'Value']
                paired_buyer_supplier.loc[:,'Score'] = np.exp(paired_buyer_supplier.loc[:,'Utility'])
                paired_buyer_supplier.loc[:, 'ratio'] = paired_buyer_supplier.loc[:, 'OutputCapacitylb'] / \
                    paired_buyer_supplier.loc[:, 'PurchaseAmountlb']
                # paired_buyer_supplier = paired_buyer_supplier.groupby(['BuyerID','Distance']).sample(frac = 1)
                paired_buyer_supplier = shuffle(paired_buyer_supplier)
                paired_buyer_supplier = paired_buyer_supplier.sort_values(['BuyerID','Score'],ascending = False).reset_index(drop=True)
                
                paired_buyer_supplier.loc[:, "cs_ratio"] = paired_buyer_supplier.groupby(['BuyerID'])['ratio'].cumsum(axis=0)
                paired_buyer_supplier.loc[:, 'met_demand'] =  0
                paired_buyer_supplier.loc[paired_buyer_supplier['cs_ratio'] >= 1, 'met_demand'] = 1
                paired_buyer_supplier.loc[:, "cs_met"] = paired_buyer_supplier.groupby(['BuyerID'])['met_demand'].cumsum(axis=0)
                selected_b2b_flow = paired_buyer_supplier.loc[paired_buyer_supplier['cs_met'] <= 1]
                selected_b2b_flow.loc[:, "total_ratio"] = selected_b2b_flow.groupby(['BuyerID'])['ratio'].transform('sum')
                selected_b2b_flow.loc[:, "TruckLoad"] = selected_b2b_flow.loc[:, "ratio"] * \
                    selected_b2b_flow.loc[:, "PurchaseAmountlb"] / selected_b2b_flow.loc[:, "total_ratio"]
                if len(selected_b2b_flow) == 0:
                    print('pairing failed for selected buyers ' + str(len(selected_buyer_by_level)))
                    continue
                # formatting output
                selected_b2b_flow = selected_b2b_flow.rename(columns={"Zone_x": "BuyerZone", 
                                                                      "NAICS_x": "BuyerNAICS", 
                                                                      "Zone_y": "SellerZone",
                                                                      "NAICS_y": "SellerNAICS"})
                selected_b2b_flow = selected_b2b_flow[['BuyerID', 'BuyerZone', 'BuyerNAICS', 'SellerID', 
                                                        'SellerZone', 'SellerNAICS', 'TruckLoad', 'SCTG_Group',
                                                        'Commodity_SCTG', 'UnitCost']]
                output_b2b_flow = pd.concat([output_b2b_flow, selected_b2b_flow])
                # if (len(chunk) > len(selected_b2b_flow)):
                #     print('incomplete match for zone ' + str(faf))
                    
                # increasing cost for selected suppliers (prevent same suppliers were overly selected)
                selected_sellers = selected_b2b_flow.SellerID.unique()
                supplier_with_distance_cost.loc[supplier_with_distance_cost['SellerID'].isin(selected_sellers), 'Distance'] += step_size
                #break
        #         break
                
        #     break

        # break
        if output_b2b_flow is None:
            print('pairing failed for selected buyers ' + str(len(selected_buyer)))
            continue
        print('writing output for commodity ' + str(com))
        output_b2b_flow.to_csv(path_to_output, index = False)
    # break