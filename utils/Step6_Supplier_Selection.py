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
from sklearn.utils import shuffle

warnings.filterwarnings("ignore")

# os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')
def split_dataframe(df, chunk_size = 10000): 
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    return chunks

  
def supplier_selection(mesozone_to_faf_file, shipment_by_distance_file,
                       shipment_distance_lookup_file, cost_by_location_file,
                       producer_file, consumer_file, cfs_to_faf_file,
                       max_load_per_shipment_file, sctg_group_file,
                       supplier_selection_param_file, output_path):
    
    print('Performing supplier selection at national-scale...')
    
    # load inputs
    mesozone_to_faf_lookup = read_csv(mesozone_to_faf_file)
    shipment_by_distance_bin_distribution = read_csv(shipment_by_distance_file)
    shipment_distance_lookup = read_csv(shipment_distance_lookup_file)
    producer = read_csv(producer_file, low_memory = False)
    consumer = read_csv(consumer_file, low_memory = False)
    cost_by_location = read_csv(cost_by_location_file)
    cfs_to_faf = read_csv(cfs_to_faf_file)
    max_load_per_shipment = read_csv(max_load_per_shipment_file)
    sctg_group = read_csv(sctg_group_file)
    supplier_selection_param = read_csv(supplier_selection_param_file)
    
    output_dir = output_path
    
    
    # <codecell>
    # pre-process data
    cost_by_location_faf = pd.merge(cost_by_location, cfs_to_faf,
                                    left_on = 'ORIG_CFS_AREA', 
                                    right_on = 'ST_MA', how = 'left')
    cost_by_location_faf = cost_by_location_faf[['FAF', 'Commodity_SCTG', 'UnitCost']] # cost per ton
    # $/ton
    producer.loc[:, 'Zone'] = producer.loc[:, 'Zone'].astype(np.int64)
    mesozone_to_faf_lookup.loc[:, 'MESOZONE'] = \
        mesozone_to_faf_lookup.loc[:, 'MESOZONE'].astype(np.int64)
    producer = pd.merge(producer, mesozone_to_faf_lookup, 
                        left_on = 'Zone', right_on = 'MESOZONE', how = 'left')
    producer.loc[:, 'FAFID'] = producer.loc[:, 'FAFID'].replace(np.nan, 0)
    producer = producer[['SellerID', 'Zone', 'NAICS', 'Commodity_SCTG', 'OutputCapacitylb', 'FAFID']]
    int_vars = ['SellerID', 'Zone', 'OutputCapacitylb', 'FAFID']
    producer.loc[:, int_vars] = producer.loc[:, int_vars].astype(np.int64)
    consumer = pd.merge(consumer, sctg_group,
                         left_on = "Commodity_SCTG", right_on = "SCTG_Code", how = 'left')
    # shipment_distance_lookup = shipment_distance_lookup.loc[shipment_distance_lookup['Alternative'] == 'Air']
    step_size = 100 # mile
    # <codecell>
    
# matching suppliers by SCTG group --> need parallel method
    total_load = 0   
    total_buyers = 0
    for k in range(5):
        sctg = k + 1
        print('process SCTG group ' + str(sctg))
        shipment_by_distance_bin_distribution.loc[:, 'probability'] = shipment_by_distance_bin_distribution.loc[:, str(sctg)]
        nbreaks = shipment_by_distance_bin_distribution.Cutpoint.tolist()
        nbreaks = np.insert(nbreaks, 0, -1)
        nlabels = shipment_by_distance_bin_distribution.IDs.tolist()
        g1_consm = consumer.loc[consumer['SCTG_Group'] == sctg]
        g1_consm.loc[:, 'Zone'] = g1_consm.loc[:, 'Zone'].astype(np.int64)
        g1_consm = pd.merge(g1_consm, mesozone_to_faf_lookup, 
                            left_on = 'Zone', right_on = 'MESOZONE', how = 'left')
        g1_consm.loc[:, 'FAFID'] = g1_consm.loc[:, 'FAFID'].replace(np.nan, 0)
    
        g1_consm = g1_consm[['BuyerID', 'Zone', 'Commodity_SCTG', 'SCTG_Group', 'NAICS', 
                             'InputCommodity', 'PurchaseAmountlb', 'FAFID']] 
        int_vars = ['BuyerID','Zone', 'Commodity_SCTG', 'SCTG_Group', 'PurchaseAmountlb', 'FAFID']
        g1_consm.loc[:, int_vars] = g1_consm.loc[:, int_vars].astype(np.int64)    
        g1_consm = pd.merge(g1_consm, supplier_selection_param,
                            on = 'SCTG_Group', how = 'left') # append supplier selection param
        list_of_commodity = g1_consm.InputCommodity.unique()
        print('total buyers from SCTG group ' + str(sctg))
        print(len(g1_consm.BuyerID.unique()))
        
        print('total industry need suppliers:')
        print(len(list_of_commodity))
        #output dir
     
        
        # process each buyer industry
        buyer_count = 0
        for com in list_of_commodity:
            # print(com)
            output_b2b_flow = None
            output_file = "sctg" + str(sctg) + '_' + str(com) + ".csv.zip"
            path_to_output = os.path.join(output_dir, output_file)

            buyer = g1_consm.loc[ (g1_consm['InputCommodity'] == com) & (g1_consm['PurchaseAmountlb'] > 0)]

            if os.path.exists(path_to_output):
                continue        # pick up from where last matching is done

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
                supplier = producer.loc[(producer['NAICS'] == com) & (producer['OutputCapacitylb'] > 0)]
            total_demand = buyer.PurchaseAmountlb.sum()
            total_supply = supplier.OutputCapacitylb.sum()
            if total_supply < total_demand:
                # print('There is a lack of supply to industry ' + com)
                scaling_factor = total_demand/ total_supply
                # print(scaling_factor)
                supplier['OutputCapacitylb'] *= scaling_factor # scale up the supply

            supplier.loc[:, 'supply_rank'] = pd.cut(supplier.loc[:, 'OutputCapacitylb'], 
                                                    bins = cut_off, labels = label, right = True, include_lowest = True)
            supplier.loc[:, 'supply_rank'] = supplier.loc[:, 'supply_rank'].fillna(max(label))
            list_of_faf = buyer.FAFID.unique()
            for faf in list_of_faf:
                # print(faf)
                selected_buyer = buyer.loc[buyer['FAFID'] == faf]
                
                available_levels = selected_buyer.demand_rank.unique()
                distance_skim = shipment_distance_lookup.loc[shipment_distance_lookup['dest_FAFID'] == faf]
                supplier_with_dist = pd.merge(supplier, distance_skim, 
                                         left_on = 'FAFID', right_on = 'orig_FAFID', how = 'left')
                same_od_idx = (supplier_with_dist['orig_FAFID'] == supplier_with_dist['dest_FAFID'])
                supplier_with_dist.loc[same_od_idx, 'Distance'] = \
                supplier_with_dist.loc[same_od_idx, 'Distance'].replace(np.nan, 5)
                supplier_with_dist.loc[:, 'Distance'] = \
                supplier_with_dist.loc[:, 'Distance'].replace(np.nan, 5000)
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
                    selected_buyer_by_level = \
                        selected_buyer.loc[selected_buyer['demand_rank'] == level]
                    orig_col = selected_buyer_by_level.columns
                    buyers_id = selected_buyer_by_level.BuyerID.unique()
                    
                    
                    # select samples of supplier based on distance
                    # selected_buyer_by_zone = selected_buyer.loc[selected_buyer['FAFID'] == faf]
                    # chunksize = 5000
                    # chunks_of_buyers = split_dataframe(selected_buyer_by_level, chunksize)
                    
                    supplier_pool = \
                        supplier_with_distance_cost.loc[supplier_with_distance_cost['supply_rank'] >= level]
                    if len(supplier_pool) == 0:                        
                            supplier_pool = supplier_with_distance_cost.copy()
                    # exclude self trade
                    supplier_pool = supplier_pool[~supplier_pool['SellerID'].isin(buyers_id)]
                    supplier_pool.loc[:, 'distance_bin'] = pd.cut(supplier_pool.loc[:, 'Distance'], 
                                                                      bins = nbreaks, labels = nlabels, right = True)
                    supplier_pool = pd.merge(supplier_pool, shipment_by_distance_bin_distribution, 
                                    left_on = 'distance_bin', right_on = 'IDs', how = 'left')
                    supplier_pool = supplier_pool[['SellerID', 'Zone', 'NAICS', 'Commodity_SCTG',
                                                   'OutputCapacitylb', 'supply_rank', 'Distance', 'UnitCost', 'SHIPMT_WGHT', 'probability']]

                    sample_size = min(3 * len(selected_buyer_by_level), len(supplier_pool))
                    if len(supplier_pool) == 0:
                        print('Unable to find suppliers for input ' + com)
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
                                                    right_on = ["NAICS", "Commodity_SCTG"], 
                                                    how = 'left')
               
                    # paired_buyer_supplier = paired_buyer_supplier.loc[paired_buyer_supplier['BuyerID'] != paired_buyer_supplier['SellerID']]
                    # catch unassigned buyers
                    unpaired_buyer = paired_buyer_supplier[paired_buyer_supplier.isna().any(axis=1)]
                    unpaired_buyer.rename(columns = {'Zone_x':'Zone',
                                                     'NAICS_x': 'NAICS'}, inplace = True)
                    unpaired_buyer = unpaired_buyer[orig_col]
                    unpaired_buyer.drop_duplicates(subset = ['BuyerID', 'PurchaseAmountlb'], 
                                                   keep = 'first', inplace = True)
                    
                    paired_buyer_supplier = paired_buyer_supplier.dropna()
                    paired_buyer_ids = paired_buyer_supplier.BuyerID.unique()
                    unpaired_buyer = \
                        unpaired_buyer[~unpaired_buyer['BuyerID'].isin(paired_buyer_ids)]
                   
                    # try reassign suppliers to un assigned buyers
                    if len(unpaired_buyer) > 0:
                        # print('pairing failed for selected buyers ' + str(len(selected_buyer_by_level)))
                        # print('drop buyer commodity type and retry')
                        unpaired_buyer.drop(columns = ['Commodity_SCTG'], inplace = True)
                        paired_buyer_supplier_add = pd.merge(unpaired_buyer, selected_supplier,
                                                    left_on = ["InputCommodity"], 
                                                    right_on = ["NAICS"], 
                                                    how = 'left')
                        # paired_buyer_supplier_add = \
                        # paired_buyer_supplier_add.loc[paired_buyer_supplier_add['BuyerID'] != paired_buyer_supplier_add['SellerID']]
                        if len(paired_buyer_supplier_add) == 0:
                            print('pairing failed for selected buyers ' + str(len(unpaired_buyer)))
                            print('retry failed')
                            continue
                        paired_buyer_supplier = pd.concat([paired_buyer_supplier,
                                                           paired_buyer_supplier_add])
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
                    
                    paired_buyer_supplier.loc[:, "cs_ratio"] = \
                    paired_buyer_supplier.groupby(['BuyerID', 'PurchaseAmountlb'])['ratio'].cumsum(axis=0)
                    paired_buyer_supplier.loc[:, 'met_demand'] =  0
                    paired_buyer_supplier.loc[paired_buyer_supplier['cs_ratio'] >= 1, 'met_demand'] = 1
                    paired_buyer_supplier.loc[:, "cs_met"] = \
                    paired_buyer_supplier.groupby(['BuyerID', 'PurchaseAmountlb'])['met_demand'].cumsum(axis=0)
                    selected_b2b_flow = paired_buyer_supplier.loc[paired_buyer_supplier['cs_met'] <= 1]
                    

                    
                    selected_b2b_flow.loc[:, "total_ratio"] = \
                    selected_b2b_flow.groupby(['BuyerID', 'PurchaseAmountlb'])['ratio'].transform('sum')
                    selected_b2b_flow.loc[:, "TruckLoad"] = selected_b2b_flow.loc[:, "ratio"] * \
                        selected_b2b_flow.loc[:, "PurchaseAmountlb"] / selected_b2b_flow.loc[:, "total_ratio"]
                    # unique_buyers = selected_b2b_flow.BuyerID.unique()
                    # buyer_count += len(unique_buyers)
                    if len(selected_b2b_flow) == 0:
                        print('pairing failed for selected buyers ' + str(len(selected_buyer_by_level)))
                        continue
                    # formatting output
                    missing_values = sum(selected_b2b_flow.isna().any())
                    if missing_values > 0:
                         selected_b2b_flow.to_csv(os.path.join(output_dir,'buyer_seller_pair_qaqc_' + com + '.csv'))
                         #print(str(missing_values) + ' missing present after sampling')
                         
                    #     break
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
                    
                    
            #     break
    
            # break
            output_b2b_flow.dropna(inplace = True)
            if output_b2b_flow is None:
                print('pairing failed for selected buyers ' + str(len(selected_buyer)))
                continue
            else:
            #print('writing output for commodity ' + str(com))
                output_b2b_flow = output_b2b_flow.astype({
                    'BuyerID': np.int64,
                    'BuyerZone': np.int64,
                    'BuyerNAICS': 'string',
                    'SellerID': np.int64, 
                    'SellerZone': np.int64,
                    'SellerNAICS': 'string',
                    'TruckLoad': 'float',
                    'SCTG_Group': 'int',
                    'Commodity_SCTG': 'int', 
                    'UnitCost': 'float'
                })
                output_b2b_flow.to_csv(path_to_output, index = False)
                load_to_add = float(output_b2b_flow.TruckLoad.sum())
                total_load += load_to_add/2000/1000 # in 1000 tons
                buyers_to_add = int(len(output_b2b_flow.BuyerID.unique()))
                # print('buyers assigned:')
                # print(buyers_to_add)
                total_buyers += buyers_to_add
        # print(buyer_count)
        # break
    print('end of supplier selection')
    print('Total load assigned = ' + str(total_load))
    print('Total buyer assigned = ' + str(total_buyers))
    print('-------------------------')
    return