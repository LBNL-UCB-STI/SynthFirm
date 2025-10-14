#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct  5 13:44:48 2022

@author: xiaodanxu
"""

from pandas import read_csv
import pandas as pd
import numpy as np
import os
# import gc
import warnings
# import matplotlib.pyplot as plt

warnings.filterwarnings("ignore")

# os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')
# forecast_year = '2040'
# load inputs

# <codecell>

def prod_cons_demand_forecast(forecast_year, synthetic_firms_no_location_file,
                              producer_file, consumer_file, prod_forecast_file,
                              cons_forecast_file, mesozone_to_faf_file, sctg_group_file,
                              consumer_by_sctg_filehead, output_path):

    print(f'Forecast SynthFirm production and consumption for year {forecast_year}...')
    firms_baseline = read_csv(synthetic_firms_no_location_file)
    production_baseline = read_csv(producer_file)
    consumer_baseline = read_csv(consumer_file)

    future_production = read_csv(prod_forecast_file)
    future_consumption = read_csv(cons_forecast_file)

    #load parameters
    mesozone_to_faf_lookup = read_csv(mesozone_to_faf_file)
    sctg_lookup = read_csv(sctg_group_file)

    # select domestic firms for forecast
    production_baseline = pd.merge(production_baseline, mesozone_to_faf_lookup,
                                   left_on = 'Zone', right_on = 'MESOZONE', how = 'left')
    
    consumer_baseline = pd.merge(consumer_baseline, mesozone_to_faf_lookup,
                                   left_on = 'Zone', right_on = 'MESOZONE', how = 'left')
    
    
    # <codecell>
    
    # generate domostic production adj factor
    lb_to_ton = 1/2000
    
    forecast_tonnage = 'tons_' + forecast_year
    
    production_baseline_agg = production_baseline.groupby(['FAFID', 'Commodity_SCTG'])[['OutputCapacitylb']].sum()
    production_baseline_agg = production_baseline_agg.reset_index()
    production_baseline_agg.loc[:, 'tons_baseline'] = production_baseline_agg.loc[:, 'OutputCapacitylb'] * lb_to_ton / 1000
    production_domestic_adj_factor = pd.merge(production_baseline_agg, future_production,
                                              left_on = ['FAFID', 'Commodity_SCTG'],
                                              right_on = ['dms_orig', 'SCTG_Code'],
                                              how = 'outer')
        
    production_domestic_adj_factor.loc[:, forecast_tonnage] = \
        production_domestic_adj_factor.loc[:, forecast_tonnage].fillna(0)
        
    production_adj_factor_with_firms = \
        production_domestic_adj_factor.loc[~production_domestic_adj_factor[ 'OutputCapacitylb'].isna()]
        
    production_adj_factor_no_firms = \
        production_domestic_adj_factor.loc[production_domestic_adj_factor[ 'OutputCapacitylb'].isna()]    
    
    production_adj_factor_with_firms.loc[:, 'adj_factor'] = \
        production_adj_factor_with_firms.loc[:, forecast_tonnage] / \
            production_adj_factor_with_firms.loc[:, 'tons_baseline']
    
    # production_adj_factor_to_plot = production_adj_factor_with_firms.loc[production_adj_factor_with_firms['adj_factor'] <= 300]
    # production_adj_factor_to_plot.adj_factor.hist(bins = 500)
    # plt.xlim([0,50])        
    # <codecell>
    from sklearn.utils import shuffle
    
    #projecting production of existing firms
    production_adj_factor_selected = production_adj_factor_with_firms[['FAFID', 'Commodity_SCTG', 'adj_factor']]
    production_projected = pd.merge(production_baseline, production_adj_factor_selected,
                                             on = ['FAFID', 'Commodity_SCTG'], how = 'left')
    production_projected.loc[:, 'OutputCapacitylb'] *= production_projected.loc[:, 'adj_factor']
    print('Total production tonnage after forecast:')
    print(production_projected['OutputCapacitylb'].sum() * lb_to_ton / 1000)

    
    # <codecell>
    # fill missing firms and productions
    # production_upper_bound = 500000000 * lb_to_ton / 1000 # hypothetical production capacity assumed in SynthFirm
    additional_production_to_add = production_adj_factor_no_firms[['dms_orig', 'SCTG_Code', forecast_tonnage]]
    firms_added = None
    for idx, row in additional_production_to_add.iterrows():
        SCTG = row['SCTG_Code']
        capacity_to_add = row[forecast_tonnage] * 2000 * 1000 # unit in lb
        # print(SCTG, capacity_to_add)
        
        #create list of firms with designated capacity
        production_template = production_projected.loc[production_projected['Commodity_SCTG'] == SCTG]
        production_template = shuffle(production_template)
        production_template.loc[:, 'acc_capacity'] = \
            production_template.loc[:, 'OutputCapacitylb'].cumsum()
        production_template = production_template.loc[production_template['acc_capacity'] <= capacity_to_add]
        scaling_factor = capacity_to_add / production_template.loc[:, 'OutputCapacitylb'].sum()
        production_template.loc[:, 'OutputCapacitylb'] *= scaling_factor
        
        # allocate zones and business IDs
        sample_size = len(production_template)
        production_template = production_template[['Commodity_SCTG', 'NAICS', 'Size', 'NonTransportUnitCost', 'OutputCapacitylb']]
        candidate_zone = mesozone_to_faf_lookup.loc[mesozone_to_faf_lookup['FAFID'] == row['dms_orig']]
        sample_zones = candidate_zone.sample(n = sample_size, random_state = 1, replace = True)
        production_template = pd.concat([production_template.reset_index(), 
                                         sample_zones.reset_index()], axis=1)
        firms_added = pd.concat([firms_added, production_template], axis = 0)
    
        # break
    # <codecell>
    
    # format production results
    #1 = '1-19',2 = '20-99',3 ='100-499',4 = '500-999',5 = '1,000-2,499',6 = '2,500-4,999',7 = 'Over 5,000'
    firms_added_formatted = firms_added[['CBPZONE', 'FAFID', 'NAICS', 'Commodity_SCTG', 'OutputCapacitylb',
                                         'Size', 'NonTransportUnitCost', 'MESOZONE']]
    
    emp_cut_off = [0, 20, 100, 500, 1000, 2500, 5000, max(firms_added_formatted.Size.max() + 1, 10000)]
    emp_size_label = [1, 2, 3, 4, 5, 6, 7]
    
    firms_added_formatted.loc[:, 'esizecat'] = pd.cut(firms_added_formatted['Size'],
                                                      bins = emp_cut_off,
                                                      labels = emp_size_label,
                                                      right = False)
    
    max_id = firms_baseline.BusID.max()

    firms_added_formatted = firms_added_formatted.reset_index()
    firms_added_formatted.loc[:, 'BusID'] = firms_added_formatted.index + 1 + max_id
    firms_added_for_production = firms_added_formatted[['CBPZONE', 'FAFID', 'esizecat', 'NAICS',
           'Commodity_SCTG', 'Size', 'BusID', 'MESOZONE']]
    
    
    firms_added_for_production.columns = ['CBPZONE', 'FAFZONE', 'esizecat', 'Industry_NAICS6_Make',
           'Commodity_SCTG', 'Emp', 'BusID', 'MESOZONE']
    
    production_added_formatted = firms_added_formatted[['Commodity_SCTG', 'NAICS', 'Size', 'BusID', 'MESOZONE',
           'NonTransportUnitCost', 'OutputCapacitylb']]
    
    production_added_formatted.columns = ['Commodity_SCTG', 'NAICS', 'Size', 'SellerID', 'Zone',
           'NonTransportUnitCost', 'OutputCapacitylb']
    
    # <codecell>
    
    # generate domestic consumption adjustment factor
    consumption_baseline_agg = consumer_baseline.groupby(['FAFID', 'Commodity_SCTG'])[['PurchaseAmountlb']].sum()
    consumption_baseline_agg = consumption_baseline_agg.reset_index()
    consumption_baseline_agg.loc[:, 'tons_baseline'] = consumption_baseline_agg.loc[:, 'PurchaseAmountlb'] * lb_to_ton / 1000
    consumption_domestic_adj_factor = pd.merge(consumption_baseline_agg, 
                                               future_consumption,
                                              left_on = ['FAFID', 'Commodity_SCTG'],
                                              right_on = ['dms_dest', 'SCTG_Code'],
                                              how = 'outer')
    
    
    consumption_domestic_adj_factor.loc[:, forecast_tonnage] = \
        consumption_domestic_adj_factor.loc[:, forecast_tonnage].fillna(0)
        
    consumption_adj_factor_with_firms = \
        consumption_domestic_adj_factor.loc[~consumption_domestic_adj_factor[ 'PurchaseAmountlb'].isna()]
        
    consumption_adj_factor_no_firms = \
        consumption_domestic_adj_factor.loc[consumption_domestic_adj_factor[ 'PurchaseAmountlb'].isna()]    
    
    consumption_adj_factor_with_firms.loc[:, 'adj_factor'] = \
        consumption_adj_factor_with_firms.loc[:, forecast_tonnage] / \
            consumption_adj_factor_with_firms.loc[:, 'tons_baseline']
    
    # consumption_adj_factor_to_plot = consumption_adj_factor_with_firms.loc[consumption_adj_factor_with_firms['adj_factor'] <= 300]
    # consumption_adj_factor_to_plot.adj_factor.hist(bins = 500)
    # plt.xlim([0,50])    
    
    # <codecell>
    
    #projecting consumption of existing firms
    consumption_adj_factor_selected = \
        consumption_adj_factor_with_firms[['FAFID', 'Commodity_SCTG', 'adj_factor']]
    consumption_projected = pd.merge(consumer_baseline, consumption_adj_factor_selected,
                                             on = ['FAFID', 'Commodity_SCTG'], how = 'left')
    consumption_projected.loc[:, 'PurchaseAmountlb'] *= consumption_projected.loc[:, 'adj_factor']
    print('Total consumption tonnage after forecast:')
    print(consumption_projected['PurchaseAmountlb'].sum() * lb_to_ton / 1000)

    
    # <codecell>
    
    # fill missing firms and consumption
    additional_consumption_to_add = consumption_adj_factor_no_firms[['dms_dest', 'SCTG_Code', forecast_tonnage]]
    firms_added_2 = None
    for idx, row in additional_consumption_to_add.iterrows():
        SCTG = row['SCTG_Code']
        capacity_to_add = row[forecast_tonnage] * 2000 * 1000 # unit in lb
        # print(SCTG, capacity_to_add)
        
        #create list of firms with designated capacity
        consumption_template = \
            consumption_projected.loc[consumption_projected['Commodity_SCTG'] == SCTG]
        consumption_template = shuffle(consumption_template)
        consumption_template.loc[:, 'acc_capacity'] = \
            consumption_template.loc[:, 'PurchaseAmountlb'].cumsum()
        consumption_template = consumption_template.loc[consumption_template['acc_capacity'] <= capacity_to_add]
        scaling_factor = capacity_to_add / consumption_template.loc[:, 'PurchaseAmountlb'].sum()
        consumption_template.loc[:, 'PurchaseAmountlb'] *= scaling_factor
        # print(production_template.OutputCapacitylb.sum(), capacity_to_add)
        
        # allocate zones and business IDs
        sample_size = len(consumption_template)
        consumption_template = consumption_template[['Commodity_SCTG', 'NAICS', 'InputCommodity', 'Buyer.SCTG',
           'Size', 'ConVal', 'PurchaseAmountlb']]
        candidate_zone = mesozone_to_faf_lookup.loc[mesozone_to_faf_lookup['FAFID'] == row['dms_dest']]
        sample_zones = candidate_zone.sample(n = sample_size, random_state = 1, replace = True)
        consumption_template = pd.concat([consumption_template.reset_index(), 
                                         sample_zones.reset_index()], axis=1)
        firms_added_2 = pd.concat([firms_added_2, consumption_template], axis = 0)
    
    
    # <codecell>
    
    # format filled consumptions    
    #1 = '1-19',2 = '20-99',3 ='100-499',4 = '500-999',5 = '1,000-2,499',6 = '2,500-4,999',7 = 'Over 5,000'
    firms_added_formatted_2 = firms_added_2[['Commodity_SCTG', 'NAICS', 'InputCommodity', 'Buyer.SCTG',
           'Size', 'ConVal', 'PurchaseAmountlb', 'CBPZONE', 'MESOZONE', 'FAFID']]
    
    emp_cut_off = [0, 20, 100, 500, 1000, 2500, 5000, max(firms_added_formatted_2.Size.max() + 1, 10000)]
    emp_size_label = [1, 2, 3, 4, 5, 6, 7]
    
    firms_added_formatted_2.loc[:, 'esizecat'] = pd.cut(firms_added_formatted_2['Size'],
                                                      bins = emp_cut_off,
                                                      labels = emp_size_label,
                                                      right = False)
    
    max_id_2 = firms_added_for_production.BusID.max()
    # print(max_id_2)
    firms_added_formatted_2 = firms_added_formatted_2.reset_index()
    firms_added_formatted_2.loc[:, 'BusID'] = firms_added_formatted_2.index + 1 + max_id_2
    
    firms_added_for_consumption = firms_added_formatted_2[['CBPZONE', 'FAFID', 'esizecat', 'NAICS',
           'Buyer.SCTG', 'Size', 'BusID', 'MESOZONE']]
    
    
    firms_added_for_consumption.columns = ['CBPZONE', 'FAFZONE', 'esizecat', 'Industry_NAICS6_Make',
           'Commodity_SCTG', 'Emp', 'BusID', 'MESOZONE']
    
    consumption_added_formatted = firms_added_formatted_2[['Commodity_SCTG', 'NAICS', 'InputCommodity', 'MESOZONE', 'Buyer.SCTG',
           'BusID', 'Size', 'ConVal', 'PurchaseAmountlb']]
    
    consumption_added_formatted.columns = ['Commodity_SCTG', 'NAICS', 'InputCommodity', 'Zone', 'Buyer.SCTG',
           'BuyerID', 'Size', 'ConVal', 'PurchaseAmountlb']  
    
    # <codecell>
    
    # Assembly output
    firms = pd.concat([firms_baseline, firms_added_for_production, firms_added_for_consumption])  
    
    firms = firms[['CBPZONE', 'FAFZONE', 'esizecat', 'Industry_NAICS6_Make',
           'Commodity_SCTG', 'Emp', 'BusID', 'MESOZONE']]
    firms = firms.astype({
    'CBPZONE': np.int64,
    'FAFZONE': np.int64,
    'esizecat': np.int64, 
    'Industry_NAICS6_Make': 'string',
    'Commodity_SCTG': np.int64,
    'Emp': 'float',
    'BusID': np.int64, 
    'MESOZONE': np.int64
    })
    
    production_attr = ['Commodity_SCTG', 'NAICS', 'Size', 'SellerID', 'Zone',
           'NonTransportUnitCost', 'OutputCapacitylb']
    
    production = pd.concat([production_projected[production_attr],
                            production_added_formatted[production_attr]])
    production = production.astype({
        'SellerID': np.int64, 
        'Zone': np.int64, 
        'NAICS': 'string',
        'Commodity_SCTG': np.int64,
        'Size': 'float',
        'OutputCapacitylb': 'float',
        'NonTransportUnitCost':  'float'
        })
    consumption_attr = ['Commodity_SCTG', 'NAICS', 'InputCommodity', 
                        'Zone', 'Buyer.SCTG', 'BuyerID', 'Size', 
                        'ConVal', 'PurchaseAmountlb']
    consumption = pd.concat([consumption_projected[consumption_attr],
                             consumption_added_formatted[consumption_attr]])
    
    consumption = consumption.astype({
        'Commodity_SCTG': np.int64, 
        'BuyerID': np.int64, 
        'Zone': np.int64,  
        'NAICS': 'string', 
        'InputCommodity': 'string', 
        'PurchaseAmountlb': 'float'})
    
    print('Total number of firms after demand forecast:')
    print(len(firms))
    firms.to_csv(synthetic_firms_no_location_file, index = False)
    production.to_csv(producer_file, index = False)
    consumption.to_csv(consumer_file, index = False)
    
    # <codecell>
    
    # split consumer by sctg group
    consumption = pd.merge(consumption, sctg_lookup, 
                           left_on = 'Commodity_SCTG', right_on = 'SCTG_Code', how = 'left')
    

    for i in range(5):
        print("Processing SCTG Group " + str(i+1))
        g1_cons = consumption.loc[consumption['SCTG_Group'] == i+1]
        g1_cons = g1_cons[['SCTG_Group', 'Commodity_SCTG', 'BuyerID', 'Zone', 'NAICS', 'InputCommodity', 'PurchaseAmountlb']]
        g1_cons = g1_cons.astype({'SCTG_Group': 'int'})
        g1_cons.to_csv(os.path.join(output_path, consumer_by_sctg_filehead + str(i+1) + ".csv"), index = False)
 
    print('Demand scaling is done!')