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
import gc
import warnings
import matplotlib.pyplot as plt

warnings.filterwarnings("ignore")

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')

# load inputs
firms_baseline = read_csv('outputs/synthetic_firms_v2.csv')
production_baseline = read_csv('outputs/synthetic_producers_V2.csv')
consumer_baseline = read_csv('outputs/synthetic_consumers_V2.csv')

future_production = read_csv("inputs/total_commodity_production_2040.csv")
future_consumption = read_csv("inputs/total_commodity_attraction_2040.csv")

#load parameters
mesozone_to_faf_lookup = read_csv("inputs/zonal_id_lookup_final.csv")
sctg_lookup = read_csv('inputs/SCTG_Groups_revised.csv')
industry_commodity_lookup = read_csv('inputs/corresp_naics6_n6io_sctg_revised.csv')

# <codecell>

# select domestic firms for forecast
production_baseline = pd.merge(production_baseline, mesozone_to_faf_lookup,
                               left_on = 'Zone', right_on = 'MESOZONE', how = 'left')

consumer_baseline = pd.merge(consumer_baseline, mesozone_to_faf_lookup,
                               left_on = 'Zone', right_on = 'MESOZONE', how = 'left')

production_baseline_domestic = production_baseline.loc[production_baseline['Zone'] < 30000]
production_baseline_foreign = production_baseline.loc[production_baseline['Zone'] >= 30000]

consumer_baseline_domestic = consumer_baseline.loc[consumer_baseline['Zone'] < 30000]
consumer_baseline_foreign = consumer_baseline.loc[consumer_baseline['Zone'] >= 30000]

# <codecell>

# generate domostic production adj factor
lb_to_ton = 1/2000
forecast_year = '2040'
forecast_tonnage = 'tons_' + forecast_year

production_baseline_domestic_agg = production_baseline_domestic.groupby(['FAFID', 'Commodity_SCTG'])[['OutputCapacitylb']].sum()
production_baseline_domestic_agg = production_baseline_domestic_agg.reset_index()
production_baseline_domestic_agg.loc[:, 'tons_baseline'] = production_baseline_domestic_agg.loc[:, 'OutputCapacitylb'] * lb_to_ton / 1000
production_domestic_adj_factor = pd.merge(production_baseline_domestic_agg, future_production,
                                          left_on = ['FAFID', 'Commodity_SCTG'],
                                          right_on = ['dms_orig', 'SCTG_Code'],
                                          how = 'outer')

production_domestic_adj_factor.head(5)

production_domestic_adj_factor.loc[:, forecast_tonnage] = \
    production_domestic_adj_factor.loc[:, forecast_tonnage].fillna(0)
    
production_adj_factor_with_firms = \
    production_domestic_adj_factor.loc[~production_domestic_adj_factor[ 'OutputCapacitylb'].isna()]
    
production_adj_factor_no_firms = \
    production_domestic_adj_factor.loc[production_domestic_adj_factor[ 'OutputCapacitylb'].isna()]    

production_adj_factor_with_firms.loc[:, 'adj_factor'] = \
    production_adj_factor_with_firms.loc[:, forecast_tonnage] / \
        production_adj_factor_with_firms.loc[:, 'tons_baseline']

production_adj_factor_to_plot = production_adj_factor_with_firms.loc[production_adj_factor_with_firms['adj_factor'] <= 300]
production_adj_factor_to_plot.adj_factor.hist(bins = 500)
plt.xlim([0,50])        
# <codecell>
from sklearn.utils import shuffle

#projecting production of existing firms
production_adj_factor_selected = production_adj_factor_with_firms[['FAFID', 'Commodity_SCTG', 'adj_factor']]
production_domestic_projected = pd.merge(production_baseline_domestic, production_adj_factor_selected,
                                         on = ['FAFID', 'Commodity_SCTG'], how = 'left')
production_domestic_projected.loc[:, 'OutputCapacitylb'] *= production_domestic_projected.loc[:, 'adj_factor']

print(production_domestic_projected['OutputCapacitylb'].sum() * lb_to_ton / 1000)
print(production_adj_factor_with_firms[forecast_tonnage].sum())

# <codecell>
# fill missing firms and productions
# production_upper_bound = 500000000 * lb_to_ton / 1000 # hypothetical production capacity assumed in SynthFirm
additional_production_to_add = production_adj_factor_no_firms[['dms_orig', 'SCTG_Code', forecast_tonnage]]
firms_added = None
for idx, row in additional_production_to_add.iterrows():
    SCTG = row['SCTG_Code']
    capacity_to_add = row[forecast_tonnage] * 2000 * 1000 # unit in lb
    print(SCTG, capacity_to_add)
    
    #create list of firms with designated capacity
    production_template = production_domestic_projected.loc[production_domestic_projected['Commodity_SCTG'] == SCTG]
    production_template = shuffle(production_template)
    production_template.loc[:, 'acc_capacity'] = \
        production_template.loc[:, 'OutputCapacitylb'].cumsum()
    production_template = production_template.loc[production_template['acc_capacity'] <= capacity_to_add]
    scaling_factor = capacity_to_add / production_template.loc[:, 'OutputCapacitylb'].sum()
    production_template.loc[:, 'OutputCapacitylb'] *= scaling_factor
    # print(production_template.OutputCapacitylb.sum(), capacity_to_add)
    
    # allocate zones and business IDs
    sample_size = len(production_template)
    production_template = production_template[['Commodity_SCTG', 'NAICS', 'Size', 'NonTransportUnitCost', 'OutputCapacitylb', 'OutputCommodity']]
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
                                     'Size', 'NonTransportUnitCost', 'MESOZONE', 'OutputCommodity']]

emp_cut_off = [0, 20, 100, 500, 1000, 2500, 5000, max(firms_added_formatted.Size.max() + 1, 10000)]
emp_size_label = [1, 2, 3, 4, 5, 6, 7]

firms_added_formatted.loc[:, 'esizecat'] = pd.cut(firms_added_formatted['Size'],
                                                  bins = emp_cut_off,
                                                  labels = emp_size_label,
                                                  right = False)

max_id = firms_baseline.BusID.max()
firms_added_formatted.loc[:, 'BusID'] = firms_added_formatted.index + 1 + max_id
firms_added_for_production = firms_added_formatted[['CBPZONE', 'FAFID', 'esizecat', 'NAICS',
       'Commodity_SCTG', 'Size', 'BusID', 'MESOZONE']]


firms_added_for_production.columns = ['CBPZONE', 'FAFZONE', 'esizecat', 'Industry_NAICS6_Make',
       'Commodity_SCTG', 'Emp', 'BusID', 'MESOZONE']

production_added_formatted = firms_added_formatted[['Commodity_SCTG', 'NAICS', 'Size', 'BusID', 'MESOZONE',
       'NonTransportUnitCost', 'OutputCapacitylb', 'OutputCommodity']]

production_added_formatted.columns = ['Commodity_SCTG', 'NAICS', 'Size', 'SellerID', 'Zone',
       'NonTransportUnitCost', 'OutputCapacitylb', 'OutputCommodity']

# <codecell>

# generate domestic consumption adjustment factor
consumption_baseline_domestic_agg = consumer_baseline_domestic.groupby(['FAFID', 'Commodity_SCTG'])[['PurchaseAmountlb']].sum()
consumption_baseline_domestic_agg = consumption_baseline_domestic_agg.reset_index()
consumption_baseline_domestic_agg.loc[:, 'tons_baseline'] = consumption_baseline_domestic_agg.loc[:, 'PurchaseAmountlb'] * lb_to_ton / 1000
consumption_domestic_adj_factor = pd.merge(consumption_baseline_domestic_agg, 
                                           future_consumption,
                                          left_on = ['FAFID', 'Commodity_SCTG'],
                                          right_on = ['dms_dest', 'SCTG_Code'],
                                          how = 'outer')

consumption_domestic_adj_factor.head(5)

consumption_domestic_adj_factor.loc[:, forecast_tonnage] = \
    consumption_domestic_adj_factor.loc[:, forecast_tonnage].fillna(0)
    
consumption_adj_factor_with_firms = \
    consumption_domestic_adj_factor.loc[~consumption_domestic_adj_factor[ 'PurchaseAmountlb'].isna()]
    
consumption_adj_factor_no_firms = \
    consumption_domestic_adj_factor.loc[consumption_domestic_adj_factor[ 'PurchaseAmountlb'].isna()]    

consumption_adj_factor_with_firms.loc[:, 'adj_factor'] = \
    consumption_adj_factor_with_firms.loc[:, forecast_tonnage] / \
        consumption_adj_factor_with_firms.loc[:, 'tons_baseline']

consumption_adj_factor_to_plot = consumption_adj_factor_with_firms.loc[consumption_adj_factor_with_firms['adj_factor'] <= 300]
consumption_adj_factor_to_plot.adj_factor.hist(bins = 500)
plt.xlim([0,50])    

# <codecell>

#projecting consumption of existing firms
consumption_adj_factor_selected = consumption_adj_factor_with_firms[['FAFID', 'Commodity_SCTG', 'adj_factor']]
consumption_domestic_projected = pd.merge(consumer_baseline_domestic, consumption_adj_factor_selected,
                                         on = ['FAFID', 'Commodity_SCTG'], how = 'left')
consumption_domestic_projected.loc[:, 'PurchaseAmountlb'] *= consumption_domestic_projected.loc[:, 'adj_factor']

print(consumption_domestic_projected['PurchaseAmountlb'].sum() * lb_to_ton / 1000)
print(consumption_adj_factor_with_firms[forecast_tonnage].sum())

# <codecell>

# fill missing firms and consumption
additional_consumption_to_add = consumption_adj_factor_no_firms[['dms_dest', 'SCTG_Code', forecast_tonnage]]
firms_added_2 = None
for idx, row in additional_consumption_to_add.iterrows():
    SCTG = row['SCTG_Code']
    capacity_to_add = row[forecast_tonnage] * 2000 * 1000 # unit in lb
    print(SCTG, capacity_to_add)
    
    #create list of firms with designated capacity
    consumption_template = consumption_domestic_projected.loc[consumption_domestic_projected['Commodity_SCTG'] == SCTG]
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
       'Size', 'ConVal', 'PurchaseAmountlb', 'PrefWeight1_UnitCost',
       'PrefWeight2_ShipTime', 'SingleSourceMaxFraction', 'OutputCommodity']]
    candidate_zone = mesozone_to_faf_lookup.loc[mesozone_to_faf_lookup['FAFID'] == row['dms_dest']]
    sample_zones = candidate_zone.sample(n = sample_size, random_state = 1, replace = True)
    consumption_template = pd.concat([consumption_template.reset_index(), 
                                     sample_zones.reset_index()], axis=1)
    firms_added_2 = pd.concat([firms_added_2, consumption_template], axis = 0)


# <codecell>

# format filled consumptions    
#1 = '1-19',2 = '20-99',3 ='100-499',4 = '500-999',5 = '1,000-2,499',6 = '2,500-4,999',7 = 'Over 5,000'
firms_added_formatted_2 = firms_added_2[['Commodity_SCTG', 'NAICS', 'InputCommodity', 'Buyer.SCTG',
       'Size', 'ConVal', 'PurchaseAmountlb', 'PrefWeight1_UnitCost',
       'PrefWeight2_ShipTime', 'SingleSourceMaxFraction', 'OutputCommodity',
       'CBPZONE', 'MESOZONE', 'FAFID']]

emp_cut_off = [0, 20, 100, 500, 1000, 2500, 5000, max(firms_added_formatted_2.Size.max() + 1, 10000)]
emp_size_label = [1, 2, 3, 4, 5, 6, 7]

firms_added_formatted_2.loc[:, 'esizecat'] = pd.cut(firms_added_formatted_2['Size'],
                                                  bins = emp_cut_off,
                                                  labels = emp_size_label,
                                                  right = False)

max_id_2 = firms_added_for_production.BusID.max()
firms_added_formatted_2.loc[:, 'BusID'] = firms_added_formatted_2.index + 1 + max_id_2

firms_added_for_consumption = firms_added_formatted_2[['CBPZONE', 'FAFID', 'esizecat', 'NAICS',
       'Buyer.SCTG', 'Size', 'BusID', 'MESOZONE']]


firms_added_for_consumption.columns = ['CBPZONE', 'FAFZONE', 'esizecat', 'Industry_NAICS6_Make',
       'Commodity_SCTG', 'Emp', 'BusID', 'MESOZONE']

consumption_added_formatted = firms_added_formatted_2[['Commodity_SCTG', 'NAICS', 'InputCommodity', 'MESOZONE', 'Buyer.SCTG',
       'BusID', 'Size', 'ConVal', 'PurchaseAmountlb', 'PrefWeight1_UnitCost',
       'PrefWeight2_ShipTime', 'SingleSourceMaxFraction', 'OutputCommodity']]

consumption_added_formatted.columns = ['Commodity_SCTG', 'NAICS', 'InputCommodity', 'Zone', 'Buyer.SCTG',
       'BuyerID', 'Size', 'ConVal', 'PurchaseAmountlb', 'PrefWeight1_UnitCost',
       'PrefWeight2_ShipTime', 'SingleSourceMaxFraction', 'OutputCommodity']  

# <codecell>

# Assembly output
firms = pd.concat([firms_baseline, firms_added_for_production, firms_added_for_consumption])  

production_attr = ['Commodity_SCTG', 'NAICS', 'Size', 'SellerID', 'Zone',
       'NonTransportUnitCost', 'OutputCapacitylb', 'OutputCommodity']

production = pd.concat([production_domestic_projected[production_attr],
                        production_baseline_foreign[production_attr],
                        production_added_formatted[production_attr]])

consumption_attr = ['Commodity_SCTG', 'NAICS', 'InputCommodity', 'Zone', 'Buyer.SCTG',
       'BuyerID', 'Size', 'ConVal', 'PurchaseAmountlb', 'PrefWeight1_UnitCost',
       'PrefWeight2_ShipTime', 'SingleSourceMaxFraction', 'OutputCommodity']
consumption = pd.concat([consumption_domestic_projected[consumption_attr],
                         consumer_baseline_foreign[consumption_attr],
                         consumption_added_formatted[consumption_attr]])

production_to_check_sctg2 = production.loc[production['Commodity_SCTG'].isin([16,17,18,19,20,22,23])]
production_to_check_sctg2 = production_to_check_sctg2.loc[production_to_check_sctg2['Zone']< 20000]
# print(production_to_check_sctg2.OutputCapacitylb.sum()/2000/1000)
firms.to_csv('outputs_aus_2040/forecasted_firms.csv', index = False)
production.to_csv('outputs_aus_2040/forecasted_production.csv', index = False)
consumption.to_csv('outputs_aus_2040/forecasted_consumption.csv', index = False)

# <codecell>

# split consumer by sctg group
consumption = pd.merge(consumption, sctg_lookup, 
                       left_on = 'Commodity_SCTG', right_on = 'SCTG_Code', how = 'left')

consumption = consumption[['SCTG_Group', 'Commodity_SCTG', 'BuyerID', 'Zone', 'NAICS', 
                           'InputCommodity', 'PurchaseAmountlb', 'SingleSourceMaxFraction']]
sctg_groups = sctg_lookup.SCTG_Group.unique()
for sg in sctg_groups:
    consumption_out = consumption.loc[consumption['SCTG_Group'] == sg]
    consumption_out.to_csv('outputs_aus_2040/forecasted_consumption_sctg' + str(sg) + '.csv', index = False)