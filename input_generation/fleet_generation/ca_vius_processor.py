#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 10 11:33:09 2025

@author: xiaodanxu
"""

from pandas import read_csv
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings("ignore")

plt.style.use('ggplot')
sns.set(font_scale=1.2)  # larger font

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')


# load input


path_to_cavius = 'PrivateData/CA_VIUS_2018/'
output_path = 'inputs_national/temporal_distribution'

cavius_file = 'CAVIUS_2018_SurveyData.csv'
cavius_data = read_csv(os.path.join(path_to_cavius, cavius_file))

cavius_attr = cavius_data.columns


# <codecell>

# Assign SynthFirm vehicle class
mdt_class = ['Class 3 (10,001-14,000 lbs)', 'Class 6 (19,501-26,000bs)',
 'Class 4 (14,001-16,000 lbs)',  'Class 5 (16,001-19,500 lbs)']
hdt_class = ['Class 8 (> 33,000 lbs)', 'Class 7 (26,001-33,000 lbs)']
cavius_data.loc[:, 'veh_class'] = None
cavius_data.loc[(cavius_data['GVW'].isin(mdt_class)) & \
                (cavius_data['VehicleType'] == 'Straight'), 'veh_class'] = 'MDT vocational'

cavius_data.loc[(cavius_data['GVW'].isin(mdt_class)) & \
                (cavius_data['VehicleType'] == 'Tractor'), 'veh_class'] = 'MDT tractor'
        
cavius_data.loc[(cavius_data['GVW'].isin(hdt_class)) & \
                (cavius_data['VehicleType'] == 'Straight'), 'veh_class'] = 'HDT vocational'
            
cavius_data.loc[(cavius_data['GVW'].isin(hdt_class)) & \
                (cavius_data['VehicleType'] == 'Tractor'), 'veh_class'] = 'HDT tractor'            
    
print(cavius_data.groupby(['veh_class'])['Weight'].sum())

# exclude MDT tractor
cavius_data = cavius_data.loc[cavius_data['veh_class'] != 'MDT tractor']

veh_count = cavius_data.groupby(['veh_class'])['Weight'].sum()
veh_count.plot(kind = 'bar')
plt.xticks(rotation = 60, ha = 'right')
plt.show()
# <codecell>

#### OUTPUT 1 -- VEHICLE TYPE BY RANGE OF OPERATION ####

# select a range
range_vars = ['0to50miles', '50to99miles', '100to149miles',
              '150to499miles', '500miles']
cavius_data.loc[:, 'range_bin'] = cavius_data[range_vars].idxmax(axis=1)
print(cavius_data.groupby(['range_bin']).size())

ro_count = cavius_data.groupby(['range_bin'])['Weight'].sum()
ro_count = ro_count.loc[range_vars]
ro_count.plot(kind = 'bar')
plt.xticks(rotation = 60, ha = 'right')
plt.show()
# <codecell>

# generate probability of truck type by range bin
veh_classes = ['MDT vocational', 'HDT vocational', 'HDT tractor'] # 'MDT tractor', 
veh_by_ro = pd.pivot_table(cavius_data, index = 'range_bin', 
                           columns = 'veh_class', values = 'Weight', aggfunc = 'sum')
veh_by_ro.loc[:, 'veh_count'] = veh_by_ro.sum(axis = 1)
veh_by_ro.loc[:, veh_classes] = \
    veh_by_ro.loc[:, veh_classes].div(veh_by_ro.loc[:, 'veh_count'], axis = 0)
    
veh_by_ro = veh_by_ro.loc[range_vars]
rgb_values = sns.color_palette("Paired", 4)
veh_by_ro.loc[:, veh_classes].plot(kind = 'bar', stacked = True, color = rgb_values)
plt.xticks(rotation = 60, ha = 'right')
plt.ylabel('Fraction of trucks')
plt.show()
# veh_by_ro = veh_by_ro.reset_index()


veh_by_ro = veh_by_ro.reset_index()
veh_by_ro.to_csv(os.path.join(output_path, 'veh_assignment_by_ro_bin.csv'), index = False)
# <codecell>

#### OUTPUT 2 -- AVG PAYLOAD BY VEH TYPE AND COMMODITY ####

# print(cavius_data.Commodity1.unique())

sctg_group_mapping = {'Food, beverage, tobacco products': 3, 
                      'Gravel / Sand and nonmetallic minerals': 1,
                      'Agriculture products': 3,
                      np.nan: 5,
                      'Manufactured products':4,
                      'Metal manufactured products': 4,
                      'Transportation equipment':4,
                      'Chemical / Pharmaceutical products': 2,
                      'Waste material':5,
                      'Electronics':4, 
                      'Wood': 1, 
                      'printed products':4, 
                      'Logs': 1, 
                      'Fuel and oil products':2, 'Crude petroleum':2,
                      'Nonmetal mineral products':4,
                      'Coal / Metallic minerals': 1}

sctg_group_desc = {1: 'Bulk (inc. coal)', 2: 'Fuel, fertilizer and other chemicals',
                   3: 'Interim product and food', 4: 'Manufactured good', 5: 'Others'}

cavius_data.loc[:, 'SCTG_Group'] = \
    cavius_data.loc[:, 'Commodity1'].map(sctg_group_mapping)
    
cavius_data.loc[:, 'sctg_desc']  = \
    cavius_data.loc[:, 'SCTG_Group'].map(sctg_group_desc)

print(cavius_data.groupby(['sctg_desc']).size())

# <codecell>
x_order = ['Bulk (inc. coal)', 'Fuel, fertilizer and other chemicals',
                   'Interim product and food', 'Manufactured good']
# range of payload by veh type and commodity
cavius_data_sel = cavius_data.loc[cavius_data['sctg_desc'] != 'Others']
sns.boxplot(cavius_data_sel, x = 'sctg_desc',  y = 'Payload1', order=x_order, 
            hue = 'veh_class', hue_order = veh_classes, 
            palette = 'Paired', showfliers = False
           )
plt.xticks(rotation = 60, ha = 'right')
plt.ylabel('Payload (lbs)')
plt.xlabel('Commodity group')
plt.legend(bbox_to_anchor = (1.01, 1))
plt.show()

# <codecell>

# calculate payload by veh type and commodity
lb_to_us_ton = 0.0005
cavius_data.loc[:, 'wgt_payload'] = cavius_data.loc[:, 'Payload1'] * cavius_data.loc[:, 'Weight']
avg_payload = cavius_data.groupby(['sctg_desc', 'veh_class'])[['Weight', 'wgt_payload']].sum()
avg_payload.loc[:, 'payload'] = avg_payload.loc[:, 'wgt_payload'] / \
    avg_payload.loc[:, 'Weight'] 
avg_payload = avg_payload.reset_index()

avg_payload = avg_payload.loc[avg_payload['sctg_desc'] != 'Others']
sns.barplot(avg_payload, x = 'sctg_desc',  y = 'payload',  order=x_order, 
            hue = 'veh_class', hue_order = veh_classes, 
            palette = 'Paired'
           )
plt.xticks(rotation = 60, ha = 'right')
plt.ylabel('Payload (lbs)')
plt.xlabel('Commodity group')
plt.legend(bbox_to_anchor = (1.01, 1))
plt.show()
avg_payload.loc[:, 'payload'] *= lb_to_us_ton

# <codecell>

#### OUTPUT 3 -- FRAC OF EMPTY TRIPS ####
cavius_data.loc[:, ['DeadheadingBobtail', 'DeadheadingEmpty']] = \
    cavius_data.loc[:, ['DeadheadingBobtail', 'DeadheadingEmpty']].fillna(0)
cavius_data.loc[:, 'Deadheading'] = cavius_data.loc[:, 'DeadheadingBobtail'] + \
    cavius_data.loc[:, 'DeadheadingEmpty']
cavius_data.loc[:, 'Deadheading'].hist(bins = 30)
plt.show()

# <codecell>
cavius_data.loc[:, 'wgt_deadhead'] = \
    cavius_data.loc[:, 'Deadheading'] * cavius_data.loc[:, 'Weight']
avg_deadhead = cavius_data.groupby(['sctg_desc', 'veh_class'])[['Weight', 'wgt_deadhead']].sum()
avg_deadhead.loc[:, 'Deadheading'] = avg_deadhead.loc[:, 'wgt_deadhead'] / \
    avg_deadhead.loc[:, 'Weight'] 
avg_deadhead = avg_deadhead.reset_index()

avg_deadhead = avg_deadhead.loc[avg_deadhead['sctg_desc'] != 'Others']
sns.barplot(avg_deadhead, x = 'sctg_desc',  y = 'Deadheading',  order=x_order, 
            hue = 'veh_class', hue_order = veh_classes, 
            palette = 'Paired'
           )
plt.xticks(rotation = 60, ha = 'right')
plt.ylabel('Percent of deadheading')
plt.xlabel('Commodity group')
plt.legend(bbox_to_anchor = (1.01, 1))
plt.show()

# <codecell>

# Deadheading by veh type and commodity

cavius_data_sel = cavius_data.loc[cavius_data['sctg_desc'] != 'Others']
sns.boxplot(cavius_data_sel, x = 'sctg_desc',  y = 'Deadheading',
            hue = 'veh_class', hue_order = veh_classes,  order=x_order, 
            palette = 'Paired', showfliers = False
           )
plt.xticks(rotation = 60, ha = 'right')
plt.ylabel('Percent of deadheading')
plt.xlabel('Commodity group')
plt.legend(bbox_to_anchor = (1.01, 1))
plt.show()

# <codecell>

# prepare output 2+3

avg_payload_deadhead = \
    cavius_data.groupby(['SCTG_Group', 'veh_class'])[['Weight', 'wgt_payload', 'wgt_deadhead']].sum()

avg_payload_deadhead = avg_payload_deadhead.reset_index()
avg_payload_deadhead = avg_payload_deadhead.loc[avg_payload_deadhead['SCTG_Group'] != 5]
avg_payload_deadhead.loc[:, 'payload'] = avg_payload_deadhead.loc[:, 'wgt_payload'] / \
    avg_payload_deadhead.loc[:, 'Weight'] * lb_to_us_ton

avg_payload_deadhead.loc[:, 'Deadheading'] = avg_payload_deadhead.loc[:, 'wgt_deadhead'] / \
    avg_payload_deadhead.loc[:, 'Weight'] 
    
avg_payload_deadhead = avg_payload_deadhead[['SCTG_Group', 'veh_class', 
                                             'payload', 'Deadheading']]

avg_payload_deadhead.to_csv(os.path.join(output_path, 'avg_payload_and_deadheading.csv'), index = False)

