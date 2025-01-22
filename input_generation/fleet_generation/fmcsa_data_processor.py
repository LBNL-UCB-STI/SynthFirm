#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 21 11:33:42 2025

@author: xiaodanxu
"""

import pandas as pd
from pandas import read_csv
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os

import warnings
warnings.filterwarnings("ignore")

plt.style.use('seaborn-v0_8-whitegrid')

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')

data_path = 'PrivateData/FMCSA'
carrier_data_file = 'Company_Census_File_20250116.csv'

fmcsa_carrier_data = read_csv(os.path.join(data_path, carrier_data_file))
print(fmcsa_carrier_data.columns)
print(len(fmcsa_carrier_data)) # 2,173,019 rows

# <codecell>

# initial cleaning

# us carriers only
fmcsa_carrier_data = fmcsa_carrier_data.loc[fmcsa_carrier_data['PHY_COUNTRY'] == 'US']

# keeping main US states
st_to_drop = ['PR', 'VI', 'AS', 'GU', 'MP']
fmcsa_carrier_data = fmcsa_carrier_data[~fmcsa_carrier_data['PHY_STATE'].isin(st_to_drop)]
# owning truck
fmcsa_carrier_data = fmcsa_carrier_data.loc[fmcsa_carrier_data['TRUCK_UNITS'] > 0]
# 1,869,374 rows

# drop record without class definition
fmcsa_carrier_data = fmcsa_carrier_data.loc[~fmcsa_carrier_data['CLASSDEF'].isna()]
# 1,869,351 rows

# fmcsa_carrier_data.loc[:, 'MCS150_MILEAGE_YEAR'] = \
#     fmcsa_carrier_data.loc[:, 'MCS150_MILEAGE_YEAR'].astype(int)
    
truck_count_by_year = \
    fmcsa_carrier_data.groupby('MCS150_MILEAGE_YEAR')[['TRUCK_UNITS']].sum()
# print(truck_count_by_year.head(5))

# eligible freight entities only
carrier_classes = fmcsa_carrier_data.CLASSDEF.unique()


fmcsa_carrier_data.loc[:, 'FOR-HIRE'] = \
    fmcsa_carrier_data['CLASSDEF'].str.contains('FOR HIRE')

# 132 missing values
fmcsa_carrier_data.loc[:, 'PRIVATE'] = \
    fmcsa_carrier_data['CLASSDEF'].str.contains('PRIVATE PROPERTY')

fmcsa_carrier_data.loc[:, 'CARRIER_TYPE'] = 'OTHER'

criteria_1 = (fmcsa_carrier_data.loc[:, 'FOR-HIRE'] == True) & \
    (fmcsa_carrier_data.loc[:, 'PRIVATE'] == False)
fmcsa_carrier_data.loc[criteria_1, 'CARRIER_TYPE'] = 'FOR-HIRE'

criteria_2 = (fmcsa_carrier_data.loc[:, 'FOR-HIRE'] == False) & \
    (fmcsa_carrier_data.loc[:, 'PRIVATE'] == True)
fmcsa_carrier_data.loc[criteria_2, 'CARRIER_TYPE'] = 'PRIVATE'

criteria_3 = (fmcsa_carrier_data.loc[:, 'FOR-HIRE'] == True) & \
    (fmcsa_carrier_data.loc[:, 'PRIVATE'] == True)
fmcsa_carrier_data.loc[criteria_3, 'CARRIER_TYPE'] = 'FOR-HIRE AND PRIVATE'

print(fmcsa_carrier_data.groupby('CARRIER_TYPE').size())
# print(fmcsa_carrier_data.groupby('CARRIER_TYPE')[['TRUCK_UNITS']].sum())


# drop record not in freight carriers
fmcsa_carrier_data = \
    fmcsa_carrier_data.loc[fmcsa_carrier_data['CARRIER_TYPE'] != 'OTHER']
# 1,824,146 rows

print(fmcsa_carrier_data.groupby('PRIOR_REVOKE_FLAG').size())
print(len(fmcsa_carrier_data)) 

# keep data subject to FMCSA regulation
fmcsa_carrier_data = \
    fmcsa_carrier_data.loc[fmcsa_carrier_data['CARRIER_OPERATION'].isin(['A', 'B'])]

print(fmcsa_carrier_data.groupby('CARRIER_TYPE')[['TRUCK_UNITS']].sum())

# <codecell>

# post-process carrier data
fleet_size_bin = [0, 2, 5, 10, 50, 100, 1000, fmcsa_carrier_data.TRUCK_UNITS.max()]
fleet_size_label = ['<=2', '3-5', '6-10', '11-50', '51-100', '101-1000', '>1000']

fmcsa_carrier_data.loc[:, 'FLEET_SIZE_BIN'] = \
    pd.cut(fmcsa_carrier_data['TRUCK_UNITS'], bins = fleet_size_bin, 
           labels = fleet_size_label, right=True)
    
carrier_summary_by_state = \
fmcsa_carrier_data.groupby(['PHY_STATE','CARRIER_TYPE', 'FLEET_SIZE_BIN']).agg({'TRUCK_UNITS':['sum', 'count']})
carrier_summary_by_state = carrier_summary_by_state.reset_index()
carrier_summary_by_state.columns = ['HB_STATE', 'CARRIER_TYPE', 'FLEET_SIZE', 'TRUCK_COUNT', 'CARRIER_COUNT']

# <codecell>

# plot state-level results
truck_count_by_state = pd.pivot_table(carrier_summary_by_state,
                                        index = 'HB_STATE', columns = 'FLEET_SIZE',
                                        values = 'TRUCK_COUNT', aggfunc='sum')

truck_count_by_state.loc[:, fleet_size_label] = \
    truck_count_by_state.loc[:, fleet_size_label].div(truck_count_by_state.loc[:, fleet_size_label].sum(axis=1), axis=0)

# plt.figure(figsize=(10,4))
truck_count_by_state.plot(kind = 'barh', stacked = True, cmap = 'coolwarm_r',
                          figsize=(6, 10))
# plt.xticks(rotation = 60, ha = 'right')
plt.legend(title = 'Fleet size', bbox_to_anchor = (1.01, 0.8))
plt.xlabel('Vehicle fraction')
plt.ylabel('Home-base state')
plt.savefig(os.path.join(data_path, 'FMCSA_truck_by_fleet_size.png'),
           dpi = 300, bbox_inches = 'tight')
plt.show()


# <codecell>

# plot state-level results
carrier_count_by_state = pd.pivot_table(carrier_summary_by_state,
                                        index = 'HB_STATE', columns = 'FLEET_SIZE',
                                        values = 'CARRIER_COUNT', aggfunc='sum')

carrier_count_by_state.loc[:, fleet_size_label] = \
    carrier_count_by_state.loc[:, fleet_size_label].div(carrier_count_by_state.loc[:, fleet_size_label].sum(axis=1), axis=0)

# plt.figure(figsize=(10,4))
carrier_count_by_state.plot(kind = 'barh', stacked = True, cmap = 'coolwarm_r',
                          figsize=(6, 10))
# plt.xticks(rotation = 60, ha = 'right')
plt.legend(title = 'Fleet size', bbox_to_anchor = (1.01, 0.8))
plt.xlabel('Carrier fraction')
plt.ylabel('Home-base state')
plt.savefig(os.path.join(data_path, 'FMCSA_carrier_by_fleet_size.png'),
            dpi = 300, bbox_inches = 'tight')
plt.show()

# <codecell>

# plot state-level results -carrier type
carrier_types = carrier_summary_by_state.CARRIER_TYPE.unique()
truck_count_by_state = pd.pivot_table(carrier_summary_by_state,
                                        index = 'HB_STATE', columns = 'CARRIER_TYPE',
                                        values = 'TRUCK_COUNT', aggfunc='sum')

truck_count_by_state.loc[:, carrier_types] = \
    truck_count_by_state.loc[:, carrier_types].div(truck_count_by_state.loc[:, carrier_types].sum(axis=1), axis=0)

# plt.figure(figsize=(10,4))
truck_count_by_state.plot(kind = 'barh', stacked = True, cmap = 'coolwarm_r',
                          figsize=(6, 10))
# plt.xticks(rotation = 60, ha = 'right')
plt.legend(title = 'Carrier type', bbox_to_anchor = (1.01, 0.8))
plt.xlabel('Vehicle fraction')
plt.ylabel('Home-base state')
plt.savefig(os.path.join(data_path, 'FMCSA_truck_by_carrier_type.png'),
           dpi = 300, bbox_inches = 'tight')
plt.show()


# <codecell>

# plot state-level results
carrier_count_by_state = pd.pivot_table(carrier_summary_by_state,
                                        index = 'HB_STATE', columns = 'CARRIER_TYPE',
                                        values = 'CARRIER_COUNT', aggfunc='sum')

carrier_count_by_state.loc[:, carrier_types] = \
    carrier_count_by_state.loc[:, carrier_types].div(carrier_count_by_state.loc[:, carrier_types].sum(axis=1), axis=0)

# plt.figure(figsize=(10,4))
carrier_count_by_state.plot(kind = 'barh', stacked = True, cmap = 'coolwarm_r',
                          figsize=(6, 10))
# plt.xticks(rotation = 60, ha = 'right')
plt.legend(title = 'Carrier type', bbox_to_anchor = (1.01, 0.8))
plt.xlabel('Carrier fraction')
plt.ylabel('Home-base state')
plt.savefig(os.path.join(data_path, 'FMCSA_carrier_by_carrier_type.png'),
            dpi = 300, bbox_inches = 'tight')
plt.show()
# <codecell>

# write carrier output
carrier_summary_by_state.loc[:, 'TRUCK_PER_CARRIER'] = \
    carrier_summary_by_state.loc[:, 'TRUCK_COUNT'] / carrier_summary_by_state.loc[:, 'CARRIER_COUNT']
carrier_summary_by_state['FLEET_SIZE']  = carrier_summary_by_state['FLEET_SIZE'].astype(str)
carrier_summary_by_state = carrier_summary_by_state.fillna(0)
carrier_summary_by_state.to_csv(os.path.join(data_path, 'FMCSA_statistics_by_fleet_size.csv'),
                                index = False)

truck_count_by_state = carrier_summary_by_state.groupby('HB_STATE')[['TRUCK_COUNT', 'CARRIER_COUNT']].sum()
truck_count_by_state = truck_count_by_state.reset_index()
truck_count_by_state.to_csv(os.path.join(data_path, 'FMCSA_truck_count_by_state.csv'),
                                index = False)
# create sample dataset
# fmcsa_carrier_data_sample = fmcsa_carrier_data.sample(10000)

# fmcsa_carrier_data_sample.to_csv(os.path.join(data_path, 'fmcsa_2025_sample.csv'),
#                                  index = False)