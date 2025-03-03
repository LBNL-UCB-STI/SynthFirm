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

# no passenger carriers
fmcsa_carrier_data = fmcsa_carrier_data.loc[fmcsa_carrier_data['CRGO_PASSENGERS'] != 'X']
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

#1,855,613 rows

print(fmcsa_carrier_data.groupby('CARRIER_TYPE').size())
# print(fmcsa_carrier_data.groupby('CARRIER_TYPE')[['TRUCK_UNITS']].sum())


# drop record not in freight carriers
fmcsa_carrier_data = \
    fmcsa_carrier_data.loc[fmcsa_carrier_data['CARRIER_TYPE'] != 'OTHER']
# 1,816,054 rows
 
# drop record being revoked
fmcsa_carrier_data = \
    fmcsa_carrier_data.loc[fmcsa_carrier_data['PRIOR_REVOKE_FLAG'] == 'N']

# 877,372 rows

print(len(fmcsa_carrier_data)) 
print(fmcsa_carrier_data.groupby('BUSINESS_ORG_ID')['TRUCK_UNITS'].sum())
print(fmcsa_carrier_data.groupby('CARRIER_OPERATION')['TRUCK_UNITS'].sum())
print(fmcsa_carrier_data.groupby('CARRIER_TYPE')[['TRUCK_UNITS']].sum())

# <codecell>

# keep data subject to FMCSA regulation
fmcsa_carrier_data_forhire = \
    fmcsa_carrier_data.loc[fmcsa_carrier_data['CARRIER_TYPE'].isin(['FOR-HIRE', 'FOR-HIRE AND PRIVATE'])]
# assign commodity group

headers = fmcsa_carrier_data_forhire.columns

# String to match at the start
prefix = 'CRGO'

# Find elements that start with the specified string
cargo_elements = [s for s in headers if s.startswith(prefix)]
cargo_elements.remove('CRGO_CARGOOTHR_DESC')
print(cargo_elements)

fmcsa_carrier_data_forhire[cargo_elements] = fmcsa_carrier_data_forhire[cargo_elements].replace(np.nan, 0)
fmcsa_carrier_data_forhire[cargo_elements] = fmcsa_carrier_data_forhire[cargo_elements].replace('X', 1)

SCTG_group_mapping = {
    1: ['CRGO_GENFREIGHT', 'CRGO_LOGPOLE', 'CRGO_BLDGMAT', 'CRGO_INTERMODAL',
        'CRGO_GRAINFEED', 'CRGO_COALCOKE', 'CRGO_DRYBULK', 'CRGO_CONSTRUCT'],
    2: ['CRGO_GENFREIGHT', 'CRGO_LIQGAS', 'CRGO_INTERMODAL', 'CRGO_CHEM'],
    3: ['CRGO_GENFREIGHT', 'CRGO_PRODUCE', 'CRGO_INTERMODAL', 'CRGO_LIVESTOCK',
        'CRGO_MEAT', 'CRGO_COLDFOOD', 'CRGO_BEVERAGES'],
    4: ['CRGO_GENFREIGHT', 'CRGO_HOUSEHOLD', 'CRGO_METALSHEET', 'CRGO_MOTOVEH',
        'CRGO_DRIVETOW', 'CRGO_MOBILEHOME', 'CRGO_MACHLRG', 'CRGO_INTERMODAL',
        'CRGO_USMAIL', 'CRGO_PAPERPROD', 'CRGO_FARMSUPP', 'CRGO_CONSTRUCT',
        'CRGO_WATERWELL'],
    5: ['CRGO_GENFREIGHT', 'CRGO_INTERMODAL', 'CRGO_OILFIELD', 'CRGO_GARBAGE',
        'CRGO_CARGOOTHR']}

list_of_sctgs = []
for col, values in SCTG_group_mapping.items():
    attr_to_add = 'SCTG' + str(col)
    list_of_sctgs.append(attr_to_add)
    fmcsa_carrier_data_forhire.loc[:, attr_to_add] = \
        fmcsa_carrier_data_forhire.loc[:, values].sum(axis = 1)
    fmcsa_carrier_data_forhire.loc[fmcsa_carrier_data_forhire[attr_to_add] > 1, attr_to_add] = 1
print(fmcsa_carrier_data_forhire[list_of_sctgs].head(5))

# <codecell>

cargo_groups = fmcsa_carrier_data_forhire.loc[:, list_of_sctgs]

cargo_groups_fraction = cargo_groups.sum()/len(cargo_groups)

cargo_groups_fraction = cargo_groups_fraction.to_frame()
cargo_groups_fraction = cargo_groups_fraction.reset_index()
cargo_groups_fraction.columns = ['SCTG_group', 'probability']
print(cargo_groups_fraction)
cargo_groups_fraction.to_csv(os.path.join(data_path, 'probability_of_cargo_group.csv'),
                             index = False)
# <codecell>

sample_cargo_data = fmcsa_carrier_data_forhire[list_of_sctgs].sample(100000)
corr_mat = sample_cargo_data.corr()
print(corr_mat)
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

# carrier_summary_by_state.loc[:, 'TRUCK_PER_CARRIER'] = \
#     carrier_summary_by_state.loc[:, 'TRUCK_COUNT'] / carrier_summary_by_state.loc[:, 'CARRIER_COUNT']
carrier_summary_by_state['FLEET_SIZE']  = carrier_summary_by_state['FLEET_SIZE'].astype(str)

carrier_summary_by_state = carrier_summary_by_state.fillna(0)
carrier_summary_by_state = carrier_summary_by_state.loc[carrier_summary_by_state['TRUCK_COUNT'] > 0]
# export for-hire statistics only
carrier_summary_by_state = \
    carrier_summary_by_state.loc[carrier_summary_by_state['CARRIER_TYPE'].isin(['FOR-HIRE', 'FOR-HIRE AND PRIVATE'])]
carrier_summary_by_state.to_csv(os.path.join(data_path, 'FMCSA_statistics_by_fleet_size.csv'),
                                index = False)

truck_count_by_state = carrier_summary_by_state.groupby(['HB_STATE'])[['TRUCK_COUNT', 'CARRIER_COUNT']].sum()
truck_count_by_state = truck_count_by_state.reset_index()
truck_count_by_state.to_csv(os.path.join(data_path, 'FMCSA_truck_count_by_state.csv'),
                                index = False)

truck_count_by_state_size = carrier_summary_by_state.groupby(['HB_STATE', 'FLEET_SIZE'])[['TRUCK_COUNT', 'CARRIER_COUNT']].sum()
truck_count_by_state_size = truck_count_by_state_size.reset_index()
truck_count_by_state_size.to_csv(os.path.join(data_path, 'FMCSA_truck_count_by_state_size.csv'),
                                index = False)
# create sample dataset
# fmcsa_carrier_data_sample = fmcsa_carrier_data.sample(10000)

# fmcsa_carrier_data_sample.to_csv(os.path.join(data_path, 'fmcsa_2025_sample.csv'),
#                                  index = False)