#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug  8 11:26:00 2023

@author: xiaodanxu
"""

import os
from pandas import read_csv
import pandas as pd
import geopandas as gps
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')

########## input and scenario definition ###########
medium_duty_class = [3, 4, 5, 6] # ref: https://afdc.energy.gov/data/10380
heavy_duty_class = [7, 8]

plt.style.use('ggplot')
sns.set(font_scale=1.2)  # larger font

min_size_lookup = {'0-2':1, '3-5':3, '6-10':6, 
                   '11-50':11, '51-100':51, '101-1000':101, 
                   '>1000':1001}

input_data_file = 'TEXAS_MDHDbybiz.csv'  # define vehicle input file
selected_state = 'TX'
output_dir = 'inputs_Austin/fleet/'

registration_data = read_csv('PrivateData/registration/' + input_data_file)

def fleet_composition_processor(data):
    size_interval = [-1, 2, 5, 10, 50, 100, 1000, data['totcount'].max()]
    interval_name = ['0-2', '3-5', '6-10', '11-50', '51-100', '101-1000', '>1000']  
    data.loc[:, 'size_group'] = pd.cut(data['totcount'], 
                                       bins = size_interval, right = True, 
                                       labels = interval_name)
    # private_truck_by_firm.head(5)
    fleet_by_size = data.groupby('size_group').agg({'business_name': 'count',
                                                    'totcount': ['sum','std'],
                                                    'DIESEL Vocational HDV': 'sum',
                                                    'DIESEL Tractor HDV': 'sum',
                                                    'DIESEL Vocational MDV': 'sum',
                                                    'GAS Vocational MDV': 'sum'})
    fleet_by_size = fleet_by_size.reset_index()
    fleet_by_size.columns = ['fleet_size', 'total_carriers', 'total_trucks', 'total_truck_std',
                            'DIESEL Vocational HDV', 'DIESEL Tractor HDV', 
                            'DIESEL Vocational MDV', 'GAS Vocational MDV']
    # print(truck_count_by_size.head(5))

    fleet_by_size.loc[:, 'min_size'] = fleet_by_size.loc[:, 'fleet_size'].map(min_size_lookup)
    fleet_by_size.loc[:, 'avg_truck_per_carrier'] = fleet_by_size.loc[:, 'total_trucks'] / fleet_by_size.loc[:, 'total_carriers']
    # truck_count_by_size.loc[:, 'avg_sut_per_carrier'] = truck_count_by_size.loc[:, 'total_single_trucks'] / truck_count_by_size.loc[:, 'total_carriers']
    # truck_count_by_size.loc[:, 'avg_ct_per_carrier'] = truck_count_by_size.loc[:, 'total_combination_trucks'] / truck_count_by_size.loc[:, 'total_carriers']
    fleet_by_size.loc[:, 'fraction_of_carrier'] = fleet_by_size.loc[:, 'total_carriers'] / fleet_by_size.loc[:, 'total_carriers'].sum()
    fleet_by_size.loc[:, 'DIESEL Vocational HDV'] = fleet_by_size.loc[:, 'DIESEL Vocational HDV'] / fleet_by_size.loc[:, 'total_trucks']
    fleet_by_size.loc[:, 'DIESEL Tractor HDV'] = fleet_by_size.loc[:, 'DIESEL Tractor HDV'] / fleet_by_size.loc[:, 'total_trucks']
    fleet_by_size.loc[:, 'DIESEL Vocational MDV'] = fleet_by_size.loc[:, 'DIESEL Vocational MDV'] / fleet_by_size.loc[:, 'total_trucks']
    fleet_by_size.loc[:, 'GAS Vocational MDV'] = fleet_by_size.loc[:, 'GAS Vocational MDV'] / fleet_by_size.loc[:, 'total_trucks']
    return(fleet_by_size)

# <codecell>

##### vehicle type assignment and selection #########
registration_data.loc[:, 'MDV'] = registration_data.loc[:, 'totcount'] * (registration_data.loc[:, 'vin_gvw'].isin(medium_duty_class)) + \
0 * (registration_data.loc[:, 'vin_gvw'].isin(heavy_duty_class))
registration_data.loc[:, 'HDV'] = 0 * (registration_data.loc[:, 'vin_gvw'].isin(medium_duty_class)) + \
registration_data.loc[:, 'totcount'] * (registration_data.loc[:, 'vin_gvw'].isin(heavy_duty_class))
print(registration_data.totcount.sum())
print(registration_data.fuel.unique())

registration_data.loc[:, 'vehicle_class'] = 'HDV'
registration_data.loc[registration_data['vin_gvw'].isin(medium_duty_class), 'vehicle_class'] = 'MDV'

truck_type_lookup = {'STRAIGHT TRUCK': 'Vocational',
                    'TRACTOR TRUCK': 'Tractor',
                    'CAB CHASSIS': 'Vocational',
                    'CUTAWAY': 'Vocational',
                    'INCOMPLETE (STRIP CHASSIS)': 'Vocational',
                    'STEP VAN': 'Vocational',
                    'VAN CARGO': 'Vocational',
                    'GLIDERS': 'Tractor',
                     'PICKUP': 'Vocational'}

# assign and filter body type
registration_data.loc[:, 'body type'] = registration_data.loc[:, 'vehicle_type'].map(truck_type_lookup)
registration_data = registration_data.dropna(subset = ['body type'])

hdv_fuels = ['DIESEL']
mdv_fuels = ['DIESEL', 'GAS']
fuel_filter_hdt = (registration_data.loc[:, 'vin_gvw'].isin(heavy_duty_class) ) & \
(registration_data.loc[:, 'fuel'].isin(hdv_fuels))
fuel_filter_mdt = (registration_data.loc[:, 'vin_gvw'].isin(medium_duty_class) ) & \
(registration_data.loc[:, 'fuel'].isin(mdv_fuels))

registration_data = registration_data.loc[fuel_filter_hdt | fuel_filter_mdt]
print(len(registration_data))

#create join vehicle type
registration_data.loc[:, 'vehicle type'] = registration_data.loc[:, 'fuel'] + ' ' + \
registration_data.loc[:, 'body type'] + ' ' + registration_data.loc[:, 'vehicle_class']
# filter by fuel

# <codecell>
##### private truck fleet distribution #########
# private truck analysis
list_of_vehicle_type = ['DIESEL Vocational HDV', 'DIESEL Tractor HDV',
                        'DIESEL Vocational MDV', 'GAS Vocational MDV'] # select type of trucks
private_carriers = registration_data.loc[registration_data['carrier_type'] == 'PRIVATE']
private_truck_by_firm = pd.pivot_table(private_carriers, index = 'business_name', 
                                       columns = 'vehicle type',
                                       values = 'totcount', aggfunc = np.sum)
private_truck_by_firm = private_truck_by_firm.reset_index()
private_truck_by_firm = private_truck_by_firm.fillna(0)
private_truck_by_firm.loc[:, 'totcount'] = private_truck_by_firm.loc[:, list_of_vehicle_type].sum(axis = 1)
private_truck_by_firm.head(5)
print('total private carriers ', len(private_truck_by_firm))
print('total private trucks ', private_truck_by_firm['totcount'].sum())
print('max private trucks in a firm', private_truck_by_firm['totcount'].max())
print('avg. private trucks in a firm', private_truck_by_firm['totcount'].mean())
print(private_truck_by_firm[list_of_vehicle_type].sum())
private_truck_by_firm['totcount'].hist(bins = 2000)
plt.xlim([0, 100])
plt.xlabel('truck count per firm')
plt.ylabel('count of firms')
plt.title('private truck count distribution')
plt.show()

private_truck_count_by_size = fleet_composition_processor(private_truck_by_firm)
private_truck_count_by_size['total_truck_std'] = \
    private_truck_count_by_size['total_truck_std'].fillna(1)
private_truck_count_by_size.to_csv(output_dir + selected_state + '_private_fleet_size_distribution.csv', 
                                   index = False, sep = ',')

# <codecell>
##### for-hire truck fleet distribution #########
for_hire_carriers = registration_data.loc[registration_data['carrier_type'] == 'FOR HIRE']


for_hire_truck_by_firm = pd.pivot_table(for_hire_carriers, index = 'business_name', 
                                       columns = 'vehicle type',
                                       values = 'totcount', aggfunc = np.sum)
for_hire_truck_by_firm = for_hire_truck_by_firm.reset_index()
for_hire_truck_by_firm = for_hire_truck_by_firm.fillna(0)
for_hire_truck_by_firm.loc[:, 'totcount'] = for_hire_truck_by_firm.loc[:, list_of_vehicle_type].sum(axis = 1)


print('total for-hire carriers ', len(for_hire_truck_by_firm))
print('total for-hire trucks ', for_hire_truck_by_firm['totcount'].sum())
print('max for-hire trucks in a firm', for_hire_truck_by_firm['totcount'].max())
print('avg. for-hire trucks in a firm', for_hire_truck_by_firm['totcount'].mean())

for_hire_truck_by_firm['totcount'].hist(bins = 100)
plt.xlim([0, 500])
plt.xlabel('truck count per firm')
plt.ylabel('count of firms')
plt.title('for-hire truck count distribution')
plt.show()

for_hire_truck_count_by_size = fleet_composition_processor(for_hire_truck_by_firm)
for_hire_truck_count_by_size['total_truck_std'] = \
    for_hire_truck_count_by_size['total_truck_std'].fillna(1)
for_hire_truck_count_by_size.to_csv(output_dir + selected_state + '_for_hire_fleet_size_distribution.csv', 
                                    index = False, sep = ',')


# <codecell>
##### for-lease truck fleet distribution #########
# for-leasing truck analysis
for_lease_carriers = registration_data.loc[registration_data['carrier_type'].isin(['FINANCE LEASE', \
                                                                                        'FULL SERVICE LEASE', \
                                                                                        'MANUFACTURER SPONSORED LEASE'])]


for_lease_truck_by_firm = pd.pivot_table(for_lease_carriers, index = 'business_name', 
                                       columns = 'vehicle type',
                                       values = 'totcount', aggfunc = np.sum)
for_lease_truck_by_firm = for_lease_truck_by_firm.reset_index()
for_lease_truck_by_firm = for_lease_truck_by_firm.fillna(0)
for_lease_truck_by_firm.loc[:, 'totcount'] = for_lease_truck_by_firm.loc[:, list_of_vehicle_type].sum(axis = 1)

print('total for-lease carriers ', len(for_lease_truck_by_firm))
print('total for-lease trucks ', for_lease_truck_by_firm['totcount'].sum())
print('max for-lease trucks in a firm', for_lease_truck_by_firm['totcount'].max())
print('avg. for-lease trucks in a firm', for_lease_truck_by_firm['totcount'].mean())

for_lease_truck_by_firm['totcount'].hist(bins = 100)
plt.xlim([0, 500])
plt.xlabel('truck count per firm')
plt.ylabel('count of firms')
plt.title('for-lease truck count distribution')
plt.show()

for_lease_truck_count_by_size = fleet_composition_processor(for_lease_truck_by_firm)
for_lease_truck_count_by_size['total_truck_std'] = \
    for_lease_truck_count_by_size['total_truck_std'].fillna(1)
for_lease_truck_count_by_size.to_csv(output_dir + selected_state + '_for_lease_fleet_size_distribution.csv', 
                                     index = False, sep = ',')


