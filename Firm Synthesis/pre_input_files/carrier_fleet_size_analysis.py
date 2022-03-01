#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb  9 13:54:20 2022

@author: xiaodanxu
"""

import os
from pandas import read_csv
import pandas as pd
import numpy as np

os.chdir('/Users/xiaodanxu/Documents/SynthFirm/BayArea_GIS')

carrier_census_data = read_csv('CENSUS_20220109_all.csv', sep = ',', encoding='latin1')
# carrier_census_data.to_csv('FMCSA_CENSUS1_2022Jan.csv', index = False)


# <codecell>
sample_carrier_data = carrier_census_data.head(1000)
sample_carrier_data.to_csv('FMCSA_CENSUS_sample.csv', index = False)
print(carrier_census_data.head(5))
print(carrier_census_data.columns)
#carrier_census_data = carrier_census_data.loc[carrier_census_data['CARSHIP'] == 'C'] # = CARRIER
carrier_census_data = carrier_census_data.loc[carrier_census_data['ACT_STAT'] == 'A'] # = ACTIVE
carrier_census_data = carrier_census_data.loc[carrier_census_data['PHY_NATN'] == 'US'] # IN US
carrier_census_data = carrier_census_data.loc[carrier_census_data['PASSENGERS'] != 'X'] # NO PASSENGER
carrier_census_data = carrier_census_data.loc[carrier_census_data['GENFREIGHT'] == 'X'] # NO PASSENGER
# carrier_census_data['TOT_PWR'].hist()
# <codecell>

# https://www.fleetowner.com/research/article/21703425/2019-fleet-owner-forhire-500-trucks
cut_off = carrier_census_data['TOT_TRUCKS'].quantile(0.999)
print(cut_off)
domestic_carrier_filtered = carrier_census_data.loc[carrier_census_data['TOT_TRUCKS'] <= cut_off]
print(domestic_carrier_filtered['TOT_TRUCKS'].sum())
domestic_carrier_filtered['TOT_TRUCKS'].hist(bins = 30)

list_of_var = ['OWNTRUCK', 'OWNTRACT', 'TRMTRUCK', 'TRMTRACT', 'TRPTRUCK', 'TRPTRACT', 'TOT_TRUCKS']
truck_per_carrier = domestic_carrier_filtered.groupby(['DOT_NUMBER'])[list_of_var].sum()

truck_per_carrier = truck_per_carrier.reset_index()
truck_per_carrier.loc[:, 'single_truck'] = truck_per_carrier.loc[:, 'OWNTRUCK'] + \
    truck_per_carrier.loc[:, 'TRMTRUCK'] + truck_per_carrier.loc[:, 'TRPTRUCK']
truck_per_carrier.loc[:, 'comb_truck'] = truck_per_carrier.loc[:, 'OWNTRACT'] + \
    truck_per_carrier.loc[:, 'TRMTRACT'] + truck_per_carrier.loc[:, 'TRPTRACT']  
    
size_interval = [-1, 2, 5, 10, 50, 100, 1000, truck_per_carrier['TOT_TRUCKS'].max()]
interval_name = ['0-2', '3-5', '6-10', '11-50', '51-100', '101-1000', '>1000']    
truck_per_carrier.loc[:, 'size_group'] = pd.cut(truck_per_carrier['TOT_TRUCKS'], 
                                                bins = size_interval, right = True, 
                                                labels = interval_name)

# <codecell>
truck_count_by_size = truck_per_carrier.groupby('size_group').agg({'DOT_NUMBER': 'count',
                                                                   'TOT_TRUCKS': 'sum',
                                                                   'single_truck': 'sum',
                                                                   'comb_truck': 'sum'})
truck_count_by_size = truck_count_by_size.reset_index()
truck_count_by_size.columns = ['fleet_size', 'total_carriers', 'total_trucks', 
                               'total_single_trucks', 'total_combination_trucks']

truck_count_by_size.loc[:, 'avg_truck_per_carrier'] = truck_count_by_size.loc[:, 'total_trucks'] / truck_count_by_size.loc[:, 'total_carriers']
truck_count_by_size.loc[:, 'avg_sut_per_carrier'] = truck_count_by_size.loc[:, 'total_single_trucks'] / truck_count_by_size.loc[:, 'total_carriers']
truck_count_by_size.loc[:, 'avg_ct_per_carrier'] = truck_count_by_size.loc[:, 'total_combination_trucks'] / truck_count_by_size.loc[:, 'total_carriers']
truck_count_by_size.loc[:, 'fraction_of_carrier'] = truck_count_by_size.loc[:, 'total_carriers'] / truck_count_by_size.loc[:, 'total_carriers'].sum()
truck_count_by_size.to_csv('fmcsa_census_fleet_size_distribution.csv', index = False, sep = ',')
# truck_per_carrier.loc[:, 'MCS150MILEAGEYEAR'] = truck_per_carrier.loc[:, 'MCS150MILEAGEYEAR'].astype(int)
#truck_per_carrier = truck_per_carrier.loc[truck_per_carrier['MCS150MILEAGEYEAR'] == 2019]