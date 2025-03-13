#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 11 13:48:31 2025

@author: xiaodanxu
"""

import pandas as pd
import os
import numpy as np
from pandas import read_csv
import warnings
warnings.filterwarnings("ignore")


########################################################
#### step 1 - configure environment and load inputs ####
########################################################

scenario_name = 'Seattle'
out_scenario_name = 'Seattle'
file_path = '/Users/xiaodanxu/Documents/SynthFirm.nosync'
parameter_dir = 'SynthFirm_parameters'
number_of_processes = 4
input_dir = 'inputs_' + scenario_name
output_path = 'outputs_' + out_scenario_name

fleet_year = 2018
fleet_name = 'Ref_highp6'
regulations = 'ACC and ACT'

# define input files --> eventually loading to pipeline
os.chdir(file_path)   

# from upstream module
synthetic_firms_with_location_file = os.path.join(output_path, 
                                                  'synthetic_firms_with_location_cal.csv')

# inputs not vary by scenario
private_fleet_file = os.path.join(parameter_dir, 'fleet/veh_per_emp_by_state.csv')
for_hire_fleet_file = os.path.join(parameter_dir, 'fleet/FMCSA_truck_count_by_state_size.csv')
cargo_type_distribution_file = os.path.join(parameter_dir, 'fleet/probability_of_cargo_group.csv')
state_fips_lookup_file = os.path.join(parameter_dir, 'us-state-ansi-fips.csv')

# inputs vary by scenario
ev_availability_file = os.path.join(parameter_dir, 'fleet/synthfirm_ev_availability.csv')
private_fuel_mix_file = os.path.join(parameter_dir, 'fleet/private_fuel_mix_scenario.csv')
hire_fuel_mix_file = os.path.join(parameter_dir, 'fleet/hire_fuel_mix_scenario.csv')
lease_fuel_mix_file = os.path.join(parameter_dir, 'fleet/lease_fuel_mix_scenario.csv')

private_stock_file = os.path.join(parameter_dir, 'fleet/private_stock_projection.csv')
hire_stock_file = os.path.join(parameter_dir, 'fleet/hire_stock_projection.csv')
lease_stock_file = os.path.join(parameter_dir, 'fleet/lease_stock_projection.csv')
# define output files --> eventually loading to pipeline
scenario_name = fleet_name + ' & ' + regulations
result_dir = os.path.join(output_path, str(fleet_year), scenario_name)
isExist = os.path.exists(result_dir)
if not isExist:
   # Create a new directory because it does not exist
   os.makedirs(result_dir)
   
firms_with_fleet_file = os.path.join(result_dir, 'synthetic_firms_with_fleet.csv')
carriers_with_fleet_file = os.path.join(result_dir, 'synthetic_carriers.csv')
leasing_with_fleet_file = os.path.join(result_dir, 'synthetic_leasing_company.csv')

# <codecell>

print('Starting national fleet generation...')

# loading input
firms = read_csv(synthetic_firms_with_location_file)
private_fleet = read_csv(private_fleet_file)
for_hire_fleet = read_csv(for_hire_fleet_file)
cargo_type_distribution = read_csv(cargo_type_distribution_file)
state_fips_lookup = read_csv(state_fips_lookup_file)
state_fips_lookup.loc[:, 'stusps'] = state_fips_lookup.loc[:, 'stusps'].str[1:]
# loading forecast values
private_fuel_mix = read_csv(private_fuel_mix_file)
hire_fuel_mix = read_csv(hire_fuel_mix_file)
lease_fuel_mix = read_csv(lease_fuel_mix_file)

private_stock = read_csv(private_stock_file)
hire_stock = read_csv(hire_stock_file)
lease_stock = read_csv(lease_stock_file)

firms.fillna(-1, inplace = True)

ev_availability = read_csv(ev_availability_file)

# generate list of all possible vehicle + fuel combinations across all years + scenarios 

private_fuel_mix.loc[:, 'veh_type'] = \
    private_fuel_mix.loc[:, 'fuel type'] + ' ' + private_fuel_mix.loc[:, 'veh_class']
veh_comb = private_fuel_mix.veh_type.unique()

# select forecasted data under current scenario
private_fuel_mix = \
    private_fuel_mix.loc[(private_fuel_mix['scenario'] == fleet_name) & 
                         (private_fuel_mix['rules'] == regulations) &
                         (private_fuel_mix['yearID'] == fleet_year)]
hire_fuel_mix = \
    hire_fuel_mix.loc[(hire_fuel_mix['scenario'] == fleet_name) & 
                         (hire_fuel_mix['rules'] == regulations)&
                         (hire_fuel_mix['yearID'] == fleet_year)]
lease_fuel_mix = \
    lease_fuel_mix.loc[(lease_fuel_mix['scenario'] == fleet_name) & 
                         (lease_fuel_mix['rules'] == regulations)&
                         (hire_fuel_mix['yearID'] == fleet_year)]
    
ev_availability = ev_availability.loc[ev_availability['scenario'] == fleet_name]

private_stock = private_stock.loc[private_stock['yearID'] == fleet_year]
hire_stock = hire_stock.loc[hire_stock['yearID'] == fleet_year]
lease_stock = lease_stock.loc[lease_stock['yearID'] == fleet_year]

vehicle_types = private_fuel_mix.veh_class.unique()

# drop missing in data
private_fleet.replace(np.inf, np.nan, inplace = True)
private_fleet.fillna(0, inplace = True)

print(private_fleet[vehicle_types].sum())
# <codecell>

# generate fleet size
private_fleet.loc[:, 'industry'] = private_fleet.loc[:, 'industry'].astype(str)
firms.loc[:, 'industry'] = firms.loc[:, 'Industry_NAICS6_Make'].astype(str).str[0:2]
firms.loc[firms['industry'].isin(["31", "32", "33"]), 'industry'] = "3133"
firms.loc[firms['industry'].isin(["44", "45", "4A"]), 'industry'] = "4445"
firms.loc[firms['industry'].isin(["48", "49"]), 'industry'] = "4849"
firms.loc[firms['industry'].isin(["S0"]), 'industry'] = "92"
# print(firms.industry.unique())

attr_to_keep = ['state_abbr', 'industry']
for veh in vehicle_types:
    rate_attr = 'rate_' + veh
    attr_to_keep.append(rate_attr)
# print(attr_to_keep)

private_fleet = private_fleet[attr_to_keep]

firms.loc[:, 'FAFZONE'] = firms.loc[:,'FAFZONE'].astype(str).str.zfill(3)    
firms.loc[:, 'st'] = firms.loc[:, 'FAFZONE'].str[:2]
firms.loc[:, 'st'] = firms.loc[:, 'st'].astype(int)
    
firms = pd.merge(firms, state_fips_lookup, on = 'st', how = 'left')
    # print(len(firms))
firms.rename(columns = {'stusps':'state_abbr'}, inplace = True)

# calculate firm fleet by emp * rate

firms_with_fleet = pd.merge(firms, private_fleet, 
                            on = ['state_abbr', 'industry'], how = 'left')
to_drop = []
for veh in vehicle_types:
    rate_attr = 'rate_' + veh
    to_drop.append(rate_attr)
    firms_with_fleet.loc[:, veh] = firms_with_fleet.loc[:, 'Emp'] * \
        firms_with_fleet.loc[:, rate_attr]
    
print(firms_with_fleet[vehicle_types].sum())
firms_with_fleet.drop(columns = to_drop, inplace = True)

# <codecell>

# split data into three sets
carriers = \
    firms_with_fleet.loc[firms_with_fleet['Industry_NAICS6_Make'].isin(['492000', '484000'])]
    
leasing = firms_with_fleet.loc[firms_with_fleet['Industry_NAICS6_Make'].isin(['532100'])]

private_fleet = \
    firms_with_fleet[~firms_with_fleet['Industry_NAICS6_Make'].isin(['492000', '484000', '532100'])]
private_fleet.loc[:, vehicle_types] = np.round(private_fleet.loc[:, vehicle_types], 0)

# <codecell>
private_fleet.loc[:, 'n_trucks'] = private_fleet.loc[:, vehicle_types].sum(axis = 1)
private_fleet_no_truck = private_fleet.loc[private_fleet['n_trucks'] == 0]
private_fleet_no_truck.drop(columns = vehicle_types, inplace = True)
private_fleet_truck = private_fleet.loc[private_fleet['n_trucks'] > 0]
print(private_fleet_truck[vehicle_types].sum())

# <codecell>
# process fuel type mix
fuel_attr = ['veh_class', 'state_abbr', 'fuel type', 'veh_fraction']
private_fuel_mix = private_fuel_mix[fuel_attr]
fuel_types = private_fuel_mix['fuel type'].unique()

idx_var = ['CBPZONE', 'FAFZONE', 'esizecat', 'Industry_NAICS6_Make',
       'Commodity_SCTG', 'Emp', 'BusID', 'MESOZONE', 'ZIPCODE', 'lat', 'lon',
       'ParcelID', 'TAZ', 'state_abbr']
# print(private_fleet[vehicle_types].sum())

private_fleet = pd.melt(private_fleet_truck, id_vars = idx_var, value_vars = vehicle_types,
                        var_name='veh_class', value_name='number_of_veh')
# keep veh types with non-zero values
private_fleet = private_fleet.loc[private_fleet['number_of_veh'] > 0]

# append fuel mix and randomly assign fuel types
private_fleet = pd.merge(private_fleet, private_fuel_mix,
                         on = ['veh_class', 'state_abbr'], how = 'left')

# print(private_fleet.isna().sum())

# <codecell>
fleet_threshold = 20
# for large fleet, assign fuel type by fraction; for small fleet, randomly assign them to single fuel
private_fleet_large = private_fleet.loc[private_fleet['number_of_veh'] >= fleet_threshold]
private_fleet_large.loc[:, 'number_of_veh'] = \
    private_fleet_large.loc[:, 'number_of_veh'] * private_fleet_large.loc[:, 'veh_fraction']
private_fleet_large.loc[:, 'number_of_veh'] = \
    np.round(private_fleet_large.loc[:, 'number_of_veh'], 0)
    
private_fleet_small = private_fleet.loc[private_fleet['number_of_veh'] < fleet_threshold]
idx_var.append('veh_class')
private_fleet_small = private_fleet_small.groupby(idx_var).sample(n=1, 
                                      weights = private_fleet['veh_fraction'],
                                      replace = False, random_state = 1)


# <codecell>

# combine small and large fleets

private_fleet_truck = pd.concat([private_fleet_large, private_fleet_small])
private_fleet_truck.loc[:, 'veh_type'] = \
    private_fleet_truck.loc[:, 'fuel type'] + ' ' + private_fleet_truck.loc[:, 'veh_class']
# veh_comb = private_fleet_truck.veh_type.unique()

private_fleet_truck = private_fleet_truck.loc[private_fleet_truck['number_of_veh'] > 0]
private_fleet_truck.loc[:, 'fleet_id']=private_fleet_truck.groupby('BusID').cumcount() + 1
idx_var = ['CBPZONE', 'FAFZONE', 'esizecat', 'Industry_NAICS6_Make',
       'Commodity_SCTG', 'Emp', 'BusID', 'MESOZONE', 'ZIPCODE', 'lat', 'lon',
       'ParcelID', 'TAZ', 'state_abbr', 'fleet_id']
# convert long table to wide
private_fleet_truck = pd.pivot_table(private_fleet_truck, index=idx_var,
                                     columns = 'veh_type', values = 'number_of_veh', aggfunc= 'sum')

private_fleet_truck = private_fleet_truck.reset_index()
private_fleet_truck.fillna(0, inplace = True)


for veh in veh_comb:
    if veh not in private_fleet_truck.columns:
            private_fleet_truck[veh] = 0
private_fleet_truck.loc[:, 'n_trucks'] = private_fleet_truck.loc[:, veh_comb].sum(axis = 1)
print(private_fleet_truck[veh_comb].sum())
