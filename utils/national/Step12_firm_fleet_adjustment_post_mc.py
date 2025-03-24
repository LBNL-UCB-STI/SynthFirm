#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 17 14:45:02 2023

@author: xiaodanxu
"""

import os
from pandas import read_csv
import pandas as pd
import matplotlib.pyplot as plt
# import seaborn as sns
import numpy as np
import warnings

warnings.filterwarnings("ignore")

# os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync/')


# vehicle assignment function
# def veh_type_simulator(n_truck, vehicle_type_fraction): # Simulate mode choice
#     choice = np.random.multinomial(n_truck, vehicle_type_fraction, size = 1)
#     choice = pd.Series(choice[0])
# #     print(choice)
#     return(choice)

# def split_dataframe(df, chunk_size = 100000): 
#     chunks = list()
#     num_chunks = len(df) // chunk_size + 1
#     for i in range(num_chunks):
#         chunks.append(df[i*chunk_size:(i+1)*chunk_size])
#     return chunks

# # load input
# def firm_fleet_generator_post_mc(fleet_year, fleet_scenario_name, synthetic_firms_with_location_file,
#                          private_fleet_file, national_fleet_composition_file,
#                          vehicle_type_by_state_file, ev_availability_file, state_fips_lookup_file,
#                          firms_with_fleet_file, firms_with_fleet_mc_adj_files, output_path):
    
# a function for post-mode-choice fleet adjustment
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

print('Load inputs and set up scenarios...')
scenario_name = fleet_name + ' & ' + regulations
analysis_year = fleet_year

result_dir = os.path.join(output_path, str(analysis_year), scenario_name)
    
firms_with_fleet_file = os.path.join(result_dir, 'synthetic_firms_with_fleet.csv')
private_fleet_file = os.path.join(parameter_dir, 'fleet/veh_per_emp_by_state.csv')

# inputs vary by scenario
ev_availability_file = os.path.join(parameter_dir, 'fleet/synthfirm_ev_availability.csv')
private_fuel_mix_file = os.path.join(parameter_dir, 'fleet/private_fuel_mix_scenario.csv')
private_fleet = read_csv(private_fleet_file)

firms = read_csv(firms_with_fleet_file)
# forecast values
private_fuel_mix = read_csv(private_fuel_mix_file)
ev_availability = read_csv(ev_availability_file)




# <codecell>
########## pre-processing data #######
print('Formating data and load B2B flow...')
# filter vehicle composition data
private_fuel_mix.loc[:, 'veh_type'] = \
    private_fuel_mix.loc[:, 'fuel type'] + ' ' + private_fuel_mix.loc[:, 'veh_class']
veh_comb = private_fuel_mix.veh_type.unique()
# print(veh_comb)

# select forecasted data under current scenario
private_fuel_mix = \
    private_fuel_mix.loc[(private_fuel_mix['scenario'] == fleet_name) & 
                         (private_fuel_mix['rules'] == regulations) &
                         (private_fuel_mix['yearID'] == fleet_year)]
    
ev_availability = ev_availability.loc[ev_availability['scenario'] == fleet_name]
ev_availability = ev_availability.loc[ev_availability['Year'] == fleet_year]

# drop missing in data
private_fleet.replace(np.inf, np.nan, inplace = True)
private_fleet.fillna(0, inplace = True)

# <codecell>
# load b2b output
combined_b2b_flow = None

for i in range(5):
    sctg = i + 1
    sctg_code = 'sctg' + str(sctg)
    file_dir = os.path.join(output_path, sctg_code + '_truck/')
    filelist = [file for file in os.listdir(file_dir) if (file.endswith('.csv.zip'))]
    print(sctg_code)
    n = 50 # chunk size  _-> save results for  every 50 files
    filelist_chunk = [filelist[i * n:(i + 1) * n] for i in range((len(filelist) + n - 1) // n )] 
    j = 0
    for chunk in filelist_chunk:
        print('loading chunk ' + str(j))
        combined_csv = pd.concat([read_csv(file_dir + f) for f in chunk ])
        # dup_id_count = combined_csv['shipment_id'].duplicated().sum()
        # print('duplicated shipment id = ' + str(dup_id_count))
        # print(len(combined_csv))
        # combined_csv = pd.concat([read_csv(file_dir + f) for f in filelist ])
        combined_csv = combined_csv.loc[combined_csv['mode_choice'] == 'Private Truck']
        combined_csv = combined_csv.groupby(['SellerID'])[['TruckLoad']].sum()
        combined_csv = combined_csv.reset_index()
        combined_b2b_flow = pd.concat([combined_b2b_flow, combined_csv])
        j += 1
    # break

selected_firms_with_load = combined_b2b_flow.groupby(['SellerID'])[['TruckLoad']].sum()
selected_firms_with_load = selected_firms_with_load.reset_index()
selected_firms_with_load.columns = ['BusID', 'Production']
print(len(selected_firms_with_load))
selected_sellers = selected_firms_with_load.BusID.unique()
firms_without_adj = firms.loc[~firms['BusID'].isin(selected_sellers)]
firms_with_adj = pd.merge(firms, selected_firms_with_load,
                          on = 'BusID', how = 'inner')
print(len(firms_without_adj))
print(len(firms_with_adj))

# <codecell>

###### adjust private fleet within study area #########

print('Regenerate private fleet...')
# format fleet composition


firms_with_adj = firms_with_adj[['CBPZONE', 'FAFZONE', 'esizecat', 'Industry_NAICS6_Make',
       'Commodity_SCTG', 'Emp', 'BusID', 'MESOZONE', 'ZIPCODE', 'lat', 'lon',
       'ParcelID', 'TAZ', 'state_abbr', 'Production']]

firms_with_adj = firms_with_adj.drop_duplicates(subset = 'BusID',
                                                keep = 'first')
# print(len(firms_with_adj.BusID.unique()))

# re-generate fleet size for selected firms
sample_size = len(firms_with_adj)


private_fleet.loc[:, 'industry'] = private_fleet.loc[:, 'industry'].astype(str)
firms_with_adj.loc[:, 'industry'] = firms_with_adj.loc[:, 'Industry_NAICS6_Make'].astype(str).str[0:2]
firms_with_adj.loc[firms_with_adj['industry'].isin(["31", "32", "33"]), 'industry'] = "3133"
firms_with_adj.loc[firms_with_adj['industry'].isin(["44", "45", "4A"]), 'industry'] = "4445"
firms_with_adj.loc[firms_with_adj['industry'].isin(["48", "49"]), 'industry'] = "4849"
firms_with_adj.loc[firms_with_adj['industry'].isin(["S0"]), 'industry'] = "92"
# print(firms.industry.unique())
vehicle_types = private_fuel_mix.veh_class.unique()
attr_to_keep = ['state_abbr', 'industry']
for veh in vehicle_types:
    rate_attr = 'rate_' + veh
    attr_to_keep.append(rate_attr)
# print(attr_to_keep)

private_fleet = private_fleet[attr_to_keep]

# calculate firm fleet by emp * rate

firms_with_fleet = pd.merge(firms_with_adj, private_fleet, 
                            on = ['state_abbr', 'industry'], how = 'left')
to_drop = []
for veh in vehicle_types:
    rate_attr = 'rate_' + veh
    to_drop.append(rate_attr)
    firms_with_fleet.loc[:, veh] = firms_with_fleet.loc[:, 'Emp'] * \
        firms_with_fleet.loc[:, rate_attr]
firms_with_fleet.drop(columns = to_drop, inplace = True)    
print(firms_with_fleet[vehicle_types].sum())


# <codecell>

# scale fleet to fit the production capacity
payload_capacity = {'Light-duty Class12A': 1, 
                    'Light-duty Class2B3': 2,
                    'Medium-duty Vocational': 4,
                   'Heavy-duty Tractor': 18,
                   'Heavy-duty Vocational': 18}

num_of_days = 300

firms_with_fleet.loc[:, 'required_capacity'] =\
    firms_with_fleet.loc[:, 'Production'] / num_of_days
payload_gap = firms_with_fleet.loc[:, 'required_capacity'].sum()
target_gap = 0.01 * payload_gap

iterator = 0
while abs(payload_gap) >= target_gap:

    firms_with_fleet.loc[:, 'simulated_capacity'] = 0
    for veh in vehicle_types:
        firms_with_fleet.loc[:, 'simulated_capacity'] = firms_with_fleet.loc[:, 'simulated_capacity']+ \
            firms_with_fleet.loc[:, veh] * payload_capacity[veh]
    
    firms_with_fleet.loc[:, 'adj_fac'] = \
        firms_with_fleet.loc[:, 'required_capacity']/ firms_with_fleet.loc[:, 'simulated_capacity']
    # firms_with_fleet.loc[firms_with_fleet['adj_fac'] <= 1, 'adj_fac'] = 1
    # firms_with_fleet.loc[firms_with_fleet['adj_fac'] >= 100, 'adj_fac'] = 100
    
    firms_with_fleet.loc[:, vehicle_types] = \
        firms_with_fleet.loc[:, vehicle_types].mul(firms_with_fleet['adj_fac'], axis = 0)
    # round up maximum to ensure at least 1 truck presents in the fleet        
    firms_with_fleet.loc[:, 'max_size'] = \
            firms_with_fleet.loc[:, vehicle_types].max(axis = 1)
            
    # ensure the firms with large scale factor will have largest trucks, no small trucks needed
    threshold_size = 50
    firms_with_fleet.loc[firms_with_fleet['adj_fac'] >= threshold_size, 'Heavy-duty Tractor'] = \
        firms_with_fleet.loc[firms_with_fleet['adj_fac'] >= threshold_size, 'max_size']
    firms_with_fleet.loc[firms_with_fleet['adj_fac'] >= threshold_size, 'Light-duty Class12A'] = 0
    firms_with_fleet.loc[firms_with_fleet['adj_fac'] >= threshold_size, 'Light-duty Class2B3'] = 0
    firms_with_fleet.loc[firms_with_fleet['adj_fac'] >= threshold_size, 'Medium-duty Vocational'] = 0
    
    for alt in vehicle_types:
        firms_with_fleet.loc[:, alt] = \
            firms_with_fleet.loc[:, alt] * (firms_with_fleet.loc[:, alt]< firms_with_fleet.loc[:, 'max_size']) + \
                np.ceil(firms_with_fleet.loc[:, alt]) * (firms_with_fleet.loc[:, alt] == firms_with_fleet.loc[:, 'max_size'])
    
    firms_with_fleet.loc[:, vehicle_types] = \
        np.round(firms_with_fleet.loc[:, vehicle_types], 0)
    firms_with_fleet.loc[:, 'simulated_capacity'] = 0
    for veh in vehicle_types:
        firms_with_fleet.loc[:, 'simulated_capacity'] = firms_with_fleet.loc[:, 'simulated_capacity']+ \
            firms_with_fleet.loc[:, veh] * payload_capacity[veh]
    insuff_idx = (firms_with_fleet.loc[:, 'required_capacity'] >= \
                  firms_with_fleet.loc[:, 'simulated_capacity'])
    payload_gap  = firms_with_fleet.loc[insuff_idx, 'required_capacity'].sum() -\
        firms_with_fleet.loc[insuff_idx, 'simulated_capacity'].sum()
    print(payload_gap)
    iterator += 1
    if iterator > 10:
        print('Failed to converge!')
        break
    # break


print(firms_with_fleet[vehicle_types].sum())   
    

# <codecell>
# print(tx_private_fleet.columns)
fuel_attr = ['veh_class', 'state_abbr', 'fuel type', 'veh_fraction']
private_fuel_mix = private_fuel_mix[fuel_attr]
fuel_types = private_fuel_mix['fuel type'].unique()

idx_var = ['CBPZONE', 'FAFZONE', 'esizecat', 'Industry_NAICS6_Make',
       'Commodity_SCTG', 'Emp', 'BusID', 'MESOZONE', 'ZIPCODE', 'lat', 'lon',
       'ParcelID', 'TAZ', 'state_abbr', 'simulated_capacity']

firms_with_fleet = pd.melt(firms_with_fleet, id_vars = idx_var, value_vars = vehicle_types,
                        var_name='veh_class', value_name='number_of_veh')
# keep veh types with non-zero values
firms_with_fleet = firms_with_fleet.loc[firms_with_fleet['number_of_veh'] > 0]

# append fuel mix and randomly assign fuel types
firms_with_fleet = pd.merge(firms_with_fleet, private_fuel_mix,
                         on = ['veh_class', 'state_abbr'], how = 'left')

fleet_threshold = 20
# for large fleet, assign fuel type by fraction; for small fleet, randomly assign them to single fuel
private_fleet_large = firms_with_fleet.loc[firms_with_fleet['number_of_veh'] >= fleet_threshold]
private_fleet_large.loc[:, 'number_of_veh'] = \
    private_fleet_large.loc[:, 'number_of_veh'] * private_fleet_large.loc[:, 'veh_fraction']
private_fleet_large.loc[:, 'number_of_veh'] = \
    np.round(private_fleet_large.loc[:, 'number_of_veh'], 0)
    
private_fleet_small = firms_with_fleet.loc[firms_with_fleet['number_of_veh'] < fleet_threshold]
idx_var.append('veh_class')
private_fleet_small = private_fleet_small.groupby(idx_var).sample(n=1, 
                                      weights = private_fleet_small['veh_fraction'],
                                      replace = False, random_state = 1)

# combine small and large fleets

private_fleet_truck = pd.concat([private_fleet_large, private_fleet_small])

# veh_comb = private_fleet_truck.veh_type.unique()

private_fleet_truck = private_fleet_truck.loc[private_fleet_truck['number_of_veh'] > 0]
private_fleet_truck.loc[:, 'fleet_id']=private_fleet_truck.groupby('BusID').cumcount() + 1

# <codecell>
# assign EV powertrain
body_types = ev_availability['vehicleType'].unique()
firms_with_fleet.loc[:, 'EV_powertrain (if any)'] = np.nan

firms_with_fleet_out = None

for bt in body_types:
    print(bt)
    vehicle_capacity = payload_capacity[bt]
    ev_availability_select = \
    ev_availability.loc[ev_availability['vehicleType'] == bt]
    powertrain = ev_availability_select.Powertrain.to_numpy()
    probability = ev_availability_select.Fraction.to_numpy()
    
    firm_to_assign = \
    private_fleet_truck.loc[private_fleet_truck['veh_class'].str.contains(bt)].reset_index()    
    sample_size_1 = len(firm_to_assign)
    firm_to_assign.loc[:, 'EV_powertrain (if any)'] = \
    pd.Series(np.random.choice(powertrain, size = sample_size_1, p=probability) )
    firm_to_assign.loc[:, 'veh_capacity'] = \
        firm_to_assign.loc[:, 'number_of_veh'] * vehicle_capacity  
    firms_with_fleet_out = pd.concat([firms_with_fleet_out, firm_to_assign])


veh_class_renaming = {'Heavy-duty Tractor': 'Class 7&8 Tractor', 
                      'Heavy-duty Vocational': 'Class 7&8 Vocational',
                      'Light-duty Class12A': 'Class 1&2A Vocational', 
                      'Light-duty Class2B3': 'Class 2&B3 Vocational',
                      'Medium-duty Vocational': 'Class 4-6 Vocational'}

firms_with_fleet_out.loc[:, 'veh_class'] = \
firms_with_fleet_out.loc[:, 'veh_class'].map(veh_class_renaming)
firms_with_fleet_out.loc[:, 'veh_type'] = \
    firms_with_fleet_out.loc[:, 'fuel type'] + ' ' + firms_with_fleet_out.loc[:, 'veh_class']

private_fuel_mix.loc[:, 'veh_class'] = \
    private_fuel_mix.loc[:, 'veh_class'].map(veh_class_renaming)
private_fuel_mix.loc[:, 'veh_type'] = \
    private_fuel_mix.loc[:, 'fuel type'] + ' ' + private_fuel_mix.loc[:, 'veh_class']
veh_comb = private_fuel_mix.veh_type.unique()
# <codecell>

###### format and write output #########

idx_var = ['CBPZONE', 'FAFZONE', 'esizecat', 'Industry_NAICS6_Make',
       'Commodity_SCTG', 'Emp', 'BusID', 'MESOZONE', 'ZIPCODE', 'lat', 'lon',
       'ParcelID', 'TAZ', 'state_abbr', 'fleet_id', 'EV_powertrain (if any)', 'veh_capacity']
# convert long table to wide
firms_with_fleet_out = pd.pivot_table(firms_with_fleet_out, index=idx_var,
                                     columns = 'veh_type', values = 'number_of_veh', aggfunc= 'sum')

firms_with_fleet_out = firms_with_fleet_out.reset_index()
firms_with_fleet_out.fillna(0, inplace = True)
for veh in veh_comb:
    if veh not in firms_with_fleet_out.columns:
            firms_with_fleet_out[veh] = 0  
            
firms_with_fleet_out.loc[:, 'n_trucks'] = firms_with_fleet_out.loc[:, veh_comb].sum(axis = 1)
print(firms_with_fleet_out[veh_comb].sum())


# <codecell>
# assign fleet to private truck shipments
firms_with_fleet_short = firms_with_fleet_out[['BusID', 'fleet_id', 'veh_capacity']]
firms_with_fleet_short.loc[:, 'pdf'] = \
firms_with_fleet_short.loc[:, 'veh_capacity'] / \
firms_with_fleet_short.groupby('BusID')['veh_capacity'].transform('sum')
firms_with_fleet_short.loc[:, 'cdf'] = \
firms_with_fleet_short.groupby('BusID')['pdf'].cumsum()


# load b2b output
# combined_b2b_flow = None
# dir_to_outputs = 'mode_choice/outputs'

for i in range(5):
    sctg = i + 1
    sctg_code = 'sctg' + str(sctg)
    print(sctg_code)
    file_dir = os.path.join(output_path, sctg_code + '_truck/')

    filelist = [file for file in os.listdir(file_dir) if (file.endswith('.csv.zip'))]
    n = 50 # chunk size  _-> save results for  every 50 files
    filelist_chunk = [filelist[i * n:(i + 1) * n] for i in range((len(filelist) + n - 1) // n )] 
    j = 0
    for chunk in filelist_chunk:
        combined_csv = pd.concat([read_csv(file_dir + f) for f in chunk ])
        combined_csv.loc[:, 'shipment_id'] = combined_csv.reset_index().index + 1 #re-indexing data so that no more duplicate id
        dup_id_count = combined_csv['shipment_id'].duplicated().sum()
        if dup_id_count >0:
            print('Warning! Duplicated shipment id = ' + str(dup_id_count))
        private_truck = combined_csv.loc[combined_csv['mode_choice'] == 'Private Truck']
        for_hire_truck = combined_csv.loc[combined_csv['mode_choice'] != 'Private Truck']
        sample_size = len(private_truck)
        if sample_size > 0:
            private_truck.loc[:, 'rand'] = pd.Series(np.random.uniform(size = sample_size))
            private_truck = pd.merge(private_truck, firms_with_fleet_short,
                                    left_on = 'SellerID', right_on = 'BusID',
                                    how = 'left')
            private_truck.loc[:, 'indicator'] = 0
            criteria = (private_truck.loc[:, 'rand'] < private_truck.loc[:, 'cdf'])
            private_truck.loc[criteria, 'indicator'] = 1
            private_truck = private_truck.loc[private_truck['indicator'] == 1]
            private_truck = private_truck.drop_duplicates(subset = ['shipment_id'], 
                                                          keep = 'first')
            # print(sample_size, len(private_truck))
            private_truck = private_truck.drop(columns=['rand',	'BusID', 'veh_capacity', 'pdf', 'cdf', 'indicator'])
            private_truck.to_csv(result_dir +  '/private_truck_shipment_' + sctg_code + '_' + str(j) + '.csv', index = False)
        if len(for_hire_truck) > 0:
            for_hire_truck.to_csv(result_dir +  '/for_hire_truck_shipment_' + sctg_code  + '_' + str(j) + '.csv', index = False)
        j += 1

# <codecell>
# fill in columns that are not selected

        
firms_with_fleet_out = firms_with_fleet_out.drop(columns=['veh_capacity'])
firms_output = pd.concat([firms_with_fleet_out, firms_without_adj])

firms_with_fleet_mc_adj_files = os.path.join(result_dir, 'synthetic_firms_with_fleet_mc_adjusted.csv')
firms_output.to_csv(firms_with_fleet_mc_adj_files, index = False)
