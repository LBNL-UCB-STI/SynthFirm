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

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync/')

print('Load inputs and set up scenarios...')
# vehicle assignment function
def veh_type_simulator(n_truck, vehicle_type_fraction): # Simulate mode choice
    choice = np.random.multinomial(n_truck, vehicle_type_fraction, size = 1)
    choice = pd.Series(choice[0])
#     print(choice)
    return(choice)

def split_dataframe(df, chunk_size = 100000): 
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    return chunks

# load input
scenario_name = 'HOP_highp6'
analysis_year = '2050'
output_dir = 'outputs_SF/'
input_dir = 'inputs_SF/'
# dir_to_outputs = 'outputs_aus_2050'
result_dir = output_dir + analysis_year + '/' + scenario_name
    
firms = read_csv(result_dir + '/synthetic_firms_with_fleet.csv')
private_fleet = read_csv(input_dir + 'fleet/CA_private_fleet_size_distribution_V2.csv')
for_hire_fleet = read_csv(input_dir + 'fleet/CA_for_hire_fleet_size_distribution_V2.csv')
for_lease_fleet = read_csv(input_dir + 'fleet/CA_for_lease_fleet_size_distribution_V2.csv')
cargo_type_distribution = read_csv(input_dir + "fleet/probability_of_cargo_group.csv")

# forecast values
national_fleet_composition = read_csv(input_dir + 'fleet/' + scenario_name + '/TDA_vehicle_stock.csv')
vehicle_type_by_state = read_csv(input_dir + 'fleet/'  + scenario_name + '/fleet_composition_by_state.csv')
ev_availability = read_csv(input_dir + 'fleet/'  + scenario_name + '/EV_availability.csv')

state_fips_lookup = read_csv(input_dir + 'us-state-ansi-fips.csv')

payload_capacity = {'Class 4-6 Vocational': 4,
                   'Class 7&8 Tractor': 18,
                   'Class 7&8 Vocational': 18}

# <codecell>
########## pre-processing data #######
print('Formating data and load B2B flow...')
# filter vehicle composition data
analysis_year = 2018
vehicle_type_by_state = \
vehicle_type_by_state.loc[vehicle_type_by_state['Year'] == analysis_year]

national_fleet_composition = \
national_fleet_composition.loc[national_fleet_composition['Year'] == analysis_year]

ev_availability = ev_availability.loc[ev_availability['Year'] == analysis_year]

# load b2b output
combined_b2b_flow = None

for i in range(5):
    sctg = i + 1
    sctg_code = 'sctg' + str(sctg)
    file_dir = output_dir + sctg_code + '_truck/'
    filelist = [file for file in os.listdir(file_dir) if (file.endswith('.csv'))]
    print(sctg_code)
    combined_csv = pd.concat([read_csv(file_dir + f, low_memory=False) for f in filelist ])
    combined_csv = combined_csv.loc[combined_csv['mode_choice'] == 'Private Truck']
    combined_b2b_flow = pd.concat([combined_b2b_flow, combined_csv])
#     break

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
list_of_veh_tech = vehicle_type_by_state['vehicle category'].unique().tolist()
print(list_of_veh_tech)

private_fleet_by_state = \
vehicle_type_by_state.loc[vehicle_type_by_state['Service type'] == 'PRIVATE']
private_fleet_by_state_wide = pd.pivot_table(private_fleet_by_state,
                                             values='veh_fraction', 
                                             index=['state'],
                                             columns=['vehicle category'], 
                                             aggfunc=np.mean, fill_value=0)
private_fleet_by_state_wide = private_fleet_by_state_wide.reset_index()

firms_with_adj = firms_with_adj[['esizecat', 'CBPZONE', 'FAFZONE', 'Industry_NAICS6_Make',
       'Commodity_SCTG', 'Emp', 'BusID', 'MESOZONE', 'lat', 'lon', 'Production']]

firms_with_adj = firms_with_adj.drop_duplicates(subset = 'BusID',
                                                keep = 'first')
# print(len(firms_with_adj.BusID.unique()))

# re-generate fleet size for selected firms
sample_size = len(firms_with_adj)
# print(tx_private_fleet.columns)
private_fleet_short = private_fleet[['fleet_size', 'min_size', 
                                    'fraction_of_carrier', 
                                    'avg_truck_per_carrier', 
                                    'total_truck_std']]

firm_fleet_sample = \
private_fleet_short.sample(n = sample_size,
                           weights = private_fleet_short['fraction_of_carrier'],
                           replace = True)
# print(len(firm_fleet_sample))
# generate random fleet size
firm_fleet_sample.loc[:, 'n_trucks'] = np.random.normal(loc = firm_fleet_sample.loc[:, 'avg_truck_per_carrier'],
                                                   scale = firm_fleet_sample.loc[:, 'total_truck_std'])
criteria = (firm_fleet_sample['n_trucks'] <= 1)
firm_fleet_sample.loc[criteria, 'n_trucks'] = 1
firm_fleet_sample.loc[:, 'n_trucks'] = np.round(firm_fleet_sample.loc[:, 'n_trucks'], 0)
firm_fleet_sample.loc[:, 'n_trucks'] = firm_fleet_sample.loc[:, 'n_trucks'].astype(int)

firm_fleet_sample['n_trucks'].hist(bins = 1000)
plt.xlim([0,100])


# append vehicle composition to firms by state
firm_fleet_sample = firm_fleet_sample.sort_values(by = ['n_trucks'], ascending = True)
firms_with_adj = firms_with_adj.sort_values(by = ['Production'], ascending = True)

firm_fleet_sample_short = firm_fleet_sample[['n_trucks']]

firms_with_fleet = pd.concat([firms_with_adj.reset_index(drop=True), 
                              firm_fleet_sample_short.reset_index(drop=True)], axis=1)

firms_with_fleet.loc[:, 'FAFZONE'] = firms_with_fleet.loc[:,'FAFZONE'].astype(str).str.zfill(3)
firms_with_fleet.loc[:, 'st'] = firms_with_fleet.loc[:, 'FAFZONE'].str[:2]
firms_with_fleet.loc[:, 'st'] = firms_with_fleet.loc[:, 'st'].astype(int)

firms_with_fleet = pd.merge(firms_with_fleet, state_fips_lookup,
                            on = 'st', how = 'left')

firms_with_fleet.loc[:,'stname'] = firms_with_fleet.loc[:,'stname'].str.upper()

firms_with_fleet = pd.merge(firms_with_fleet, private_fleet_by_state_wide,
                            left_on = 'stname', right_on = 'state', how = 'left')

# assign vehicle technology
chunks = split_dataframe(firms_with_fleet)
i = 0
var_to_keep = ['esizecat', 'CBPZONE', 'FAFZONE', 'Industry_NAICS6_Make',
       'Commodity_SCTG', 'Emp', 'BusID', 'MESOZONE', 'lat', 'lon', 'n_trucks',
       'st', 'stname']

for chunk in chunks:
    print('processing chunk ' + str(i))
    chunk[list_of_veh_tech] = \
    chunk.apply(
            lambda row: veh_type_simulator(row['n_trucks'], row[list_of_veh_tech]), axis=1, result_type ='expand')
    # print(np.random.multinomial(testing_fleet_sample['n_trucks'], vehicle_type_fraction))
    chunk = pd.melt(chunk, id_vars = var_to_keep, 
                    value_vars = list_of_veh_tech, 
                   var_name = 'veh_type',
                   value_name = 'number_of_veh')
    chunk = chunk.reset_index()
    chunk = chunk.loc[chunk['number_of_veh'] > 0]
    chunk.loc[:, 'fleet_id']=chunk.groupby('BusID').cumcount() + 1
    if i==0:
        firms_with_fleet = chunk
    else:
        firms_with_fleet = pd.concat([firms_with_fleet, chunk])
    i += 1


# assign EV powertrain
body_types = ev_availability['vehicle type'].unique()
firms_with_fleet.loc[:, 'EV_powertrain (if any)'] = np.nan

firms_with_fleet_out = None

for bt in body_types:
    print(bt)
    vehicle_capacity = payload_capacity[bt]
    ev_availability_select = \
    ev_availability.loc[ev_availability['vehicle type'] == bt]
    powertrain = ev_availability_select.Powertrain.to_numpy()
    probability = ev_availability_select.EV_fraction.to_numpy()
    
    firm_to_assign = \
    firms_with_fleet.loc[firms_with_fleet['veh_type'].str.contains(bt)].reset_index()    
    sample_size_1 = len(firm_to_assign)
    firm_to_assign.loc[:, 'EV_powertrain (if any)'] = \
    pd.Series(np.random.choice(powertrain, size = sample_size_1, p=probability) )
    
    firm_to_assign.loc[firm_to_assign['EV_powertrain (if any)'].isin(['PHEV Diesel', 'PHEV Gasoline']), 'EV_powertrain (if any)'] = 'PHEV'
    firm_to_assign.loc[:, 'veh_capacity'] = \
    firm_to_assign.loc[:, 'number_of_veh'] * vehicle_capacity
    firms_with_fleet_out = pd.concat([firms_with_fleet_out, firm_to_assign])

# <codecell>

###### format and write output #########

print('Writing output...')
# format data
index_var = ['esizecat', 'CBPZONE', 'FAFZONE', 'Industry_NAICS6_Make',
       'Commodity_SCTG', 'Emp', 'BusID', 'MESOZONE', 'lat', 'lon',
            'st', 'stname', 'EV_powertrain (if any)', 'fleet_id', 'veh_capacity']

avail_veh_tech = firms_with_fleet_out.veh_type.unique()
firms_with_fleet_out = firms_with_fleet_out.pivot(values = 'number_of_veh',
                               index = index_var, columns = 'veh_type')
firms_with_fleet_out = firms_with_fleet_out.reset_index()
firms_with_fleet_out = firms_with_fleet_out.fillna(0)
firms_with_fleet_out.loc[:, 'n_trucks'] = \
firms_with_fleet_out.loc[:, avail_veh_tech].sum(axis = 1)

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
    file_dir = output_dir + sctg_code + '_truck/'

    filelist = [file for file in os.listdir(file_dir) if (file.endswith('.csv'))]
    print(sctg_code)
    combined_csv = pd.concat([read_csv(file_dir + f, low_memory=False) for f in filelist ])
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
        private_truck = private_truck.drop_duplicates(subset = 'shipment_id', keep = 'first')
        # print(sample_size, len(private_truck))
        private_truck = private_truck.drop(columns=['rand',	'BusID', 'veh_capacity', 'pdf', 'cdf', 'indicator'])
        private_truck.to_csv(result_dir +  '/private_truck_shipment_' + sctg_code +  '.csv')
    if len(for_hire_truck) > 0:
        for_hire_truck.to_csv(result_dir +  '/for_hire_truck_shipment_' + sctg_code + '.csv')

# fill in columns that are not selected
for veh in list_of_veh_tech:
    if veh not in firms_with_fleet_out.columns:
        firms_with_fleet_out[veh] = 0
        
firms_with_fleet_out = firms_with_fleet_out.drop(columns=['veh_capacity'])
firms_output = pd.concat([firms_with_fleet_out, firms_without_adj])



firms_output.to_csv(result_dir + '/synthetic_firms_with_fleet_mc_adjusted.csv')
