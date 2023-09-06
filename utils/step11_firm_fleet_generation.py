#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug  8 10:47:47 2023

@author: xiaodanxu
"""

import os
from pandas import read_csv
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import warnings
from shapely.geometry import Point, Polygon
import geopandas as gpd
import random

warnings.filterwarnings("ignore")

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync/')

########## vehicle assignment function #############
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

########### define inputs ################
print('loading fleet inputs...')

scenario_name = 'HOP_highp6'
input_dir = 'inputs_SF/'
output_dir = 'outputs_SF/'
firm_name = 'synthetic_firms_with_location.csv'
firms = read_csv(output_dir + firm_name)

private_fleet = read_csv(input_dir + 'fleet/CA_private_fleet_size_distribution_V2.csv')
for_hire_fleet = read_csv(input_dir + 'fleet/CA_for_hire_fleet_size_distribution_V2.csv')
for_lease_fleet = read_csv(input_dir + 'fleet/CA_for_lease_fleet_size_distribution_V2.csv')
cargo_type_distribution = read_csv(input_dir + "fleet/probability_of_cargo_group.csv")

# forecast values
national_fleet_composition = read_csv(input_dir + 'fleet/' + scenario_name + '/TDA_vehicle_stock.csv')
vehicle_type_by_state = read_csv(input_dir + 'fleet/'  + scenario_name + '/fleet_composition_by_state.csv')
# ev_fraction = read_csv('inputs/fleet/' + scenario_name + '/EV_fraction_by_type.csv')
ev_availability = read_csv(input_dir + 'fleet/'  + scenario_name + '/EV_availability.csv')

state_fips_lookup = read_csv(input_dir + 'us-state-ansi-fips.csv')

# <codecell>

############## pre-processing data ################
print('initial fleet composition generation...')
# filter vehicle composition data
analysis_year = 2050
vehicle_type_by_state = \
vehicle_type_by_state.loc[vehicle_type_by_state['Year'] == analysis_year]

national_fleet_composition = \
national_fleet_composition.loc[national_fleet_composition['Year'] == analysis_year]

ev_availability = ev_availability.loc[ev_availability['Year'] == analysis_year]

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


for_hire_fleet_by_state = \
vehicle_type_by_state.loc[vehicle_type_by_state['Service type'] == 'FOR HIRE']
for_hire_fleet_by_state_wide = pd.pivot_table(for_hire_fleet_by_state,
                                             values='veh_fraction', 
                                             index=['state'],
                                             columns=['vehicle category'], 
                                             aggfunc=np.mean, fill_value=0)
for_hire_fleet_by_state_wide = for_hire_fleet_by_state_wide.reset_index()


for_lease_fleet_by_state = \
vehicle_type_by_state.loc[vehicle_type_by_state['Service type'] == 'LEASE']
for_lease_fleet_by_state_wide = pd.pivot_table(for_lease_fleet_by_state,
                                             values='veh_fraction', 
                                             index=['state'],
                                             columns=['vehicle category'], 
                                             aggfunc=np.mean, fill_value=0)
for_lease_fleet_by_state_wide = for_lease_fleet_by_state_wide.reset_index()

# <codecell>
print('Fleet assignment for private companies (this can take 2 hours to run)...')
########## fleet size and type generation for all firms ############
sample_size = len(firms)
print('number of firms = ' + str(sample_size))

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
criteria = (firm_fleet_sample['n_trucks'] <= firm_fleet_sample['min_size'])
firm_fleet_sample.loc[criteria, 'n_trucks'] = firm_fleet_sample.loc[criteria, 'min_size']
firm_fleet_sample.loc[:, 'n_trucks'] = np.round(firm_fleet_sample.loc[:, 'n_trucks'], 0)
firm_fleet_sample.loc[:, 'n_trucks'] = firm_fleet_sample.loc[:, 'n_trucks'].astype(int)

firm_fleet_sample['n_trucks'].hist(bins = 1000)
plt.xlim([0,100])

# append vehicle composition to firms by state
firm_fleet_sample = firm_fleet_sample.sort_values(by = ['n_trucks'], ascending = True)
firms = firms.sort_values(by = ['Emp'], ascending = True)

firms.loc[:, 'FAFZONE'] = firms.loc[:,'FAFZONE'].astype(str).str.zfill(3)
firms.loc[:, 'st'] = firms.loc[:, 'FAFZONE'].str[:2]
firms.loc[:, 'st'] = firms.loc[:, 'st'].astype(int)

firms = pd.merge(firms, state_fips_lookup, on = 'st', how = 'left')
# print(len(firms))
firms.loc[:,'stname'] = firms.loc[:,'stname'].str.upper()

firm_fleet_sample_short = firm_fleet_sample[['n_trucks']]

firms_with_fleet = pd.concat([firms.reset_index(drop=True), 
                              firm_fleet_sample_short.reset_index(drop=True)], axis=1)


firms_with_fleet = pd.merge(firms_with_fleet, private_fleet_by_state_wide,
                            left_on = 'stname', right_on = 'state', how = 'left')

# assign vehicle technology
print('total trucks from initial assignment = ')
print(firms_with_fleet.n_trucks.sum())

chunks = split_dataframe(firms_with_fleet)
var_to_keep = ['esizecat', 'CBPZONE', 'FAFZONE', 'Industry_NAICS6_Make',
       'Commodity_SCTG', 'Emp', 'BusID', 'MESOZONE', 'lat', 'lon', 'n_trucks',
       'st', 'stname']

i = 0
firms_with_fleet = None
for chunk in chunks:
    print('processing chunk ' + str(i))
    chunk[list_of_veh_tech] = \
    chunk.apply(
            lambda row: veh_type_simulator(row['n_trucks'], row[list_of_veh_tech]), axis=1, result_type ='expand')
    chunk = pd.melt(chunk, id_vars = var_to_keep, 
                    value_vars = list_of_veh_tech, 
                   var_name = 'veh_type',
                   value_name = 'number_of_veh')
    chunk = chunk.reset_index()
    chunk = chunk.loc[chunk['number_of_veh'] > 0]
    chunk.loc[:, 'fleet_id']=chunk.groupby('BusID').cumcount() + 1
    firms_with_fleet = pd.concat([firms_with_fleet, chunk])
    i += 1

print(firms_with_fleet.number_of_veh.sum())

# <codecell>
######## fleet generation for carriers #########
print('Fleet assignment for carriers...')
# processing carriers
firms['Industry_NAICS6_Make'] = firms['Industry_NAICS6_Make'].astype(str)

new_firms = firms[['esizecat', 'CBPZONE', 'FAFZONE',
                              'Industry_NAICS6_Make', 'Commodity_SCTG',	
                              'Emp', 'BusID', 'MESOZONE', 'lat', 'lon',
                              'stname', 'st']]


carriers = new_firms.loc[new_firms['Industry_NAICS6_Make'].isin(['492000', '484000'])]
sample_size = len(carriers)

for_hire_fleet_short = for_hire_fleet[['fleet_size', 'min_size', 
                                           'fraction_of_carrier', 
                                           'avg_truck_per_carrier', 
                                           'total_truck_std']]

fleet_sample = for_hire_fleet_short.sample(n = sample_size,
                                            weights = for_hire_fleet_short['fraction_of_carrier'],
                                            replace = True)
print('number of carriers = ')
print(len(fleet_sample))

var_to_keep = ['esizecat', 'CBPZONE', 'FAFZONE', 'Industry_NAICS6_Make',
       'Commodity_SCTG', 'Emp', 'BusID', 'MESOZONE', 'lat', 'lon', 'n_trucks',
       'st', 'stname']

fleet_sample.loc[:, 'n_trucks'] = np.random.normal(loc = fleet_sample.loc[:, 'avg_truck_per_carrier'],
                                                   scale = fleet_sample.loc[:, 'total_truck_std'])
fleet_sample.loc[:, 'n_trucks']  = fleet_sample.loc[:, 'n_trucks'].fillna(1)
criteria = (fleet_sample['n_trucks'] <= fleet_sample['min_size'])
fleet_sample.loc[criteria, 'n_trucks'] = fleet_sample.loc[criteria, 'min_size']
fleet_sample.loc[:, 'n_trucks'] = np.round(fleet_sample.loc[:, 'n_trucks'], 0)
fleet_sample.loc[:, 'n_trucks'] = fleet_sample.loc[:, 'n_trucks'].astype(int)

fleet_sample = fleet_sample.sort_values(by = ['n_trucks'], ascending = True)
carriers = carriers.sort_values(by = ['Emp'], ascending = True)

fleet_sample_short = fleet_sample[['n_trucks']]

carriers_with_fleet = pd.concat([carriers.reset_index(drop=True), 
                                 fleet_sample_short.reset_index(drop=True)], axis=1)

carriers_with_fleet = pd.merge(carriers_with_fleet, for_hire_fleet_by_state_wide,
                            left_on = 'stname', right_on = 'state', how = 'left')

carriers_with_fleet[list_of_veh_tech] = \
carriers_with_fleet.apply(
        lambda row: veh_type_simulator(row['n_trucks'], row[list_of_veh_tech]), axis=1, result_type ='expand')
# print(np.random.multinomial(testing_fleet_sample['n_trucks'], vehicle_type_fraction))

carriers_with_fleet = pd.melt(carriers_with_fleet, id_vars = var_to_keep, 
                value_vars = list_of_veh_tech, 
               var_name = 'veh_type',
               value_name = 'number_of_veh')
carriers_with_fleet = carriers_with_fleet.reset_index()
carriers_with_fleet = carriers_with_fleet.loc[carriers_with_fleet['number_of_veh'] > 0]
carriers_with_fleet.loc[:, 'fleet_id'] = \
carriers_with_fleet.groupby('BusID').cumcount() + 1

# <codecell>
######## fleet generation for leasing #########
print('Fleet assignment for leasing company...')
leasing = new_firms.loc[new_firms['Industry_NAICS6_Make'].isin(['532100'])]
sample_size = len(leasing)
# print(tx_private_fleet.columns)
for_lease_fleet_short = for_lease_fleet[['fleet_size', 'min_size', 
                                           'fraction_of_carrier', 
                                           'avg_truck_per_carrier', 
                                           'total_truck_std']]

fleet_sample = for_lease_fleet_short.sample(n = sample_size,
                                            weights = for_lease_fleet_short['fraction_of_carrier'],
                                            replace = True)
print('number of leasing company = ')
print(len(fleet_sample))

fleet_sample.loc[:, 'n_trucks'] = np.random.normal(loc = fleet_sample.loc[:, 'avg_truck_per_carrier'],
                                                   scale = fleet_sample.loc[:, 'total_truck_std'])
criteria = (fleet_sample['n_trucks'] <= fleet_sample['min_size'])
fleet_sample.loc[criteria, 'n_trucks'] = fleet_sample.loc[criteria, 'min_size']
fleet_sample.loc[:, 'n_trucks'] = np.round(fleet_sample.loc[:, 'n_trucks'], 0)
fleet_sample.loc[:, 'n_trucks'] = fleet_sample.loc[:, 'n_trucks'].astype(int)

fleet_sample = fleet_sample.sort_values(by = ['n_trucks'], ascending = True)
leasing = leasing.sort_values(by = ['Emp'], ascending = True)

fleet_sample_short = fleet_sample[['n_trucks']]

leasing_with_fleet = pd.concat([leasing.reset_index(drop=True), 
                              fleet_sample_short.reset_index(drop=True)], axis=1)

leasing_with_fleet = pd.merge(leasing_with_fleet, for_lease_fleet_by_state_wide,
                            left_on = 'stname', right_on = 'state', how = 'left')

leasing_with_fleet[list_of_veh_tech] = \
leasing_with_fleet.apply(
        lambda row: veh_type_simulator(row['n_trucks'], row[list_of_veh_tech]), axis=1, result_type ='expand')
# print(np.random.multinomial(testing_fleet_sample['n_trucks'], vehicle_type_fraction))
leasing_with_fleet = pd.melt(leasing_with_fleet, 
                             id_vars = var_to_keep, 
                value_vars = list_of_veh_tech, 
               var_name = 'veh_type',
               value_name = 'number_of_veh')
leasing_with_fleet = leasing_with_fleet.reset_index()
leasing_with_fleet = leasing_with_fleet.loc[leasing_with_fleet['number_of_veh'] > 0]
leasing_with_fleet.loc[:, 'fleet_id'] = leasing_with_fleet.groupby('BusID').cumcount() + 1

# <codecell>
######## fleet generation for leasing #########
# post-processing results

print('Adjusting fleet size to match national vehicle stock...')

firms_with_fleet = \
firms_with_fleet[~firms_with_fleet['Industry_NAICS6_Make'].isin(['492000', '484000', '532100'])]
print(len(firms_with_fleet))

# adjust the total fleet
firms_with_fleet_nonzero = firms_with_fleet.dropna(subset = ['number_of_veh'])
firm_fleet_agg = firms_with_fleet_nonzero.groupby(['veh_type'])[['number_of_veh']].sum()
firm_fleet_agg = firm_fleet_agg.reset_index()
firm_fleet_agg.columns = ['vehicle_type', 'total']
firm_fleet_agg.loc[:, 'source'] = 'firms'


carrier_fleet_agg = carriers_with_fleet.groupby(['veh_type'])[['number_of_veh']].sum()
carrier_fleet_agg = carrier_fleet_agg.reset_index()
carrier_fleet_agg.columns = ['vehicle_type', 'total']
carrier_fleet_agg.loc[:, 'source'] = 'carrier'

lease_fleet_agg = leasing_with_fleet.groupby(['veh_type'])[['number_of_veh']].sum()
lease_fleet_agg = lease_fleet_agg.reset_index()
lease_fleet_agg.columns = ['vehicle_type', 'total']
lease_fleet_agg.loc[:, 'source'] = 'lease'

modeled_fleet_agg = pd.concat([firm_fleet_agg, carrier_fleet_agg, lease_fleet_agg])

# calculate the scaling factor
total_modeled_veh = pd.pivot_table(modeled_fleet_agg, values='total', 
                                   index=['vehicle_type'], columns=['source'], aggfunc=np.sum)
total_modeled_veh = total_modeled_veh.reset_index()


total_modeled_veh = total_modeled_veh.reset_index()

national_fleet_adj = pd.merge(national_fleet_composition, 
                              total_modeled_veh,
                              left_on = 'vehicle category',
                              right_on = 'vehicle_type', how = 'left')
national_fleet_adj.loc[:, 'private_stock'] = national_fleet_adj.loc[:, 'Stock'] - \
national_fleet_adj.loc[:, 'carrier'] - national_fleet_adj.loc[:, 'lease'] 
national_fleet_adj.loc[:, 'adj_factor'] = national_fleet_adj.loc[:, 'private_stock'] / \
national_fleet_adj.loc[:, 'firms']

# adjust private fleet only
# print(national_fleet_adj['private_stock'].sum())

national_fleet_adj_short = national_fleet_adj[['vehicle_type', 'adj_factor']]
firms_with_fleet_adj = pd.merge(firms_with_fleet, 
                                national_fleet_adj_short,
                               left_on = 'veh_type', right_on = 'vehicle_type',
                               how = 'left')
firms_with_fleet_adj.loc[~firms_with_fleet_adj['number_of_veh'].isna(), 'number_of_veh'] = \
firms_with_fleet_adj.loc[~firms_with_fleet_adj['number_of_veh'].isna(), 'number_of_veh'] * \
firms_with_fleet_adj.loc[~firms_with_fleet_adj['number_of_veh'].isna(), 'adj_factor']

firms_with_fleet_adj.loc[:, 'number_of_veh'] = \
firms_with_fleet_adj.loc[:, 'number_of_veh'].fillna(0)
firms_with_fleet_adj.loc[:, 'number_of_veh'] = \
np.round(firms_with_fleet_adj.loc[:, 'number_of_veh'],0)

# due to rounding error, the final number doesn't match national total, so need the second adj
new_scale = 5
while abs(new_scale - 1) > 0.05:
    scale = national_fleet_adj['private_stock'].sum() / firms_with_fleet_adj['number_of_veh'].sum()
    firms_with_fleet_adj.loc[~firms_with_fleet_adj['number_of_veh'].isna(), 'number_of_veh'] *= scale
    firms_with_fleet_adj.loc[:, 'number_of_veh'] = \
    np.round(firms_with_fleet_adj.loc[:, 'number_of_veh'], 0)
    new_scale = national_fleet_adj['private_stock'].sum() / firms_with_fleet_adj['number_of_veh'].sum()

print('number of private vehicles after scaling = ')
print(firms_with_fleet_adj['number_of_veh'].sum())  

# <codecell>

############# assign EV type #############
print('Assign EV powertrain types...')
body_types = ev_availability['vehicle type'].unique()
firms_with_fleet_adj.loc[:, 'EV_powertrain (if any)'] = np.nan
carriers_with_fleet.loc[:, 'EV_powertrain (if any)'] = np.nan
leasing_with_fleet.loc[:, 'EV_powertrain (if any)'] = np.nan

firms_with_fleet_out = None
carriers_with_fleet_out = None
leasing_with_fleet_out = None

for bt in body_types:
    # print(bt)
    ev_availability_select = \
    ev_availability.loc[ev_availability['vehicle type'] == bt]
    powertrain = ev_availability_select.Powertrain.to_numpy()
    probability = ev_availability_select.EV_fraction.to_numpy()
    
    firm_to_assign = \
    firms_with_fleet_adj.loc[firms_with_fleet_adj['veh_type'].str.contains(bt)].reset_index()    
    sample_size_1 = len(firm_to_assign)
    firm_to_assign.loc[:, 'EV_powertrain (if any)'] = \
    pd.Series(np.random.choice(powertrain, size = sample_size_1, p=probability) )
    
    carrier_to_assign = \
    carriers_with_fleet.loc[carriers_with_fleet['veh_type'].str.contains(bt)].reset_index()   
    sample_size_2 = len(carrier_to_assign)
    carrier_to_assign.loc[:, 'EV_powertrain (if any)'] = \
    pd.Series(np.random.choice(powertrain, size = sample_size_2, p=probability) )
        
    lease_to_assign = \
    leasing_with_fleet.loc[leasing_with_fleet['veh_type'].str.contains(bt)].reset_index()   
    sample_size_3 = len(lease_to_assign)
    lease_to_assign.loc[:, 'EV_powertrain (if any)'] = \
    pd.Series(np.random.choice(powertrain, size = sample_size_3, p=probability) )


    firm_to_assign.loc[firm_to_assign['EV_powertrain (if any)'].isin(['PHEV Diesel', 'PHEV Gasoline']), 'EV_powertrain (if any)'] = 'PHEV'
    carrier_to_assign.loc[carrier_to_assign['EV_powertrain (if any)'].isin(['PHEV Diesel', 'PHEV Gasoline']), 'EV_powertrain (if any)'] = 'PHEV'
    lease_to_assign.loc[lease_to_assign['EV_powertrain (if any)'].isin(['PHEV Diesel', 'PHEV Gasoline']), 'EV_powertrain (if any)'] = 'PHEV'
    firms_with_fleet_out = pd.concat([firms_with_fleet_out, firm_to_assign])
    carriers_with_fleet_out = pd.concat([carriers_with_fleet_out, carrier_to_assign])
    leasing_with_fleet_out = pd.concat([leasing_with_fleet_out, lease_to_assign])

# <codecell>


########### writing output #############
print('Writing outputs...')
index_var = ['esizecat', 'CBPZONE', 'FAFZONE', 'Industry_NAICS6_Make',
       'Commodity_SCTG', 'Emp', 'BusID', 'MESOZONE', 'lat', 'lon',
            'st', 'stname', 'EV_powertrain (if any)', 'fleet_id']
available_veh_tech = firms_with_fleet_out.veh_type.unique()
firms_with_fleet_out = firms_with_fleet_out.pivot(values = 'number_of_veh',
                               index = index_var, columns = 'veh_type')
firms_with_fleet_out = firms_with_fleet_out.reset_index()
firms_with_fleet_out = firms_with_fleet_out.fillna(0)
firms_with_fleet_out.loc[:, 'n_trucks'] = \
firms_with_fleet_out.loc[:, available_veh_tech].sum(axis = 1)

available_veh_tech = carriers_with_fleet_out.veh_type.unique()
carriers_with_fleet_out = carriers_with_fleet_out.pivot(values = 'number_of_veh',
                               index = index_var, columns = 'veh_type')
carriers_with_fleet_out = carriers_with_fleet_out.reset_index()
carriers_with_fleet_out = carriers_with_fleet_out.fillna(0)
carriers_with_fleet_out.loc[:, 'n_trucks'] = \
carriers_with_fleet_out.loc[:, available_veh_tech].sum(axis = 1)

available_veh_tech = leasing_with_fleet_out.veh_type.unique()
leasing_with_fleet_out = leasing_with_fleet_out.pivot(values = 'number_of_veh',
                               index = index_var, columns = 'veh_type')
leasing_with_fleet_out = leasing_with_fleet_out.reset_index()
leasing_with_fleet_out = leasing_with_fleet_out.fillna(0)
leasing_with_fleet_out.loc[:, 'n_trucks'] = \
leasing_with_fleet_out.loc[:, available_veh_tech].sum(axis = 1)

# assign cargo type for each carrier
unique_cargo = cargo_type_distribution.Cargo.unique()
sample_size = len(carriers_with_fleet_out)
for cargo in unique_cargo:
    fraction = \
    cargo_type_distribution.loc[cargo_type_distribution['Cargo'] == cargo, 'probability']
    carriers_with_fleet_out.loc[:, cargo] = np.random.binomial(1, fraction, sample_size)
carriers_with_fleet_out.head(5)
carriers_with_fleet_out.loc[:, 'cargo_check'] = carriers_with_fleet_out.loc[:, unique_cargo].sum(axis = 1)
carriers_with_fleet_out.loc[carriers_with_fleet_out['cargo_check'] == 0, 'other_cargo'] = 1

sctg1_cargo = ['construction_material', 'farm_product', 'large_equipment', 'other_bulk', 'other_cargo']
sctg2_cargo = ['farm_product', 'chemical', 'other_cargo']
sctg3_cargo = ['food', 'farm_product', 'other_bulk', 'other_cargo']
sctg4_cargo = ['construction_material', 'vehicle_home', 'large_equipment', 
               'other_bulk', 'household', 'other_cargo']
sctg5_cargo = ['garbage', 'other_bulk', 'intermodal_container', 'other_cargo']

carriers_with_fleet_out.loc[:, 'SCTG1'] = carriers_with_fleet_out.loc[:, sctg1_cargo].sum(axis = 1)
carriers_with_fleet_out.loc[:, 'SCTG2'] = carriers_with_fleet_out.loc[:, sctg2_cargo].sum(axis = 1)
carriers_with_fleet_out.loc[:, 'SCTG3'] = carriers_with_fleet_out.loc[:, sctg3_cargo].sum(axis = 1)
carriers_with_fleet_out.loc[:, 'SCTG4'] = carriers_with_fleet_out.loc[:, sctg4_cargo].sum(axis = 1)
carriers_with_fleet_out.loc[:, 'SCTG5'] = carriers_with_fleet_out.loc[:, sctg5_cargo].sum(axis = 1)

carriers_with_fleet_out.loc[carriers_with_fleet_out['SCTG1'] > 1, 'SCTG1'] = 1
carriers_with_fleet_out.loc[carriers_with_fleet_out['SCTG2'] > 1, 'SCTG2'] = 1
carriers_with_fleet_out.loc[carriers_with_fleet_out['SCTG3'] > 1, 'SCTG3'] = 1
carriers_with_fleet_out.loc[carriers_with_fleet_out['SCTG4'] > 1, 'SCTG4'] = 1
carriers_with_fleet_out.loc[carriers_with_fleet_out['SCTG5'] > 1, 'SCTG5'] = 1

carriers_with_fleet_out = carriers_with_fleet_out.drop(columns=['cargo_check'])

# writing output

# fill in columns that are not selected
for veh in list_of_veh_tech:
    if veh not in firms_with_fleet_out.columns:
        firms_with_fleet_out[veh] = 0
    if veh not in carriers_with_fleet_out.columns:
        carriers_with_fleet_out[veh] = 0
    if veh not in leasing_with_fleet_out.columns:
        leasing_with_fleet_out[veh] = 0
        
result_dir = output_dir + '/' + str(analysis_year) + '/' + scenario_name
isExist = os.path.exists(result_dir)
if not isExist:
   # Create a new directory because it does not exist
   os.makedirs(result_dir)
firms_with_fleet_out.to_csv(result_dir + '/synthetic_firms_with_fleet.csv')
carriers_with_fleet_out.to_csv(result_dir + '/synthetic_carriers.csv')
leasing_with_fleet_out.to_csv(result_dir + '/synthetic_leasing_company.csv')

print('Finished! please find outputs under ' + result_dir)
