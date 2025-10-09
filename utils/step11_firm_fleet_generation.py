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

# scenario_name = 'Seattle'
# out_scenario_name = 'Seattle'
# file_path = '/Users/xiaodanxu/Documents/SynthFirm.nosync'
# parameter_dir = 'SynthFirm_parameters'
# number_of_processes = 4
# input_dir = 'inputs_' + scenario_name
# output_path = 'outputs_' + out_scenario_name

# fleet_year = 2030
# fleet_name = 'Ref_highp6'
# regulations = 'ACC and ACT'

# # define input files --> eventually loading to pipeline
# os.chdir(file_path)   

# # from upstream module
# synthetic_firms_with_location_file = os.path.join(output_path, 
#                                                   'synthetic_firms_with_location.csv')

# # inputs not vary by scenario
# private_fleet_file = os.path.join(parameter_dir, 'fleet/veh_per_emp_by_state.csv')
# for_hire_fleet_file = os.path.join(parameter_dir, 'fleet/FMCSA_truck_count_by_state_size.csv')
# cargo_type_distribution_file = os.path.join(parameter_dir, 'fleet/probability_of_cargo_group.csv')
# state_fips_lookup_file = os.path.join(parameter_dir, 'us-state-ansi-fips.csv')

# # inputs vary by scenario
# ev_availability_file = os.path.join(parameter_dir, 'fleet/synthfirm_ev_availability.csv')
# private_fuel_mix_file = os.path.join(parameter_dir, 'fleet/private_fuel_mix_scenario.csv')
# hire_fuel_mix_file = os.path.join(parameter_dir, 'fleet/hire_fuel_mix_scenario.csv')
# lease_fuel_mix_file = os.path.join(parameter_dir, 'fleet/lease_fuel_mix_scenario.csv')

# private_stock_file = os.path.join(parameter_dir, 'fleet/private_stock_projection.csv')
# hire_stock_file = os.path.join(parameter_dir, 'fleet/hire_stock_projection.csv')
# lease_stock_file = os.path.join(parameter_dir, 'fleet/lease_stock_projection.csv')
# define output files --> eventually loading to pipeline

def firm_fleet_generator(fleet_year, fleet_name, regulations,
                         synthetic_firms_with_location_file, private_fleet_file,
                         for_hire_fleet_file, cargo_type_distribution_file, state_fips_lookup_file,
                         private_fuel_mix_file, hire_fuel_mix_file, lease_fuel_mix_file,
                         private_stock_file, hire_stock_file, lease_stock_file,
                         firms_with_fleet_file, carriers_with_fleet_file, leasing_with_fleet_file, 
                         ev_availability_file, output_path, 
                         need_regional_calibration = False, regional_variable = None):
    
    scenario_name = fleet_name + ' & ' + regulations
    result_dir = os.path.join(output_path, str(fleet_year), scenario_name)
    isExist = os.path.exists(result_dir)
    if not isExist:
       # Create a new directory because it does not exist
       os.makedirs(result_dir)
       
    firms_with_fleet_file = os.path.join(firms_with_fleet_file)
    carriers_with_fleet_file = os.path.join(carriers_with_fleet_file)
    leasing_with_fleet_file = os.path.join(leasing_with_fleet_file)
    
    # <codecell>
    
    print('Starting national fleet generation...')
    
    # loading input
    firms = read_csv(synthetic_firms_with_location_file)
    print('Total firms before fleet assignment:')
    print(len(firms))
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
    ev_availability = ev_availability.loc[ev_availability['Year'] == fleet_year]
    
    private_stock = private_stock.loc[private_stock['yearID'] == fleet_year]
    hire_stock = hire_stock.loc[hire_stock['yearID'] == fleet_year]
    lease_stock = lease_stock.loc[lease_stock['yearID'] == fleet_year]
    
    vehicle_types = private_fuel_mix.veh_class.unique()
    
    # drop missing in data

    private_fleet.replace(np.inf, np.nan, inplace = True)
    private_fleet.fillna(0, inplace = True)
    # print(private_fleet[vehicle_types].sum())
    # <codecell>
    
    # --------------------------------------------------------
    # Section 2: Generate fleet size by state
    # --------------------------------------------------------
    
    # generate fleet size
    private_fleet.loc[:, 'industry'] = private_fleet.loc[:, 'industry'].astype(str)
    firms.loc[:, 'industry'] = firms.loc[:, 'Industry_NAICS6_Make'].astype(str).str[0:2]
    firms.loc[firms['industry'].isin(["31", "32", "33"]), 'industry'] = "3133"
    firms.loc[firms['industry'].isin(["44", "45", "4A"]), 'industry'] = "4445"
    firms.loc[firms['industry'].isin(["48", "49"]), 'industry'] = "4849"
    firms.loc[firms['industry'].isin(["S0"]), 'industry'] = "92"

    
    attr_to_keep = ['state_abbr', 'industry']
    for veh in vehicle_types:
        rate_attr = 'rate_' + veh
        attr_to_keep.append(rate_attr)

    
    private_fleet = private_fleet[attr_to_keep]
    
    firms.loc[:, 'FAFZONE'] = firms.loc[:,'FAFZONE'].astype(str).str.zfill(3)    
    firms.loc[:, 'st'] = firms.loc[:, 'FAFZONE'].str[:2]
    firms.loc[:, 'st'] = firms.loc[:, 'st'].astype(int)
        
    firms = pd.merge(firms, state_fips_lookup, on = 'st', how = 'left')
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
        
    # print(firms_with_fleet[vehicle_types].sum())
    firms_with_fleet.drop(columns = to_drop, inplace = True)
    print('Total firms after fleet size generation:')
    print(len(firms_with_fleet))
    # <codecell>
    
    # split data into three sets
    carriers = \
        firms_with_fleet.loc[firms_with_fleet['Industry_NAICS6_Make'].isin(['492000', '484000'])]
        
    leasing = firms_with_fleet.loc[firms_with_fleet['Industry_NAICS6_Make'].isin(['532100'])]
    
    private_fleet = \
        firms_with_fleet[~firms_with_fleet['Industry_NAICS6_Make'].isin(['492000', '484000', '532100'])]
    private_fleet.loc[:, vehicle_types] = np.round(private_fleet.loc[:, vehicle_types], 0)
    
    firms_count = len(private_fleet)
    carrier_count = len(carriers)
    leasing_count = len(leasing)
    print(f'Total firms, carriers and leased firms before fleet generation = \
          {firms_count}, {carrier_count}, and {leasing_count}')
    # <codecell>
    
    # ------------------------------------------------------------
    # Section 3: Generate fleet characteristics for private fleet
    # ------------------------------------------------------------
    # process private fleet
    private_fleet.loc[:, 'n_trucks'] = private_fleet.loc[:, vehicle_types].sum(axis = 1)
    private_fleet_no_truck = private_fleet.loc[private_fleet['n_trucks'] == 0]
    private_fleet_no_truck.drop(columns = vehicle_types, inplace = True)
    private_fleet_truck = private_fleet.loc[private_fleet['n_trucks'] > 0]
    # print(private_fleet_truck[vehicle_types].sum())
    
    # <codecell>
    
    # process fuel type mix for private fleet
    
    fuel_attr = ['veh_class', 'state_abbr', 'fuel type', 'veh_fraction']
    private_fuel_mix = private_fuel_mix[fuel_attr]
    # fuel_types = private_fuel_mix['fuel type'].unique()
    
    idx_var = ['CBPZONE', 'FAFZONE', 'esizecat', 'Industry_NAICS6_Make',
           'Commodity_SCTG', 'Emp', 'BusID', 'MESOZONE', 'lat', 'lon',
           'state_abbr'] # 'ZIPCODE'
    if need_regional_calibration:
        idx_var.extend(regional_variable)
    private_fleet = pd.melt(private_fleet_truck, id_vars = idx_var, value_vars = vehicle_types,
                            var_name='veh_class', value_name='number_of_veh')
    
    # keep veh types with non-zero values
    private_fleet = private_fleet.loc[private_fleet['number_of_veh'] > 0]
    
    # append fuel mix and randomly assign fuel types
    private_fleet = pd.merge(private_fleet, private_fuel_mix,
                             on = ['veh_class', 'state_abbr'], how = 'left')
    
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
                                          weights = private_fleet_small['veh_fraction'],
                                          replace = False, random_state = 1)
    
    # combine small and large fleets
    
    private_fleet_truck = pd.concat([private_fleet_large, private_fleet_small])
    
    private_fleet_truck = private_fleet_truck.loc[private_fleet_truck['number_of_veh'] > 0]
    private_fleet_truck.loc[:, 'fleet_id']=private_fleet_truck.groupby('BusID').cumcount() + 1
    
    
    # <codecell>
    
    # --------------------------------------------------------
    # Section 4: Generate carrier characteristics
    # --------------------------------------------------------
    
    # process carrier 
    
    # round fleet size and ensure the minimum = 1
    carriers_with_fleet = carriers.copy()
    carriers_with_fleet.loc[:, 'max_size'] = \
            carriers_with_fleet.loc[:, vehicle_types].max(axis = 1)
    
    # round up maximum to ensure at least 1 truck presents in the fleet        
    for alt in vehicle_types:
        carriers_with_fleet.loc[:, alt] = \
            carriers_with_fleet.loc[:, alt] * (carriers_with_fleet.loc[:, alt]< carriers_with_fleet.loc[:, 'max_size']) + \
                np.ceil(carriers_with_fleet.loc[:, alt]) * (carriers_with_fleet.loc[:, alt] == carriers_with_fleet.loc[:, 'max_size'])
    
    carriers_with_fleet.loc[:, vehicle_types] = \
        np.round(carriers_with_fleet.loc[:, vehicle_types], 0)
    # print(carriers_with_fleet.loc[:, vehicle_types].sum())
    
    
    # scale resulting fleet size 
    carriers_sum = carriers_with_fleet.loc[:, vehicle_types].sum()
    carriers_sum = carriers_sum.to_frame()
    carriers_sum = carriers_sum.reset_index()
    carriers_sum.columns = ['veh_class', 'modeled']
    abs_error = pd.merge(hire_stock, carriers_sum, on = 'veh_class')
    
    abs_error.loc[:, 'error'] = np.abs(abs_error.loc[:, 'modeled'] - \
                                       abs_error.loc[:, 'population_by_year'])
    error_metric = int(abs_error.loc[:, 'error'].sum())
    # print(error_metric)
    err_threshold = 0.01 * hire_stock.population_by_year.sum()
    
    # <codecell>
    
    # adjust fleet by class
    while error_metric > err_threshold:
        # adj fleet by fleet
        for alt in vehicle_types:
            target = int(hire_stock.loc[hire_stock['veh_class'] == alt, 'population_by_year'])
            modeled = int(carriers_with_fleet.loc[:, alt].sum())
            scale_factor = target/modeled
            # print(scale_factor)
            carriers_with_fleet.loc[:, alt] = \
                carriers_with_fleet.loc[:, alt] * scale_factor
            carriers_with_fleet.loc[:, alt] = \
                np.round(carriers_with_fleet.loc[:, alt], 0)
        
        # update error metric
        carriers_sum = carriers_with_fleet.loc[:, vehicle_types].sum()
        carriers_sum = carriers_sum.to_frame()
        carriers_sum = carriers_sum.reset_index()
        carriers_sum.columns = ['veh_class', 'modeled']
        abs_error = pd.merge(hire_stock, carriers_sum, on = 'veh_class')
    
        abs_error.loc[:, 'error'] = np.abs(abs_error.loc[:, 'modeled'] - \
                                           abs_error.loc[:, 'population_by_year'])
        error_metric = int(abs_error.loc[:, 'error'].sum())
        # print(error_metric)
        # break
    # scale fleet size

    carriers_with_fleet.loc[:, 'n_trucks'] = \
        carriers_with_fleet.loc[:, vehicle_types].sum(axis = 1)
    # print(carriers_with_fleet.loc[:, 'n_trucks'].min())
    
    # <codecell>
    # sampling carrier by state
    carrier_veh_type = ['Heavy-duty Tractor', 'Heavy-duty Vocational', 'Medium-duty Vocational']
    carriers_with_fleet.loc[:, 'n_trucks'] = \
        carriers_with_fleet.loc[:, carrier_veh_type].sum(axis = 1)
    # print(carriers_with_fleet.loc[:, 'n_trucks'].min())
    
    to_comb = ['101-1000', '>1000']
    for_hire_fleet.loc[for_hire_fleet['FLEET_SIZE'].isin(to_comb), 'FLEET_SIZE'] = '>100'
    for_hire_fleet = \
        for_hire_fleet.groupby(['HB_STATE', 'FLEET_SIZE'])[['TRUCK_COUNT', 'CARRIER_COUNT']].sum()
    for_hire_fleet= for_hire_fleet.reset_index()    
    fleet_size_bin = [-1, 2, 5, 10, 50, 100, carriers_with_fleet.loc[:, 'n_trucks'].max()]
    bin_labels = ['<=2', '3-5', '6-10', '11-50', '51-100', '>100']
    carriers_with_fleet.loc[:, 'FLEET_SIZE'] = \
        pd.cut(carriers_with_fleet.loc[:, 'n_trucks'], bins = fleet_size_bin,
               labels=bin_labels, right =  True)
        
    # print(carriers_with_fleet.groupby('FLEET_SIZE').size())
    
    # scaling up carrier fleet based on FMCSA data
    carriers_with_fleet_resampled = None
    print('Total trucks from FMCSA:')
    print(for_hire_fleet.TRUCK_COUNT.sum())
    
    prev_size = '<=2'
    
    for state in for_hire_fleet.HB_STATE.unique():
        # print(state)
        for_hire_state = for_hire_fleet.loc[for_hire_fleet['HB_STATE'] == state]
        for size in for_hire_state['FLEET_SIZE'].unique():
            sample_size =  for_hire_state.loc[for_hire_state['FLEET_SIZE'] == size, \
                                              'CARRIER_COUNT']
            sample_size = int(sample_size)
            fleet_size = for_hire_state.loc[for_hire_state['FLEET_SIZE'] == size, 
                                            'TRUCK_COUNT']
            fleet_size = int(fleet_size)
            # truck_per_carrier = int(fleet_size/sample_size)
            pool_of_carriers = \
                carriers_with_fleet.loc[(carriers_with_fleet['state_abbr'] == state)  & \
                                        (carriers_with_fleet['FLEET_SIZE'] == size)]
            pool_size = len(pool_of_carriers)
            if pool_size == 0:
                # print(f'There is no carrier in {state} to sample from under size {size}')
                # collecting samples from previous size groups and reassign fleet size
                pool_of_carriers = \
                    carriers_with_fleet.loc[(carriers_with_fleet['state_abbr'] == state)  & \
                                            (carriers_with_fleet['FLEET_SIZE'] == prev_size)]
                pool_size = len(pool_of_carriers)
            else:
                prev_size = size # update size
                
            sample_carriers = pool_of_carriers.sample(n = sample_size, replace = True)
            sample_carriers.loc[:, 'fleet_id'] = \
                sample_carriers.groupby('BusID').cumcount() + 1
            # scaling_factor = fleet_size / sample_carriers.loc[:, 'n_trucks'].sum()
            # scaling_factor = max(1, scaling_factor)
            
            # for alt in carrier_veh_type:
            #     sample_carriers.loc[:, alt] *= scaling_factor
            #     sample_carriers.loc[:, alt] = \
            #         np.round(sample_carriers.loc[:, alt], 0)
            # sample_carriers.loc[:, 'n_trucks'] = \
            #     sample_carriers.loc[:, carrier_veh_type].sum(axis = 1)
            carriers_with_fleet_resampled = pd.concat([carriers_with_fleet_resampled,
                                                       sample_carriers])
            
    print('Total trucks after resampling SynthFirm carriers:')
    print(carriers_with_fleet_resampled.n_trucks.sum())
    
    # <codecell>
    # assign commodity type
    unique_cargo = cargo_type_distribution.SCTG_group.unique()
    sample_size = len(carriers_with_fleet_resampled)
    
    for cargo in unique_cargo:
        fraction = \
        cargo_type_distribution.loc[cargo_type_distribution['SCTG_group'] == cargo, 'probability']
        carriers_with_fleet_resampled.loc[:, cargo] = np.random.binomial(1, fraction, sample_size)
    
    carriers_with_fleet_resampled.loc[:, 'cargo_check'] = carriers_with_fleet_resampled.loc[:, unique_cargo].sum(axis = 1)
    carriers_with_fleet_resampled.loc[carriers_with_fleet_resampled['cargo_check'] == 0, 'other_cargo'] = 1
    
    
    # <codecell>
    # assign carrier fuel type
    
    idx_var = ['CBPZONE', 'FAFZONE', 'esizecat', 'Industry_NAICS6_Make',
           'Commodity_SCTG', 'Emp', 'BusID', 'MESOZONE',  'lat', 'lon',
           'state_abbr', 'fleet_id'] # 'ZIPCODE',
    # add cargo types
    if need_regional_calibration:
        idx_var.extend(regional_variable)
    idx_var.extend(unique_cargo)
    
    carriers_with_fleet = pd.melt(carriers_with_fleet_resampled, 
                                  id_vars = idx_var, value_vars = vehicle_types,
                            var_name='veh_class', value_name='number_of_veh')
    carriers_with_fleet = carriers_with_fleet.reset_index()
    
    # keep veh types with non-zero values
    carriers_with_fleet = carriers_with_fleet.loc[carriers_with_fleet['number_of_veh'] > 0]
    
    fuel_attr = ['veh_class', 'fuel type', 'veh_fraction']
    hire_fuel_mix = hire_fuel_mix[fuel_attr]
    # fuel_types = hire_fuel_mix['fuel type'].unique()
    
    # append fuel mix and randomly assign fuel types
    carriers_with_fleet = pd.merge(carriers_with_fleet, hire_fuel_mix,
                             on = ['veh_class'], how = 'left')
    
    fleet_threshold = 20
    # for large fleet, assign fuel type by fraction; for small fleet, randomly assign them to single fuel
    carriers_with_fleet_large = carriers_with_fleet.loc[carriers_with_fleet['number_of_veh'] >= fleet_threshold]
    carriers_with_fleet_large.loc[:, 'number_of_veh'] = \
        carriers_with_fleet_large.loc[:, 'number_of_veh'] * carriers_with_fleet_large.loc[:, 'veh_fraction']
    carriers_with_fleet_large.loc[:, 'number_of_veh'] = \
        np.round(carriers_with_fleet_large.loc[:, 'number_of_veh'], 0)
        
    carriers_with_fleet_small = carriers_with_fleet.loc[carriers_with_fleet['number_of_veh'] < fleet_threshold]
    idx_var.append('veh_class')
    carriers_with_fleet_small = carriers_with_fleet_small.groupby(idx_var).sample(n=1, 
                                          weights = carriers_with_fleet_small['veh_fraction'],
                                          replace = False, random_state = 1)
    
    # combine small and large fleets
    carriers_with_fleet = pd.concat([carriers_with_fleet_large, carriers_with_fleet_small])
    
    
    carriers_with_fleet = carriers_with_fleet.loc[carriers_with_fleet['number_of_veh'] > 0]
    carriers_with_fleet.loc[:, 'fleet_id']=carriers_with_fleet.groupby('BusID').cumcount() + 1
    
    
    # <codecell>
    
    
    # --------------------------------------------------------
    # Section 5: Generate lease characteristics
    # --------------------------------------------------------
    
    leasing_with_fleet = leasing.copy()
    fuel_attr = ['veh_class', 'fuel type', 'veh_fraction']
    lease_fuel_mix = lease_fuel_mix[fuel_attr]
    # fuel_types = lease_fuel_mix['fuel type'].unique()
    
    leasing_with_fleet.loc[:, 'max_size'] = \
            leasing_with_fleet.loc[:, vehicle_types].max(axis = 1)
    
    # round up maximum to ensure at least 1 truck presents in the fleet        
    for alt in vehicle_types:
        leasing_with_fleet.loc[:, alt] = \
            leasing_with_fleet.loc[:, alt] * (leasing_with_fleet.loc[:, alt]< leasing_with_fleet.loc[:, 'max_size']) + \
                np.ceil(leasing_with_fleet.loc[:, alt]) * (leasing_with_fleet.loc[:, alt] == leasing_with_fleet.loc[:, 'max_size'])
    
    leasing_with_fleet.loc[:, vehicle_types] = \
        np.round(leasing_with_fleet.loc[:, vehicle_types], 0)
    # print(hire_stock)
    # print(leasing_with_fleet.loc[:, vehicle_types].sum())
    
    # scale resulting fleet size 
    lease_sum = leasing_with_fleet.loc[:, vehicle_types].sum()
    lease_sum = lease_sum.to_frame()
    lease_sum = lease_sum.reset_index()
    lease_sum.columns = ['veh_class', 'modeled']
    abs_error = pd.merge(lease_stock, lease_sum, on = 'veh_class')
    
    abs_error.loc[:, 'error'] = np.abs(abs_error.loc[:, 'modeled'] - \
                                       abs_error.loc[:, 'population_by_year'])
    error_metric = int(abs_error.loc[:, 'error'].sum())
    # print(error_metric)
    err_threshold = 0.01 * lease_stock.population_by_year.sum()
    
    # <codecell>
    
    # adjust fleet by class
    while error_metric > err_threshold:
        # adj fleet by fleet
        for alt in vehicle_types:
            target = int(lease_stock.loc[lease_stock['veh_class'] == alt, 'population_by_year'])
            modeled = int(leasing_with_fleet.loc[:, alt].sum())
            scale_factor = target/modeled
            # print(scale_factor)
            leasing_with_fleet.loc[:, alt] = \
                leasing_with_fleet.loc[:, alt] * scale_factor
            leasing_with_fleet.loc[:, alt] = \
                np.round(leasing_with_fleet.loc[:, alt], 0)
        
        # update error metric
        carriers_sum = leasing_with_fleet.loc[:, vehicle_types].sum()
        carriers_sum = carriers_sum.to_frame()
        carriers_sum = carriers_sum.reset_index()
        carriers_sum.columns = ['veh_class', 'modeled']
        abs_error = pd.merge(lease_stock, carriers_sum, on = 'veh_class')
    
        abs_error.loc[:, 'error'] = np.abs(abs_error.loc[:, 'modeled'] - \
                                           abs_error.loc[:, 'population_by_year'])
        error_metric = int(abs_error.loc[:, 'error'].sum())
        # print(error_metric)
        # break
    
    # scale fleet size
    
    leasing_with_fleet.loc[:, 'n_trucks'] = \
        leasing_with_fleet.loc[:, vehicle_types].sum(axis = 1)
    # print(leasing_with_fleet.loc[:, 'n_trucks'].min())
    
    # <codecell>
    # assign lease fuel type
    
    idx_var = ['CBPZONE', 'FAFZONE', 'esizecat', 'Industry_NAICS6_Make',
           'Commodity_SCTG', 'Emp', 'BusID', 'MESOZONE', 'lat', 'lon',
           'state_abbr'] # 'ZIPCODE', 
    if need_regional_calibration:
        idx_var.extend(regional_variable)
    # add cargo types
    
    leasing_with_fleet = pd.melt(leasing_with_fleet, 
                                  id_vars = idx_var, value_vars = vehicle_types,
                            var_name='veh_class', value_name='number_of_veh')
    leasing_with_fleet = leasing_with_fleet.reset_index()
    
    # keep veh types with non-zero values
    leasing_with_fleet = leasing_with_fleet.loc[leasing_with_fleet['number_of_veh'] > 0]
    
    
    # append fuel mix and randomly assign fuel types
    leasing_with_fleet = pd.merge(leasing_with_fleet, lease_fuel_mix,
                             on = ['veh_class'], how = 'left')
    
    fleet_threshold = 20
    # for large fleet, assign fuel type by fraction; for small fleet, randomly assign them to single fuel
    leasing_with_fleet_large = leasing_with_fleet.loc[leasing_with_fleet['number_of_veh'] >= fleet_threshold]
    leasing_with_fleet_large.loc[:, 'number_of_veh'] = \
        leasing_with_fleet_large.loc[:, 'number_of_veh'] * leasing_with_fleet_large.loc[:, 'veh_fraction']
    leasing_with_fleet_large.loc[:, 'number_of_veh'] = \
        np.round(leasing_with_fleet_large.loc[:, 'number_of_veh'], 0)
        
    leasing_with_fleet_small = leasing_with_fleet.loc[leasing_with_fleet['number_of_veh'] < fleet_threshold]
    idx_var.append('veh_class')
    leasing_with_fleet_small = leasing_with_fleet_small.groupby(idx_var).sample(n=1, 
                                          weights = leasing_with_fleet_small['veh_fraction'],
                                          replace = False, random_state = 1)
    
    # combine small and large fleets
    leasing_with_fleet = pd.concat([leasing_with_fleet_large, leasing_with_fleet_small])
    
    
    leasing_with_fleet = leasing_with_fleet.loc[leasing_with_fleet['number_of_veh'] > 0]
    leasing_with_fleet.loc[:, 'fleet_id'] = leasing_with_fleet.groupby('BusID').cumcount() + 1
    
    
    # <codecell>
    
    # --------------------------------------------------------
    # Section 6: Assign powertrain and writing outputs
    # --------------------------------------------------------
    
    ############# assign EV type #############
    print('Assign EV powertrain types...')
    body_types = ev_availability['vehicleType'].unique()
    private_fleet_truck.loc[:, 'EV_powertrain (if any)'] = np.nan
    carriers_with_fleet.loc[:, 'EV_powertrain (if any)'] = np.nan
    leasing_with_fleet.loc[:, 'EV_powertrain (if any)'] = np.nan
    
    firms_with_fleet_out = None
    carriers_with_fleet_out = None
    leasing_with_fleet_out = None
    
    for bt in body_types:
        # print(bt)
        ev_availability_select = \
        ev_availability.loc[ev_availability['vehicleType'] == bt]
        powertrain = ev_availability_select.Powertrain.to_numpy()
        probability = ev_availability_select.Fraction.to_numpy()
        
        firm_to_assign = \
        private_fleet_truck.loc[private_fleet_truck['veh_class'].str.contains(bt)].reset_index()    
        sample_size_1 = len(firm_to_assign)
        firm_to_assign.loc[:, 'EV_powertrain (if any)'] = \
        pd.Series(np.random.choice(powertrain, size = sample_size_1, p=probability) )
        
        carrier_to_assign = \
        carriers_with_fleet.loc[carriers_with_fleet['veh_class'].str.contains(bt)].reset_index()   
        sample_size_2 = len(carrier_to_assign)
        carrier_to_assign.loc[:, 'EV_powertrain (if any)'] = \
        pd.Series(np.random.choice(powertrain, size = sample_size_2, p=probability) )
            
        lease_to_assign = \
        leasing_with_fleet.loc[leasing_with_fleet['veh_class'].str.contains(bt)].reset_index()   
        sample_size_3 = len(lease_to_assign)
        lease_to_assign.loc[:, 'EV_powertrain (if any)'] = \
        pd.Series(np.random.choice(powertrain, size = sample_size_3, p=probability) )
        
        firms_with_fleet_out = pd.concat([firms_with_fleet_out, firm_to_assign])
        carriers_with_fleet_out = pd.concat([carriers_with_fleet_out, carrier_to_assign])
        leasing_with_fleet_out = pd.concat([leasing_with_fleet_out, lease_to_assign])
    
    veh_class_renaming = {'Heavy-duty Tractor': 'Class 7&8 Tractor', 
                          'Heavy-duty Vocational': 'Class 7&8 Vocational',
                          'Light-duty Class12A': 'Class 1&2A Vocational', 
                          'Light-duty Class2B3': 'Class 2B&3 Vocational',
                          'Medium-duty Vocational': 'Class 4-6 Vocational'}
    
    firms_with_fleet_out.loc[:, 'veh_class'] = \
    firms_with_fleet_out.loc[:, 'veh_class'].map(veh_class_renaming)
    firms_with_fleet_out.loc[:, 'veh_type'] = \
        firms_with_fleet_out.loc[:, 'fuel type'] + ' ' + firms_with_fleet_out.loc[:, 'veh_class']
    
    carriers_with_fleet_out.loc[:, 'veh_class'] = \
    carriers_with_fleet_out.loc[:, 'veh_class'].map(veh_class_renaming)    
    carriers_with_fleet_out.loc[:, 'veh_type'] = \
        carriers_with_fleet_out.loc[:, 'fuel type'] + ' ' + carriers_with_fleet_out.loc[:, 'veh_class']
    
    leasing_with_fleet_out.loc[:, 'veh_class'] = \
    leasing_with_fleet_out.loc[:, 'veh_class'].map(veh_class_renaming)    
    leasing_with_fleet_out.loc[:, 'veh_type'] = \
        leasing_with_fleet_out.loc[:, 'fuel type'] + ' ' + leasing_with_fleet_out.loc[:, 'veh_class']
    
    private_fuel_mix.loc[:, 'veh_class'] = \
        private_fuel_mix.loc[:, 'veh_class'].map(veh_class_renaming)
    private_fuel_mix.loc[:, 'veh_type'] = \
        private_fuel_mix.loc[:, 'fuel type'] + ' ' + private_fuel_mix.loc[:, 'veh_class']
    veh_comb = private_fuel_mix.veh_type.unique()
    
    # <codecell>
    
    # convert long table to wide table
    
    idx_var = ['CBPZONE', 'FAFZONE', 'esizecat', 'Industry_NAICS6_Make',
           'Commodity_SCTG', 'Emp', 'BusID', 'MESOZONE', 'lat', 'lon',
           'state_abbr', 'fleet_id', 'EV_powertrain (if any)'] # 'ZIPCODE', 
    # convert long table to wide
    if need_regional_calibration:
        idx_var.extend(regional_variable)
    firms_with_fleet_out = pd.pivot_table(firms_with_fleet_out, index=idx_var,
                                         columns = 'veh_type', values = 'number_of_veh', aggfunc= 'sum')
    
    firms_with_fleet_out = firms_with_fleet_out.reset_index()
    firms_with_fleet_out.fillna(0, inplace = True)
    
    idx_var = ['CBPZONE', 'FAFZONE', 'esizecat', 'Industry_NAICS6_Make',
           'Commodity_SCTG', 'Emp', 'BusID', 'MESOZONE', 'lat', 'lon',
           'state_abbr', 'fleet_id', 'EV_powertrain (if any)'] # 'ZIPCODE', 
    if need_regional_calibration:
        idx_var.extend(regional_variable)
    idx_var.extend(unique_cargo)
    # convert long table to wide
    carriers_with_fleet_out = pd.pivot_table(carriers_with_fleet_out, index=idx_var,
                                         columns = 'veh_type', values = 'number_of_veh', aggfunc= 'sum')
    
    carriers_with_fleet_out = carriers_with_fleet_out.reset_index()
    carriers_with_fleet_out.fillna(0, inplace = True)
    
    idx_var = ['CBPZONE', 'FAFZONE', 'esizecat', 'Industry_NAICS6_Make',
           'Commodity_SCTG', 'Emp', 'BusID', 'MESOZONE', 'lat', 'lon',
           'state_abbr', 'fleet_id', 'EV_powertrain (if any)'] # 'ZIPCODE'
    if need_regional_calibration:
        idx_var.extend(regional_variable)
    # convert long table to wide
    leasing_with_fleet_out = pd.pivot_table(leasing_with_fleet_out, index=idx_var,
                                         columns = 'veh_type', values = 'number_of_veh', aggfunc= 'sum')
    
    leasing_with_fleet_out = leasing_with_fleet_out.reset_index()
    leasing_with_fleet_out.fillna(0, inplace = True)
    
    
    for veh in veh_comb:
        if veh not in firms_with_fleet_out.columns:
                firms_with_fleet_out[veh] = 0    
        if veh not in carriers_with_fleet_out.columns:
                carriers_with_fleet_out[veh] = 0
        if veh not in leasing_with_fleet_out.columns:
                leasing_with_fleet_out[veh] = 0
    
    print('Final private fleet:')
    firms_with_fleet_out.loc[:, 'n_trucks'] = firms_with_fleet_out.loc[:, veh_comb].sum(axis = 1)
    print(firms_with_fleet_out[veh_comb].sum())
    private_fleet_no_truck.drop(columns = ['industry', 'st', 'stname'], inplace = True)
    firms_with_fleet_out = pd.concat([firms_with_fleet_out, private_fleet_no_truck])
    firms_with_fleet_out.loc[:, 'EV_powertrain (if any)'] = \
        firms_with_fleet_out.loc[:, 'EV_powertrain (if any)'].fillna('Battery Electric')
    firms_with_fleet_out.fillna(0, inplace = True)
    
    print('Final for-hire fleet:')           
    carriers_with_fleet_out.loc[:, 'n_trucks'] = carriers_with_fleet_out.loc[:, veh_comb].sum(axis = 1)
    print(carriers_with_fleet_out[veh_comb].sum())
    
    print('Final lease fleet:')
    leasing_with_fleet_out.loc[:, 'n_trucks'] = leasing_with_fleet_out.loc[:, veh_comb].sum(axis = 1)
    print(leasing_with_fleet_out[veh_comb].sum())
    
    # check remaining business count
    firms_count = len(firms_with_fleet_out.BusID.unique())
    carrier_count = len(carriers_with_fleet_out.BusID.unique())
    leasing_count = len(leasing_with_fleet_out.BusID.unique())
    # firms_count = firms_count + carrier_count + leasing_count
    
    print(f'Total firms, carriers and leased firms after fleet generation = \
          {firms_count}, {carrier_count}, and {leasing_count}')
    
    # format data output
    data_format = {
    'CBPZONE': np.int64,
    'FAFZONE': np.int64,
    'esizecat': np.int64, 
    'Industry_NAICS6_Make': 'string',
    'Commodity_SCTG': np.int64,
    'Emp': 'float',
    'BusID': np.int64, 
    'MESOZONE': np.int64, 
    'lat': 'float', 
    'lon': 'float',
    'state_abbr': 'string', 
    'fleet_id': np.int64, 
    'EV_powertrain (if any)': 'string'
    }
    firms_with_fleet_out = firms_with_fleet_out.astype(data_format)
    carriers_with_fleet_out = carriers_with_fleet_out.astype(data_format)
    leasing_with_fleet_out = leasing_with_fleet_out.astype(data_format)
    
    # <codecell>
    
    
    firms_sample_file = os.path.join(result_dir,'sample_firms_with_fleet.csv')
    
    sample_firms = firms_with_fleet_out.sample(n = 10000)
    sample_firms.to_csv(firms_sample_file, index = False)
    
    firms_with_fleet_out.to_csv(firms_with_fleet_file, index = False)
    carriers_with_fleet_out.to_csv(carriers_with_fleet_file, index = False)
    leasing_with_fleet_out.to_csv(leasing_with_fleet_file, index = False)
    
    print('Finished! please find outputs under ' + result_dir)

