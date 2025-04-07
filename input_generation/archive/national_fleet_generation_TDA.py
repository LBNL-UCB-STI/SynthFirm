#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug  8 15:25:27 2023

@author: xiaodanxu
"""

from pandas import read_csv
import pandas as pd
import numpy as np
import os
import warnings
import seaborn as sns
import matplotlib.pyplot as plt

warnings.filterwarnings('ignore')
plt.style.use('ggplot')
sns.set(font_scale=1.2)  # larger font



########### define inputs and scenarios ############
os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')


powertrain_lookup = {'Battery Electric': 'Electric',
                    'Diesel CI': 'Diesel',
                    'Flex Fuel': 'Other',
                    'Gasoline SI': 'Gasoline',
                    'H2 Fuel Cell': 'Electric',
                    'LPG': 'Other',
                    'Natural Gas': 'Other',
                    'PHEV Diesel': 'Electric',
                    'PHEV Gasoline':'Electric'}

truck_type_lookup = {'STRAIGHT TRUCK': 'Vocational',
                    'TRACTOR TRUCK': 'Tractor',
                    'CAB CHASSIS': 'Vocational',
                    'CUTAWAY': 'Vocational',
                    'INCOMPLETE (STRIP CHASSIS)': 'Vocational',
                    'STEP VAN': 'Vocational',
                    'VAN CARGO': 'Vocational',
                    'GLIDERS': 'Tractor'}

carrier_type_lookup = {'FOR HIRE': 'FOR HIRE',
                       'LOCAL GOVERNMENT (U.S. ONLY)': 'OTHER',
                       'PRIVATE': 'PRIVATE', 
                       'STATE GOVERNMENT (U.S. ONLY)': 'OTHER', 
                       'U.S. GOVERNMENT': 'OTHER', 
                       'DEALER': 'OTHER', 
                       'INDIVIDUAL': 'PRIVATE', 
                       'FINANCE LEASE': 'LEASE',
                       'MANUFACTURER SPONSORED LEASE': 'LEASE', 
                       'FULL SERVICE LEASE': 'LEASE', 
                       'VEHICLE MANUFACTURER': 'OTHER', 
                       'UTILITIES/COMMUNICATIONS': 'OTHER', 
                       'CANADIAN GOVERNMENT': 'OTHER'}

# Process Polk registration data by state --> current fleet composition
fleet_by_state = read_csv('PrivateData/registration/MDHDbyState.csv')
file_name = 'PrivateData/registration/TDA_results/BEAMFreightSensitivity_Ref.xlsx'
list_of_scenarios = ['Ref_highp2', 'Ref_highp4', 'Ref_highp6',
                    'Ref_highp8', 'Ref_highp10']
output_dir = 'inputs_BayArea/fleet/'

# <codecell>

############# processing national registration data ############
carrier_types = fleet_by_state.carrier_type.unique()
vehicle_types = fleet_by_state.vehicle_type.unique()
# print(carrier_types)
# print(vehicle_types)

# generate fleet attributes and clean data
fleet_by_state.loc[:, 'Weight class'] = 'Class 4-6'
fleet_by_state.loc[fleet_by_state['vin_gvw'].isin([7,8]), 'Weight class'] = 'Class 7&8'
fleet_by_state.loc[:, 'Body type'] = fleet_by_state.loc[:, 'vehicle_type'].map(truck_type_lookup)
fleet_by_state.loc[:, 'Service type'] = fleet_by_state.loc[:, 'carrier_type'].map(carrier_type_lookup)
fleet_by_state_filtered = fleet_by_state.dropna(subset = ['Body type'])

carrier_types = ['PRIVATE', 'FOR HIRE', 'LEASE']
fleet_by_state_filtered = \
fleet_by_state_filtered.loc[fleet_by_state_filtered['Service type'].isin(carrier_types)]
fleet_by_state_filtered.loc[:, 'vehicle type'] = \
fleet_by_state_filtered.loc[:, 'Weight class'] + ' ' + fleet_by_state_filtered.loc[:, 'Body type']
# fleet_by_state_filtered.head(5)

# summarize fleet by state
fleet_by_state_output = \
fleet_by_state_filtered.groupby(['state', 'Service type', 'vehicle type'])[['totcount']].sum()
fleet_by_state_output = fleet_by_state_output.reset_index()

fleet_by_state_output.loc[:, 'Truck_fraction'] = \
fleet_by_state_output.loc[:, 'totcount'] / \
fleet_by_state_output.groupby(['state', 'Service type'])['totcount'].transform('sum')

fleet_by_state_output = \
fleet_by_state_output.loc[fleet_by_state_output['vehicle type'] != 'Class 4-6 Tractor']

# process TDA results by scenario --> future projections


for scenario_name in list_of_scenarios:
    print('processing fleet under ' + scenario_name)
    vehicle_stock = pd.read_excel(file_name, sheet_name = scenario_name)  
#     print(vehicle_stock.columns)
    # vehicle_stock = read_csv('registration/TDAResults.csv')
    # print(vehicle_stock.columns)
#     list_of_veh_class = vehicle_stock.Class.unique()
#     list_of_fuel_type = vehicle_stock.fuel_1.unique()

    # assign fleet attributes and clean data
    to_exclude = ['Class 3 Vocational', 'Class 3 Pickup and Van']
    vehicle_stock = vehicle_stock.loc[~vehicle_stock['Class'].isin(to_exclude)]

    vehicle_stock.loc[:, 'Body type'] = 'Vocational'
    tractor_types = ['Class 7&8 Sleeper Tractors', 'Class 7&8 Day Cab Tractors']
    vehicle_stock.loc[vehicle_stock['Class'].isin(tractor_types), 'Body type'] = 'Tractor'

    vehicle_stock.loc[:, 'Weight Class'] = 'Class 7&8'
    medium_duty_truck_type = ['Class 4-6 Vocational']
    vehicle_stock.loc[vehicle_stock['Class'].isin(medium_duty_truck_type), 'Weight Class'] = 'Class 4-6'

    vehicle_stock.loc[:, 'vehicle type'] = vehicle_stock.loc[:, 'Weight Class'] + ' ' + \
    vehicle_stock.loc[:, 'Body type']
    vehicle_stock.loc[:, 'fuel type'] = vehicle_stock.loc[:, 'Powertrain'].map(powertrain_lookup)

    # summarize results
    vehicle_stock_output = vehicle_stock.groupby(['Year', 'vehicle type', 'fuel type'])[['Stock', 'VMT_millions']].sum()
    vehicle_stock_output = vehicle_stock_output.reset_index()
    vehicle_stock_output = vehicle_stock_output.loc[vehicle_stock_output['fuel type'] != 'Other']

    vehicle_stock_output.loc[:, 'vehicle category'] = vehicle_stock_output.loc[:, 'fuel type'] + ' ' + \
    vehicle_stock_output.loc[:, 'vehicle type']

    to_drop = ['Gasoline Class 7&8 Tractor', 'Gasoline Class 7&8 Vocational']
    vehicle_stock_output = vehicle_stock_output.loc[~vehicle_stock_output['vehicle category'].isin(to_drop)]

    vehicle_stock_output.loc[:, 'Fraction'] = vehicle_stock_output.loc[:, 'Stock'] / \
    vehicle_stock_output.groupby(['Year', 'vehicle type'])['Stock'].transform('sum')
    
    # summarize EV fleet
    EV_stock = vehicle_stock.loc[vehicle_stock['fuel type'] == 'Electric']
    EV_stock_by_veh_type = EV_stock.groupby(['Year', 'vehicle type', 'Powertrain'])[['Stock']].sum()
    EV_stock_by_veh_type = EV_stock_by_veh_type.reset_index()
    EV_stock_by_veh_type = EV_stock_by_veh_type.loc[EV_stock_by_veh_type['Stock']>= 0]
    EV_stock_by_veh_type.loc[:, 'EV_fraction'] = EV_stock_by_veh_type.loc[:, 'Stock'] / \
    EV_stock_by_veh_type.groupby(['Year', 'vehicle type'])['Stock'].transform('sum')

    # generate future fleet composition by state
    fleet_by_state_output_2 = pd.merge(fleet_by_state_output,
                                 vehicle_stock_output,
                                 on = 'vehicle type', how = 'left')
    fleet_by_state_output_2.loc[:, 'veh_fraction'] = \
    fleet_by_state_output_2.loc[:, 'Truck_fraction'] * \
    fleet_by_state_output_2.loc[:, 'Fraction']
    fleet_by_state_output_2 = \
    fleet_by_state_output_2[['Year', 'state', 'Service type',	'vehicle type', 
                           'fuel type', 'vehicle category', 'veh_fraction']]
    
    # writing output

    output_path = output_dir + scenario_name
    isExist = os.path.exists(output_path)
    if not isExist:
       # Create a new directory because it does not exist
       os.makedirs(output_path)
    fleet_by_state_output_2.to_csv(output_path + '/fleet_composition_by_state.csv', index = False)
    vehicle_stock_output.to_csv(output_path + '/TDA_vehicle_stock.csv', index = False)
    # EV_stock_output.to_csv('registration/output/' + scenario_name + '/EV_fraction_by_type.csv', index = False)
    EV_stock_by_veh_type.to_csv(output_path + '/EV_availability.csv', index = False)
    # visualize fleet composition
    vehicle_stock_agg_2 = vehicle_stock_output.groupby(['Year', 'vehicle type', 'fuel type'])[['Stock', 'VMT_millions']].sum()
    vehicle_stock_agg_2 = vehicle_stock_agg_2.reset_index()

    sns.relplot(
        data = vehicle_stock_agg_2, x = "Year", y = "Stock",
        row = 'vehicle type', hue = "fuel type", 
        kind="line", height = 3.5, aspect = 1.5
    )

    plt.savefig('PrivateData/registration/plot/TDA_fleet_by_year_' + scenario_name + '.png', dpi = 200, bbox_inches = 'tight')

    sns.relplot(
        data = vehicle_stock_agg_2, x = "Year", y = "VMT_millions",
        row = 'vehicle type', hue = "fuel type", 
        kind="line", height = 3.5, aspect = 1.5
    )

    plt.savefig('PrivateData/registration/plot/TDA_VMT_by_year_' + scenario_name + '.png', dpi = 200, bbox_inches = 'tight')
#     break
# vehicle_stock_output.head(10)
