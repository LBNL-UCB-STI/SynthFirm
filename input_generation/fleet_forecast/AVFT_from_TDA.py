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
# fleet_by_state = read_csv('PrivateData/registration/MDHDbyState.csv')
file_name = 'PrivateData/registration/TDA_results/BEAMFreightSensitivity_Ref.xlsx'
list_of_scenarios = ['Ref_highp2', 'Ref_highp4', 'Ref_highp6',
                    'Ref_highp8', 'Ref_highp10']
output_dir = 'RawData/MOVES/turnover/'
path_to_moves = 'RawData/MOVES'
source_type_hpms = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'source_type_HPMS')
hpms_definition = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'HPMS_definition')

fuel_type_distribution = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'fuel_type_distribution')

fuel_type_definition = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'fuel_type_definition')

com_veh_ids = [32, 52, 53, 61, 62]
com_source_type_hpms = \
    source_type_hpms.loc[source_type_hpms['sourceTypeID'].isin(com_veh_ids)]
plot_dir = 'RawData/MOVES/plot_forecast/'
begin_year = 2021
# <codecell>

# process TDA results by scenario --> future projections
fuel_type_lookup = {'Diesel': 2, 
                        'Gasoline': 1, 
                        'LPG': 0, 
                        'Natural Gas': 3, 
                        'Electricity': 9,
                        'Hydrogen': 9}

eng_tech_lookup = {'Diesel': 1, 
                   'Gasoline': 1, 
                   'LPG': 0, 
                   'Natural Gas': 1,
                   'Electricity': 30,
                   'Hydrogen': 40}

truck_type_lookup = {'Class 7&8 Vocational': 'Single Unit Trucks',
                    'Class 7&8 Sleeper Tractors': 'Combination Trucks',
                    'Class 4-6 Vocational': 'Single Unit Trucks',
                    'Class 3 Vocational': 'Single Unit Trucks',
                    'Class 3 Pickup and Van': 'Single Unit Trucks',
                    'Class 7&8 Day Cab Tractors': 'Combination Trucks'}


for scenario_name in list_of_scenarios:
    print('processing fleet under ' + scenario_name)
    vehicle_stock = pd.read_excel(file_name, sheet_name = scenario_name)  
#     print(vehicle_stock.columns)

    list_of_veh_class = vehicle_stock.Class.unique()
    list_of_fuel_type = vehicle_stock.fuel_1.unique()
    vehicle_stock.loc[:, 'HPMSVtypeName'] = \
        vehicle_stock.loc[:, 'Class'].map(truck_type_lookup)
    vehicle_stock.loc[:, 'fuelTypeID'] = \
        vehicle_stock.loc[:, 'fuel_1'].map(fuel_type_lookup)
    vehicle_stock.loc[:, 'engTechID'] = \
        vehicle_stock.loc[:, 'fuel_1'].map(eng_tech_lookup)
        
    # data filtering
    vehicle_stock = vehicle_stock.loc[vehicle_stock['fuelTypeID'] != 0]
    to_exclude = ['Class 3 Vocational', 'Class 3 Pickup and Van']
    vehicle_stock = vehicle_stock.loc[~vehicle_stock['Class'].isin(to_exclude)]
    
    # print(vehicle_stock.head(5))
    new_sale = \
        vehicle_stock.loc[vehicle_stock['Year'] == vehicle_stock['MY']]
    new_sale.rename(columns = {'MY': 'modelYearID'}, inplace = True)
    agg_var = ['HPMSVtypeName', 'modelYearID', 'fuelTypeID', 'engTechID']
    avft_from_tda = new_sale.groupby(agg_var)[['Stock']].sum()
    avft_from_tda = avft_from_tda.reset_index()
    avft_from_tda = avft_from_tda.loc[avft_from_tda['modelYearID'] >= begin_year]
    avft_from_tda.loc[:, 'stmyFraction'] = \
        avft_from_tda.loc[:, 'Stock'] /  \
            avft_from_tda.groupby(['HPMSVtypeName', 'modelYearID'])['Stock'].transform('sum')
            
    agg_var_2 = ['HPMSVtypeName', 'Year', 'fuelTypeID', 'engTechID']
    stock_from_tda = vehicle_stock.groupby(agg_var_2)[['Stock']].sum()
    stock_from_tda = stock_from_tda.reset_index()
    stock_from_tda.loc[:, 'Fraction'] = \
        stock_from_tda.loc[:, 'Stock'] /  \
            stock_from_tda.groupby(['HPMSVtypeName', 'Year'])['Stock'].transform('sum')
    # assign fleet attributes and clean data
    avft_from_tda_sum = \
        avft_from_tda.groupby(['HPMSVtypeName', 'modelYearID', 'fuelTypeID'])['stmyFraction'].sum()
    avft_from_tda_sum = avft_from_tda_sum.reset_index()
    ax = sns.relplot(avft_from_tda_sum, x= 'modelYearID', y = 'stmyFraction', hue = 'fuelTypeID',
                row = 'HPMSVtypeName', kind = 'line', palette='Dark2_r')
    ax.set_titles("{row_name}")
    plt.savefig(os.path.join(plot_dir, 'avft_' + scenario_name + '.png'), dpi = 300)
    plt.show()
    
    avft_from_tda = pd.merge(avft_from_tda, hpms_definition,
                              on = ['HPMSVtypeName'], how = 'left')
    avft_from_tda = pd.merge(avft_from_tda, com_source_type_hpms,
                              on = ['HPMSVtypeID'], how = 'left')
    
    avft_from_tda.drop(columns = ['Stock', 'HPMSVtypeID'], inplace = True)
    output_file_name = 'TDA_AVFT_' + scenario_name + '.csv'
    avft_from_tda.to_csv(output_dir + output_file_name)
    # break

# <codecell>

# checking MOVES fuel mix

fuel_type_agg_frac = \
    fuel_type_distribution.groupby(['sourceTypeID', 'modelYearID', 'fuelTypeID'])[['stmyFraction']].sum()
fuel_type_agg_frac = fuel_type_agg_frac.reset_index()    
fuel_type_agg_frac = fuel_type_agg_frac.loc[fuel_type_agg_frac['sourceTypeID'].isin(com_veh_ids)]
fuel_type_agg_frac = fuel_type_agg_frac.loc[fuel_type_agg_frac['modelYearID'] >= begin_year]
fuel_type_agg_frac = fuel_type_agg_frac.loc[fuel_type_agg_frac['modelYearID'] <= 2050]
fuel_type_agg_frac = pd.merge(fuel_type_agg_frac, com_source_type_hpms,
                          on = ['sourceTypeID'], how = 'left')
fuel_type_agg_frac = fuel_type_agg_frac.sort_values(by = 'sourceTypeID')
ax = sns.relplot(fuel_type_agg_frac, x= 'modelYearID', y = 'stmyFraction', hue = 'fuelTypeID',
            col = 'sourceTypeName', col_wrap = 3, kind = 'line', palette='Dark2_r')
ax.set_titles("{col_name}")
plt.savefig(os.path.join(plot_dir, 'avft_MOVES_default.png'), dpi = 300)
plt.show()