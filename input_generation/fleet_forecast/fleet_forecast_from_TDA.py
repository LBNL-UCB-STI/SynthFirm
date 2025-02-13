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




# carrier_type_lookup = {'FOR HIRE': 'FOR HIRE',
#                        'LOCAL GOVERNMENT (U.S. ONLY)': 'OTHER',
#                        'PRIVATE': 'PRIVATE', 
#                        'STATE GOVERNMENT (U.S. ONLY)': 'OTHER', 
#                        'U.S. GOVERNMENT': 'OTHER', 
#                        'DEALER': 'OTHER', 
#                        'INDIVIDUAL': 'PRIVATE', 
#                        'FINANCE LEASE': 'LEASE',
#                        'MANUFACTURER SPONSORED LEASE': 'LEASE', 
#                        'FULL SERVICE LEASE': 'LEASE', 
#                        'VEHICLE MANUFACTURER': 'OTHER', 
#                        'UTILITIES/COMMUNICATIONS': 'OTHER', 
#                        'CANADIAN GOVERNMENT': 'OTHER'}

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

MOVES_fleet_forecast = pd.read_csv(os.path.join(path_to_moves, 
                                                'turnover', 'pop_growth_and_turnover_rate.csv'))



com_veh_ids = [32, 52, 53, 61, 62]
com_source_type_hpms = \
    source_type_hpms.loc[source_type_hpms['sourceTypeID'].isin(com_veh_ids)]
plot_dir = 'RawData/MOVES/plot_forecast/'
begin_year = 2021
end_year = 2050

# <codecell>
# pull LDT forecast from MOVES for imputation

ldt_fleet_forecast = MOVES_fleet_forecast.loc[MOVES_fleet_forecast['sourceTypeID'] == 32]
ldt_fleet_forecast.loc[:, 'vehicleType'] = 'Light-duty Class2B3'
ldt_fleet_forecast = ldt_fleet_forecast.loc[(ldt_fleet_forecast['yearID']<=end_year) & \
                                            (ldt_fleet_forecast['yearID']>=begin_year)]
    
ldt_fleet_forecast = ldt_fleet_forecast[['yearID', 'vehicleType', 'PopGrowthFactor',
                                         'Cumulative growth rate', 'scrappage rate', 
                                         'new sale rate']]
ldt_fleet_forecast.columns = ['Year', 'vehicleType', 
       'PopGrowthFactor', 'Cumulative growth rate', 'scrappage rate', 'new sale rate']

ldt_fleet_forecast_2 = ldt_fleet_forecast.copy()
ldt_fleet_forecast_2.loc[:, 'vehicleType'] = 'Light-duty Class12A'

ldt_fleet_forecast = pd.concat([ldt_fleet_forecast, ldt_fleet_forecast_2])
# <codecell>

# process TDA results by scenario --> future projections

powertrain_lookup = {'Battery Electric': 'Electric',
                    'Diesel CI': 'Diesel',
                    'Flex Fuel': 'Other',
                    'Gasoline SI': 'Gasoline',
                    'H2 Fuel Cell': 'Electric',
                    'LPG': 'Other',
                    'Natural Gas': 'Other',
                    'PHEV Diesel': 'Electric',
                    'PHEV Gasoline':'Electric'}

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

truck_type_lookup = {'Class 7&8 Vocational': 'HDT vocational',
                    'Class 7&8 Sleeper Tractors': 'HDT tractor',
                    'Class 4-6 Vocational': 'MDT vocational',
                    'Class 3 Vocational': 'MDT vocational',
                    'Class 3 Pickup and Van': 'MDT vocational',
                    'Class 7&8 Day Cab Tractors': 'HDT tractor'}

iterator = 0
for scenario_name in list_of_scenarios:
    print('processing fleet under ' + scenario_name)
    vehicle_stock = pd.read_excel(file_name, sheet_name = scenario_name)  
#     print(vehicle_stock.columns)

    list_of_veh_class = vehicle_stock.Class.unique()
    list_of_fuel_type = vehicle_stock.fuel_1.unique()
    vehicle_stock.loc[:, 'vehicleType'] = \
        vehicle_stock.loc[:, 'Class'].map(truck_type_lookup)
    vehicle_stock.loc[:, 'fuelType'] = \
        vehicle_stock.loc[:, 'Powertrain'].map(powertrain_lookup)
    # vehicle_stock.loc[:, 'fuelTypeID'] = \
    #     vehicle_stock.loc[:, 'fuel_1'].map(fuel_type_lookup)
    # vehicle_stock.loc[:, 'engTechID'] = \
    #     vehicle_stock.loc[:, 'fuel_1'].map(eng_tech_lookup)
        
    # data filtering
    vehicle_stock = vehicle_stock.loc[vehicle_stock['fuelType'] != 'Other']
    to_exclude = ['Class 3 Vocational', 'Class 3 Pickup and Van']
    vehicle_stock = vehicle_stock.loc[~vehicle_stock['Class'].isin(to_exclude)]
    
    new_sale = \
        vehicle_stock.loc[vehicle_stock['Year'] == vehicle_stock['MY']]
        
    if iterator == 0:
        vehicle_stock_by_year = vehicle_stock.groupby(['Year', 'vehicleType'])[['Stock', 'VMT_millions']].sum()
        vehicle_stock_by_year = vehicle_stock_by_year.reset_index()
        
        # calculate stock growth factor
        vehicle_stock_by_year = vehicle_stock_by_year.loc[vehicle_stock_by_year['Year'] >= begin_year]
        vehicle_stock_by_year = vehicle_stock_by_year.sort_values(by = 'Year', ascending = True)
        vehicle_stock_by_year.loc[:, 'StockPre'] = \
            vehicle_stock_by_year.groupby('vehicleType')['Stock'].shift(1)
        vehicle_stock_by_year.loc[:, 'PopGrowthFactor'] = \
            vehicle_stock_by_year.loc[:, 'Stock']/ \
                vehicle_stock_by_year.loc[:, 'StockPre']
        vehicle_stock_by_year.loc[:, 'PopGrowthFactor'].fillna(1, inplace = True)
        
        vehicle_stock_by_year.loc[:, 'Cumulative growth rate'] = \
            vehicle_stock_by_year.groupby('vehicleType')['PopGrowthFactor'].cumprod()
            
        sns.lineplot(data=vehicle_stock_by_year, 
                     x="Year", y="Cumulative growth rate", 
                     hue="vehicleType", style = 'vehicleType',
                     markers=True)
        plt.legend(bbox_to_anchor = (1.01, 1))
        plt.savefig(os.path.join(plot_dir, 'synthfirm_stock_growth_' + scenario_name + '.png'), 
                    dpi = 300, bbox_inches = 'tight')
        plt.show()
        
        vehicle_stock_by_year.loc[:, 'DeltaPop'] = \
            vehicle_stock_by_year.loc[:, 'StockPre'] * \
                (vehicle_stock_by_year.loc[:, 'PopGrowthFactor'] - 1)
        vehicle_stock_by_year.loc[:, 'DeltaPop'].fillna(0, inplace = True)
        
        new_sale_by_veh = new_sale.groupby(['Year', 'vehicleType'])[['Stock']].sum()
        new_sale_by_veh = new_sale_by_veh.reset_index()
        new_sale_by_veh.rename(columns = {'Stock': 'NewSale'}, inplace = True)
    
        vehicle_stock_by_year = pd.merge(vehicle_stock_by_year,
                                         new_sale_by_veh,
                                         on = ['Year', 'vehicleType'],
                                         how = 'left')
        
    
        vehicle_stock_by_year.loc[:, 'ScrappedVeh'] = \
            vehicle_stock_by_year.loc[:, 'NewSale'] - \
                vehicle_stock_by_year.loc[:, 'DeltaPop']
        
        vehicle_stock_by_year.loc[:, 'scrappage rate'] = \
            vehicle_stock_by_year.loc[:, 'ScrappedVeh']/ \
                vehicle_stock_by_year.loc[:, 'Stock']
        vehicle_stock_by_year.loc[:, 'new sale rate'] = \
            vehicle_stock_by_year.loc[:, 'NewSale']/ \
                vehicle_stock_by_year.loc[:, 'Stock']
        vehicle_stock_by_year = vehicle_stock_by_year[['Year', 'vehicleType',                                                   
               'PopGrowthFactor', 'Cumulative growth rate', 'scrappage rate', 'new sale rate']]
        vehicle_stock_by_year = pd.concat([vehicle_stock_by_year, ldt_fleet_forecast])
        # scrappage_frac = source_type_population_with_sale[['yearID', 'sourceTypeID',
        #                                                    'scrappage rate', 'new sale rate']]
        vehicle_stock_by_year.to_csv(os.path.join(path_to_moves, 'turnover', 
                                                  'SynthFirm_pop_growth_and_turnover_rate.csv'),
                                 index = False)
        print(vehicle_stock_by_year.head(5))

    new_sale.rename(columns = {'MY': 'modelYearID'}, inplace = True)
    agg_var = ['vehicleType', 'modelYearID', 'fuelType']
    
    
    avft_from_tda = new_sale.groupby(agg_var)[['Stock']].sum()
    avft_from_tda = avft_from_tda.reset_index()
    avft_from_tda = avft_from_tda.loc[avft_from_tda['modelYearID'] >= begin_year]
    avft_from_tda.loc[:, 'stmyFraction'] = \
        avft_from_tda.loc[:, 'Stock'] /  \
            avft_from_tda.groupby(['vehicleType', 'modelYearID'])['Stock'].transform('sum')
            
    agg_var_2 = ['vehicleType', 'Year', 'fuelType']
    stock_from_tda = vehicle_stock.groupby(agg_var_2)[['Stock']].sum()
    stock_from_tda = stock_from_tda.reset_index()
    stock_from_tda.loc[:, 'Fraction'] = \
        stock_from_tda.loc[:, 'Stock'] /  \
            stock_from_tda.groupby(['vehicleType', 'Year'])['Stock'].transform('sum')
            
    # assign fleet attributes and clean data
    avft_from_tda_sum = \
        avft_from_tda.groupby(['vehicleType', 'modelYearID', 'fuelType'])['stmyFraction'].sum()
    avft_from_tda_sum = avft_from_tda_sum.reset_index()
    # ax = sns.relplot(avft_from_tda_sum, x= 'modelYearID', y = 'stmyFraction', hue = 'fuelType',
    #             row = 'vehicleType', kind = 'line', palette='Set2')
    # ax.set_titles("{row_name}")
    # plt.savefig(os.path.join(plot_dir, 'synthfirm_avft_' + scenario_name + '.png'), dpi = 300)
    # plt.show()
    
    # avft_from_tda = pd.merge(avft_from_tda, hpms_definition,
    #                           on = ['HPMSVtypeName'], how = 'left')
    # avft_from_tda = pd.merge(avft_from_tda, com_source_type_hpms,
    #                           on = ['HPMSVtypeID'], how = 'left')
    
    # avft_from_tda.drop(columns = ['Stock', 'HPMSVtypeID'], inplace = True)
    output_file_name = 'synthfirm_TDA_AVFT_' + scenario_name + '.csv'
    # avft_from_tda_sum.to_csv(output_dir + output_file_name)
    iterator += 1
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