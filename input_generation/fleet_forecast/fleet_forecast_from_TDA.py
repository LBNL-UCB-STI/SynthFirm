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


# Process Polk registration data by state --> current fleet composition

file_names = ['PrivateData/registration/TDA_results/BEAMFreightSensitivity_Ref.xlsx',
              'PrivateData/registration/TDA_results/BEAMFreightSensitivity_HOPmod.xlsx']

list_of_scenarios = {'PrivateData/registration/TDA_results/BEAMFreightSensitivity_Ref.xlsx':\
                     ['Ref_highp2', 'Ref_highp4', 'Ref_highp6', 'Ref_highp8', 'Ref_highp10'],
                     'PrivateData/registration/TDA_results/BEAMFreightSensitivity_HOPmod.xlsx':\
                         ['HOP_highp2', 'HOP_highp4', 'HOP_highp6', 'HOP_highp8', 'HOP_highp10']}
    
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

EV_sale_target = pd.read_csv(os.path.join(path_to_moves, 
                                                'turnover', 'EV_sale_target.csv'))

com_veh_ids = [32, 52, 53, 61, 62]
com_source_type_hpms = \
    source_type_hpms.loc[source_type_hpms['sourceTypeID'].isin(com_veh_ids)]
plot_dir = 'RawData/MOVES/plot_forecast/'
begin_year = 2018
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

truck_type_lookup = {'Class 7&8 Vocational': 'Heavy-duty Vocational',
                    'Class 7&8 Sleeper Tractors': 'Heavy-duty Tractor',
                    'Class 4-6 Vocational': 'Medium-duty Vocational',
                    'Class 3 Vocational': 'Medium-duty Vocational',
                    'Class 3 Pickup and Van': 'Medium-duty Vocational',
                    'Class 7&8 Day Cab Tractors': 'Heavy-duty Tractor'}

iterator = 1
output_ev_frac = None
output_EV_powertrain_frac = None

for file in file_names:
    scenarios = list_of_scenarios[file]
    
    for scenario_name in scenarios:
        print('processing fleet under ' + scenario_name)
        vehicle_stock = pd.read_excel(file, sheet_name = scenario_name)  
    #     print(vehicle_stock.columns)
    
        list_of_veh_class = vehicle_stock.Class.unique()
        list_of_fuel_type = vehicle_stock.fuel_1.unique()
        vehicle_stock.loc[:, 'vehicleType'] = \
            vehicle_stock.loc[:, 'Class'].map(truck_type_lookup)
        vehicle_stock.loc[:, 'fuelType'] = \
            vehicle_stock.loc[:, 'Powertrain'].map(powertrain_lookup)
            
        # data filtering
        vehicle_stock = vehicle_stock.loc[vehicle_stock['fuelType'] != 'Other']
        to_exclude = ['Class 3 Vocational', 'Class 3 Pickup and Van']
        vehicle_stock = vehicle_stock.loc[~vehicle_stock['Class'].isin(to_exclude)]
        
        new_sale = \
            vehicle_stock.loc[vehicle_stock['Year'] == vehicle_stock['MY']]
            
        if iterator == 0: # produce fleet forecast using one TDA output
            print('generate fleet projection parameters...')
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

            vehicle_stock_by_year.to_csv(os.path.join(path_to_moves, 'turnover', 
                                                      'SynthFirm_pop_growth_and_turnover_rate.csv'),
                                     index = False)
    
        new_sale.rename(columns = {'MY': 'modelYearID'}, inplace = True)
        agg_var = ['vehicleType', 'modelYearID', 'fuelType']
        
        # combine two phev
        PHEV_classes = ['PHEV Diesel', 'PHEV Gasoline']
        new_sale.loc[new_sale['Powertrain'].isin(PHEV_classes), 'Powertrain'] = 'PHEV'
        vehicle_stock.loc[vehicle_stock['Powertrain'].isin(PHEV_classes), 'Powertrain'] = 'PHEV'
        
        avft_from_tda = new_sale.groupby(agg_var)[['Stock']].sum()
        avft_from_tda = avft_from_tda.reset_index()
        avft_from_tda = avft_from_tda.loc[avft_from_tda['modelYearID'] >= begin_year]
        avft_from_tda.loc[:, 'stmyFraction'] = \
            avft_from_tda.loc[:, 'Stock'] /  \
                avft_from_tda.groupby(['vehicleType', 'modelYearID'])['Stock'].transform('sum')
        output_attr = ['vehicleType', 'modelYearID', 'fuelType', 'stmyFraction']
        avft_from_tda = avft_from_tda.loc[avft_from_tda['fuelType'] == 'Electric',
                                          output_attr]   
        
        agg_var_2 = ['vehicleType', 'Year', 'fuelType', 'Powertrain']
        
        powertrain_from_tda = vehicle_stock.groupby(agg_var_2)[['Stock']].sum()
        powertrain_from_tda = powertrain_from_tda.reset_index()
        powertrain_from_tda = powertrain_from_tda.loc[powertrain_from_tda['Year'] >= begin_year]
        powertrain_from_tda = \
            powertrain_from_tda.loc[powertrain_from_tda['fuelType'] == 'Electric']

        powertrain_from_tda.loc[:, 'row_sum'] = \
                powertrain_from_tda.groupby(['vehicleType', 'Year', 'fuelType'])['Stock'].transform('sum')
        imp_idx = (powertrain_from_tda['row_sum'] == 0) & \
            (powertrain_from_tda['Powertrain'] == 'Battery Electric')
        powertrain_from_tda.loc[imp_idx, 'Stock'] = 1 # if entire stock missing, assuming 100% BEV
        powertrain_from_tda.loc[imp_idx, 'row_sum'] = 1
        powertrain_from_tda.loc[:, 'Fraction'] = \
            powertrain_from_tda.loc[:, 'Stock'] /  \
                powertrain_from_tda.loc[:, 'row_sum']
        
        powertrain_from_tda = \
            powertrain_from_tda[['vehicleType', 'Year', 'fuelType', 'Powertrain', 'Fraction']]    
        powertrain_from_tda.fillna(0, inplace = True)
        avft_from_tda.loc[:, 'scenario'] = scenario_name
        powertrain_from_tda.loc[:, 'scenario'] = scenario_name
        output_ev_frac = pd.concat([output_ev_frac, avft_from_tda])
        output_EV_powertrain_frac = pd.concat([output_EV_powertrain_frac, powertrain_from_tda ])
        iterator += 1
    #     break
    # break

# <codecell>

# impute LDT EV adoption

LDT_ev_frac = output_ev_frac.loc[output_ev_frac['vehicleType'] == 'Medium-duty Vocational']
LDT_ev_frac['vehicleType'] = 'Light-duty Class2B3'

epa_target = fuel_type_distribution.loc[(fuel_type_distribution['sourceTypeID'] == 32) & \
                                        (fuel_type_distribution['modelYearID'] <= end_year) &
                                        (fuel_type_distribution['modelYearID'] >= begin_year)]
                            
epa_target = epa_target.groupby(['modelYearID', 'fuelTypeID'])['stmyFraction'].sum()
epa_target = epa_target.reset_index()
epa_target = epa_target.loc[epa_target['fuelTypeID'] == 9]
epa_target.drop(columns = ['fuelTypeID'], inplace = True)

LDT_ev_frac = pd.merge(LDT_ev_frac, epa_target, on = 'modelYearID', how = 'left')
LDT_ev_frac.loc[:, 'stmyFraction'] = \
LDT_ev_frac.loc[:, ["stmyFraction_x", "stmyFraction_y"]].max(axis=1)
LDT_ev_frac.drop(columns = ["stmyFraction_x", "stmyFraction_y"], inplace = True)

LDT_ev_frac_2 = LDT_ev_frac.copy()
LDT_ev_frac_2['vehicleType'] = 'Light-duty Class12A'

output_ev_frac = pd.concat([output_ev_frac, LDT_ev_frac, LDT_ev_frac_2])

# <codecell>

# impute LDT EV powertrain

LDT_ev_pt_frac = output_EV_powertrain_frac.loc[output_EV_powertrain_frac['vehicleType'] == 'Medium-duty Vocational']
LDT_ev_pt_frac['vehicleType'] = 'Light-duty Class2B3'

LDT_ev_pt_frac_2 = LDT_ev_pt_frac.copy()
LDT_ev_pt_frac_2['vehicleType'] = 'Light-duty Class12A'

output_EV_powertrain_frac = pd.concat([output_EV_powertrain_frac, 
                                       LDT_ev_pt_frac, LDT_ev_pt_frac_2])

# <codecell>

# write baseline output --- no EV target
output_ev_frac.to_csv(os.path.join(path_to_moves, 'turnover', 'synthfirm_ev_market.csv'),
                          index = False)

output_EV_powertrain_frac.to_csv(os.path.join(path_to_moves, 'turnover', 'synthfirm_ev_availability.csv'),
                          index = False)


# <codecell>

# adjust EV ratio to account for ACC+ACT rules
output_ev_frac_act = output_ev_frac.copy()
EV_sale_target = EV_sale_target[['vehicleType', 'modelYearID', 'EV_sale']]
EV_sale_target['EV_sale'] = EV_sale_target['EV_sale'].astype(float)
output_ev_frac_act = pd.merge(output_ev_frac_act, EV_sale_target,
                               on = ['vehicleType', 'modelYearID'],
                               how = 'left')

output_ev_frac_act.fillna(0, inplace = True)
output_ev_frac_act.loc[:, 'stmyFraction'] = \
    output_ev_frac_act.loc[:, ["stmyFraction", "EV_sale"]].max(axis=1)
    
output_ev_frac_act.drop(columns = ['EV_sale'], inplace = True)
output_ev_frac_act.loc[:, 'rules'] = 'ACC and ACT'
output_ev_frac.loc[:, 'rules'] = 'no ACC and ACT'

output_ev_frac_comb = pd.concat([output_ev_frac, output_ev_frac_act])


output_ev_frac_comb.rename(columns = {'stmyFraction': 'EV_fraction'}, inplace = True)
# <codecell>

# plot EV market penetration
sns.set_theme(style="whitegrid", font_scale=1.4)
ax = sns.relplot(data = output_ev_frac_comb, x= 'modelYearID', y = 'EV_fraction',
            hue = 'scenario', row='rules', col='vehicleType',  
            kind = 'line', palette='Spectral')
ax.set_titles("{row_name} | {col_name}")
ax.set_ylabels("EV fraction")
ax.set(ylim=(0, 1))
plt.savefig(os.path.join(path_to_moves, 'plot_forecast', 'synthfirm_EV_new_sale.png'), dpi = 300,
            bbox_inches = 'tight')
plt.show()

# <codecell>

# write baseline output --- with EV target
output_ev_frac_comb.to_csv(os.path.join(path_to_moves, 'turnover', 'synthfirm_ev_market.csv'),
                          index = False)


