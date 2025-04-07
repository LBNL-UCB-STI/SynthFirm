#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  5 10:59:11 2025

@author: xiaodanxu
"""

from pandas import read_csv
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings("ignore")

plt.style.use('ggplot')
sns.set(font_scale=1.2)  # larger font

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')


# load input

# list_of_matrix_file = ['penn_2018_7_75_50.csv']
path_to_moves = 'RawData/MOVES'
path_to_experian = 'PrivateData/registration/'
path_to_output = 'SynthFirm_parameters/fleet'

ev_fraction = read_csv(os.path.join(path_to_moves, 'turnover', 'synthfirm_ev_market.csv'))
ev_fraction.rename(columns = {'vehicleType':'veh_class'}, inplace = True)

age_distribution_private = \
    read_csv(os.path.join(path_to_moves, 'turnover', 'synthfirm_age_distribution_private.csv'))
    
age_distribution_hire = \
    read_csv(os.path.join(path_to_moves, 'turnover', 'synthfirm_age_distribution_hire.csv'))

age_distribution_lease = \
    read_csv(os.path.join(path_to_moves, 'turnover', 'synthfirm_age_distribution_lease.csv'))
    
icev_fuel_distribution = read_csv(os.path.join(path_to_experian, 
                                              'experian_national_fuel_distribution.csv'))

# <codecell>

# extend age distribution to include 2018-2020
begin_year = 2018
end_year = 2050
year_to_add = [2018, 2019, 2020]

for year in year_to_add:
    add_age_distribution_private = \
        age_distribution_private.loc[age_distribution_private['yearID'] == 2021]
    add_age_distribution_private.loc[:, 'yearID'] = year
    age_distribution_private = pd.concat([age_distribution_private, 
                                          add_age_distribution_private])
    
    add_age_distribution_hire = \
        age_distribution_hire.loc[age_distribution_hire['yearID'] == 2021]
    add_age_distribution_hire.loc[:, 'yearID'] = year
    age_distribution_hire = pd.concat([age_distribution_hire, 
                                          add_age_distribution_hire])
    
    add_age_distribution_lease = \
        age_distribution_lease.loc[age_distribution_lease['yearID'] == 2021]
    add_age_distribution_lease.loc[:, 'yearID'] = year
    age_distribution_lease = pd.concat([age_distribution_lease,
                                        add_age_distribution_lease])
    
# <codecell>

# extend EV fraction to include all model year
age_distribution_private.loc[:, 'modelYearID'] = \
    age_distribution_private.loc[:, 'yearID'] - \
        age_distribution_private.loc[:, 'ageID']
 
        
age_distribution_hire.loc[:, 'modelYearID'] = \
     age_distribution_hire.loc[:, 'yearID'] - \
         age_distribution_hire.loc[:, 'ageID']

age_distribution_lease.loc[:, 'modelYearID'] = \
    age_distribution_lease.loc[:, 'yearID'] - \
        age_distribution_lease.loc[:, 'ageID']

min_model_year = min(age_distribution_private.loc[:, 'modelYearID'].min(),
                     age_distribution_hire.loc[:, 'modelYearID'].min(),
                     age_distribution_lease.loc[:, 'modelYearID'].min())
start_model_year = ev_fraction.modelYearID.min()
year_to_fill = range(min_model_year, start_model_year)   

# print(list(year_to_fill))     
for year in year_to_fill:
    ev_fraction_add = ev_fraction.loc[ev_fraction['modelYearID'] == start_model_year]
    ev_fraction_add.loc[:, 'modelYearID'] = year
    ev_fraction_add.loc[:, 'EV_fraction'] = 0
    ev_fraction = pd.concat([ev_fraction, ev_fraction_add])

print(ev_fraction.modelYearID.min())
# <codecell>

# calculate EV fraction for each year

def EV_penetration_calculator(age_df, ev_df, loc_attr = None):
        
    df_out = pd.merge(age_df,
                      ev_df, on = ['veh_class', 'modelYearID'],
                      how = 'left')
    
    df_out['EV_fraction'].fillna(0, inplace = True)
    df_out.loc[:, 'EV_fraction'] = \
        df_out.loc[:, 'EV_fraction'] * \
            df_out.loc[:, 'ageFraction']
    
    grouping_var = ['yearID', 'veh_class', 'scenario', 'rules']
    if loc_attr is not None:
        grouping_var.append(loc_attr)
    df_out = \
        df_out.groupby(grouping_var)[['EV_fraction', 'population_by_year']].sum()
        
    df_out = df_out.reset_index()
    return(df_out)

age_fuel_distribution_private = EV_penetration_calculator(age_distribution_private,
                                                          ev_fraction, 'state_abbr')

age_fuel_distribution_hire = EV_penetration_calculator(age_distribution_hire,
                                                          ev_fraction)

age_fuel_distribution_lease = EV_penetration_calculator(age_distribution_lease,
                                                          ev_fraction)

# <codecell>

# plot EV penetration - private fleet

scenario_mapping = {
    "HOP_highp2": "High fuel, 0.6 * elec",
    "HOP_highp4": "High fuel, 0.8 * elec",
    "HOP_highp6": "High fuel, 1.0 * elec",
    "HOP_highp8": "High fuel, 1.2 * elec",
    "HOP_highp10": "High fuel, 1.4 * elec",
    "Ref_highp2": "Base fuel, 0.6 * elec",
    "Ref_highp4": "Base fuel, 0.8 * elec",
    "Ref_highp6": "Base fuel, 1.0 * elec",
    "Ref_highp8": "Base fuel, 1.2 * elec",
    "Ref_highp10": "Base fuel, 1.4 * elec"
}

age_fuel_distribution_private.loc[:, 'fuel scenario'] = \
    age_fuel_distribution_private.loc[:, 'scenario'].map(scenario_mapping)
    
age_fuel_distribution_hire.loc[:, 'fuel scenario'] = \
    age_fuel_distribution_hire.loc[:, 'scenario'].map(scenario_mapping)
    
age_fuel_distribution_lease.loc[:, 'fuel scenario'] = \
    age_fuel_distribution_lease.loc[:, 'scenario'].map(scenario_mapping)
    
hue_orders = ["High fuel, 0.6 * elec", "High fuel, 0.8 * elec", 
          "High fuel, 1.0 * elec", "High fuel, 1.2 * elec", 
          "High fuel, 1.4 * elec", "Base fuel, 0.6 * elec", 
          "Base fuel, 0.8 * elec", "Base fuel, 1.0 * elec", 
          "Base fuel, 1.2 * elec", "Base fuel, 1.4 * elec"]

sns.set_theme(style="whitegrid", font_scale=1.4)
state = 'CA'
plot_distribution_private = age_fuel_distribution_private.loc[age_fuel_distribution_private['state_abbr'] == state]
ax = sns.relplot(data = plot_distribution_private, x= 'yearID', y = 'EV_fraction',
            hue = 'fuel scenario', hue_order = hue_orders, row='rules', col='veh_class',  
            kind = 'line', palette='Spectral')
ax.set_titles("{row_name} | {col_name}")
ax.set_ylabels("EV fraction")
ax.set(ylim=(0, 1))
plt.savefig(os.path.join(path_to_moves, 'plot_forecast', 'synthfirm_EV_penetration_private_' + state + '.png'), dpi = 300,
            bbox_inches = 'tight')
plt.show()

# plot EV penetration - for hire and for lease

sns.set_theme(style="whitegrid", font_scale=1.4)
ax = sns.relplot(data = age_fuel_distribution_hire, x= 'yearID', y = 'EV_fraction',
            hue = 'fuel scenario', hue_order = hue_orders, row='rules', col='veh_class',  
            kind = 'line', palette='Spectral')
ax.set_titles("{row_name} | {col_name}")
ax.set_ylabels("EV fraction")
ax.set(ylim=(0, 1))
plt.savefig(os.path.join(path_to_moves, 'plot_forecast', 'synthfirm_EV_penetration_hire.png'), dpi = 300,
            bbox_inches = 'tight')
plt.show()

sns.set_theme(style="whitegrid", font_scale=1.4)
ax = sns.relplot(data = age_fuel_distribution_lease, x= 'yearID', y = 'EV_fraction',
            hue = 'fuel scenario', hue_order = hue_orders, row='rules', col='veh_class',  
            kind = 'line', palette='Spectral')
ax.set_titles("{row_name} | {col_name}")
ax.set_ylabels("EV fraction")
ax.set(ylim=(0, 1))
plt.savefig(os.path.join(path_to_moves, 'plot_forecast', 'synthfirm_EV_penetration_lease.png'), dpi = 300,
            bbox_inches = 'tight')
plt.show()


# <codecell>

# generate fraction of ICEV and assign diesel and gasoline fraction

def gen_fuel_mix(age_df, fuel_df, service_type, fuel_sel = ['Diesel', 'Gasoline'],
                 loc_attr = None):
    age_df.loc[:, 'ICEV_fraction'] = \
        1 - age_df.loc[:, 'EV_fraction']
    
    fuel_df = \
        fuel_df.loc[fuel_df['service_type']  == service_type]
    group_var = ['veh_class']
    idx_var = ['yearID', 'veh_class', 'scenario', 'fuel scenario', 'rules']
    
    if loc_attr is not None:
        group_var.append(loc_attr)
        idx_var.append(loc_attr)
        
    fuel_df = pd.pivot_table(fuel_df,
                             index = group_var,
                             columns = 'fuel_type',
                             values = 'vehicle_count',
                             aggfunc = 'sum')
    

    fuel_df = fuel_df[fuel_sel]
    fuel_df = fuel_df.reset_index()
    # calculate fuel fraction among ICEVs
    fuel_df.loc[:, fuel_sel] = \
        fuel_df.loc[:, fuel_sel].divide(fuel_df.loc[:, fuel_sel].sum(axis = 1), axis = 0)
    fuel_df.fillna(0, inplace = True)
    
    output_df = pd.merge(age_df,fuel_df,
                         on = group_var,
                         how = 'left')
    
    for fuel in fuel_sel:
        output_df.loc[:, fuel] = \
            output_df.loc[:, fuel] * \
                output_df.loc[:, 'ICEV_fraction']
    output_df.loc[:, 'Electric'] = \
        output_df.loc[:, 'EV_fraction']
    
    fuel_sel.append('Electric')
    
    output_df = pd.melt(output_df, 
                        id_vars = idx_var,
                        value_vars = fuel_sel,
                        var_name = 'fuel type', value_name='veh_fraction')
    output_df = output_df.reset_index()
    
    hdt_classes = ['Heavy-duty Tractor', 'Heavy-duty Vocational']
    
    comb_to_drop = (output_df.loc[:, 'fuel type'] == 'Gasoline') & \
        (output_df.loc[:, 'veh_class'].isin(hdt_classes))
    output_df = output_df.loc[~comb_to_drop]
    
    # rescale the fuel mix among remaining fuel
    output_df.loc[:, 'veh_fraction'] = \
        output_df.loc[:, 'veh_fraction'] / \
        output_df.groupby(idx_var)['veh_fraction'].transform('sum')

    return(output_df)

output_age_fuel_private = gen_fuel_mix(age_fuel_distribution_private, icev_fuel_distribution, 
                                       'PRIVATE', fuel_sel = ['Diesel', 'Gasoline'],
                                       loc_attr = 'state_abbr')

output_age_fuel_hire = gen_fuel_mix(age_fuel_distribution_hire, icev_fuel_distribution, 
                                    'FOR HIRE', fuel_sel = ['Diesel', 'Gasoline'])

output_age_fuel_lease = gen_fuel_mix(age_fuel_distribution_lease, icev_fuel_distribution, 
                                       'LEASE', fuel_sel = ['Diesel', 'Gasoline'])

# <codecell>

# writing output

private_stock = \
    age_distribution_private.groupby(['state_abbr', 'yearID', 'veh_class'])[['population_by_year']].sum()
private_stock = private_stock.reset_index()    
forhire_stock = \
        age_distribution_hire.groupby(['yearID', 'veh_class'])[['population_by_year']].sum()
forhire_stock = forhire_stock.reset_index()
forlease_stock = \
        age_distribution_lease.groupby(['yearID', 'veh_class'])[['population_by_year']].sum()
forlease_stock = forlease_stock.reset_index()

private_stock.to_csv(os.path.join(path_to_output, 'private_stock_projection.csv'), index = False)
forhire_stock.to_csv(os.path.join(path_to_output, 'hire_stock_projection.csv'), index = False)
forlease_stock.to_csv(os.path.join(path_to_output, 'lease_stock_projection.csv'), index = False)

output_age_fuel_private.to_csv(os.path.join(path_to_output, 'private_fuel_mix_scenario.csv'), index = False)
output_age_fuel_hire.to_csv(os.path.join(path_to_output, 'hire_fuel_mix_scenario.csv'), index = False)
output_age_fuel_lease.to_csv(os.path.join(path_to_output, 'lease_fuel_mix_scenario.csv'), index = False)