#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 15 14:22:12 2024

@author: xiaodanxu
"""

import pandas as pd
from pandas import read_csv
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os

import warnings
warnings.filterwarnings("ignore")

plt.style.use('seaborn-v0_8-whitegrid')
sns.set(font_scale=1.4)  # larger font  
sns.set_style("whitegrid")

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')

path_to_moves = 'RawData/MOVES'

fuel_type_distribution = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'fuel_type_distribution')

fuel_type_definition = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'fuel_type_definition')

year_begin  = 2021
year_end = 2050
com_st = [32, 52, 53, 61, 62]

fuel_type_agg_frac = \
    fuel_type_distribution.groupby(['sourceTypeID', 'modelYearID', 'fuelTypeID', 'engTechID'])[['stmyFraction']].sum()
fuel_type_agg_frac = fuel_type_agg_frac.reset_index()  

fuel_type_baseline = fuel_type_agg_frac.loc[fuel_type_agg_frac['sourceTypeID'].isin(com_st)]
fuel_type_baseline = \
    fuel_type_baseline.loc[fuel_type_baseline['modelYearID'] <= year_end]
    
fuel_type_baseline = \
    fuel_type_baseline.groupby(['sourceTypeID', 'modelYearID', 'fuelTypeID'])[['stmyFraction']].sum()
fuel_type_baseline = fuel_type_baseline.reset_index()  

fuel_type_baseline.loc[:, 'avft_scenario'] = 'MOVES default'

fuel_type_definition = fuel_type_definition[['fuelTypeID', 'fuelTypeDesc']]
fuel_type_baseline = pd.merge(fuel_type_baseline,
                              fuel_type_definition,
                              on = 'fuelTypeID',
                              how = 'left')
# <codecell>

# step 1 -- compile AVFT scenarios


# select AVFT from two sets of data, for model year <= 2050 
ldt_fuel_type_agg_frac = \
    fuel_type_agg_frac.loc[(fuel_type_agg_frac['sourceTypeID'] == 32) & \
                            (fuel_type_agg_frac['modelYearID'] <= year_end)]
com_fuel_type_agg_frac = \
fuel_type_agg_frac.loc[(fuel_type_agg_frac['sourceTypeID'].isin(com_st)) & \
                       (fuel_type_agg_frac['modelYearID'] < year_begin)]



list_of_vmt_dist_file = ['vmt_fraction_moves_baseline.csv',
                         'vmt_fraction_vius_baseline.csv']
                         # 'vmt_fraction_moves_mandate.csv',
                         # 'vmt_fraction_vius_mandate.csv']

list_of_avft_file = ['TDA_AVFT_HOP_highp2.csv', 'TDA_AVFT_HOP_highp6.csv', 'TDA_AVFT_HOP_highp10.csv',
                     'TDA_AVFT_Ref_highp2.csv', 'TDA_AVFT_Ref_highp6.csv', 'TDA_AVFT_Ref_highp10.csv']

scenario_lookup = {'TDA_AVFT_HOP_highp2.csv': 'TDA high oil, low elec', 
                   'TDA_AVFT_HOP_highp6.csv': 'TDA high oil, mid elec',
                   'TDA_AVFT_HOP_highp10.csv': 'TDA high oil, high elec',
                     'TDA_AVFT_Ref_highp2.csv': 'TDA low oil, low elec', 
                     'TDA_AVFT_Ref_highp6.csv': 'TDA low oil, mid elec',
                     'TDA_AVFT_Ref_highp10.csv': 'TDA low oil, high elec'}

vmt_scenario_lookup = {'vmt_fraction_moves_baseline.csv': 'MOVES baseline',
                         'vmt_fraction_vius_baseline.csv': 'VIUS baseline'}
                         # 'vmt_fraction_moves_mandate.csv': 'MOVES retiring old trucks',
                         # 'vmt_fraction_vius_mandate.csv': 'VIUS retiring old trucks'}

# <codecell>
fuel_mix_combined = fuel_type_baseline
ldt_avft_2 = \
    ldt_fuel_type_agg_frac.groupby(['sourceTypeID', 'modelYearID', 'fuelTypeID'])[['stmyFraction']].sum()
ldt_avft_2 = ldt_avft_2.reset_index()
# compile TDA results
for fuel_file in list_of_avft_file:
    
    scenario_name = scenario_lookup[fuel_file]
    print(scenario_name)
    mhd_avft = read_csv(os.path.join(path_to_moves, 'turnover', fuel_file))
    mhd_avft.drop(columns = ['Unnamed: 0', 'HPMSVtypeName', 'sourceTypeName'], inplace = True)
    ldt_avft = mhd_avft.loc[mhd_avft['sourceTypeID'] == 52]
    ldt_avft.loc[:, 'sourceTypeID'] = 32
    com_avft = pd.concat([ldt_avft, mhd_avft, com_fuel_type_agg_frac])
    
    com_avft = \
        com_avft.groupby(['sourceTypeID', 'modelYearID', 'fuelTypeID'])[['stmyFraction']].sum()
    com_avft = com_avft.reset_index()    
    ldt_avft = com_avft.loc[com_avft['sourceTypeID'] == 32]
    # ldt_avft.loc[:, 'sourceTypeID'] = 32
    ldt_avft = pd.merge(ldt_avft_2, ldt_avft, 
                        on = ['sourceTypeID', 'modelYearID', 'fuelTypeID'], how = 'left')
    # ldt_avft.fillna(0)
    ldt_avft.loc[:, 'stmyFraction'] = ldt_avft.loc[:, 'stmyFraction_x']
    # adj EV projection
    ev_idx = (ldt_avft['fuelTypeID'] == 9)
    ldt_avft.loc[ev_idx, 'stmyFraction'] = \
    ldt_avft.loc[ev_idx, ["stmyFraction_x", "stmyFraction_y"]].max(axis=1)
    ldt_avft = ldt_avft[['sourceTypeID', 'modelYearID', 'fuelTypeID', 'stmyFraction']]
    ldt_avft_no_adj = ldt_avft.loc[ev_idx]
    ldt_avft_total = ldt_avft_no_adj.drop(columns = ['fuelTypeID'])
    ldt_avft_total.loc[:, 'total'] = 1- ldt_avft_total.loc[:, 'stmyFraction']
    ldt_avft_total = ldt_avft_total[['sourceTypeID', 'modelYearID', 'total']]
    ldt_avft_adj = ldt_avft.loc[~ev_idx]
    ldt_avft_adj = pd.merge(ldt_avft_adj, ldt_avft_total, 
                            on = ['sourceTypeID', 'modelYearID'], how = 'left')
    ldt_avft_adj.loc[:, 'stmyFraction'] = \
        ldt_avft_adj.loc[:, 'stmyFraction']/ \
            ldt_avft_adj.groupby(['sourceTypeID', 'modelYearID'])['stmyFraction'].transform('sum') * \
            ldt_avft_adj.loc[:, 'total']
    ldt_avft_adj = ldt_avft_adj[['sourceTypeID', 'modelYearID', 'fuelTypeID', 'stmyFraction']]
    ldt_avft = pd.concat([ldt_avft_no_adj, ldt_avft_adj])
    com_avft = com_avft.loc[com_avft['sourceTypeID'] != 32]
    com_avft = pd.concat([ ldt_avft, com_avft])
    com_avft.loc[:, 'avft_scenario'] = scenario_name
    # fuel_type_definition = fuel_type_definition[['fuelTypeID', 'fuelTypeDesc']]
    com_avft = pd.merge(com_avft,
                        fuel_type_definition,
                        on = 'fuelTypeID',
                        how = 'left')
    fuel_mix_combined = pd.concat([fuel_mix_combined, com_avft])

    # break

fuel_mix_combined.to_csv(os.path.join(path_to_moves, 'turnover', 'avft_by_scenario.csv'))
print(fuel_mix_combined.avft_scenario.unique())

# <codecell>
color_order = ['MOVES default', 'TDA high oil, low elec',
      'TDA high oil, mid elec',  'TDA high oil, high elec',
      'TDA low oil, low elec',
       'TDA low oil, mid elec', 'TDA low oil, high elec']
# plot avft
fuel_mix_combined_to_plot = fuel_mix_combined.loc[fuel_mix_combined['fuelTypeID'] == 9]
fuel_mix_combined_to_plot = \
    fuel_mix_combined_to_plot.loc[fuel_mix_combined_to_plot['modelYearID'] >= 2021]
    
ax = sns.relplot(fuel_mix_combined_to_plot, x = 'modelYearID', y = 'stmyFraction',
            hue = 'avft_scenario', style = 'avft_scenario', 
            col = 'sourceTypeID', col_wrap = 3,
            kind="line", palette = 'rainbow_r', hue_order = color_order,
            linewidth = 2, height=4.5, aspect=1.3)

# ax.set_titles('{col_name}', fontsize = 10)
ax.set_ylabels('VMT fraction')
# <codecell>

# produce VMT distribution by fuel type

vmt_distribution_combined = None
col_to_keep = [ 'sourceTypeID', 'sourceTypeName','HPMSVtypeID',  'HPMSVtypeName', 
               'yearID', 'ageID', 'modelYearID',
               'sourceTypePopulation', 'population_by_year',
               'ageFraction', 'vmt_fraction', 'scenario']
for vmt_file in list_of_vmt_dist_file:
    vmt_scenario = vmt_scenario_lookup[vmt_file]
    print(vmt_scenario)
    vmt_distribution = read_csv(os.path.join(path_to_moves, 'turnover', vmt_file))
    vmt_distribution = vmt_distribution.loc[vmt_distribution['sourceTypeID'].isin(com_st)]
    vmt_distribution.loc[:, 'scenario'] = vmt_scenario
    vmt_distribution.loc[:, 'modelYearID'] = vmt_distribution.loc[:, 'yearID'] - \
        vmt_distribution.loc[:, 'ageID']
    vmt_distribution = vmt_distribution[col_to_keep]
    print(vmt_distribution.vmt_fraction.sum())
    vmt_distribution_combined = pd.concat([vmt_distribution_combined,
                                           vmt_distribution])
    # break
vmt_distribution_combined = \
    vmt_distribution_combined.loc[vmt_distribution_combined['yearID'] <= year_end]
    
# <codecell>

print(vmt_distribution_combined.vmt_fraction.sum())

vmt_distribution_with_fuel = pd.merge(vmt_distribution_combined,
                                      fuel_mix_combined,
                                      on = ['sourceTypeID', 'modelYearID'],
                                      how = 'left')
# rescale fuel mix 

group_var = ['scenario', 'avft_scenario', 'yearID', 'sourceTypeID', 'modelYearID']
vmt_distribution_with_fuel.loc[:, 'stmyFraction'] = \
    vmt_distribution_with_fuel.loc[:, 'stmyFraction']/ \
    vmt_distribution_with_fuel.groupby(group_var)['stmyFraction'].transform('sum')

vmt_distribution_with_fuel.loc[:, 'vmt_fraction'] = \
    vmt_distribution_with_fuel.loc[:, 'vmt_fraction'] * vmt_distribution_with_fuel.loc[:, 'stmyFraction']

vmt_distribution_with_fuel.loc[:, 'ageFraction'] = \
    vmt_distribution_with_fuel.loc[:, 'ageFraction'] * vmt_distribution_with_fuel.loc[:, 'stmyFraction']
    
vmt_distribution_with_fuel.to_csv(os.path.join(path_to_moves, 'turnover', 'vmt_distribution_by_scenario.csv'), 
                                  index = False)
print(len(vmt_distribution_with_fuel))
# should be 840
print(vmt_distribution_with_fuel.loc[:, 'vmt_fraction'].sum())

# checking results -> each row should be 30 --> 30 forecast years
fraction_check = vmt_distribution_with_fuel.groupby(['scenario', 'avft_scenario'])['vmt_fraction'].sum()

vmt_distribution_to_check = \
    vmt_distribution_with_fuel.loc[(vmt_distribution_with_fuel['scenario'] == 'MOVES baseline') & \
        (vmt_distribution_with_fuel['avft_scenario'] == 'MOVES baseline')]
# print(vmt_distribution_with_fuel.loc[:, 'ageFraction'].sum())

# <codecell>

# plot results -- VMT by fuel type and HPMS type
fuel_group_var = ['HPMSVtypeID', 'HPMSVtypeName',
                  'yearID', 'scenario', 'avft_scenario','fuelTypeID','fuelTypeDesc']
fuel_mix_by_year_scenario = \
    vmt_distribution_with_fuel.groupby(fuel_group_var)[['vmt_fraction']].sum()
fuel_mix_by_year_scenario = fuel_mix_by_year_scenario.reset_index()

fuel_mix_by_year_scenario.loc[:, 'vmt_fraction'] =\
    fuel_mix_by_year_scenario.loc[:, 'vmt_fraction']/ \
        fuel_mix_by_year_scenario.groupby(['yearID', 'scenario', 'avft_scenario', 'HPMSVtypeID'])['vmt_fraction'].transform('sum')
        
print(fuel_mix_by_year_scenario.loc[:, 'vmt_fraction'].sum())

# plot line 
fuel_mix_by_year_to_plot = \
    fuel_mix_by_year_scenario.loc[fuel_mix_by_year_scenario['fuelTypeID'] == 9]

# to_drop = ['MOVES retiring old trucks', 'VIUS retiring old trucks']
# fuel_mix_by_year_to_plot = \
#     fuel_mix_by_year_to_plot[~fuel_mix_by_year_to_plot['scenario'].isin(to_drop)]


  
color_order = ['MOVES default', 'TDA high oil, low elec',
      'TDA high oil, mid elec',  'TDA high oil, high elec',
      'TDA low oil, low elec',
       'TDA low oil, mid elec', 'TDA low oil, high elec']
ax = sns.relplot(fuel_mix_by_year_to_plot, x = 'yearID', y = 'vmt_fraction',
            hue = 'avft_scenario', style = 'avft_scenario', col = 'HPMSVtypeName', row = 'scenario',
            kind="line", palette = 'rainbow_r', hue_order = color_order,
            linewidth = 2, height=4.5, aspect=1.3)

ax.set_titles('{row_name}' ' | ' '{col_name}', fontsize = 10)
ax.set_ylabels('VMT fraction (%)')
from matplotlib.ticker import FuncFormatter
def to_percent(y, position):
    s = str(round(100*y, 1))
    return s + '%'
formatter = FuncFormatter(to_percent)
plt.gca().yaxis.set_major_formatter(formatter)

plt.savefig(os.path.join(path_to_moves, 'plot_forecast', \
                         'electrification_fraction_by_scenario.png'), dpi = 300)
plt.show()


# <codecell>

def weighted_median_by_count(df):
    df_sorted = df.sort_values('ageID')
    cumsum = df_sorted['ageFraction'].cumsum()
    cutoff = df_sorted['ageFraction'].sum() / 2.
    median = df_sorted[cumsum >= cutoff]['ageID'].iloc[0]
    return(median)

def weighted_median_by_vmt(df):
    df_sorted = df.sort_values('ageID')
    cumsum = df_sorted['vmt_fraction'].cumsum()
    cutoff = df_sorted['vmt_fraction'].sum() / 2.
    median = df_sorted[cumsum >= cutoff]['ageID'].iloc[0]
    return(median)

age_group_var = ['HPMSVtypeID', 'HPMSVtypeName',
                  'yearID', 'scenario', 'avft_scenario']

vmt_distribution_baseline_fuel = \
    vmt_distribution_with_fuel.loc[vmt_distribution_with_fuel['avft_scenario'] == 'MOVES default']


# dropping the two mandate scenario as VMT results are weird
# to_drop = ['MOVES retiring old trucks', 'VIUS retiring old trucks']
# vmt_distribution_baseline_fuel = \
#     vmt_distribution_baseline_fuel[~vmt_distribution_baseline_fuel['scenario'].isin(to_drop)]

median_age_by_count = vmt_distribution_baseline_fuel.groupby(age_group_var).apply(weighted_median_by_count)
median_age_by_count = median_age_by_count.reset_index()
median_age_by_count.rename(columns = {0: 'median age'}, inplace = True)

median_age_by_vmt = vmt_distribution_baseline_fuel.groupby(age_group_var).apply(weighted_median_by_vmt)
median_age_by_vmt = median_age_by_vmt.reset_index()
median_age_by_vmt.rename(columns = {0: 'median age'}, inplace = True)

# <codecell>
ax = sns.relplot(median_age_by_count, x = 'yearID', y = 'median age',
            hue = 'scenario', col = 'HPMSVtypeName', style = 'scenario',
            kind="line", linewidth=2)
ax.set_titles('{col_name}', fontsize = 10)
plt.savefig(os.path.join(path_to_moves, 'plot_forecast', \
                         'median_age_count_by_scenario.png'), dpi = 300)
plt.show()

ax = sns.relplot(median_age_by_vmt, x = 'yearID', y = 'median age',
            hue = 'scenario', col = 'HPMSVtypeName', style = 'scenario', 
            kind="line", linewidth=2)
ax.set_titles('{col_name}', fontsize = 10)
plt.savefig(os.path.join(path_to_moves, 'plot_forecast', \
                         'median_age_VMT_by_scenario.png'), dpi = 300)
plt.show()