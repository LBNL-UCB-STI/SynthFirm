#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 26 10:42:57 2024

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
os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')
plt.style.use('seaborn-v0_8-whitegrid')
sns.set(font_scale=1.4)  # larger font  
sns.set_style("whitegrid")

analysis_years = [2021, 2030, 2040, 2050]

pollutant_lookup = {91: 'Energy use', 98: 'CO_2e',
                    2: 'CO', 3: 'NO_x', 30: 'NH_3', 31:'SO_2', 
                    33: 'NO_2', 87: 'VOC', 
                    110: 'PM_2.5', 116: 'PM_2.5', 117: 'PM_2.5', 
                    100: 'PM_10',  106: 'PM_10', 107: 'PM_10'} # Brake and tire wear

unit_lookup = {'CO':'gram/mile', 'NO_x':'gram/mile', 
               'NH_3':'gram/mile', 'SO_2':'gram/mile', 
               'NO_2':'gram/mile','VOC':'gram/mile', 
               'Energy use':'kJ/mile', 'CO_2e':'gram/mile',
               'PM_2.5':'gram/mile', 'PM_10':'gram/mile'} 

title_lookup = {'CO':'CO', 'NO_x':'NO_x', 
               'NH_3':'NH_3', 'SO_2':'SO_2', 
               'NO_2':'NO_2','VOC':'VOC', 
               'Energy use':'Energy\ use', 'CO_2e':'CO_2e',
               'PM_2.5':'PM_{{2.5}}', 'PM_10':'PM_{10}'} 

list_of_pollutant = ['Energy use', 'CO_2e',
                    'CO', 'NO_x', 'NH_3', 'SO_2', 
                    'NO_2', 'VOC', 'PM_2.5', 'PM_10']

age_bin = [-1, 3, 5, 7, 9, 14, 19, 31]

age_bin_label = ['age<=3', '3<age<=5','5<age<=7', 
                 '7<age<=9', '9<age<=14', '14<age<=19', 'age>=20']


path_to_vius = 'RawData/US_VIUS_2021'
path_to_moves = 'RawData/MOVES'

# load VMT distribution
vmt_distribution = read_csv(os.path.join(path_to_moves, 'turnover', 
                                         'vmt_distribution_by_scenario.csv'))
to_drop = ['MOVES retiring old trucks', 'VIUS retiring old trucks']
vmt_distribution = \
    vmt_distribution[~vmt_distribution['scenario'].isin(to_drop)]
    
vmt_distribution.loc[:, 'AgeBin'] = pd.cut(vmt_distribution['ageID'],
                                      bins=age_bin, 
                                      right=True, labels=age_bin_label)

vmt_distribution = vmt_distribution.loc[vmt_distribution['yearID'].isin(analysis_years)]

# <codecell>

# prepare speed distribution

road_type_distribution = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'road_type_distribution')
speed_distribution = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'speed_distribution')
hourDayID = 85 # weekday hour = 8
selected_type = [32, 52, 53, 61, 62]

speed_distribution = \
speed_distribution.loc[speed_distribution['sourceTypeID'].isin(selected_type)]

speed_distribution = \
speed_distribution.loc[speed_distribution['hourDayID'] == hourDayID]

road_type_distribution = \
road_type_distribution.loc[road_type_distribution['roadTypeID'] != 1] 
#drop off-network

speed_road_distribution = pd.merge(speed_distribution,
                                  road_type_distribution,
                                  on = ['sourceTypeID', 'roadTypeID'],
                                  how = 'left')
speed_road_distribution.loc[:, 'op_vmt_fraction'] = \
speed_road_distribution.loc[:, 'avgSpeedFraction'] * \
speed_road_distribution.loc[:, 'roadTypeVMTFraction']

print(len(speed_road_distribution)) # should be 320
print(speed_road_distribution['op_vmt_fraction'].sum()) # should be 5 (source type)
speed_road_distribution = \
speed_road_distribution.drop(columns = ['hourDayID', 'avgSpeedFraction', 'roadTypeVMTFraction'])
speed_road_distribution.head(5)

# plot speed distribution

speed_distribution_by_st = \
    speed_road_distribution.groupby(['sourceTypeID', 'avgSpeedBinID'])[['op_vmt_fraction']].sum()

speed_distribution_by_st = speed_distribution_by_st.reset_index()


# <codecell>

# load emission rate

combined_moveser = None
pollutants = list(pollutant_lookup.keys())
for year in analysis_years:
    print(year)
    er_file = 'Seattle_MOVES4_emission_rate_per_mile_' + str(year) + '.csv'
    moveser_dir = os.path.join(path_to_moves, er_file)
    moveser = read_csv(moveser_dir)


    moveser = \
    moveser.loc[moveser['pollutantID'].isin(pollutants)]

    moveser = moveser[['yearID', 'hourID','pollutantID', 
                       'processID', 'sourceTypeID', 'fuelTypeID', 
                       'modelYearID', 'roadTypeID', 
                       'avgSpeedBinID', 'ratePerDistance']]
    moveser.loc[:, 'pollutant'] = \
    moveser.loc[:, 'pollutantID'].map(pollutant_lookup)
    print(len(moveser))
    combined_moveser = pd.concat([combined_moveser, moveser])
    # break

# <codecell>

# redistribute VMT fraction among HPMS class

group_var = ['HPMSVtypeID', 'HPMSVtypeName',
                  'yearID', 'scenario', 'avft_scenario']
# redistribute VMT fraction among HPMS class
vmt_distribution.loc[:, 'vmt_fraction'] = vmt_distribution.loc[:, 'vmt_fraction']/ \
    vmt_distribution.groupby(group_var)['vmt_fraction'].transform('sum')
print(vmt_distribution.loc[:, 'vmt_fraction'].sum())

# <codecell>

# assign emission rates
var_to_match = ['yearID', 'sourceTypeID', 'fuelTypeID', 'modelYearID']


# process MOVES emission rates for default fleet
VMT_fraction_with_op = pd.merge(vmt_distribution, speed_road_distribution,
                                on = 'sourceTypeID', how = 'left')
print(len(VMT_fraction_with_op))
VMT_fraction_with_op.loc[:, 'vmt_fraction'] = \
VMT_fraction_with_op.loc[:, 'vmt_fraction'] * \
VMT_fraction_with_op.loc[:, 'op_vmt_fraction']

var_to_match = ['yearID', 'sourceTypeID', 'fuelTypeID', 'modelYearID', 
                'roadTypeID', 'avgSpeedBinID']

VMT_fraction_with_er = pd.merge(VMT_fraction_with_op,
                                combined_moveser,
                                on = var_to_match,
                                how = 'left')

print(len(VMT_fraction_with_er))


VMT_fraction_with_er.loc[:, 'emissions'] = \
VMT_fraction_with_er.loc[:, 'ratePerDistance'] * \
VMT_fraction_with_er.loc[:, 'vmt_fraction']


# <codecell>

# emission rate per mile by scenario
er_result_checking = \
    VMT_fraction_with_er.loc[(VMT_fraction_with_er['scenario'] == 'MOVES baseline')]
                             # (VMT_fraction_with_er['avft_scenario'] == 'MOVES default')]
er_result_checking = er_result_checking.loc[er_result_checking['yearID'] == 2050]
er_result_checking = er_result_checking.loc[er_result_checking['pollutantID'] == 3]
er_result_checking = er_result_checking.loc[er_result_checking['processID'] == 1]
er_result_checking = er_result_checking.loc[er_result_checking['HPMSVtypeID'] == 25]

group_var = ['yearID', 'avft_scenario', 'HPMSVtypeID', 'HPMSVtypeName', 
             'pollutantID', 'processID', 'pollutant']
er_result_by_pol = \
    er_result_checking.groupby(group_var)[['emissions','vmt_fraction']].sum()

# results seems okay, MOVES doesn't produce emission rates for EV X pollutants, 
# which is fine and no need to scale VMT

# <codecell>

group_var = ['scenario', 'avft_scenario', 'yearID', 
             'HPMSVtypeID', 'HPMSVtypeName', 'pollutant']

emission_rate_by_scenario = VMT_fraction_with_er.groupby(group_var)[['emissions']].sum()
emission_rate_by_scenario = emission_rate_by_scenario.reset_index()

# <codecell>

# plot emission rate by scenario
pol_to_plot = ['Energy use', 'CO_2e','NO_x', 'PM_2.5']
emission_rate_to_plot = \
    emission_rate_by_scenario.loc[emission_rate_by_scenario['pollutant'].isin(pol_to_plot)]
# emission_rate_to_plot = \
# emission_rate_to_plot.loc[emission_rate_to_plot['yearID']>=2030]    
color_order = ['MOVES default', 'TITAN high oil, low elec',
      'TITAN high oil, mid elec',  'TITAN high oil, high elec',
      'TITAN low oil, low elec',
       'TITAN low oil, mid elec', 'TITAN low oil, high elec']

for pol in pol_to_plot:  
    emission_rate_sel = \
        emission_rate_to_plot.loc[emission_rate_to_plot['pollutant']== pol]
    ax = sns.relplot(emission_rate_sel, x = 'yearID', y = 'emissions',
                hue = 'avft_scenario', style = 'scenario', 
                col = 'HPMSVtypeName', 
                kind="line", palette = 'rainbow_r', hue_order = color_order,
                linewidth = 1,
                facet_kws={'sharey': False})
    
    pol_label = title_lookup[pol]
    ax.set_titles(rf'${pol_label}$' ' | '  '{col_name}', fontsize = 10)
    # ax.set(yscale="log")
    unit = unit_lookup[pol]
    ax.set_ylabels('Emission rate (' + unit + ')')
    # plt.ylim(0)
    
    plt.savefig(os.path.join(path_to_moves, 'plot_forecast', \
                             'HPMS/emission_rate_by_scenario_' + pol + '.png'), 
                bbox_inches='tight', dpi = 300)
    plt.show()
# <codecell>
sns.set(font_scale=1.4)  # larger font 
sns.set_style("whitegrid")  
# plot results for 2050 only  
for pol in pol_to_plot:  
    emission_rate_sel = \
        emission_rate_to_plot.loc[emission_rate_to_plot['pollutant']== pol]
    emission_rate_sel = emission_rate_sel.loc[emission_rate_sel['yearID'] == 2050]
    # emission_rate_sel = emission_rate_sel.loc[emission_rate_sel['HPMSVtypeID'] > 25]
    ax = sns.catplot(emission_rate_sel, x = 'scenario', y = 'emissions',
                hue = 'avft_scenario', col = 'HPMSVtypeName', 
                kind="bar", palette = 'rainbow_r', hue_order = color_order,
                height=4, aspect=1.05)
                # facet_kws={'sharey': False})
    
    pol_label = title_lookup[pol]
    ax.set_titles(rf'${pol_label}$' ' | '  '{col_name}', fontsize = 10)
    # ax.set(yscale="log")
    unit = unit_lookup[pol]
    ax.set_ylabels('Emission rate (' + unit + ')')
    # plt.ylim(0)
    
    plt.savefig(os.path.join(path_to_moves, 'plot_forecast', \
                             'HPMS/emission_rate_2050_' + pol + '.png'), 
                bbox_inches='tight', dpi = 300)
    plt.show()
    
# <codecell>
from matplotlib.ticker import FuncFormatter
def to_percent(y, position):
    s = str(round(100*y, 1))
    return s + '%'  

# plot % change from baseline for 2050 only  
sns.set(font_scale=1.4)  # larger font 
sns.set_style("whitegrid")  
for pol in pol_to_plot:  
    emission_rate_sel = \
        emission_rate_to_plot.loc[emission_rate_to_plot['pollutant']== pol]
    emission_rate_sel = emission_rate_sel.loc[emission_rate_sel['yearID'] == 2050]
    # emission_rate_sel = emission_rate_sel.loc[emission_rate_sel['HPMSVtypeID'] > 25]
    baseline_rate = \
        emission_rate_sel.loc[emission_rate_sel['avft_scenario'] == 'MOVES default']
    baseline_rate.drop(columns = ['yearID', 'avft_scenario', 'HPMSVtypeName'], inplace = True)
    baseline_rate.rename(columns = {'emissions': 'base_emissions'}, inplace = True)
    print(baseline_rate.columns)
    emission_rate_sel = pd.merge(emission_rate_sel, baseline_rate,
                                 on = ['scenario','HPMSVtypeID', 'pollutant'],
                                 how = 'left')
    emission_rate_sel.loc[:, 'change'] = \
        emission_rate_sel.loc[:, 'emissions']/ \
            emission_rate_sel.loc[:, 'base_emissions'] - 1
    ax = sns.catplot(emission_rate_sel, x = 'scenario', y = 'change',
                hue = 'avft_scenario', col = 'HPMSVtypeName', 
                kind="bar", palette = 'rainbow_r', hue_order = color_order,
                height=4, aspect=1.05)
                # facet_kws={'sharey': False})
    
    pol_label = title_lookup[pol]
    ax.set_titles(rf'${pol_label}$' ' | '  '{col_name}', fontsize = 10)
    # ax.set(yscale="log")
    # unit = unit_lookup[pol]
    ax.set_ylabels('Percent change (%)')
    # plt.ylim([0,1])

    formatter = FuncFormatter(to_percent)
    plt.gca().yaxis.set_major_formatter(formatter)
    plt.savefig(os.path.join(path_to_moves, 'plot_forecast', \
                              'HPMS/emission_change_2050_' + pol + '.png'), 
                bbox_inches='tight', dpi = 300)
    plt.show()
    
    # break