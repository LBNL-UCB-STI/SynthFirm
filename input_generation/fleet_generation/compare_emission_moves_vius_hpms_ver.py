#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  3 14:41:23 2024

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

# define constant
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
               'PM_2.5':'PM_{2.5}', 'PM_10':'PM_{10}'} 

list_of_pollutant = ['Energy use', 'CO_2e',
                    'CO', 'NO_x', 'NH_3', 'SO_2', 
                    'NO_2', 'VOC', 'PM_2.5', 'PM_10']

# hpms_veh_lookup = {62: 'combination truck',
#                    61: 'combination truck',
#                    52: 'single-unit truck',
#                    53: 'single-unit truck',
#                    32:  'light commercial truck'}

age_bin = [-1, 3, 5, 7, 9, 14, 19, 31]

age_bin_label = ['age<=3', '3<age<=5','5<age<=7', 
                 '7<age<=9', '9<age<=14', '14<age<=19', 'age>=20']

path_to_vius = 'RawData/US_VIUS_2021'
path_to_moves = 'RawData/MOVES'
analysis_year = 2021


# 1. Emission rate

moveser_dir = os.path.join(path_to_moves, 'Seattle_MOVES4_emission_rate_per_mile_2021.csv')
moveser_baseline = read_csv(moveser_dir)

pollutants = list(pollutant_lookup.keys())
print(pollutants)

moveser_baseline = \
moveser_baseline.loc[moveser_baseline['pollutantID'].isin(pollutants)]

moveser_baseline = moveser_baseline[['yearID', 'hourID','pollutantID', 
                                     'processID', 'sourceTypeID', 'fuelTypeID', 
                                     'modelYearID', 'roadTypeID', 'avgSpeedBinID', 'ratePerDistance']]
moveser_baseline.loc[:, 'pollutant'] = \
moveser_baseline.loc[:, 'pollutantID'].map(pollutant_lookup)
# 2. fleet data

# load MOVES data
MOVES_data_path = os.path.join(path_to_moves, 'MOVES_VMT_fraction_with_fuel_com_only.csv')
MOVES_fleet = read_csv(MOVES_data_path)
MOVES_fleet.loc[:, 'AgeBin'] = pd.cut(MOVES_fleet['ageID'],
                                      bins=age_bin, 
                                      right=True, labels=age_bin_label)
print(MOVES_fleet.columns)

# load vius data
vius_data_path = os.path.join(path_to_vius, 'VIUS_VMT_fraction_with_fuel_com_only.csv')
vius_fleet = read_csv(vius_data_path)
vius_fleet.loc[:, 'AgeBin'] = pd.cut(vius_fleet['ageID'],
                                      bins=age_bin, 
                                      right=True, labels=age_bin_label)
vius_fleet.head(5)

# 3. other distribution
road_type_distribution = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'road_type_distribution')
speed_distribution = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'speed_distribution')


# 4. load variable definition
source_type_definition = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'source_type_HPMS')

speed_bin_definition = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'speed_bin_definition')


# filter non-name attributes
speed_bin_definition = \
    speed_bin_definition[['avgSpeedBinID', 'avgBinSpeed', 'avgSpeedBinDesc']]
    
HPMS_definition = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'HPMS_definition')
vius_fleet = pd.merge(vius_fleet, source_type_definition,
                      on = 'sourceTypeID', how = 'left')

vius_fleet = pd.merge(vius_fleet, HPMS_definition,
                      on = 'HPMSVtypeID', how = 'left')

print(MOVES_fleet.HPMSVtypeName.unique())
print(vius_fleet.HPMSVtypeName.unique())

# <codecell>

# define stacked bar chart generator

def plot_clustered_stacked(dfall, size, labels, title, ylabelname, unit, H="/", **kwargs):
    """Given a list of dataframes, with identical columns and index, create a clustered stacked bar plot. 
labels is a list of the names of the dataframe, used for the legend
title is a string for the title of the plot
H is the hatch used for identification of the different dataframe"""
    plt.figure(figsize = size)
    n_df = len(dfall)
    n_col = len(dfall[0].columns) 
    n_ind = len(dfall[0].index)
    axe = plt.subplot(111)

    for df in dfall : # for each data frame
        axe = df.plot(kind="bar",
                      linewidth=0,
                      stacked=True,
                      ax=axe,
                      legend=False,
                      grid=False, cmap= 'plasma_r',
                      **kwargs)  # make bar plots

    h,l = axe.get_legend_handles_labels() # get the handles we want to modify
    for i in range(0, n_df * n_col, n_col): # len(h) = n_col * n_df
        for j, pa in enumerate(h[i:i+n_col]):
            for rect in pa.patches: # for each index
                rect.set_x(rect.get_x() + 1 / float(n_df + 1) * i / float(n_col))
                rect.set_hatch(H * int(i / n_col)) #edited part     
                rect.set_width(1 / float(n_df + 1))

    axe.set_xticks((np.arange(0, 2 * n_ind, 2) + 1 / float(n_df + 1)) / 2.)
    axe.set_xticklabels(df.index, rotation = 0)
    axe.set_title(rf"${title}$")

    # Add invisible data to add another legend
    n=[]        
    for i in range(n_df):
        n.append(axe.bar(0, 0, color="gray", hatch=H * i * 2))

    l1 = axe.legend(h[:n_col], l[:n_col], 
                    bbox_to_anchor=(0.99, 1), loc="upper left", fontsize = 7)
    if labels is not None:
        l2 = plt.legend(n, labels, loc=[1.01, 0.1]) 
    axe.add_artist(l1)
    plt.xticks(rotation = 60, ha = 'right')
    plt.ylabel(f'{ylabelname} ({unit})')
    plt.xlabel('')
    plt.tight_layout()
    
    return axe
# <codecell>
# prep speed distribution
# VMT by speed bin and road type for passenger vehicles
hourDayID = 85 # weekday hour = 8
selected_type = [32, 52, 53, 61, 62]

speed_distribution = \
speed_distribution.loc[speed_distribution['sourceTypeID'].isin(selected_type)]

speed_distribution = \
speed_distribution.loc[speed_distribution['hourDayID'] == hourDayID]

speed_distribution.head(5)

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

speed_distribution_by_st = pd.merge(speed_distribution_by_st, source_type_definition,
                                    on = 'sourceTypeID', how = 'left')

speed_distribution_by_st = pd.merge(speed_distribution_by_st, speed_bin_definition,
                                    on = 'avgSpeedBinID', how = 'left')

sns.barplot(speed_distribution_by_st, x = 'avgSpeedBinDesc', y = 'op_vmt_fraction',
            hue = 'sourceTypeName', palette = 'plasma_r')
plt.xticks(rotation = 60, ha = 'right')
plt.xlabel('Speed bin')
plt.ylabel('Fraction of VMT')
plt.savefig(os.path.join(path_to_moves, 'plot', 'speed_distribution_by_source_type.png'),
            dpi = 300, bbox_inches = 'tight')
plt.show()

# <codecell>
# redistribute VMT fraction among HPMS class
MOVES_fleet.loc[:, 'vmt_fraction'] = MOVES_fleet.loc[:, 'vmt_fraction']/ \
    MOVES_fleet.groupby('HPMSVtypeID')['vmt_fraction'].transform('sum')
print(MOVES_fleet.loc[:, 'vmt_fraction'].sum())

vius_fleet.loc[:, 'VMT_Fraction'] = vius_fleet.loc[:, 'VMT_Fraction']/ \
    vius_fleet.groupby('HPMSVtypeID')['VMT_Fraction'].transform('sum')
print(vius_fleet.loc[:, 'VMT_Fraction'].sum())

# <codecell>

print(len(MOVES_fleet))
print(len(vius_fleet))

# compute MOVES and VIUS emission per mile
var_to_match = ['yearID', 'sourceTypeID', 'fuelTypeID', 'modelYearID']

vius_fleet.loc[:, 'yearID'] = analysis_year
MOVES_fleet.loc[:, 'yearID'] = analysis_year

vius_fleet.loc[:, 'modelYearID'] = \
    vius_fleet['yearID'] - vius_fleet.loc[:, 'ageID'] 

# process MOVES emission rates for default fleet
VMT_fraction_combined_MOVES = pd.merge(MOVES_fleet, speed_road_distribution,
                                on = 'sourceTypeID', how = 'left')
print(len(VMT_fraction_combined_MOVES))
VMT_fraction_combined_MOVES.loc[:, 'vmt_fraction'] = \
VMT_fraction_combined_MOVES.loc[:, 'vmt_fraction'] * \
VMT_fraction_combined_MOVES.loc[:, 'op_vmt_fraction']

var_to_match = ['yearID', 'sourceTypeID', 'fuelTypeID', 'modelYearID', 
                'roadTypeID', 'avgSpeedBinID']
MOVES_VMT_fraction_with_er = pd.merge(VMT_fraction_combined_MOVES,
                                moveser_baseline,
                                on = var_to_match,
                                how = 'left')

MOVES_VMT_fraction_with_er.loc[:, 'emissions'] = \
MOVES_VMT_fraction_with_er.loc[:, 'ratePerDistance'] * \
MOVES_VMT_fraction_with_er.loc[:, 'vmt_fraction']

MOVES_VMT_fraction_with_er.loc[:, 'Source'] = 'MOVES'
# check composition by source type

# process MOVES emission rates for VIUS fleet
VMT_fraction_combined_VIUS = pd.merge(vius_fleet, speed_road_distribution,
                                on = 'sourceTypeID', how = 'left')
print(len(VMT_fraction_combined_VIUS))
VMT_fraction_combined_VIUS.loc[:, 'vmt_fraction'] = \
VMT_fraction_combined_VIUS.loc[:, 'VMT_Fraction'] * \
VMT_fraction_combined_VIUS.loc[:, 'op_vmt_fraction']

VIUS_VMT_fraction_with_er = pd.merge(VMT_fraction_combined_VIUS,
                                moveser_baseline,
                                on = var_to_match,
                                how = 'left')

VIUS_VMT_fraction_with_er.loc[:, 'emissions'] = \
VIUS_VMT_fraction_with_er.loc[:, 'ratePerDistance'] * \
VIUS_VMT_fraction_with_er.loc[:, 'vmt_fraction']


VIUS_VMT_fraction_with_er.loc[:, 'Source'] = 'VIUS'

# create a combined + cleaned dataset
output_attr = ['yearID', 'sourceTypeID', 'fuelTypeID', 'modelYearID', 
                'roadTypeID', 'avgSpeedBinID', 'Source', 'pollutant', 
                'sourceTypeName', 'AgeBin', 'HPMSVtypeID', 'HPMSVtypeName', 'emissions']

VIUS_emission = VIUS_VMT_fraction_with_er[output_attr]
MOVES_emission = MOVES_VMT_fraction_with_er[output_attr]

combined_emission = pd.concat([MOVES_emission, VIUS_emission])


# <codecell>

# plot emission by HPMS type and age
order_of_col = ['Light Duty Vehicles', 'Single Unit Trucks', 'Combination Trucks']
emission_by_hpms_type = pd.pivot_table(combined_emission, 
                                         index = ['Source', 'HPMSVtypeName', 'pollutant'], 
                                         columns = 'AgeBin',
                                         values= 'emissions', aggfunc = 'sum')
emission_by_hpms_type = emission_by_hpms_type[age_bin_label]
emission_by_hpms_type = emission_by_hpms_type.reset_index()


# define size for subplot
figsize = (3.5, 5)

for pol in list_of_pollutant:

    print(pol)
    # ax = plt.subplot(a, b, i)
    emission_to_plot = \
    emission_by_hpms_type.loc[emission_by_hpms_type['pollutant'] == pol]
    emission_to_plot = emission_to_plot.drop(columns = ['pollutant'])
    emission_to_plot = emission_to_plot.set_index(['Source'])
    df1 = emission_to_plot.loc['MOVES']
    df1 = df1.set_index('HPMSVtypeName')
    df1 = df1.loc[order_of_col]
    
    df2 = emission_to_plot.loc['VIUS']
    df2 = df2.set_index('HPMSVtypeName')
    df2 = df2.loc[order_of_col]
    pp = title_lookup[pol]
    unit = unit_lookup[pol]
    
    plot_clustered_stacked([df1, df2], figsize, ["MOVES", "VIUS"], pp, 'Rate', unit)
    plt.savefig(os.path.join(path_to_moves, 'plot', 'HPMS', 'emission_rate_by_hpms_age_' + pol + '.png'),
                dpi = 300, bbox_inches = 'tight')
    plt.show()

# <codecell>

# plot results by HPMS and fuel type

MOVES_fuel_lookup = {1: 'Gasoline', 
                    2: 'Diesel',
                    3: 'CNG',
                    5: 'Other',
                    9: 'Electricity'}

combined_emission.loc[:, 'fuelTypeName'] = \
    combined_emission.loc[:, 'fuelTypeID'].map(MOVES_fuel_lookup)
    
emission_by_fuel_type = pd.pivot_table(combined_emission, 
                                     index = ['Source', 'pollutant', 'HPMSVtypeName'], 
                                     columns = 'fuelTypeName',
                                     values= 'emissions', aggfunc = 'sum')
emission_by_fuel_type = emission_by_fuel_type.reset_index()


for pol in list_of_pollutant:
    # ax = plt.subplot(a, b, i)
    emission_to_plot = \
    emission_by_fuel_type.loc[emission_by_fuel_type['pollutant'] == pol]
    emission_to_plot = emission_to_plot.drop(columns = ['pollutant'])
    emission_to_plot = emission_to_plot.set_index(['Source'])
    df1 = emission_to_plot.loc['MOVES']
    df1 = df1.set_index('HPMSVtypeName')
    df1 = df1.loc[order_of_col]
    
    df2 = emission_to_plot.loc['VIUS']
    df2 = df2.set_index('HPMSVtypeName')
    df2 = df2.loc[order_of_col]
    pp = title_lookup[pol]
    unit = unit_lookup[pol]
    
    plot_clustered_stacked([df1, df2], figsize, ["MOVES", "VIUS"], pp, 'Rate', unit)
    plt.savefig(os.path.join(path_to_moves, 'plot', 'HPMS', 'emission_rate_by_hpms_fuel_' + pol + '.png'),
                dpi = 300, bbox_inches = 'tight')
    plt.show()
    # break

# <codecell>

# plot results by HPMS and speed bin 
# title_of_col_lookup = {'Light Duty Vehicles': 'Light\ Duty\ Vehicles', 
#                        'Single Unit Trucks': 'Single\ Unit\ Trucks', 
#                        'Combination Trucks': 'Combination\ Trucks'}
  
emission_by_speed_bin = pd.pivot_table(combined_emission, 
                                     index = ['Source', 'pollutant', 'HPMSVtypeName', 'avgSpeedBinID'], 
                                     columns = 'AgeBin',
                                     values= 'emissions', aggfunc = 'sum')
emission_by_speed_bin = emission_by_speed_bin[age_bin_label]
emission_by_speed_bin = emission_by_speed_bin.reset_index()
emission_by_speed_bin = pd.merge(emission_by_speed_bin,
                                 speed_bin_definition,
                                 on = 'avgSpeedBinID', how = 'left')
emission_by_speed_bin = \
emission_by_speed_bin.drop(columns = ['avgSpeedBinID', 'avgSpeedBinDesc'])

figsize = (5.5, 3.5)
for veh in order_of_col:
    emission_by_veh = \
        emission_by_speed_bin.loc[emission_by_speed_bin['HPMSVtypeName'] == veh]
    veh_name = veh.replace(' ', '\ ')
    for pol in list_of_pollutant:
        # ax = plt.subplot(a, b, i)
        emission_to_plot = \
        emission_by_veh.loc[emission_by_veh['pollutant'] == pol]
        emission_to_plot = \
            emission_to_plot.drop(columns = ['HPMSVtypeName','pollutant'])
        emission_to_plot = emission_to_plot.set_index(['Source'])
        df1 = emission_to_plot.loc['MOVES']
        df1 = df1.set_index('avgBinSpeed')
        # df1 = df1.loc[order_of_col]
        
        df2 = emission_to_plot.loc['VIUS']
        df2 = df2.set_index('avgBinSpeed')
        # df2 = df2.loc[order_of_col]
        
        pp = veh_name + ': ' + title_lookup[pol]
        unit = unit_lookup[pol]
        
        plot_clustered_stacked([df1, df2], figsize, ["MOVES", "VIUS"], pp, 'Rate', unit)
        plt.xlabel('Average bin speed (mph)')
        # plt.xticks(rotation = 0)
        plt.savefig(os.path.join(path_to_moves, 'plot', 'HPMS', 'emission_rate_by_speed_' + veh + '_' + pol + '.png'),
                    dpi = 300, bbox_inches = 'tight')
        plt.show()
    #     break
    # break

# <codecell>

# plot results by HPMS and hauling mode
hauling_mode_lookup = {'Single Unit Short-haul Truck': 'short-haul',
'Single Unit Long-haul Truck': 'long-haul', 
'Combination Short-haul Truck': 'short-haul', 
'Combination Long-haul Truck': 'long-haul'}
combined_emission_sel = combined_emission.loc[combined_emission['HPMSVtypeName'] != 'Light Duty Vehicles']
combined_emission_sel.loc[:, 'hauling_mode'] = \
    combined_emission_sel.loc[:, 'sourceTypeName'].map(hauling_mode_lookup)
    
emission_by_hauling_mode = pd.pivot_table(combined_emission_sel, 
                                     index = ['Source', 'pollutant', 'HPMSVtypeName'], 
                                     columns = 'hauling_mode',
                                     values= 'emissions', aggfunc = 'sum')
emission_by_hauling_mode = emission_by_hauling_mode.reset_index()

order_of_col_2 = ['Single Unit Trucks', 'Combination Trucks']
for pol in list_of_pollutant:
    # ax = plt.subplot(a, b, i)
    emission_to_plot = \
    emission_by_hauling_mode.loc[emission_by_hauling_mode['pollutant'] == pol]
    emission_to_plot = emission_to_plot.drop(columns = ['pollutant'])
    emission_to_plot = emission_to_plot.set_index(['Source'])
    df1 = emission_to_plot.loc['MOVES']
    df1 = df1.set_index('HPMSVtypeName')
    df1 = df1.loc[order_of_col_2]
    
    df2 = emission_to_plot.loc['VIUS']
    df2 = df2.set_index('HPMSVtypeName')
    df2 = df2.loc[order_of_col_2]
    pp = title_lookup[pol]
    unit = unit_lookup[pol]
    
    plot_clustered_stacked([df1, df2], figsize, ["MOVES", "VIUS"], pp, 'Rate', unit)
    plt.savefig(os.path.join(path_to_moves, 'plot', 'HPMS', 'emission_rate_by_hpms_haul_' + pol + '.png'),
                dpi = 300, bbox_inches = 'tight')
    plt.show()

# <codecell>
# redistribute VMT fraction among source type and calculate emission rate
MOVES_fleet.loc[:, 'vmt_fraction'] = MOVES_fleet.loc[:, 'vmt_fraction']/ \
    MOVES_fleet.groupby('sourceTypeID')['vmt_fraction'].transform('sum')
print(MOVES_fleet.loc[:, 'vmt_fraction'].sum())

vius_fleet.loc[:, 'VMT_Fraction'] = vius_fleet.loc[:, 'VMT_Fraction']/ \
    vius_fleet.groupby('sourceTypeID')['VMT_Fraction'].transform('sum')
print(vius_fleet.loc[:, 'VMT_Fraction'].sum())

# process MOVES emission rates for default fleet
VMT_fraction_combined_MOVES = pd.merge(MOVES_fleet, speed_road_distribution,
                                on = 'sourceTypeID', how = 'left')
print(len(VMT_fraction_combined_MOVES))
VMT_fraction_combined_MOVES.loc[:, 'vmt_fraction'] = \
VMT_fraction_combined_MOVES.loc[:, 'vmt_fraction'] * \
VMT_fraction_combined_MOVES.loc[:, 'op_vmt_fraction']

var_to_match = ['yearID', 'sourceTypeID', 'fuelTypeID', 'modelYearID', 
                'roadTypeID', 'avgSpeedBinID']
MOVES_VMT_fraction_with_er = pd.merge(VMT_fraction_combined_MOVES,
                                moveser_baseline,
                                on = var_to_match,
                                how = 'left')

MOVES_VMT_fraction_with_er.loc[:, 'emissions'] = \
MOVES_VMT_fraction_with_er.loc[:, 'ratePerDistance'] * \
MOVES_VMT_fraction_with_er.loc[:, 'vmt_fraction']

MOVES_VMT_fraction_with_er.loc[:, 'Source'] = 'MOVES'
# check composition by source type

# process MOVES emission rates for VIUS fleet
VMT_fraction_combined_VIUS = pd.merge(vius_fleet, speed_road_distribution,
                                on = 'sourceTypeID', how = 'left')
print(len(VMT_fraction_combined_VIUS))
VMT_fraction_combined_VIUS.loc[:, 'vmt_fraction'] = \
VMT_fraction_combined_VIUS.loc[:, 'VMT_Fraction'] * \
VMT_fraction_combined_VIUS.loc[:, 'op_vmt_fraction']

VIUS_VMT_fraction_with_er = pd.merge(VMT_fraction_combined_VIUS,
                                moveser_baseline,
                                on = var_to_match,
                                how = 'left')

VIUS_VMT_fraction_with_er.loc[:, 'emissions'] = \
VIUS_VMT_fraction_with_er.loc[:, 'ratePerDistance'] * \
VIUS_VMT_fraction_with_er.loc[:, 'vmt_fraction']


VIUS_VMT_fraction_with_er.loc[:, 'Source'] = 'VIUS'

# create a combined + cleaned dataset
output_attr = ['yearID', 'sourceTypeID', 'fuelTypeID', 'modelYearID', 
                'roadTypeID', 'avgSpeedBinID', 'Source', 'pollutant', 
                'sourceTypeName', 'AgeBin', 'HPMSVtypeID', 'HPMSVtypeName', 'emissions']

VIUS_emission = VIUS_VMT_fraction_with_er[output_attr]
MOVES_emission = MOVES_VMT_fraction_with_er[output_attr]

combined_emission = pd.concat([MOVES_emission, VIUS_emission])

# <codecell>

# plot emission by source type and age
order_of_col = ['Light Commercial Truck', 'Single Unit Short-haul Truck',
'Single Unit Long-haul Truck', 'Combination Short-haul Truck', 'Combination Long-haul Truck']
emission_by_stmy = pd.pivot_table(combined_emission,
                                  index = ['Source', 'sourceTypeName', 'pollutant'], 
                                  columns = 'AgeBin',
                                  values= 'emissions', aggfunc = 'sum')
emission_by_stmy = emission_by_stmy[age_bin_label]
emission_by_stmy = emission_by_stmy.reset_index()


# define size for subplot
figsize = (4, 5)

for pol in list_of_pollutant:

    print(pol)
    # ax = plt.subplot(a, b, i)
    emission_to_plot = \
    emission_by_stmy.loc[emission_by_stmy['pollutant'] == pol]
    emission_to_plot = emission_to_plot.drop(columns = ['pollutant'])
    emission_to_plot = emission_to_plot.set_index(['Source'])
    df1 = emission_to_plot.loc['MOVES']
    df1 = df1.set_index('sourceTypeName')
    df1 = df1.loc[order_of_col]
    
    df2 = emission_to_plot.loc['VIUS']
    df2 = df2.set_index('sourceTypeName')
    df2 = df2.loc[order_of_col]
    pp = title_lookup[pol]
    unit = unit_lookup[pol]
    
    plot_clustered_stacked([df1, df2], figsize, ["MOVES", "VIUS"], pp, 'Rate', unit)
    plt.savefig(os.path.join(path_to_moves, 'plot', 'SourceType', 'emission_rate_by_st_age_' + pol + '.png'),
                dpi = 300, bbox_inches = 'tight')
    plt.show()

# <codecell>

# plot results by source and fuel type

MOVES_fuel_lookup = {1: 'Gasoline', 
                    2: 'Diesel',
                    3: 'CNG',
                    5: 'Other',
                    9: 'Electricity'}

combined_emission.loc[:, 'fuelTypeName'] = \
    combined_emission.loc[:, 'fuelTypeID'].map(MOVES_fuel_lookup)
    
emission_by_fuel_type = pd.pivot_table(combined_emission, 
                                     index = ['Source', 'pollutant', 'sourceTypeName'], 
                                     columns = 'fuelTypeName',
                                     values= 'emissions', aggfunc = 'sum')
emission_by_fuel_type = emission_by_fuel_type.reset_index()


for pol in list_of_pollutant:
    # ax = plt.subplot(a, b, i)
    emission_to_plot = \
    emission_by_fuel_type.loc[emission_by_fuel_type['pollutant'] == pol]
    emission_to_plot = emission_to_plot.drop(columns = ['pollutant'])
    emission_to_plot = emission_to_plot.set_index(['Source'])
    df1 = emission_to_plot.loc['MOVES']
    df1 = df1.set_index('sourceTypeName')
    df1 = df1.loc[order_of_col]
    
    df2 = emission_to_plot.loc['VIUS']
    df2 = df2.set_index('sourceTypeName')
    df2 = df2.loc[order_of_col]
    pp = title_lookup[pol]
    unit = unit_lookup[pol]
    
    plot_clustered_stacked([df1, df2], figsize, ["MOVES", "VIUS"], pp, 'Rate', unit)
    plt.savefig(os.path.join(path_to_moves, 'plot', 'SourceType', 'emission_rate_by_st_fuel_' + pol + '.png'),
                dpi = 300, bbox_inches = 'tight')
    plt.show()
# <codecell>
# plot results by source type and speed bin 

  
emission_by_speed_bin = pd.pivot_table(combined_emission, 
                                     index = ['Source', 'pollutant', 'sourceTypeName', 'avgSpeedBinID'], 
                                     columns = 'AgeBin',
                                     values= 'emissions', aggfunc = 'sum')
emission_by_speed_bin = emission_by_speed_bin[age_bin_label]
emission_by_speed_bin = emission_by_speed_bin.reset_index()
emission_by_speed_bin = pd.merge(emission_by_speed_bin,
                                 speed_bin_definition,
                                 on = 'avgSpeedBinID', how = 'left')
emission_by_speed_bin = \
emission_by_speed_bin.drop(columns = ['avgSpeedBinID', 'avgSpeedBinDesc'])

figsize = (5.5, 3.5)
for veh in order_of_col:
    emission_by_veh = \
        emission_by_speed_bin.loc[emission_by_speed_bin['sourceTypeName'] == veh]
    veh_name = veh.replace(' ', '\ ')
    for pol in list_of_pollutant:
        # ax = plt.subplot(a, b, i)
        emission_to_plot = \
        emission_by_veh.loc[emission_by_veh['pollutant'] == pol]
        emission_to_plot = \
            emission_to_plot.drop(columns = ['sourceTypeName','pollutant'])
        emission_to_plot = emission_to_plot.set_index(['Source'])
        df1 = emission_to_plot.loc['MOVES']
        df1 = df1.set_index('avgBinSpeed')
        # df1 = df1.loc[order_of_col]
        
        df2 = emission_to_plot.loc['VIUS']
        df2 = df2.set_index('avgBinSpeed')
        # df2 = df2.loc[order_of_col]
        
        pp = veh_name + ': ' + title_lookup[pol]
        unit = unit_lookup[pol]
        
        plot_clustered_stacked([df1, df2], figsize, ["MOVES", "VIUS"], pp, 'Rate', unit)
        plt.xlabel('Average bin speed (mph)')
        # plt.xticks(rotation = 0)
        plt.savefig(os.path.join(path_to_moves, 'plot', 'SourceType', 'emission_rate_by_speed_' + veh + '_' + pol + '.png'),
                    dpi = 300, bbox_inches = 'tight')
        plt.show()

