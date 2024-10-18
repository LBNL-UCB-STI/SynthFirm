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

MOVES_fuel_lookup = {1: 'Gasoline', 
                    2: 'Diesel',
                    3: 'CNG',
                    5: 'Other',
                    9: 'Electricity'}

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

road_type_definition = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'road_type_definition')


# filter non-name attributes
speed_bin_definition = \
    speed_bin_definition[['avgSpeedBinID', 'avgBinSpeed']]
    
HPMS_definition = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'HPMS_definition')
vius_fleet = pd.merge(vius_fleet, source_type_definition,
                      on = 'sourceTypeID', how = 'left')

vius_fleet = pd.merge(vius_fleet, HPMS_definition,
                      on = 'HPMSVtypeID', how = 'left')

# <codecell>

# define stacked bar chart generator

def plot_clustered_stacked(dfall, labels, title, ylabelname, unit, H="/", **kwargs):
    """Given a list of dataframes, with identical columns and index, create a clustered stacked bar plot. 
labels is a list of the names of the dataframe, used for the legend
title is a string for the title of the plot
H is the hatch used for identification of the different dataframe"""
    plt.figure(figsize = (6, 5))
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
# redistribute VMT fraction among source type and calculate emission rate
MOVES_fleet.loc[:, 'vmt_fraction'] = MOVES_fleet.loc[:, 'vmt_fraction']/ \
    MOVES_fleet.groupby('sourceTypeID')['vmt_fraction'].transform('sum')
print(MOVES_fleet.loc[:, 'vmt_fraction'].sum())

vius_fleet.loc[:, 'VMT_Fraction'] = vius_fleet.loc[:, 'VMT_Fraction']/ \
    vius_fleet.groupby('sourceTypeID')['VMT_Fraction'].transform('sum')
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


var_to_match = ['yearID', 'sourceTypeID', 'fuelTypeID', 'modelYearID']
MOVES_VMT_fraction_with_er = pd.merge(MOVES_fleet,
                                moveser_baseline,
                                on = var_to_match,
                                how = 'left')

MOVES_VMT_fraction_with_er.loc[:, 'emissions'] = \
MOVES_VMT_fraction_with_er.loc[:, 'ratePerDistance'] * \
MOVES_VMT_fraction_with_er.loc[:, 'vmt_fraction']

MOVES_VMT_fraction_with_er.loc[:, 'Source'] = 'MOVES'



VIUS_VMT_fraction_with_er = pd.merge(vius_fleet,
                                moveser_baseline,
                                on = var_to_match,
                                how = 'left')

VIUS_VMT_fraction_with_er.loc[:, 'emissions'] = \
VIUS_VMT_fraction_with_er.loc[:, 'ratePerDistance'] * \
VIUS_VMT_fraction_with_er.loc[:, 'VMT_Fraction']


VIUS_VMT_fraction_with_er.loc[:, 'Source'] = 'VIUS'

# create a combined + cleaned dataset
output_attr = ['yearID', 'sourceTypeID', 'fuelTypeID', 'modelYearID', 
                'roadTypeID', 'avgSpeedBinID', 'Source', 'pollutant', 
                'sourceTypeName', 'AgeBin', 'HPMSVtypeID', 'HPMSVtypeName', 'emissions']

VIUS_emission = VIUS_VMT_fraction_with_er[output_attr]
MOVES_emission = MOVES_VMT_fraction_with_er[output_attr]

combined_emission = pd.concat([MOVES_emission, VIUS_emission])


# <codecell>

# plot emission by source type
order_of_col = ['Light Commercial Truck', 'Single Unit Short-haul Truck',
'Single Unit Long-haul Truck', 'Combination Short-haul Truck', 'Combination Long-haul Truck']

grouping_var = ['Source', 'pollutant', 'roadTypeID', 'avgSpeedBinID', 
                'sourceTypeID', 'sourceTypeName']
emission_by_source_type = combined_emission.groupby(grouping_var) [['emissions']].sum()

emission_by_source_type = emission_by_source_type.reset_index()

emission_by_source_type = pd.merge(emission_by_source_type, 
                               speed_bin_definition,
                               on = 'avgSpeedBinID', how = 'left')

emission_by_source_type = pd.merge(emission_by_source_type, 
                               road_type_definition,
                               on = 'roadTypeID', how = 'left')
emission_by_source_type = \
    emission_by_source_type.sort_values(by = 'sourceTypeID', ascending = False)

emission_by_source_type = \
            emission_by_source_type.sort_values(by = 'avgSpeedBinID', ascending = True)


# urban local
emission_selected = \
    emission_by_source_type.loc[emission_by_source_type['roadTypeID'] == 5]

emission_selected = \
    emission_selected.loc[emission_selected['avgSpeedBinID'] >= 3]
    
i = 1
for pol in list_of_pollutant:

    print(pol)
    # ax = plt.subplot(a, b, i)
    emission_to_plot = \
    emission_selected.loc[emission_selected['pollutant'] == pol] 
    emission_to_plot = \
            emission_to_plot.sort_values(by = 'sourceTypeID', ascending = False)
    if i == 10:
        ax = sns.lineplot(
            data=emission_to_plot,
            x="avgBinSpeed", y="emissions",
            hue="sourceTypeName", style="Source", palette = 'Paired',
            legend = 'auto')
        plt.legend(fontsize = 10)
    else: 
        ax = sns.lineplot(
            data=emission_to_plot,
            x="avgBinSpeed", y="emissions",
            hue="sourceTypeName", style="Source", palette = 'Paired',
            legend = False)
    # speed_bins = speed_bin_definition['avgSpeedBinDesc'].tolist()[2:]
    # ax.set_xticks(np.arange(len(speed_bins)), speed_bins)

    pp = title_lookup[pol]
    plt.title(rf"${pp}$")
    plt.xlabel('Average speed (mph)')
    plt.ylabel('Emission rate ({})'.format(unit_lookup[pol]))
    # plt.xticks(rotation = 60, ha = 'right')
    plt.savefig(os.path.join(path_to_moves, 'plot', 'Sensitivity', 'emission_rate_urban_local_' + pol + '.png'),
                dpi = 300, bbox_inches = 'tight')
    plt.show()
    i += 1


# <codecell>
# urban freeway
emission_selected = \
    emission_by_source_type.loc[emission_by_source_type['roadTypeID'] == 4]

emission_selected = \
    emission_selected.loc[emission_selected['avgSpeedBinID'] >= 3]
    
i = 1
for pol in list_of_pollutant:

    print(pol)
    # ax = plt.subplot(a, b, i)
    emission_to_plot = \
    emission_selected.loc[emission_selected['pollutant'] == pol] 
    emission_to_plot = \
            emission_to_plot.sort_values(by = 'sourceTypeID', ascending = False)
    if i == 10:
        ax = sns.lineplot(
            data=emission_to_plot,
            x="avgBinSpeed", y="emissions",
            hue="sourceTypeName", style="Source", palette = 'Paired',
            legend = 'auto')
        plt.legend(fontsize = 10)
    else: 
        ax = sns.lineplot(
            data=emission_to_plot,
            x="avgBinSpeed", y="emissions",
            hue="sourceTypeName", style="Source", palette = 'Paired',
            legend = False)
    # speed_bins = speed_bin_definition['avgSpeedBinDesc'].tolist()[2:]
    # ax.set_xticks(np.arange(len(speed_bins)), speed_bins)

    pp = title_lookup[pol]
    plt.title(rf"${pp}$")
    plt.xlabel('Average speed (mph)')
    plt.ylabel('Emission rate ({})'.format(unit_lookup[pol]))
    # plt.xticks(rotation = 60, ha = 'right')
    plt.savefig(os.path.join(path_to_moves, 'plot', 'Sensitivity', 'emission_rate_urban_highway_' + pol + '.png'),
                dpi = 300, bbox_inches = 'tight')
    plt.show()
    i += 1

# <codecell>
# all road
# emission_selected = \
#     emission_by_source_type.loc[emission_by_source_type['roadTypeID'] == 4]

emission_selected = \
    emission_by_source_type.loc[emission_by_source_type['avgSpeedBinID'] >= 3]
    
i = 1
style = {'MOVES': '-', 'VIUS':'--'}
for pol in list_of_pollutant:
    plt.figure(figsize = (5, 4.5))
    print(pol)
    # ax = plt.subplot(a, b, i)
    emission_to_plot = \
    emission_selected.loc[emission_selected['pollutant'] == pol] 

    emission_to_plot = \
            emission_to_plot.sort_values(by = 'sourceTypeID', ascending = False)
    # emission_to_plot = \
    #     emission_to_plot.sort_values(by = 'Source', ascending = True)
    if i == 10:
        ax = sns.lineplot(
            data=emission_to_plot,
            x="avgBinSpeed", y="emissions",
            hue="sourceTypeName", style="Source", style_order = ['MOVES', 'VIUS'],
            palette = 'Paired',
            legend = 'auto', errorbar=("pi", 100))
        plt.legend(fontsize = 9)
    else: 
        ax = sns.lineplot(
            data=emission_to_plot,
            x="avgBinSpeed", y="emissions",
            hue="sourceTypeName", style="Source", style_order = ['MOVES', 'VIUS'],
            palette = 'Paired',
            legend = False, errorbar=("pi", 100))
    # speed_bins = speed_bin_definition['avgSpeedBinDesc'].tolist()[2:]
    # ax.set_xticks(np.arange(len(speed_bins)), speed_bins)

    pp = title_lookup[pol]
    plt.title(rf"${pp}$")
    plt.xlabel('Average speed (mph)')
    plt.ylabel('Emission rate ({})'.format(unit_lookup[pol]))
    # plt.xticks(rotation = 60, ha = 'right')
    plt.savefig(os.path.join(path_to_moves, 'plot', 'Sensitivity', 'emission_rate_all_road_' + pol + '.png'),
                dpi = 300, bbox_inches = 'tight')
    plt.show()
    i += 1


