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

# load MOVES emission rates
pollutant_lookup = {2: 'CO', 3: 'NOx', 30: 'NH3', 31:'SO2', 
                    33: 'NO2', 87: 'VOC', 110: 'PM2.5',
                   116: 'PM2.5', 117: 'PM2.5', 100: 'PM10',
                   106: 'PM10', 107: 'PM10'} # Brake and tire wear


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
print(MOVES_fleet.columns)

# load vius data
vius_data_path = os.path.join(path_to_vius, 'VIUS_VMT_fraction_with_fuel_com_only.csv')
vius_fleet = read_csv(vius_data_path)
vius_fleet.head(5)

# 3. other distribution
road_type_distribution = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'road_type_distribution')
speed_distribution = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'speed_distribution')

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
# <codecell>

print(len(MOVES_fleet))
print(len(vius_fleet))

# compute MOVES emission per mile
var_to_match = ['yearID', 'sourceTypeID', 'fuelTypeID', 'modelYearID']

vius_fleet.loc[:, 'yearID'] = analysis_year
MOVES_fleet.loc[:, 'yearID'] = analysis_year

vius_fleet.loc[:, 'modelYearID'] = \
    vius_fleet['yearID'] - vius_fleet.loc[:, 'ageID'] 
    
MOVES_VMT_fraction_with_er = pd.merge(MOVES_fleet,
                                moveser_baseline,
                                on = var_to_match,
                                how = 'left')
print(len(MOVES_VMT_fraction_with_er))
# VMT_fraction_combined.head(5)

MOVES_VMT_fraction_with_er.loc[:, 'Emission_rate'] = \
    MOVES_VMT_fraction_with_er.loc[:, 'ratePerDistance'] * \
        MOVES_VMT_fraction_with_er.loc[:, 'vmt_fraction']
        
grouping_var = ['yearID', 'pollutant', 'roadTypeID', 'avgSpeedBinID']
MOVES_emission_agg = \
MOVES_VMT_fraction_with_er.groupby(grouping_var)[['Emission_rate']].sum()
MOVES_emission_agg = MOVES_emission_agg.reset_index()

MOVES_emission_agg.loc[:, 'roadTypeID'] = MOVES_emission_agg.loc[:, 'roadTypeID'].astype(str)
sns.catplot(MOVES_emission_agg, x = 'avgSpeedBinID', y = 'Emission_rate',
            hue = 'roadTypeID', col = 'pollutant', col_wrap = 4, kind = 'bar', 
            sharey=False, palette = 'Set2')
plt.show()

# <codecell>
VMT_fraction_combined = pd.merge(MOVES_fleet, speed_road_distribution,
                                on = 'sourceTypeID', how = 'left')
print(len(VMT_fraction_combined))
VMT_fraction_combined.loc[:, 'vmt_fraction'] = \
VMT_fraction_combined.loc[:, 'vmt_fraction'] * \
VMT_fraction_combined.loc[:, 'op_vmt_fraction']

var_to_match = ['yearID', 'sourceTypeID', 'fuelTypeID', 'modelYearID', 
                'roadTypeID', 'avgSpeedBinID']
MOVES_VMT_fraction_with_er = pd.merge(VMT_fraction_combined,
                                moveser_baseline,
                                on = var_to_match,
                                how = 'left')

MOVES_VMT_fraction_with_er.loc[:, 'emissions'] = \
MOVES_VMT_fraction_with_er.loc[:, 'ratePerDistance'] * \
MOVES_VMT_fraction_with_er.loc[:, 'vmt_fraction']
# check composition by source type
MOVES_emission_by_source_type = pd.pivot_table(MOVES_VMT_fraction_with_er, 
                                               index = ['pollutant',  'avgSpeedBinID'], 
                                               columns = 'sourceTypeID',
                                               values= 'emissions', aggfunc = 'sum')
MOVES_emission_by_source_type = MOVES_emission_by_source_type.reset_index()

# <codecell>

# try plot
#  Categorical Data
a = 2  # number of rows
b = 4  # number of columns
i = 1  # initialize plot counter

fig = plt.figure(figsize = (18,10))
for pol in MOVES_emission_by_source_type.pollutant.unique():
    ax = plt.subplot(a, b, i)
    emission_to_plot = \
    MOVES_emission_by_source_type.loc[MOVES_emission_by_source_type['pollutant'] == pol]
    emission_to_plot = emission_to_plot.drop(columns = ['pollutant'])
    
    if i == 8:
        emission_to_plot.plot(kind = 'bar', stacked = True, x = 'avgSpeedBinID',
                         cmap= 'plasma_r', ax = ax)
        plt.title(pol)
        plt.legend(fontsize = 10)
    else:
        emission_to_plot.plot(kind = 'bar', stacked = True, x = 'avgSpeedBinID',
                         cmap= 'plasma_r', ax = ax, legend = False)
        plt.title(pol)
        # plt.legend(fontsize = 10)        
    i += 1
    # plt.show
    # break
plt.show()





