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
os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')

# <codecell>

# step 1 -- compile AVFT scenarios
path_to_moves = 'RawData/MOVES'

fuel_type_distribution = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'fuel_type_distribution')


year_begin  = 2021
year_end = 2050
fuel_type_agg_frac = \
    fuel_type_distribution.groupby(['sourceTypeID', 'modelYearID', 'fuelTypeID', 'engTechID'])[['stmyFraction']].sum()
fuel_type_agg_frac = fuel_type_agg_frac.reset_index()  

# select AVFT from two sets of data, for model year <= 2050 
ldt_fuel_type_agg_frac = \
    fuel_type_agg_frac.loc[(fuel_type_agg_frac['sourceTypeID'] == 32) & \
                           (fuel_type_agg_frac['modelYearID'] <= year_end)]
mhd_fuel_type_agg_frac = \
fuel_type_agg_frac.loc[(fuel_type_agg_frac['sourceTypeID'].isin([52, 53, 61, 62])) & \
                       (fuel_type_agg_frac['modelYearID'] < year_begin)]
com_fuel_type_agg_frac = pd.concat([ldt_fuel_type_agg_frac, mhd_fuel_type_agg_frac])

fuel_type_definition = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'fuel_type_definition')

list_of_avft_file = ['TDA_AVFT_HOP_highp2.csv', 'TDA_AVFT_HOP_highp6.csv', 'TDA_AVFT_HOP_highp10.csv',
                     'TDA_AVFT_Ref_highp2.csv', 'TDA_AVFT_Ref_highp6.csv', 'TDA_AVFT_Ref_highp10.csv']

scenario_lookup = {'TDA_AVFT_HOP_highp2.csv': 'high oil, low elec', 
                   'TDA_AVFT_HOP_highp6.csv': 'high oil, mid elec',
                   'TDA_AVFT_HOP_highp10.csv': 'high oil, high elec',
                     'TDA_AVFT_Ref_highp2.csv': 'low oil, low elec', 
                     'TDA_AVFT_Ref_highp6.csv': 'low oil, mid elec',
                     'TDA_AVFT_Ref_highp10.csv': 'low oil, high elec'}

for fuel_file in list_of_avft_file:
    mhd_avft = read_csv(os.path.join(path_to_moves, 'turnover', fuel_file))
    mhd_avft.drop(columns = ['Unnamed: 0', 'HPMSVtypeName', 'sourceTypeName'], inplace = True)
    com_avft = pd.concat([mhd_avft, com_fuel_type_agg_frac])
    break