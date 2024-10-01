#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 30 11:48:14 2024

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

hpms_definition = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'HPMS_definition')
source_type_hpms = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'source_type_HPMS')
source_type_population = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'source_type_population')

hpms_vmt = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'HPMS_VMT')
age_distribution = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'AGE_distribution')
fuel_type_distribution = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'fuel_type_distribution')
fuel_type_definition = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'fuel_type_definition')
RMAR_factor = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'RMAR')

# <codecell>

# Calculate VMT and population growth factor
baseline_year = 2021
hpms_vmt = hpms_vmt.loc[hpms_vmt['yearID'] >= baseline_year]
hpms_vmt = hpms_vmt.sort_values(by = 'yearID', ascending = True)
hpms_vmt.loc[:, 'HPMSPreYearVMT'] = hpms_vmt.groupby('HPMSVtypeID')['HPMSBaseYearVMT'].shift(1)
hpms_vmt.loc[:, 'VMTGrowthFactor'] = hpms_vmt.loc[:, 'HPMSBaseYearVMT']/hpms_vmt.loc[:, 'HPMSPreYearVMT']
hpms_vmt.loc[:, 'VMTGrowthFactor'].fillna(1, inplace = True)

hpms_vmt.loc[:, 'Cumulative VMT growth rate'] = hpms_vmt.groupby('HPMSVtypeID')['VMTGrowthFactor'].cumprod()

hpms_vmt_with_def = pd.merge(hpms_vmt, hpms_definition,
                    on = 'HPMSVtypeID', how = 'left')
sns.lineplot(data=hpms_vmt_with_def, x="yearID", y="Cumulative VMT growth rate", 
             hue="HPMSVtypeName")

# <codecell>

source_type_population = \
    source_type_population.loc[source_type_population['yearID'] >= baseline_year]
    
source_type_population = \
    source_type_population.sort_values(by = 'yearID', ascending = True)
source_type_population.loc[:, 'sourceTypePopulationPre'] = \
    source_type_population.groupby('sourceTypeID')['sourceTypePopulation'].shift(1)
source_type_population.loc[:, 'PopGrowthFactor'] = \
    source_type_population.loc[:, 'sourceTypePopulation']/ \
        source_type_population.loc[:, 'sourceTypePopulationPre']
source_type_population.loc[:, 'PopGrowthFactor'].fillna(1, inplace = True)

source_type_population.loc[:, 'Cumulative growth rate'] = \
    source_type_population.groupby('sourceTypeID')['PopGrowthFactor'].cumprod()
    
selected_type = [32, 52, 53, 61, 62]
source_type_population_with_def = pd.merge(source_type_population, source_type_hpms,
                    on = 'sourceTypeID', how = 'left')
source_type_population_with_def = \
    source_type_population_with_def.loc[source_type_population_with_def['sourceTypeID'].isin(selected_type)]
sns.lineplot(data=source_type_population_with_def, 
             x="yearID", y="Cumulative growth rate", 
             hue="sourceTypeName", style = 'sourceTypeName',
             markers=True)
plt.legend(bbox_to_anchor = (1.01, 1))