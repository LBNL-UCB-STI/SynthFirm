# -*- coding: utf-8 -*-
"""
Created on Mon May 26 12:41:35 2025

@author: xiaodanxu
"""

import pandas as pd
import os
from os import listdir
# import constants as c
import numpy as np
from pandas import read_csv
import warnings

#define paths
warnings.filterwarnings("ignore")

data_dir = 'C:/SynthFirm'
os.chdir(data_dir)
scenario_name = 'national'
out_scenario_name = 'national'
parameter_dir = 'SynthFirm_parameters'

input_dir = 'inputs_' + scenario_name
output_dir = 'outputs_' + out_scenario_name

# load freight trip output
trip_file = 'daily_trips_by_vehicle_type.csv.zip'
model_output_with_time = read_csv(os.path.join(output_dir, trip_file))

# load FAF VMT
data_source = '_17 Dom'
faf_vmt_by_link = read_csv(os.path.join('Validation', 'FAF_VMT' + data_source + '.csv'))

# load commodity crosswalk
sctg_file = 'SCTG_FAF_GROUP_LOOKUP.csv'
sctg_lookup = read_csv(os.path.join(parameter_dir, sctg_file))

# <codecell>
import matplotlib.pyplot as plt
import seaborn as sns

hourly_VMT = \
    model_output_with_time.groupby(['start_hour', 'veh_class'])[['Distance']].sum()
hourly_VMT = hourly_VMT.reset_index()
print(hourly_VMT['Distance'].sum())

sns.lineplot(data = hourly_VMT, x ='start_hour', y = 'Distance',
             hue = 'veh_class')
plt.show()

VMT_by_class = hourly_VMT.groupby('veh_class')[['Distance']].sum()
VMT_by_class = VMT_by_class.reset_index()
VMT_by_class.loc[:, 'Fraction'] = VMT_by_class.loc[:, 'Distance'] /\
    VMT_by_class.loc[:, 'Distance'].sum()
    
VMT_by_sctg = model_output_with_time.groupby('Commodity_SCTG')[['Distance']].sum()
VMT_by_sctg = VMT_by_sctg.reset_index()

sctg_lookup.rename(columns = {'SCTG': 'Commodity_SCTG'}, inplace = True)
VMT_by_sctg = pd.merge(VMT_by_sctg, sctg_lookup, on = 'Commodity_SCTG', how = 'left')

# <codecell>
VMT_by_faf_group = VMT_by_sctg.groupby('FAF_group')[['Distance']].sum()
VMT_by_faf_group = VMT_by_faf_group.reset_index()

VMT_by_faf_group.columns = ['FAF_group', 'SynthFirm VMT']
VMT_by_faf_group.loc[:, 'FAF5 VMT'] = 0
for index, row in VMT_by_faf_group.iterrows():
    elem = row['FAF_group']
    attr = 'VMT_' + elem
    faf_vmt = faf_vmt_by_link[attr].sum()
    VMT_by_faf_group.loc[index, 'FAF5 VMT'] = faf_vmt

VMT_by_faf_group.loc[:, 'VMT_DIFF'] = VMT_by_faf_group.loc[:, 'SynthFirm VMT'] -\
    VMT_by_faf_group.loc[:, 'FAF5 VMT']
VMT_by_faf_group = VMT_by_faf_group.sort_values(by = 'VMT_DIFF', ascending = False)
print(VMT_by_faf_group[[ 'SynthFirm VMT', 'FAF5 VMT']].sum())

# <codecell>
modeled_VMT_v2 = \
    model_output_with_time.groupby(['start_hour',  'veh_class'])[['length']].sum()
modeled_VMT_v2 = modeled_VMT_v2.reset_index()
print(modeled_VMT_v2['length'].sum())

sns.lineplot(data = modeled_VMT_v2, x ='start_hour', y = 'length',
             hue = 'veh_class')
plt.show()


