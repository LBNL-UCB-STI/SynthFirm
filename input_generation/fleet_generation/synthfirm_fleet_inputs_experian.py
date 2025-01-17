#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 16 15:44:43 2025

@author: xiaodanxu
"""
import pandas as pd
from pandas import read_csv
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
from os import listdir
from pygris import states


import warnings
warnings.filterwarnings("ignore")

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')
path_to_output = 'PrivateData/registration/' # access for private data
cleaned_experian_file = os.path.join(path_to_output, 'cleaned_experian_data_national.csv')
experian_national_fleet = read_csv(cleaned_experian_file)

# <codecell>

# check industry missing value
veh_ct_by_naics = experian_national_fleet.groupby(['naics_lvl3'])[['vehicle_count']].sum()
veh_ct_by_naics = veh_ct_by_naics.reset_index()

veh_ct_by_naics.loc[:, 'fraction'] = veh_ct_by_naics.loc[:, 'vehicle_count'] / \
    veh_ct_by_naics.loc[:, 'vehicle_count'].sum()

# <codecell>
# aggregate data to state level

experian_national_fleet.loc[:, 'service_type'] = 'PRIVATE'
experian_national_fleet.loc[experian_national_fleet['naics_lvl3'].isin(['484', '492']), 'service_type'] = 'FOR HIRE'
experian_national_fleet.loc[experian_national_fleet['naics_lvl3'].isin(['532']), 'service_type'] = 'LEASE'
experian_national_fleet.loc[:, 'service type'] = 'PRIVATE'
grouping_var = ['state_abbr', 'veh_class', 'age', 'AGE_BIN', 'service_type', 'fuel_ty']
national_fleet_by_state = \
    experian_national_fleet.groupby(grouping_var)[['vehicle_count']].sum()

national_fleet_by_state = national_fleet_by_state.reset_index()

# <codecell>
summary_by_state = pd.pivot_table(national_fleet_by_state, 
                                  index = ['state_abbr', 'veh_class'],
                                  columns = 'service_type', values = 'vehicle_count',
                                  aggfunc='sum')
summary_by_state = summary_by_state.reset_index()

summary_output = os.path.join(path_to_output, 'experian_summary_by_state.csv')
summary_by_state.to_csv(summary_output, index = False)