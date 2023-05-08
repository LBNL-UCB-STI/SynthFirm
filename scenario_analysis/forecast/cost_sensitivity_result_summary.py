#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May  1 15:40:00 2023

@author: xiaodanxu
"""

import pandas as pd
import os
import numpy as np
from pandas import read_csv
import matplotlib.pyplot as plt
import seaborn as sns
import warnings

# change to data dir

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')

op_cost_by_scenario = read_csv('Parameter/' + 'opcost_sensitivity_analysis.csv', sep = ',')
scenario_dirs = ['Ref_p2', 'Ref_p4', 'Ref_p6', 'Ref_p8', 'Ref_p10',
                 'HOP_p2', 'HOP_p4', 'HOP_p6', 'HOP_p8', 'HOP_p10']

mode_choice_output_comb = None
for sc_dir in scenario_dirs:
    print('processing scenario ' + sc_dir)
    mode_choice_output = read_csv('outputs_aus_2050/' + sc_dir + '/processed_b2b_flow_summary.csv')
    mode_choice_output.loc[:, 'scenario'] = sc_dir
    mode_choice_output_comb = pd.concat([mode_choice_output_comb, mode_choice_output])

    # break

# <codecell>
mode_choice_output_comb = mode_choice_output_comb.loc[mode_choice_output_comb['mode_choice'] != 'Other']
mode_list = mode_choice_output_comb['mode_choice'].unique()
tonnage_by_mode = pd.pivot_table(mode_choice_output_comb, values='ShipmentLoad', index=['scenario'],
                    columns=['mode_choice'], aggfunc=np.sum)
tonnage_by_mode.loc[:, 'sum'] = tonnage_by_mode.sum(axis=1)
tonnage_by_mode = tonnage_by_mode.loc[:, mode_list].div(tonnage_by_mode["sum"], axis=0)
tonnage_by_mode.to_csv('outputs_aus_2050/' + 'mode_choice_cost_sensitivity_load.csv')

count_by_mode = pd.pivot_table(mode_choice_output_comb, values='count', index=['scenario'],
                    columns=['mode_choice'], aggfunc=np.sum)
count_by_mode.loc[:, 'sum'] = count_by_mode.sum(axis=1)
count_by_mode = count_by_mode.loc[:, mode_list].div(count_by_mode["sum"], axis=0)
count_by_mode.to_csv('outputs_aus_2050/' + 'mode_choice_cost_sensitivity_count.csv')