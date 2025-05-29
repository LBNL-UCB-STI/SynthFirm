#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 29 09:44:43 2025

@author: xiaodanxu
"""
import os
from pandas import read_csv
import pandas as pd
import matplotlib.pyplot as plt
# import seaborn as sns
import numpy as np
import warnings


scenario_name = 'Seattle'
out_scenario_name = 'Seattle'
file_path = '/Users/xiaodanxu/Documents/SynthFirm.nosync'
parameter_dir = 'SynthFirm_parameters'
input_dir = 'inputs_' + scenario_name
output_path = 'outputs_' + out_scenario_name
# define input files --> eventually loading to pipeline
os.chdir(file_path)   

# from upstream module
synthetic_firms_with_location_file = os.path.join(output_path, 
                                                  'synthetic_firms_with_location.csv')


firms = read_csv(synthetic_firms_with_location_file)
# <codecell>

missing_bus_file = 'missing_BusID_only.csv'
missing_bus_id = read_csv(missing_bus_file)
ids_to_match = missing_bus_id['missing_BusID'].unique()

firms_sel = firms.loc[firms['BusID'].isin(ids_to_match)]

# <codecell>

matched_ids = firms_sel['BusID'].unique()

failed_id = set(matched_ids)-set(ids_to_match)