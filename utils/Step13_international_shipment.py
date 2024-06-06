#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun  6 13:59:01 2024

@author: xiaodanxu
"""

import pandas as pd
import os
import numpy as np
from pandas import read_csv
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings("ignore")

print("Start international shipment generation...")

########################################################
#### step 1 - configure environment and load inputs ####
########################################################

# load model config temporarily here
scenario_name = 'Seattle'
out_scenario_name = 'Seattle'
file_path = '/Users/xiaodanxu/Documents/SynthFirm.nosync'
parameter_dir = 'SynthFirm_parameters'
input_dir = 'inputs_' + scenario_name
output_dir = 'outputs_' + out_scenario_name

c_n6_n6io_sctg_file = 'corresp_naics6_n6io_sctg_revised.csv'
# synthetic_firms_no_location_file = "synthetic_firms.csv" 
zonal_id_file = "zonal_id_lookup_final.csv" # zonal ID lookup table 
# international flow
regional_import_file = 'port/FAF_regional_import.csv'
regional_export_file = 'port/FAF_regional_export.csv'
port_level_import_file = 'port/port_level_import.csv'
port_level_export_file = 'port/port_level_export.csv'

mesozone_faf_lookup = read_csv(os.path.join(file_path, input_dir, zonal_id_file))
c_n6_n6io_sctg = read_csv(os.path.join(file_path, parameter_dir, c_n6_n6io_sctg_file))

regional_import = read_csv(os.path.join(file_path, input_dir, regional_import_file))
regional_export = read_csv(os.path.join(file_path, input_dir, regional_export_file))
port_level_import = read_csv(os.path.join(file_path, input_dir, port_level_import_file))
port_level_export = read_csv(os.path.join(file_path, input_dir, port_level_export_file))

# <codecell>

########################################################
#### step 2 - Create list of port and values ###########
########################################################

# import by port

var_to_group = ['CBP Port Location', ]


