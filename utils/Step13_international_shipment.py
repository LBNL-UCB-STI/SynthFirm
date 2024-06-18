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

var_to_group = ['CBP Port Location', 'FAF', 'is_airport', 'CFS_CODE', 'CFS_NAME']
import_by_port = port_level_import.groupby(var_to_group)[['Customs Value (Gen) ($US)']].sum()
import_by_port.columns = ['import value']
import_by_port.loc[:, 'import value'] /= 10 ** 6 # convert value to million
import_by_port = import_by_port.reset_index()

# export by port
export_by_port = port_level_export.groupby(var_to_group)[['Total Exports Value ($US)']].sum()
export_by_port.columns = ['export value']
export_by_port.loc[:, 'export value'] /= 10 ** 6 # convert value to million
export_by_port = export_by_port.reset_index()

# total import in region
faf_import_scaling = \
    regional_import.groupby(['CFS_CODE', 'CFS_NAME'])['value_2017'].sum()

faf_import_scaling = faf_import_scaling.reset_index()
faf_import_scaling = \
    faf_import_scaling.rename(columns = {'value_2017':'region import total'})

# total export in region
faf_export_scaling = \
    regional_export.groupby(['CFS_CODE', 'CFS_NAME'])['value_2017'].sum()

faf_export_scaling = faf_export_scaling.reset_index()
faf_export_scaling = \
    faf_export_scaling.rename(columns = {'value_2017':'region export total'})
    
# calculating fraction of port-level value among whole region
import_by_port = pd.merge(import_by_port, faf_import_scaling,
                          on = ['CFS_CODE', 'CFS_NAME'], how = 'left')
export_by_port = pd.merge(export_by_port, faf_export_scaling,
                          on = ['CFS_CODE', 'CFS_NAME'], how = 'left')

import_by_port.loc[:, 'import_frac'] = import_by_port.loc[:, 'import value']/ \
    import_by_port.loc[:, 'region import total']
    
export_by_port.loc[:, 'export_frac'] = export_by_port.loc[:, 'export value']/ \
    export_by_port.loc[:, 'region export total']
    
import_by_port['import_frac'].hist()
print('range of import adj. factors')
print(import_by_port['import_frac'].min(), import_by_port['import_frac'].max())
export_by_port['export_frac'].hist()
print('range of export adj. factors')
print(export_by_port['export_frac'].min(), export_by_port['export_frac'].max())


# <codecell>

########################################################
#### step 3 - Disaggregate FAF value to port ###########
########################################################

# FAF value grouped by foreign country, domestic destination and sctg for entire region
print('total import before disaggregation:')
print(regional_import['value_2017'].sum())
agg_var_in = ['CFS_CODE', 'CFS_NAME', 'dms_dest', 'sctg2']
regional_import_agg = \
    regional_import.groupby(agg_var_in)[['tons_2017', 'value_2017']].sum()
regional_import_agg = regional_import_agg.reset_index()
regional_import_agg = \
    regional_import_agg.loc[regional_import_agg['value_2017'] > 0]

# FAF value grouped by foreign country, domestic destination and sctg for entire region


agg_var_out = ['CFS_CODE', 'CFS_NAME', 'dms_orig', 'sctg2']
print('total export before disaggregation:')
print(regional_export['value_2017'].sum())
regional_export_agg = \
    regional_export.groupby(agg_var_out)[['tons_2017', 'value_2017']].sum()    
regional_export_agg = regional_export_agg.reset_index()
regional_export_agg = \
    regional_export_agg.loc[regional_export_agg['value_2017'] > 0]
    

import_by_port_by_dest = pd.merge(import_by_port,
                                  regional_import_agg,
                                  on = ['CFS_CODE', 'CFS_NAME'], how = 'left')

import_by_port_by_dest.loc[:, 'value_2017'] *= import_by_port_by_dest.loc[:, 'import_frac']
import_by_port_by_dest.loc[:, 'tons_2017'] *= import_by_port_by_dest.loc[:, 'import_frac']

print('total impprt after disaggregation:')
print(import_by_port_by_dest['value_2017'].sum())    

export_by_port_by_orig = pd.merge(export_by_port,
                                  regional_export_agg,
                                  on = ['CFS_CODE', 'CFS_NAME'], how = 'left')

export_by_port_by_orig.loc[:, 'value_2017'] *= export_by_port_by_orig.loc[:, 'export_frac']
export_by_port_by_orig.loc[:, 'tons_2017'] *= export_by_port_by_orig.loc[:, 'export_frac']

print('total impprt after disaggregation:')
print(export_by_port_by_orig['value_2017'].sum()) 