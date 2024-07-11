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
scenario_name = 'BayArea'
out_scenario_name = 'BayArea'
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
SCTG_group_file = 'SCTG_Groups_revised.csv'
int_shipment_size_file = 'international_shipment_size.csv'

mesozone_faf_lookup = read_csv(os.path.join(file_path, input_dir, zonal_id_file))
c_n6_n6io_sctg = read_csv(os.path.join(file_path, parameter_dir, c_n6_n6io_sctg_file))
sctg_lookup = read_csv(os.path.join(file_path, parameter_dir, SCTG_group_file))
int_shipment_size = read_csv(os.path.join(file_path, parameter_dir, int_shipment_size_file))

regional_import = read_csv(os.path.join(file_path, input_dir, regional_import_file))
regional_export = read_csv(os.path.join(file_path, input_dir, regional_export_file))
port_level_import = read_csv(os.path.join(file_path, input_dir, port_level_import_file))
port_level_export = read_csv(os.path.join(file_path, input_dir, port_level_export_file))

# <codecell>

########################################################
#### step 2 - Generate list of shipments ###############
########################################################

# append shipment size
sctg_lookup_short = sctg_lookup[['SCTG_Code', 'SCTG_Group']]
int_shipment_size_short = \
    int_shipment_size[['SCTG', 'CFS_CODE', 'median_weight_ton']]
    
regional_import = \
    regional_import.rename(columns = {'sctg2': 'SCTG_Code'})
    
regional_export = \
    regional_export.rename(columns = {'sctg2': 'SCTG_Code'})

int_shipment_size_short = \
    int_shipment_size_short.rename(columns = {'SCTG': 'SCTG_Code'})
   
regional_import_by_size = pd.merge(regional_import,
                                   int_shipment_size_short,
                                   on = ['SCTG_Code', 'CFS_CODE'], how = 'left')


regional_export_by_size = pd.merge(regional_export,
                                   int_shipment_size_short,
                                   on = ['SCTG_Code', 'CFS_CODE'], how = 'left')



# <codecell>
# generate shipment count
regional_import_by_size.loc[:, "ship_count"] = \
    regional_import_by_size.loc[:, "tons_2017"] * 1000 / \
            regional_import_by_size.loc[:, "median_weight_ton"] 
regional_import_by_size.loc[:, "ship_count"] = \
    np.round(regional_import_by_size.loc[:, "ship_count"], 0)
    
regional_import_by_size.loc[regional_import_by_size["ship_count"] < 1, "ship_count"] = 1

# import count can be really large, trim off the long tail
cut_off = np.round(regional_import_by_size["ship_count"].quantile(0.999), 0)
print(cut_off)
regional_import_by_size.loc[regional_import_by_size["ship_count"] >= cut_off, "ship_count"] = cut_off      

regional_export_by_size.loc[:, "ship_count"] = \
    regional_export_by_size.loc[:, "tons_2017"] * 1000 / \
            regional_export_by_size.loc[:, "median_weight_ton"] 
regional_export_by_size.loc[:, "ship_count"] = \
    np.round(regional_export_by_size.loc[:, "ship_count"], 0)
regional_export_by_size.loc[regional_export_by_size["ship_count"] < 1, "ship_count"] = 1

            
print('Total import shipments before scaling:')
print(regional_import_by_size.loc[:, "ship_count"].sum())

print('Total export shipments before scaling:')
print(regional_export_by_size.loc[:, "ship_count"].sum())
# <codecell>


########################################################
#### step 3 - Create list of port and values ###########
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


total_import_shipment = regional_import_by_size.loc[:, "ship_count"].sum()

total_export_shipment = regional_export_by_size.loc[:, "ship_count"].sum()

# FAF value grouped by foreign country, domestic destination and sctg for entire region
print('total import before disaggregation:')
print(regional_import['value_2017'].sum())
agg_var_in = ['CFS_CODE', 'CFS_NAME', 'dms_dest', 'SCTG_Code']
regional_import_agg = \
    regional_import_by_size.groupby(agg_var_in)[['tons_2017', 'value_2017', 'ship_count']].sum()
regional_import_agg = regional_import_agg.reset_index()
regional_import_agg = \
    regional_import_agg.loc[regional_import_agg['value_2017'] > 0]

# FAF value grouped by foreign country, domestic destination and sctg for entire region


agg_var_out = ['CFS_CODE', 'CFS_NAME', 'dms_orig', 'SCTG_Code']
print('total export before disaggregation:')
print(regional_export['value_2017'].sum())
regional_export_agg = \
    regional_export_by_size.groupby(agg_var_out)[['tons_2017', 'value_2017', 'ship_count']].sum()    
regional_export_agg = regional_export_agg.reset_index()
regional_export_agg = \
    regional_export_agg.loc[regional_export_agg['value_2017'] > 0]
    

import_by_port_by_dest = pd.merge(import_by_port,
                                  regional_import_agg,
                                  on = ['CFS_CODE', 'CFS_NAME'], how = 'left')

print(len(import_by_port_by_dest))
import_by_port_by_dest.loc[:, 'value_2017'] *= import_by_port_by_dest.loc[:, 'import_frac']
import_by_port_by_dest.loc[:, 'tons_2017'] *= import_by_port_by_dest.loc[:, 'import_frac']
import_by_port_by_dest.loc[:, 'ship_count'] *= import_by_port_by_dest.loc[:, 'import_frac']

import_by_port_by_dest = import_by_port_by_dest.loc[import_by_port_by_dest['ship_count']  >= 1]
import_by_port_by_dest.loc[:, 'ship_count'] = np.round(import_by_port_by_dest.loc[:, 'ship_count'], 0)
print(len(import_by_port_by_dest))
import_by_port_by_dest.loc[:, 'value_density'] = \
    import_by_port_by_dest.loc[:, 'value_2017'] / import_by_port_by_dest.loc[:, 'tons_2017'] * 1000
import_by_port_by_dest.loc[:, "TruckLoad"] = import_by_port_by_dest.loc[:, "tons_2017"] * 1000  / \
            import_by_port_by_dest.loc[:, "ship_count"] # unit is ton

# $/ton

print('total import shipment after disaggregation:')
print(import_by_port_by_dest['ship_count'].sum()) 

print('total import value (million $) after disaggregation:')
print(import_by_port_by_dest['value_2017'].sum())    

print('total import tonnage after disaggregation:')
print(import_by_port_by_dest['tons_2017'].sum() * 1000)  


# <codecell>
export_by_port_by_orig = pd.merge(export_by_port,
                                  regional_export_agg,
                                  on = ['CFS_CODE', 'CFS_NAME'], how = 'left')
print(len(export_by_port_by_orig))
export_by_port_by_orig.loc[:, 'value_2017'] *= export_by_port_by_orig.loc[:, 'export_frac']
export_by_port_by_orig.loc[:, 'tons_2017'] *= export_by_port_by_orig.loc[:, 'export_frac']
export_by_port_by_orig.loc[:, 'ship_count'] *= export_by_port_by_orig.loc[:, 'export_frac']
export_by_port_by_orig = export_by_port_by_orig.loc[export_by_port_by_orig['ship_count']  >= 1]
export_by_port_by_orig.loc[:, 'ship_count'] = np.round(export_by_port_by_orig.loc[:, 'ship_count'], 0)
print(len(export_by_port_by_orig))
export_by_port_by_orig.loc[:, 'value_density'] = \
    export_by_port_by_orig.loc[:, 'value_2017'] / export_by_port_by_orig.loc[:, 'tons_2017'] * 1000
export_by_port_by_orig.loc[:, "TruckLoad"] = export_by_port_by_orig.loc[:, "tons_2017"] * 1000  / \
            export_by_port_by_orig.loc[:, "ship_count"] # unit is ton

print('total export shipment after disaggregation:')
print(export_by_port_by_orig['ship_count'].sum())  

print('total export value (million $) after disaggregation:')
print(export_by_port_by_orig['value_2017'].sum()) 

print('total export tonnage after disaggregation:')
print(export_by_port_by_orig['tons_2017'].sum() * 1000)  



# <codecell>
# import_count_by_sctg = \
#     import_by_shipment_size.groupby('SCTG_Code')[["ship_count"]].sum()
# import_count_by_sctg = import_count_by_sctg.reset_index()


# export_count_by_sctg = \
#     export_by_shipment_size.groupby('SCTG_Code')[["ship_count"]].sum()
# export_count_by_sctg = export_count_by_sctg.reset_index()

# <codecell>

# create separate row for each shipment
# def split_dataframe(df, chunk_size = 10 ** 6): 
#     chunks = list()
#     num_chunks = len(df) // chunk_size + 1
#     for i in range(num_chunks):
#         chunks.append(df[i*chunk_size:(i+1)*chunk_size])
#     return chunks

chunk_size = 10 ** 5
import_attr = ['CBP Port Location', 'FAF', 'is_airport', 'CFS_CODE', 'CFS_NAME',
       'dms_dest', 'SCTG_Code', 'TruckLoad', 'ship_count', 'value_2017', 'value_density', 'SCTG_Group']
export_attr = ['CBP Port Location', 'FAF', 'is_airport', 'CFS_CODE', 'CFS_NAME',
       'dms_orig', 'SCTG_Code', 'TruckLoad', 'ship_count', 'value_2017', 'value_density', 'SCTG_Group']
# output_dir = os.path.join(file_path, output_dir, )

import_by_port_by_dest = pd.merge(import_by_port_by_dest,
                                   sctg_lookup_short,
                                   on = ['SCTG_Code'], how = 'left')

output_path = os.path.join(file_path, output_dir)
import_output = import_by_port_by_dest[import_attr]
import_output.to_csv(os.path.join(output_path, 'import_od.csv'), index = False)

export_by_port_by_orig = pd.merge(export_by_port_by_orig,
                                   sctg_lookup_short,
                                   on = ['SCTG_Code'], how = 'left')
export_output = export_by_port_by_orig[export_attr]
export_output.to_csv(os.path.join(output_path, 'export_od.csv'), index = False)

