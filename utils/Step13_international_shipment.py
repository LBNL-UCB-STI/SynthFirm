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
sctg_by_port_file = 'commodity_to_port_constraint.csv'

mesozone_faf_lookup = read_csv(os.path.join(file_path, input_dir, zonal_id_file))
c_n6_n6io_sctg = read_csv(os.path.join(file_path, parameter_dir, c_n6_n6io_sctg_file))
sctg_lookup = read_csv(os.path.join(file_path, parameter_dir, SCTG_group_file))
int_shipment_size = read_csv(os.path.join(file_path, parameter_dir, int_shipment_size_file))
sctg_by_port_constraint = read_csv(os.path.join(file_path, parameter_dir, sctg_by_port_file))

regional_import = read_csv(os.path.join(file_path, input_dir, regional_import_file))
regional_export = read_csv(os.path.join(file_path, input_dir, regional_export_file))
port_level_import = read_csv(os.path.join(file_path, input_dir, port_level_import_file))
port_level_export = read_csv(os.path.join(file_path, input_dir, port_level_export_file))


# <codecell>

########################################################
#### step 1 - Scale regional import-export flow ########
########################################################

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

# total import from usato    
usato_import_scaling = \
    port_level_import.groupby(['CFS_CODE', 'CFS_NAME'])[['Customs Value (Gen) ($US)']].sum()
usato_import_scaling.columns = ['import value']
usato_import_scaling.loc[:, 'import value'] /= 10 ** 6 # convert value to million
usato_import_scaling = usato_import_scaling.reset_index()

# total export from usato 
usato_export_scaling = \
    port_level_export.groupby(['CFS_CODE', 'CFS_NAME'])[['Total Exports Value ($US)']].sum()
usato_export_scaling.columns = ['export value']
usato_export_scaling.loc[:, 'export value'] /= 10 ** 6 # convert value to million
usato_export_scaling = usato_export_scaling.reset_index()

# derive regional scaling factor
regional_import_scaling = pd.merge(usato_import_scaling, faf_import_scaling,
                          on = ['CFS_CODE', 'CFS_NAME'], how = 'left')
regional_export_scaling = pd.merge(usato_export_scaling, faf_export_scaling,
                          on = ['CFS_CODE', 'CFS_NAME'], how = 'left')

regional_import_scaling.loc[:, 'import_frac'] = \
    regional_import_scaling.loc[:, 'import value']/ \
    regional_import_scaling.loc[:, 'region import total']

print(regional_import_scaling.loc[:, 'import_frac'].min(), 
      regional_import_scaling.loc[:, 'import_frac'].max())    

regional_export_scaling.loc[:, 'export_frac'] = \
    regional_export_scaling.loc[:, 'export value']/ \
    regional_export_scaling.loc[:, 'region export total']
print(regional_export_scaling.loc[:, 'export_frac'].min(), 
      regional_export_scaling.loc[:, 'export_frac'].max())  

# <codecell>

########################################################
#### step 2 - Generate list of shipments ###############
########################################################
regional_import_scaling = \
    regional_import_scaling[['CFS_CODE', 'CFS_NAME', 'import_frac']]
regional_export_scaling = \
    regional_export_scaling[['CFS_CODE', 'CFS_NAME', 'export_frac']]


# append shipment size
sctg_lookup_short = sctg_lookup[['SCTG_Code', 'SCTG_Group']]
int_shipment_size_short = \
    int_shipment_size[['SCTG', 'CFS_CODE', 'median_weight_ton']]
    
regional_import = \
    regional_import.rename(columns = {'sctg2': 'SCTG_Code'})

regional_import_scaled = pd.merge(regional_import, regional_import_scaling,
                                  on = ['CFS_CODE', 'CFS_NAME'], how = 'left') 
regional_import_scaled.loc[:, 'tons_2017'] *= regional_import_scaled.loc[:, 'import_frac']   
regional_import_scaled.loc[:, 'value_2017'] *= regional_import_scaled.loc[:, 'import_frac']   

regional_export = \
    regional_export.rename(columns = {'sctg2': 'SCTG_Code'})

regional_export_scaled = pd.merge(regional_export, regional_export_scaling,
                                  on = ['CFS_CODE', 'CFS_NAME'], how = 'left') 
regional_export_scaled.loc[:, 'tons_2017'] *= regional_export_scaled.loc[:, 'export_frac']   
regional_export_scaled.loc[:, 'value_2017'] *= regional_export_scaled.loc[:, 'export_frac']   

int_shipment_size_short = \
    int_shipment_size_short.rename(columns = {'SCTG': 'SCTG_Code'})
    
   
regional_import_by_size = pd.merge(regional_import_scaled,
                                   int_shipment_size_short,
                                   on = ['SCTG_Code', 'CFS_CODE'], how = 'left')


regional_export_by_size = pd.merge(regional_export_scaled,
                                   int_shipment_size_short,
                                   on = ['SCTG_Code', 'CFS_CODE'], how = 'left')

# generate shipment count
regional_import_by_size.loc[:, "ship_count"] = \
    regional_import_by_size.loc[:, "tons_2017"] * 1000 / \
            regional_import_by_size.loc[:, "median_weight_ton"] 
regional_import_by_size.loc[:, "ship_count"] = \
    np.round(regional_import_by_size.loc[:, "ship_count"], 0)
    
regional_import_by_size.loc[regional_import_by_size["ship_count"] < 1, "ship_count"] = 1

# import count can be really large, trim off the long tail
cut_off = np.round(regional_import_by_size["ship_count"].quantile(0.999), 0)
# cut_off = regional_import_by_size["ship_count"].quantile(0.75) + \
#     1.5 * (regional_import_by_size["ship_count"].quantile(0.75) - \
#            regional_import_by_size["ship_count"].quantile(0.25))
# cut_off = np.round(cut_off, 0)
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
#### step 3 - Assign airport flow first (less sctg) ####
########################################################

# import by port

var_to_group = ['PORTID', 'CBP Port Location', 'FAF', 'CBPZONE', 'MESOZONE', 'TYPE', 
                'is_airport', 'CFS_CODE', 'CFS_NAME']
import_by_port = port_level_import.groupby(var_to_group)[['Customs Value (Gen) ($US)']].sum()
import_by_port.columns = ['import value']
import_by_port.loc[:, 'import value'] /= 10 ** 6 # convert value to million
import_by_port = import_by_port.reset_index()

# export by port
export_by_port = port_level_export.groupby(var_to_group)[['Total Exports Value ($US)']].sum()
export_by_port.columns = ['export value']
export_by_port.loc[:, 'export value'] /= 10 ** 6 # convert value to million
export_by_port = export_by_port.reset_index()

# split airport/other port
import_by_airport = import_by_port.loc[import_by_port['TYPE'] == 'Airport']
import_by_other_port = import_by_port.loc[import_by_port['TYPE'] != 'Airport']

export_by_airport = export_by_port.loc[export_by_port['TYPE'] == 'Airport']
export_by_other_port = export_by_port.loc[export_by_port['TYPE'] != 'Airport']

# select faf values with SCTG accessible at airport

sctg_with_ap_access = \
    sctg_by_port_constraint.loc[sctg_by_port_constraint['Airport'] == 1, 'SCTG'].unique()

faf_import_ap_only = \
    regional_import_by_size.loc[regional_import_by_size['SCTG_Code'].isin(sctg_with_ap_access)]
faf_import_scaling_ap_only = faf_import_ap_only.groupby(['CFS_CODE', 'CFS_NAME'])['value_2017'].sum()

faf_import_scaling_ap_only = faf_import_scaling_ap_only.reset_index()
faf_import_scaling_ap_only = \
    faf_import_scaling_ap_only.rename(columns = {'value_2017':'airport import total'})
    
faf_export_ap_only = \
    regional_export_by_size.loc[regional_export_by_size['SCTG_Code'].isin(sctg_with_ap_access)]
faf_export_scaling_ap_only = faf_export_ap_only.groupby(['CFS_CODE', 'CFS_NAME'])['value_2017'].sum()

faf_export_scaling_ap_only = faf_export_scaling_ap_only.reset_index()
faf_export_scaling_ap_only = \
    faf_export_scaling_ap_only.rename(columns = {'value_2017':'airport export total'})
    
# <codecell>
# calculating fraction of airport-level value among sctgs available to airport

import_by_airport = pd.merge(import_by_airport, faf_import_scaling_ap_only,
                          on = ['CFS_CODE', 'CFS_NAME'], how = 'left')
export_by_airport = pd.merge(export_by_airport, faf_export_scaling_ap_only,
                          on = ['CFS_CODE', 'CFS_NAME'], how = 'left')

import_by_airport.loc[:, 'import_frac'] = import_by_airport.loc[:, 'import value']/ \
    import_by_airport.loc[:, 'airport import total']
    
export_by_airport.loc[:, 'export_frac'] = export_by_airport.loc[:, 'export value']/ \
    export_by_airport.loc[:, 'airport export total']
    
import_by_airport['import_frac'].hist()
print('range of import adj. factors')
print(import_by_airport['import_frac'].min(), import_by_airport['import_frac'].max())
export_by_airport['export_frac'].hist()
print('range of export adj. factors')
print(export_by_airport['export_frac'].min(), export_by_airport['export_frac'].max())


# <codecell>

# assign destination to airport import
agg_var_in = ['CFS_CODE', 'CFS_NAME', 'dms_dest', 'SCTG_Code']
airport_import_agg = \
    faf_import_ap_only.groupby(agg_var_in)[['tons_2017', 'value_2017', 'ship_count']].sum()
airport_import_agg = airport_import_agg.reset_index()
airport_import_agg = \
    airport_import_agg.loc[airport_import_agg['value_2017'] > 0]

import_by_airport_by_dest = pd.merge(import_by_airport,
                                  airport_import_agg,
                                  on = ['CFS_CODE', 'CFS_NAME'], how = 'left')

print(len(import_by_airport_by_dest))
import_by_airport_by_dest.loc[:, 'value_2017'] *= import_by_airport_by_dest.loc[:, 'import_frac']
import_by_airport_by_dest.loc[:, 'tons_2017'] *= import_by_airport_by_dest.loc[:, 'import_frac']
import_by_airport_by_dest.loc[:, 'ship_count'] *= import_by_airport_by_dest.loc[:, 'import_frac']

import_by_airport_by_dest = import_by_airport_by_dest.loc[import_by_airport_by_dest['ship_count']  >= 1]
import_by_airport_by_dest.loc[:, 'ship_count'] = np.round(import_by_airport_by_dest.loc[:, 'ship_count'], 0)
print(len(import_by_airport_by_dest))
import_by_airport_by_dest.loc[:, 'value_density'] = \
    import_by_airport_by_dest.loc[:, 'value_2017'] / import_by_airport_by_dest.loc[:, 'tons_2017'] * 1000
import_by_airport_by_dest.loc[:, "TruckLoad"] = import_by_airport_by_dest.loc[:, "tons_2017"] * 1000  / \
            import_by_airport_by_dest.loc[:, "ship_count"] # unit is ton

# $/ton

print('airport import shipment after disaggregation:')
print(import_by_airport_by_dest['ship_count'].sum()) 

print('airport import value (million $) after disaggregation:')
print(import_by_airport_by_dest['value_2017'].sum())    

print('airport import tonnage after disaggregation:')
print(import_by_airport_by_dest['tons_2017'].sum() * 1000)  

# <codecell>

# FAF value grouped by foreign country, domestic destination and sctg for entire region
agg_var_out = ['CFS_CODE', 'CFS_NAME', 'dms_orig', 'SCTG_Code']
airport_export_agg = \
    faf_export_ap_only.groupby(agg_var_out)[['tons_2017', 'value_2017', 'ship_count']].sum()    
airport_export_agg = airport_export_agg.reset_index()
airport_export_agg = \
    airport_export_agg.loc[airport_export_agg['value_2017'] > 0]
    
export_by_airport_by_orig = pd.merge(export_by_airport,
                                  airport_export_agg,
                                  on = ['CFS_CODE', 'CFS_NAME'], how = 'left')
print(len(export_by_airport_by_orig))
export_by_airport_by_orig.loc[:, 'value_2017'] *= export_by_airport_by_orig.loc[:, 'export_frac']
export_by_airport_by_orig.loc[:, 'tons_2017'] *= export_by_airport_by_orig.loc[:, 'export_frac']
export_by_airport_by_orig.loc[:, 'ship_count'] *= export_by_airport_by_orig.loc[:, 'export_frac']
export_by_airport_by_orig = export_by_airport_by_orig.loc[export_by_airport_by_orig['ship_count']  >= 1]
export_by_airport_by_orig.loc[:, 'ship_count'] = np.round(export_by_airport_by_orig.loc[:, 'ship_count'], 0)
print(len(export_by_airport_by_orig))
export_by_airport_by_orig.loc[:, 'value_density'] = \
    export_by_airport_by_orig.loc[:, 'value_2017'] / export_by_airport_by_orig.loc[:, 'tons_2017'] * 1000
export_by_airport_by_orig.loc[:, "TruckLoad"] = export_by_airport_by_orig.loc[:, "tons_2017"] * 1000  / \
            export_by_airport_by_orig.loc[:, "ship_count"] # unit is ton

print('airport export shipment after disaggregation:')
print(export_by_airport_by_orig['ship_count'].sum())  

print('airport export value (million $) after disaggregation:')
print(export_by_airport_by_orig['value_2017'].sum()) 

print('airport export tonnage after disaggregation:')
print(export_by_airport_by_orig['tons_2017'].sum() * 1000)  

# <codecell>


########################################################
#### step 5 - Assign rest of flow to other port ########
########################################################

# adjust import to assign
total_faf_imports = \
    regional_import_by_size.groupby(agg_var_in)[['tons_2017', 'value_2017', 'ship_count']].sum()
total_faf_imports = total_faf_imports.reset_index()
total_faf_imports = total_faf_imports.loc[total_faf_imports['value_2017']> 0]
import_assigned_to_ap = \
    import_by_airport_by_dest.groupby(agg_var_in)[['value_2017']].sum()
import_assigned_to_ap = import_assigned_to_ap.reset_index()
import_assigned_to_ap = \
import_assigned_to_ap.rename(columns = {'value_2017': 'values_airport'})

remain_imports_to_assign = pd.merge(total_faf_imports, import_assigned_to_ap,
                                   on = agg_var_in, how = 'left')
remain_imports_to_assign = remain_imports_to_assign.fillna(0)
remain_imports_to_assign.loc[:, 'value_other'] = \
    remain_imports_to_assign.loc[:, 'value_2017'] - \
        remain_imports_to_assign.loc[:, 'values_airport']

remain_imports_to_assign.loc[:, 'scaling_factor'] = \
    remain_imports_to_assign.loc[:, 'value_other'] / \
        remain_imports_to_assign.loc[:, 'value_2017']
        
remain_imports_to_assign.loc[:, 'ship_count'] *= remain_imports_to_assign.loc[:, 'scaling_factor']
remain_imports_to_assign.loc[:, 'tons_2017'] *= remain_imports_to_assign.loc[:, 'scaling_factor']
remain_imports_to_assign.loc[:, 'value_2017'] *= remain_imports_to_assign.loc[:, 'scaling_factor']

remain_imports_to_assign = remain_imports_to_assign.drop(columns = ['values_airport',
                                                                    'value_other'])        
print(remain_imports_to_assign.loc[:, 'scaling_factor'].min())

# adjust export to assign
total_faf_exports = \
    regional_export_by_size.groupby(agg_var_out)[['tons_2017', 'value_2017', 'ship_count']].sum()
total_faf_exports = total_faf_exports.reset_index()
total_faf_exports = total_faf_exports.loc[total_faf_exports['value_2017']> 0]
export_assigned_to_ap = \
    export_by_airport_by_orig.groupby(agg_var_out)[['value_2017']].sum()
export_assigned_to_ap = export_assigned_to_ap.reset_index()
export_assigned_to_ap = \
export_assigned_to_ap.rename(columns = {'value_2017': 'values_airport'})

remain_exports_to_assign = pd.merge(total_faf_exports, export_assigned_to_ap,
                                   on = agg_var_out, how = 'left')
remain_exports_to_assign = remain_exports_to_assign.fillna(0)
remain_exports_to_assign.loc[:, 'value_other'] = \
    remain_exports_to_assign.loc[:, 'value_2017'] - \
        remain_exports_to_assign.loc[:, 'values_airport']

remain_exports_to_assign.loc[:, 'scaling_factor'] = \
    remain_exports_to_assign.loc[:, 'value_other'] / \
        remain_exports_to_assign.loc[:, 'value_2017']
        
remain_exports_to_assign.loc[:, 'ship_count'] *= remain_exports_to_assign.loc[:, 'scaling_factor']
remain_exports_to_assign.loc[:, 'tons_2017'] *= remain_exports_to_assign.loc[:, 'scaling_factor']
remain_exports_to_assign.loc[:, 'value_2017'] *= remain_exports_to_assign.loc[:, 'scaling_factor']

remain_exports_to_assign = remain_exports_to_assign.drop(columns = ['values_airport',
                                                                    'value_other'])        
print(remain_exports_to_assign.loc[:, 'scaling_factor'].min())
# <codecell>

faf_import_scaling_no_ap = remain_imports_to_assign.groupby(['CFS_CODE', 'CFS_NAME'])['value_2017'].sum()

faf_import_scaling_no_ap = faf_import_scaling_no_ap.reset_index()
faf_import_scaling_no_ap = \
    faf_import_scaling_no_ap.rename(columns = {'value_2017':'other import total'})
    
faf_export_scaling_no_ap = remain_exports_to_assign.groupby(['CFS_CODE', 'CFS_NAME'])['value_2017'].sum()

faf_export_scaling_no_ap = faf_export_scaling_no_ap.reset_index()
faf_export_scaling_no_ap = \
    faf_export_scaling_no_ap.rename(columns = {'value_2017':'other export total'})

# calculating fraction of other port-level value among remaining sctgs

import_by_other_port = pd.merge(import_by_other_port, faf_import_scaling_no_ap,
                          on = ['CFS_CODE', 'CFS_NAME'], how = 'left')
export_by_other_port = pd.merge(export_by_other_port, faf_export_scaling_no_ap,
                          on = ['CFS_CODE', 'CFS_NAME'], how = 'left')

import_by_other_port.loc[:, 'import_frac'] = import_by_other_port.loc[:, 'import value']/ \
    import_by_other_port.loc[:, 'other import total']
    
export_by_other_port.loc[:, 'export_frac'] = export_by_other_port.loc[:, 'export value']/ \
    export_by_other_port.loc[:, 'other export total']
    
import_by_other_port['import_frac'].hist()
print('range of import adj. factors')
print(import_by_other_port['import_frac'].min(), 
      import_by_other_port['import_frac'].max())
export_by_other_port['export_frac'].hist()
print('range of export adj. factors')
print(export_by_other_port['export_frac'].min(), 
      export_by_other_port['export_frac'].max())

# <codecell>
# assign destination to other ports import

import_by_other_port_by_dest = pd.merge(import_by_other_port,
                                  remain_imports_to_assign,
                                  on = ['CFS_CODE', 'CFS_NAME'], how = 'left')

print(len(import_by_other_port_by_dest))
import_by_other_port_by_dest.loc[:, 'value_2017'] *= import_by_other_port_by_dest.loc[:, 'import_frac']
import_by_other_port_by_dest.loc[:, 'tons_2017'] *= import_by_other_port_by_dest.loc[:, 'import_frac']
import_by_other_port_by_dest.loc[:, 'ship_count'] *= import_by_other_port_by_dest.loc[:, 'import_frac']

import_by_other_port_by_dest = import_by_other_port_by_dest.loc[import_by_other_port_by_dest['ship_count']  >= 1]
import_by_other_port_by_dest.loc[:, 'ship_count'] = np.round(import_by_other_port_by_dest.loc[:, 'ship_count'], 0)
print(len(import_by_other_port_by_dest))
import_by_other_port_by_dest.loc[:, 'value_density'] = \
    import_by_other_port_by_dest.loc[:, 'value_2017'] / import_by_other_port_by_dest.loc[:, 'tons_2017'] * 1000
import_by_other_port_by_dest.loc[:, "TruckLoad"] = import_by_other_port_by_dest.loc[:, "tons_2017"] * 1000  / \
            import_by_other_port_by_dest.loc[:, "ship_count"] # unit is ton

# $/ton

print('other import shipment after disaggregation:')
print(import_by_other_port_by_dest['ship_count'].sum()) 

print('other import value (million $) after disaggregation:')
print(import_by_other_port_by_dest['value_2017'].sum())    

print('other import tonnage after disaggregation:')
print(import_by_other_port_by_dest['tons_2017'].sum() * 1000)  

# <codecell>
export_by_other_port_by_orig = pd.merge(export_by_other_port,
                                  remain_exports_to_assign,
                                  on = ['CFS_CODE', 'CFS_NAME'], how = 'left')
print(len(export_by_other_port_by_orig))
export_by_other_port_by_orig.loc[:, 'value_2017'] *= export_by_other_port_by_orig.loc[:, 'export_frac']
export_by_other_port_by_orig.loc[:, 'tons_2017'] *= export_by_other_port_by_orig.loc[:, 'export_frac']
export_by_other_port_by_orig.loc[:, 'ship_count'] *= export_by_other_port_by_orig.loc[:, 'export_frac']
export_by_other_port_by_orig = export_by_other_port_by_orig.loc[export_by_other_port_by_orig['ship_count']  >= 1]
export_by_other_port_by_orig.loc[:, 'ship_count'] = np.round(export_by_other_port_by_orig.loc[:, 'ship_count'], 0)
print(len(export_by_other_port_by_orig))
export_by_other_port_by_orig.loc[:, 'value_density'] = \
    export_by_other_port_by_orig.loc[:, 'value_2017'] / export_by_other_port_by_orig.loc[:, 'tons_2017'] * 1000
export_by_other_port_by_orig.loc[:, "TruckLoad"] = export_by_other_port_by_orig.loc[:, "tons_2017"] * 1000  / \
            export_by_other_port_by_orig.loc[:, "ship_count"] # unit is ton

print('other export shipment after disaggregation:')
print(export_by_other_port_by_orig['ship_count'].sum())  

print('other export value (million $) after disaggregation:')
print(export_by_other_port_by_orig['value_2017'].sum()) 

print('other export tonnage after disaggregation:')
print(export_by_other_port_by_orig['tons_2017'].sum() * 1000)  

# <codecell>
# import_count_by_sctg = \
#     import_by_shipment_size.groupby('SCTG_Code')[["ship_count"]].sum()
# import_count_by_sctg = import_count_by_sctg.reset_index()


# export_count_by_sctg = \
#     export_by_shipment_size.groupby('SCTG_Code')[["ship_count"]].sum()
# export_count_by_sctg = export_count_by_sctg.reset_index()

# <codecell>

import_attr = ['PORTID', 'CBP Port Location', 'FAF', 'CBPZONE', 'MESOZONE', 'TYPE', 
               'is_airport', 'CFS_CODE', 'CFS_NAME', 'dms_dest', 'SCTG_Code', 
               'TruckLoad', 'ship_count', 'value_2017', 'value_density']
export_attr = ['PORTID', 'CBP Port Location', 'FAF', 'CBPZONE', 'MESOZONE', 'TYPE', 
               'is_airport', 'CFS_CODE', 'CFS_NAME','dms_orig', 'SCTG_Code', 
               'TruckLoad', 'ship_count', 'value_2017', 'value_density']

import_output_airport = import_by_airport_by_dest[import_attr]
import_output_other_port = import_by_other_port_by_dest[import_attr]

import_by_port_by_dest = pd.concat([import_output_airport,
                                    import_output_other_port])

import_by_port_by_dest = pd.merge(import_by_port_by_dest,
                                   sctg_lookup_short,
                                   on = ['SCTG_Code'], how = 'left')

output_path = os.path.join(file_path, output_dir)

import_by_port_by_dest.to_csv(os.path.join(output_path, 'import_od.csv'), index = False)

export_output_airport = export_by_airport_by_orig[export_attr]
export_output_other_port = export_by_other_port_by_orig[export_attr]

export_by_port_by_orig = pd.concat([export_output_airport,
                                    export_output_other_port])

export_by_port_by_orig = pd.merge(export_by_port_by_orig,
                                   sctg_lookup_short,
                                   on = ['SCTG_Code'], how = 'left')

export_by_port_by_orig.to_csv(os.path.join(output_path, 'export_od.csv'), index = False)

print('Total import shipments after scaling:')
print(import_by_port_by_dest.loc[:, "ship_count"].sum())

print('Total export shipments after scaling:')
print(export_by_port_by_orig.loc[:, "ship_count"].sum())