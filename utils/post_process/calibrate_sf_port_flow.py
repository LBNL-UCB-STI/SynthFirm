#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 24 11:52:47 2025

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

############### Bay Area International Flow Calibration ##################
## re-distribute commodity among SF, Richmond, Redwood City and Oakland ##
##########################################################################

file_path = '/Users/xiaodanxu/Documents/SynthFirm.nosync'
scenario_name = 'BayArea'
out_scenario_name = 'BayArea'
input_dir = 'inputs_' + scenario_name
output_dir = 'outputs_' + out_scenario_name
us_ton_to_ton = 0.907185

input_path = os.path.join(file_path, input_dir)
output_path = os.path.join(file_path, output_dir)

port_location_file = 'port_location_in_region.csv'
export_with_firm_file = 'export_OD_with_seller.csv'
import_with_firm_file = 'import_OD_with_buyer.csv'
port_commodity_calibration_file = 'port_cargo_calibration.xlsx'


import_flow = read_csv(os.path.join(output_path, 'international', import_with_firm_file))
export_flow = read_csv(os.path.join(output_path, 'international', export_with_firm_file))

port_location = read_csv(os.path.join(input_path, 'port', port_location_file))
import_calibrators = \
pd.read_excel(os.path.join(input_path, 'port', port_commodity_calibration_file), 
              sheet_name='import')

# load calibration parameters
import_calibrators = import_calibrators[['SCTG_Code', 'CBP Port Location', 'Fraction']]
import_calibrators.rename(columns = {'SCTG_Code': 'Commodity_SCTG'}, inplace = True)
export_calibrators = \
pd.read_excel(os.path.join(input_path, 'port', port_commodity_calibration_file),
              sheet_name='export')
export_calibrators = export_calibrators[['SCTG_Code', 'CBP Port Location', 'Fraction']]
export_calibrators.rename(columns = {'SCTG_Code': 'Commodity_SCTG'}, inplace = True)

import_flow.loc[:, 'total_load'] = \
import_flow.loc[:, 'TruckLoad'] * \
import_flow.loc[:, 'shipments'] / 1000 

export_flow.loc[:, 'total_load'] = \
export_flow.loc[:, 'TruckLoad'] * \
export_flow.loc[:, 'shipments'] / 1000 

print('total import load:')
print(import_flow.loc[:, 'total_load'].sum())

print('total export load:')
print(export_flow.loc[:, 'total_load'].sum())

# <codecell>

# data pre-processing
port_attr = ['PORTID', 'CBP Port Location', 'FAF', 'CBPZONE', 'MESOZONE', 
             'TYPE', 'is_airport']

port_location = port_location[port_attr]
port_location.rename(columns = {'MESOZONE': 'PORTZONE'}, inplace = True)
import_calibrators = pd.merge(import_calibrators, port_location, on = 'CBP Port Location')
export_calibrators = pd.merge(export_calibrators, port_location, on = 'CBP Port Location')

import_set = import_calibrators['CBP Port Location'].unique()
export_set = export_calibrators['CBP Port Location'].unique()

import_flow_to_cal = import_flow.loc[import_flow['CBP Port Location'].isin(import_set)]
import_flow_no_cal = import_flow.loc[~import_flow['CBP Port Location'].isin(import_set)]

export_flow_to_cal = export_flow.loc[export_flow['CBP Port Location'].isin(export_set)]
export_flow_no_cal = export_flow.loc[~export_flow['CBP Port Location'].isin(export_set)]

print('total import load to assign:')
print(import_flow_to_cal.loc[:, 'total_load'].sum())

print('total export load to assign:')
print(export_flow_to_cal.loc[:, 'total_load'].sum())


import_tonnage_by_port = \
import_flow_to_cal.groupby(['CBP Port Location'])[['total_load']].sum()

export_tonnage_by_port = \
export_flow_to_cal.groupby(['CBP Port Location'])[['total_load']].sum()

import_tonnage_by_port.rename(columns = {'total_load': 'import'}, inplace = True)
export_tonnage_by_port.rename(columns = {'total_load': 'export'}, inplace = True)

tonnage_by_port = pd.merge(import_tonnage_by_port, export_tonnage_by_port,
                           on = 'CBP Port Location', how = 'left')
# tonnage_by_port = tonnage_by_port.set_index('PORTID')
tonnage_by_port.plot(kind = 'bar', stacked = True)

# <codecell>

# calibrate import
port_attr_drop = ['PORTID', 'CBP Port Location', 'FAF', 'CBPZONE', 'PORTZONE', 
             'TYPE', 'is_airport']

essential_var = import_flow.columns
# adjust import flows
import_flow_to_cal.drop(columns = port_attr_drop, inplace = True)

import_flow_to_cal = pd.merge(import_flow_to_cal, import_calibrators,
                              on = ['Commodity_SCTG'], how = 'left')


essential_var = ['CFS_CODE', 'CFS_NAME', 'dms_dest', 'Commodity_SCTG', 'TruckLoad',
       'SCTG_Group', 'mode_choice', 'shipments', 'Distance', 'bundle_id',
       'BuyerID', 'BuyerZone', 'BuyerNAICS', 'total_load']
import_flow_to_cal = import_flow_to_cal.groupby(essential_var).sample(n = 1, 
                                                         weights = import_flow_to_cal['Fraction'],
                                                         replace = True, 
                                                         random_state = 1)



import_tonnage_by_port = \
import_flow_to_cal.groupby(['CBP Port Location'])[['total_load']].sum()
print(import_tonnage_by_port.total_load.sum())
import_tonnage_by_port.plot(kind = 'bar')

# <codecell>
# calibrate export
port_attr_drop = ['PORTID', 'CBP Port Location', 'FAF', 'CBPZONE', 'PORTZONE', 
             'TYPE', 'is_airport']

essential_var = export_flow.columns
# adjust import flows
export_flow_to_cal.drop(columns = port_attr_drop, inplace = True)

export_flow_to_cal = pd.merge(export_flow_to_cal, export_calibrators,
                              on = ['Commodity_SCTG'], how = 'left')


essential_var = ['CFS_CODE', 'CFS_NAME', 'dms_orig', 'Commodity_SCTG',
'TruckLoad', 'SCTG_Group', 'mode_choice', 'shipments', 'Distance',
'bundle_id', 'SellerID', 'SellerZone', 'SellerNAICS', 'total_load']
export_flow_to_cal = export_flow_to_cal.groupby(essential_var).sample(n = 1, 
                                                         weights = export_flow_to_cal['Fraction'],
                                                         replace = True, 
                                                         random_state = 1)
# us_ton_to_ton = 0.907185


export_tonnage_by_port = \
export_flow_to_cal.groupby(['CBP Port Location'])[['total_load']].sum()
print(export_tonnage_by_port.total_load.sum())
export_tonnage_by_port.plot(kind = 'bar')

# <codecell>
import_tonnage_by_port.rename(columns = {'total_load': 'import'}, inplace = True)
export_tonnage_by_port.rename(columns = {'total_load': 'export'}, inplace = True)

tonnage_by_port = pd.merge(import_tonnage_by_port, export_tonnage_by_port,
                           on = 'CBP Port Location', how = 'left')
# tonnage_by_port = tonnage_by_port.set_index('PORTID')
tonnage_by_port.plot(kind = 'bar', stacked = True)

# writing
# <codecell>
# writing output

import_flow_calib = pd.concat([import_flow_to_cal, import_flow_no_cal])
export_flow_calib = pd.concat([export_flow_to_cal, export_flow_no_cal])

print('total import load:')
print(import_flow_calib.loc[:, 'total_load'].sum())

print('total export load:')
print(export_flow_calib.loc[:, 'total_load'].sum())


import_flow_calib.drop(columns = ['total_load', 'Fraction'], inplace = True)
export_flow_calib.drop(columns = ['total_load', 'Fraction'], inplace = True)

import_flow_calib.to_csv(os.path.join(output_path, 'international', 
                                      'calibrated_' + import_with_firm_file), index = False)

export_flow_calib.to_csv(os.path.join(output_path, 'international', 
                                      'calibrated_' + export_with_firm_file), index = False)

