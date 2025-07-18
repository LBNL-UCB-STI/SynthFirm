#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 17 10:22:01 2025

@author: xiaodanxu
"""

import os
from pandas import read_csv
import pandas as pd
import geopandas as gps
import matplotlib.pyplot as plt
import seaborn as sns
import visualkit as vk

import warnings
warnings.filterwarnings('ignore')

# <codecell>
os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')

scenario_name = 'Seattle'
out_scenario_name = 'Seattle'
param_dir = 'SynthFirm_parameters'
input_dir = 'inputs_' + scenario_name
output_dir = 'outputs_' + out_scenario_name
plot_dir = 'plots_' + out_scenario_name
validation_dir = 'Validation'

analysis_year = 2017

# mapping FAF mode to SynthFirm mode
faf_mode_lookup = {1: 'Truck', 2: 'Rail', 3: 'Other', 4: 'Air', 
               5: 'Parcel', 6: 'Other', 7: 'Other', 8: 'Other'}

CFS_to_FAF_mapping = {'For-hire Truck':'Truck', 
                      'Private Truck': 'Truck', 
                      'Rail/IMX': 'Rail', 
                      'Air':'Air', 
                      'Parcel':'Parcel'}
#define scenario input
us_ton_to_ton = 0.907185
miles_to_km = 1.60934
shipment_load_attr = 'tons_' + str(analysis_year)
shipment_tonmile_attr = 'tmiles_' + str(analysis_year)

region_code = [411, 531, 532, 539]
focus_region = 531

FAF_mode_mapping = {'Truck':['For-hire Truck', 'Private Truck'], 
                    'Rail':['Rail/IMX'], 
                    'Air': ['Air'], 'Parcel':['Parcel']}



# <codecell>

#load modeled, FAF and CFS results
faf_data = read_csv(os.path.join(validation_dir, 'FAF5.3.csv'), sep = ',')
cfs_data = read_csv(os.path.join(validation_dir,'CFS2017_stats_by_zone.csv'), sep = ',')
modeled_data = read_csv(os.path.join(output_dir, 'processed_b2b_flow_summary.csv'), sep = ',')
modeled_data = modeled_data.loc[modeled_data['mode_choice'] != 'Other']

#load parameters
sctg_group_lookup = read_csv(os.path.join(param_dir, 'SCTG_Groups_revised_V2.csv'), sep = ',')
mesozone_lookup = read_csv(os.path.join(input_dir, 'zonal_id_lookup_final.csv'), sep = ',')
sctg_names = sctg_group_lookup['SCTG_Name'].unique()
sctg_group_definition = sctg_group_lookup.loc[:, ['SCTG_Group', 'SCTG_Name']]
sctg_group_definition = sctg_group_definition.drop_duplicates()

faf_data = pd.merge(faf_data, sctg_group_lookup, left_on = 'sctg2', right_on = 'SCTG_Code', how = 'left')

# <codecell>
# harmonize variable name
faf_data.rename(columns = {shipment_load_attr: 'Load',
                           shipment_tonmile_attr: 'Tonmiles',
                           'SCTG_Name': 'Commodity',
                           'dms_orig': 'Origin',
                           'dms_dest': 'Destination'}, inplace = True)

cfs_data.rename(columns = {'wgted_wght_ton_th': 'Load',
                           'tonmile': 'Tonmiles',
                           'SHIPMT_DIST_ROUTED': 'Distance',
                           'commodity': 'Commodity',
                           'mode_agg5': 'Mode Choice',
                           'ORIG_FAFID': 'Origin', 
                           'DEST_FAFID': 'Destination'}, inplace = True)

modeled_data.rename(columns = {'ShipmentLoad': 'Load',
                           shipment_tonmile_attr: 'Tonmiles',
                           'SCTG_Name': 'Commodity',
                           'mode_choice': 'Mode Choice', 
                           'orig_FAFID': 'Origin',
                           'orig_FAFNAME': 'Origin Zone',
                           'dest_FAFID': 'Destination',
                           'dest_FAFNAME': 'Destination Zone'}, inplace = True)

# <codecell>

# process data and generate 4-alt mode before validation
faf_data.loc[:, 'Mode'] = faf_data.loc[:, 'dms_mode'].map(faf_mode_lookup)

faf_data.loc[:, 'Distance'] = 1000 * faf_data.loc[:, 'Tonmiles'] / faf_data.loc[:, 'Load']
faf_data.loc[:, 'Tonmiles'] *= 1000000 
faf_data = faf_data.dropna(subset = ['Distance'])
trade_type_id = 1 # domestic only
faf_data_domestic = faf_data.loc[faf_data['trade_type'] == trade_type_id] #select domestic shipment only
faf_data_domestic = \
faf_data_domestic.loc[faf_data_domestic['Mode'] != 'Other']
faf_data_domestic = \
faf_data_domestic.loc[faf_data_domestic['Commodity'] != 'other']

cfs_data.loc[:, 'Mode'] = cfs_data.loc[:, 'Mode Choice'].map(CFS_to_FAF_mapping)


#getting directional flow for validation data
if region_code is not None: # regional
    faf_outflow = faf_data_domestic.loc[faf_data_domestic['Origin'].isin(region_code)]
    faf_inflow = faf_data_domestic.loc[faf_data_domestic['Destination'].isin(region_code)]
    cfs_outflow = cfs_data.loc[cfs_data['Origin'].isin(region_code)] 
    cfs_inflow = cfs_data.loc[cfs_data['Destination'].isin(region_code)] 
else: # national
    faf_outflow = faf_data_domestic.copy()
    faf_inflow = faf_data_domestic.copy()
    cfs_outflow = cfs_data.copy()
    cfs_inflow = cfs_data.copy()
    
# clean modeled data
modeled_data.loc[:, 'Mode'] = \
    modeled_data.loc[:, 'Mode Choice'].map(CFS_to_FAF_mapping)
modeled_data = modeled_data.loc[modeled_data['Commodity'] != 'other']
modeled_outflow = modeled_data.loc[modeled_data['outbound'] == 1]
modeled_inflow = modeled_data.loc[modeled_data['inbound'] == 1]

# <codecell>

# calculate summary statistics for the entire modeled region
in_region = (faf_data_domestic['Origin'].isin(region_code) | \
             faf_data_domestic['Destination'].isin(region_code))
faf_region = faf_data_domestic.loc[in_region]

faf_regional_total = faf_region.loc[:, 'Load'].sum() * us_ton_to_ton
print('Total regional load from FAF (in 1000 metric ton):')
print(faf_regional_total)

model_regional_total = modeled_data.loc[:, 'Load'].sum() * us_ton_to_ton
print('Total regional load from SynthFirm (in 1000 metric ton):')
print(model_regional_total)

# truck only
faf_region_truck = faf_region.loc[faf_region['Mode'] == 'Truck']
faf_region_truck_total = faf_region_truck.loc[:, 'Load'].sum() * us_ton_to_ton
print('Total regional truck load from FAF (in 1000 metric ton):')
print(faf_region_truck_total)

modeled_data_truck = \
modeled_data.loc[modeled_data['Mode'] == 'Truck']
modeled_data_truck_total = modeled_data_truck.loc[:, 'Load'].sum() * us_ton_to_ton
print('Total regional truck load from SynthFirm (in 1000 metric ton):')
print(modeled_data_truck_total)

# <codecell>
if focus_region is not None:
    in_zone = ((faf_data_domestic['Origin'] == focus_region) | \
                 (faf_data_domestic['Destination'] == focus_region))
    faf_load_in_zone = faf_data_domestic.loc[in_zone]
    faf_zone_total = faf_load_in_zone.loc[:, 'Load'].sum() * us_ton_to_ton
    print('Total load from FAF zone ' + str(focus_region))
    print(faf_zone_total)
    
    model_in_zone = ((modeled_data['Origin'] == focus_region) | \
                 (modeled_data['Destination'] == focus_region))
    modeled_data_in_zone = modeled_data[model_in_zone]
    model_zone_total = modeled_data_in_zone.loc[:, 'Load'].sum() * us_ton_to_ton
    print('Total load from SynthFirm zone ' + str(focus_region))
    print(model_zone_total)
    
    # truck only
    faf_zone_truck = faf_load_in_zone.loc[faf_load_in_zone['Mode'] == 'Truck']
    faf_zone_truck_total = faf_zone_truck.loc[:, 'Load'].sum() * us_ton_to_ton
    print('Total truck load from FAF zone ' + str(focus_region))
    print(faf_zone_truck_total)
    
    modeled_zone_truck = \
    modeled_data_in_zone.loc[modeled_data_in_zone['Mode'] == 'Truck']
    model_zone_truck_total = modeled_zone_truck.loc[:, 'Load'].sum() * us_ton_to_ton
    print('Total truck load from SynthFirm zone ' + str(focus_region))
    print(model_zone_truck_total)

# <codecell>

summary_statistics = {}
def summary_statistics_generator(data, tonmile_unit_factor = 1000000, shipment_load_unit_factor = 1000, 
                                 distance_var = 'Distance', load_var = 'weight', tonmile_var = 'tmiles'):
    mean_distance = (data[tonmile_var].sum() * tonmile_unit_factor) / \
    (data[load_var].sum() * shipment_load_unit_factor)
    max_distance = data.loc[data[load_var]>0, distance_var].max()
    min_distance = data.loc[data[load_var]>0, distance_var].min()
    data = data.sort_values(distance_var)
    data['weight'] = data[load_var].cumsum()
    cutoff = data[load_var].sum() / 2.0
    median_distance = data.loc[data['weight'] >= cutoff, distance_var].min()
    total_shipment_load = shipment_load_unit_factor * data[load_var].sum() # tons
    return(min_distance, max_distance, mean_distance, median_distance, total_shipment_load)

  

mode_choice_5alt = modeled_data['Mode Choice'].unique()
mode_choice_4alt = set(CFS_to_FAF_mapping.values())


# production
modeled_statistics_mode = {}
filename = os.path.join(plot_dir, 'outflow_distance_comparison_allmode.png')
vk.plot_distance_kde(faf_outflow, 
                  cfs_outflow, 
                  modeled_outflow, 
                  'All', filename)
# individual plot by mode
for mode in mode_choice_4alt:
    print(mode)
       
    cfs_data_to_describe = \
    cfs_outflow.loc[cfs_outflow['Mode'] == mode]
    cfs_data_to_describe.loc[:, 'Distance'] *= miles_to_km
    
    modeled_data_to_describe = \
    modeled_outflow.loc[modeled_outflow['Mode'] == mode]
    modeled_data_to_describe.loc[:, 'Distance'] *= miles_to_km
    
    faf_data_to_describe = \
        faf_outflow.loc[faf_outflow['Mode'] == mode]
    faf_data_to_describe.loc[:, 'Distance'] *= miles_to_km

    filename = os.path.join(plot_dir, 'outflow_distance_comparison_for ' + mode + '.png')
    vk.plot_distance_kde(faf_data_to_describe, 
                      cfs_data_to_describe, 
                      modeled_data_to_describe, 
                          mode, filename)
    
# attraction

filename = os.path.join(plot_dir, 'inflow_distance_comparison_allmode.png')
vk.plot_distance_kde(faf_inflow, 
                  cfs_inflow, 
                  modeled_inflow, 
                  'All', filename)
# individual plot by mode
for mode in mode_choice_4alt:
    print(mode)

    cfs_data_to_describe = cfs_inflow.loc[cfs_inflow['Mode'] == mode]
    modeled_data_to_describe = modeled_inflow.loc[modeled_inflow['Mode'] == mode]
    faf_data_to_describe = faf_inflow.loc[faf_inflow['Mode'] == mode]
    faf_data_to_describe.loc[:, 'Distance'] *= miles_to_km
    cfs_data_to_describe.loc[:, 'Distance'] *= miles_to_km
    modeled_data_to_describe.loc[:, 'Distance'] *= miles_to_km   
    
    filename = os.path.join(plot_dir, 'inflow_distance_comparison_for ' + mode + '.png')
    vk.plot_distance_kde(faf_data_to_describe, 
                      cfs_data_to_describe, 
                      modeled_data_to_describe, 
                          mode, filename)
    
# <codecell>

# tonnage by SCTG group and mode

faf_outflow['Load'] *= us_ton_to_ton
cfs_outflow['Load'] *= us_ton_to_ton
modeled_outflow['Load'] *= us_ton_to_ton

faf_inflow['Load'] *= us_ton_to_ton
cfs_inflow['Load'] *= us_ton_to_ton
modeled_inflow['Load'] *= us_ton_to_ton

# validation by SCTG
filename = os.path.join(plot_dir, 'outflow_shipment_comparison_by_sctg.png')
vk.plot_shipment_by_sector_bar(faf_outflow, cfs_outflow,  modeled_outflow,
   'Commodity', 'Load', '1000 tons', filename)

filename = os.path.join(plot_dir, 'inflow_shipment_comparison_by_sctg.png')
vk.plot_shipment_by_sector_bar(faf_inflow, cfs_inflow,  modeled_inflow,
    'Commodity', 'Load', '1000 tons', filename)

# validation by MODE
filename = os.path.join(plot_dir, 'outflow_shipment_comparison_by_mode.png')
vk.plot_shipment_by_sector_bar(faf_outflow, cfs_outflow,  modeled_outflow,
   'Mode', 'Load', '1000 tons', filename)

filename = os.path.join(plot_dir, 'inflow_shipment_comparison_by_mode.png')
vk.plot_shipment_by_sector_bar(faf_inflow, cfs_inflow,  modeled_inflow,
    'Mode', 'Load', '1000 tons', filename)

# <codecell>

# mode split by 5 mode


# mode split validation by 5 MODE
filename = os.path.join(plot_dir, 'outflow_mode_split_by_5mode.png')
vk.plot_shipment_by_5mode_bar(cfs_outflow,  modeled_outflow,
   'Mode Choice', 'Load', '%', filename)

filename = os.path.join(plot_dir, 'inflow_mode_split_by_5mode.png')
vk.plot_shipment_by_5mode_bar(cfs_inflow,  modeled_inflow,
    'Mode Choice', 'Load', '%', filename)

# <codecell>

# bar plot for shipment by zone  -- better for regional study

if region_code is not None:
    
    # shipment attributes by zone
    filename = os.path.join(plot_dir, 'outflow_shipment_by_zone.png')
    vk.plot_shipment_comparison_by_zone(faf_outflow, cfs_outflow, 
    modeled_outflow, 'Origin', 'Origin Zone', 'Load', '1000 ton',
    filename)

    filename = os.path.join(plot_dir, 'inflow_shipment_by_zone.png')
    vk.plot_shipment_comparison_by_zone(faf_inflow, cfs_inflow, 
    modeled_inflow, 'Destination', 'Destination Zone', 'Load', '1000 ton',
    filename)
    
# <codecell>

# shipment by top origin/destination


# validation by TOP Origin
nzones = 10
filename = os.path.join(plot_dir, 'percent_outflow_shipment_comparison.png')
vk.plot_top_OD_bar(faf_outflow, cfs_outflow,  modeled_outflow,
   nzones, 'Destination', 'Destination Zone', 'Load', '%', filename)

filename = os.path.join(plot_dir, 'percent_inflow_shipment_comparison.png')
vk.plot_top_OD_bar(faf_inflow, cfs_inflow,  modeled_inflow,
    nzones, 'Origin', 'Origin Zone', 'Load', '%', filename)

# validation by TOP Destination

