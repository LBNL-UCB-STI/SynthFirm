#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 11 14:29:51 2025

@author: xiaodanxu
"""
# set environment and import packages
import os
from pandas import read_csv
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import shapely.wkt
import geopandas as gpd
import contextily as cx
import warnings
from shapely.geometry import Point

warnings.filterwarnings("ignore")

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')

'''
Notes:
    >FAF data is used as spatial allocation factor, and could vary by year
    >USATO data provide base year flow by port, provide benchmark values, 
     and will be projected for future years use scenario-specific inputs.
     Therefore, the input USATO value does not vary by year.
'''
# mapping FAF mode to SynthFirm mode
mode_lookup = {1: 'Truck', 2: 'Rail', 3: 'Other', 4: 'Air', 
               5: 'Parcel', 6: 'Other', 7: 'Other', 8: 'Other'}

#define scenario input
analysis_year = 2017 # for FAF inputs
us_ton_to_ton = 0.907185
miles_to_km = 1.60934
shipment_load_attr = 'tons_' + str(analysis_year)
shipment_tonmile_attr = 'tmiles_' + str(analysis_year)
shipment_value_attr = 'value_' + str(analysis_year)
region_name = 'BayArea'
out_scenario_name = 'BayArea'
# region_code = [531, 532, 539, 411] # seattle
region_code = [62, 64, 65, 69] #Bay Area

path_to_write = 'inputs_' + region_name # update this based on analysis region 
path_to_plot = 'plots_' + out_scenario_name # update this based on analysis region
faf_data = read_csv('Validation/' + 'FAF5.6.1.csv', sep = ',')
faf_zone_file = 'Validation/FAF5Zones.geojson'
faf_zone = gpd.read_file(faf_zone_file)
# faf_zone.plot()

crosswalk_file = 'SynthFirm_parameters/international_trade_zone_lookup.csv'
international_zonal_crosswalk = read_csv(crosswalk_file)


col_to_keep = ['fr_orig', 'dms_orig', 'dms_dest', 'fr_dest', 'fr_inmode', 'dms_mode',
       'fr_outmode', 'sctg2', 'trade_type', 'dist_band', shipment_load_attr,
               shipment_value_attr, shipment_tonmile_attr]
faf_data = faf_data[col_to_keep]
faf_val_by_trade_type = \
faf_data.groupby('trade_type')[[shipment_load_attr,
        shipment_value_attr, shipment_tonmile_attr]].sum()
print(faf_val_by_trade_type)

faf_data = faf_data.loc[faf_data['trade_type'] != 1]

# <codecell>
# process USATO data
# load USATO data
port_file = 'RawData/Port/port_code_cbp.csv'
list_of_ports = read_csv(port_file)

port_loc_file = 'SynthFirm_parameters/port_location_CA_WA_OR_MA.geojson'
port_locations = gpd.read_file(port_loc_file)

import_flow_file = 'RawData/Port/Flow/SF and Seattle Port-level Imports (2017)_with OR.csv'
regional_import_flow = read_csv(import_flow_file)

export_flow_file = 'RawData/Port/Flow/SF and Seattle Port-level Exports (2017)_with OR.csv'
regional_export_flow = read_csv(export_flow_file)

study_region_definition = region_name + '_freight.geojson'
study_region = gpd.read_file(os.path.join(path_to_write, study_region_definition))



# <codecell>
# generate import/export flows from FAF

faf_crosswalk = \
international_zonal_crosswalk[['FAFID',	'CFS_CODE','CFS_NAME']]

faf_crosswalk = faf_crosswalk.drop_duplicates()

# FAF import attributes  
regional_import = faf_data.loc[faf_data['trade_type'] == 2]
regional_import = \
regional_import.loc[regional_import['dms_orig'].isin(region_code)]

regional_import['fr_orig'] = regional_import['fr_orig'].astype(int)

faf_crosswalk['FAFID'] = faf_crosswalk['FAFID'].astype(int)

regional_import = pd.merge(regional_import, faf_crosswalk,
                           left_on = 'fr_orig', 
                           right_on = 'FAFID', how = 'left')
regional_import = regional_import.drop(columns = 'FAFID')
regional_import.loc[:, 'distance'] = \
1000 * regional_import.loc[:, shipment_tonmile_attr] / \
    regional_import.loc[:, shipment_load_attr]

print('total import within selected region (load, value, tmile):')
print(regional_import[[shipment_load_attr,
        shipment_value_attr, shipment_tonmile_attr]].sum())


# FAF export attributes  
regional_export = faf_data.loc[faf_data['trade_type'] == 3]
regional_export = \
regional_export.loc[regional_export['dms_dest'].isin(region_code)]

regional_export['dms_dest'] = regional_export['dms_dest'].astype(int)

regional_export = pd.merge(regional_export, faf_crosswalk,
                           left_on = 'fr_dest', 
                           right_on = 'FAFID', how = 'left')
regional_export = regional_export.drop(columns = 'FAFID')

regional_export.loc[:, 'distance'] = \
1000 * regional_export.loc[:, shipment_tonmile_attr] / \
    regional_export.loc[:, shipment_load_attr]
print('total export within selected region (load, value, tmile):')
print(regional_export[[shipment_load_attr,
        shipment_value_attr, shipment_tonmile_attr]].sum())

# <codecell>
# create mode split results for validation
mode_lookup = {1: 'Truck', 2: 'Rail', 3: 'Other', 4: 'Air', 
               5: 'Parcel', 6: 'Other', 7: 'Other', 8: 'Other'}
regional_import.loc[:, 'mode_def'] = \
regional_import.loc[:, 'dms_mode'].map(mode_lookup)

regional_export.loc[:, 'mode_def'] = \
regional_export.loc[:, 'dms_mode'].map(mode_lookup)

# mode split by tonnage
import_mode_split = \
regional_import.groupby('mode_def')[[shipment_load_attr]].sum()
import_mode_split = import_mode_split.reset_index()
import_mode_split.loc[:, 'direction'] = 'import'
export_mode_split = \
regional_export.groupby('mode_def')[[shipment_load_attr]].sum()
export_mode_split = export_mode_split.reset_index()
export_mode_split.loc[:, 'direction'] = 'export'
mode_split_faf = pd.concat([import_mode_split, export_mode_split] )

mode_split_faf.loc[:, 'fraction'] = \
mode_split_faf.loc[:, shipment_load_attr] / \
mode_split_faf.groupby(['direction'])[shipment_load_attr].transform('sum')
mode_split_faf.to_csv(os.path.join(path_to_write, 'port', 
                                   'FAF_mode_split_' + str(analysis_year)+'.csv'),
                      index = False)

# <codecell>
# write to SynthFirm inputs
regional_import.to_csv(os.path.join(path_to_write, 'port', 
                                    'FAF_regional_import_'+ str(analysis_year)+'.csv'),
                      index = False)

regional_export.to_csv(os.path.join(path_to_write, 'port', 
                                    'FAF_regional_export_'+ str(analysis_year)+'.csv'),
                      index = False)



# <codecell>

# processing port shapefile/boundary

list_of_ports.loc[:, 'NAME'] = \
list_of_ports.loc[:, 'CBP Port Location'].str.split('(').str[0]
list_of_ports.loc[:, 'NAME'] = \
list_of_ports.loc[:, 'NAME'].str[:-1]

port_locations_with_id = pd.merge(port_locations,
                                  list_of_ports,
                                  on = 'NAME',
                                  how = 'left')
print(len(port_locations_with_id))

port_locations_with_id_df = port_locations_with_id.drop(columns = 'geometry')
port_locations_centroid = port_locations_with_id["geometry"].centroid
port_locations_centroid = \
gpd.GeoDataFrame(geometry = gpd.GeoSeries(port_locations_centroid))
port_locations_centroid = pd.concat([port_locations_centroid, 
                               port_locations_with_id_df], axis = 1)

faf_zone = faf_zone.to_crs('EPSG:4326')

# assign port to faf zone
port_in_faf = \
port_locations_centroid.sjoin_nearest(faf_zone, how="left")

port_in_faf['FAF'] = port_in_faf['FAF'].astype(int)
port_in_faf.drop(columns = ['index_right'], inplace = True)

# select port in region
port_in_region = \
port_in_faf.loc[port_in_faf['FAF'].isin(region_code)]
study_region = study_region.to_crs('EPSG:4326')

# If selected Bay Area, adjusting location for SF port
if region_name == 'BayArea':
    sf_port_coord = Point(-122.385773, 37.745646)
    port_in_region.loc[port_in_region['NAME'] == 'San Francisco, CA',
    "geometry"] = sf_port_coord

port_in_region = \
port_in_region.sjoin_nearest(study_region, how="left")
port_in_region.drop(columns = ['index_right'], inplace = True)
port_in_region[['lat', 'lon']] = \
    port_in_region.apply(lambda p: (p.geometry.y, p.geometry.x), axis=1, 
                        result_type='expand')
    
port_in_region_df = port_in_region.drop(columns = 'geometry')



port_in_region.to_file(os.path.join(path_to_write, 'Port/port_location_in_region.geojson'),
                   driver="GeoJSON")

port_in_region_df.to_csv(os.path.join(path_to_write, 'Port/port_location_in_region.csv'),
                   index = False)

# <codecell>
# select USATO port and flow in study region, and generete attributes

international_end_lookup = \
international_zonal_crosswalk[['Country', 'CFS_CODE', 'CFS_NAME']]

international_end_lookup = \
international_end_lookup.drop_duplicates(subset= 'Country')


# parsing import
regional_import_flow['Customs Value (Gen) ($US)'] = \
regional_import_flow['Customs Value (Gen) ($US)'].astype(float)

regional_import_flow.loc[:, 'HS Code'] = \
regional_import_flow.loc[:, 'Commodity'].str.split(' ').str[0]

regional_import_flow.loc[:, 'HS Code'] = \
regional_import_flow.loc[:, 'HS Code'].astype(int)

regional_import_flow = pd.merge(regional_import_flow,
                               international_end_lookup,
                               on = 'Country', how = 'left')

# parsing export
regional_export_flow['Total Exports Value ($US)'] = \
regional_export_flow['Total Exports Value ($US)'].astype(float)

regional_export_flow.loc[:, 'HS Code'] = \
regional_export_flow.loc[:, 'Commodity'].str.split(' ').str[0]

regional_export_flow.loc[:, 'HS Code'] = \
regional_export_flow.loc[:, 'HS Code'].astype(int)

regional_export_flow = pd.merge(regional_export_flow,
                               international_end_lookup,
                               on = 'Country', how = 'left')

usato_import_in_region = pd.merge(port_in_region_df,
                                  regional_import_flow,
                                  left_on = 'CBP Port Location',
                                  right_on = 'Port',
                                  how = 'left')

print('total import in region:')
print(usato_import_in_region['Customs Value (Gen) ($US)'].sum()/10**6)

usato_export_in_region = pd.merge(port_in_region_df,
                                  regional_export_flow,
                                  left_on = 'CBP Port Location',
                                  right_on = 'Port',
                                  how = 'left')

print('total export in region:')
print(usato_export_in_region['Total Exports Value ($US)'].sum()/10**6)

usato_import_in_region.to_csv(os.path.join(path_to_write, 'port', 'port_level_import.csv'), index = False)
usato_export_in_region.to_csv(os.path.join(path_to_write, 'port', 'port_level_export.csv'), index = False)