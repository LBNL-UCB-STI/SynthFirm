#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 12 11:24:42 2021

@author: xiaodanxu
"""

from pandas import read_csv
import pandas as pd
import geopandas as gps
import os

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')
scenario_name = 'Austin'
input_dir = 'inputs_' + scenario_name + '/'


# load mesozone id and FAF zone reference tables
# mesozone_id_lookup = read_csv(c.param_dir + c.mesozone_geoid_lookup_file, sep = ',')
CFS_FAF_lookup = read_csv('RawData/' + 'CFS_FAF_LOOKUP.csv', sep = ',')
FAF_county_lookup = read_csv(input_dir + scenario_name + '_FAFCNTY.csv', sep = ',')
freight_model_geojson = gps.read_file(input_dir + scenario_name + '_freight.geojson')

# fill FAF zone id
study_area_FAF_and_county = FAF_county_lookup.loc[FAF_county_lookup['CBPZONE'] > 999]
# list_of_cbpzone_in_bay_area = bay_area_FAF_and_county['CBPZONE'].tolist()
FAF_county_lookup = FAF_county_lookup.loc[:, ['FAFID', 'CBPZONE']]
FAF_county_lookup = FAF_county_lookup.drop_duplicates()
freight_model_zonal_attr = freight_model_geojson.loc[:, ['GEOID', 'CBPZONE', 'MESOZONE']]
freight_model_zonal_attr_with_fafid = pd.merge(freight_model_zonal_attr, 
                                               FAF_county_lookup, 
                                               on = 'CBPZONE', 
                                               how = 'left')

# assign FAF zone name
FAF_names = CFS_FAF_lookup.loc[:, ['FAF', 'SHORTNAME']]
FAF_names.columns = ['FAFID', 'FAFNAME']
freight_model_zonal_attr_with_name = pd.merge(freight_model_zonal_attr_with_fafid, 
                                              FAF_names, on='FAFID', how = 'left')
freight_model_zonal_attr_with_name.to_csv(input_dir + 'zonal_id_lookup_final.csv', index = False)