#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 12 11:44:23 2021

@author: xiaodanxu
"""

# directories
param_dir = 'Parameter/'
input_dir = 'Input/'
output_dir = 'Output/'
cfs_dir = 'CFS2017/'

freight_model_zonal_map =  'sfbay_freight.geojson'

# input file
faf_version_4_file = 'FAF4.5.1.csv'
cfs_generation_file = 'cfs17_generated_shpmt_by_dest_commodity.csv'
cfs_attraction_file = 'cfs17_attracted_shpmt_by_orig_commodity.csv'
cfs_distribution_file = 'cfs17_shpmt_dist_210916.csv' # mode added
cfs_summary_file = 'CFS_summary_statistics.csv'
modeled_file = 'processed_b2b_flow_summary.csv'


# parameter file
mesozone_id_lookup_file = 'zonal_id_lookup_final.csv'
mesozone_geoid_lookup_file = 'MESOZONE_GEOID_LOOKUP.csv'
CFS_FAF_lookup_file = 'CFS_FAF_LOOKUP.csv'
FAF_county_lookup_file = 'SFBay_FAFCNTY.csv'
sctg_group_lookup_file = 'SCTG_Groups.csv'
sctg_definition_file = 'SCTG_definition.csv'
sctg_unit_cost_file = 'data_unitcost.csv'
value_density_file = 'value_density_by_SCTG_group.csv' # dollar per lb by sctg group
distance_travel_skim_file = 'combined_travel_time_skim.csv'

# other parameters
bay_area_region_code = [64, 62, 65, 69] 
default_truck_fraction = 0.9 # use previous assumption from from PMG.R
truck_mode_id = 1 # mode ==> truck
trade_type_id = 1 # domestic only
list_of_sctg_group = ['sctg1', 'sctg2', 'sctg3', 'sctg4', 'sctg5'] #currently hard coded, can be improved
sctg_def = {'sctg1': 'bulk', 'sctg2': 'fuel_fert', 'sctg3':'interm_food', 'sctg4': 'mfr_goods', 'sctg5': 'other'}
lb_to_ton = 1/2000
NAICS_wholesale = [42]
NAICS_mfr = [31, 32, 33]
NAICS_mgt = [55]
NAICS_retail = [44, 45]
NAICS_info = [51]
NAICS_mining = [21]
max_shipment_load = 5000
max_ton_lookup = {'sctg1': 218.327, 'sctg2': 51.13091, 'sctg3': 94.56035, 'sctg4': 4.4861, 'sctg5': 51.64694} # in 1000 ton