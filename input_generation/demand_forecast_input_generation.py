#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  4 17:00:06 2024

@author: xiaodanxu
"""

import os
from pandas import read_csv
import pandas as pd
import geopandas as gps
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings("ignore")

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')


plt.style.use('ggplot')
sns.set(font_scale=1.2)  # larger font

# mapping FAF mode to SynthFirm mode
mode_lookup = {1: 'Truck', 2: 'Rail', 3: 'Other', 4: 'Air', 
               5: 'Parcel', 6: 'Other', 7: 'Other', 8: 'Other'}

#define scenario input
analysis_year = 2050
shipment_load_attr = 'tons_' + str(analysis_year)
shipment_tonmile_attr = 'tmiles_' + str(analysis_year)
shipment_value_attr = 'value_' + str(analysis_year)

base_year = 2017
shipment_load_base = 'tons_' + str(base_year)
shipment_tonmile_base = 'tmiles_' + str(base_year)
shipment_value_base = 'value_' + str(base_year)


# load FAF5 data

# load FAF5 data

faf_data = read_csv('validation/FAF5.6.1.csv', sep = ',')
faf_data_import = faf_data.loc[faf_data['trade_type'] == 2]
faf_data_export = faf_data.loc[faf_data['trade_type'] == 3]
faf_data = faf_data.loc[faf_data['trade_type'] == 1]

print(faf_data.columns)

sctg_group_lookup = read_csv('SynthFirm_parameters/SCTG_Groups_revised_V2.csv', sep = ',')
sctg_group_lookup.head(5)

cfs_faf_lookup = read_csv('SynthFirm_parameters/CFS_FAF_LOOKUP.csv')

crosswalk_file = 'SynthFirm_parameters/international_trade_zone_lookup.csv'
international_zonal_crosswalk = read_csv(crosswalk_file)
# sctg_group_lookup.head(5)

faf_data.loc[:, 'mode_def'] = faf_data.loc[:, 'dms_mode'].map(mode_lookup)
faf_data = faf_data.loc[faf_data['mode_def'] != 'Other']
faf_data = pd.merge(faf_data, sctg_group_lookup, left_on = 'sctg2', 
                    right_on = 'SCTG_Code', how = 'left')
# faf_data.head(5)

# <codecell>
############################################################
#### part 1 - domestic demand forecast (absolute value) ####
############################################################

# total production
aggr_var = [shipment_load_attr, shipment_tonmile_attr, shipment_value_attr]
faf_production = faf_data.groupby(['dms_orig', 'SCTG_Code', 'SCTG_Name'])[aggr_var].sum()
faf_production = faf_production.reset_index()

converstion_factor = 10**6/(1000*2000)
value_density_attr = 'value_density_' + str(analysis_year)

# faf_production.loc[:, value_density_base] = faf_production.loc[:, shipment_value_base] * \
#     converstion_factor / faf_production.loc[:, shipment_load_base]

faf_production.loc[:, value_density_attr] = faf_production.loc[:, shipment_value_attr] * \
    converstion_factor / faf_production.loc[:, shipment_load_attr]

# <codecell>

# process attraction
faf_attraction = faf_data.groupby(['dms_dest', 'SCTG_Code', 'SCTG_Name'])[aggr_var].sum()
faf_attraction = faf_attraction.reset_index()


faf_attraction.loc[:, value_density_attr] = faf_attraction.loc[:, shipment_value_attr] * converstion_factor / \
faf_attraction.loc[:, shipment_load_attr]

# <codecell>
# saving output
production_output_attr = ['dms_orig', 'SCTG_Code',	shipment_load_attr, value_density_attr]
attraction_output_attr = ['dms_dest', 'SCTG_Code',	shipment_load_attr, value_density_attr]

faf_production_output = faf_production[production_output_attr]
faf_attraction_output = faf_attraction[attraction_output_attr]

faf_production_output.to_csv('SynthFirm_parameters/total_commodity_production_' + str(analysis_year) + '.csv', 
                             sep = ',', index = False)
faf_attraction_output.to_csv('SynthFirm_parameters/total_commodity_attraction_' + str(analysis_year) + '.csv', 
                             sep = ',', index = False)

# <codecell>
# generate unit cost

unit_cost_forecast = faf_production_output[['dms_orig', 'SCTG_Code', value_density_attr]]

unit_cost_forecast = pd.merge(unit_cost_forecast, cfs_faf_lookup,
                              left_on = 'dms_orig', right_on = 'FAF',
                              how = 'left')
unit_cost_forecast = unit_cost_forecast[['ST_MA', 'SCTG_Code', value_density_attr]]
unit_cost_forecast[value_density_attr] *= 2000 # convert to $/us ton
unit_cost_forecast = \
unit_cost_forecast.rename(columns = {'ST_MA': 'ORIG_CFS_AREA',
                                     'SCTG_Code': 'Commodity_SCTG',
                                    value_density_attr: 'UnitCost'})
unit_cost_forecast.to_csv('SynthFirm_parameters/data_unitcost_by_zone_faf' + str(analysis_year) + '.csv', 
                          sep = ',', index = False)


# <codecell>
#################################################################
#### part 2 - International demand forecast (percent change) ####
#################################################################

# append CFS code
international_zonal_crosswalk_short = \
    international_zonal_crosswalk[['FAFID', 'CFS_CODE']]
faf_data_import = pd.merge(faf_data_import, international_zonal_crosswalk_short,
                           left_on = 'fr_orig', right_on = 'FAFID', how = 'left')

faf_data_export = pd.merge(faf_data_export, international_zonal_crosswalk_short,
                           left_on = 'fr_dest', right_on = 'FAFID', how = 'left')

# <codecell>
# generate import growth rate (% forecast - base)
attr_var = [shipment_load_attr, shipment_value_attr, \
             shipment_load_base, shipment_value_base]
group_var = ['CFS_CODE', 'dms_orig'] # foreign country, entry port

import_projection_factor = \
    faf_data_import.groupby(group_var)[attr_var].sum().reset_index()
    
import_projection_factor.loc[:, 'load_factor'] = \
    import_projection_factor.loc[:, shipment_load_attr] / \
        import_projection_factor.loc[:, shipment_load_base]

import_projection_factor.loc[:, 'value_factor'] = \
    import_projection_factor.loc[:, shipment_value_attr] / \
        import_projection_factor.loc[:, shipment_value_base]
import_projection_factor.dropna(inplace = True)

print("Range of import load adjustment:")
print(import_projection_factor.loc[:, 'load_factor'].min(),
      import_projection_factor.loc[:, 'load_factor'].max())

print("Range of import value adjustment:")
print(import_projection_factor.loc[:, 'value_factor'].min(),
      import_projection_factor.loc[:, 'value_factor'].max())

output_attr = group_var.copy()
output_attr.extend(['load_factor', 'value_factor'])
import_projection_factor = import_projection_factor[output_attr]

# <codecell>

# generate export growth rate (% forecast - base)

group_var = ['dms_dest', 'CFS_CODE'] # foreign country, entry port

export_projection_factor = \
    faf_data_export.groupby(group_var)[attr_var].sum().reset_index()
    
export_projection_factor.loc[:, 'load_factor'] = \
    export_projection_factor.loc[:, shipment_load_attr] / \
        export_projection_factor.loc[:, shipment_load_base]

export_projection_factor.loc[:, 'value_factor'] = \
    export_projection_factor.loc[:, shipment_value_attr] / \
        export_projection_factor.loc[:, shipment_value_base]
export_projection_factor.dropna(inplace = True)

print("Range of import load adjustment:")
print(export_projection_factor.loc[:, 'load_factor'].min(),
      export_projection_factor.loc[:, 'load_factor'].max())

print("Range of import value adjustment:")
print(export_projection_factor.loc[:, 'value_factor'].min(),
      export_projection_factor.loc[:, 'value_factor'].max())

output_attr = group_var.copy()
output_attr.extend(['load_factor', 'value_factor'])
export_projection_factor = export_projection_factor[output_attr]

# <codecell>

# write international output
import_projection_factor.to_csv('SynthFirm_parameters/factor_import_' + str(analysis_year) + '.csv', 
                             sep = ',', index = False)
export_projection_factor.to_csv('SynthFirm_parameters/factor_export_' + str(analysis_year) + '.csv', 
                             sep = ',', index = False)