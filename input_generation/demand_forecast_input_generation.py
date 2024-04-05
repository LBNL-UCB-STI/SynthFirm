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
analysis_year = 2017
shipment_load_attr = 'tons_' + str(analysis_year)
shipment_tonmile_attr = 'tmiles_' + str(analysis_year)
shipment_value_attr = 'value_' + str(analysis_year)

base_year = 2017
shipment_load_base = 'tons_' + str(base_year)
shipment_tonmile_base = 'tmiles_' + str(base_year)
shipment_value_base = 'value_' + str(base_year)


# load FAF5 data

faf_data = read_csv('validation/FAF5.3.csv', sep = ',')
# print(faf_data.columns)

sctg_group_lookup = read_csv('Parameter/SCTG_Groups_revised.csv', sep = ',')
# sctg_group_lookup.head(5)

faf_data.loc[:, 'mode_def'] = faf_data.loc[:, 'dms_mode'].map(mode_lookup)
faf_data = pd.merge(faf_data, sctg_group_lookup, left_on = 'sctg2', 
                    right_on = 'SCTG_Code', how = 'left')
# faf_data.head(5)

# <codecell>

# total production
aggr_var = [shipment_load_base, shipment_tonmile_base, shipment_value_base, 
            shipment_load_attr, shipment_tonmile_attr, shipment_value_attr]
faf_production = faf_data.groupby(['dms_orig', 'SCTG_Code', 'SCTG_Name'])[aggr_var].sum()
faf_production = faf_production.reset_index()

converstion_factor = 10**6/(1000*2000)
value_density_base = 'value_density_' + str(base_year)
value_density_attr = 'value_density_' + str(analysis_year)

faf_production.loc[:, value_density_base] = faf_production.loc[:, shipment_value_base] * converstion_factor / \
faf_production.loc[:, shipment_load_base]

faf_production.loc[:, value_density_attr] = faf_production.loc[:, shipment_value_attr] * converstion_factor / \
faf_production.loc[:, shipment_load_attr]

# print(len(faf_production))
# print(len(faf_production.dms_orig.unique()))
# faf_production.head(5)

# <codecell>

# process attraction
faf_attraction = faf_data.groupby(['dms_dest', 'SCTG_Code', 'SCTG_Name'])[aggr_var].sum()
faf_attraction = faf_attraction.reset_index()

faf_attraction.loc[:, value_density_base] = faf_attraction.loc[:, shipment_value_base] * converstion_factor / \
faf_attraction.loc[:, shipment_load_base]

faf_attraction.loc[:, value_density_attr] = faf_attraction.loc[:, shipment_value_attr] * converstion_factor / \
faf_attraction.loc[:, shipment_load_attr]

print(len(faf_attraction))
print(len(faf_attraction.dms_dest.unique()))
faf_attraction.head(5)
# <codecell>
# saving output
production_output_attr = ['dms_orig', 'SCTG_Code',	shipment_load_attr, value_density_attr]
attraction_output_attr = ['dms_dest', 'SCTG_Code',	shipment_load_attr, value_density_attr]

faf_production_output = faf_production[production_output_attr]
faf_attraction_output = faf_attraction[attraction_output_attr]

faf_production_output.to_csv('SynthFirm_parameters/total_commodity_production_' + str(analysis_year) + '.csv', sep = ',', index = False)
faf_attraction_output.to_csv('SynthFirm_parameters/total_commodity_attraction_' + str(analysis_year) + '.csv', sep = ',', index = False)