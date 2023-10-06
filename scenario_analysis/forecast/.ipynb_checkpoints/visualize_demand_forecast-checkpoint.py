#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 27 09:20:00 2023

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


faf_data = read_csv('validation/FAF5.3.csv', sep = ',')
# print(faf_data.columns)

sctg_group_lookup = read_csv('Parameter/SCTG_Groups_revised.csv', sep = ',')
# sctg_group_lookup.head(5)

faf_data.loc[:, 'mode_def'] = faf_data.loc[:, 'dms_mode'].map(mode_lookup)
faf_data = pd.merge(faf_data, sctg_group_lookup, left_on = 'sctg2', right_on = 'SCTG_Code', how = 'left')
# faf_data.head(5)

# <codecell>
#define scenario input
analysis_years = [2017, 2030, 2040, 2050]
converstion_factor = 10**6/(1000*2000)
faf_production_output = None
faf_attraction_output = None
for analysis_year in analysis_years:
    shipment_load_attr = 'tons_' + str(analysis_year)
    shipment_tonmile_attr = 'tmiles_' + str(analysis_year)
    shipment_value_attr = 'value_' + str(analysis_year) 
    value_density_attr = 'value_density_' + str(analysis_year)
    
    aggr_var = [shipment_load_attr, shipment_tonmile_attr, shipment_value_attr]
    faf_production = faf_data.groupby(['dms_orig', 'SCTG_Code', 'SCTG_Name'])[aggr_var].sum()
    faf_production = faf_production.reset_index()
    faf_production_tonnage_agg = \
    faf_production.groupby(['SCTG_Name'])[[shipment_load_attr, 
                                           shipment_value_attr]].sum()
    
    
    faf_production_tonnage_agg.loc[:, value_density_attr] = \
        faf_production_tonnage_agg.loc[:, shipment_value_attr] * converstion_factor / \
            faf_production_tonnage_agg.loc[:, shipment_load_attr]
    faf_production_tonnage_agg.columns = ['tonnage', 'tonmile', 'value_density']
    faf_production_tonnage_agg.loc[:, 'year'] = analysis_year
    faf_production_output = pd.concat([faf_production_output, faf_production_tonnage_agg])
    
    faf_attraction = faf_data.groupby(['dms_dest', 'SCTG_Code', 'SCTG_Name'])[aggr_var].sum()
    faf_attraction = faf_attraction.reset_index()
    faf_attraction_tonnage_agg = \
    faf_attraction.groupby(['SCTG_Name'])[[shipment_load_attr, 
                                           shipment_value_attr]].sum()
    
    
    faf_attraction_tonnage_agg.loc[:, value_density_attr] = \
        faf_attraction_tonnage_agg.loc[:, shipment_value_attr] * converstion_factor / \
            faf_attraction_tonnage_agg.loc[:, shipment_load_attr]
    faf_attraction_tonnage_agg.columns = ['tonnage', 'tonmile', 'value_density']
    faf_attraction_tonnage_agg.loc[:, 'year'] = analysis_year
    faf_attraction_output = pd.concat([faf_attraction_output, faf_attraction_tonnage_agg])
    # break
faf_production_output = faf_production_output.reset_index()
faf_attraction_output = faf_attraction_output.reset_index() 
# <codecell>  

sns.catplot(kind = 'bar', data = faf_production_output, x = 'SCTG_Name', y = 'tonnage',
            hue = 'year', height = 5, aspect = 1.2)
plt.savefig('validation/tonnage_production_growth.png', dpi = 200)
plt.show()

sns.catplot(kind = 'bar', data = faf_attraction_output, x = 'SCTG_Name', y = 'tonnage',
            hue = 'year', height = 5, aspect = 1.2)
plt.savefig('validation/tonnage_attraction_growth.png', dpi = 200)
plt.show()

sns.catplot(kind = 'bar', data = faf_production_output, x = 'SCTG_Name', y = 'value_density',
            hue = 'year', height = 5, aspect = 1.2)
plt.ylabel('value density ($/lb)')
plt.savefig('validation/value_density_production_growth.png', dpi = 200)
plt.show()

sns.catplot(kind = 'bar', data = faf_attraction_output, x = 'SCTG_Name', y = 'value_density',
            hue = 'year', height = 5, aspect = 1.2)
plt.ylabel('value density ($/lb)')
plt.savefig('validation/value_density_attraction_growth.png', dpi = 200)
plt.show()
    
    # faf_production_tonnage_agg[[shipment_load_base, shipment_load_attr]].plot(kind = 'bar')
    # plt.ylabel('total production (1000 tons)')
    # plt.show()
    
    # faf_production_tonnage_agg[[value_density_base, value_density_attr]].plot(kind = 'bar')
    # plt.ylabel('average value density ($/lb)')
    # plt.show()
# shipment_load_attr = 'tons_' + str(analysis_year)
# shipment_tonmile_attr = 'tmiles_' + str(analysis_year)
# shipment_value_attr = 'value_' + str(analysis_year)

# base_year = 2017
# shipment_load_base = 'tons_' + str(base_year)
# shipment_tonmile_base = 'tmiles_' + str(base_year)
# shipment_value_base = 'value_' + str(base_year)