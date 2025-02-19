#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb  5 10:17:52 2025

@author: xiaodanxu
"""

import pandas as pd
import geopandas as gps
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
import os
from pandas import read_csv
import numpy as np
from pygris import block_groups
import contextily as cx

warnings.filterwarnings("ignore")

data_path = '/Users/xiaodanxu/Documents/SynthFirm.nosync'
os.chdir(data_path)

analysis_year = '2018'
region_name = 'Seattle'
psrc_crs = 'EPSG:2285'

'''
This post-processor assign parcel ID to individual firms within psrc region

'''
input_dir = 'Inputs_' + region_name
output_dir = 'outputs_' + region_name
# load parcel data
psrc_parcel_path = os.path.join(input_dir, 'parcel_data_' + analysis_year + '.csv')
psrc_parcels = pd.read_csv(psrc_parcel_path, sep = ',')

# need to replace this file with firms with fleet after fleet generation updated
synthetic_firm_file = os.path.join(output_dir, 'synthetic_firms_with_location.csv')
synthetic_firm_to_calibrate = read_csv(synthetic_firm_file, sep = ',')

# format geoid

psrc_parcels.loc[:, 'Census2010BlockGroup'] = \
psrc_parcels.loc[:, 'Census2010BlockGroup'].astype(int).astype(str).str.zfill(12)


synthetic_firm_to_calibrate.loc[:, 'MESOZONE'] = \
    synthetic_firm_to_calibrate.loc[:, 'MESOZONE'].astype(int).astype(str).str.zfill(12)
# <codecell>
# create spatial map of parcel
import matplotlib
plot_dir = 'plots_' + region_name
psrc_parcels_gdf = gps.GeoDataFrame(
    psrc_parcels, 
    geometry=gps.points_from_xy(psrc_parcels.xcoord_p, 
                                psrc_parcels.ycoord_p), crs=psrc_crs)
ax = psrc_parcels_gdf.plot(column = 'emptot_p',
                               cmap='viridis',
                               alpha = 0.3, 
                               markersize = 0.005 * psrc_parcels_gdf['emptot_p'],
                               vmin = 0, vmax =1000,
                               legend=True,
                               norm=matplotlib.colors.LogNorm(vmin=1, 
                                                              vmax = psrc_parcels_gdf['emptot_p'].max()),
                               legend_kwds = {'shrink': 0.8}, antialiased=False)
cx.add_basemap(ax, source = cx.providers.CartoDB.Positron, crs = psrc_crs)
plt.axis('off')
plt.savefig(os.path.join(plot_dir, 'parcel_level_emp.png'), dpi = 300, 
            bbox_inches = 'tight')
plt.show()

# <codecell>

# for firms within PSRC region, reallocate their locations
CT_in_PSRC = psrc_parcels.loc[:, 'FIPS'].unique()
CT_in_PSRC = [int(x) for x in CT_in_PSRC]
print(CT_in_PSRC)

synthetic_firm_in_region = \
    synthetic_firm_to_calibrate.loc[synthetic_firm_to_calibrate['CBPZONE'].isin(CT_in_PSRC)]
synthetic_firm_in_region.drop(columns = ['lat', 'lon'], inplace = True)

synthetic_firm_out_region = \
    synthetic_firm_to_calibrate.loc[~synthetic_firm_to_calibrate['CBPZONE'].isin(CT_in_PSRC)]

# <codecell>

# format parcel data into long table

psrc_sector = ['PSRC_Education', 'PSRC_Government', 'PSRC_Industrial', 'PSRC_Medical',
              'PSRC_Other', 'PSRC_Retail', 'PSRC_Office', 'PSRC_Service']

psrc_parcels.loc[:, 'PSRC_Education'] = psrc_parcels.loc[:, 'empedu_p']
psrc_parcels.loc[:, 'PSRC_Government'] = psrc_parcels.loc[:, 'empgov_p']
psrc_parcels.loc[:, 'PSRC_Industrial'] = psrc_parcels.loc[:, 'empind_p']
psrc_parcels.loc[:, 'PSRC_Medical'] = psrc_parcels.loc[:, 'empmed_p']
psrc_parcels.loc[:, 'PSRC_Other'] = psrc_parcels.loc[:, 'empoth_p']
psrc_parcels.loc[:, 'PSRC_Retail'] = psrc_parcels.loc[:, 'empret_p']
psrc_parcels.loc[:, 'PSRC_Office'] = psrc_parcels.loc[:, 'empofc_p']
psrc_parcels.loc[:, 'PSRC_Service'] = psrc_parcels.loc[:, 'empfoo_p'] + \
psrc_parcels.loc[:, 'empsvc_p']

psrc_emp_long = pd.melt(psrc_parcels, id_vars=['Census2010BlockGroup', 'ParcelID'],
                        value_vars=psrc_sector, var_name='industry', value_name='PSRC_emp')


psrc_emp_long.loc[:,'industry'] = psrc_emp_long.loc[:,'industry'].str.split('_').str[1]
psrc_emp_long = psrc_emp_long.loc[psrc_emp_long['PSRC_emp'] > 0]
print(psrc_emp_long['industry'].unique())


# Assign PSRC industry code to synthetic firms
synthetic_firm_in_region.loc[:, 'n2'] = \
    synthetic_firm_in_region.loc[:, 'Industry_NAICS6_Make'].astype(str).str[0:2]

print(synthetic_firm_in_region.loc[:, 'n2'].unique())
industry_mapping = {
    'Other': ['11', '21', '23'],
    'Industrial': ['31', '32', '33', '22', '42', '48', '49'],
    'Retail': ['44', '45', '4A'],
    'Office': ['51', '52', '53', '54', '55', '56'],
    'Education': ['61'],
    'Medical': ['62'],
    'Service': ['71', '72', '81'],
    'Government': ['92', 'S0']
}

synthetic_firm_in_region.loc[:, 'industry'] = np.nan
for col, values in industry_mapping.items():
    synthetic_firm_in_region.loc[synthetic_firm_in_region['n2'].isin(values), 'industry'] = col
print(synthetic_firm_in_region.loc[:, 'industry'].unique())

# <codecell>

# assign parcel id to firms in region

psrc_emp_long.rename(columns = {'Census2010BlockGroup': 'MESOZONE'}, inplace = True)
print('firms before assign parcel ID:')
print(len(synthetic_firm_in_region))


synthetic_firm_with_parcel = pd.merge(synthetic_firm_in_region, psrc_emp_long,
                                      on = ['MESOZONE', 'industry'], how = 'left')

print('firms when assign parcel ID:')
print(len(synthetic_firm_with_parcel.BusID.unique()))

# <codecell>

# random assign parcel ID
essential_attr = ['CBPZONE', 'FAFZONE',	'esizecat', 'Industry_NAICS6_Make',
                'Commodity_SCTG', 'Emp', 'BusID', 'MESOZONE', 'ZIPCODE', 'industry']

# some CBG do not have parcels with valid employment by that industry --> assign those later
synthetic_firm_to_impute = \
synthetic_firm_with_parcel.loc[synthetic_firm_with_parcel['PSRC_emp'].isna()]

# assign parcel ID to CBGs with non-zero parcels
synthetic_firm_with_parcel = synthetic_firm_with_parcel.dropna()
synthetic_firm_with_parcel
synthetic_firm_with_parcel = \
    synthetic_firm_with_parcel.groupby(essential_attr).sample(1,
                                     weights = synthetic_firm_with_parcel['PSRC_emp'],
                                     replace = True, random_state = 1)
print('firms after assign parcel ID:')
print(len(synthetic_firm_with_parcel))

# <codecell>

# impute firms with missing parcel

psrc_emp_by_cbg = psrc_parcels.groupby(['Census2010BlockGroup', 'ParcelID'])[['emptot_p']].sum()
psrc_emp_by_cbg = psrc_emp_by_cbg.reset_index()
psrc_emp_by_cbg.columns = ['MESOZONE', 'ParcelID', 'PSRC_emp']
psrc_emp_by_cbg = psrc_emp_by_cbg.loc[psrc_emp_by_cbg['PSRC_emp'] > 0]

synthetic_firm_to_impute.drop(columns = ['ParcelID', 'PSRC_emp'], inplace = True)

synthetic_firm_to_impute = pd.merge(synthetic_firm_to_impute, psrc_emp_by_cbg,
                                      on = ['MESOZONE'], how = 'left')

# <codecell>
# remaining CBG do not have parcels with valid employment --> mostly due to crosswalk issue between zip and cbg
synthetic_firm_remaining = \
synthetic_firm_to_impute.loc[synthetic_firm_to_impute['PSRC_emp'].isna()]

# # assign parcel ID to CBGs with non-zero parcels
# synthetic_firm_with_parcel = synthetic_firm_with_parcel.dropna()

synthetic_firm_to_impute = \
    synthetic_firm_to_impute.groupby(essential_attr).sample(1,
                                     weights = synthetic_firm_with_parcel['PSRC_emp'],
                                     replace = True, random_state = 1)
print('firms after assign parcel ID:')
print(len(synthetic_firm_to_impute))
