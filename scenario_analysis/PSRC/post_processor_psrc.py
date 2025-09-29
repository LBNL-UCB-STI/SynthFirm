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
import matplotlib

warnings.filterwarnings("ignore")

data_path = '/Users/xiaodanxu/Documents/SynthFirm.nosync'
os.chdir(data_path)


region_name = 'Seattle'
out_scenario_name = 'Seattle'
psrc_crs = 'EPSG:2285'

'''
This post-processor assign parcel ID to individual firms within psrc region

'''
input_dir = 'Inputs_' + region_name
output_dir = 'outputs_' + out_scenario_name
plot_dir = 'plots_' + out_scenario_name
# load parcel data => check year before running the code
psrc_parcel_path = os.path.join(input_dir, 'parcel_data_2018.csv')
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

# print(synthetic_firm_in_region.loc[:, 'n2'].unique())
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
print('Firms before assign parcel ID:')
print(len(synthetic_firm_in_region))


synthetic_firm_with_parcel = pd.merge(synthetic_firm_in_region, psrc_emp_long,
                                      on = ['MESOZONE', 'industry'], how = 'left')

print('Firms selected in parcel ID assignments:')
print(len(synthetic_firm_with_parcel.BusID.unique()))

# <codecell>

# random assign parcel ID
essential_attr = ['CBPZONE', 'FAFZONE',	'esizecat', 'Industry_NAICS6_Make',
                'Commodity_SCTG', 'Emp', 'BusID', 'MESOZONE', 'industry']

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
# print('Firms after assigning parcel ID:')
# print(len(synthetic_firm_with_parcel))

# <codecell>

# impute firms with missing parcel
if len(synthetic_firm_to_impute) > 0:
    sample_size = len(synthetic_firm_to_impute)
    print('Parcel IDs are imputed for ' + str(sample_size) + ' firms')
    psrc_emp_by_cbg = psrc_parcels.groupby(['Census2010BlockGroup', 'ParcelID'])[['emptot_p']].sum()
    psrc_emp_by_cbg = psrc_emp_by_cbg.reset_index()
    psrc_emp_by_cbg.columns = ['MESOZONE', 'ParcelID', 'PSRC_emp']
    psrc_emp_by_cbg = psrc_emp_by_cbg.loc[psrc_emp_by_cbg['PSRC_emp'] > 0]
    
    synthetic_firm_to_impute.drop(columns = ['ParcelID', 'PSRC_emp'], inplace = True)
    
    synthetic_firm_to_impute = pd.merge(synthetic_firm_to_impute, psrc_emp_by_cbg,
                                          on = ['MESOZONE'], how = 'left')
    
    # remaining CBG do not have parcels with valid employment --> mostly due to crosswalk issue between zip and cbg
    synthetic_firm_remaining = \
    synthetic_firm_to_impute.loc[synthetic_firm_to_impute['PSRC_emp'].isna()]
    
    # # assign parcel ID to CBGs with non-zero parcels
    synthetic_firm_to_impute = synthetic_firm_to_impute.dropna(subset = ['PSRC_emp'])
    
    synthetic_firm_to_impute = \
        synthetic_firm_to_impute.groupby(essential_attr).sample(1,
                                         weights = synthetic_firm_to_impute['PSRC_emp'],
                                         replace = True, random_state = 1)

    synthetic_firm_with_parcel = pd.concat([synthetic_firm_with_parcel, 
                                            synthetic_firm_to_impute])

    print('Firms after assigning parcel ID:')
    print(len(synthetic_firm_with_parcel))

# <codecell>
essential_attr_2 = ['CBPZONE', 'FAFZONE','esizecat', 'Industry_NAICS6_Make',
                'Commodity_SCTG', 'Emp', 'BusID', 'industry']
# impute the last batch of missing if presented, using only county and industry
# regenerate mesozone selection
if len(synthetic_firm_remaining) > 0:
    sample_size = len(synthetic_firm_remaining)
    print('WARNING!!: There are unassigned firms after parcel ID generation and the sample size is ' + str(sample_size))
    
    psrc_emp_long.loc[:, 'CBPZONE'] =\
        psrc_emp_long.loc[:, 'MESOZONE'].str[0:5].astype(int)
    synthetic_firm_remaining.drop(columns = ['MESOZONE', 'ParcelID', 'PSRC_emp'], inplace = True)
    
    synthetic_firm_remaining = pd.merge(synthetic_firm_remaining, 
                                        psrc_emp_long,
                                        on = ['CBPZONE', 'industry'], 
                                        how = 'left')
    print('Parcel IDs are imputed for ' + str(sample_size) + ' firms')
    
    # remaining CBG do not have parcels with valid employment --> mostly due to crosswalk issue between zip and cbg
    synthetic_firm_nomatch = \
    synthetic_firm_remaining.loc[synthetic_firm_remaining['PSRC_emp'].isna()]
    
    # # assign parcel ID to CBGs with non-zero parcels
    synthetic_firm_remaining = \
        synthetic_firm_remaining.dropna(subset = ['PSRC_emp'])
    
    synthetic_firm_remaining = \
        synthetic_firm_remaining.groupby(essential_attr_2).sample(1,
                                         weights = synthetic_firm_remaining['PSRC_emp'],
                                         replace = True, random_state = 1)

    synthetic_firm_with_parcel = pd.concat([synthetic_firm_with_parcel, 
                                            synthetic_firm_remaining])

# <codecell>

# assign lat/lon to parcels
wgs84_crs = 'EPSG:4326'
psrc_parcels_gdf_short = psrc_parcels_gdf[['ParcelID', 'TAZ', 'geometry']]
psrc_parcels_gdf_short = psrc_parcels_gdf_short.to_crs(wgs84_crs)
synthetic_firm_with_parcel = psrc_parcels_gdf_short.merge(synthetic_firm_with_parcel, 
                                      on = 'ParcelID', how = 'inner')
# plt.figure()
ax = synthetic_firm_with_parcel.plot(column = 'Emp',
                               cmap='viridis',figsize = (6,5), 
                               markersize = 0.1, alpha = 0.3)

cx.add_basemap(ax, source = cx.providers.CartoDB.Positron, crs = wgs84_crs)
plt.title('Firm location and employment')
plt.axis('off')
plt.savefig(os.path.join(plot_dir, 'parcel_level_synthetic_firms.png'), dpi = 300, 
            bbox_inches = 'tight')

# <codecell>
# plot parcel level emp after calibration
synthetic_emp_by_parcel = \
synthetic_firm_with_parcel.groupby(['ParcelID'])[['Emp']].sum().reset_index()
parcel_with_synthetic_emp = psrc_parcels_gdf_short.merge(synthetic_emp_by_parcel, 
                                      on = 'ParcelID', how = 'inner')
# plt.figure()
ax = parcel_with_synthetic_emp.plot(column = 'Emp',
                               cmap='viridis',
                               alpha = 0.3, 
                               markersize = 0.005 * parcel_with_synthetic_emp['Emp'],
                               vmin = 0, vmax =1000,
                               legend=True,
                               norm=matplotlib.colors.LogNorm(vmin=1, 
                                                              vmax = parcel_with_synthetic_emp['Emp'].max()),
                               legend_kwds = {'shrink': 0.8}, antialiased=False)
cx.add_basemap(ax, source = cx.providers.CartoDB.Positron, crs = wgs84_crs)
plt.axis('off')
plt.savefig(os.path.join(plot_dir, 'parcel_level_emp_modeled.png'), dpi = 300, 
            bbox_inches = 'tight')
plt.show()
# <codecell>
# add random noise to coordinates
noise_meters = 100

# Convert the noise to degrees
noise_degrees = noise_meters / 111320

# Generate a random noise in degrees
synthetic_firm_with_parcel['lat'] = synthetic_firm_with_parcel.geometry.y
synthetic_firm_with_parcel['lon'] = synthetic_firm_with_parcel.geometry.x

synthetic_firm_with_parcel_df = \
    pd.DataFrame(synthetic_firm_with_parcel.drop(columns = 'geometry'))
    
# Add the noise to the coordinates
sample_size = len(synthetic_firm_with_parcel_df)
synthetic_firm_with_parcel_df['lat'] = \
    synthetic_firm_with_parcel_df['lat'] + np.random.uniform(0, noise_degrees, sample_size)
synthetic_firm_with_parcel_df['lon'] = \
    synthetic_firm_with_parcel_df['lon'] + np.random.uniform(0, noise_degrees, sample_size)

# <codecell>
# PLOT RESULTS
firm_parcels_gdf = gps.GeoDataFrame(
    synthetic_firm_with_parcel_df, 
    geometry=gps.points_from_xy(synthetic_firm_with_parcel_df.lon, 
                                synthetic_firm_with_parcel_df.lat), crs = wgs84_crs)

ax = firm_parcels_gdf.plot(figsize = (6,5), markersize = 0.1, alpha = 0.3)

cx.add_basemap(ax, source = cx.providers.CartoDB.Positron, crs = wgs84_crs)
plt.axis('off')
plt.savefig(os.path.join(plot_dir, 'parcel_level_synthetic_firms_w_noise.png'), dpi = 300, 
            bbox_inches = 'tight')
# <codecell>
# compare results by CBG
from sklearn.metrics import mean_squared_error 
from sklearn.metrics import r2_score

synthfirm_by_cbg = synthetic_firm_with_parcel_df.groupby('MESOZONE')[['Emp']].sum()
synthfirm_by_cbg = synthfirm_by_cbg.reset_index()
psrc_emp_by_cbg = psrc_parcels.groupby(['Census2010BlockGroup'])[['emptot_p']].sum()
psrc_emp_by_cbg = psrc_emp_by_cbg.reset_index()
psrc_emp_by_cbg.columns = ['MESOZONE', 'PSRC_emp']
firm_comparison_by_cbg = pd.merge(synthfirm_by_cbg, psrc_emp_by_cbg, 
                                     on = 'MESOZONE',
                                     how = 'left')
firm_comparison_by_cbg.columns = ['GEOID', 'SynthFirm employment', 'PSRC employment']
rmse_emp = mean_squared_error(firm_comparison_by_cbg['SynthFirm employment'], 
                              firm_comparison_by_cbg['PSRC employment'])
rmse_emp = np.sqrt(rmse_emp)
r2_emp = r2_score(firm_comparison_by_cbg['PSRC employment'], 
                  firm_comparison_by_cbg['SynthFirm employment'])
rmse_emp = np.round(rmse_emp, 1)
r2_emp = np.round(r2_emp, 2)
print(firm_comparison_by_cbg['PSRC employment'].sum())
print(firm_comparison_by_cbg['SynthFirm employment'].sum())
print(rmse_emp, r2_emp)
plt.style.use('seaborn-v0_8-white')
# plt.rcParams['axes.facecolor'] = 'white'
sns.set(font_scale=1.4)  # crazy big
sns.set_style("white")
sns.lmplot(
    data=firm_comparison_by_cbg,
    x="PSRC employment", y="SynthFirm employment", 
    height=4.5, aspect = 1.2, line_kws={'color': 'grey'}, 
    scatter_kws = {'alpha':0.3})
# g.set_facecolor("white")

plt.xlim([0, 60000])
plt.ylim([0, 60000])
plt.xlabel('PSRC Employment')
plt.ylabel('SynthFirm Employment')
plt.title('Employment by CBG, $R^{2}$ = ' + str(r2_emp) + \
          ' , RMSE =' + str(rmse_emp), fontsize = 14)
plt.savefig(os.path.join(plot_dir, 'emp_by_CBG_validation_synthfirm.png'), dpi = 200,
           bbox_inches = 'tight')


# <codecell>

# write output
output_attr = ['CBPZONE', 'FAFZONE', 'esizecat', 'Industry_NAICS6_Make',
       'Commodity_SCTG', 'Emp', 'BusID', 'MESOZONE', 'lat', 'lon', 
       'ParcelID', 'TAZ']
synthetic_firm_in_region = synthetic_firm_with_parcel_df[output_attr]
synthetic_firm_output = pd.concat([synthetic_firm_in_region,
                                   synthetic_firm_out_region])
synthetic_firm_output.fillna(-1, inplace = True)
synthetic_firm_output = synthetic_firm_output.astype({
'CBPZONE': np.int64,
'FAFZONE': np.int64,
'esizecat': np.int64, 
'Industry_NAICS6_Make': 'string',
'Commodity_SCTG': np.int64,
'Emp': 'float',
'BusID': np.int64, 
'MESOZONE': np.int64, 
'lat': 'float', 
'lon': 'float',
'ParcelID': np.int64, 
'TAZ': np.int64
})
calibrated_firm_file = os.path.join(output_dir, 'synthetic_firms_with_location.csv')
synthetic_firm_output.to_csv(calibrated_firm_file, index = False)



