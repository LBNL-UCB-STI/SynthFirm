#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 19 09:24:43 2024

@author: xiaodanxu
"""

import pandas as pd
import os
import numpy as np
from pandas import read_csv
import geopandas as gpd
from shapely.geometry import box

path2file = "/Users/xiaodanxu/Documents/SynthFirm.nosync"
os.chdir(path2file)

# selected_state = 'CA'
# selected_region = 'BayArea'

selected_state = 'TX'
selected_region = 'Austin'

dir_to_write = 'inputs_' + selected_region

# load inputs
faf_county_file = os.path.join(dir_to_write, selected_region + '_FAFCNTY.csv')
naics_file_name = os.path.join('SynthFirm_parameters', 'US_naics.csv')
lehd_boundary_file = os.path.join('SynthFirm_parameters', 'US_bg.geojson')
faf_zone_file = os.path.join("RawData", "FAF5Zones.geojson")
cfs_faf_file = os.path.join('RawData','CFS_FAF_LOOKUP.csv')

faf_county_crosswalk = read_csv(faf_county_file)
lehd_employment = read_csv(naics_file_name)
faf_zone = gpd.read_file(faf_zone_file)
cbg_boundary = gpd.read_file(lehd_boundary_file)
CFS_FAF_lookup = read_csv(cfs_faf_file, sep = ',')

# lehd_employment.drop(columns = ['metalayer_id'], inplace = True)

lehd_employment.loc[:, 'GEOID'] = lehd_employment.loc[:, 'GEOID'].astype(str).str.zfill(12)
lehd_employment.loc[:, 'ST_CNTY'] = lehd_employment.loc[:, 'GEOID'].str[0:5]
faf_county_crosswalk.loc[:, 'ST_CNTY'] = \
faf_county_crosswalk.loc[:, 'ST_CNTY'].astype(str).str.zfill(5)

faf_county_crosswalk = faf_county_crosswalk[['ST_CNTY', 'FAFID', 'CBPZONE']]

lehd_employment = pd.merge(lehd_employment, faf_county_crosswalk, 
                           on = 'ST_CNTY', how = 'left')

# <codecell>

# creating mesozone IDs
lehd_employment.loc[:, 'MESOZONE'] = lehd_employment.loc[:, 'GEOID']
lehd_employment.loc[:, 'IS_CBG'] = 1
# grouping CBGs within FAF zone
faf_zone_idx = lehd_employment['CBPZONE'] < 1000
lehd_employment.loc[faf_zone_idx, 'MESOZONE'] = lehd_employment.loc[:, 'CBPZONE'] + 20000
lehd_employment.loc[faf_zone_idx, 'IS_CBG'] = 0
unique_mesozone = lehd_employment.loc[:, 'MESOZONE'].unique()
print(len(unique_mesozone))

# <codecell>

#aggregate employment to mesozone level
emp_attr = [ 'n11', 'n21', 'n22', 'n23', 'n3133', 'n42', 'n4445', 'n4849',
       'n51', 'n52', 'n53', 'n54', 'n55', 'n56', 'n61', 'n62', 'n71', 'n72',
       'n81', 'n92']

emp_colnames = ["rank11",
  "rank21",
  "rank22",
  "rank23",
  "rank3133",
  "rank42",
  "rank4445",
  "rank4849",
  "rank51",
  "rank52",
  "rank53",
  "rank54",
  "rank55",
  "rank56",
  "rank61",
  "rank62",
  "rank71",
  "rank72",
  "rank81",
  "rank92"]

lehd_employment_by_mesozone = \
    lehd_employment.groupby(['FAFID', 'MESOZONE', 'CBPZONE', 'IS_CBG'])[emp_attr].sum()
lehd_employment_by_mesozone.columns = emp_colnames

lehd_employment_by_mesozone = lehd_employment_by_mesozone.reset_index()

# <codecell>

# generate county ID (to match with CBP data)
lehd_employment_by_mesozone.loc[:, 'COUNTY'] = np.nan
lehd_employment_by_mesozone.loc[lehd_employment_by_mesozone['IS_CBG']==1, 'COUNTY'] = \
    lehd_employment_by_mesozone.loc[lehd_employment_by_mesozone['IS_CBG']==1, 'MESOZONE'].str[0:5]

# lehd_employment_by_mesozone.loc[:, 'cnty_id'] = lehd_employment_by_mesozone.loc[:, 'COUNTY'] # to align the format 
mesozone_empranking = lehd_employment_by_mesozone.drop(columns = ['FAFID', 'CBPZONE', 'IS_CBG'])

# overwrite 0 with nan, so it is aligned with old version
mesozone_empranking.replace(0, np.nan, inplace=True)
# write emp ranking output

mesozone_empranking.to_csv(os.path.join(dir_to_write, 'data_mesozone_emprankings.csv'),
                           index = False)

# <codecell>

# generate freight GIS file


spatial_crosswalk = lehd_employment_by_mesozone[['FAFID', 'MESOZONE', 'CBPZONE', 'IS_CBG']]


# collect cbg within study area
region_cbg = spatial_crosswalk.loc[spatial_crosswalk['IS_CBG'] == 1]
region_cbg.loc[:, 'GEOID'] = region_cbg.loc[:, 'MESOZONE']
region_fafid = region_cbg.FAFID.unique()
region_county = \
    faf_county_crosswalk.loc[faf_county_crosswalk['FAFID'].isin(region_fafid)]
# <codecell>
cbg_boundary.loc[:, 'GEOID'] = cbg_boundary.loc[:, 'GEOID'].astype(str).str.zfill(12)
cbg_boundary = cbg_boundary[['GEOID', 'geometry']]
cbg_boundary.loc[:, 'ST_CNTY'] = cbg_boundary.loc[:, 'GEOID'].str[0:5]
region_county = region_county[['ST_CNTY', 'FAFID']]
cbg_boundary_in_region = \
    cbg_boundary.merge(region_county, on = 'ST_CNTY', how = 'inner')
region_cbg = region_cbg[['MESOZONE', 'CBPZONE', 'GEOID']]
region_cbg['CBPZONE'] =region_cbg['CBPZONE'].astype(int)
region_cbg['CBPZONE'] = region_cbg['CBPZONE'].astype(str).str.zfill(5)
cbg_boundary_in_region = cbg_boundary_in_region.merge(region_cbg, on = 'GEOID', how = 'left')


# <codecell>

# fill in NA mesozone
na_idx = (cbg_boundary_in_region['MESOZONE'].isna())
cbg_boundary_in_region.loc[na_idx, 'MESOZONE'] = \
    cbg_boundary_in_region.loc[na_idx, 'GEOID']
cbg_boundary_in_region.loc[na_idx, 'CBPZONE'] = \
    cbg_boundary_in_region.loc[na_idx, 'ST_CNTY']
    

cbg_boundary_in_region = \
    cbg_boundary_in_region[['GEOID', 'FAFID', 'MESOZONE', 'CBPZONE', 'geometry']]
# <codecell>

# collect faf zones outside study area
polygon = box(-170, 10, -66.96466, 71.365162)
faf_zone = faf_zone.clip(polygon)
faf_zone = faf_zone.rename(columns = {'FAF': 'FAFID'})
faf_zone.loc[:, 'FAFID'] = faf_zone.loc[:, 'FAFID'].astype(int)

# faf_zone.plot()

external_zones = spatial_crosswalk.loc[spatial_crosswalk['IS_CBG'] == 0]
external_zones.loc[:, 'FAFID'] = external_zones.loc[:, 'FAFID'].astype(int)
if len(external_zones) > 0: # this is a regional model
    faf_zone_selected = faf_zone.merge(external_zones, on = 'FAFID', how = 'inner')
    faf_zone_selected.loc[:, 'GEOID'] = faf_zone_selected.loc[:, 'FAFID']
    faf_zone_selected = faf_zone_selected[['GEOID', 'FAFID', 'MESOZONE', 'CBPZONE', 'geometry']]
    freight_zone_output = pd.concat([faf_zone_selected, cbg_boundary_in_region])
else:
    freight_zone_output = cbg_boundary_in_region

freight_zone_output_df = freight_zone_output.drop(columns = 'geometry')
freight_zone_output = freight_zone_output.to_crs('EPSG:4326')
freight_zone_centroid = freight_zone_output["geometry"].centroid
freight_zone_centroid = \
gpd.GeoDataFrame(geometry = gpd.GeoSeries(freight_zone_centroid))
freight_zone_centroid = pd.concat([freight_zone_centroid, freight_zone_output_df], 
                                  axis = 1)

freight_zone_output.to_file(os.path.join(dir_to_write, selected_region + '_freight.geojson'), driver="GeoJSON") 
freight_zone_centroid.to_file(os.path.join(dir_to_write, selected_region + '_freight_centroids.geojson'), driver="GeoJSON")

freight_zone_centroid.plot() 

# <codecell>

# write spatial crosswalk file
FAF_names = CFS_FAF_lookup.loc[:, ['FAF', 'SHORTNAME']]
FAF_names.columns = ['FAFID', 'FAFNAME']
freight_zone_output_df = pd.merge(freight_zone_output_df, 
                                  FAF_names, on='FAFID', how = 'left')

freight_zone_output_df.to_csv(os.path.join(dir_to_write, 'zonal_id_lookup_final.csv'), index = False)