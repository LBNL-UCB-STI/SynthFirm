#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 14 10:26:14 2025

@author: xiaodanxu
"""

from pandas import read_csv
import pandas as pd
import numpy as np
import os
import geopandas as gpd
import visualkit as vk

import warnings
warnings.filterwarnings('ignore')

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')

########################################################
#### step 1 - configure environment and load inputs ####
########################################################

# define scatter plot

# define scenario
scenario_name = 'Seattle'
out_scenario_name = 'Seattle'
param_dir = 'SynthFirm_parameters'
input_dir = 'inputs_' + scenario_name
output_dir = 'outputs_' + out_scenario_name
plot_dir = 'plots_' + out_scenario_name
# region_code = None # using None for national --> need to be replaced with run type
# focus_region = None # using None for national --> need to be replaced with run type
region_code = [411, 531, 532, 539]
# region_code = [62, 64, 65, 69]
focus_region = 531
lehd_file = 'US_naics.csv'
map_file = scenario_name + '_freight.geojson'
add_base_map_selection = True
logscale_selection = False
port_analysis = True
truck_only = False

if not os.path.exists(plot_dir):
    os.mkdir(plot_dir)
else:
  print("plot directory exists")

# load input directories --> will be replaced by config
synthetic_firms_with_location_file = os.path.join(output_dir, 'synthetic_firms_with_location.csv')
spatial_boundary_file = os.path.join(input_dir, map_file)
mesozone_to_faf_file = os.path.join(os.path.join(input_dir, 'zonal_id_lookup_final.csv'))
lehd_file = os.path.join(param_dir, lehd_file) # add this to config
us_county_map_file = os.path.join(param_dir, 'US_countries.geojson') # add this to config
domestic_summary_zone_file = os.path.join(output_dir, 
                                          'domestic_b2b_flow_summary_mesozone.csv')



# load input files
synthfirm_all = read_csv(synthetic_firms_with_location_file)
region_map = gpd.read_file(spatial_boundary_file)
mesozone_id_lookup = read_csv(mesozone_to_faf_file)
lehd_validation = read_csv(lehd_file)
synthfirm_output_domestic = read_csv(domestic_summary_zone_file)

if port_analysis:
    international_summary_zone_file = os.path.join(output_dir, 
                                               'international_b2b_flow_summary_mesozone.csv')
    synthfirm_output_international = read_csv(international_summary_zone_file)

# <codecell>

region_map.dropna(subset = ['geometry'], inplace = True)
# calculate land area
region_map.loc[:, "area"] = \
region_map['geometry'].to_crs({'proj':'cea'}).map(lambda p: p.area / 10**6) 

# load shifted county geometry
analysis_year = 2017
us_counties = gpd.read_file(us_county_map_file)

# <codecell>

##################################################################
#### step 2 - validate firm employment by county and industry ####
##################################################################

firm_by_zone = synthfirm_all.groupby(['MESOZONE']).agg({'BusID':'count',
                                                       'Emp':'sum'})
firm_by_zone = firm_by_zone.reset_index()
firm_by_zone.columns = ['MESOZONE', 'firm_count', 'employment_count']

print('Total national firms and employments:')
print(firm_by_zone[['firm_count', 'employment_count']].sum())
firm_by_zone = pd.merge(firm_by_zone, mesozone_id_lookup, 
                        on = 'MESOZONE', how = 'left')

# <codecell>
if region_code is not None:
    # select data from the region
    firm_in_study_area = \
    firm_by_zone.loc[firm_by_zone['FAFID'].isin(region_code)]
    print('Total firms in study area:')
    print(firm_in_study_area.firm_count.sum())
    synthfirm_in_study_area = synthfirm_all.loc[synthfirm_all['CBPZONE'] >= 1000]
    print('Total employment in study area:')
    print(synthfirm_in_study_area.Emp.sum())
    print('Total industries in study area:')
    print(len(synthfirm_in_study_area.Industry_NAICS6_Make.unique()))
else: # national validation
    firm_in_study_area = firm_by_zone.copy()
    synthfirm_in_study_area = synthfirm_all.copy()
    

# summarize employment by county and CBG
firm_in_study_area_by_county = firm_in_study_area.groupby('CBPZONE')[['employment_count']].sum()
firm_in_study_area_by_county = firm_in_study_area_by_county.reset_index()


mesozone_id_lookup_short = mesozone_id_lookup[['MESOZONE', 'GEOID']]
firm_in_study_area_by_cbg = firm_in_study_area.groupby('MESOZONE')[['employment_count']].sum()
firm_in_study_area_by_cbg = firm_in_study_area_by_cbg.reset_index()
firm_in_study_area_by_cbg = pd.merge(firm_in_study_area_by_cbg,
                                     mesozone_id_lookup_short,
                                     on = 'MESOZONE', how = 'left')

# <codecell>

# compare to LEHD data

list_of_attr = ['n11', 'n21', 'n22', 'n23', 'n3133', 'n42',
                'n4445', 'n4849', 'n51', 'n52', 'n53', 'n54', 
                'n55', 'n56', 'n61', 'n62', 'n71', 'n72', 'n81', 'n92']
lehd_validation = pd.merge(lehd_validation, mesozone_id_lookup,
                               on = 'GEOID', how = 'inner')
lehd_validation.loc[:, 'total'] = lehd_validation.loc[:, list_of_attr].sum(axis = 1)


# comparison by county
lehd_firm_by_county = lehd_validation.groupby('CBPZONE')[['total']].sum()
lehd_firm_by_county = lehd_firm_by_county.reset_index()
# # print(lehd_firm_by_county.head(5))
firm_comparison_by_county = pd.merge(lehd_firm_by_county, 
                                     firm_in_study_area_by_county,
                                     on = 'CBPZONE',
                                     how = 'outer')
firm_comparison_by_county.columns = ['County', 'LEHD employment', 'SynthFirm employment']
firm_comparison_by_county.fillna(0, inplace = True)
plot_file_1 = os.path.join(plot_dir, 'emp_by_county_validation.png')
vk.plot_emp_comparison_scatter(firm_comparison_by_county,  'LEHD employment', 
                    'SynthFirm employment', 'County', plot_file_1)

# <codecell>

# comparison by CBG
# compare results by CBG
lehd_firm_by_cbg = lehd_validation.groupby('GEOID')[['total']].sum()
lehd_firm_by_cbg = lehd_firm_by_cbg.reset_index()

firm_in_study_area_by_cbg.drop(columns = 'MESOZONE', inplace = True)

lehd_firm_by_cbg.loc[:, 'GEOID'] = \
lehd_firm_by_cbg.loc[:, 'GEOID'].astype(np.int64).astype(str).str.zfill(12)

firm_in_study_area_by_cbg.loc[:, 'GEOID'] = \
firm_in_study_area_by_cbg.loc[:, 'GEOID'].astype(np.int64).astype(str).str.zfill(12)

firm_comparison_by_cbg = pd.merge(lehd_firm_by_cbg, 
                                     firm_in_study_area_by_cbg,
                                     on = 'GEOID',
                                     how = 'outer')
firm_comparison_by_cbg.columns = ['GEOID', 'LEHD employment', 'SynthFirm employment']
firm_comparison_by_cbg.fillna(0, inplace = True)

plot_file_2 = os.path.join(plot_dir, 'emp_by_cbg_validation.png')
vk.plot_emp_comparison_scatter(firm_comparison_by_cbg,  'LEHD employment', 
                    'SynthFirm employment', 'CBG', plot_file_2, alpha = 0.1)

# <codecell>

# comparison by industry

industry_lookup = {'11':'11', '21':'21', '22':'22', '23':'23',
                   '31':'31-33', '32':'31-33', '33':'31-33',
                   '42':'42', '44':'44-45', '45':'44-45', '4A': '44-45',
                   '48':'48-49', '49':'48-49', '51':'51',
                   '52':'52', '53':'53', '54':'54', '55':'55',
                   '56':'56', '61':'61', '62':'62', '71':'71',
                   '72':'72', '81':'81', '92':'92', 'S0': '92'}

selected_counties = firm_comparison_by_county.County.unique()
lehd_firm_by_industry = pd.melt(lehd_validation, id_vars = 'CBPZONE',
                               value_vars=list_of_attr, 
                                var_name='industry', value_name='emp')

lehd_firm_by_industry = lehd_firm_by_industry.reset_index()
lehd_firm_by_industry.loc[:, 'industry'] = \
lehd_firm_by_industry.loc[:, 'industry'].str.split('n').str[1]

lehd_firm_by_industry.loc[lehd_firm_by_industry['industry'] == '3133', 'industry'] = '31-33'
lehd_firm_by_industry.loc[lehd_firm_by_industry['industry'] == '4445', 'industry'] = '44-45'
lehd_firm_by_industry.loc[lehd_firm_by_industry['industry'] == '4849', 'industry'] = '48-49'
lehd_firm_by_industry = \
lehd_firm_by_industry.loc[lehd_firm_by_industry['CBPZONE'].isin(selected_counties)]


lehd_firm_by_industry = lehd_firm_by_industry.groupby(['industry'])[['emp']].sum()
print(lehd_firm_by_industry['emp'].sum())

synthfirm_in_study_area.loc[:, 'Industry_NAICS6_Make'] = \
    synthfirm_in_study_area.loc[:, 'Industry_NAICS6_Make'].astype(str)
synthfirm_in_study_area.loc[:, 'NAICS2'] = \
synthfirm_in_study_area.loc[:, 'Industry_NAICS6_Make'].str[:2]
print(synthfirm_in_study_area.loc[:, 'NAICS2'].unique())
synthfirm_in_study_area.loc[:, 'industry'] = \
synthfirm_in_study_area.loc[:, 'NAICS2'].map(industry_lookup)

firm_in_study_area_by_industry = \
    synthfirm_in_study_area.groupby(['industry'])[['Emp']].sum()
firm_in_study_area_by_industry = \
    firm_in_study_area_by_industry.reset_index()

firm_comparison_by_industry = pd.merge(lehd_firm_by_industry,
                                       firm_in_study_area_by_industry,
                                       on = 'industry', how = 'outer')

firm_comparison_by_industry.columns = ['Industry', 'LEHD employment', 'SynthFirm employment']
firm_comparison_by_industry.fillna(0, inplace = True)

plot_file_3 = os.path.join(plot_dir, 'emp_by_industry_validation.png')
vk.plot_emp_comparison_bar(firm_comparison_by_industry,  'LEHD employment', 
                    'SynthFirm employment', 'Industry', plot_file_3)

# <codecell>
#################################################
#### step 3 - visualize firm and employment  ####
#################################################
agg_level = 'MESOZONE'
# plot firm and employment results by CBG zone

region_map.loc[:, 'MESOZONE'] = \
region_map.loc[:, 'MESOZONE'].astype(np.int64).astype(str).str.zfill(12)
# plot firm and employment distribution
if focus_region is not None:
    firm_in_region = firm_by_zone.loc[firm_by_zone['FAFID'].isin([focus_region])]
else:
    firm_in_region = firm_by_zone.copy()
firm_in_region.loc[:, 'MESOZONE'] = \
firm_in_region.loc[:, 'MESOZONE'].astype(np.int64).astype(str).str.zfill(12)


region_map_with_firm = region_map.merge(firm_in_region, on = agg_level, 
                                        how='inner')
region_map_with_firm.loc[:, 'firm_per_area'] = \
region_map_with_firm.loc[:, 'firm_count'] / region_map_with_firm.loc[:, 'area']
region_map_with_firm.loc[:, 'emp_per_area'] = \
region_map_with_firm.loc[:, 'employment_count'] / region_map_with_firm.loc[:, 'area']

# <codecell>
from pygris.utils import shift_geometry

if region_code is None: # shift geometry for national run
    region_map_with_firm = shift_geometry(region_map_with_firm)

map_file_1 = os.path.join(plot_dir, 'region_firm_count.png')
vk.plot_region_map(region_map_with_firm, 'firm_per_area', 
                'Firm Density (firms/$km^{2}$)', # title
                    map_file_1, add_basemap = True, 
                    vmin=0, vmax=50)

map_file_2 = os.path.join(plot_dir, 'region_emp_count.png')
vk.plot_region_map(region_map_with_firm, 'emp_per_area', 
                'Employment Density (employees/$km^{2}$)', # title
                map_file_2, add_basemap = True, 
                    vmin=0, vmax=1000)


# <codecell>

# re-plot at county level
agg_level = 'CBPZONE'
# plot firm and employment results by CBG zone
us_counties.rename(columns = {'GEOID': 'CBPZONE'}, inplace = True)
us_counties.loc[:, 'CBPZONE'] = \
us_counties.loc[:, 'CBPZONE'].astype(np.int64).astype(str).str.zfill(5)
us_counties.loc[:, 'area'] = (us_counties.loc[:, 'ALAND'] + \
                              us_counties.loc[:, 'AWATER'])/ 1000000
# plot firm and employment distribution

firm_in_region.loc[:, 'CBPZONE'] = \
firm_in_region.loc[:, 'CBPZONE'].astype(np.int64).astype(str).str.zfill(5)

firm_in_region_ct = firm_in_region.groupby(agg_level)[['firm_count', 'employment_count']].sum()
firm_in_region_ct = firm_in_region_ct.reset_index()
county_map_with_firm = us_counties.merge(firm_in_region_ct, on = agg_level, 
                                        how='inner')
county_map_with_firm.loc[:, 'firm_per_area'] = \
county_map_with_firm.loc[:, 'firm_count'] / county_map_with_firm.loc[:, 'area']
county_map_with_firm.loc[:, 'emp_per_area'] = \
county_map_with_firm.loc[:, 'employment_count'] / county_map_with_firm.loc[:, 'area']

# <codecell>
map_file_1c = os.path.join(plot_dir, 'region_firm_count_ct.png')
vk.plot_county_map(county_map_with_firm, 'firm_per_area', 
                'Firm Density (firms/$km^{2}$)', # title
                    map_file_1c, logscale = True, 
                    vmin=0, vmax=100)

map_file_2c = os.path.join(plot_dir, 'region_emp_count_ct.png')
vk.plot_county_map(county_map_with_firm, 'emp_per_area', 
                'Employment Density (employees/$km^{2}$)', # title
                map_file_2c, logscale = True, 
                    vmin=0, vmax=1000)

# <codecell>

#########################################################
#### step 4 - visualize production and consumption ######
#########################################################

# plot production and consumption results by CBG zone
# production density

truck_modes = ['For-hire Truck', 'Private Truck']

prod_attr = ['SellerZone', 'mode_choice', 'ShipmentLoad']
cons_attr = ['BuyerZone', 'mode_choice', 'ShipmentLoad']
intr_attr = ['MESOZONE', 'mode_choice', 'ShipmentLoad']
if focus_region is not None:
    production_in_region = \
        synthfirm_output_domestic.loc[synthfirm_output_domestic['orig_FAFID'].isin([focus_region])]
    production_in_region = production_in_region[prod_attr]
    attraction_in_region = \
        synthfirm_output_domestic.loc[synthfirm_output_domestic['dest_FAFID'].isin([focus_region])]
    attraction_in_region = attraction_in_region[cons_attr]
else:
    production_in_region = synthfirm_output_domestic.copy()
    production_in_region = production_in_region[prod_attr]
    attraction_in_region = synthfirm_output_domestic.copy()
    attraction_in_region = attraction_in_region[cons_attr]

if port_analysis: # add international flow if generated
    production_in_region_int = \
        synthfirm_output_international.loc[synthfirm_output_international['Source'] == 'Import']
    production_in_region_int = \
        production_in_region_int.loc[production_in_region_int['orig_FAFID'].isin([focus_region])]
    production_in_region_int = production_in_region_int[intr_attr]
    production_in_region_int.rename(columns = {'MESOZONE': 'SellerZone'}, inplace = True)
    production_in_region = pd.concat([production_in_region, production_in_region_int])
    
    attraction_in_region_int = \
        synthfirm_output_international.loc[synthfirm_output_international['Source'] == 'Export']
    attraction_in_region_int = \
        attraction_in_region_int.loc[attraction_in_region_int['dest_FAFID'].isin([focus_region])]
    attraction_in_region_int = attraction_in_region_int[intr_attr]
    attraction_in_region_int.rename(columns = {'MESOZONE': 'BuyerZone'}, inplace = True)
    attraction_in_region = pd.concat([attraction_in_region, attraction_in_region_int])

if truck_only: # select truck for plotting
    production_in_region = \
        production_in_region.loc[production_in_region['mode_choice'].isin(truck_modes)]
    attraction_in_region = \
        attraction_in_region.loc[attraction_in_region['mode_choice'].isin(truck_modes)]
    
production_in_region = production_in_region.groupby(['SellerZone'])[['ShipmentLoad']].sum()
production_in_region = production_in_region.reset_index()
production_in_region = production_in_region.rename(columns = {'SellerZone': 'MESOZONE'})
production_in_region['MESOZONE'] = \
production_in_region['MESOZONE'].astype(np.int64).astype(str).str.zfill(12)
production_in_region.rename(columns = {'ShipmentLoad': 'production'}, inplace = True)

attraction_in_region = attraction_in_region.groupby(['BuyerZone'])[['ShipmentLoad']].sum()
attraction_in_region = attraction_in_region.reset_index()
attraction_in_region = attraction_in_region.rename(columns = {'BuyerZone': 'MESOZONE'})
attraction_in_region['MESOZONE'] = \
attraction_in_region['MESOZONE'].astype(np.int64).astype(str).str.zfill(12)
attraction_in_region.rename(columns = {'ShipmentLoad': 'consumption'}, inplace = True)

joint_commodity_flow = pd.merge(production_in_region,
                                attraction_in_region,
                                on = 'MESOZONE', how = 'outer')
joint_commodity_flow.fillna(0, inplace = True)
region_map_with_cf = \
region_map.merge(joint_commodity_flow, on='MESOZONE', how='inner')

if region_code is None: # shift geometry for national run
    region_map_with_cf = shift_geometry(region_map_with_cf)
# plot normalized production
region_map_with_cf.loc[:, 'production_per_area'] = \
region_map_with_cf.loc[:, 'production'] * 0.907185/ \
region_map_with_cf.loc[:, 'area']

region_map_with_cf.loc[:, 'consumption_per_area'] = \
region_map_with_cf.loc[:, 'consumption'] * 0.907185/ \
region_map_with_cf.loc[:, 'area']

# <codecell>
if truck_only:
    map_file_3 = os.path.join(plot_dir, 'region_production_truck.png')
else:
    map_file_3 = os.path.join(plot_dir, 'region_production_allmodes.png')
vk.plot_region_map(region_map_with_cf, 'production_per_area', 
                'Commodity Production (1000 tons/$km^{2}$)', # title
                    map_file_3, add_basemap = True, 
                    vmin=0, vmax=100)
# plot normalized production

if truck_only:
    map_file_4 = os.path.join(plot_dir, 'region_attraction_truck.png')
else:
    map_file_4 = os.path.join(plot_dir, 'region_attraction_allmodes.png')
    
vk.plot_region_map(region_map_with_cf, 'consumption_per_area', 
                'Commodity Attraction (1000 tons/$km^{2}$)', # title
                map_file_4, add_basemap = True, 
                    vmin=0, vmax=100)

# <codecell>

# re-plot at county level
agg_level = 'CBPZONE'

mesozone_to_county = mesozone_id_lookup[['MESOZONE', 'CBPZONE']]
mesozone_to_county.loc[:, 'MESOZONE'] = \
    mesozone_to_county.loc[:, 'MESOZONE'].astype(np.int64).astype(str).str.zfill(12)
mesozone_to_county.loc[:, 'CBPZONE'] = \
    mesozone_to_county.loc[:, 'CBPZONE'].astype(np.int64).astype(str).str.zfill(5)

production_in_region_ct = pd.merge(production_in_region, mesozone_to_county,
                                   on = 'MESOZONE', how = 'left')
production_in_region_ct = \
    production_in_region_ct.groupby(agg_level)[['production']].sum().reset_index()

attraction_in_region_ct = pd.merge(attraction_in_region, mesozone_to_county,
                                   on = 'MESOZONE', how = 'left')
attraction_in_region_ct = \
    attraction_in_region_ct.groupby(agg_level)[['consumption']].sum().reset_index()

joint_commodity_flow_ct = pd.merge(production_in_region_ct,
                                attraction_in_region_ct,
                                on = agg_level, how = 'outer')
joint_commodity_flow_ct.fillna(0, inplace = True)

# <codecell>
county_map_with_cf = \
us_counties.merge(joint_commodity_flow_ct, on = agg_level, how='inner')

# plot normalized production
county_map_with_cf.loc[:, 'production_per_area'] = \
county_map_with_cf.loc[:, 'production'] * 0.907185/ \
county_map_with_cf.loc[:, 'area']
if truck_only:
    map_file_3c = os.path.join(plot_dir, 'region_production_truck_ct.png')
else:
    map_file_3c = os.path.join(plot_dir, 'region_production_allmodes_ct.png')
vk.plot_county_map(county_map_with_cf, 'production_per_area', 
                'Commodity Production (1000 tons/$km^{2}$)', # title
                    map_file_3c, logscale = True, 
                    vmin=0, vmax=100)
# plot normalized production
county_map_with_cf.loc[:, 'consumption_per_area'] = \
county_map_with_cf.loc[:, 'consumption'] * 0.907185/ \
county_map_with_cf.loc[:, 'area']

if truck_only:
    map_file_4c = os.path.join(plot_dir, 'region_attraction_truck_ct.png')
else:
    map_file_4c = os.path.join(plot_dir, 'region_attraction_allmodes_ct.png')
vk.plot_county_map(county_map_with_cf, 'consumption_per_area', 
                'Commodity Attraction (1000 tons/$km^{2}$)', # title
                map_file_4c, logscale = True,
                    vmin=0, vmax=100)