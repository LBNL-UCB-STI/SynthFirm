#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 18 10:45:56 2024

@author: xiaodanxu
"""

import pandas as pd
from pandas import read_csv
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
from os import listdir
from pygris import states
import geopandas as gpd

import warnings
warnings.filterwarnings("ignore")

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')

# load registration data
path_to_vius = 'RawData/US_VIUS_2021'
path_to_moves = 'RawData/MOVES'
path_to_experian = 'PrivateData/registration/experian/finalCleanedExperian' # access for private data

age_bin = [-1, 3, 5, 7, 9, 14, 19, 50]

age_bin_label = ['age<=3', '3<age<=5','5<age<=7', 
                 '7<age<=9', '9<age<=14', 
                 '14<age<=19', 'age>=20']

###### load parameters #########

# assign 2a/2b to class 2
vius_2a2b_split = read_csv(os.path.join(path_to_vius, 'parameter',
                                        'class_2a2b_by_body_type.csv'))
vius_2a2b_split = \
vius_2a2b_split.groupby(['BTYPE', 'GVWR_CLASS'])[['TABWEIGHT']].sum()
vius_2a2b_split = vius_2a2b_split.reset_index()
vius_2a2b_split.loc[:, 'Fraction'] = vius_2a2b_split.loc[:, 'TABWEIGHT']/ \
vius_2a2b_split.groupby('BTYPE')['TABWEIGHT'].transform('sum')
print(vius_2a2b_split.groupby('GVWR_CLASS')['TABWEIGHT'].sum())

# load fraction of commercial vehicles
vius_commercial_fraction = read_csv(os.path.join(path_to_vius, 'parameter',
                                        'commercial_use_by_class.csv'))

vius_commercial_fraction = \
vius_commercial_fraction.loc[vius_commercial_fraction['BUSRELATED'] == 'Some commercial activity']

vius_commercial_fraction = vius_commercial_fraction[['GVWR_CLASS', 'AGE_BIN', 
                                                     'com_fraction']]

# <codecell>
# load US state boundary
from pygris.utils import shift_geometry


analysis_year = 2023 # pre-determined

body_type_to_drop = ['Bus - Non-School',
                     'Bus - School', 
                     'Fire Truck', 'Motor Home', 
                     'Military', 'Unknown']

us_states = states(year = analysis_year)
exclude_st = ['PR', 'GU', 'MP', 'VI', 'AS']
us_states = us_states.loc[~us_states['STUSPS'].isin(exclude_st)]
us_states = shift_geometry(us_states)

us_states.plot()

from pygris import counties
us_counties = counties(year = analysis_year)

# <codecell>
states_fips = us_states.STUSPS.unique()

combined_national_fleet = None

body_type_to_drop = ['Bus - Non-School',
                     'Bus - School', 
                     'Fire Truck', 'Motor Home', 
                     'Military', 'Unknown']

sale_type_to_drop = ['Government - County', 'Government - Local',
'Government - Federal', 'Government - Federal', 'Government - Other',
'Fleet - Taxi', 'Government - State', 'Government - Police']

van_types = [ 'Cargo Van', 'Passenger Van', 'Van', 'Box Van']

class_label_mapping = {'Pickup': 'Vocational', 
                   'Chassis Cab': 'Vocational', 
                   'Cargo Van': 'Vocational', 
                   'Passenger Van': 'Vocational',
                   'Cutaway': 'Vocational', 
                   'Straight Truck': 'Vocational',
                   'Tractor': 'Tractor', 
                   'SUV': 'Vocational', 
                   'Step Van': 'Vocational', 
                   'Dump': 'Vocational', 
                   'Glider':'Tractor', 
                   'Van': 'Vocational',     
                   'Box Van':'Vocational', 
                   'Concrete': 'Vocational'}

ldt1_classes = ['2A']
ldt2_classes = ['2B', '3']
md_classes = ['4', '5', '6']
hd_classes = ['7', '8']

for state in  states_fips:
    print(state)
    file_to_read = 'experian_' + state + '_cleaned.csv'
    experian_data = read_csv(os.path.join(path_to_experian, file_to_read))
    print('unfiltered vehicle count:')
    print(experian_data.vehicle_count.sum())
    
    # filter out fleet not within VIUS scope --> then we can apply VIUS based fractions
    experian_data = \
        experian_data.loc[~experian_data['body_style_cleaned'].isin(body_type_to_drop)]
    experian_data = \
        experian_data.loc[~experian_data['sales_cat_level3_tx'].isin(sale_type_to_drop)]
    print('vehicle count after dropping OOS vehicles:')
    print(experian_data.vehicle_count.sum())
    
    # assign class 2 to 2a/2b based on fraction by body type
    experian_data.loc[:,'body_type'] = experian_data.loc[:,'body_style_cleaned']
    experian_data.loc[:,'age'] = analysis_year - experian_data.loc[:,'model_year']
    experian_data.loc[experian_data['body_type'].isin(van_types), 'body_type'] = 'Van'
    experian_data_class2 = experian_data.loc[experian_data['assign_wt_class'] == 2]
    experian_data_class3above = experian_data.loc[experian_data['assign_wt_class'] > 2]
    
    class2_body_type_to_drop = ['Concrete', 'Straight Truck', 'Tractor']
    experian_data_class2 = \
    experian_data_class2.loc[~experian_data_class2['body_type'].isin(class2_body_type_to_drop)]
    # print(experian_data_class2['body_type'].unique())
    # print(experian_data_class2.groupby('body_type')['vehicle_count'].sum())
    
    experian_data_class2_assigned = None
    body_types = experian_data_class2.body_type.unique()
    for bt in body_types:
        # print(bt)
        experian_data_selected = \
        experian_data_class2.loc[experian_data_class2['body_type'] == bt].reset_index()
        fraction_select = \
        vius_2a2b_split.loc[vius_2a2b_split['BTYPE'] == bt]
        options = fraction_select.GVWR_CLASS.to_numpy()
        probability = fraction_select.Fraction.to_numpy()
        # assign 2a 2b
        sample_size = len(experian_data_selected)
        experian_data_selected.loc[:, 'class2'] = \
        pd.Series(np.random.choice(options, size = sample_size, p = probability) )
        experian_data_class2_assigned = pd.concat([experian_data_class2_assigned,
                                                   experian_data_selected])
        # break
    # print(experian_data_class2_assigned.groupby('class2')[['vehicle_count']].sum())
    sum_2a = \
    experian_data_class2_assigned.loc[experian_data_class2_assigned['class2'] == '2A', 'vehicle_count'].sum()
    sum_2b = \
    experian_data_class2_assigned.loc[experian_data_class2_assigned['class2'] == '2B', 'vehicle_count'].sum()
    print(sum_2a / sum_2b)
    experian_data_class2_assigned.drop(columns = ['index'], inplace = True)
    
    experian_data_class2_assigned.loc[:, 'assign_wt_class']= \
        experian_data_class2_assigned.loc[:, 'class2']

    experian_data_class2_assigned.drop(columns = ['class2'], inplace = True)
    experian_data_processed = pd.concat([experian_data_class2_assigned,
                                    experian_data_class3above])
    print('vehicle count after assigning 2a and 2b trucks:')
    print(experian_data_processed.vehicle_count.sum())
    
    # selecting commercial vehicles from the sample
    experian_data_processed.loc[:, 'AGE_BIN'] = \
        pd.cut(experian_data_processed.loc[:, 'age'],
               bins = age_bin, labels = age_bin_label)

    experian_data_processed['assign_wt_class'] = \
    experian_data_processed['assign_wt_class'].astype(str)

    experian_data_com_sample = pd.merge(experian_data_processed,
                                       vius_commercial_fraction,
                                       left_on = ['assign_wt_class', 'AGE_BIN'],
                                       right_on = ['GVWR_CLASS', 'AGE_BIN'],
                                       how = 'left')
    experian_data_com_sample.drop(columns = ['GVWR_CLASS'], inplace = True)
    
    experian_data_com_sample['randnum'] = np.random.rand(len(experian_data_com_sample))
    experian_data_com_sample.loc[:, 'com_use'] = 0
    com_idx = (experian_data_com_sample['randnum'] <= experian_data_com_sample['com_fraction'])
    experian_data_com_sample.loc[com_idx, 'com_use'] = 1



      
    # check realized commercial vehicles
    com_veh_by_class = \
    experian_data_com_sample.groupby(['assign_wt_class', 'com_use'])[['vehicle_count']].sum()
    
    com_veh_by_class = com_veh_by_class.reset_index()
    com_veh_by_class.loc[:, 'com_fraction'] = \
    com_veh_by_class.loc[:, 'vehicle_count'] / \
    com_veh_by_class.groupby('assign_wt_class')['vehicle_count'].transform('sum')
    
    com_veh_by_class = com_veh_by_class.loc[com_veh_by_class['com_use'] == 1]
    print(com_veh_by_class)    
    experian_data_com_sample = \
    experian_data_com_sample.loc[experian_data_com_sample['com_use'] == 1]
    experian_data_com_sample.drop(columns = ['com_fraction', 'randnum', 'com_use'],
                                 inplace = True)
    
    print('vehicle count after select commercial use:')
    print(experian_data_com_sample.vehicle_count.sum())
    
    # assign synthfirm type and hpms type


    experian_data_com_sample.loc[:, 'body_type'] = \
    experian_data_com_sample.loc[:, 'body_style_cleaned'].map(class_label_mapping)
    experian_data_com_sample.loc[:, 'veh_class'] = 'Light-duty Class12A'
    
    ldt2_criteria = (experian_data_com_sample['assign_wt_class'].isin(ldt2_classes))
    experian_data_com_sample.loc[ldt2_criteria, 'veh_class'] = 'Light-duty Class2B3'
    
    md_criteria = (experian_data_com_sample['assign_wt_class'].isin(md_classes))
    experian_data_com_sample.loc[md_criteria, 'veh_class'] = 'Medium-duty Vocational'
    
    hd_voc_criteria = (experian_data_com_sample['assign_wt_class'].isin(hd_classes) & \
                       (experian_data_com_sample['body_type'] == 'Vocational'))
    experian_data_com_sample.loc[hd_voc_criteria,'veh_class'] = 'Heavy-duty Vocational'
    
    hd_trc_criteria = (experian_data_com_sample['assign_wt_class'].isin(hd_classes) & \
                       (experian_data_com_sample['body_type'] == 'Tractor'))
    experian_data_com_sample.loc[hd_trc_criteria,'veh_class'] = 'Heavy-duty Tractor'
    # print(experian_data_com_sample.veh_class.unique())
    
    experian_data_com_sample.loc[:, 'HPMS_Class'] = 'Light-duty vehicle'
    sut_criteria = (experian_data_com_sample['veh_class'].isin(['Medium-duty Vocational',
                                                              'Heavy-duty Vocational']))
    experian_data_com_sample.loc[sut_criteria, 'HPMS_Class'] = 'Single-unit truck'
    ct_criteria = (experian_data_com_sample['veh_class'].isin(['Heavy-duty Tractor']))
    experian_data_com_sample.loc[ct_criteria, 'HPMS_Class'] = 'Combination truck'    
    combined_national_fleet = pd.concat([combined_national_fleet, 
                                         experian_data_com_sample])
    # break
# <codecell>

path_to_output = 'PrivateData/registration/' # access for private data
combined_national_fleet.to_csv(os.path.join(path_to_output, 'cleaned_experian_data_national.csv'), index= False)

# <codecell>

# structure state-level count for validation
validation_file = 'VIUS_truck_count_by_state.csv'
national_truck_count_vius = \
read_csv(os.path.join(path_to_vius, validation_file))


national_count_by_class_st = \
    combined_national_fleet.groupby(['state_abbr', 'assign_wt_class'])[['vehicle_count']].sum()
national_count_by_class_st = national_count_by_class_st.reset_index()
national_count_by_class_st.columns = ['state', 'GVWR_class', 'vehicle_count']
veh_classes = national_count_by_class_st.GVWR_class.unique()

national_truck_count_vius.loc[:, 'REGSTATE'] = \
    national_truck_count_vius.loc[:, 'REGSTATE'].str.upper()
us_states.loc[:, 'NAME'] = us_states.loc[:, 'NAME'].str.upper()

national_truck_count_vius = pd.merge(national_truck_count_vius,
                                      us_states, left_on = 'REGSTATE',
                                      right_on = 'NAME', how = 'left')

national_truck_count_vius = pd.melt(national_truck_count_vius,
                                    id_vars = ['STUSPS'], value_vars = veh_classes,
                                    var_name = 'GVWR_class', value_name='vehicle_count')


national_truck_count_vius = national_truck_count_vius.reset_index()
national_truck_count_vius.drop(columns = ['index'], inplace = True)
national_truck_count_vius.columns = ['state', 'GVWR_class', 'vehicle_count']
# <codecell>
# compare results

national_count_by_class_st.loc[:, 'source'] = 'Experian'
national_truck_count_vius.loc[:, 'source'] = '2021 US VIUS'
compare_truck_count = pd.concat([national_count_by_class_st,
                                 national_truck_count_vius])
# compare_truck_count = \
#     compare_truck_count.loc[compare_truck_count['GVWR_class'].isin(veh_classes)]
for state in  states_fips:
    print(state)
    compare_truck_count_sel = \
        compare_truck_count.loc[compare_truck_count['state'] == state]
    sns.barplot(compare_truck_count_sel, x = 'GVWR_class', y = 'vehicle_count', 
                hue = 'source')
    plt.title(state)
    plt.savefig(os.path.join(path_to_output, 'plot', 'validation', 
                             'experian_count_validation_' + state + '.png'), 
                dpi = 300, bbox_inches = 'tight')
    plt.show()
        
    # break

# <codecell>
# plot state-level results

national_count_by_type_st = \
    combined_national_fleet.groupby(['state_abbr', 'veh_class'])[['vehicle_count']].sum()
national_count_by_type_st = national_count_by_type_st.reset_index()
national_count_by_type_st.columns = ['state', 'veh_type', 'vehicle_count']
veh_types = national_count_by_type_st.veh_type.unique()

national_count_by_type_st = pd.pivot_table(national_count_by_type_st,
                                           values ='vehicle_count', 
                                           index = 'state', 
                                           columns = 'veh_type', aggfunc='sum', 
                                           fill_value = 0)
national_count_by_type_st = national_count_by_type_st.astype(int)
national_count_by_type_st = national_count_by_type_st.reset_index()

us_states.rename(columns = {'STUSPS':'state'}, inplace = True)
us_fleet_to_plot = us_states.merge(national_count_by_type_st, on = 'state',
                                   how = 'left')

for vt in veh_types:

    ax = us_fleet_to_plot.plot(column = vt,
                               cmap='viridis',
                               alpha = 0.6, 
                               linewidth=0.01, 
                               legend=True, figsize = (8,6),
                               legend_kwds = {'shrink': 0.5})
    us_states.plot(ax = ax, facecolor='none', edgecolor='k',linewidth = 0.5)
    plt.title(vt)
    ax.set_axis_off()
    plt.savefig(os.path.join(path_to_output, 'plot', 
                             'experian_count_state_for_' + vt + '.png'), 
                dpi = 300, bbox_inches = 'tight')
    plt.show()
    
# <codecell>

us_states_df = pd.DataFrame(us_states.drop(columns = ['geometry']))
us_states_df = us_states_df[['STATEFP', 'state']]
us_counties = us_counties.merge(us_states_df, on = 'STATEFP', how = 'left')
us_counties['NAME'] = us_counties['NAME'].str.upper()

# <codecell>
national_count_by_type_ct = \
    combined_national_fleet.groupby(['state_abbr', 'county_name', 'veh_class'])[['vehicle_count']].sum()
national_count_by_type_ct = national_count_by_type_ct.reset_index()
national_count_by_type_ct.columns = ['state', 'NAME', 'veh_type', 'vehicle_count']
veh_types = national_count_by_type_ct.veh_type.unique()

national_count_by_type_ct = pd.pivot_table(national_count_by_type_ct,
                                           values ='vehicle_count', 
                                           index = ['state', 'NAME'], 
                                           columns = 'veh_type', aggfunc='sum', 
                                           fill_value = 0)
national_count_by_type_ct = national_count_by_type_ct.astype(int)
national_count_by_type_ct = national_count_by_type_ct.reset_index()
national_count_by_type_ct.loc[:, 'NAME'] = \
    national_count_by_type_ct.loc[:, 'NAME'].str.strip()
# <codecell>
# from fuzzywuzzy import fuzz
# from fuzzywuzzy import process

# us_counties_df = pd.DataFrame(us_counties.drop(columns = ['geometry']))
# print(us_counties_df.columns)
# us_counties_df = us_counties_df[['state', 'NAME']]


import matplotlib
# plot county level results
us_counties = shift_geometry(us_counties)
us_counties_to_plot = us_counties.merge(national_count_by_type_ct, 
                                on = ['state', 'NAME'], how = 'left')
for vt in veh_types:

    ax = us_counties_to_plot.plot(column = vt,
                                cmap='viridis',
                                alpha = 0.6, 
                                linewidth=0.01, 
                                legend=True, figsize = (8,6),
                                norm=matplotlib.colors.LogNorm(vmin=1, 
                                                              vmax = us_counties_to_plot[vt].max()),
                                legend_kwds = {'shrink': 0.5}) 
    plt.title(vt)
    us_states.plot(ax = ax, facecolor='none', edgecolor='k',linewidth = 0.5)
    ax.set_axis_off()
    plt.savefig(os.path.join(path_to_output, 'plot', 
                              'experian_count_county_for_' + vt + '.png'), 
                dpi = 300, bbox_inches = 'tight')
    
    plt.show()



