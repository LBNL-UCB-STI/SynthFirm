#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 16 15:44:43 2025

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


import warnings
warnings.filterwarnings("ignore")

# load input

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')
path_to_output = 'PrivateData/registration/' # access for private data
cleaned_experian_file = os.path.join(path_to_output, 'cleaned_experian_data_national.csv')
experian_national_fleet = read_csv(cleaned_experian_file)

emp_file = 'SynthFirm_parameters/US_naics.csv'
emp_by_naics2 = read_csv(emp_file)

state_fips_file = 'SynthFirm_parameters/us-state-ansi-fips.csv'
state_fips = read_csv(state_fips_file)
state_fips.loc[:, 'stusps'] = state_fips.loc[:, 'stusps'].str[1:]

class1_ratio_file = 'SynthFirm_parameters/class1_ratio_vius.csv'
class1_ratio = read_csv(class1_ratio_file)

# <codecell>

# pre-processing class 1 ratio
class1_ratio = pd.merge(class1_ratio, state_fips, 
                        on = 'stname', how = 'left')
class1_ratio_short = class1_ratio[['stusps', 'ratio_class1']]
class1_ratio_short.rename(columns = {'stusps': 'state_abbr'}, inplace = True)
# <codecell>

# vehicle count by class and state --> quality check only
experian_national_fleet['assign_wt_class'] = experian_national_fleet['assign_wt_class'].astype(str)
print(experian_national_fleet['assign_wt_class'].unique())
veh_ct_by_class = \
    pd.pivot_table(experian_national_fleet, index = 'state_abbr', columns = 'assign_wt_class',
                   values = 'vehicle_count', aggfunc = 'sum')
    
veh_ct_by_class['state total'] = veh_ct_by_class.sum(axis=1)

# Add column sums as a new row
column_sums = veh_ct_by_class.sum(axis=0)
veh_ct_by_class.loc['class total'] = column_sums
veh_ct_by_class = veh_ct_by_class.reset_index()


veh_ct_by_class.to_csv(os.path.join(path_to_output, 'experian_state_class_summary.csv'), index = False)

# <codecell>

# check industry missing value

veh_ct_by_naics = \
    experian_national_fleet.groupby(['veh_class', 'naics_lvl3'])[['vehicle_count']].sum()
veh_ct_by_naics = veh_ct_by_naics.reset_index()

veh_ct_by_naics.loc[:, 'fraction'] = veh_ct_by_naics.loc[:, 'vehicle_count'] / \
    veh_ct_by_naics.groupby('veh_class')['vehicle_count'].transform('sum')

# <codecell>

# OUTPUT 1 -- calculate fleet size per employment

# load employment 
emp_attr = [ 'n11', 'n21', 'n22', 'n23', 'n3133', 'n42', 'n4445', 'n4849',
       'n51', 'n52', 'n53', 'n54', 'n55', 'n56', 'n61', 'n62', 'n71', 'n72',
       'n81', 'n92']

emp_by_naics2.loc[:, 'GEOID'] = emp_by_naics2.loc[:, 'GEOID'].astype(str).str.zfill(12)
emp_by_naics2.loc[:, 'st'] = emp_by_naics2.loc[:, 'GEOID'].str[0:2]

# generate employment by 2-digit NAICS code

emp_by_naics2_st = \
    emp_by_naics2.groupby(['st'])[emp_attr].sum()

emp_by_naics2_st = emp_by_naics2_st.reset_index()

emp_by_naics2_st = pd.melt(emp_by_naics2_st, id_vars = ["st"],
                               value_vars = emp_attr,
                                  var_name= 'industry', value_name='emp_lehd')

emp_by_naics2_st = emp_by_naics2_st.reset_index()
emp_by_naics2_st.loc[:, 'industry'] = \
    emp_by_naics2_st.loc[:, 'industry'].str.split('n').str[1]
emp_by_naics2_st.drop(columns = ['index'], inplace = True)

state_fips.loc[:, 'st'] = state_fips.loc[:, 'st'].astype(str).str.zfill(2)
state_fips_short = state_fips[['st', 'stusps']]
emp_by_naics2_st = pd.merge(emp_by_naics2_st, state_fips_short,
                            on = 'st', how = 'left')
emp_by_naics2_st.rename(columns = {'stusps': 'state_abbr'}, inplace = True)

# <codecell>

# generate experian vehicle count by state and industry
synthfirm_veh_classes = experian_national_fleet.veh_class.unique()
experian_national_fleet.dropna(subset = ['naics_lvl3'], inplace = True)
experian_national_fleet.loc[:, 'industry'] = \
    experian_national_fleet.loc[:, 'naics_lvl3'].astype(str).str[0:2] 
    
experian_national_fleet.loc[experian_national_fleet['industry'].isin(["31", "32", "33"]), 'industry'] = "3133"
experian_national_fleet.loc[experian_national_fleet['industry'].isin(["44", "45"]), 'industry'] = "4445"
experian_national_fleet.loc[experian_national_fleet['industry'].isin(["48", "49"]), 'industry'] = "4849"

# <codecell>

# merge fleet and employment to calculate fleet per employment
registration_data_by_st_ind = pd.pivot_table(experian_national_fleet, 
                                             values = 'vehicle_count',
                                             index = ['state_abbr', 'industry'],
                                             columns = 'veh_class',
                                             aggfunc = 'sum')
registration_data_by_st_ind = registration_data_by_st_ind.reset_index()
registration_data_by_st_ind = \
    registration_data_by_st_ind.loc[registration_data_by_st_ind['industry'] != '  ']
registration_data_by_st_ind.fillna(0, inplace = True)

# add ratio for class 1
registration_data_by_st_ind = pd.merge(registration_data_by_st_ind, class1_ratio_short,
                                       on = 'state_abbr', how = 'left')

# NH has missing class 1 frac, fill in with mean values
registration_data_by_st_ind['ratio_class1'].fillna(registration_data_by_st_ind['ratio_class1'].mean(), inplace = True)

# inflate class 12A count using ratio of 1/2A
print(registration_data_by_st_ind.loc[:, 'Light-duty Class12A'].sum())
registration_data_by_st_ind.loc[:, 'Light-duty Class12A'] = \
    registration_data_by_st_ind.loc[:, 'Light-duty Class12A'] * \
        (1 + registration_data_by_st_ind.loc[:, 'ratio_class1'])
registration_data_by_st_ind.loc[:, 'Light-duty Class12A'] = \
    registration_data_by_st_ind.loc[:, 'Light-duty Class12A'].astype(int)
print(registration_data_by_st_ind.loc[:, 'Light-duty Class12A'].sum())

# <codecell>

# calculate vehicle per emp and writing output
veh_per_emp = pd.merge(registration_data_by_st_ind, emp_by_naics2_st,
                       on = ['state_abbr', 'industry'], how = 'left')

for veh in synthfirm_veh_classes:
    out_attr = 'rate_' + veh
    veh_per_emp.loc[:, out_attr] = veh_per_emp.loc[:, veh] / \
    veh_per_emp.loc[:, 'emp_lehd']

summary_output = os.path.join(path_to_output, 'veh_per_emp_by_state.csv')
veh_per_emp.to_csv(summary_output, index = False)


#### END OF OUTPUT 1 -- Fleet size per employment #####
# <codecell>


## plot results from output 1
# plotting veh per emp distribution
naics_2_digit_codes = {
    '11': "Agriculture",
    '21': "Mining",
    '22': "Utilities",
    '23': "Construction",
    '3133': "Manufacturing",
    '42': "Wholesale",
    '4445': "Retail",    
    '4849': "Transportation",
    '51': "Information",
    '52': "Finance",
    '53': "Real Estate",
    '54': "Professional Services",
    '55': "Management",
    '56': "Administrative Services",
    '61': "Education",
    '62': "Health Care",
    '71': "Entertainment",
    '72': "Food Services",
    '81': "Other Services",
    '92': "Public Administration"
}

veh_per_emp.loc[:, 'ind_desc'] = veh_per_emp.loc[:, 'industry'].map(naics_2_digit_codes)
for veh in synthfirm_veh_classes:
    plot_attr = 'rate_' + veh
    plt.figure(figsize = (8,4))
    sns.boxplot(veh_per_emp, x = 'ind_desc',
                y = plot_attr, showfliers = False)
    
    plt.xticks(rotation = 60, ha = 'right')
    plt.ylabel('vehicle per employee')
    plt.xlabel('')
    plt.title(veh)
    plt.savefig(os.path.join(path_to_output, 'plot', 'veh_per_emp_' + veh + '.png'), dpi = 300,
                bbox_inches = 'tight')
    plt.show()


# <codecell>

# OUTPUT 2 -- AGE DISTRIBUTION BY STATE 

# aggregate data to state level
experian_national_fleet.loc[:, 'service_type'] = 'PRIVATE'
experian_national_fleet.loc[experian_national_fleet['naics_lvl3'].isin(['484', '492']), 'service_type'] = 'FOR HIRE'
experian_national_fleet.loc[experian_national_fleet['naics_lvl3'].isin(['532']), 'service_type'] = 'LEASE'
experian_national_fleet.loc[:, 'service type'] = 'PRIVATE'
grouping_var = ['state_abbr', 'veh_class', 'age', 'AGE_BIN', 'service_type', 'fuel_ty']
national_fleet_by_state = \
    experian_national_fleet.groupby(grouping_var)[['vehicle_count']].sum()

national_fleet_by_state = national_fleet_by_state.reset_index()

# <codecell>

# drop fuel -- create age distribution and check variation across state and service type
grouping_var = ['state_abbr', 'veh_class', 'age', 'AGE_BIN', 'service_type']
national_age_distribution = national_fleet_by_state.groupby(grouping_var)[['vehicle_count']].sum()
national_age_distribution = national_age_distribution.reset_index()

age_bins = national_age_distribution.AGE_BIN.unique()
veh_classes = national_age_distribution.veh_class.unique()
service_types = national_age_distribution.service_type.unique()

# age distribution by state
state_age_summary = pd.pivot_table(national_age_distribution,
                                      index = ['state_abbr', 'veh_class'], columns='AGE_BIN',
                                      values = 'vehicle_count', aggfunc='sum')
state_age_summary.loc[:,'total'] = state_age_summary.sum(axis = 1)
state_age_summary.loc[:, age_bins] = \
state_age_summary.loc[:, age_bins].divide(state_age_summary.loc[:,'total'], axis = 0)
state_age_summary = state_age_summary.reset_index()
# plt.xticks(rotation = 60, ha = 'right')
for veh in veh_classes:
    state_age_plotting = state_age_summary.loc[state_age_summary['veh_class'] == veh]

    state_age_plotting.set_index('state_abbr', inplace = True)
    state_age_plotting.drop(columns = 'total', inplace = True)
    state_age_plotting = state_age_plotting.loc[:, age_bins]
    state_age_plotting.plot(kind = 'barh', stacked = True, cmap = 'plasma_r',
                              figsize=(6, 10))
    plt.legend(title = 'Age bin', bbox_to_anchor = (1.01, 0.8))
    plt.xlabel('Vehicle fraction')
    plt.ylabel('Registration state')
    plt.title(veh)
    plt.savefig(os.path.join(path_to_output, 'plot', veh + '_age_distribution_by_state.png'),
               dpi = 300, bbox_inches = 'tight')
    plt.show()
    
# age distribution by service type

service_age_summary = pd.pivot_table(national_age_distribution,
                                      index = ['service_type', 'veh_class'], columns='AGE_BIN',
                                      values = 'vehicle_count', aggfunc='sum')
service_age_summary.loc[:,'total'] = service_age_summary.sum(axis = 1)
service_age_summary.loc[:, age_bins] = \
service_age_summary.loc[:, age_bins].divide(service_age_summary.loc[:,'total'], axis = 0)
service_age_summary = service_age_summary.reset_index()
# plt.xticks(rotation = 60, ha = 'right')
for veh in veh_classes:
    service_age_plotting = service_age_summary.loc[service_age_summary['veh_class'] == veh]

    service_age_plotting.set_index('service_type', inplace = True)
    service_age_plotting.drop(columns = 'total', inplace = True)
    service_age_plotting = service_age_plotting.loc[:, age_bins]
    service_age_plotting.plot(kind = 'bar', stacked = True, cmap = 'plasma_r')
    plt.legend(title = 'Age bin', bbox_to_anchor = (1.01, 0.8))
    plt.xlabel('Service type')
    plt.ylabel('Vehicle fraction')
    plt.title(veh)
    plt.savefig(os.path.join(path_to_output, 'plot', veh + '_age_distribution_by_service.png'),
               dpi = 300, bbox_inches = 'tight')
    plt.show()

# write output
national_age_distribution.to_csv(os.path.join(path_to_output, 
                                              'experian_national_age_distribution.csv'))

#### END OF OUTPUT 2 -- AGE DISTRIBUTION #####
# <codecell>

# OUTPUT 3 -- FUEL MIX BY STATE 
fuel_type_mapping = {'Diesel':'Diesel', 
                     'Gasoline': 'Gasoline', 
                     'Natural Gas':'Natural Gas', 
                     'Unknown':'Other', 
                     'LPG': 'Other', 
                     'Biodiesel':'Diesel',
                     'Electric': 'Electric', 
                     'Flex Fuel': 'Gasoline', 
                     'Gas-Electric': 'Gasoline', 
                     'Diesel-Electric': 'Diesel'}

national_fleet_by_state.loc[:, 'fuel_type'] = \
    national_fleet_by_state.loc[:, 'fuel_ty'].map(fuel_type_mapping)
grouping_var = ['state_abbr', 'veh_class', 'fuel_type', 'service_type']
national_fuel_distribution = national_fleet_by_state.groupby(grouping_var)[['vehicle_count']].sum()
national_fuel_distribution = national_fuel_distribution.reset_index()

fuel_types = national_fuel_distribution.fuel_type.unique()
service_types = national_fuel_distribution.service_type.unique()

# age distribution by state
state_fuel_summary = pd.pivot_table(national_fuel_distribution,
                                      index = ['state_abbr', 'veh_class'], 
                                      columns='fuel_type',
                                      values = 'vehicle_count', aggfunc='sum')
state_fuel_summary.loc[:,'total'] = state_fuel_summary.sum(axis = 1)
state_fuel_summary.loc[:, fuel_types] = \
state_fuel_summary.loc[:, fuel_types].divide(state_fuel_summary.loc[:,'total'], axis = 0)
state_fuel_summary = state_fuel_summary.reset_index()
# plt.xticks(rotation = 60, ha = 'right')
for veh in veh_classes:
    state_fuel_plotting = state_fuel_summary.loc[state_fuel_summary['veh_class'] == veh]

    state_fuel_plotting.set_index('state_abbr', inplace = True)
    state_fuel_plotting.drop(columns = 'total', inplace = True)

    state_fuel_plotting.plot(kind = 'barh', stacked = True, cmap = 'Set2',
                              figsize=(6, 10))
    plt.legend(title = 'Fuel type', bbox_to_anchor = (1.01, 0.8))
    plt.xlabel('Vehicle fraction')
    plt.ylabel('Registration state')
    plt.title(veh)
    plt.savefig(os.path.join(path_to_output, 'plot', veh + '_fuel_distribution_by_state.png'),
               dpi = 300, bbox_inches = 'tight')
    plt.show()
    
# age distribution by service type

service_fuel_summary = pd.pivot_table(national_fuel_distribution,
                                      index = ['service_type', 'veh_class'], 
                                      columns='fuel_type',
                                      values = 'vehicle_count', aggfunc='sum')
service_fuel_summary.loc[:,'total'] = service_fuel_summary.sum(axis = 1)
service_fuel_summary.loc[:, fuel_types] = \
service_fuel_summary.loc[:, fuel_types].divide(service_fuel_summary.loc[:,'total'], axis = 0)
service_fuel_summary = service_fuel_summary.reset_index()
# plt.xticks(rotation = 60, ha = 'right')
for veh in veh_classes:
    service_fuel_plotting = service_fuel_summary.loc[service_fuel_summary['veh_class'] == veh]

    service_fuel_plotting.set_index('service_type', inplace = True)
    service_fuel_plotting.drop(columns = 'total', inplace = True)
    service_fuel_plotting.plot(kind = 'bar', stacked = True, cmap = 'Set2')
    plt.legend(title = 'Fuel type', bbox_to_anchor = (1.01, 0.8))
    plt.xlabel('Service type')
    plt.ylabel('Vehicle fraction')
    plt.title(veh)
    plt.savefig(os.path.join(path_to_output, 'plot', veh + '_fuel_distribution_by_service.png'),
               dpi = 300, bbox_inches = 'tight')
    plt.show()

# <codecell>

# write fuel mix output
to_keep = ['Diesel', 'Gasoline']
national_fuel_distribution_out = \
    national_fuel_distribution.loc[national_fuel_distribution['fuel_type'].isin(to_keep)]

national_fuel_distribution_out.loc[:, 'fuel_fraction'] = \
    national_fuel_distribution_out.loc[:, 'vehicle_count'] / \
        national_fuel_distribution_out.groupby(['state_abbr', 'veh_class', 'service_type'])['vehicle_count'].transform('sum')
national_fuel_distribution_out.drop(columns = ['vehicle_count'], inplace = True)
# write output
national_fuel_distribution_out.to_csv(os.path.join(path_to_output, 
                                              'experian_national_fuel_distribution.csv'))


# <codecell>

# OUTPUT 3 - FUEL TYPE DISTRIBUTION #
summary_by_state = pd.pivot_table(national_fleet_by_state, 
                                  index = ['state_abbr', 'veh_class'],
                                  columns = 'service_type', values = 'vehicle_count',
                                  aggfunc='sum')
summary_by_state = summary_by_state.reset_index()

summary_output = os.path.join(path_to_output, 'experian_summary_by_state.csv')
summary_by_state.to_csv(summary_output, index = False)