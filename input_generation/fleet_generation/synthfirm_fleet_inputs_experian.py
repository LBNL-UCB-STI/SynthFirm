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

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')
path_to_output = 'PrivateData/registration/' # access for private data
cleaned_experian_file = os.path.join(path_to_output, 'cleaned_experian_data_national.csv')
experian_national_fleet = read_csv(cleaned_experian_file)

emp_file = 'SynthFirm_parameters/US_naics.csv'
emp_by_naics2 = read_csv(emp_file)

state_fips_file = 'SynthFirm_parameters/us-state-ansi-fips.csv'
state_fips = read_csv(state_fips_file)

# <codecell>

# check industry missing value
veh_ct_by_naics = \
    experian_national_fleet.groupby(['veh_class', 'naics_lvl3'])[['vehicle_count']].sum()
veh_ct_by_naics = veh_ct_by_naics.reset_index()

veh_ct_by_naics.loc[:, 'fraction'] = veh_ct_by_naics.loc[:, 'vehicle_count'] / \
    veh_ct_by_naics.groupby('veh_class')['vehicle_count'].transform('sum')

# <codecell>

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
emp_by_naics2_st.loc[:, 'state_abbr'] = emp_by_naics2_st.loc[:, 'state_abbr'].str[1:]
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
registration_data_by_st_ind = pd.pivot_table(experian_national_fleet, 
                                             values = 'vehicle_count',
                                             index = ['state_abbr', 'industry'],
                                             columns = 'veh_class',
                                             aggfunc = 'sum')
registration_data_by_st_ind = registration_data_by_st_ind.reset_index()
registration_data_by_st_ind = \
    registration_data_by_st_ind.loc[registration_data_by_st_ind['industry'] != '  ']
registration_data_by_st_ind.fillna(0, inplace = True)

# <codecell>

# calculate vehicle per emp
veh_per_emp = pd.merge(registration_data_by_st_ind, emp_by_naics2_st,
                       on = ['state_abbr', 'industry'], how = 'left')

for veh in synthfirm_veh_classes:
    out_attr = 'rate_' + veh
    veh_per_emp.loc[:, out_attr] = veh_per_emp.loc[:, veh] / \
    veh_per_emp.loc[:, 'emp_lehd']

summary_output = os.path.join(path_to_output, 'veh_per_emp_by_state.csv')
veh_per_emp.to_csv(summary_output, index = False)

# <codecell>

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
# ldt ownership
# classes_1_to_3 = ['Light-duty Class2B3', 'Light-duty Class12A']
# experian_class123 = \
#     experian_national_fleet.loc[experian_national_fleet['veh_class'].isin(classes_1_to_3)]
    
# ldt_ct_by_naics = experian_class123.groupby(['veh_class', 'naics_lvl3'])[['vehicle_count']].sum()
# ldt_ct_by_naics = ldt_ct_by_naics.reset_index()
# ldt_ct_by_naics.loc[:, 'Fraction'] = \
#     ldt_ct_by_naics.loc[:, 'vehicle_count'] / \
#         ldt_ct_by_naics.groupby('veh_class')['vehicle_count'].transform('sum')

# ldt_ct_by_naics = \
#     ldt_ct_by_naics.groupby('veh_class').apply(lambda x: x.sort_values('Fraction', ascending=False))  
# ldt_ct_by_naics = ldt_ct_by_naics[['veh_class', 'naics_lvl3', 'vehicle_count', 'Fraction']]
# <codecell>

# ldt_ct_by_naics = ldt_ct_by_naics.sort_values('Fraction', ascending = False)
# ldt_ct_by_naics.loc[:, 'cum_frac'] = \
#     ldt_ct_by_naics.groupby('veh_class')['Fraction'].transform('cumsum')


# <codecell>
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
summary_by_state = pd.pivot_table(national_fleet_by_state, 
                                  index = ['state_abbr', 'veh_class'],
                                  columns = 'service_type', values = 'vehicle_count',
                                  aggfunc='sum')
summary_by_state = summary_by_state.reset_index()

summary_output = os.path.join(path_to_output, 'experian_summary_by_state.csv')
summary_by_state.to_csv(summary_output, index = False)