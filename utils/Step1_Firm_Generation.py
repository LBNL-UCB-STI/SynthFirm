#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 16 10:29:16 2024

@author: xiaodanxu
"""

import pandas as pd
import os
import numpy as np
from pandas import read_csv

print("Start synthetic firm generation...")

########################################################
#### step 1 - configure environment and load inputs ####
########################################################

# load model config temporarily here
scenario_name = 'Seattle'
out_scenario_name = 'Seattle'
file_path = '/Users/xiaodanxu/Documents/SynthFirm.nosync'
parameter_dir = 'SynthFirm_parameters'
input_dir = 'inputs_' + scenario_name
output_dir = 'outputs_' + out_scenario_name

cbp_file = 'data_emp_cbp_imputed.csv'
mzemp_file = 'data_mesozone_emprankings.csv'
c_n6_n6io_sctg_file = 'corresp_naics6_n6io_sctg_revised.csv'
employment_per_firm_file = 'employment_by_firm_size_naics.csv'
employment_per_firm_gapfill_file = 'employment_by_firm_size_gapfill.csv'


# load model inputs
cbp = read_csv(os.path.join(file_path, input_dir, cbp_file))
mzemp = read_csv(os.path.join(file_path, input_dir, mzemp_file))

c_n6_n6io_sctg = read_csv(os.path.join(file_path, parameter_dir, c_n6_n6io_sctg_file))
employment_per_firm = read_csv(os.path.join(file_path, parameter_dir, employment_per_firm_file))
employment_per_firm_gapfill = read_csv(os.path.join(file_path, parameter_dir, employment_per_firm_gapfill_file))


# define result directory
result_dir = os.path.join(file_path, output_dir)
if not os.path.exists(result_dir):
    os.path(result_dir)
else:
  print("output directory exists")


# <codecell>

########################################################
#### step 2 - Enumerate list of firms and workers ######
########################################################

print("Enumerating Firms")
criteria = (cbp.loc[:, 'employment'] < cbp.loc[:, 'establishment'])
cbp.loc[criteria, 'employment'] = cbp.loc[criteria, 'establishment']
total_employment = cbp.loc[:, 'employment'].sum()

# drop invalid record (if any)
cbp = cbp.dropna(subset=['Industry_NAICS6_CBP', 'FAFZONE', 'CBPZONE'])

cbp_by_industry = pd.merge(cbp, c_n6_n6io_sctg, 
                           on = 'Industry_NAICS6_CBP', 
                           how = 'left')

cbp_by_industry.loc[:, 'n2'] = \
    cbp_by_industry.loc[:, 'Industry_NAICS6_CBP'].astype(str).str[0:2]
cbp_by_industry.loc[:, 'n4'] = \
    cbp_by_industry.loc[:, 'Industry_NAICS6_CBP'].astype(str).str[0:4]

cbp_long = pd.melt(cbp_by_industry, 
                   id_vars=["Industry_NAICS6_CBP", "CBPZONE", "FAFZONE", "Industry_NAICS6_Make", "Commodity_SCTG", "n2", "n4"], 
                   value_vars= ['e1', 'e2', 'e3', 'e4', 'e5', 'e6', 'e7'],
                   var_name='esizecat', value_name='est')

cbp_long.loc[:, 'esizecat'] = cbp_long.loc[:, 'esizecat'].str[1:2].astype(int)
cbp_long = cbp_long.loc[cbp_long['est'] > 0]

employment_per_firm_short = employment_per_firm[['NAICS', 'size_group', 'emp_per_est']]
cbp_long = pd.merge(cbp_long, employment_per_firm_short, 
                    left_on = ['Industry_NAICS6_CBP', 'esizecat'],
                    right_on = ['NAICS', 'size_group'], how = 'left')

cbp_long_to_fill = cbp_long.loc[cbp_long['emp_per_est'].isna()]
cbp_long_no_fill = cbp_long.loc[~cbp_long['emp_per_est'].isna()]

employment_per_firm_gapfill = \
    employment_per_firm_gapfill[['size_group', 'emp_per_est']]
cbp_long_to_fill = cbp_long_to_fill.drop(columns = ['size_group', 'emp_per_est'])
cbp_long_to_fill = pd.merge(cbp_long_to_fill, employment_per_firm_gapfill,
                            left_on = 'esizecat', right_on = 'size_group',
                            how = 'left')

cbp_long = pd.concat([cbp_long_no_fill, cbp_long_to_fill])

firms = pd.DataFrame(cbp_long.values.repeat(cbp_long.est, axis=0), 
                     columns=cbp_long.columns)

firms.loc[:, 'BusID'] = firms.reset_index().index + 1