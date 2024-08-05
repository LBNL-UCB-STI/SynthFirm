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

synthetic_firms_no_location_file = "synthetic_firms.csv" 


# load model inputs
cbp = read_csv(os.path.join(file_path, input_dir, cbp_file))
mzemp = read_csv(os.path.join(file_path, input_dir, mzemp_file))
mzemp = mzemp.drop(columns = ['cnty_id'])
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
# 8,396,679 firms nationwide

# <codecell>

# adjust employment

emp_obs = \
    cbp.groupby(['Industry_NAICS6_CBP','FAFZONE', 'CBPZONE'])[['employment']].sum() 
emp_obs.columns = ['emp_obs']
emp_obs = emp_obs.reset_index()

emp_sim = \
    firms.groupby(['Industry_NAICS6_CBP','FAFZONE', 'CBPZONE'])[['emp_per_est']].sum()
emp_sim.columns = ['emp_sim']
emp_sim = emp_sim.reset_index()

emp_adj = pd.merge( emp_obs, emp_sim, 
                   on = ['Industry_NAICS6_CBP', 'CBPZONE', 'FAFZONE'],
                   how = 'left')

emp_adj.loc[:, 'emp_adj'] = \
    emp_adj.loc[:, 'emp_obs'] / emp_adj.loc[:, 'emp_sim']
    
emp_adj = emp_adj[['Industry_NAICS6_CBP', 'CBPZONE', 'FAFZONE', 'emp_adj']]

firms = pd.merge(firms, emp_adj, 
                 on = ['Industry_NAICS6_CBP', 'CBPZONE', 'FAFZONE'],
                 how = 'left')
firms.loc[:, 'emp_per_est'] *= firms.loc[:, 'emp_adj']
# firms.loc[:, 'emp_per_est'] =np.round(firms.loc[:, 'emp_per_est'].astype(float), 2)

# validate employment
total_employment_est = firms.loc[:, 'emp_per_est'].sum()

print('total number of input firm is ')
print(cbp_long.loc[:, 'est'].sum())
print('total number of output firm is ')
print(len(firms))

print('total number of input employment is ')
print(cbp.loc[:, 'employment'].sum())
print('total number of output employment is ')
print(total_employment_est)

# <codecell>

########################################################################
# Step 3 - Allocating commodity and location for each establishment ####
########################################################################

essential_attr = ['CBPZONE', 'FAFZONE',	'esizecat', 'Industry_NAICS6_Make',
               'Commodity_SCTG', 'emp_per_est', 'BusID', "n2"]
firms_out_boundary = firms.loc[firms['CBPZONE'] <= 999, essential_attr]

firms_out_boundary.loc[:, 'MESOZONE'] = \
    firms_out_boundary.loc[:, 'CBPZONE']  + 20000

  
firms_in_boundary = \
    firms.loc[firms['CBPZONE'] > 999, essential_attr]

print('number of firms within study area:')
print(len(firms_in_boundary))
# for Seattle, there are 287,324 firms

firms_in_boundary.loc[firms_in_boundary['n2'].isin(["31", "32", "33"]), 'n2'] = "3133"
firms_in_boundary.loc[firms_in_boundary['n2'].isin(["44", "45"]), 'n2'] = "4445"
firms_in_boundary.loc[firms_in_boundary['n2'].isin(["48", "49"]), 'n2'] = "4849"


emp_ranking_in_boundary = pd.melt(mzemp, id_vars = ["COUNTY", "MESOZONE"],
                                  var_name= 'industry', value_name='EmpRank')

emp_ranking_in_boundary = emp_ranking_in_boundary.reset_index()
emp_ranking_in_boundary = \
    emp_ranking_in_boundary.rename(columns = {"COUNTY": 'CBPZONE'})
emp_ranking_in_boundary.loc[:, 'n2'] = \
    emp_ranking_in_boundary.loc[:, 'industry'].str.split('rank').str[1]

emp_ranking_in_boundary = \
    emp_ranking_in_boundary.loc[~emp_ranking_in_boundary['EmpRank'].isna()]
# if a CBG has 0 employment for selected industry, it is not a valid candidate
# therefore, CBGs with missing ranking is dropped

# <codecell> 
unique_counties = firms_in_boundary.CBPZONE.unique()
firms_in_boundary_out = None
# randomly assign CBG
# loop through counties, so it doesn't explode memory usage

for ct in unique_counties:
    # print('assigning CBG within county fips ' + str(ct))
    firms_in_boundary_sel = firms_in_boundary.loc[firms_in_boundary['CBPZONE'] == ct]
    # print(len(firms_in_boundary_sel))
    firms_in_boundary_sel = pd.merge(firms_in_boundary_sel, 
                             emp_ranking_in_boundary,
                             on = ["CBPZONE", "n2"],
                             how = 'left') # Merge the rankings dataset to the firms database based on county
    firms_in_boundary_sel['EmpRank'] = firms_in_boundary_sel['EmpRank'].fillna(1)
    # print(len(firms_in_boundary_sel.MESOZONE.unique()))
    
    # Sometimes, LODES report 0 employment in a county, while firm data as non-zero
    # may attributed to imputation for non-payroll workers
    # fill minimum ranking for all zones as no information is available for the ranking
    firms_in_boundary_sel = \
        firms_in_boundary_sel.groupby(essential_attr).sample(1, 
                                                       weights = firms_in_boundary_sel['EmpRank'],
                                                       replace = False, random_state = 1)
    firms_in_boundary_sel = \
        firms_in_boundary_sel.drop(columns = ['index', 'industry', 'EmpRank'])
    firms_in_boundary_sel.loc[:, 'MESOZONE'].fillna(method = 'ffill', inplace = True)
    # print(len(firms_in_boundary_sel.MESOZONE.unique()))
    # for firms with no emp ranking, forward fill it with locations from nearest industry
    # check na
    firms_with_na = firms_in_boundary_sel.loc[firms_in_boundary_sel['MESOZONE'].isna()]
    if len(firms_with_na) > 0:
        print(str(len(firms_with_na)) + 'firms failed to have zone assigned')
        # break
    firms_in_boundary_out = pd.concat([firms_in_boundary_out, firms_in_boundary_sel])
    # break

    
# <codecell>

####################################################
# Step 4 - final formatting and writing outputs ####
####################################################

firms = pd.concat([firms_out_boundary, firms_in_boundary_out])

print('number of firms before writing output:')
print(len(firms))
    
output_attr = ['CBPZONE', 'FAFZONE',	'esizecat', 'Industry_NAICS6_Make',
               'Commodity_SCTG', 'emp_per_est', 'BusID', 'MESOZONE']

firms = firms[output_attr]
firms = firms.rename(columns = {'emp_per_est': 'Emp'})
firms.to_csv(os.path.join(file_path, output_dir, synthetic_firms_no_location_file), 
             index = False)
