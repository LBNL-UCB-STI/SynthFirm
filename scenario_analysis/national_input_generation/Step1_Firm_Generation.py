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



########################################################
#### step 1 - configure environment and load inputs ####
########################################################

scenario_name = 'Seattle'
out_scenario_name = 'Seattle'
file_path = '/Users/xiaodanxu/Documents/SynthFirm.nosync'
parameter_dir = 'SynthFirm_parameters'
number_of_processes = 4
input_dir = 'inputs_' + scenario_name
output_path = 'outputs_' + out_scenario_name

os.chdir(file_path)    
cbp_file = os.path.join(input_dir, 'data_emp_cbp_imputed.csv')
mzemp_file = os.path.join(input_dir, 'data_mesozone_emprankings.csv')
mesozone_to_faf_file = os.path.join(input_dir, 'zonal_id_lookup_final.csv')

c_n6_n6io_sctg_file = os.path.join(parameter_dir, 'corresp_naics6_n6io_sctg_revised.csv')
employment_per_firm_file = os.path.join(parameter_dir, 'employment_by_firm_size_naics.csv')
employment_per_firm_gapfill_file = os.path.join(parameter_dir, 'employment_by_firm_size_gapfill.csv')
zip_to_tract_file = os.path.join(parameter_dir, 'ZIP_TRACT_LOOKUP_2016.csv')
synthetic_firms_no_location_file = os.path.join(output_path, 'synthetic_firms.csv')

# def synthetic_firm_generation(cbp_file, mzemp_file, c_n6_n6io_sctg_file, employment_per_firm_file,
#                               employment_per_firm_gapfill_file, synthetic_firms_no_location_file,
#                               output_path):
print("Start synthetic firm generation...")
# load model inputs
cbp = read_csv(cbp_file)
mzemp = read_csv(mzemp_file)
mesozone_to_faf = read_csv(mesozone_to_faf_file)
# mzemp = mzemp.drop(columns = ['cnty_id'])
c_n6_n6io_sctg = read_csv(c_n6_n6io_sctg_file)
employment_per_firm = read_csv(employment_per_firm_file)
employment_per_firm_gapfill = read_csv(employment_per_firm_gapfill_file)
zip_to_tract_crosswalk = read_csv(zip_to_tract_file)

# create result directory if not exist

if not os.path.exists(output_path):
    os.mkdir(output_path)
else:
  print("output directory exists")
    
    
    # <codecell>
    
    ########################################################
    #### step 2 - Enumerate list of firms and workers ######
    ########################################################
    
print("Enumerating Firms")
criteria = (cbp.loc[:, 'employment'] < cbp.loc[:, 'establishment'])
cbp.loc[criteria, 'employment'] = cbp.loc[criteria, 'establishment']


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
                    id_vars=["Industry_NAICS6_CBP", "CBPZONE", "FAFZONE", "COUNTY","ZIPCODE",
                             "Industry_NAICS6_Make", "Commodity_SCTG", "n2", "n4"], 
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


#### scale employment to align with LEHD total (by NAICS 2-DIGIT code)
# pre-process employment ranking
mesozone_to_cbpzone = mesozone_to_faf[['MESOZONE', 'CBPZONE']]
lehd_emp_for_scaling = pd.merge(mzemp, mesozone_to_cbpzone, on = 'MESOZONE')

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

lehd_emp_for_scaling.loc[:, emp_colnames] = lehd_emp_for_scaling.loc[:, emp_colnames].fillna(0)
lehd_emp_for_scaling = lehd_emp_for_scaling.groupby('CBPZONE')[emp_colnames].sum()
lehd_emp_for_scaling = lehd_emp_for_scaling.reset_index()
lehd_emp_for_scaling = pd.melt(lehd_emp_for_scaling, id_vars = ["CBPZONE"],
                               value_vars = emp_colnames,
                                  var_name= 'industry', value_name='emp_lehd')

lehd_emp_for_scaling = lehd_emp_for_scaling.reset_index()
lehd_emp_for_scaling.loc[:, 'industry'] = \
    lehd_emp_for_scaling.loc[:, 'industry'].str.split('rank').str[1]
    

print('total LEHD employment:')
print(lehd_emp_for_scaling.emp_lehd.sum())
# develop firm emp scaling factor
firms.loc[:, 'industry'] = firms.loc[:, 'n2']
firms.loc[firms['industry'].isin(["31", "32", "33"]), 'industry'] = "3133"
firms.loc[firms['industry'].isin(["44", "45"]), 'industry'] = "4445"
firms.loc[firms['industry'].isin(["48", "49"]), 'industry'] = "4849"

emp_sim = \
    firms.groupby(['industry', 'CBPZONE'])[['emp_per_est']].sum()
emp_sim.columns = ['emp_sim']
emp_sim = emp_sim.reset_index()

emp_adj = pd.merge( lehd_emp_for_scaling, emp_sim, 
                    on = ['industry', 'CBPZONE'],
                    how = 'left')

emp_adj.loc[:, 'emp_adj'] = \
    emp_adj.loc[:, 'emp_lehd'] / emp_adj.loc[:, 'emp_sim']
# emp_adj = emp_adj.fillna(0)
# if CBP < LEHD, do not adjust employment -> to keep employments from imputation step
emp_adj.loc[emp_adj['emp_adj']<1, 'emp_adj'] = 1

emp_adj = emp_adj[['industry', 'CBPZONE', 'emp_adj']]
firms.drop(columns = ['emp_adj'], inplace = True)
firms = pd.merge(firms, emp_adj, 
                  on = ['industry', 'CBPZONE'],
                  how = 'left')

firms.loc[:, 'emp_adj'].fillna(1, inplace = True)
firms.loc[:, 'emp_per_est'] *= firms.loc[:, 'emp_adj']
# firms.loc[:, 'emp_per_est'] =np.round(firms.loc[:, 'emp_per_est'].astype(float), 2)

# validate employment
total_employment_est = firms.loc[:, 'emp_per_est'].sum()

print('total number of output employment is ')
print(total_employment_est)

# <codecell>

# separate firms inside/outside study reion
emp_ranking_in_boundary = mzemp.dropna(subset = ['COUNTY'])
emp_ranking_in_boundary = \
    emp_ranking_in_boundary.rename(columns = {"COUNTY": 'CBPZONE'})
cbpzone_in_region = emp_ranking_in_boundary.CBPZONE.unique()


essential_attr = ['CBPZONE', 'FAFZONE',	'esizecat', 'Industry_NAICS6_Make', 'COUNTY', 'ZIPCODE',
                'Commodity_SCTG', 'emp_per_est', 'BusID', 'industry']
firms_out_boundary = \
    firms.loc[~firms['CBPZONE'].isin(cbpzone_in_region), essential_attr]

firms_in_boundary = \
    firms.loc[firms['CBPZONE'].isin(cbpzone_in_region), essential_attr]


print('number of firms outside study area:')
print(len(firms_out_boundary))

print('number of firms within study area:')
print(len(firms_in_boundary))

# <codecell>

# generate mesozone ID for firms outside boundary
firms_out_boundary = pd.merge(firms_out_boundary,
                              mesozone_to_cbpzone,
                              on = 'CBPZONE', how = 'left')

firms_out_boundary = \
    firms_out_boundary.groupby(essential_attr).sample(1, replace = False, random_state = 1)

# assign mesozone for firms within boundary

# separate firms with/without zipcode
firms_in_boundary_nozip = firms_in_boundary.loc[firms_in_boundary['ZIPCODE'] == 99999]
firms_in_boundary_withzip = firms_in_boundary.loc[firms_in_boundary['ZIPCODE'] != 99999]

# create tract ID from CBG id
emp_ranking_in_boundary.loc[:, 'MESOZONE'] = \
    emp_ranking_in_boundary.loc[:, 'MESOZONE'].astype(int).astype(str).str.zfill(12)
emp_ranking_in_boundary.loc[:, 'geoid'] = \
    emp_ranking_in_boundary.loc[:, 'MESOZONE'].str[0:11]

# <codecell>
zip_to_tract_crosswalk.loc[:, 'geoid'] = \
    zip_to_tract_crosswalk.loc[:, 'geoid'].astype(int).astype(str).str.zfill(11)
    
emp_ranking_in_boundary = pd.melt(emp_ranking_in_boundary, 
                                  id_vars = ['CBPZONE', 'MESOZONE', 'geoid'],
                                  value_vars = emp_colnames,
                                  var_name= 'industry', value_name='emp_lehd')

emp_ranking_in_boundary = emp_ranking_in_boundary.reset_index()
emp_ranking_in_boundary = \
    emp_ranking_in_boundary.loc[emp_ranking_in_boundary['emp_lehd']>0]
# if a CBG has 0 employment for selected industry, it is not a valid candidate
# therefore, CBGs with missing ranking is dropped
emp_ranking_in_boundary.loc[:, 'industry'] = \
    emp_ranking_in_boundary.loc[:, 'industry'].str.split('rank').str[1]
    
print('total LEHD employment within study area:')
print(emp_ranking_in_boundary.emp_lehd.sum())

# <codecell>

# assign mesozone to firms with zip code ID

industries = emp_ranking_in_boundary.loc[:, 'industry'].unique()
zip_to_tract_crosswalk.rename(columns = {'zip': 'ZIPCODE'}, inplace = True)
zip_to_tract_crosswalk.drop(columns = ['bus_ratio', 'fraction'], inplace = True)
print(len(firms_in_boundary_withzip))

# <codecell>
firms_out_withzip = None

for ind in industries:

    firms_to_assign = firms_in_boundary_withzip.loc[firms_in_boundary_withzip['industry'] == ind]
    print('numbers of firms to assign from industry = ' + str(ind))
    print(len(firms_to_assign.BusID.unique()))
    
    firms_to_assign = pd.merge(firms_to_assign, zip_to_tract_crosswalk,
                               on = 'ZIPCODE', how = 'left')
    firms_to_assign = pd.merge(firms_to_assign, emp_ranking_in_boundary,
                               on = ['CBPZONE', 'geoid', 'industry'], how = 'left')
    firms_to_assign['emp_lehd'] = firms_to_assign['emp_lehd'].fillna(1)
    # print(len(firms_to_assign.BusID.unique()))
    
    # Sometimes, LODES report 0 employment in a county, while firm data as non-zero
    # may attributed to imputation for non-payroll workers
    # fill minimum ranking for all zones as no information is available for the ranking
    
    firms_to_assign = \
        firms_to_assign.groupby(essential_attr).sample(1,
                                                        weights = firms_to_assign['emp_lehd'],
                                                        replace = False, random_state = 1)
    firms_to_assign = \
        firms_to_assign.drop(columns = ['index', 'industry', 'emp_lehd'])
    firms_to_assign.loc[:, 'MESOZONE'].fillna(method = 'ffill', inplace = True)
    firms_to_assign.loc[:, 'MESOZONE'].fillna(method = 'bfill', inplace = True)
    
    firms_out_withzip = pd.concat([firms_out_withzip, firms_to_assign])
    
    # break
print(len(firms_out_withzip))

# <codecell>

# assign mesozone to firms without zip code
firms_out_nozip = None

def split_dataframe(df, chunk_size = 10 ** 5): 
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    return chunks

for ind in industries:

    firms_to_assign = firms_in_boundary_nozip.loc[firms_in_boundary_nozip['industry'] == ind]
    print('numbers of firms to assign from industry = ' + str(ind))
    print(len(firms_to_assign.BusID.unique()))
    chunks = split_dataframe(firms_to_assign, 10000)
    post_firms_to_assign = None
    for chunk in chunks:

        chunk = pd.merge(chunk, emp_ranking_in_boundary,
                                   on = ['CBPZONE', 'industry'], how = 'left')
        # print(len(firms_to_assign.BusID.unique()))
        chunk['emp_lehd'] = chunk['emp_lehd'].fillna(1)
        # print(len(firms_to_assign.BusID.unique()))
        
        chunk = \
            chunk.groupby(essential_attr).sample(1,
                                                 weights = chunk['emp_lehd'],
                                                 replace = False, random_state = 1)
        chunk = \
            chunk.drop(columns = ['index', 'industry', 'emp_lehd'])
        chunk.loc[:, 'MESOZONE'].fillna(method = 'ffill', inplace = True)
        chunk.loc[:, 'MESOZONE'].fillna(method = 'bfill', inplace = True)
        post_firms_to_assign = pd.concat([post_firms_to_assign, chunk])
    firms_out_nozip = pd.concat([firms_out_nozip, post_firms_to_assign])
    
    # break
print(len(firms_out_nozip))


# <codecell> 
# unique_counties = firms_in_boundary.CBPZONE.unique()
# firms_in_boundary_out = None
# # randomly assign CBG
# # loop through counties, so it doesn't explode memory usage

# for ct in unique_counties:
#     # print('assigning CBG within county fips ' + str(ct))
#     firms_in_boundary_sel = firms_in_boundary.loc[firms_in_boundary['CBPZONE'] == ct]
#     firms_in_boundary_sel = pd.merge(firms_in_boundary_sel, 
#                               emp_ranking_in_boundary,
#                               on = ["CBPZONE", "n2"],
#                               how = 'left') # Merge the rankings dataset to the firms database based on county
#     firms_in_boundary_sel['EmpRank'] = firms_in_boundary_sel['EmpRank'].fillna(1)
    

#     firms_in_boundary_sel = \
#         firms_in_boundary_sel.groupby(essential_attr).sample(1, 
#                                                         weights = firms_in_boundary_sel['EmpRank'],
#                                                         replace = False, random_state = 1)
#     firms_in_boundary_sel = \
#         firms_in_boundary_sel.drop(columns = ['index', 'industry', 'EmpRank'])
#     firms_in_boundary_sel.loc[:, 'MESOZONE'].fillna(method = 'ffill', inplace = True)
#     firms_in_boundary_sel.loc[:, 'MESOZONE'].fillna(method = 'bfill', inplace = True)
#     # for firms with no emp ranking, forward fill it with locations from nearest industry
#     # check na
#     firms_with_na = firms_in_boundary_sel.loc[firms_in_boundary_sel['MESOZONE'].isna()]
#     if len(firms_with_na) > 0:
#         print(str(len(firms_with_na)) + ' firms failed to have zone assigned')
#         print('and their firm ids are:')
#         print(firms_with_na.BusID.unique())
#         print('WARNING!!: The following pipeline can fail due to this error')
#         # break
#     firms_in_boundary_out = pd.concat([firms_in_boundary_out, firms_in_boundary_sel])
#     # break

    
# <codecell>

####################################################
# Step 4 - final formatting and writing outputs ####
####################################################

firms = pd.concat([firms_out_boundary, firms_out_withzip, firms_out_nozip])

print('number of firms before writing output:')
print(len(firms))
    
output_attr = ['CBPZONE', 'FAFZONE',	'esizecat', 'Industry_NAICS6_Make',
                'Commodity_SCTG', 'emp_per_est', 'BusID', 'MESOZONE', 'ZIPCODE']

firms = firms[output_attr]
firms = firms.rename(columns = {'emp_per_est': 'Emp'})
firms.to_csv(synthetic_firms_no_location_file, index = False)
