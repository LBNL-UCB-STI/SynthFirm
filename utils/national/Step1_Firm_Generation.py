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
import warnings
import time

warnings.filterwarnings("ignore")
from concurrent.futures import ProcessPoolExecutor, as_completed


########################################################
#### step 1 - configure environment and load inputs ####
########################################################

scenario_name = 'national'
out_scenario_name = 'national'
file_path = 'C:\SynthFirm'
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

susb_file = os.path.join(parameter_dir, 'SUSB_msa_3digitnaics_2016.csv')
county_to_msa_file = os.path.join(parameter_dir, 'county_msa_crosswalk.csv')
firm_enterprise_file = os.path.join(output_path, 'synthetic_enterprise.csv')

# def synthetic_firm_generation(cbp_file, mzemp_file, mesozone_to_faf_file, 
#                               c_n6_n6io_sctg_file, employment_per_firm_file,
#                               employment_per_firm_gapfill_file, zip_to_tract_file,
#                               synthetic_firms_no_location_file, output_path):
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

susb_data = read_csv(susb_file)
county_to_msa = read_csv(county_to_msa_file)
county_to_msa.rename(columns = {'County Code': 'CBPZONE'}, 
                     inplace = True)
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

cbp_long['esizecat'] = cbp_long['esizecat'].str[1:2].astype(int)
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

firms.drop(columns = ['emp_adj'], inplace = True)
# assign business to firms

# <codecell>
    
    ##################################################
    #### step 3 - Assign enterprise information ######
    ##################################################
# assign enterprise ID
susb_data.loc[:, 'MSA Code'] =\
    'C' + susb_data.loc[:, 'MSA'].astype(str).str[0:4]


susb_data_short = susb_data[['NAICS', 'ENTRSIZE', 'FIRM', 'ESTB', 'EMPL',
                             'ENTRSIZEDSCR',  'MSA Code']]
susb_data_short['NAICS'] = susb_data_short['NAICS'].astype(str)
susb_data_short.loc[:, 'susb_emp_per_est'] = \
    susb_data_short.loc[:, 'EMPL']/ susb_data_short.loc[:, 'ESTB']

firms.loc[:, 'n3'] = \
    firms.loc[:, 'Industry_NAICS6_CBP'].astype(str).str[0:3]

modeled_naics3 = firms['n3'].unique()
susb_naics3 = susb_data_short['NAICS'].unique()

# check differences in naics code
diff_naics = set(modeled_naics3) - set(susb_naics3)

# create list of enterprise
enterprise = pd.DataFrame(susb_data_short.values.repeat(susb_data_short.FIRM, axis=0), 
                      columns=susb_data_short.columns)

#  employment size group for firms
# SynthFirm group: e1 = '1-19',e2 = '20-99',e3 ='100-499',
#e4 = '500-999',e5 = '1,000-2,499',e6 = '2,500-4,999',e7 = 'Over 5,000'
# SUSB group: '02:  0-4', '03:  5-9', '04:  10-19', 
#'06:  20-99', '07:  100-499', '09:  500+'

# merge group: 1-19, 20-99, 100-499, 500+
enterprise.loc[:, 'FirmID'] = enterprise.reset_index().index + 1
enterprise.loc[:, 'member'] = enterprise['ESTB'] / enterprise['FIRM']
enterprise['member'] = np.ceil(enterprise['member'].astype(float))

def adjust_row(group):
    if len(group) > 1:
        total = group['ESTB'].mean()
        in_group_sum = group['member'].sum()
        residual = int(in_group_sum - total)

        while residual > 0:
            # residual is the delta to reassign
            # take 1 member out of first n(residual) firms
            group.iloc[:residual, group.columns.get_loc('member')] -= 1

            residual =  group['member'].sum() - total
        group = group.copy()            
        # group.iloc[-1, group.columns.get_loc('member')] = value_n_1
    return group

group_var = ['NAICS', 'MSA Code', 'ENTRSIZEDSCR']

enterprise = enterprise.groupby(group_var, group_keys=False).apply(adjust_row)

print(enterprise['member'].min())

# 7,295,131 establishments
# <codecell>
# merge group: 1-19, 20-99, 100-499, 500+
size_bin = [-1, 19, 99, 499, enterprise['susb_emp_per_est'].max()]
size_bin_label = [1, 2, 3, 4]
enterprise.loc[:, 'emp_size'] = pd.cut(enterprise['susb_emp_per_est'],
                                       bins = size_bin, labels = size_bin_label,
                                       right = True)
enterprise['emp_size'] = enterprise['emp_size'].astype(int)
# print(enterprise['emp_size'].unique())
enterprise.rename(columns = {'NAICS': 'n3'}, inplace = True)


firms.loc[:, 'emp_size'] = 1
firms.loc[firms['esizecat'] == 2, 'emp_size'] = 2
firms.loc[firms['esizecat'] == 3, 'emp_size'] = 3
firms.loc[firms['esizecat'] >= 4, 'emp_size'] = 4


# assign single est firm to est (minimize diff in emp)
print(len(firms))
county_to_msa_short = county_to_msa[['CBPZONE', 'MSA Code']]
firms['CBPZONE'] = firms['CBPZONE'].astype(int)
firms = pd.merge(firms, county_to_msa, on = 'CBPZONE',
                 how = 'left')

# <codecell>

# performing firm-enterprise matching with all three matching layers
# MSA, Industry and size

t0=time.time()
enterprise_remaining = enterprise.copy()
firms_current_iter = firms.copy()
firms_not_assigned = None
final_enterprise_pair = None
# try assign firms with 10 iterations

for k in range(50):
    if len(enterprise_remaining)<=10000:
        break
    print('Start iteration ' + str(k))
    list_of_msa = enterprise_remaining['MSA Code'].unique()
    print(len(list_of_msa))
    threshold_rows = (k+1) * 5000000
    firms_not_assigned = None
    print('remaining firms:')
    print(len(firms_current_iter))
    print('remaining enterprises:')
    print(len(enterprise_remaining))
    # prepare data for current iteration
    firm_msas=np.array(firms_current_iter['MSA Code'], dtype=str)
    ent_msas=np.array(enterprise_remaining['MSA Code'], dtype=str)
    i = 0
    enterprise_pair_out = None
    for msa in list_of_msa:
        
        chunk_pairs_list = []
        if i % 50 == 0:
            print('Processing batch ' + str(i),  np.round(time.time()-t0, 2))

        chunk = firms_current_iter.iloc[np.where(firm_msas==msa)[0]]
 
        bus_to_assign = set(chunk.BusID.unique())

        enterprise_to_assign = \
            enterprise_remaining.iloc[np.where(ent_msas==msa)[0]]
        enterprise_to_assign['members_left'] = \
            enterprise_to_assign['member']
     
    # Merge only necessary columns to reduce memory usage
        merge_cols = ['MSA Code', 'emp_size', 'n3']
        
        # size check 
        num_of_row = len(enterprise_to_assign) * len(chunk)
        sampling_frac = threshold_rows/num_of_row
        if sampling_frac < 1:
            split_frac = np.sqrt(sampling_frac)
            chunk_sel = chunk.sample(frac = split_frac, replace = False)
            enterprise_sel = enterprise_to_assign.sample(frac=split_frac, replace=False)
        else:
            enterprise_sel = enterprise_to_assign
            chunk_sel = chunk
        enterprise_with_firm = pd.merge(
            enterprise_sel[merge_cols + ['FirmID', 'susb_emp_per_est', 'members_left']],
            chunk_sel[merge_cols + ['BusID', 'emp_per_est']],
            on=merge_cols,
            how='inner'
        )
    
        enterprise_with_firm.loc[:, 'emp_diff'] = \
            np.abs(enterprise_with_firm.loc[:, 'susb_emp_per_est'] - \
                enterprise_with_firm.loc[:, 'emp_per_est'])    

        enterprise_with_firm.loc[:, 'pairID'] = \
            enterprise_with_firm.loc[:, 'FirmID'].astype(str) + '-' + \
                enterprise_with_firm.loc[:, 'BusID'].astype(str)
        min_left = 0
        j = 0
    
        while len(bus_to_assign) > min_left:
            starting_ids = len(bus_to_assign)                    
            # need a filter here
            enterprise_with_firm = \
                enterprise_with_firm.loc[enterprise_with_firm['members_left'] > 0]
            
            empdiff=np.array(enterprise_with_firm['emp_diff'], dtype=float)
            srtid=np.argsort(empdiff)
            enterprise_with_firm1=enterprise_with_firm.iloc[srtid]
            
            enterprise_business_pair = (
                # enterprise_with_firm.sort_values('emp_diff')
                enterprise_with_firm1
                .drop_duplicates('BusID')
                .drop_duplicates('FirmID')
            )
            
            
            assigned_firm_ids = set(enterprise_business_pair.FirmID.unique())
            assigned_bus_ids = set(enterprise_business_pair.BusID.unique())
            
            # Remove assigned businesses from bus_to_assign
            bus_to_assign -= assigned_bus_ids
            
            # Remove assigned pairs from enterprise_with_firm
            enterprise_with_firm = enterprise_with_firm[
                ~enterprise_with_firm['BusID'].isin(assigned_bus_ids)
            ]
                  
            # remove one member from enteprise if the pair has formed
            enterprise_with_firm.loc[enterprise_with_firm['FirmID'].isin(assigned_firm_ids), 'members_left'] -= 1
            chunk_pairs_list.append(enterprise_business_pair)
            # append matched business-enterprise to output
            
            ending_ids = len(bus_to_assign)
    
            j += 1
            if (ending_ids == starting_ids) or (j > 20):
                # print('Reaching end of assignment and failed to match number of businesses ' + str(ending_ids))
                min_left = ending_ids
                leftover_firms = chunk.loc[chunk['BusID'].isin(bus_to_assign)]
                firms_not_assigned = pd.concat([firms_not_assigned,
                                                leftover_firms])
            
            # end of pairing process

        chunk_pairs = pd.concat(chunk_pairs_list, ignore_index=True)
        enterprise_pair_out = pd.concat([enterprise_pair_out, chunk_pairs])
        # end current MSA assignment
        i+=1


    # check post interation count
    member_count = \
            enterprise_pair_out.groupby('FirmID')[['BusID']].size().to_frame(name='members_assigned')
    member_count = member_count.reset_index()

    enterprise_remaining = pd.merge(enterprise_remaining, member_count,
                                       on = 'FirmID', how = 'left')
    enterprise_remaining['members_assigned'].fillna(0, inplace = True)
    enterprise_remaining.loc[:, 'member'] -= \
        enterprise_remaining.loc[:, 'members_assigned']
    enterprise_remaining.drop(columns = ['members_assigned'], inplace = True) 
    enterprise_remaining = \
        enterprise_remaining.loc[enterprise_remaining['member'] > 0]
    
    firms_current_iter = firms_not_assigned
    final_enterprise_pair = pd.concat([final_enterprise_pair, enterprise_pair_out])
    print('End iteration ' + str(k))
    print(time.time()-t0)
    
# <codecell>
# drop location constraint and continue assign

list_of_n3 = enterprise_remaining['n3'].astype(str).unique()
print(len(list_of_n3))

firms_not_assigned = None
enterprise_pair_out = None

print('remaining firms:')
print(len(firms_current_iter))
print('remaining enterprises:')
print(len(enterprise_remaining))

# prepare data for current iteration
firm_n3=np.array(firms_current_iter['n3'], dtype=str)
ent_n3=np.array(enterprise_remaining['n3'], dtype=str)
i = 0
output_attr = ['MSA Code', 'emp_size', 'n3', 'FirmID', 'susb_emp_per_est',
       'members_left', 'BusID', 'emp_per_est', 'emp_diff', 'pairID']    
for n3 in list_of_n3:
    
    chunk_pairs_list = []
    # if i % 10 == 0:
    print('Processing industry ' + n3,  np.round(time.time()-t0, 2))
    chunk = firms_current_iter.iloc[np.where(firm_n3==n3)[0]]
    firm_size = len(chunk)
    bus_to_assign = set(chunk.BusID.unique())

    enterprise_to_assign = \
        enterprise_remaining.iloc[np.where(ent_n3==n3)[0]]
        
    enterprise_to_assign = pd.DataFrame(enterprise_to_assign.values.repeat(enterprise_to_assign.member, axis=0), 
                          columns=enterprise_to_assign.columns)
    enterprise_to_assign = enterprise_to_assign.sort_values('susb_emp_per_est', ascending = False)
    chunk = chunk.sort_values('emp_per_est', ascending = False)
    ent_size = len(enterprise_to_assign)
    pair_size = min(ent_size, firm_size)
    
    enterprise_to_pair = \
        enterprise_to_assign.head(pair_size)[['MSA Code', 'emp_size', 'n3', 
                                              'FirmID', 'susb_emp_per_est', 'member']]
    enterprise_to_pair.rename(columns = {'member': 'members_left'}, inplace = True)
    chunk_to_pair = chunk.head(pair_size)[['BusID', 'emp_per_est']]
    enterprise_to_pair = pd.concat([enterprise_to_pair.reset_index(),
                                    chunk_to_pair.reset_index()],
                                   axis=1)
    enterprise_to_pair.loc[:, 'emp_diff'] = \
        np.abs(enterprise_to_pair.loc[:, 'susb_emp_per_est'] - \
            enterprise_to_pair.loc[:, 'emp_per_est'])    

    enterprise_to_pair.loc[:, 'pairID'] = \
        enterprise_to_pair.loc[:, 'FirmID'].astype(str) + '-' + \
            enterprise_to_pair.loc[:, 'BusID'].astype(str)
    print(ent_size, pair_size, len(enterprise_to_pair))
    enterprise_to_pair = enterprise_to_pair.loc[:, output_attr]
    final_enterprise_pair = \
        pd.concat([final_enterprise_pair, enterprise_to_pair])

# <codecell>
# quickly check results
final_enterprise_pair_sample = final_enterprise_pair.sample(10000)
# import matplotlib.pyplot as plt
final_enterprise_pair_sample['emp_diff'].hist(bins = 40)

# format output
final_enterprise_pair = \
final_enterprise_pair[['FirmID', 'MSA Code', 'n3', 
        'BusID', 'emp_per_est']]

final_enterprise_pair.loc[:, 'INSUSB'] = 1

bus_enterprise_duo = final_enterprise_pair[['BusID', 'FirmID']]
print(len(firms))
firms = pd.merge(firms, bus_enterprise_duo, on = 'BusID',
                 how = 'left')
print(len(firms))

# <codecell>
# fill missing firm
starting_firm_id = firms['FirmID'].max() + 1
firms_with_ent = firms.loc[~firms['FirmID'].isna()]

firms_miss_ent = firms.loc[firms['FirmID'].isna()]
firms_miss_ent['FirmID'] = firms_miss_ent.reset_index().index + \
    starting_firm_id
# print(firms_miss_ent['FirmID'].head(5))
imputed_enterprise_pair = firms_miss_ent[['FirmID','MSA Code', 'n3', 
        'BusID', 'emp_per_est']]
imputed_enterprise_pair.loc[:, 'INSUSB'] = 0

final_enterprise_pair = pd.concat([final_enterprise_pair, \
                                   imputed_enterprise_pair])
final_enterprise_pair.rename(columns = {'n3': 'NAICS'}, inplace = True)

final_enterprise_pair.to_csv(firm_enterprise_file, index = False)

firms = pd.concat([firms_with_ent, firms_miss_ent])
firms.drop(columns = ['n3', 'emp_size', 'MSA Code', 'MSA Title'],
           inplace = True)
# <codecell>
########################################################################
# Step 3 - Allocating commodity and location for each establishment ####
########################################################################


### scale employment to align with LEHD total (by NAICS 2-DIGIT code)
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
firms.loc[firms['industry'].isin(["44", "45", "4A"]), 'industry'] = "4445"
firms.loc[firms['industry'].isin(["48", "49"]), 'industry'] = "4849"
firms.loc[firms['industry'].isin(["S0"]), 'industry'] = "92"
print(firms['industry'].unique())

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
firms['ZIPCODE'] = firms['ZIPCODE'].astype(np.int64)
emp_ranking_in_boundary = mzemp.dropna(subset = ['COUNTY'])
emp_ranking_in_boundary = \
    emp_ranking_in_boundary.rename(columns = {"COUNTY": 'CBPZONE'})
cbpzone_in_region = emp_ranking_in_boundary.CBPZONE.unique()


essential_attr = ['CBPZONE', 'FAFZONE',	'esizecat', 'Industry_NAICS6_Make', 'COUNTY', 'ZIPCODE',
                'Commodity_SCTG', 'emp_per_est', 'BusID', 'industry']

assign_enterprise = True
if assign_enterprise:
    essential_attr.append('FirmID')
    
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
# <codecell>
# separate firms with/without zipcode
firms_in_boundary_nozip = firms_in_boundary.loc[firms_in_boundary['ZIPCODE'] == 99999]
firms_in_boundary_withzip = firms_in_boundary.loc[firms_in_boundary['ZIPCODE'] != 99999]

# create tract ID from CBG id
emp_ranking_in_boundary.loc[:, 'MESOZONE'] = \
    emp_ranking_in_boundary.loc[:, 'MESOZONE'].astype(np.int64).astype(str).str.zfill(12)
emp_ranking_in_boundary.loc[:, 'geoid'] = \
    emp_ranking_in_boundary.loc[:, 'MESOZONE'].str[0:11]

# <codecell>
zip_to_tract_crosswalk['zip'] = zip_to_tract_crosswalk['zip'].astype(np.int64)
zip_to_tract_crosswalk.loc[:, 'geoid'] = \
    zip_to_tract_crosswalk.loc[:, 'geoid'].astype(np.int64).astype(str).str.zfill(11)
    
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

nozip_col = firms_in_boundary_nozip.columns
for ind in industries:

    firms_to_assign = \
        firms_in_boundary_withzip.loc[firms_in_boundary_withzip['industry'] == ind]
    print('numbers of firms to assign from industry = ' + str(ind))
    print(len(firms_to_assign.BusID.unique()))
    
    firms_to_assign = pd.merge(firms_to_assign, zip_to_tract_crosswalk,
                                on = 'ZIPCODE', how = 'left')
    firms_to_assign = pd.merge(firms_to_assign, emp_ranking_in_boundary,
                                on = ['CBPZONE', 'geoid', 'industry'], how = 'left')
    
    # find firms that do not have valid mesozone in the CBP region
    firms_in_boundary_nozip_add = firms_to_assign.loc[firms_to_assign['emp_lehd'].isna()]
    firms_in_boundary_nozip_add = firms_in_boundary_nozip_add[nozip_col]
    firms_in_boundary_nozip_add.drop_duplicates(keep = 'first', inplace = True)

        
    firms_to_assign = firms_to_assign.dropna(subset = ['emp_lehd'])
    bus_ids = firms_to_assign.BusID.unique()
    firms_in_boundary_nozip_add = \
        firms_in_boundary_nozip_add[~firms_in_boundary_nozip_add['BusID'].isin(bus_ids)]
    print('firms without valid CBG in Zip code:')
    print(len(firms_in_boundary_nozip_add))
    firms_in_boundary_nozip = \
        pd.concat([firms_in_boundary_nozip, firms_in_boundary_nozip_add])
    if firms_to_assign is not None:
        if len(firms_to_assign) > 0:
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
print('firms in region with valid zip')
print(len(firms_out_withzip))

print('firms in region without valid zip')
print(len(firms_in_boundary_nozip))

# <codecell>

# assign mesozone to firms without zip code
firms_out_nozip = None
final_missing = None

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
        
        chunk_missing = chunk.loc[chunk['emp_lehd'].isna()]
        chunk_missing = chunk_missing[nozip_col]
        chunk_missing.drop_duplicates(keep = 'first', inplace = True)

            
        chunk = chunk.dropna(subset = ['emp_lehd'])
        bus_ids = chunk.BusID.unique()
        chunk_missing = \
            chunk_missing[~chunk_missing['BusID'].isin(bus_ids)]
        final_missing = pd.concat([final_missing, chunk_missing])

        chunk = chunk.dropna(subset = ['emp_lehd'])
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

# impute last chunk of missing --> county has no lehd emp by industry, so drop industry
final_missing.drop(columns = ['industry'], inplace = True)
final_missing = pd.merge(final_missing, emp_ranking_in_boundary,
                            on = ['CBPZONE'], how = 'left')

final_missing = final_missing.dropna(subset = ['emp_lehd']) 
essential_attr = ['CBPZONE', 'FAFZONE',	'esizecat', 'Industry_NAICS6_Make', 'COUNTY', 'ZIPCODE',
                'Commodity_SCTG', 'emp_per_est', 'BusID']
if assign_enterprise:
    essential_attr.append('FirmID')
final_missing = \
            final_missing.groupby(essential_attr).sample(1,
                                                  weights = final_missing['emp_lehd'],
                                                  replace = False, random_state = 1)
final_missing.drop(columns = ['index', 'industry', 'emp_lehd'], inplace = True)


    
# <codecell>

####################################################
# Step 4 - final formatting and writing outputs ####
####################################################

firms = pd.concat([firms_out_boundary, firms_out_withzip, firms_out_nozip, final_missing])

print('number of firms before writing output:')
print(len(firms))
    
output_attr = ['CBPZONE', 'FAFZONE', 'esizecat', 'Industry_NAICS6_Make',
                'Commodity_SCTG', 'emp_per_est', 'BusID', 'MESOZONE', 'ZIPCODE']
if assign_enterprise:
    output_attr.append('FirmID')
firms = firms[output_attr]
firms = firms.rename(columns = {'emp_per_est': 'Emp'})
firms.to_csv(synthetic_firms_no_location_file, index = False)
