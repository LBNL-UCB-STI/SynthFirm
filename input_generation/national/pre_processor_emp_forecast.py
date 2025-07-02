#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb  5 10:17:52 2025

@author: xiaodanxu
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
import os
from pandas import read_csv
import numpy as np

warnings.filterwarnings("ignore")

# define synthfirm parameters
data_path = '/Users/xiaodanxu/Documents/SynthFirm.nosync'
analysis_year = '2020'
region_name = 'national'
naics_location_name = 'national'
os.chdir(data_path)

### calibration inputs ###
# path to PSRC data
data_dir = 'inputs_' + region_name

### SynthFirm inputs ###
# employment by zone #
synthfirm_empranking_file = 'data_mesozone_emprankings.csv'
emp_ranking_file = os.path.join(data_dir, synthfirm_empranking_file)
emp_ranking = read_csv(emp_ranking_file)

# emp calibration 
emp_calibration_file = naics_location_name + '_' + analysis_year + '_naics_county.csv'
emp_calibration_path = os.path.join(data_dir, emp_calibration_file)
emp_calibration = pd.read_csv(emp_calibration_path, sep = ',')

# <codecell>

# processing emp
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

emp_calibration = emp_calibration.fillna(0)
emp_calibration = \
    emp_calibration.groupby(['CBPZONE'])[emp_attr].sum()
emp_calibration.columns = emp_colnames
emp_calibration = emp_calibration.reset_index()
emp_calibration.rename(columns = {'CBPZONE': 'COUNTY'}, inplace = True)

# <codecell>
## scale emp ranking
emp_ranking.fillna(0, inplace = True)
emprank_for_scaling = emp_ranking.groupby('COUNTY')[emp_colnames].sum()
emprank_for_scaling = emprank_for_scaling.reset_index()

emprank_for_scaling = pd.melt(emprank_for_scaling, id_vars = ["COUNTY"],
                               value_vars = emp_colnames,
                                  var_name= 'industry', value_name='emp_base')

emp_calibration_for_scaling = pd.melt(emp_calibration, id_vars = ["COUNTY"],
                               value_vars = emp_colnames,
                                  var_name= 'industry', value_name='emp_calib')

# <codecell>

emp_adj = pd.merge( emprank_for_scaling, emp_calibration_for_scaling, 
                    on = ['industry', 'COUNTY'],
                    how = 'left')
emp_adj.fillna(0, inplace = True)
emp_adj.loc[:, 'emp_adj'] = \
    emp_adj.loc[:, 'emp_calib'] / emp_adj.loc[:, 'emp_base']
idx_zero = ((emp_adj.loc[:, 'emp_calib'] == 0) &\
            (emp_adj.loc[:, 'emp_base'] == 0))
emp_adj.loc[idx_zero, 'emp_adj'] = 0
# psrc_emp_long = pd.melt(psrc_emp_by_cbg, id_vars=['Census2010BlockGroup', 'BaseYear'],
#                         value_vars=psrc_sector, var_name='industry', value_name='PSRC_emp')
# <codecell>
emp_ranking_long = pd.melt(emp_ranking, id_vars=['MESOZONE', 'COUNTY'],
                        var_name='industry', value_name='emp_base')

emp_adj = emp_adj[['industry', 'COUNTY', 'emp_calib', 'emp_adj']]

emp_adj_is_inf = emp_adj.loc[np.isinf(emp_adj['emp_adj'])]
emp_adj_no_inf = emp_adj.loc[~np.isinf(emp_adj['emp_adj'])]



# # formatting industry names
# emp_ranking_long.loc[:, 'NAICS'] = emp_ranking_long.loc[:, 'NAICS'].str.split('rank').str[1]
# print(emp_ranking_long['NAICS'].unique())
# psrc_emp_long.loc[:,'industry'] = psrc_emp_long.loc[:,'industry'].str.split('_').str[1]
# print(psrc_emp_long['industry'].unique())

# industry_mapping = {
#     'Other': ['11', '21', '23'],
#     'Industrial': ['3133', '22', '42', '4849'],
#     'Retail': ['4445'],
#     'Office': ['51', '52', '53', '54', '55', '56'],
#     'Education': ['61'],
#     'Medical': ['62'],
#     'Service': ['71', '72', '81'],
#     'Government': ['92']
# }

# emp_ranking_long.loc[:, 'industry'] = np.nan
# for col, values in industry_mapping.items():
#     emp_ranking_long.loc[emp_ranking_long['NAICS'].isin(values), 'industry'] = col
    
# # <codecell>

# # generating fraction of emp by naics among each industry

# psrc_emp_long.rename(columns = {'Census2010BlockGroup': 'MESOZONE'}, inplace = True)
# psrc_emp_long.loc[:, 'MESOZONE'] = psrc_emp_long.loc[:, 'MESOZONE'].astype(int)

# emp_ranking_long = pd.merge(emp_ranking_long, psrc_emp_long,
#                             on = ['MESOZONE', 'industry'], how = 'left')


# print('total employment from PSRC data:')
# print(psrc_emp_long.loc[:, 'PSRC_emp'].sum())
# emp_ranking_long.loc[:, 'PSRC_emp'].fillna(0, inplace = True)


# frac_among_ind = emp_ranking_long.groupby(['industry', 'NAICS'])['LEHD_emp'].sum()
# frac_among_ind = frac_among_ind.reset_index()
# frac_among_ind.loc[:, 'fraction'] = frac_among_ind.loc[:, 'LEHD_emp']/ \
#     frac_among_ind.groupby('industry')['LEHD_emp'].transform('sum')
# frac_among_ind.rename(columns = {'LEHD_emp': 'emp_by_naics'}, inplace = True)

# emp_ranking_long = pd.merge(emp_ranking_long, frac_among_ind,
#                             on = ['industry', 'NAICS'], how = 'left')

# # <codecell>

# # Define imputation method, variable 'imp_flag'
# emp_ranking_long.loc[:, 'emp_adj'] = emp_ranking_long.loc[:, 'LEHD_emp']

# emp_ranking_long.loc[:, 'LEHD_emp_sum'] = \
#     emp_ranking_long.groupby(['MESOZONE', 'industry'])['LEHD_emp'].transform('sum')
    
# emp_ranking_long.loc[:, 'imp_flag'] = 1 # scaling employment if both LEHD and PSRC data are non-zero
 
# criteria_1 = (emp_ranking_long['LEHD_emp_sum'] == 0) & (emp_ranking_long['PSRC_emp'] == 0)
# emp_ranking_long.loc[criteria_1, 'imp_flag'] = 0 # no adjustment ==> all zero

# criteria_2 = (emp_ranking_long['LEHD_emp_sum'] > 0) & (emp_ranking_long['PSRC_emp'] == 0)
# emp_ranking_long.loc[criteria_2, 'imp_flag'] = 2 # wipe out LEHD emp

# criteria_3 = (emp_ranking_long['LEHD_emp_sum'] == 0) & (emp_ranking_long['PSRC_emp'] > 0)
# emp_ranking_long.loc[criteria_3, 'imp_flag'] = 3 # assign PSRC emp and distribute across NAICS

# adj_threshold = 0.001
# psrc_total = psrc_emp_long.loc[:, 'PSRC_emp'].sum()
# diff_ratio = abs(emp_ranking_long.loc[:, 'emp_adj'].sum()/psrc_total - 1)

# iterator = 1
# while diff_ratio > adj_threshold:
#     emp_ranking_long.loc[:, 'LEHD_emp_sum'] = \
#         emp_ranking_long.groupby(['MESOZONE', 'industry'])['emp_adj'].transform('sum')
        
#     print('this is the iteration number ' + str(iterator))
#     print('total employment from LEHD data (before adjustment):')
#     print(emp_ranking_long.loc[:, 'emp_adj'].sum())
    
#     emp_ranking_long.loc[:, 'adj_factor'] = \
#         emp_ranking_long.loc[:, 'PSRC_emp'] / emp_ranking_long.loc[:, 'LEHD_emp_sum'] 
    
#     # wipe out emp if PSRC_emp is zero    
#     emp_ranking_long.loc[emp_ranking_long['imp_flag']==2, 'emp_adj'] = 0
    
#     # distribute PSRC employment if LEHD employment is zero    
#     emp_ranking_long.loc[emp_ranking_long['imp_flag']==3, 'emp_adj'] = \
#         emp_ranking_long.loc[emp_ranking_long['imp_flag']==3,'PSRC_emp'] * \
#             emp_ranking_long.loc[emp_ranking_long['imp_flag']==3,'fraction']
            
#     # scale lehd employment if both sets are none zero   
#     emp_ranking_long.loc[emp_ranking_long['imp_flag']==1, 'emp_adj'] = \
#         emp_ranking_long.loc[emp_ranking_long['imp_flag']==1,'emp_adj'] * \
#             emp_ranking_long.loc[emp_ranking_long['imp_flag']==1,'adj_factor']
#     emp_ranking_long.loc[:, 'emp_adj'] = np.round(emp_ranking_long.loc[:, 'emp_adj'], 0)  
     
#     print('total employment from LEHD data (after adjustment):')
#     print(emp_ranking_long.loc[:, 'emp_adj'].sum())
#     diff_ratio = abs(emp_ranking_long.loc[:, 'emp_adj'].sum()/psrc_total - 1)
    
#     print(diff_ratio)
#     iterator += 1

# # <codecell>

# # convert data back to emp ranking
# emp_ranking_adjusted = emp_ranking_long[['MESOZONE', 'COUNTY', 'NAICS', 'emp_adj']]
# emp_ranking_adjusted.loc[:, 'NAICS'] = \
#     'rank' + emp_ranking_adjusted.loc[:, 'NAICS']

# emp_ranking_adjusted = pd.pivot_table(emp_ranking_adjusted, 
#                                       index = ['MESOZONE', 'COUNTY'],
#                                       columns = 'NAICS', 
#                                       values = 'emp_adj',
#                                       aggfunc= 'sum')

# emp_ranking_adjusted = emp_ranking_adjusted.reset_index()
# emp_ranking_output = pd.concat([emp_ranking_adjusted, emp_ranking_no_adj])

# output_file = os.path.join(output_dir,'data_mesozone_emprankings_2050.csv')
# emp_ranking_output.to_csv(output_file, index = False)

