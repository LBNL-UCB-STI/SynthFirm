#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 11 13:14:15 2023

@author: xiaodanxu
"""

import pandas as pd
import biogeme.database as db
import biogeme.biogeme as bio
import biogeme.models as models
from biogeme.expressions import Beta, DefineVariable, Derive, log, bioDraws, MonteCarlo
import math
import random
import biogeme.results as res
from random import randint
import os, inspect
from sklearn.utils import shuffle
import numpy as np


import warnings
warnings.filterwarnings("ignore")

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')

# <codecell>
# load input tables

region_df = pd.read_csv('RawData/CFS/CFS2017_national_forML_short.csv')
# CFS2017 data with imputation 
factor_df = pd.read_csv('RawData/CFS/data_unitcost_by_zone_cfs2017.csv')
# unit cost from CFS 2017
sctg_group = pd.read_csv('RawData/SCTG_Groups_revised.csv')
# pre-defined sctg group
cfs_to_faf_lookup = pd.read_csv('RawData/CFS_FAF_LOOKUP.csv')
# cfs and faf zone crosswalk

# <codecell>
# clean the data and add attributes

df_clean = region_df.loc[region_df.EXPORT_YN == 'N'] # Cleans out international exports

def removeOutliers(sName, df):
    # Computing IQR
    Q1 = df[sName].quantile(0.25)
    Q3 = df[sName].quantile(0.75)
    IQR = Q3 - Q1

    # Filtering Values between Q1-1.5IQR and Q3+1.5IQR
    filtered = df.query(sName + '<= (@Q3 + 3 * @IQR)')
    
    return filtered

df_clean = removeOutliers('SHIPMT_WGHT', df_clean)
df_clean = removeOutliers('SHIPMT_DIST', df_clean)
print(len(df_clean))

# add SCTG group
df_clean = pd.merge(df_clean, sctg_group,
                    left_on = 'SCTG',
                    right_on = 'SCTG_Code',
                    how = 'left')

# keep essential variables
var_to_keep = ['SHIPMT_ID', 'ORIG_CFS_AREA', 'DEST_CFS_AREA', 'NAICS', 'naics_name',
               'SCTG', 'SCTG_Group', 'SHIPMT_VALUE', 'SHIPMT_WGHT', 'SHIPMT_DIST', 'value_density']

df_clean_choice_model = df_clean[var_to_keep]

# <codecell>
# generate avg. routed distance between O-D
dist_matrix = df_clean_choice_model.groupby(['ORIG_CFS_AREA', 'DEST_CFS_AREA'])[['SHIPMT_DIST']].mean()
# dist_matrix.columns = ['Distance']
dist_matrix = dist_matrix.reset_index()

cfs_to_faf_short = cfs_to_faf_lookup[['ST_MA',	'FAF']]

dist_matrix = pd.merge(dist_matrix, cfs_to_faf_short, 
                       left_on = 'ORIG_CFS_AREA', right_on = 'ST_MA', how = 'left')
dist_matrix = dist_matrix.rename(columns = {'FAF': 'orig_FAFID'})
dist_matrix = pd.merge(dist_matrix, cfs_to_faf_short, 
                       left_on = 'DEST_CFS_AREA', right_on = 'ST_MA', how = 'left')
dist_matrix = dist_matrix.rename(columns = {'FAF': 'dest_FAFID'})
dist_matrix = dist_matrix[['ORIG_CFS_AREA', 'DEST_CFS_AREA', 'orig_FAFID', 'dest_FAFID', 'SHIPMT_DIST']]
dist_matrix_out = dist_matrix.rename(columns = {'SHIPMT_DIST': 'Distance'})
dist_matrix_out.to_csv('RawData/CFS/CFS2017_routed_distance_matrix.csv', index = False)

# <codecell>
# generate chosen and non-chosen data
df_clean_choice_model = df_clean_choice_model.sample(frac=0.05, replace=False, random_state=1)
# use 5% of CFS data to estimate model

# select non-chosen candidate locations by NAICS industry 
# (different industry has different spatial distribution)
unique_naics = df_clean_choice_model.NAICS.unique()
df_clean_choice_model.loc[:, 'chosen'] = 0
supplier_selection_set = None
batch_size = 10
for naics in unique_naics:
    print(naics)
    all_suppliers = \
    df_clean_choice_model.loc[df_clean_choice_model['NAICS'] == naics] 
    # find selected suppliers in specified industry
    
    chunk_of_suppliers = np.array_split(all_suppliers, batch_size)
    # create chunks for batch processing

    for i in range(batch_size):
        chunk = chunk_of_suppliers[i]
        chunk.loc[:, 'chosen'] = 1
        # the code below generates non-chosen suppliers under the same industry and their attributes
        shipment_to_match = chunk[['SHIPMT_ID', 'DEST_CFS_AREA', 
                                   'NAICS', 'naics_name', 'SCTG', 'SCTG_Group', 'SHIPMT_WGHT']]
        selected_shipment = chunk.SHIPMT_ID.unique()
        non_chosen_set = \
        all_suppliers.loc[~all_suppliers['SHIPMT_ID'].isin(selected_shipment)]
        non_chosen_set = non_chosen_set[['ORIG_CFS_AREA', 'NAICS', 
                                         'naics_name', 'SCTG', 'value_density', 'chosen']]
        non_chosen_set = pd.merge(shipment_to_match, non_chosen_set,
                                  on = ['NAICS', 'naics_name', 'SCTG'], how = 'left')
        #impute distance
        non_chosen_set = pd.merge(non_chosen_set, dist_matrix, 
                                  on = ['ORIG_CFS_AREA', 'DEST_CFS_AREA'], how = 'left')       
        non_chosen_set.loc[:, 'SHIPMT_DIST'].fillna(5000, inplace = True)
        #impute total value based on supplier's unit cost
        non_chosen_set.loc[:, 'value_density'] = \
        non_chosen_set.loc[:,'value_density'].fillna(non_chosen_set.groupby('SCTG')['value_density'].transform('mean'))
        non_chosen_set.loc[:, 'SHIPMT_VALUE'] = \
        non_chosen_set.loc[:, 'SHIPMT_WGHT'] * non_chosen_set.loc[:, 'value_density']
        non_chosen_set = non_chosen_set.groupby('SHIPMT_ID').sample(n=9, replace = True, random_state=1)
        
        # combine chosen and non-chosen sets
        combined_set = pd.concat([chunk, non_chosen_set])
        combined_set = combined_set.sort_values('SHIPMT_ID')
        supplier_selection_set = pd.concat([supplier_selection_set, combined_set])

# <codecell>
# creat dependent variable label
supplier_selection_set.loc[:, 'SHIPMT_VALUE'] = \
supplier_selection_set.loc[:, 'SHIPMT_VALUE'].fillna(supplier_selection_set.groupby('SHIPMT_ID')['SHIPMT_VALUE'].transform('mean'))
supplier_selection_set = shuffle(supplier_selection_set)
print(len(supplier_selection_set))
supplier_selection_set = supplier_selection_set.sort_values('SHIPMT_ID')
supplier_selection_set['alternative'] = \
supplier_selection_set.groupby('SHIPMT_ID').cumcount() + 1
supplier_selection_set['choice'] = 0
supplier_selection_set.loc[supplier_selection_set['chosen'] == 1, 'choice'] = \
supplier_selection_set.loc[supplier_selection_set['chosen'] == 1, 'alternative']

# append capacity (a variable not used but worth looking into)
supplier_selection_set = pd.merge(supplier_selection_set, factor_df,
                                 left_on = ['ORIG_CFS_AREA', 'SCTG'],
                                 right_on = ['ORIG_CFS_AREA', 'Commodity_SCTG'],
                                 how = 'left')
supplier_selection_set.loc[:, 'Capacity'] = \
supplier_selection_set.loc[:, 'Capacity'].fillna(supplier_selection_set.groupby('SHIPMT_ID')['Capacity'].transform('mean'))
supplier_selection_set.head(5)

# <codecell>
# convert long data to wide for estimation
choice = supplier_selection_set.loc[supplier_selection_set['chosen'] == 1, 
                                     ['SHIPMT_ID',	'NAICS', 'SCTG_Group', 'choice']]

factor_1 = pd.pivot_table(supplier_selection_set, values='SHIPMT_VALUE', index=['SHIPMT_ID'],
                    columns=['alternative'], aggfunc=np.mean)
factor_1.columns = ['value_' + str(i+1) for i in range(10)]
factor_1 = factor_1.reset_index()
# factor_1.head(5)

factor_2 = pd.pivot_table(supplier_selection_set, values='SHIPMT_DIST', index=['SHIPMT_ID'],
                    columns=['alternative'], aggfunc=np.mean)
factor_2.columns = ['distance_' + str(i+1) for i in range(10)]
# factor_2 = factor_2.fillna(1)
factor_2 = factor_2.reset_index()

factor_3 = pd.pivot_table(supplier_selection_set, values='Capacity', index=['SHIPMT_ID'],
                    columns=['alternative'], aggfunc=np.mean)
factor_3.columns = ['capacity_' + str(i+1) for i in range(10)]
# factor_2 = factor_2.fillna(1)
factor_3 = factor_3.reset_index()
# factor_2.head(5)

destination_choice_data_wide = pd.merge(choice, factor_1, 
                                        on = 'SHIPMT_ID', how = 'left')
destination_choice_data_wide = pd.merge(destination_choice_data_wide, factor_2, 
                                        on = 'SHIPMT_ID', how = 'left')
destination_choice_data_wide = pd.merge(destination_choice_data_wide, factor_3, 
                                        on = 'SHIPMT_ID', how = 'left')

# add availability
destination_choice_data_wide.loc[:, 'av_1'] = 1
destination_choice_data_wide.loc[:, 'av_2'] = 1
destination_choice_data_wide.loc[:, 'av_3'] = 1
destination_choice_data_wide.loc[:, 'av_4'] = 1
destination_choice_data_wide.loc[:, 'av_5'] = 1
destination_choice_data_wide.loc[:, 'av_6'] = 1
destination_choice_data_wide.loc[:, 'av_7'] = 1
destination_choice_data_wide.loc[:, 'av_8'] = 1
destination_choice_data_wide.loc[:, 'av_9'] = 1
destination_choice_data_wide.loc[:, 'av_10'] = 1

# <codecell>
sctg_groups = destination_choice_data_wide.SCTG_Group.unique()
for sctg in sctg_groups:
    print(sctg)
    destination_choice_data_selected = \
    destination_choice_data_wide.loc[destination_choice_data_wide['SCTG_Group'] == sctg]
    database = db.Database('destination_choice', destination_choice_data_selected)
    globals().update(database.variables)
    database.fullData
    # define parameters
    B_VALUE = Beta('B_VALUE', 0, None, None, 0)
    B_DISTANCE_LOW = Beta('B_DISTANCE_LOW', 0, None, None, 0)
    B_DISTANCE_HIGH = Beta('B_DISTANCE_HIGH', 0, None, None, 0)
    B_DISTANCE_CONST = Beta('B_DISTANCE_CONST', 0, None, None, 0)
#     B_CAPACITY = Beta('B_CAPACITY', 0, None, None, 0)

    # B_VALUE_S = Beta('B_VALUE_S', 0.0001, None, None, 0)
    # B_DISTANCE_S = Beta('B_DISTANCE_S', 0.01, None, None, 0)
    # Define a random parameter with a normal distribution, designed to be used
    # for quasi Monte-Carlo simulation with Halton draws (base 5).
    # B_VALUE_RND = B_VALUE + B_VALUE_S * bioDraws('B_TIME_RND', 'NORMAL_HALTON5')
    # B_DISTANCE_RND = B_DISTANCE + B_DISTANCE_S * bioDraws('B_DISTANCE_RND', 'NORMAL_HALTON5')


    V1 = B_VALUE * value_1 + B_DISTANCE_LOW * distance_1 * (distance_1 <= 500) + \
    B_DISTANCE_HIGH * distance_1 * (distance_1 > 500) + \
    B_DISTANCE_CONST * (distance_1 > 500)
    
    V2 = B_VALUE * value_2 + B_DISTANCE_LOW * distance_2 * (distance_2 <= 500) + \
    B_DISTANCE_HIGH * distance_2 * (distance_2 > 500) + \
    B_DISTANCE_CONST * (distance_2 > 500)
    
    V3 = B_VALUE * value_3 + B_DISTANCE_LOW * distance_3 * (distance_3 <= 500) + \
    B_DISTANCE_HIGH * distance_3 * (distance_3 > 500) + \
    B_DISTANCE_CONST * (distance_3 > 500)
    
    V4 = B_VALUE * value_4 + B_DISTANCE_LOW * distance_4 * (distance_4 <= 500) + \
    B_DISTANCE_HIGH * distance_4 * (distance_4 > 500) + \
    B_DISTANCE_CONST * (distance_4 > 500)
    
    V5 = B_VALUE * value_5 + B_DISTANCE_LOW * distance_5 * (distance_5 <= 500) + \
    B_DISTANCE_HIGH * distance_5 * (distance_5 > 500) + \
    B_DISTANCE_CONST * (distance_5 > 500)
    
    V6 = B_VALUE * value_6 + B_DISTANCE_LOW * distance_6 * (distance_6 <= 500) + \
    B_DISTANCE_HIGH * distance_6 * (distance_6 > 500) + \
    B_DISTANCE_CONST * (distance_6 > 500)
    
    V7 = B_VALUE * value_7 + B_DISTANCE_LOW * distance_7 * (distance_7 <= 500) + \
    B_DISTANCE_HIGH * distance_7 * (distance_7 > 500) + \
    B_DISTANCE_CONST * (distance_7 > 500)
    
    V8 = B_VALUE * value_8 + B_DISTANCE_LOW * distance_8 * (distance_8 <= 500) + \
    B_DISTANCE_HIGH * distance_8 * (distance_8 > 500) + \
    B_DISTANCE_CONST * (distance_8 > 500)
    
    V9 = B_VALUE * value_9 + B_DISTANCE_LOW * distance_9 * (distance_9 <= 500) + \
    B_DISTANCE_HIGH * distance_9 * (distance_9 > 500) + \
    B_DISTANCE_CONST * (distance_9 > 500)
    
    V10 = B_VALUE * value_10 + B_DISTANCE_LOW * distance_10 * (distance_10 <= 500) + \
    B_DISTANCE_HIGH * distance_10 * (distance_10 > 500) + \
    B_DISTANCE_CONST * (distance_10 > 500)

    V = {1: V1, 2: V2, 3: V3, 4: V4, 5: V5,
        6: V6, 7: V7, 8: V8, 9: V9, 10: V10}

    av = {1: av_1, 2: av_2, 3: av_3, 4: av_4, 5: av_5, 
          6: av_6, 7: av_7, 8: av_8, 9: av_9, 10: av_10}
    # logprob = models.loglogit(V, None, choice)
    logprob = models.loglogit(V, av, choice)
    biogeme = bio.BIOGEME(database, logprob)
    biogeme.modelName = 'supplier_selection_national_sctg' + str(sctg)
    biogeme.calculateNullLoglikelihood(av)
    # Estimate the parameters
    results = biogeme.estimate()

    # Get the results in a pandas table
    pandasResults = results.getEstimatedParameters()
    goodness_of_fit = results.getGeneralStatistics()
    print('estimation results')
    print(pandasResults)
    print(goodness_of_fit['Rho-square for the null model'][0])