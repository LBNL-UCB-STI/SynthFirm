#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 21 22:41:44 2025

@author: xiaodanxu
"""

import pandas as pd
import os
import math
import random
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
faf_gc_distance = pd.read_csv('RawData/FAF_od_gc_distance.csv')
faf_routed_distance = pd.read_csv('RawData/FAF_od_highway_distance.csv')
# <codecell>
df_clean = region_df.loc[region_df.EXPORT_YN == 'N'] # Cleans out international exports

var_to_keep = ['SHIPMT_ID', 'ORIG_CFS_AREA', 'DEST_CFS_AREA', 'NAICS', 'naics_name',
               'SCTG', 'SCTG_Group', 'SHIPMT_VALUE', 'SHIPMT_WGHT', 'SHIPMT_DIST', 'value_density']
# add SCTG group
df_clean = pd.merge(df_clean, sctg_group,
                    left_on = 'SCTG',
                    right_on = 'SCTG_Code',
                    how = 'left')

df_clean_choice_model = df_clean[var_to_keep]

# <codecell>
# generate avg. routed distance between O-D
dist_matrix = df_clean_choice_model.groupby(['ORIG_CFS_AREA', 'DEST_CFS_AREA'])[['SHIPMT_DIST']].mean()
# dist_matrix.columns = ['Distance']
dist_matrix = dist_matrix.reset_index()

cfs_to_faf_short = cfs_to_faf_lookup[['ST_MA',	'FAF']]

dist_matrix = pd.merge(cfs_to_faf_short, dist_matrix, 
                       left_on = 'ST_MA',right_on = 'ORIG_CFS_AREA',  how = 'left')
dist_matrix = dist_matrix.rename(columns = {'FAF': 'orig_FAFID'})
dist_matrix = pd.merge(cfs_to_faf_short, dist_matrix, 
                       left_on = 'ST_MA', right_on = 'DEST_CFS_AREA', how = 'left')
dist_matrix = dist_matrix.rename(columns = {'FAF': 'dest_FAFID'})
dist_matrix = dist_matrix[['ORIG_CFS_AREA', 'DEST_CFS_AREA', 'orig_FAFID', 'dest_FAFID', 'SHIPMT_DIST']]

# <codecell>
dist_matrix_out = dist_matrix.rename(columns = {'SHIPMT_DIST': 'Distance'})
dist_matrix_out.to_csv('RawData/CFS/CFS2017_routed_distance_matrix.csv', index = False)