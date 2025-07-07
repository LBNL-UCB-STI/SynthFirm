#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul  7 10:15:34 2025

@author: xiaodanxu
"""

## Built-in modules
import os

## Third party modules
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from collections import OrderedDict

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')

# CFS data
df = pd.read_csv('RawData/CFS/cfs-2017-puf-csv.csv')

# parameters
cfs_zone = pd.read_csv('SynthFirm_parameters/CFS_FAF_LOOKUP.csv')
sctg_lookup = pd.read_csv('SynthFirm_parameters/SCTG_Groups_revised_V2.csv')

# DROP EXPORT
df.drop(df[df['EXPORT_YN'] == 'Y'].index, inplace = True)

# <codecell>

# variable generation and cleaning

# mode
mode_agg5_dict = {4:'For-hire Truck', 
                  5:'Private Truck', 
                  6:'Rail/IMX', 
                  15:'Rail/IMX', 
                  11:'Air', 
                  14:'Parcel', 
                  0:'Other',
                  2:'Other',
                  3:'Other',
                  7:'Other',
                  8:'Other',
                  10:'Other',
                  12:'Other',
                  13:'Other',
                  16:'Other',
                  17:'Other',
                  18:'Other',
                  19:'Other',
                  20:'Other',
                  101:'Other'}

df['mode_agg5'] = df['MODE'].map(mode_agg5_dict)

## Remove shipment with mode other than the 5 modes

df = df[df['mode_agg5'] != 'Other'] 

# <codecell>

# sctg
df['SCTG'] = df['SCTG'].astype(int)


sctg_lookup = sctg_lookup[['SCTG_Code', 'SCTG_Name', 'SCTG_Group']]
sctg_lookup.rename(columns = {'SCTG_Code': 'SCTG', 
                              'SCTG_Name': 'commodity'}, inplace = True)

df = pd.merge(df, sctg_lookup, on = 'SCTG', how = 'left')

# naics
df['naics2'] = df['NAICS'].astype(str).str[:2].astype(int)
df['naics_name'] = (df.naics2).replace({21:'Mining', 
                                        31:'Manufacturing',
                                        32:'Manufacturing',
                                        33:'Manufacturing',
                                        42:'Wholesale',
                                        45:'Retail',
                                        49:'Trans_Warehouse',
                                        51:'Information',
                                        55:'Mgt_companies'})
# others
df['value_density'] = df['SHIPMT_VALUE']/df['SHIPMT_WGHT']

df['SHIPMT_WGHT_TON'] = df['SHIPMT_WGHT'] / 2000

df['wght_ton_th'] = df['SHIPMT_WGHT_TON'] / 1000

df['wgted_wght_ton_th'] = df['WGT_FACTOR'] * df['SHIPMT_WGHT_TON'] / 1000

# <codecell>

# data cleaning
## based on Stinson et al. (2017)
df.drop(df[(df['mode_agg5'] == 'Air') & (df['SHIPMT_WGHT'] > 15000)].index, inplace = True)
df.drop(df[(df['mode_agg5'] == 'Air') & (df['SHIPMT_WGHT'] > 150) & (df['value_density'] < 1)].index, inplace = True)
df.drop(df[(df['mode_agg5'] == 'Parcel') & (df['SHIPMT_WGHT'] > 150) & (df['value_density'] < 1)].index, inplace = True)
df.drop(df[(df['MODE'] == 6) & (df['SHIPMT_WGHT'] < 1500)].index, inplace = True)
df.drop(df[(df['MODE'] == 6) & (df['value_density'] >= 4)].index, inplace = True)

# <codecell>

# add faf name


faf_mapping = dict(cfs_zone[['ST_MA', 'FAF']].values)

mapping = dict(cfs_zone[['ST_MA', 'SHORTNAME']].values)

df['ORIG_FAFID'] = df['ORIG_CFS_AREA'].map(faf_mapping)
df['DEST_FAFID'] = df['DEST_CFS_AREA'].map(faf_mapping)

df['ORIG_NAME'] = df['ORIG_CFS_AREA'].map(mapping)
df['DEST_NAME'] = df['DEST_CFS_AREA'].map(mapping)

# <codecell>
df = df[['WGT_FACTOR','ORIG_CFS_AREA','ORIG_FAFID', 'ORIG_NAME','DEST_CFS_AREA',
         'DEST_FAFID', 'DEST_NAME', 'SHIPMT_DIST_GC', 'SHIPMT_DIST_ROUTED',
         'commodity', 'SCTG_Group', 'naics_name','wght_ton_th', 'wgted_wght_ton_th',
         'mode_agg5']]

df.loc[:, 'tonmile'] = \
1000 * df.loc[:, 'wgted_wght_ton_th'] * df.loc[:, 'SHIPMT_DIST_ROUTED']

# this part replace the CFS processes in validation code
agg_var = ['ORIG_FAFID', 'ORIG_NAME', 'DEST_FAFID', 'DEST_NAME', 'commodity', 'SCTG_Group', 'mode_agg5']
cfs_distribution_by_zone = \
df.groupby(agg_var)[['wgted_wght_ton_th', 'tonmile']].sum()
cfs_distribution_by_zone = \
cfs_distribution_by_zone.reset_index()
cfs_distribution_by_zone.loc[:, 'SHIPMT_DIST_ROUTED'] = \
cfs_distribution_by_zone.loc[:, 'tonmile'] / cfs_distribution_by_zone.loc[:, 'wgted_wght_ton_th'] / 1000

# write output
cfs_distribution_by_zone.to_csv('Validation/CFS2017_stats_by_zone.csv', index = False)