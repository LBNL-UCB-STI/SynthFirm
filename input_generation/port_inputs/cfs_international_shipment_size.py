#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 14 10:29:54 2025

@author: xiaodanxu
"""

# set environment and import packages
import os
from pandas import read_csv
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import warnings

warnings.filterwarnings("ignore")

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')

CFS_file = 'RawData/CFS/CFS2017_national_export-only_20240607.csv'
CFS_export_df = read_csv(CFS_file)
print(CFS_export_df.columns)

# weighted tonnage
print('CFS export sample size:')
print(len(CFS_export_df))
CFS_export_df.loc[:, 'WGT_TONNES'] = CFS_export_df.loc[:, 'WGT_FACTOR'] * \
CFS_export_df.loc[:, 'SHIPMT_WGHT_TON']

# mode by sctg
print('export mode split:')
print(CFS_export_df.groupby('mode_exports')['WGT_TONNES'].sum())
all_mode = CFS_export_df.mode_exports.unique()
sctg_by_mode = pd.pivot_table(CFS_export_df, index = 'SCTG',
                             columns = ['mode_exports'],
                             values = 'WGT_TONNES', aggfunc = 'sum')
sctg_by_mode.to_csv('SynthFirm_parameters/commodity_by_mode_international.csv',
                        index = True)


sctg_by_mode.loc[:, all_mode] = \
    sctg_by_mode.loc[:, all_mode].div(sctg_by_mode.loc[:, all_mode].sum(axis=1), axis=0)
sctg_by_mode.loc[:, 'Airport'] = 1
sctg_by_mode.loc[sctg_by_mode['Air'].isna(), 'Airport'] = 0

sctg_by_mode.loc[:, 'Port'] = 1

sctg_by_mode.loc[:, 'Crossing'] = 1
sctg_by_mode_out = sctg_by_mode[['Airport', 'Port', 'Crossing']]
sctg_by_mode_out = sctg_by_mode_out.reset_index()
sctg_by_mode_out.to_csv('SynthFirm_parameters/commodity_to_port_constraint.csv',
                        index = False)

# generate shipment size by sctg, maybe also region
def quantile(x):
    return x.quantile(0.8)

def lowerci(x):
    return x.quantile(0.025)

def upperci(x):
    return x.quantile(0.975)
CFS_export_df["EXPORT_CNTRY"] = \
CFS_export_df["EXPORT_CNTRY"].astype("category")
shipment_size_by_sctg_cntry = \
CFS_export_df.groupby(['SCTG', 'EXPORT_CNTRY']).agg({'SHIPMT_WGHT_TON':['count', quantile, lowerci, upperci]})
print(len(shipment_size_by_sctg_cntry))
shipment_size_by_sctg_cntry.columns = ['sample size', 'median_weight_ton',
                                       'lower bound', 'upper bound']
shipment_size_by_sctg_cntry = \
shipment_size_by_sctg_cntry.reset_index()
# shipment_size_by_sctg_cntry.head(5)

shipment_size_by_sctg = \
CFS_export_df.groupby(['SCTG']).agg({'SHIPMT_WGHT_TON':['count', quantile, 
                                                        lowerci, upperci]})
shipment_size_by_sctg.columns = ['sample size', 'median_weight_ton', 
                                 'lower bound', 'upper bound']
shipment_size_by_sctg = shipment_size_by_sctg.reset_index()
# shipment_size_by_sctg.head(5)

low_sample_size = shipment_size_by_sctg_cntry['sample size'] <= 10
shipment_size_by_sctg_cntry.loc[low_sample_size, 'median_weight_ton'] = np.nan

shipment_size_by_sctg_to_fill = \
shipment_size_by_sctg_cntry.loc[low_sample_size]

shipment_size_by_sctg_no_fill = \
shipment_size_by_sctg_cntry.loc[~low_sample_size]

shipment_size_by_sctg_to_fill = \
shipment_size_by_sctg_to_fill.drop(columns = ['sample size', 
                                              'median_weight_ton',
                                              'upper bound', 'lower bound'])

shipment_size_by_sctg_to_fill = \
pd.merge(shipment_size_by_sctg_to_fill,
        shipment_size_by_sctg, on = 'SCTG', how = 'left')


shipment_size_by_sctg_cntry = \
pd.concat([shipment_size_by_sctg_to_fill, shipment_size_by_sctg_no_fill])

shipment_size_16_to_fill = \
shipment_size_by_sctg_cntry.loc[shipment_size_by_sctg_cntry['SCTG'] == 17]
shipment_size_16_to_fill.loc[:, 'SCTG'] = 16
shipment_size_by_sctg_cntry = \
pd.concat([shipment_size_by_sctg_cntry, shipment_size_16_to_fill])

print(len(shipment_size_by_sctg_cntry))



shipment_size_by_sctg_cntry.drop(columns = 'sample size', inplace = True)
shipment_size_by_sctg_cntry.columns = ['SCTG', 'CFS_CODE', 'median_weight_ton', 'lower bound', 'upper bound']
shipment_size_by_sctg_cntry.to_csv('SynthFirm_parameters/international_shipment_size.csv',
                                  index = False)

shipment_size_by_sctg_cntry = shipment_size_by_sctg_cntry.reset_index()

# <codecell>
sns.catplot(kind = 'bar', data = shipment_size_by_sctg_cntry,
            x = 'SCTG', y = 'median_weight_ton', 
            row = 'CFS_CODE', sharey = False,
           height = 4.5, aspect = 1.4)
plt.xticks(fontsize = 8)