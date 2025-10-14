#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 11 12:03:20 2023

@author: xiaodanxu
"""

import pandas as pd
import os, inspect
from sklearn.utils import shuffle
import numpy as np
from statsmodels.stats.weightstats import DescrStatsW
from pandas import read_csv
import matplotlib.pyplot as plt
import seaborn as sns

import warnings
warnings.filterwarnings("ignore")

plt.style.use('ggplot')
sns.set(font_scale=1.2)  # larger font

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync/')

# <codecell>
# load 2017 CFS data
CFS_df = pd.read_csv('RawData/CFS/cfs-2017-puf-csv.csv', sep = ',')
print(len(CFS_df))
# load additional input data
naics_lookup = pd.read_csv('RawData/corresp_naics6_n6io_sctg_revised.csv')
# load industry-commodity lookup
sctg_group_lookup = pd.read_csv('RawData/SCTG_Groups_revised_V2.csv')
# load sctg - sctg group lookup
sctg_group_lookup.loc[:, 'SCTG_Code'] = sctg_group_lookup.loc[:, 'SCTG_Code'].astype(str)
sctg_group_lookup.loc[:, 'SCTG_Code'] = sctg_group_lookup.loc[:, 'SCTG_Code'].str.zfill(2)

cfs_to_faf_lookup = pd.read_csv('RawData/CFS_FAF_LOOKUP.csv', sep = ',')
# load CFS zone and FAF zone lookup
# clean CFS data

df_clean = CFS_df.loc[CFS_df.EXPORT_YN == 'N'] # Cleans out international exports
lb_to_ton = 0.0005

df_clean.loc[df_clean['SCTG'] == '15-19', 'SCTG'] = '16'
# value_density_by_sctg_zone.loc[value_density_by_sctg_zone['SCTG'] == '15-19', 'SCTG'] = '16'
to_drop = ['35-38', '25-30', '31-34', '10-14', '06-09', 
           '20-24', '01-05', '39-43']
df_clean = \
df_clean[~df_clean['SCTG'].isin(to_drop)]

df_clean = pd.merge(df_clean, sctg_group_lookup,
                   left_on = 'SCTG', right_on = 'SCTG_Code', how = 'left')

df_clean.loc[:, 'SHIPMT_WGHT_scaled'] = df_clean.loc[:, 'SHIPMT_WGHT'] * \
df_clean.loc[:, 'WGT_FACTOR'] * lb_to_ton # in tons
df_clean.loc[:, 'SHIPMT_VALUE_scaled'] = df_clean.loc[:, 'SHIPMT_VALUE'] * df_clean.loc[:, 'WGT_FACTOR']
df_clean.loc[:, 'unitcost'] = df_clean.loc[:, 'SHIPMT_VALUE'] / \
df_clean.loc[:, 'SHIPMT_WGHT'] / lb_to_ton # in $/ton


# <codecell>
# Output 1 - compute value density by SCTG as a surrogate of unit cost
# estimate value density -- no region code
value_density_by_sctg = df_clean.groupby(['SCTG'])[['SHIPMT_WGHT_scaled', 'SHIPMT_VALUE_scaled']].sum()
value_density_by_sctg = value_density_by_sctg.reset_index()
                                  
# value_density_by_sctg.head(10)
value_density_by_sctg.loc[:, 'unitcost'] = value_density_by_sctg.loc[:, 'SHIPMT_VALUE_scaled'] / \
value_density_by_sctg.loc[:, 'SHIPMT_WGHT_scaled'] # in $/ton

value_density_by_sctg = value_density_by_sctg[['SCTG', 'unitcost']]
value_density_by_sctg.columns = ['Commodity_SCTG', 'UnitCost']
value_density_by_sctg.to_csv('RawData/CFS/data_unitcost_cfs2017.csv', index = False)

# estimate variation for checking
def weighted_std(df):
    stats = DescrStatsW(df['unitcost'], weights=df['SHIPMT_WGHT_scaled'], ddof=1)
    w_mean = stats.mean
    w_std = stats.std
    return(pd.Series([w_mean, w_std]))
value_density_by_sctg_2 = df_clean.groupby(['SCTG']).apply(weighted_std) 
value_density_by_sctg_2 = value_density_by_sctg_2.reset_index() # weighted mean and std
#value_density_by_sctg_2.head(10)

# <codecell>
# Output 2 - generate production capacity and unit cost by origin/seller zone

value_density_by_sctg_zone = df_clean.groupby(['SCTG', 'ORIG_CFS_AREA'])[['SHIPMT_WGHT_scaled', 'SHIPMT_VALUE_scaled']].sum()

value_density_by_sctg_zone = value_density_by_sctg_zone.reset_index()
value_density_by_sctg_zone.loc[:, 'unitcost'] = value_density_by_sctg_zone.loc[:, 'SHIPMT_VALUE_scaled'] / \
value_density_by_sctg_zone.loc[:, 'SHIPMT_WGHT_scaled']

value_density_by_sctg_zone = \
value_density_by_sctg_zone[~value_density_by_sctg_zone['SCTG'].isin(to_drop)]
value_density_by_sctg_zone = value_density_by_sctg_zone[['SCTG', 'ORIG_CFS_AREA', 'SHIPMT_WGHT_scaled', 'unitcost']]
value_density_by_sctg_zone.columns = ['Commodity_SCTG', 'ORIG_CFS_AREA', 'Capacity', 'UnitCost']

value_density_by_sctg_zone.head(5)
value_density_by_sctg_zone.to_csv('RawData/CFS/data_unitcost_by_zone_cfs2017.csv', index = False)

# <codecell>
# Output 3 - estimate shipment size (using pre-defined quantile cut-off point)
def weighted_quantile(df, quantile=0.5):
    df_sorted = df.sort_values('SHIPMT_WGHT')
    cumsum = df_sorted['SHIPMT_WGHT_scaled'].cumsum()
    cutoff = df_sorted['SHIPMT_WGHT_scaled'].sum() * quantile
    value = df_sorted[cumsum >= cutoff]['SHIPMT_WGHT'].iloc[0]
    return(value)
load_by_sctg = df_clean.groupby(['SCTG']).apply(weighted_quantile)
load_by_sctg = load_by_sctg.reset_index()

load_by_sctg.columns = ['Commodity_SCTG', 'SHIPMT_WGHT']
load_by_sctg.to_csv('RawData/CFS/max_load_per_shipment_50percent.csv', index = False)

# <codecell>
# check sample size by sctg
count_by_sctg = df_clean.groupby(['SCTG'])[['SHIPMT_WGHT']].count()
count_by_sctg.columns = ['sample_size']
count_by_sctg = count_by_sctg.reset_index()
print(count_by_sctg)

# <codecell>
# Output 4 - Generate shipment value allocation factor by zone

value_fraction_by_origin = df_clean.groupby(['SCTG', 'ORIG_CFS_AREA'])[['SHIPMT_VALUE_scaled']].sum()
value_fraction_by_origin = value_fraction_by_origin.reset_index()

value_fraction_by_dest = df_clean.groupby(['SCTG', 'DEST_CFS_AREA'])[['SHIPMT_VALUE_scaled']].sum()
value_fraction_by_dest = value_fraction_by_dest.reset_index()


value_fraction_by_origin_faf = pd.merge(value_fraction_by_origin, 
                                        cfs_to_faf_lookup, 
                                        left_on = 'ORIG_CFS_AREA',
                                        right_on = 'ST_MA', how = 'left')

value_fraction_by_dest_faf = pd.merge(value_fraction_by_dest, 
                                        cfs_to_faf_lookup, 
                                        left_on = 'DEST_CFS_AREA',
                                        right_on = 'ST_MA', how = 'left')

value_fraction_by_origin_faf = value_fraction_by_origin_faf.dropna(subset = ['FAF'])
value_fraction_by_origin_faf.loc[:, 'value_fraction'] = value_fraction_by_origin_faf.loc[:, 'SHIPMT_VALUE_scaled'] / \
value_fraction_by_origin_faf.groupby('SCTG')['SHIPMT_VALUE_scaled'].transform('sum')


value_fraction_by_dest_faf = value_fraction_by_dest_faf.dropna(subset = ['FAF'])
value_fraction_by_dest_faf.loc[:, 'value_fraction'] = value_fraction_by_dest_faf.loc[:, 'SHIPMT_VALUE_scaled'] / \
value_fraction_by_dest_faf.groupby('SCTG')['SHIPMT_VALUE_scaled'].transform('sum')

output_attr = ['SCTG', 'FAF', 'value_fraction']
output_attr_label = ['Commodity_SCTG', 'FAF', 'value_fraction']
value_fraction_by_origin_faf = value_fraction_by_origin_faf[output_attr]
value_fraction_by_origin_faf.columns = output_attr_label

value_fraction_by_dest_faf = value_fraction_by_dest_faf[output_attr]
value_fraction_by_dest_faf.columns = output_attr_label

value_fraction_by_origin_faf.to_csv('RawData/CFS/producer_value_fraction_by_faf.csv', index = False)
value_fraction_by_dest_faf.to_csv('RawData/CFS/consumer_value_fraction_by_faf.csv', index = False)

# <codecell>
# Output 5 - seasonal allocation factor for national freight trip generation -- future use
shipment_by_season = df_clean.groupby(['SCTG_Group', 'SCTG_Name', 'QUARTER'])[['SHIPMT_WGHT_scaled', 'WGT_FACTOR']].sum()
shipment_by_season = shipment_by_season.reset_index()

shipment_by_season.loc[:, 'weight_fraction'] = \
shipment_by_season.loc[:, 'SHIPMT_WGHT_scaled'] / \
shipment_by_season.groupby('SCTG_Group')['SHIPMT_WGHT_scaled'].transform('sum')

shipment_by_season.loc[:, 'count_fraction'] = \
shipment_by_season.loc[:, 'WGT_FACTOR'] / \
shipment_by_season.groupby('SCTG_Group')['WGT_FACTOR'].transform('sum')
shipment_by_season.loc[:, 'SCTG_Group'] = shipment_by_season.loc[:, 'SCTG_Group'].astype(int)

shipment_by_season_out = shipment_by_season[['SCTG_Group', 'SCTG_Name', 'QUARTER', 'weight_fraction']]
shipment_by_season_out.head(5)
shipment_by_season_out.to_csv('RawData/CFS/seasonal_allocation_factor_by_sctg.csv')

sns.barplot(data=shipment_by_season_out, 
            x="SCTG_Name", y="weight_fraction", hue="QUARTER")
plt.legend(loc='upper right', bbox_to_anchor=(1.2,1.02),
          title = 'Quarter')
plt.xlabel('SCTG Group')
plt.ylabel('Fraction of shipment by weight')
plt.savefig('RawData/CFS/CFS2017_seasonal_variation.png', dpi = 200, bbox_inches = 'tight')