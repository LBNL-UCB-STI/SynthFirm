#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 13:36:21 2024

@author: xiaodanxu
"""

from pandas import read_csv
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings("ignore")

plt.style.use('ggplot')
sns.set(font_scale=1.2)  # larger font

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')


# load input

# list_of_matrix_file = ['penn_2018_7_75_50.csv']
path_to_moves = 'RawData/MOVES'
path_to_vius = 'RawData/US_VIUS_2021'
plot_dir = 'RawData/MOVES/plot_forecast/'

hpms_definition = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'HPMS_definition')
source_type_hpms = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'source_type_HPMS')
RMAR_factor = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'RMAR')

hpms_vmt_growth_rate = read_csv(os.path.join(path_to_moves, 'turnover', 'VMT_growth_rate.csv'))
pop_turnover_rate = \
read_csv(os.path.join(path_to_moves, 'turnover', 'pop_growth_and_turnover_rate.csv'))

age_distribution_forecast = read_csv(os.path.join(path_to_moves, 'turnover', 
                                                  'age_distribution_moves_baseline.csv'))
age_distribution_forecast = pd.merge(age_distribution_forecast, source_type_hpms,
                                     on = 'sourceTypeID', how = 'left')
print(age_distribution_forecast.columns)
print(age_distribution_forecast.ageFraction.sum())
baseline_year = 2021
selected_type = [32, 52, 53, 61, 62]

# <codecell>

# generate VMT distribution by forecast year
hpms_vmt = hpms_vmt_growth_rate[['HPMSVtypeID', 'yearID', 'HPMSBaseYearVMT']]
fleet_mix_by_hpms = pd.merge(age_distribution_forecast, hpms_vmt,
                              on = ['HPMSVtypeID', 'yearID'], how = 'left')

fleet_mix_by_hpms = pd.merge(fleet_mix_by_hpms, hpms_definition,
                              on = 'HPMSVtypeID', how = 'left')

RMAR_factor = RMAR_factor[['sourceTypeID', 'ageID', 'relativeMAR']]

fleet_mix_by_hpms = pd.merge(fleet_mix_by_hpms, RMAR_factor,
                              on = ['sourceTypeID', 'ageID'], 
                              how = 'left') 

# <codecell>
print('Total VMT before assignment:')
print(hpms_vmt['HPMSBaseYearVMT'].sum())
fleet_mix_by_hpms.loc[:, 'weighted_vmt_rate'] =  \
    fleet_mix_by_hpms.loc[:, 'population_by_year'] * \
        fleet_mix_by_hpms.loc[:, 'relativeMAR']
        
fleet_mix_by_hpms.loc[:, 'weighted_vmt_by_hpms'] =  \
    fleet_mix_by_hpms.loc[:, 'weighted_vmt_rate'] / \
        fleet_mix_by_hpms.groupby(['HPMSVtypeID', 'yearID'])['weighted_vmt_rate'].transform('sum')
        
fleet_mix_by_hpms.loc[:, 'weighted_vmt_by_hpms'] = \
    fleet_mix_by_hpms.loc[:, 'weighted_vmt_by_hpms']* \
        fleet_mix_by_hpms.loc[:, 'HPMSBaseYearVMT'] 

# calculate VMT fraction
# fleet_mix_by_hpms.loc[:, 'vmt_fraction_by_hpms'] =  \
#     fleet_mix_by_hpms.loc[:, 'weighted_vmt_by_hpms'] / \
#         fleet_mix_by_hpms.groupby(['HPMSVtypeID', 'yearID'])['weighted_vmt_by_hpms'].transform('sum')
        
fleet_mix_by_hpms.loc[:, 'vmt_fraction'] =  \
    fleet_mix_by_hpms.loc[:, 'weighted_vmt_by_hpms'] / \
        fleet_mix_by_hpms.groupby(['yearID'])['weighted_vmt_by_hpms'].transform('sum')
        
print(fleet_mix_by_hpms.loc[:, 'vmt_fraction'].sum())

fleet_mix_com_only = \
    fleet_mix_by_hpms.loc[fleet_mix_by_hpms['sourceTypeID'].isin(selected_type)]
    
fleet_mix_com_only.loc[:, 'vmt_fraction'] =  \
    fleet_mix_com_only.loc[:, 'weighted_vmt_by_hpms'] / \
        fleet_mix_com_only.groupby(['yearID'])['weighted_vmt_by_hpms'].transform('sum')

print('Total VMT after allocation:')
print(fleet_mix_com_only['weighted_vmt_by_hpms'].sum())   
        
fleet_mix_com_only.to_csv(os.path.join(path_to_moves, 'turnover', 'vmt_fraction_moves_baseline.csv'))         
     
moves_vmt_by_st = \
    fleet_mix_by_hpms.groupby(['yearID','HPMSVtypeID','HPMSVtypeName', 'sourceTypeID','sourceTypeName'])['weighted_vmt_by_hpms'].sum()
moves_vmt_by_st = moves_vmt_by_st.reset_index()
moves_vmt_by_st = moves_vmt_by_st.rename(columns = {'weighted_vmt_by_hpms':'annualVMT'})
moves_vmt_by_st.to_csv(os.path.join(path_to_moves, 'turnover', 'moves_vmt_forecast_mandate.csv')) 

# <codecell>

# plot LDV+LDT VMT projection
LDT_VMT_by_year = \
    fleet_mix_by_hpms.loc[fleet_mix_by_hpms['sourceTypeID'].isin([21, 31, 32])]
    
LDT_VMT_by_year = \
    LDT_VMT_by_year.groupby(['yearID', 'sourceTypeID', 'sourceTypeName'])[['weighted_vmt_by_hpms']].sum()
LDT_VMT_by_year = LDT_VMT_by_year.reset_index()

# plot VMT of LDV+LDTs over year
sns.lineplot(LDT_VMT_by_year, x = 'yearID', y = 'weighted_vmt_by_hpms', hue = 'sourceTypeName')
plt.show()
# <codecell>

moves_vmt_by_st = moves_vmt_by_st.sort_values(by = 'yearID', ascending = True)
moves_vmt_by_st.loc[:, 'PreYearVMT'] = \
    moves_vmt_by_st.groupby('sourceTypeID')['annualVMT'].shift(1)
moves_vmt_by_st.loc[:, 'VMTGrowthFactor'] = moves_vmt_by_st.loc[:, 'annualVMT']/ \
    moves_vmt_by_st.loc[:, 'PreYearVMT']
moves_vmt_by_st.loc[:, 'VMTGrowthFactor'].fillna(1, inplace = True)

moves_vmt_by_st.loc[:, 'Cumulative VMT growth rate'] = \
    moves_vmt_by_st.groupby('sourceTypeID')['VMTGrowthFactor'].cumprod()


vmt_rate_to_plot = \
    moves_vmt_by_st.loc[moves_vmt_by_st['sourceTypeID'].isin([21, 31, 32])]
ax = sns.lineplot(data=vmt_rate_to_plot, x="yearID", y="Cumulative VMT growth rate", 
             hue="sourceTypeName")

hpms_rate_to_plot = hpms_vmt_growth_rate.loc[hpms_vmt_growth_rate['HPMSVtypeID'] == 25]
sns.lineplot(data=hpms_rate_to_plot, x="yearID", y="Cumulative VMT growth rate", 
             hue="HPMSVtypeName", ax = ax, palette= 'Reds', linestyle='dashed')

plt.legend(fontsize = 12)
plt.ylim([1, 2.5])
plt.savefig(os.path.join(plot_dir, 'LDV_growth_factor_moves_mandate.png'), dpi = 300,
            bbox_inches = 'tight')
plt.show()


# <codecell>

# plot commercial vehicle growth factor based on MOVES

com_vmt_rate = \
    moves_vmt_by_st.loc[moves_vmt_by_st['sourceTypeID'].isin(selected_type)]
com_vmt_rate = com_vmt_rate.sort_values(by = 'sourceTypeID')    
ax = sns.lineplot(data=com_vmt_rate, x="yearID", y="Cumulative VMT growth rate", 
             hue = "sourceTypeName", style = "sourceTypeName")
plt.legend(fontsize = 12)
plt.ylim([1, 2.5])
plt.savefig(os.path.join(plot_dir, 'com_growth_factor_moves_mandate.png'), dpi = 300,
            bbox_inches = 'tight')
plt.show()

# <codecell>

# only run this if age distribution come from baseline
mhd_rate = \
    hpms_vmt_growth_rate.loc[hpms_vmt_growth_rate['HPMSVtypeID'].isin([50, 60])]
ldt_rate = moves_vmt_by_st.loc[moves_vmt_by_st['sourceTypeID'] == 32]
ldt_rate.rename(columns = {'annualVMT': 'HPMSBaseYearVMT'}, inplace = True)

output_attr = ['yearID','HPMSVtypeID','HPMSVtypeName', 
               'HPMSBaseYearVMT','VMTGrowthFactor', 'Cumulative VMT growth rate']
mhd_rate = mhd_rate[output_attr]
ldt_rate = ldt_rate[output_attr]
vmt_growth_rate_for_vius = pd.concat([mhd_rate, ldt_rate])
vmt_growth_rate_for_vius.to_csv(os.path.join(path_to_moves, 'turnover', 
                                             'vius_vmt_forecast_factor.csv')) 

vmt_growth_rate_for_vius = vmt_growth_rate_for_vius.sort_values(by = 'HPMSVtypeID')
ax = sns.lineplot(data=vmt_growth_rate_for_vius, x="yearID", y="HPMSBaseYearVMT", 
             hue = "HPMSVtypeName", style = "HPMSVtypeName")
plt.legend(fontsize = 12)
plt.savefig(os.path.join(plot_dir, 'com_VMT_growth_vius.png'), dpi = 300,
            bbox_inches = 'tight')
plt.show()


  

