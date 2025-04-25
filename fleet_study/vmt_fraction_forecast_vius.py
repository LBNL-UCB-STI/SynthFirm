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
RMAR_factor = read_csv(os.path.join(path_to_vius, 'vius_rmar.csv'))

vmt_growth_rate_for_vius = read_csv(os.path.join(path_to_moves, 'turnover', 
                                             'vius_vmt_forecast_factor.csv')) 

hpms_vmt_from_vius = read_csv(os.path.join(path_to_vius, 'vius_stmy_composition.csv'))
hpms_vmt_from_vius = pd.merge(hpms_vmt_from_vius, source_type_hpms,
                                     on = 'sourceTypeID', how = 'left')
hpms_vmt_from_vius = pd.merge(hpms_vmt_from_vius, hpms_definition,
                                     on = 'HPMSVtypeID', how = 'left')

age_distribution_forecast = read_csv(os.path.join(path_to_moves, 'turnover', 
                                                  'age_distribution_vius_baseline.csv'))
age_distribution_forecast = pd.merge(age_distribution_forecast, source_type_hpms,
                                     on = 'sourceTypeID', how = 'left')
age_distribution_forecast = pd.merge(age_distribution_forecast, hpms_definition,
                                     on = 'HPMSVtypeID', how = 'left')
print(age_distribution_forecast.columns)
baseline_year = 2021
selected_type = [32, 52, 53, 61, 62]

# <codecell>

# generate forecasted VMT for VIUS
vmt_rate_sel = vmt_growth_rate_for_vius[['yearID', 'HPMSVtypeID', 'HPMSVtypeName', 
                                         'Cumulative VMT growth rate']]
vius_hpms_vmt = \
    hpms_vmt_from_vius.groupby(['HPMSVtypeID', 'HPMSVtypeName'])[['WGT_VMT']].sum()
vius_hpms_vmt = vius_hpms_vmt.reset_index()   

vius_hpms_vmt.rename(columns = {'WGT_VMT':'HPMSBaseYearVMT'}, 
                              inplace = True)
print('total base year VMT:')
print(vius_hpms_vmt.HPMSBaseYearVMT.sum())
vius_hpms_vmt_forecast = pd.merge(vius_hpms_vmt,
                                  vmt_rate_sel,
                                  on = 'HPMSVtypeID', how = 'left')

vius_hpms_vmt_forecast.loc[:, 'HPMSBaseYearVMT'] *= \
    vius_hpms_vmt_forecast.loc[:, 'Cumulative VMT growth rate'] 
    
# <codecell>

# generate fleet mix for VIUS data
fleet_mix_by_hpms = pd.merge(age_distribution_forecast, vius_hpms_vmt_forecast,
                              on = ['HPMSVtypeID', 'yearID'], how = 'left')

RMAR_factor = RMAR_factor[['sourceTypeID', 'ageID', 'relativeMAR']]

fleet_mix_by_hpms = pd.merge(fleet_mix_by_hpms, RMAR_factor,
                              on = ['sourceTypeID', 'ageID'], 
                              how = 'left') 

print('Total VMT before assignment:')
print(vius_hpms_vmt_forecast['HPMSBaseYearVMT'].sum())

fleet_mix_by_hpms.loc[:, 'weighted_vmt_rate'] =  \
    fleet_mix_by_hpms.loc[:, 'population_by_year'] * \
        fleet_mix_by_hpms.loc[:, 'relativeMAR']
        
fleet_mix_by_hpms.loc[:, 'weighted_vmt_by_hpms'] =  \
    fleet_mix_by_hpms.loc[:, 'weighted_vmt_rate'] / \
        fleet_mix_by_hpms.groupby(['HPMSVtypeID', 'yearID'])['weighted_vmt_rate'].transform('sum')
        
fleet_mix_by_hpms.loc[:, 'weighted_vmt_by_hpms'] = \
    fleet_mix_by_hpms.loc[:, 'weighted_vmt_by_hpms']* \
        fleet_mix_by_hpms.loc[:, 'HPMSBaseYearVMT'] 


fleet_mix_by_hpms.loc[:, 'vmt_fraction'] =  \
    fleet_mix_by_hpms.loc[:, 'weighted_vmt_by_hpms'] / \
        fleet_mix_by_hpms.groupby(['yearID'])['weighted_vmt_by_hpms'].transform('sum')
fleet_mix_by_hpms.to_csv(os.path.join(path_to_moves, 'turnover', 'vmt_fraction_vius_mandate.csv'))         

print('Total VMT after allocation:')
print(fleet_mix_by_hpms['weighted_vmt_by_hpms'].sum())    
    
vius_vmt_by_st = \
    fleet_mix_by_hpms.groupby(['yearID','HPMSVtypeID','HPMSVtypeName', 'sourceTypeID','sourceTypeName'])['weighted_vmt_by_hpms'].sum()
vius_vmt_by_st = vius_vmt_by_st.reset_index()
vius_vmt_by_st = vius_vmt_by_st.rename(columns = {'weighted_vmt_by_hpms':'annualVMT'})
vius_vmt_by_st.to_csv(os.path.join(path_to_moves, 'turnover', 'vius_vmt_forecast_mandate.csv'))


# <codecell>
sns.set(font_scale=1.4)  # larger font  
sns.set_style("whitegrid")
# calculate VMT growth factor from VIUS
vius_vmt_by_st = vius_vmt_by_st.sort_values(by = 'yearID', ascending = True)
vius_vmt_by_st.loc[:, 'PreYearVMT'] = \
    vius_vmt_by_st.groupby('sourceTypeID')['annualVMT'].shift(1)
vius_vmt_by_st.loc[:, 'VMTGrowthFactor'] = vius_vmt_by_st.loc[:, 'annualVMT']/ \
    vius_vmt_by_st.loc[:, 'PreYearVMT']
vius_vmt_by_st.loc[:, 'VMTGrowthFactor'].fillna(1, inplace = True)

vius_vmt_by_st.loc[:, 'Cumulative VMT growth rate'] = \
    vius_vmt_by_st.groupby('sourceTypeID')['VMTGrowthFactor'].cumprod()
    
com_vmt_rate = \
    vius_vmt_by_st.loc[vius_vmt_by_st['sourceTypeID'].isin(selected_type)]
year_to_plot = [2021, 2030, 2040, 2050]
com_vmt_rate = \
        com_vmt_rate.loc[com_vmt_rate['yearID'].isin(year_to_plot)]
com_vmt_rate = com_vmt_rate.sort_values(by = 'sourceTypeID')    
ax = sns.lineplot(data=com_vmt_rate, x="yearID", y="Cumulative VMT growth rate", 
             hue = "sourceTypeName", style = "sourceTypeName")
plt.legend(fontsize = 12)
plt.ylim([1, 2])
plt.savefig(os.path.join(plot_dir, 'com_growth_factor_vius_baseline.png'), dpi = 300,
            bbox_inches = 'tight')
plt.show()