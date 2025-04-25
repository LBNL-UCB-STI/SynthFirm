#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 30 09:08:03 2024

@author: xiaodanxu
"""

import pandas as pd
from pandas import read_csv
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os

import warnings
warnings.filterwarnings("ignore")

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')
path_to_vius = 'RawData/US_VIUS_2021'
path_to_moves = 'RawData/MOVES'
analysis_year = 2021

# conversions
veh_st_map = {'combination long-haul truck': 62,
              'combination short-haul truck': 61,
              'single-unit short-haul truck': 52,
              'single-unit long-haul truck': 53,
              'light commercial truck':  32}

MOVES_fuel_lookup = {'Gasoline': 1, 
                    'Diesel': 2,
                    'CNG': 3,
                    'Other':5,
                    'Electricity': 9}

VIUS_fuel_lookup = {'Gasoline': 'Gasoline', 
                    'Diesel': 'Diesel',
                    'Compressed natural gas': 'CNG',
                    'Propane': 'Other',
                    'Combination': 'Other',
                    'Liquified natural gas': 'Other',
                    'Alcohol fuels': 'Other',
                    'Electricity': 'Electricity'}

regclass_lookup = {30:'LDT', 41:'LHD2b3', 
                   42:'LHD45', 46:'MHD67',
                   47:'HHD8 (w. glider)', 
                   49:'HHD8 (w. glider)'}

age_bin = [-1, 3, 5, 7, 9, 14, 19, 31]

age_bin_label = ['age<=3', '3<age<=5','5<age<=7', 
                 '7<age<=9', '9<age<=14', '14<age<=19', 'age>=20']

age_bin_order = {'age<=3':1, '3<age<=5':2, '5<age<=7':3, 
                 '7<age<=9':4, '9<age<=14':5, 
                 '14<age<=19':6, 'age>=20':7}

# load vius data
vius_data_path = 'vius_2021_com_crosswalk_20240624.csv'
vius_fleet = read_csv(os.path.join(path_to_vius, vius_data_path))
vius_fleet.head(5)


age_distribution = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'AGE_distribution')

moves_vmt_distribution = read_csv(os.path.join(path_to_moves, 
                                               'MOVES_VMT_fraction_com_only.csv'))

# weighted VMT
vius_fleet.loc[:, 'WGT_VMT'] = \
vius_fleet.loc[:, 'TABWEIGHT'] * vius_fleet.loc[:, 'MILESANNL']

# vehicle age
vius_fleet.loc[:, 'VEH_AGE'] = \
analysis_year - vius_fleet.loc[:, 'MODELYEAR'] # model year 1999 -> including pre-1999

# assign MOVES sourceTypeID
vius_fleet.loc[:, 'VEH_CLASS_MOVES'] = vius_fleet.loc[:, 'VEH_CLASS_MOVES'].str.lower()
vius_fleet.loc[:, 'sourceTypeID'] = \
    vius_fleet.loc[:, 'VEH_CLASS_MOVES'].map(veh_st_map)

# assign regular class ID    
vius_fleet.loc[:, 'regClassID'] = 0
# LDT
vius_fleet.loc[vius_fleet['GVWR_CLASS'].isin(['1', '2A']), 'regClassID'] = 30
# LHD2B3
vius_fleet.loc[vius_fleet['GVWR_CLASS'].isin(['2B', '3']), 'regClassID'] = 41
# LHD45
vius_fleet.loc[vius_fleet['GVWR_CLASS'].isin(['4', '5']), 'regClassID'] = 42
# MHD67
vius_fleet.loc[vius_fleet['GVWR_CLASS'].isin(['6', '7']), 'regClassID'] = 46
# HHD8 (including glider)
vius_fleet.loc[vius_fleet['GVWR_CLASS'].isin(['8']), 'regClassID'] = 47
    
# <codecell>
sample_size_by_age = vius_fleet.groupby(['VEH_CLASS_MOVES', 'VEH_AGE']).size()

#calculate vehicle population
vius_count_by_st = \
vius_fleet.groupby('sourceTypeID')[['TABWEIGHT']].sum()
vius_count_by_st = vius_count_by_st.reset_index()

vius_count_by_st.columns = ['sourceTypeID', 'sourceTypePopulation']
vius_count_by_st.loc[:, 'yearID'] = analysis_year
print('total commercial truck population is:')
print(vius_count_by_st['sourceTypePopulation'].sum()) # 28.7 million

# calculate age distribution by source type
pop_and_vmt_by_age = \
    vius_fleet.groupby(['sourceTypeID', 'VEH_AGE'])[['TABWEIGHT', 'WGT_VMT']].sum()
    
pop_and_vmt_by_age = pop_and_vmt_by_age.reset_index()
print('truck count in age distribution is:')
print(pop_and_vmt_by_age['TABWEIGHT'].sum()) # 28.7 million

print('truck VMT in age distribution is:')
print(pop_and_vmt_by_age['WGT_VMT'].sum()) # 28.7 million

pop_and_vmt_by_age.loc[:, 'VMT_per_veh'] = \
    pop_and_vmt_by_age.loc[:, 'WGT_VMT'] / pop_and_vmt_by_age.loc[:, 'TABWEIGHT']
    
sns.lineplot(pop_and_vmt_by_age, 
            x="VEH_AGE", y="VMT_per_veh", palette = 'Set2',
            hue="sourceTypeID", 
            errorbar = None)
plt.xticks(rotation = 30, ha= 'right')
plt.xlabel('Age bin')
plt.ylabel('Annual mileage per truck')
plt.legend(fontsize = 8, loc = 1)
plt.savefig('RawData/MOVES/plot/VIUS_VMT_per_truck_by_stmy.png',
            dpi = 300, bbox_inches = 'tight')
plt.show()

# <codecell>

# impute count and VMT for veh age > 23
age_distribution_by_year = \
    age_distribution.loc[age_distribution['yearID'] == analysis_year]

age_distribution_by_year.loc[:, 'VEH_AGE'] = \
    age_distribution_by_year.loc[:, 'ageID']

age_distribution_by_year.loc[age_distribution_by_year['VEH_AGE']>=23, 'VEH_AGE'] = 23

age_distribution_no_adj = \
    age_distribution_by_year.loc[age_distribution_by_year['VEH_AGE'] <= 22]
age_distribution_no_adj.loc[:, 'ageFraction'] = 1 # do not change VIUS count and VMT before age 22

# allocate count and VMT for vehicle age >= 23
age_distribution_to_adj = \
    age_distribution_by_year.loc[age_distribution_by_year['VEH_AGE'] > 22]

age_distribution_to_adj.loc[:, 'ageFraction'] = \
    age_distribution_to_adj.loc[:, 'ageFraction'] / \
        age_distribution_to_adj.groupby(['sourceTypeID'])['ageFraction'].transform('sum')

# check allocation factor
print('sum of allocation factor (should be 13):')
print(age_distribution_to_adj.loc[:, 'ageFraction'].sum())

ade_distribution_for_impute = pd.concat([age_distribution_no_adj, 
                                         age_distribution_to_adj])

moves_vmt_distribution = moves_vmt_distribution[['sourceTypeID', 'ageID', 'vmt_fraction']]
moves_vmt_distribution.loc[:, 'VEH_AGE'] = \
    moves_vmt_distribution.loc[:, 'ageID']

moves_vmt_distribution.loc[moves_vmt_distribution['VEH_AGE']>=23, 'VEH_AGE'] = 23

vmt_distribution_no_adj = \
    moves_vmt_distribution.loc[moves_vmt_distribution['VEH_AGE'] <= 22]
vmt_distribution_no_adj.loc[:, 'vmt_fraction'] = 1 # do not change VIUS count and VMT before age 22

# allocate count and VMT for vehicle age >= 23
vmt_distribution_to_adj = \
    moves_vmt_distribution.loc[moves_vmt_distribution['VEH_AGE'] > 22]

vmt_distribution_to_adj.loc[:, 'vmt_fraction'] = \
    vmt_distribution_to_adj.loc[:, 'vmt_fraction'] / \
        vmt_distribution_to_adj.groupby(['sourceTypeID'])['vmt_fraction'].transform('sum')

print('sum of VMT allocation factor (should be 5):')
print(vmt_distribution_to_adj.loc[:, 'vmt_fraction'].sum())
vmt_distribution_for_impute = pd.concat([vmt_distribution_no_adj, 
                                         vmt_distribution_to_adj])

vmt_and_age_to_impute = pd.merge(ade_distribution_for_impute,
                                 vmt_distribution_for_impute,
                                 on = ['sourceTypeID', 'ageID', 'VEH_AGE'],
                                 how = 'inner')
# <codecell>
# combine imputation factor with VIUS data
print('total count and VMT before allocation:')
print(pop_and_vmt_by_age[['TABWEIGHT', 'WGT_VMT']].sum())
pop_and_vmt_by_age_imputed = pd.merge(pop_and_vmt_by_age,
                                      vmt_and_age_to_impute,
                                      on = ['sourceTypeID', 'VEH_AGE'], 
                                      how = 'left')
pop_and_vmt_by_age_imputed.loc[:, 'TABWEIGHT'] = \
    pop_and_vmt_by_age_imputed.loc[:, 'TABWEIGHT'] * \
        pop_and_vmt_by_age_imputed.loc[:, 'ageFraction']
        
pop_and_vmt_by_age_imputed.loc[:, 'WGT_VMT'] = \
    pop_and_vmt_by_age_imputed.loc[:, 'WGT_VMT'] * \
        pop_and_vmt_by_age_imputed.loc[:, 'vmt_fraction']  
        
print('total count and VMT after allocation:')        
print(pop_and_vmt_by_age_imputed[['TABWEIGHT', 'WGT_VMT']].sum())

pop_and_vmt_by_age_imputed.loc[:, 'VMT_per_veh'] = \
    pop_and_vmt_by_age_imputed.loc[:, 'WGT_VMT'] / \
        pop_and_vmt_by_age_imputed.loc[:, 'TABWEIGHT']
    
sns.lineplot(pop_and_vmt_by_age_imputed, 
            x="ageID", y="VMT_per_veh", palette = 'Set2',
            hue="sourceTypeID", 
            errorbar = None)
plt.xticks(rotation = 30, ha= 'right')
plt.xlabel('Age bin')
plt.ylabel('Annual mileage per truck')
plt.legend(fontsize = 8, loc = 1)
plt.savefig('RawData/MOVES/plot/VIUS_VMT_per_truck_by_stmy_imputed.png',
            dpi = 300, bbox_inches = 'tight')
plt.show()


# <codecell>
# generate age distribution and RMAR from VIUS
vius_age_distribution = \
pop_and_vmt_by_age_imputed[['sourceTypeID', 'TABWEIGHT', 'WGT_VMT', 
                            'VMT_per_veh','yearID', 'ageID']]

vius_age_distribution.loc[:, 'ageFraction'] = \
    vius_age_distribution.loc[:, 'TABWEIGHT'] / \
        vius_age_distribution.groupby(['sourceTypeID'])['TABWEIGHT'].transform('sum')
        
vius_age_distribution.loc[:, 'VMT_Fraction'] = \
    vius_age_distribution.loc[:, 'WGT_VMT'] / \
        vius_age_distribution.groupby(['sourceTypeID'])['WGT_VMT'].transform('sum')        

vius_age_distribution.to_csv(os.path.join(path_to_vius,
                                          'vius_stmy_composition.csv'),
                             index = False)

sns.lineplot(vius_age_distribution, 
             x="ageID", y="ageFraction", 
             hue="sourceTypeID", palette = 'Set2',
             errorbar = None)
# plt.xticks(rotation = 30, ha= 'right')
plt.xlabel('Age ID')
plt.ylabel('Fraction of vehicle population')
# plt.legend(fontsize = 8, loc = 1)
plt.savefig('RawData/MOVES/plot/VIUS_age_distribution.png',
             dpi = 300, bbox_inches = 'tight')   
plt.show()

sns.lineplot(vius_age_distribution, 
             x="ageID", y="VMT_Fraction", 
             hue="sourceTypeID", palette = 'Set2',
             errorbar = None)
# plt.xticks(rotation = 30, ha= 'right')
plt.xlabel('Age ID')
plt.ylabel('Fraction of vehicle VMT')
# plt.legend(fontsize = 8, loc = 1)
plt.savefig('RawData/MOVES/plot/VIUS_vmt_by_age_distribution.png',
             dpi = 300, bbox_inches = 'tight')   
plt.show()


# <codecell>

# avg. age per group
def weighted_average(df,data_col,weight_col,by_col):
    df['data_times_weight'] = df[data_col] * df[weight_col]
    df['weight_where_notnull'] = df[weight_col]*pd.notnull(df[data_col])
    g = df.groupby(by_col)
    mean = g['data_times_weight'].sum() / g['weight_where_notnull'].sum()

    return mean

def weighted_sd(input_df):
    weights = input_df['ageFraction']
    vals = input_df['ageID']

    weighted_avg = np.average(vals, weights=weights)
    
    numer = np.sum(weights * (vals - weighted_avg)**2)
    denom = ((vals.count()-1)/vals.count())*np.sum(weights)
    std = np.sqrt(numer/denom)
    return std

VIUS_avg_age_by_group = \
    weighted_average(vius_age_distribution, 'ageID', 'ageFraction', 'sourceTypeID')
    
VIUS_age_std_by_group = \
    vius_age_distribution.groupby('sourceTypeID').apply(weighted_sd)

age_distribution_curr_year = \
    age_distribution.loc[age_distribution['yearID'] == analysis_year]    
    
MOVES_avg_age_by_group = \
    weighted_average(age_distribution_curr_year, 'ageID', 'ageFraction', 'sourceTypeID')

MOVES_age_std_by_group = \
    age_distribution_curr_year.groupby('sourceTypeID').apply(weighted_sd)
  
# <codecell>

# calculate RMAR factor of VIUS data
vius_RMAR = vius_age_distribution[['sourceTypeID', 'ageID', 'VMT_per_veh']]   
vius_RMAR_denominator = vius_RMAR.loc[vius_RMAR['ageID'] == 0]
vius_RMAR_denominator.rename(columns = {'VMT_per_veh':'base_VMT'}, inplace = True)
vius_RMAR_denominator.drop(columns = 'ageID', inplace = True)
vius_RMAR = pd.merge(vius_RMAR, vius_RMAR_denominator, on = 'sourceTypeID', how = 'left')
vius_RMAR.loc[:, 'relativeMAR'] = vius_RMAR.loc[:, 'VMT_per_veh']/ \
    vius_RMAR.loc[:, 'base_VMT']

sns.lineplot(vius_RMAR, 
             x="ageID", y="relativeMAR", 
             hue="sourceTypeID", palette = 'Set2',
             errorbar = None)
# plt.xticks(rotation = 30, ha= 'right')
plt.xlabel('Age ID')
plt.ylabel('Relative mileage accumulation factor')
# plt.legend(fontsize = 8, loc = 1)
plt.savefig('RawData/MOVES/plot/VIUS_RMAR.png',
             dpi = 300, bbox_inches = 'tight')   
plt.show()    

vius_RMAR.to_csv(os.path.join(path_to_vius,'vius_rmar.csv'),
                             index = False)
# <codecell>

vius_fleet_with_fuel = \
vius_fleet.loc[vius_fleet['FUELTYPE'] != 'Not reported']
print(len(vius_fleet_with_fuel))
vius_fleet_with_fuel.loc[:, 'fuelTypeName'] = \
vius_fleet_with_fuel.loc[:, 'FUELTYPE'].map(VIUS_fuel_lookup)

vius_fleet_with_fuel.loc[:, 'fuelTypeID'] = \
vius_fleet_with_fuel.loc[:, 'fuelTypeName'].map(MOVES_fuel_lookup)
# generate fuel distribution
vius_fleet_with_fuel.loc[:, 'AgeBin'] = pd.cut(vius_fleet_with_fuel['VEH_AGE'],
                                               bins=age_bin, right=True, labels=age_bin_label)
VIUS_fuel_distribution = \
    vius_fleet_with_fuel.groupby(['sourceTypeID', 'AgeBin', 'fuelTypeID']).agg({'TABWEIGHT': ['count', 'sum']})  

VIUS_fuel_distribution.columns = ['sample_size', 'TABWEIGHT']    
VIUS_fuel_distribution = VIUS_fuel_distribution.reset_index()
print('total sample before filter')
print(VIUS_fuel_distribution['sample_size'].sum())
# drop unavailable fuel - source type combo in MOVES
criteria_1 = (VIUS_fuel_distribution['fuelTypeID'] != 5)
criteria_2 = (VIUS_fuel_distribution['fuelTypeID'] == 3) & (VIUS_fuel_distribution['sourceTypeID'] == 32)
criteria_3 = (VIUS_fuel_distribution['fuelTypeID'] == 1) & (VIUS_fuel_distribution['sourceTypeID'] == 62)
VIUS_fuel_distribution = VIUS_fuel_distribution.loc[criteria_1]
VIUS_fuel_distribution = VIUS_fuel_distribution.loc[~criteria_2]
VIUS_fuel_distribution = VIUS_fuel_distribution.loc[~criteria_3]
print('total sample after filter')
print(VIUS_fuel_distribution['sample_size'].sum())
VIUS_fuel_distribution.loc[:, 'fuel_fraction'] = \
    VIUS_fuel_distribution['TABWEIGHT'] / \
        VIUS_fuel_distribution.groupby(['sourceTypeID', 'AgeBin'])['TABWEIGHT'].transform('sum')

print('total fuel adj. factor (should be 35):') 
print(VIUS_fuel_distribution.loc[:, 'fuel_fraction'].sum())        

# <codecell>
vius_age_distribution.loc[:, 'AgeBin'] = pd.cut(vius_age_distribution['ageID'],
                                               bins=age_bin, right=True, labels=age_bin_label)
vius_age_distribution_rescaled = vius_age_distribution

vius_age_distribution_rescaled.loc[:, 'ageFraction'] = \
    vius_age_distribution_rescaled.loc[:, 'TABWEIGHT'] / \
        vius_age_distribution_rescaled.loc[:, 'TABWEIGHT'].sum()
        
vius_age_distribution_rescaled.loc[:, 'VMT_Fraction'] = \
    vius_age_distribution_rescaled.loc[:, 'WGT_VMT'] / \
        vius_age_distribution_rescaled.loc[:, 'WGT_VMT'].sum()    

print('total age fraction and vmt age fraction (both should be 0):')
print(vius_age_distribution_rescaled.loc[:, 'ageFraction'].sum())
print(vius_age_distribution_rescaled.loc[:, 'VMT_Fraction'].sum())

fuel_distribution_to_use = \
    VIUS_fuel_distribution[['sourceTypeID', 'AgeBin', 'fuelTypeID', 'fuel_fraction']]

vius_age_distribution_with_fuel = pd.merge(vius_age_distribution_rescaled,
                                           fuel_distribution_to_use, 
                                           on = ['sourceTypeID', 'AgeBin'],
                                           how = 'left')

vius_age_distribution_with_fuel.loc[:, 'ageFraction'] = \
    vius_age_distribution_with_fuel.loc[:, 'ageFraction'] * \
        vius_age_distribution_with_fuel.loc[:, 'fuel_fraction']
        
vius_age_distribution_with_fuel.loc[:, 'VMT_Fraction'] = \
    vius_age_distribution_with_fuel.loc[:, 'VMT_Fraction'] * \
        vius_age_distribution_with_fuel.loc[:, 'fuel_fraction']

vius_age_distribution_with_fuel.loc[:, 'TABWEIGHT'] = \
    vius_age_distribution_with_fuel.loc[:, 'TABWEIGHT'] * \
        vius_age_distribution_with_fuel.loc[:, 'fuel_fraction']
        
vius_age_distribution_with_fuel.loc[:, 'WGT_VMT'] = \
    vius_age_distribution_with_fuel.loc[:, 'WGT_VMT'] * \
        vius_age_distribution_with_fuel.loc[:, 'fuel_fraction']
        
print('total age X fuel fraction and vmt age fraction (both should be 0):')
print(vius_age_distribution_with_fuel.loc[:, 'ageFraction'].sum())
print(vius_age_distribution_with_fuel.loc[:, 'VMT_Fraction'].sum())

print('total population after assignment:')
print(vius_age_distribution_with_fuel.loc[:, 'TABWEIGHT'].sum())
print('total VMT after assignment:')
print(vius_age_distribution_with_fuel.loc[:, 'WGT_VMT'].sum())

# <codecell>
var_list = ['sourceTypeID', 'TABWEIGHT', 'WGT_VMT', 'VMT_per_veh', 'yearID',
       'ageID', 'ageFraction', 'VMT_Fraction', 'AgeBin', 'fuelTypeID', 'fuel_fraction']
vius_age_distribution_with_fuel = vius_age_distribution_with_fuel[var_list]
vius_age_distribution_with_fuel.to_csv(os.path.join(path_to_vius,
                                                    'VIUS_VMT_fraction_with_fuel_com_only.csv'), index = False)