#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 30 11:48:14 2024

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

hpms_definition = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'HPMS_definition')
source_type_hpms = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'source_type_HPMS')
source_type_population = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'source_type_population')

hpms_vmt = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'HPMS_VMT')
age_distribution = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'AGE_distribution')
fuel_type_distribution = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'fuel_type_distribution')
fuel_type_definition = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'fuel_type_definition')
RMAR_factor = pd.read_excel(os.path.join(path_to_moves, 'moves_definition.xlsx'), 
                                sheet_name = 'RMAR')

# <codecell>

# Calculate VMT and population growth factor
baseline_year = 2021
hpms_vmt = hpms_vmt.loc[hpms_vmt['yearID'] >= baseline_year]
hpms_vmt = hpms_vmt.sort_values(by = 'yearID', ascending = True)
hpms_vmt.loc[:, 'HPMSPreYearVMT'] = hpms_vmt.groupby('HPMSVtypeID')['HPMSBaseYearVMT'].shift(1)
hpms_vmt.loc[:, 'VMTGrowthFactor'] = hpms_vmt.loc[:, 'HPMSBaseYearVMT']/hpms_vmt.loc[:, 'HPMSPreYearVMT']
hpms_vmt.loc[:, 'VMTGrowthFactor'].fillna(1, inplace = True)

hpms_vmt.loc[:, 'Cumulative VMT growth rate'] = hpms_vmt.groupby('HPMSVtypeID')['VMTGrowthFactor'].cumprod()

hpms_vmt_with_def = pd.merge(hpms_vmt, hpms_definition,
                    on = 'HPMSVtypeID', how = 'left')
sns.lineplot(data=hpms_vmt_with_def, x="yearID", y="Cumulative VMT growth rate", 
             hue="HPMSVtypeName")
plt.show()

# <codecell>

source_type_population = \
    source_type_population.loc[source_type_population['yearID'] >= baseline_year]
    
source_type_population = \
    source_type_population.sort_values(by = 'yearID', ascending = True)
source_type_population.loc[:, 'sourceTypePopulationPre'] = \
    source_type_population.groupby('sourceTypeID')['sourceTypePopulation'].shift(1)
source_type_population.loc[:, 'PopGrowthFactor'] = \
    source_type_population.loc[:, 'sourceTypePopulation']/ \
        source_type_population.loc[:, 'sourceTypePopulationPre']
source_type_population.loc[:, 'PopGrowthFactor'].fillna(1, inplace = True)

source_type_population.loc[:, 'Cumulative growth rate'] = \
    source_type_population.groupby('sourceTypeID')['PopGrowthFactor'].cumprod()
    
selected_type = [21, 32, 52, 53, 61, 62]
source_type_population_with_def = pd.merge(source_type_population, source_type_hpms,
                    on = 'sourceTypeID', how = 'left')
source_type_population_with_def = \
    source_type_population_with_def.loc[source_type_population_with_def['sourceTypeID'].isin(selected_type)]
sns.lineplot(data=source_type_population_with_def, 
             x="yearID", y="Cumulative growth rate", 
             hue="sourceTypeName", style = 'sourceTypeName',
             markers=True)
plt.legend(bbox_to_anchor = (1.01, 1))
plt.show()


# <codecell>

# calculate vehicle new sale scrappage
source_type_population.loc[:, 'DeltaPop'] = \
    source_type_population.loc[:, 'sourceTypePopulationPre'] * (source_type_population.loc[:, 'PopGrowthFactor'] - 1)
source_type_population.loc[:, 'DeltaPop'].fillna(0, inplace = True)
new_sale_fraction = age_distribution.loc[age_distribution['ageID'] == 0]

source_type_population_with_sale = pd.merge(source_type_population,
                                            new_sale_fraction,
                                            on = ['yearID', 'sourceTypeID'],
                                            how = 'left')
source_type_population_with_sale.loc[:, 'NewSale'] = \
    source_type_population_with_sale.loc[:, 'sourceTypePopulation'] * \
        source_type_population_with_sale.loc[:, 'ageFraction']

source_type_population_with_sale.loc[:, 'ScrappedVeh'] = \
    source_type_population_with_sale.loc[:, 'NewSale'] - \
        source_type_population_with_sale.loc[:, 'DeltaPop']

source_type_population_with_sale = \
    source_type_population_with_sale.sort_values(by = 'yearID', ascending = True)
source_type_population_with_sale.loc[:, 'ScrappedVeh'] = \
    source_type_population_with_sale.groupby('sourceTypeID')['ScrappedVeh'].shift(-1)
source_type_population_with_sale.loc[:, 'ScrappedVeh'].fillna(0, inplace = True)

# <codecell>

# get baseline source type model year mix
source_type_population_baseyear = \
    source_type_population.loc[source_type_population['yearID'] == baseline_year]
source_type_population_baseyear = \
    source_type_population_baseyear[['sourceTypeID', 'sourceTypePopulation']]

age_distribution_baseyear = \
    age_distribution.loc[age_distribution['yearID'] == baseline_year]
age_distribution_baseyear.loc[:, 'modelYearID'] = \
    baseline_year - age_distribution_baseyear.loc[:, 'ageID'] 
    
survival_rate = RMAR_factor[['ageID', 'sourceTypeID', 'survivalRate']]
fleet_mix_baseyear = pd.merge(source_type_population_baseyear,
                              age_distribution_baseyear,
                              on = 'sourceTypeID', how = 'left')

# fleet_mix_baseyear = pd.merge(fleet_mix_baseyear,
#                               survival_rate,
#                               on = ['sourceTypeID', 'ageID'], how = 'left')

fleet_mix_baseyear.loc[:, 'population_by_year'] =  \
    fleet_mix_baseyear.loc[:, 'sourceTypePopulation'] * \
        fleet_mix_baseyear.loc[:, 'ageFraction']

fleet_total_baseyear = \
    source_type_population_with_sale.loc[source_type_population_with_sale['yearID'] == baseline_year]
    
print(fleet_total_baseyear[['sourceTypeID', 'sourceTypePopulation']])
print(fleet_mix_baseyear.groupby('sourceTypeID')['population_by_year'].sum())

# <codecell>

# generate stochastic survival rate
def add_noise_to_survival_curve(survival_rate, scaling_factor = 0.01):
    survival_rate_alt = survival_rate.copy()
    survival_rate_alt.loc[:, 'noise'] = \
        scaling_factor * np.random.normal(loc=0,scale=1,size=len(survival_rate_alt))
    adj_idx = (survival_rate_alt['ageID']>0)
    survival_rate_alt.loc[adj_idx, 'survivalRate'] = \
        survival_rate_alt.loc[adj_idx, 'survivalRate'] + survival_rate_alt.loc[adj_idx, 'noise']
    survival_rate_alt.loc[survival_rate_alt['survivalRate']>1, 'survivalRate'] = 1
    survival_rate_alt.loc[survival_rate_alt['survivalRate']<= 0 , 'survivalRate'] = 0.01
    survival_rate_alt.drop(columns = ['noise'], inplace = True)
    return(survival_rate_alt)

# <codecell>

# fleet projection


# fleet_mix_age_30_plus = fleet_mix_current_year.loc[fleet_mix_current_year['ageID'] == 30]



n_iter = 100
# survival_rate_mandate = survival_rate.copy()
# mandate_idx = (survival_rate_mandate['ageID'] >= 20) & \
#     (survival_rate_mandate['sourceTypeID'].isin([32, 52, 53, 61, 62]))
# survival_rate_mandate.loc[mandate_idx, 'survivalRate'] *= 0.5
# survival_rate_alt = add_noise_to_survival_curve(survival_rate_mandate, 0.05)
results_by_iteration = None
# annual_scrappage = None
for iteration in range(n_iter):
    print('running fleet projection for iteration ' + str(iteration))
    # prep input at the start of iteration
    survival_rate_alt = add_noise_to_survival_curve(survival_rate, 0.05)
    fleet_mix_current_year = fleet_mix_baseyear.drop(columns = ['modelYearID'])
    fleet_mix_by_year = fleet_mix_current_year # all year mix
    fleet_mix_next_year = None
    end_year = source_type_population.yearID.max()
    list_of_years = list(range(baseline_year, end_year))
    
    # fleet projection over years
    for year in list_of_years:
        next_year = year + 1
        print('generate age distribution for year=' + str(next_year))
        
        fleet_total_by_year = \
            source_type_population_with_sale.loc[source_type_population_with_sale['yearID'] == year]
        fleet_total_by_year = \
            fleet_total_by_year.sort_values('sourceTypeID', ascending = True)
        
        fleet_mix_next_year = fleet_mix_current_year.copy()
        # re-append survival rate for every year, because after normalization it will be gone
        # using randomized survival curve
        
        
        fleet_mix_next_year = pd.merge(fleet_mix_next_year,
                                      survival_rate_alt,
                                      on = ['sourceTypeID', 'ageID'], how = 'left')
                
        fleet_mix_next_year.loc[:, 'veh_to_scrap'] = \
            fleet_mix_next_year.loc[:, 'population_by_year'] * \
                (1 - fleet_mix_next_year.loc[:, 'survivalRate'])
        scrappage_goal = fleet_total_by_year[['sourceTypeID', 'ScrappedVeh']]
        scrappage_est = fleet_mix_next_year.groupby('sourceTypeID')['veh_to_scrap'].sum()
        scrappage_est = scrappage_est.reset_index()
        # print('scrappage vehicle compare before k-adjust (left - goal, right - estimate):')
        # print(scrappage_goal.ScrappedVeh.sum(), scrappage_est.veh_to_scrap.sum())
        
        # calculate and apply k-adj factor
        k_factor = pd.merge(scrappage_goal, scrappage_est, 
                            on = 'sourceTypeID', how = 'outer')
        k_factor.loc[:, 'k_factor'] = \
            k_factor.loc[:, 'ScrappedVeh'] / k_factor.loc[:, 'veh_to_scrap']
        k_factor = k_factor[['sourceTypeID',  'k_factor']]
        fleet_mix_next_year = pd.merge(fleet_mix_next_year,
                                       k_factor, on = 'sourceTypeID', how = 'left')
        fleet_mix_next_year.loc[:, 'veh_to_scrap'] = \
            fleet_mix_next_year.loc[:, 'veh_to_scrap'] * fleet_mix_next_year.loc[:, 'k_factor']
    
        scrappage_est_2 = fleet_mix_next_year.groupby('sourceTypeID')['veh_to_scrap'].sum()
        scrappage_est_2 = scrappage_est_2.reset_index()
        # print('scrappage vehicle compare after k-adjust (left - goal, right - estimate):')
        # print(scrappage_goal.ScrappedVeh.sum(), scrappage_est_2.veh_to_scrap.sum())   
        
        # generate population after scrappage
        fleet_mix_next_year.loc[:, 'population_by_year'] = \
            fleet_mix_next_year.loc[:, 'population_by_year'] - fleet_mix_next_year.loc[:, 'veh_to_scrap']
        fleet_mix_next_year.loc[:, 'yearID'] = next_year
        # scrap_to_check = fleet_mix_next_year[['yearID', 'sourceTypeID', 'ageID', 
        #                                       'population_by_year', 'veh_to_scrap', 'k_factor']]
        # annual_scrappage = pd.concat([annual_scrappage, scrap_to_check])
        fleet_mix_next_year.drop(columns = ['veh_to_scrap', 'k_factor'], inplace = True)
                
        # increasing age id by 1
        fleet_mix_next_year.loc[:, 'ageID'] += 1

        # append new sale
        next_year_new_sale = \
            source_type_population_with_sale.loc[source_type_population_with_sale['yearID'] == next_year]
        next_year_new_sale = \
        next_year_new_sale[['sourceTypeID', 'sourceTypePopulation', 'yearID', 'ageID',
                            'ageFraction', 'NewSale']]   
        next_year_new_sale.loc[:, 'survivalRate'] = 1
    
        next_year_new_sale.rename(columns = {'NewSale': 'population_by_year'}, inplace = True)
        fleet_mix_next_year = pd.concat([fleet_mix_next_year, next_year_new_sale])
        
        fleet_mix_next_year.loc[:, 'ageFraction'] = \
            fleet_mix_next_year.loc[:, 'population_by_year']/ \
            fleet_mix_next_year.groupby('sourceTypeID')['population_by_year'].transform('sum')
    
        # fleet_mix_next_year.loc[fleet_mix_next_year['ageID'] > 30, 'ageFraction'] = \
        #     fleet_mix_next_year.loc[fleet_mix_next_year['ageID'] > 30, 'ageFraction'].shift(1)
        fleet_mix_next_year.loc[fleet_mix_next_year['ageID'] > 30, 'ageID'] = 30
    
    
        # renormalize age distribution
        agg_var = ['sourceTypeID', 'yearID', 'ageID']
        fleet_mix_next_year = fleet_mix_next_year.groupby(agg_var)[['population_by_year']].sum()
        
        fleet_mix_next_year =fleet_mix_next_year.reset_index()
        
        
        fleet_mix_next_year.loc[:, 'sourceTypePopulation'] = \
            fleet_mix_next_year.groupby('sourceTypeID')['population_by_year'].transform('sum')
    
        fleet_mix_next_year.loc[:, 'ageFraction'] = \
            fleet_mix_next_year.loc[:, 'population_by_year'] / fleet_mix_next_year.loc[:, 'sourceTypePopulation']
        
        # fix the tail
        # source_type_next_year = \
        #     fleet_mix_next_year.groupby(['sourceTypeID', 'yearID'])[['population_by_year']].sum()
        # source_type_next_year = source_type_next_year.reset_index()
        # source_type_next_year.rename(columns = {'population_by_year': 'sourceTypePopulation'}, 
        #                               inplace = True)
        # age_distribution_next_year = \
        #     fleet_mix_next_year[['sourceTypeID', 'ageID', 'ageFraction']]
        # age_distribution_next_year = \
        #     age_distribution_next_year.loc[age_distribution_next_year['ageID'] <= 29]
        # age_distribution_tail = fleet_mix_age_30_plus[['sourceTypeID', 'ageID',
        #         'ageFraction']]
        # age_distribution_next_year = pd.concat([age_distribution_next_year,
        #                                         age_distribution_tail])
        
        # # re-normalize the age distribution
        # age_distribution_next_year.loc[:, 'adj_bin'] = 'yes'
        # age_distribution_next_year.loc[age_distribution_next_year['ageID'].isin([0, 30]), 'adj_bin'] = 'no'
        # tail_adj_factor = pd.pivot_table(age_distribution_next_year,
        #                             columns = 'adj_bin',
        #                             index = 'sourceTypeID', 
        #                             values = 'ageFraction', aggfunc='sum')
        # tail_adj_factor = tail_adj_factor.reset_index()
        
        # tail_adj_factor.loc[:, 'adj_factor'] = \
        #     (1- tail_adj_factor.loc[:, 'no'])/ \
        #         tail_adj_factor.loc[:, 'yes']
        # tail_adj_factor = tail_adj_factor[['sourceTypeID', 'adj_factor']]
        # age_distribution_next_year = pd.merge(age_distribution_next_year,
        #                                       tail_adj_factor, on = 'sourceTypeID',
        #                                       how = 'left')
        
        # adj_index = (age_distribution_next_year['adj_bin'] == 'yes')
        # age_distribution_next_year.loc[adj_index, 'ageFraction'] *= \
        #     age_distribution_next_year.loc[adj_index, 'adj_factor']
        # # print(age_distribution_next_year.groupby('sourceTypeID')['ageFraction'].sum())
        # age_distribution_next_year.drop(columns = ['adj_bin', 'adj_factor'], inplace = True)
        
        # fleet_mix_next_year = pd.merge(source_type_next_year,
        #                                 age_distribution_next_year,
        #                                 on = 'sourceTypeID', how = 'left')
        # fleet_mix_next_year.loc[:, 'population_by_year'] = \
        #     fleet_mix_next_year.loc[:, 'ageFraction'] * fleet_mix_next_year.loc[:, 'sourceTypePopulation']
        
        
        
        # prepare for next iteration
        fleet_mix_by_year = pd.concat([fleet_mix_by_year, fleet_mix_next_year])
        fleet_mix_current_year = fleet_mix_next_year.copy()
        # break
    
    # at the end of each iteration, write in iteration id and append it to output
    fleet_mix_by_year.loc[:, 'iteration'] = iteration
    results_by_iteration = pd.concat([results_by_iteration, fleet_mix_by_year])
    # break
file_name = 'age_distribution_' + str(n_iter) + '_iterations.csv'
results_by_iteration.to_csv(os.path.join(path_to_moves, 'turnover',file_name),
                          index = False)

# <codecell>
sns.set_theme(style="whitegrid", font_scale=1.4)
# plot selected age distribution
year_list = [2021, 2030, 2040, 2050]
fleet_mix_to_plot = results_by_iteration.loc[results_by_iteration['sourceTypeID'].isin(selected_type)]
fleet_mix_to_plot = fleet_mix_to_plot.loc[fleet_mix_to_plot['yearID'].isin(year_list)]
sns.relplot(data = fleet_mix_to_plot, x= 'ageID', y = 'ageFraction',
            hue = 'yearID', col='sourceTypeID', col_wrap = 3, kind = 'line')
plt.savefig(os.path.join(path_to_moves, 'plot_forecast', 'com_age_distribution_stochastic_100run.png'), dpi = 300,
            bbox_inches = 'tight')
plt.show()

# fleet_mix_to_plot_2 = results_by_iteration.loc[results_by_iteration['sourceTypeID'] == 21]
# fleet_mix_to_plot_2 = fleet_mix_to_plot_2.loc[fleet_mix_to_plot_2['yearID'] == 2060]
# sns.lineplot(data = fleet_mix_to_plot_2, x= 'ageID', y = 'ageFraction',
#            errorbar=('ci', 95))
# plt.savefig(os.path.join(path_to_moves, 'plot_forecast', 'pax_age_distribution_stochastic_100run.png'), dpi = 300,
#             bbox_inches = 'tight')
# plt.show()

# <codecell>
# plot scrappage curve
scrappage_to_plot = source_type_population_with_sale[source_type_population_with_sale['yearID']<= 2059]
scrappage_to_plot = scrappage_to_plot[scrappage_to_plot['sourceTypeID'].isin([32, 52, 53, 61, 62])]
scrappage_to_plot.loc[:, 'sourceTypeID'] = scrappage_to_plot.loc[:, 'sourceTypeID'].astype(str)
sns.relplot(data = scrappage_to_plot, x= 'yearID', y = 'ScrappedVeh',
            hue = 'sourceTypeID', kind = 'line')
plt.savefig(os.path.join(path_to_moves, 'plot_forecast', 'scrappage_vehicle_curve.png'), dpi = 300,
            bbox_inches = 'tight')
plt.show()