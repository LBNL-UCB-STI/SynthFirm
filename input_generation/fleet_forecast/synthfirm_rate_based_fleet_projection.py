#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  3 15:58:07 2025

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
path_to_experian = 'PrivateData/registration/'

baseline_year = 2021
# max_age = 40

pop_turnover_rate = \
read_csv(os.path.join(path_to_moves, 'turnover', 'SynthFirm_pop_growth_and_turnover_rate.csv'))

survival_rate = read_csv(os.path.join(path_to_moves, 'MOVES5_RMAR_and_Survival_rate.csv'))

fleet_mix_baseyear = read_csv(os.path.join(path_to_experian, 'experian_national_age_distribution.csv'))

# long_haul_by_state = read_csv(os.path.join(path_to_vius, 'VIUS_count_by_hauling.csv'))

state_fips_file = 'SynthFirm_parameters/us-state-ansi-fips.csv'
state_fips = read_csv(state_fips_file)
state_fips.loc[:, 'stusps'] = state_fips.loc[:, 'stusps'].str[1:]
# <codecell>

# post-process survival rate
survival_rate_sel = survival_rate[['ageID', 'sourceTypeID', 'survivalRate']]
ldt_survival_rate = survival_rate_sel.loc[survival_rate_sel[ 'sourceTypeID'] == 32]
ldt_survival_rate.loc[:, 'veh_class'] = 'Light-duty Class12A'
ldt_survival_rate_2 = ldt_survival_rate.copy()
ldt_survival_rate_2.loc[:, 'veh_class'] = 'Light-duty Class2B3'
ldt_survival_rate = pd.concat([ldt_survival_rate, ldt_survival_rate_2])


MHD_st = [52, 53, 61, 62]
veh_type = {52: 'vocational', 53: 'vocational', 
            61: 'tractor', 62: 'tractor'}
hauling_mode = {52: 'short haul', 53: 'long haul', 
                61: 'short haul', 62: 'long haul'}

veh_type_mapping = {'vocational': [ 'Medium-duty Vocational', 'Heavy-duty Vocational'], 
            'tractor': ['Heavy-duty Tractor']}

mhd_survival_rate = survival_rate_sel.loc[survival_rate_sel['sourceTypeID'].isin(MHD_st)]
mhd_survival_rate.loc[:, 'vocation'] = mhd_survival_rate.loc[:, 'sourceTypeID'].map(veh_type)
mhd_survival_rate.loc[:, 'hauling_mode'] = mhd_survival_rate.loc[:, 'sourceTypeID'].map(hauling_mode)
mhd_survival_rate = mhd_survival_rate.loc[mhd_survival_rate['hauling_mode'] == 'long haul']
mhd_survival_rate.loc[:, 'veh_class'] = mhd_survival_rate.loc[:, 'vocation'].map(veh_type_mapping)
mhd_survival_rate = mhd_survival_rate.explode('veh_class')

survival_rate_synthfirm = pd.concat([mhd_survival_rate, ldt_survival_rate ])
survival_rate_synthfirm = survival_rate_synthfirm[['veh_class', 'ageID', 'survivalRate']]

# <codecell>

# pre-process age distribution by state
fleet_mix_baseyear.loc[:, 'yearID'] = baseline_year
fleet_mix_baseyear.loc[:, 'ageID'] = fleet_mix_baseyear.loc[:, 'age']
max_age = survival_rate_synthfirm.ageID.max()
fleet_mix_baseyear.loc[fleet_mix_baseyear['ageID']>=max_age, 'ageID'] = max_age
print(fleet_mix_baseyear.ageID.max())


fleet_mix_baseyear_agg = \
    fleet_mix_baseyear.groupby(['state_abbr', 'service_type', 'veh_class', 'yearID', 'ageID'])[['vehicle_count']].sum()
fleet_mix_baseyear_agg = fleet_mix_baseyear_agg.reset_index()
print(len(fleet_mix_baseyear_agg))

agg_var = ['state_abbr', 'service_type', 'yearID', 'veh_class']

fleet_mix_baseyear_agg = fleet_mix_baseyear_agg.set_index('ageID')
fleet_mix_baseyear_agg = \
    fleet_mix_baseyear_agg.groupby(agg_var).apply(lambda x: x.reindex(range(0, max_age+1)))
    
fleet_mix_baseyear_agg = fleet_mix_baseyear_agg[['vehicle_count']]
fleet_mix_baseyear_agg = fleet_mix_baseyear_agg.reset_index()
print(len(fleet_mix_baseyear_agg))

# private fleet composition by state
fleet_mix_baseyear_private = \
fleet_mix_baseyear_agg.loc[fleet_mix_baseyear_agg['service_type'] == 'PRIVATE']
fleet_mix_baseyear_private.drop(columns = ['service_type'], inplace = True)
# for-hire fleet composition for US
fleet_mix_baseyear_hire = \
fleet_mix_baseyear_agg.loc[fleet_mix_baseyear_agg['service_type'] == 'FOR HIRE']
fleet_mix_baseyear_hire = \
    fleet_mix_baseyear_hire.groupby(['veh_class', 'yearID', 'ageID'])[['vehicle_count']].sum()
fleet_mix_baseyear_hire = fleet_mix_baseyear_hire.reset_index()
# for-hire fleet composition for US
fleet_mix_baseyear_lease = \
fleet_mix_baseyear_agg.loc[fleet_mix_baseyear_agg['service_type'] == 'LEASE']
fleet_mix_baseyear_lease = \
    fleet_mix_baseyear_lease.groupby(['veh_class', 'yearID', 'ageID'])[['vehicle_count']].sum()
fleet_mix_baseyear_lease = fleet_mix_baseyear_lease.reset_index()

def gen_age_dist(df, agg_var):
    df.fillna(0, inplace =  True)
    df.loc[:, 'ageFraction'] =  \
        df.loc[:, 'vehicle_count'] / \
            df.groupby(agg_var)['vehicle_count'].transform('sum')
    
    df.rename(columns = {'vehicle_count': 'population_by_year'}, inplace = True)
    df.loc[:, 'vehTypePopulation'] =\
        df.groupby(agg_var)['population_by_year'].transform('sum') 
    return(df)

agg_var = ['state_abbr', 'yearID', 'veh_class']
fleet_mix_baseyear_private = gen_age_dist(fleet_mix_baseyear_private, 
                                          agg_var)

agg_var_2 = ['yearID', 'veh_class']
fleet_mix_baseyear_hire = gen_age_dist(fleet_mix_baseyear_hire, 
                                          agg_var_2)

fleet_mix_baseyear_lease = gen_age_dist(fleet_mix_baseyear_lease, 
                                          agg_var_2)
   
# <codecell>

# produce annual fleet composition

pop_turnover_rate.rename(columns = {'Year': 'yearID', 'vehicleType': 'veh_class'},  
                         inplace = True)

# <codecell>

def pop_gen_by_year(df, agg_var, pop_turnover_rate):
    df = \
        df.groupby(agg_var)[['population_by_year']].sum()
    df = df.reset_index()
    df.rename(columns = {'population_by_year':'vehTypePopulation'}, 
                                  inplace = True)
    
    df_out = pd.merge(df,
                      pop_turnover_rate,
                      on = 'veh_class', how = 'left')
    
    df_out.loc[:, 'vehTypePopulation'] *= \
        df_out.loc[:, 'Cumulative growth rate']
        
    df_out.loc[:, 'NewSale'] = \
        df_out.loc[:, 'vehTypePopulation'] * \
            df_out.loc[:, 'new sale rate']
    
    df_out = \
        df_out.sort_values(by = 'yearID', ascending = True)
    df_out.loc[:, 'vehTypePopulationPre'] = \
        df_out.groupby(agg_var)['vehTypePopulation'].shift(1)
    
    df_out.loc[:, 'DeltaPop'] = \
        df_out.loc[:, 'vehTypePopulationPre'] * \
            (df_out.loc[:, 'PopGrowthFactor'] - 1)
    df_out.loc[:, 'DeltaPop'].fillna(0, inplace = True)
    
    
    df_out.loc[:, 'ScrappedVeh'] = \
        df_out.loc[:, 'NewSale'] - \
            df_out.loc[:, 'DeltaPop']
    
    df_out = \
        df_out.sort_values(by = 'yearID', ascending = True)
    df_out.loc[:, 'ScrappedVeh'] = \
        df_out.groupby(agg_var)['ScrappedVeh'].shift(-1)
    df_out.loc[:, 'ScrappedVeh'].fillna(0, inplace = True) 
    
    int_var = ['vehTypePopulation', 'NewSale', 'DeltaPop', 'ScrappedVeh']
    df_out.loc[:, int_var] = np.round(df_out.loc[:, int_var], 0)
    return df_out

population_baseyear_private = pop_gen_by_year(fleet_mix_baseyear_private, 
                                          ['state_abbr', 'veh_class'],
                                          pop_turnover_rate)

agg_var_2 = ['yearID', 'veh_class']
population_baseyear_hire = pop_gen_by_year(fleet_mix_baseyear_hire, 
                                          ['veh_class'], pop_turnover_rate)

population_baseyear_lease = pop_gen_by_year(fleet_mix_baseyear_lease, 
                                          ['veh_class'], pop_turnover_rate)

# <codecell>

def fleet_turnover_gen(initial_df, population_df, survival_rate, 
                       veh_attr = 'veh_class', 
                       loc_attr = None):
    fleet_mix_current_year = initial_df.copy()

    # fleet_mix_age_30_plus = fleet_mix_current_year.loc[fleet_mix_current_year['ageID'] == 30]
    fleet_mix_next_year = None

    end_year = population_df.yearID.max()
    list_of_years = list(range(baseline_year, end_year))

    fleet_mix_by_year = fleet_mix_current_year.copy() # all year mix


    for year in list_of_years:
        next_year = year + 1
        print('generate age distribution for year=' + str(next_year))
        
        fleet_total_by_year = \
            population_df.loc[population_df['yearID'] == year]
        
        fleet_mix_next_year = fleet_mix_current_year.copy()

        # re-append survival rate for every year, because after normalization it will be gone
        fleet_mix_next_year = pd.merge(fleet_mix_next_year,
                                      survival_rate_synthfirm,
                                      on = [veh_attr, 'ageID'], how = 'left')
        # print(len(fleet_mix_next_year))       
        fleet_mix_next_year.loc[:, 'veh_to_scrap'] = \
            fleet_mix_next_year.loc[:, 'population_by_year'] * \
                (1 - fleet_mix_next_year.loc[:, 'survivalRate'])
        # define level of resolution        
        var_to_keep = [veh_attr, 'ScrappedVeh']
        group_var = [veh_attr]
        agg_var = [veh_attr, 'yearID', 'ageID']
        new_sale_var = [veh_attr, 'vehTypePopulation', 'yearID', 
                            'new sale rate', 'NewSale']
        k_var = [veh_attr, 'k_factor']
        
        if loc_attr is not None:
            var_to_keep.append(loc_attr)
            group_var.append(loc_attr)
            agg_var.append(loc_attr)
            new_sale_var.append(loc_attr)
            k_var.append(loc_attr)
        
        # scrap old vehicle based on survival curve
        scrappage_goal = fleet_total_by_year[var_to_keep]
        scrappage_est = \
            fleet_mix_next_year.groupby(group_var)['veh_to_scrap'].sum()
        scrappage_est = scrappage_est.reset_index()
        print('scrappage vehicle compare before k-adjust (left - goal, right - estimate):')
        print(scrappage_goal.ScrappedVeh.sum(), scrappage_est.veh_to_scrap.sum())
        
        # calculate and apply k-adj factor
        k_factor = pd.merge(scrappage_goal, scrappage_est, 
                            on = group_var, how = 'outer')
        k_factor.loc[:, 'k_factor'] = \
            k_factor.loc[:, 'ScrappedVeh'] / k_factor.loc[:, 'veh_to_scrap']
        k_factor = k_factor[k_var]
        fleet_mix_next_year = pd.merge(fleet_mix_next_year,
                                       k_factor, 
                                       on = group_var, how = 'left')
        # print(len(fleet_mix_next_year))
        fleet_mix_next_year.loc[:, 'veh_to_scrap'] = \
            fleet_mix_next_year.loc[:, 'veh_to_scrap'] * fleet_mix_next_year.loc[:, 'k_factor']

        scrappage_est_2 = \
            fleet_mix_next_year.groupby(group_var)['veh_to_scrap'].sum()
        scrappage_est_2 = scrappage_est_2.reset_index()
        print('scrappage vehicle compare after k-adjust (left - goal, right - estimate):')
        print(scrappage_goal.ScrappedVeh.sum(), scrappage_est_2.veh_to_scrap.sum())   
        
        # generate population after scrappage
        fleet_mix_next_year.loc[:, 'population_by_year'] = \
            fleet_mix_next_year.loc[:, 'population_by_year'] - \
                fleet_mix_next_year.loc[:, 'veh_to_scrap']
        fleet_mix_next_year.drop(columns = ['veh_to_scrap', 'k_factor'], inplace = True)
               
        # increasing age id by 1
        fleet_mix_next_year.loc[:, 'ageID'] += 1
        fleet_mix_next_year.loc[:, 'yearID'] = next_year
        # append new sale
        next_year_new_sale = \
            population_df.loc[population_df['yearID'] == next_year]
        next_year_new_sale = \
        next_year_new_sale[new_sale_var]   
        next_year_new_sale.loc[:, 'survivalRate'] = 1
        next_year_new_sale.loc[:, 'ageID'] = 0

        next_year_new_sale.rename(columns = {'NewSale': 'population_by_year',
                                             'new sale rate': 'ageFraction'}, 
                                  inplace = True)
        fleet_mix_next_year = pd.concat([fleet_mix_next_year, next_year_new_sale])
        
        fleet_mix_next_year.loc[:, 'ageFraction'] = \
            fleet_mix_next_year.loc[:, 'population_by_year']/ \
            fleet_mix_next_year.groupby(group_var)['population_by_year'].transform('sum')

        fleet_mix_next_year.loc[fleet_mix_next_year['ageID'] > max_age, 'ageID'] = max_age


        # renormalize age distribution
        
        fleet_mix_next_year = fleet_mix_next_year.groupby(agg_var)[['population_by_year']].sum()     
        fleet_mix_next_year =fleet_mix_next_year.reset_index()
      
        fleet_mix_next_year.loc[:, 'vehTypePopulation'] = \
            fleet_mix_next_year.groupby(group_var)['population_by_year'].transform('sum')

        fleet_mix_next_year.loc[:, 'ageFraction'] = \
            fleet_mix_next_year.loc[:, 'population_by_year'] / \
                fleet_mix_next_year.loc[:, 'vehTypePopulation']
        
       # prepare for next iteration
        fleet_mix_by_year = pd.concat([fleet_mix_by_year, fleet_mix_next_year])
        fleet_mix_current_year = fleet_mix_next_year.copy()
    return(fleet_mix_by_year)


print('fleet turnover for for-lease trucks...')
fleet_mix_by_year_lease = fleet_turnover_gen(fleet_mix_baseyear_lease, 
                                               population_baseyear_lease, 
                                               survival_rate_synthfirm, 
                                               veh_attr = 'veh_class'
                                               )

# <codecell>
print('fleet turnover for private trucks...')
fleet_mix_by_year_private = fleet_turnover_gen(fleet_mix_baseyear_private, 
                                               population_baseyear_private, 
                                               survival_rate_synthfirm, 
                                               veh_attr = 'veh_class',
                                               loc_attr = 'state_abbr'
                                               )
# <codecell>
print('fleet turnover for for-hire trucks...')
fleet_mix_by_year_hire = fleet_turnover_gen(fleet_mix_baseyear_hire, 
                                               population_baseyear_hire, 
                                               survival_rate_synthfirm, 
                                               veh_attr = 'veh_class'
                                               )


# <codecell>

# plot sample results for selected states and year
state = 'WA'
years = [2021, 2030, 2040, 2050]
sns.set_theme(style="whitegrid", font_scale=1.4)

# plot selected age distribution
fleet_mix_to_plot = fleet_mix_by_year_private.loc[fleet_mix_by_year_private['state_abbr'] == state]
fleet_mix_to_plot = fleet_mix_to_plot.loc[fleet_mix_to_plot['yearID'].isin(years)]

ax = sns.relplot(data = fleet_mix_to_plot, x= 'ageID', y = 'ageFraction',
            hue = 'yearID', col='veh_class', col_wrap = 3, kind = 'line', palette='Spectral')
ax.set_titles("{col_name}")
ax.set(ylim=(0, 0.15))
plt.savefig(os.path.join(path_to_moves, 'plot_forecast', 'synthfirm_age_distribution_' + state + '.png'), dpi = 300,
            bbox_inches = 'tight')
plt.show()


# fleet_mix_to_plot = fleet_mix_by_year_private.loc[fleet_mix_by_year_private['state_abbr'] == state]
fleet_mix_to_plot = fleet_mix_by_year_hire.loc[fleet_mix_by_year_hire['yearID'].isin(years)]

ax = sns.relplot(data = fleet_mix_to_plot, x= 'ageID', y = 'ageFraction',
            hue = 'yearID', col='veh_class', col_wrap = 3, kind = 'line', palette='Spectral')
ax.set_titles("{col_name}")
ax.set(ylim=(0, 0.15))
plt.savefig(os.path.join(path_to_moves, 'plot_forecast', 'synthfirm_age_distribution_for-hire.png'), dpi = 300,
            bbox_inches = 'tight')
plt.show()


fleet_mix_to_plot = fleet_mix_by_year_lease.loc[fleet_mix_by_year_lease['yearID'].isin(years)]

ax = sns.relplot(data = fleet_mix_to_plot, x= 'ageID', y = 'ageFraction',
            hue = 'yearID', col='veh_class', col_wrap = 3, kind = 'line', palette='Spectral')
ax.set_titles("{col_name}")
ax.set(ylim=(0, 0.15))
plt.savefig(os.path.join(path_to_moves, 'plot_forecast', 'synthfirm_age_distribution_for-lease.png'), dpi = 300,
            bbox_inches = 'tight')
plt.show()


# <codecell>

# writing output

fleet_mix_by_year_private.to_csv(os.path.join(path_to_moves, 'turnover', 'synthfirm_age_distribution_private.csv'),
                          index = False)

fleet_mix_by_year_hire.to_csv(os.path.join(path_to_moves, 'turnover', 'synthfirm_age_distribution_hire.csv'),
                          index = False)

fleet_mix_by_year_lease.to_csv(os.path.join(path_to_moves, 'turnover', 'synthfirm_age_distribution_lease.csv'),
                          index = False)

