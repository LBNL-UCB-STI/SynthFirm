#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb  9 13:54:20 2022

@author: xiaodanxu
"""

import os
from pandas import read_csv
import pandas as pd
import numpy as np


os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync/BayArea_GIS')

carrier_census_data = read_csv('CENSUS_20220109_all.csv', sep = ',', encoding='latin1')
# carrier_census_data.to_csv('FMCSA_CENSUS1_2022Jan.csv', index = False)


# <codecell>
sample_carrier_data = carrier_census_data.head(1000)
sample_carrier_data.to_csv('FMCSA_CENSUS_sample.csv', index = False)
print(carrier_census_data.head(5))
print(carrier_census_data.columns)
#carrier_census_data = carrier_census_data.loc[carrier_census_data['CARSHIP'] == 'C'] # = CARRIER
carrier_census_data = carrier_census_data.loc[carrier_census_data['ACT_STAT'] == 'A'] # = ACTIVE
carrier_census_data = carrier_census_data.loc[carrier_census_data['PHY_NATN'] == 'US'] # IN US
carrier_census_data = carrier_census_data.loc[carrier_census_data['PASSENGERS'] != 'X'] # NO PASSENGER
carrier_census_data = carrier_census_data.loc[carrier_census_data['GENFREIGHT'] == 'X'] # NO PASSENGER
# carrier_census_data['TOT_PWR'].hist()
# <codecell>

list_of_cargo_var = ['HOUSEHOLD', 'METALSHEET', 'MOTORVEH', 'DRIVETOW', 'LOGPOLE', 'BLDGMAT', 
                     'MOBILEHOME', 'MACHLRG', 'PRODUCE', 'LIQGAS', 'INTERMODAL', 'OILFIELD', 
                     'LIVESTOCK', 'GRAINFEED', 'COALCOKE', 'MEAT', 'GARBAGE', 'USMAIL', 'CHEM', 
                     'DRYBULK', 'COLDFOOD', 'BEVERAGES', 'PAPERPROD', 'UTILITY', 'FARMSUPP', 
                     'CONSTRUCT', 'WATERWELL', 'CARGOOTHR']

carrier_census_data.loc[:, list_of_cargo_var] = carrier_census_data.loc[:, list_of_cargo_var].replace('X', 1)
carrier_census_data.loc[:, list_of_cargo_var] = carrier_census_data.loc[:, list_of_cargo_var].replace(np.nan, 0)
print(carrier_census_data.loc[:, list_of_cargo_var].head(5))

# <codecell>
import seaborn as sns
from sklearn.manifold import TSNE
from sklearn.decomposition import FactorAnalysis, PCA
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from factor_analyzer import FactorAnalyzer
from factor_analyzer.factor_analyzer import calculate_kmo


carrier_census_data.loc[:, 'cargo_type_sum'] = carrier_census_data.loc[:, list_of_cargo_var].sum(axis = 1)
carrier_census_data_filtered = carrier_census_data.loc[carrier_census_data['cargo_type_sum'] > 0]
cluster_x = carrier_census_data_filtered.loc[:, list_of_cargo_var]

corr_matrix = cluster_x.corr()

plt.imshow(corr_matrix, cmap="RdBu_r", vmin=-1, vmax=1)
plt.show()


# factor analysis
kmo_all,kmo_model=calculate_kmo(cluster_x)
print(kmo_model)
fa = FactorAnalyzer(10, rotation=None)
fa.fit(cluster_x)
ev, v = fa.get_eigenvalues()

plt.scatter(range(1, cluster_x.shape[1]+1),ev)
plt.plot(range(1, cluster_x.shape[1]+1),ev)
plt.title('Scree Plot')
plt.xlabel('Factors')
plt.ylabel('Eigenvalue')
plt.grid()
plt.show()

fa = FactorAnalyzer(n_factors = 8,rotation='varimax')
fa.fit(cluster_x)
print(pd.DataFrame(fa.loadings_, index = cluster_x.columns))

factor_loading = pd.DataFrame(fa.loadings_, index = cluster_x.columns)
factor_loading.to_csv('cargo_type_factor_loading.csv')

# <codecell>
group_1_var = ['BLDGMAT', 'LOGPOLE', 'METALSHEET', 'MACHLRG', 'CONSTRUCT'] # construction material
group_2_var = ['COLDFOOD', 'PRODUCE', 'MEAT'] # food
group_3_var = ['GRAINFEED', 'FARMSUPP', 'LIVESTOCK'] # farm product
group_4_var = ['MOTORVEH', 'DRIVETOW', 'MOBILEHOME'] # vehicle
group_5_var = ['LIQGAS', 'CHEM'] # chemical product
group_6_var = ['OILFIELD', 'WATERWELL', 'COALCOKE', 'UTILITY'] # large equipment
group_7_var = ['GARBAGE'] # garbage
group_8_var = ['PAPERPROD', 'BEVERAGES', 'DRYBULK', 'USMAIL'] # other bulk
group_9_var = ['HOUSEHOLD'] # household good
group_10_var = ['INTERMODAL'] # intermodal container
group_11_var = ['CARGOOTHR'] # other cargo


carrier_census_data_filtered.loc[:, '1_construction_material'] = carrier_census_data_filtered.loc[:, group_1_var].max(axis = 1)
carrier_census_data_filtered.loc[:, '2_food'] = carrier_census_data_filtered.loc[:, group_2_var].max(axis = 1)
carrier_census_data_filtered.loc[:, '3_farm_product'] = carrier_census_data_filtered.loc[:, group_3_var].max(axis = 1)
carrier_census_data_filtered.loc[:, '4_vehicle_home'] = carrier_census_data_filtered.loc[:, group_4_var].max(axis = 1)
carrier_census_data_filtered.loc[:, '5_chemical'] = carrier_census_data_filtered.loc[:, group_5_var].max(axis = 1)
carrier_census_data_filtered.loc[:, '6_large_equipment'] = carrier_census_data_filtered.loc[:, group_6_var].max(axis = 1)
carrier_census_data_filtered.loc[:, '7_garbage'] = carrier_census_data_filtered.loc[:, group_7_var].max(axis = 1)
carrier_census_data_filtered.loc[:, '8_other_bulk'] = carrier_census_data_filtered.loc[:, group_8_var].max(axis = 1)
carrier_census_data_filtered.loc[:, '9_household'] = carrier_census_data_filtered.loc[:, group_9_var].max(axis = 1)
carrier_census_data_filtered.loc[:, '10_intermodal_container'] = carrier_census_data_filtered.loc[:, group_10_var].max(axis = 1)
carrier_census_data_filtered.loc[:, '11_other_cargo'] = carrier_census_data_filtered.loc[:, group_11_var].max(axis = 1)

var_to_plot = ['1_construction_material', '2_food', '3_farm_product', '4_vehicle_home',
               '5_chemical', '6_large_equipment', '7_garbage', '8_other_bulk',
               '9_household', '10_intermodal_container', '11_other_cargo']

cargo_groups = carrier_census_data_filtered.loc[:, var_to_plot]

cargo_groups_fraction = cargo_groups.sum()/len(cargo_groups)

cargo_groups_fraction = cargo_groups_fraction.to_frame()
cargo_groups_fraction.columns = ['probability']
print(cargo_groups_fraction)
cargo_groups_fraction.to_csv('probability_of_cargo_group.csv', sep = ',')
# corr = cargo_groups.corr()
plt.figure(figsize = (8, 6))
sns.heatmap(cargo_groups.corr(), cmap = 'coolwarm')
plt.savefig('cargo_group_corr_matrix.png', dpi = 200, bbox_inches = 'tight')
plt.show()

# hierarchical cluster
# from scipy.cluster.hierarchy import dendrogram, linkage

# linked = linkage(cluster_x, 'single')

# import scipy.cluster.hierarchy as shc
# plt.figure(figsize=(10, 7))  
# plt.title("Dendrograms")  
# dend = shc.dendrogram(shc.linkage(cluster_x, method='ward'))
# tsne = TSNE(n_components=2, verbose=1, random_state=123)
# results = tsne.fit_transform(cluster_x) 
# pca = PCA(n_components=2)
# results = pca.fit(cluster_x).transform(cluster_x)
# df = pd.DataFrame()
# df["comp-1"] = results[:,0]
# df["comp-2"] = results[:,1]

# sns.scatterplot(x="comp-1", y="comp-2", data=df, alpha = 0.1)
# cut_off = carrier_census_data['TOT_TRUCKS'].quantile(0.999)
# print(cut_off)
# domestic_carrier_filtered = carrier_census_data.loc[carrier_census_data['TOT_TRUCKS'] <= cut_off]
# print(domestic_carrier_filtered['TOT_TRUCKS'].sum())
# domestic_carrier_filtered['TOT_TRUCKS'].hist(bins = 30)

# list_of_var = ['OWNTRUCK', 'OWNTRACT', 'TRMTRUCK', 'TRMTRACT', 'TRPTRUCK', 'TRPTRACT', 'TOT_TRUCKS']
# truck_per_carrier = domestic_carrier_filtered.groupby(['DOT_NUMBER'])[list_of_var].sum()

# truck_per_carrier = truck_per_carrier.reset_index()
# truck_per_carrier.loc[:, 'single_truck'] = truck_per_carrier.loc[:, 'OWNTRUCK'] + \
#     truck_per_carrier.loc[:, 'TRMTRUCK'] + truck_per_carrier.loc[:, 'TRPTRUCK']
# truck_per_carrier.loc[:, 'comb_truck'] = truck_per_carrier.loc[:, 'OWNTRACT'] + \
#     truck_per_carrier.loc[:, 'TRMTRACT'] + truck_per_carrier.loc[:, 'TRPTRACT']  
    
# size_interval = [-1, 2, 5, 10, 50, 100, 1000, truck_per_carrier['TOT_TRUCKS'].max()]
# interval_name = ['0-2', '3-5', '6-10', '11-50', '51-100', '101-1000', '>1000']    
# truck_per_carrier.loc[:, 'size_group'] = pd.cut(truck_per_carrier['TOT_TRUCKS'], 
#                                                 bins = size_interval, right = True, 
#                                                 labels = interval_name)

# # <codecell>
# truck_count_by_size = truck_per_carrier.groupby('size_group').agg({'DOT_NUMBER': 'count',
#                                                                    'TOT_TRUCKS': 'sum',
#                                                                    'single_truck': 'sum',
#                                                                    'comb_truck': 'sum'})
# truck_count_by_size = truck_count_by_size.reset_index()
# truck_count_by_size.columns = ['fleet_size', 'total_carriers', 'total_trucks', 
#                                'total_single_trucks', 'total_combination_trucks']

# truck_count_by_size.loc[:, 'avg_truck_per_carrier'] = truck_count_by_size.loc[:, 'total_trucks'] / truck_count_by_size.loc[:, 'total_carriers']
# truck_count_by_size.loc[:, 'avg_sut_per_carrier'] = truck_count_by_size.loc[:, 'total_single_trucks'] / truck_count_by_size.loc[:, 'total_carriers']
# truck_count_by_size.loc[:, 'avg_ct_per_carrier'] = truck_count_by_size.loc[:, 'total_combination_trucks'] / truck_count_by_size.loc[:, 'total_carriers']
# truck_count_by_size.loc[:, 'fraction_of_carrier'] = truck_count_by_size.loc[:, 'total_carriers'] / truck_count_by_size.loc[:, 'total_carriers'].sum()
# truck_count_by_size.to_csv('fmcsa_census_fleet_size_distribution.csv', index = False, sep = ',')
# # truck_per_carrier.loc[:, 'MCS150MILEAGEYEAR'] = truck_per_carrier.loc[:, 'MCS150MILEAGEYEAR'].astype(int)
# #truck_per_carrier = truck_per_carrier.loc[truck_per_carrier['MCS150MILEAGEYEAR'] == 2019]