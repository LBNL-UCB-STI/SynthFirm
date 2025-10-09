# -*- coding: utf-8 -*-
"""
Created on Wed May 28 10:27:06 2025

@author: xiaodanxu
"""

import os
from pandas import read_csv
import pandas as pd
import geopandas as gps
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings("ignore")

os.chdir('C:/SynthFirm')


plt.style.use('ggplot')
sns.set(font_scale=1.2)  # larger font
# load network data

path_to_file = 'Validation/FAF5_Highway_Assignment_Results'
path_to_param = 'SynthFirm_parameters'

net_geometry_file = \
'FAF5_links.geojson'

network_gdf = gps.read_file(os.path.join(path_to_file, net_geometry_file))
print(len(network_gdf))

# <codecell>

# load FAF5 network statistics

# load truck flow -- domestic only

truck_assignment_file = \
'FAF5_2017_Highway_Assignment_Results/CSV Format/FAF5 Domestic Truck Flows by Commodity_2017.csv'

truck_flow_df = read_csv(os.path.join(path_to_file, truck_assignment_file))
print(len(truck_flow_df))
print(truck_flow_df.columns)

sctg_file = 'SCTG_FAF_GROUP_LOOKUP.csv'
sctg_lookup = read_csv(os.path.join(path_to_param, sctg_file))

# <codecell>

# identify list of trips
faf_groups = sctg_lookup.FAF_group.unique()

trip_prefix = 'TOT '
trip_suffix = '-Trips'
data_source = '_17 Dom'

trip_attr_list = [trip_prefix + element + trip_suffix + data_source for element in faf_groups]
print(trip_attr_list)

# <codecell>

# calculate vmt

# select trip file
sum_attr = 'TOT Trips' + data_source
sel_attr = ['ID', sum_attr]
sel_attr.extend(trip_attr_list)
truck_flow_df_sel = truck_flow_df[sel_attr]

network_gdf_sel = network_gdf[['OBJECTID', 'ID', 'LENGTH', 
                               'Country', 'STATE', 'STFIPS', 
                               'County_Name', 'CTFIPS',
                               'Urban_Code', 'FAFZONE', 'F_Class', 'geometry']]
network_gdf_sel = network_gdf_sel.merge(truck_flow_df_sel, on = 'ID',
                                       how = 'left')
print(network_gdf_sel[[sum_attr]].sum())

# <codecell>

network_gdf_sel.loc[:, 'VMT_total'] = network_gdf_sel.loc[:, sum_attr] * \
network_gdf_sel.loc[:, 'LENGTH']

vmt_attr_list = []
for elem in faf_groups:
    print(elem)
    trip_attr = trip_prefix + elem + trip_suffix + data_source
    vmt_attr = 'VMT_' + elem
    vmt_attr_list.append(vmt_attr)
    network_gdf_sel.loc[:, vmt_attr] = network_gdf_sel.loc[:, trip_attr] * \
    network_gdf_sel.loc[:, 'LENGTH']
# daily

# <codecell>

# writing vmt output
output_attr = ['ID', 'LENGTH', 'Country', 'STATE', 'STFIPS', 
               'County_Name', 'CTFIPS', 'Urban_Code', 'FAFZONE', 
               'F_Class', sum_attr, 'VMT_total']
output_attr.extend(vmt_attr_list)
network_output_df = network_gdf_sel[output_attr]
network_output_df.to_csv(os.path.join('Validation', 'FAF_VMT' + data_source + '.csv'),
             index = False)