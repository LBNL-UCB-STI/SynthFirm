#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep  2 11:21:56 2025

@author: xiaodanxu
"""

# set environment and import packages
import os
from pandas import read_csv
import pandas as pd
import shapely.wkt
import geopandas as gpd
import contextily as cx
import matplotlib.pyplot as plt
import seaborn as sns
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from shapely.geometry import Polygon
import numpy as np
import re
import warnings
warnings.filterwarnings("ignore")

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')

# load port location data
port_loc_dir = 'RawData/Port/Location/Ports_2014NEI.shp'
airport_loc_dir = 'RawData/Port/Location/airport_area.shp'
port_list_dir = 'RawData/Port/port_code_cbp.csv'
border_entry_dir = 'RawData/Port/Location/border_entry.txt'
# filter by location
selected_states = ['California', 'Washington', 'Oregon', 'Massachusetts']
selected_fips = ['CA', 'WA', 'OR', 'MA']

#sea and land
port_location = gpd.read_file(port_loc_dir)
port_location.loc[port_location['NAME'] == 'Carquinez', 'TYPE'] = 'Land'
port_location.loc[port_location['NAME'] == 'Richmond, CA', 'TYPE'] = 'Land'
port_location = \
port_location.loc[port_location['TYPE'] == 'Land']
port_location.loc[:, 'STATE'] = \
port_location.loc[:, 'NAME'].str.split(', ').str[1]
# port_location = \
# port_location.loc[port_location['STATE'].isin(selected_fips)]
port_location = port_location.to_crs('4326')

airport_location = gpd.read_file(airport_loc_dir)
airport_location = \
airport_location.loc[airport_location['STATE'].isin(selected_states)]
airport_location = airport_location.to_crs('4326')
airport_location.plot()

f = open(border_entry_dir,'r')
border_entry = f.readlines()
f.close()
list_of_ports = read_csv(port_list_dir)

# <codecell>

# separate list of ports by airport and sea/land ports
list_of_ports.loc[:, 'NAME'] = \
list_of_ports.loc[:, 'CBP Port Location'].str.split('(').str[0]

list_of_ports.loc[:, 'STATE'] = list_of_ports.loc[:, 'NAME'].str.split(',').str[1]
list_of_ports.loc[:, 'STATE'] = list_of_ports.loc[:, 'STATE'].str.replace(" ", "")
list_of_ports = list_of_ports.loc[list_of_ports['STATE'].isin(selected_fips)]

print(len(list_of_ports))

list_of_airports = \
list_of_ports.loc[list_of_ports['is_airport'] == 1]
print('numbers of airports in selected region:')
print(len(list_of_airports))
print(list_of_airports.NAME.unique())

list_of_other_ports = \
list_of_ports.loc[list_of_ports['is_airport'] == 0]
print('numbers of sea/land ports in selected region:')
print(len(list_of_other_ports))

# <codecell>
# match airport

airport_names = list_of_airports.NAME.unique()
output_airport_gdf = None
matched_port = []
airport_location['NAME'] = airport_location['NAME'].str.lower()
for airport in airport_names:
    print(airport)
    airport_loc = \
    process.extract(airport, airport_location['NAME'], limit=1)[0][0]
    airport_score = \
    process.extract(airport, airport_location['NAME'], limit=1)[0][1]

    # manually match some:
    if airport == 'Portland International Airport, OR ':
        # print('find the match!')
        airport_loc = 'portland intl'
    print(airport_loc, airport_score)
    airport_location_sel = \
    airport_location.loc[airport_location['NAME'] == airport_loc]
    airport_location_dissolved = airport_location_sel.dissolve()
    # airport_location_to_plot = airport_location_sel.buffer(0.5)
    ax = airport_location_sel.plot(alpha = 0.5)
    airport_location_dissolved.plot(ax = ax, facecolor='none', 
                              edgecolor='k',linewidth = 0.5)
    cx.add_basemap(ax, crs = 'EPSG:4236', 
                   source = cx.providers.CartoDB.Positron, zoom = 13)
    plt.title(airport)
    plt.savefig('RawData/Port/Plot/' + airport_loc + '_loc.png')
    airport_location_dissolved.loc[:, 'PORTID'] = airport
    airport_location_dissolved = \
    airport_location_dissolved[['NAME', 'STATE', 'geometry', 'PORTID']]
    output_airport_gdf = \
    pd.concat([output_airport_gdf, airport_location_dissolved])
    matched_port.append(airport)
    
# <codecell>

# match other ports


otherport_names = list_of_other_ports.NAME.unique()
output_port_gdf = None

port_location['NAME'] = port_location['NAME'].str.lower()


port_location = \
port_location.loc[port_location['NAME'] != 'other ports l.a. district, ca']
port_location = \
port_location.loc[port_location['FIPS'] != '44005']
canadian_border = ['Sumas, WA ', 
                   'Danville, WA ', 
                   'Ferry, WA ',
                'Boundary, WA ', 
                   'Laurier, WA ', 
                   'Oroville, WA ',
                'Frontier, WA ', 
                   'Lynden, WA ', 
                   'Metaline Falls, WA '] # 9 port
for port in otherport_names:
    print(port)
    port_loc = \
    process.extract(port, port_location['NAME'], limit=1)[0][0]
    port_score = \
    process.extract(port, port_location['NAME'], limit=1)[0][1]

    # manually match some:
    if port == 'Monterey, CA ':
        # print('find the match!')
        port_loc = 'monterey'
    if port == 'Richmond, CA ':
        # print('find the match!')
        port_loc = 'richmond, ca'
    if port == 'Selby, CA ':
        port_loc = 'san pablo bay, ca'
    if port == 'San Joaquin River, CA ':
        port_loc = 'stockton'
    if port == 'Carquinez Strait, CA ':
        port_loc = 'carquinez'
    if port == 'Aberdeen-Hoquiam, WA ':
        port_loc = 'grays harbor'
    if port in canadian_border:
        print('do not process '  + port)
        continue
    print(port_loc, port_score)
    port_location_sel = \
    port_location.loc[port_location['NAME'] == port_loc]
    port_location_dissolved = port_location_sel.dissolve()
    # airport_location_to_plot = airport_location_sel.buffer(0.5)
    ax = port_location_sel.plot(alpha = 0.5)
    port_location_dissolved.plot(ax = ax, facecolor='none', 
                              edgecolor='k',linewidth = 0.5)
    cx.add_basemap(ax, crs = 'EPSG:4236', 
                   source = cx.providers.CartoDB.Positron, zoom = 13)
    plt.title(port)
    plt.savefig('RawData/Port/Plot/' + port_loc + '_loc.png')
    port_location_dissolved.loc[:, 'PORTID'] = port
    port_location_dissolved = \
    port_location_dissolved[['NAME', 'STATE', 'geometry', 'PORTID']]
    output_port_gdf = \
    pd.concat([output_port_gdf, port_location_dissolved])
    matched_port.append(port)
output_port_gdf.loc[:, 'TYPE'] = 'Port'

# <codecell>

# find US Canada border in WA
lwas=[]
start=False
for l in border_entry:
    if "Washington" in l:
        start=True
    if "Idaho" in l:
        break
    if start==True:
        lwas.append(l)
    
pos=[] # position file of WA borders
for l in lwas:
    if "″N" in l:
        ls=l.split('\t')
        p=ls[-1].split()
        # print(ls)
        pos.append([ls[0]]+p)
        # print("#####",pos[-1])

def parse(w):
    w=re.split('°|′|″', w)[:3]
    w=[float(i) for i in w]
    w=w[0]+w[1]/60.+w[2]/60/60
    return w

p2=[]
for p in pos:
    q=np.array([-parse(p[2][:-1]), parse(p[1][:-1])])
    q1=[q+np.array(i)*1e-2 for i in [(-1,0),(0,-1),(1,0),(0,1)]]
    p1 = Polygon(q1)
    p2.append(p1)

# create geodataframe for border crossing
d = {'NAME': [p[0] for p in pos]} # NAME
g = gpd.GeoSeries(p2) # GEOMETRY
df = pd.DataFrame(d)
border_cross_gdf=gpd.GeoDataFrame(df, geometry=g, crs= 'EPSG:4326')
border_cross_gdf.plot()

for port in canadian_border:
    print(port)
    port_loc = \
    process.extract(port, border_cross_gdf['NAME'], limit=1)[0][0]
    port_score = \
    process.extract(port, border_cross_gdf['NAME'], limit=1)[0][1]

    print(port_loc, port_score)
    border_cross_sel = \
    border_cross_gdf.loc[border_cross_gdf['NAME'] == port_loc]
    border_cross_sel_dissolved = border_cross_sel.dissolve()
    # airport_location_to_plot = airport_location_sel.buffer(0.5)
    ax = border_cross_sel.plot(alpha = 0.5)
    border_cross_sel_dissolved.plot(ax = ax, facecolor='none', 
                              edgecolor='k',linewidth = 0.5)
    cx.add_basemap(ax, crs = 'EPSG:4236', 
                   source = cx.providers.CartoDB.Positron)
    plt.title(port)
    plt.savefig('RawData/Port/Plot/' + port_loc + '_loc.png')
    border_cross_sel_dissolved.loc[:, 'PORTID'] = port
    border_cross_sel_dissolved.loc[:, 'STATE'] = 'WA'
    border_cross_sel_dissolved = \
    border_cross_sel_dissolved[['NAME', 'STATE', 'geometry', 'PORTID']]
    border_cross_sel_dissolved.loc[:, 'TYPE'] = 'Crossing'
    output_port_gdf = \
    pd.concat([output_port_gdf, border_cross_sel_dissolved])
    matched_port.append(port)
    
# <codecell>
output_airport_gdf.loc[:, 'TYPE'] = 'Airport'

combined_port_gdf = pd.concat([output_airport_gdf, output_port_gdf])
print(len(combined_port_gdf))

combined_port_gdf.loc[:, 'PORTID'] = combined_port_gdf.loc[:, 'PORTID'].str[:-1]
print(combined_port_gdf.PORTID.unique()[0:5])
# combined_port_gdf.head(5)

combined_port_gdf.loc[:, 'NAME'] = combined_port_gdf.loc[:, 'PORTID']

# # relabeling PORTID
# generate numerical port IDs
combined_port_gdf.loc[:, 'PORTID'] = 1e8 + \
    combined_port_gdf.loc[:, 'PORTID'].reset_index().index
combined_port_gdf.loc[:, 'PORTID'] = combined_port_gdf.loc[:, 'PORTID'].astype(int)


combined_port_df = combined_port_gdf.drop(columns = 'geometry')
combined_port_centroid = combined_port_gdf["geometry"].centroid
combined_port_centroid = \
gpd.GeoDataFrame(geometry = gpd.GeoSeries(combined_port_centroid))
combined_port_centroid = pd.concat([combined_port_centroid, 
                               combined_port_df], axis = 1)

ax = combined_port_centroid.plot(figsize = (4,6), column='TYPE',
                                 marker='*', markersize=8, legend = True)
cx.add_basemap(ax, crs = 'EPSG:4236', 
                   source = cx.providers.CartoDB.Positron)
plt.savefig('RawData/Port/Plot/all_ports_location.png')
plt.show()

combined_port_centroid.loc[:, 'state_abbr'] = \
combined_port_centroid.loc[:, 'NAME'].str.split(', ').str[1]
combined_port_centroid_ca = \
combined_port_centroid.loc[combined_port_centroid['state_abbr'] == 'CA']
print(len(combined_port_centroid_ca))
ax = combined_port_centroid_ca.plot(figsize = (5,5), column='TYPE',
                                 marker='*', markersize=8, legend = True)
ax.set_xlim(-123,-121.5)
ax.set_ylim(37,38.5)
cx.add_basemap(ax, crs = 'EPSG:4236', 
                   source = cx.providers.CartoDB.Positron)
plt.savefig('RawData/Port/Plot/CA_ports_location.png')
plt.show()

out_path = 'SynthFirm_parameters/port_location_CA_WA_OR_MA.geojson'
combined_port_gdf.to_file(out_path, driver="GeoJSON")  
