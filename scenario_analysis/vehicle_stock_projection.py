#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep  1 11:50:11 2022

@author: xiaodanxu
"""

from pandas import read_csv
import pandas as pd
import numpy as np
import os
import warnings
import seaborn as sns
import matplotlib.pyplot as plt

warnings.filterwarnings('ignore')
plt.style.use('ggplot')
sns.set(font_scale=1.2)  # larger font

#import math

# change to data dir

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync/BayArea_GIS')

vehicle_stock = read_csv('registration/AEORefCase.csv')

list_of_veh_class = vehicle_stock.Class.unique()
list_of_fuel_type = vehicle_stock.fuel1.unique()

vehicle_stock.loc[:, 'Body type'] = 'Vocational'
tractor_types = ['Class 7&8 Sleeper Tractors', 'Class 7&8 Day Cab Tractors']
vehicle_stock.loc[vehicle_stock['Class'].isin(tractor_types), 'Body type'] = 'Tractor'

vehicle_stock.loc[:, 'Weight Class'] = 'Class 7&8'
vehicle_stock.loc[vehicle_stock['Class'] == 'Class 4-6 Vocational', 'Weight Class'] = 'Class 4-6'

# <codecell>
vehicle_stock_agg = vehicle_stock.groupby(['Year', 'Weight Class', 'Body type', 'fuel1'])['Stock'].sum()
vehicle_stock_agg = vehicle_stock_agg.reset_index()

sns.relplot(
    data = vehicle_stock_agg, x = "Year", y = "Stock",
    row = 'Weight Class', col = "Body type", hue = "fuel1", 
    kind="line"
)

plt.savefig('registration/AEO_fleet_by_year.png', dpi = 200, bbox_inches = 'tight')

vehicle_stock_agg.to_csv('registration/AEO_fleet_by_year.csv', index = False)