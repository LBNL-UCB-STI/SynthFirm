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

os.chdir('/Users/xiaodanxu/Documents/SynthFirm/BayArea_GIS')

carrier_census_data = read_csv('FMCSA_CENSUS1_2022Jan.txt', sep = ',', encoding='latin1')
# carrier_census_data.to_csv('FMCSA_CENSUS1_2022Jan.csv', index = False)


# <codecell>
sample_carrier_data = carrier_census_data.head(1000)
sample_carrier_data.to_csv('FMCSA_CENSUS1_sample.csv', index = False)
# print(carrier_census_data.head(5))
# print(carrier_census_data.columns)
carrier_data_freight = carrier_census_data.loc[carrier_census_data['PC_FLAG'] == 'N']
domestic_carrier = carrier_data_freight.loc[carrier_data_freight['PHY_COUNTRY'] == 'US']
domestic_carrier['NBR_POWER_UNIT'].hist(bins = 20)

cut_off = domestic_carrier['NBR_POWER_UNIT'].quantile(0.999)
domestic_carrier_filtered = domestic_carrier.loc[domestic_carrier['NBR_POWER_UNIT'] <= cut_off]
print(domestic_carrier_filtered['NBR_POWER_UNIT'].sum())
