#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 12 14:56:00 2022

@author: xiaodanxu
"""

import pandas as pd
import os
import numpy as np
from pandas import read_csv

data_dir = '/Users/xiaodanxu/Documents/SynthFirm.nosync/BayArea_GIS/'
os.chdir(data_dir)

io_2012 = read_csv('BEA_IO/BEA_IO_2012_6digit_USE.csv')
io_2017 = read_csv('BEA_IO/BEA_IO_2017_USE.csv')

naics_lookup = read_csv('BEA_IO/NAICS_reference_table_final.csv')
synthfirm_naics = read_csv('BEA_IO/corresp_naics6_n6io_sctg_revised.csv')

# <codecell>

# matching NAICS code to IO table
io_2012 = io_2012.fillna(0)
io_2017 = io_2017.replace('---', 0)

io_2012_long = pd.melt(io_2012, id_vars = ['Code', '﻿Industry Description'], 
                                  var_name = 'use', value_name = 'value')

io_2017_long = pd.melt(io_2017, id_vars = ['Index', 'Commodities/Industries'], 
                                  var_name = 'use', value_name = 'value')
io_2017_long.loc[:, 'value'] = io_2017_long.loc[:, 'value'].astype(float)
io_2017_long.columns = ['make_17', 'Description', 'use_17', 'value_17']
io_2017_long = io_2017_long[['make_17', 'use_17', 'value_17']]

naics_lookup = naics_lookup[['NAICS12', 'NAICS17']]

io_2012_long = pd.merge(io_2012_long, naics_lookup, 
                        left_on = 'Code', right_on = 'NAICS12', how = 'left')

io_2012_long = io_2012_long[['Code', 'use', 'value', 'NAICS17']]
io_2012_long.columns = ['make', 'use', 'value', 'make_17']

io_2012_long = pd.merge(io_2012_long, naics_lookup, 
                        left_on = 'use', right_on = 'NAICS12', how = 'left')
io_2012_long = io_2012_long[['make', 'use', 'value', 'make_17', 'NAICS17']]
io_2012_long.columns = ['make', 'use', 'value_12', 'make_17', 'use_17']

print(io_2012_long.value_12.sum())
print(io_2017_long.value_17.sum())

# <codecell>

# apply IPF and adjust 2012 IO
io_2012_long_to_adj = io_2012_long.copy()
io_2017_row_sum = io_2017_long.groupby(['make_17'])[['value_17']].sum()
io_2017_row_sum = io_2017_row_sum.reset_index()

io_2017_col_sum = io_2017_long.groupby(['use_17'])[['value_17']].sum()
io_2017_col_sum = io_2017_col_sum.reset_index()


niter = 10
for i in range(niter):
    print('the total error in iteration ' + str(i + 1) + ' is')
# compute row (make) sum and adjust row value
    io_2012_row_sum = io_2012_long_to_adj.groupby(['make_17'])[['value_12']].sum()
    io_2012_row_sum = io_2012_row_sum.reset_index()
    
    io_row_sum = pd.merge(io_2012_row_sum, io_2017_row_sum, on = 'make_17', how = 'left')
    error1 = abs(io_row_sum.value_17.sum() - io_row_sum.value_12.sum())
    # print(error1)
    
    io_row_sum.loc[:, 'row_factor'] = io_row_sum.loc[:, 'value_17'] / io_row_sum.loc[:, 'value_12']
    io_row_sum = io_row_sum[['make_17', 'row_factor']]
    
    io_2012_long_to_adj = pd.merge(io_2012_long_to_adj, io_row_sum, on = 'make_17', how = 'left')
    io_2012_long_to_adj.loc[:, 'value_12'] = io_2012_long_to_adj.loc[:, 'value_12'] * \
        io_2012_long_to_adj.loc[:, 'row_factor']
    io_2012_long_to_adj = io_2012_long_to_adj[['make', 'use', 'value_12', 'make_17', 'use_17']]
        
    # compute column (use) sum and adjust column sum
    io_2012_col_sum = io_2012_long_to_adj.groupby(['use_17'])[['value_12']].sum()
    io_2012_col_sum = io_2012_col_sum.reset_index()
    
    io_col_sum = pd.merge(io_2012_col_sum, io_2017_col_sum, on = 'use_17', how = 'left')
    error2 = abs(io_col_sum.value_17.sum() - io_col_sum.value_12.sum())
    # print(error2)
    
    io_col_sum.loc[:, 'col_factor'] = io_col_sum.loc[:, 'value_17'] / io_col_sum.loc[:, 'value_12']
    io_col_sum = io_col_sum[['use_17', 'col_factor']]
    
    io_2012_long_to_adj = pd.merge(io_2012_long_to_adj, io_col_sum, on = 'use_17', how = 'left')
    io_2012_long_to_adj.loc[:, 'value_12'] = io_2012_long_to_adj.loc[:, 'value_12'] * \
        io_2012_long_to_adj.loc[:, 'col_factor']
    io_2012_long_to_adj = io_2012_long_to_adj[['make', 'use', 'value_12', 'make_17', 'use_17']]
    
    total_error = error1 + error2
    print(total_error)
print(io_2012_long_to_adj.value_12.sum())
# <codecell>

# compare to current IO data
current_io = read_csv('BEA_IO/data_2017io.csv')
current_io_long = pd.melt(current_io, id_vars = ['Industry_NAICS6_MakeUse'], 
                                  var_name = 'Use', value_name = 'value')
naics_code_make = current_io_long.Industry_NAICS6_MakeUse.unique()

current_io_long['Use'] = current_io_long['Use'].str[1:]
naics_code_use = current_io_long.Use.unique()

io_2012_long_filtered = io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(naics_code_make)]
io_2012_long_filtered = io_2012_long_filtered.loc[io_2012_long_filtered['use'].isin(naics_code_use)]
print(current_io_long.value.sum())
print(io_2012_long_filtered.value_12.sum())
# <codecell>
synthfirm_naics_unique = synthfirm_naics.Industry_NAICS6_Make.unique()
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['230302']), 'make'] = '230301'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['233210']), 'make'] = '230301'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['233230']), 'make'] = '230301'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['233240']), 'make'] = '230301'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['233262']), 'make'] = '230301'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['2332A0']), 'make'] = '230301'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['2332C0']), 'make'] = '230301'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['2332D0']), 'make'] = '230301'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['233411']), 'make'] = '230301'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['233412']), 'make'] = '230301'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['2334A0']), 'make'] = '230301'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['311224']), 'make'] = '31122A'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['331313']), 'make'] = '33131A'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['331410']), 'make'] = '331419'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['332119']), 'make'] = '33211B'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['332999']), 'make'] = '33299B'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['333242']), 'make'] = '333295'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['333316']), 'make'] = '333315'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['333318']), 'make'] = '33331A'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['333517']), 'make'] = '33351B'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['334118']), 'make'] = '33411A'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['33712N']), 'make'] = '33712A'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['4200ID']), 'make'] = '425000'
# io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['423100']), 'make'] = '420000'
# io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['423400']), 'make'] = '420000'
# io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['423600']), 'make'] = '420000'
# io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['423800']), 'make'] = '420000'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['423A00']), 'make'] = '423900'
# io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['424200']), 'make'] = '420000'
# io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['424400']), 'make'] = '420000'
# io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['424700']), 'make'] = '420000'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['424A00']), 'make'] = '424900'
# io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['425000']), 'make'] = '420000'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['444000']), 'make'] = '4A0000'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['446000']), 'make'] = '4A0000'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['447000']), 'make'] = '4A0000'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['448000']), 'make'] = '4A0000'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['454000']), 'make'] = '4A0000'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['493000']), 'make'] = '492000'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['4B0000']), 'make'] = '4A0000'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['524113']), 'make'] = '524100'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['5241XX']), 'make'] = '524100'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['531HSO']), 'make'] = '531000'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['531HST']), 'make'] = '531000'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['531ORE']), 'make'] = '531000'
io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(['331419']), 'make'] = '331410'


io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['230302']), 'use'] = '230301'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['233210']), 'use'] = '230301'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['233230']), 'use'] = '230301'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['233240']), 'use'] = '230301'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['233262']), 'use'] = '230301'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['2332A0']), 'use'] = '230301'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['2332C0']), 'use'] = '230301'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['2332D0']), 'use'] = '230301'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['233411']), 'use'] = '230301'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['233412']), 'use'] = '230301'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['2334A0']), 'use'] = '230301'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['311224']), 'use'] = '31122A'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['331313']), 'use'] = '33131A'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['331410']), 'use'] = '331419'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['332119']), 'use'] = '33211B'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['332999']), 'use'] = '33299B'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['333242']), 'use'] = '333295'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['333316']), 'use'] = '333315'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['333318']), 'use'] = '33331A'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['333517']), 'use'] = '33351B'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['334118']), 'use'] = '33411A'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['33712N']), 'use'] = '33712A'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['4200ID']), 'use'] = '425000'
# io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['423100']), 'use'] = '420000'
# io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['423400']), 'use'] = '420000'
# io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['423600']), 'use'] = '420000'
# io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['423800']), 'use'] = '420000'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['423A00']), 'use'] = '423900'
# io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['424200']), 'use'] = '420000'
# io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['424400']), 'use'] = '420000'
# io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['424700']), 'use'] = '420000'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['424A00']), 'use'] = '424900'
# io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['425000']), 'use'] = '420000'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['444000']), 'use'] = '4A0000'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['446000']), 'use'] = '4A0000'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['447000']), 'use'] = '4A0000'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['448000']), 'use'] = '4A0000'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['454000']), 'use'] = '4A0000'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['493000']), 'use'] = '492000'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['4B0000']), 'use'] = '4A0000'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['524113']), 'use'] = '524100'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['5241XX']), 'use'] = '524100'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['531HSO']), 'use'] = '531000'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['531HST']), 'use'] = '531000'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['531ORE']), 'use'] = '531000'
io_2012_long_to_adj.loc[io_2012_long_to_adj['use'].isin(['331419']), 'use'] = '331410'


io_2012_long_filtered = io_2012_long_to_adj.loc[io_2012_long_to_adj['make'].isin(synthfirm_naics_unique)]
io_2012_long_filtered = io_2012_long_filtered.loc[io_2012_long_filtered['use'].isin(synthfirm_naics_unique)]
print(io_2012_long_filtered.value_12.sum())
io_2012_long_filtered.loc[:, 'use'] = 'X' + io_2012_long_filtered.loc[:, 'use']
io_2012_wide = pd.pivot_table(io_2012_long_filtered, values='value_12', index=['make'],
                    columns=['use'], aggfunc=np.sum)
io_2012_wide.to_csv('BEA_IO/data_2017io_revised_USE.csv')

