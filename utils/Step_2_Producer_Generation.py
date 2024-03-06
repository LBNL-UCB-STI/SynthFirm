#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  4 10:20:28 2024

@author: xiaodanxu
"""

import pandas as pd
import os
import numpy as np
from pandas import read_csv

print("Generating synthetic producers...")

########################################################
#### step 1 - configure environment and load inputs ####
########################################################


scenario_name = 'Seattle'
out_scenario_name = 'Seattle'
file_path = '/Users/xiaodanxu/Documents/SynthFirm.nosync'
parameter_dir = 'SynthFirm_parameters'
input_dir = 'inputs_' + scenario_name
output_dir = 'outputs_' + out_scenario_name

# specifications that has been defined before
c_n6_n6io_sctg_file = 'corresp_naics6_n6io_sctg_revised.csv'
synthetic_firms_no_location_file = "synthetic_firms.csv" 

# specifications that are new to this code
zonal_id_file = "zonal_id_lookup_final.csv" # zonal ID lookup table 
foreign_prod_file = "data_foreign_prod.csv" # total foreign production (pre-defined from prior studies)
BEA_io_2017_file = "data_2017io_revised_USE_value_added.csv" # final scaled BEA I-O use table
agg_unit_cost_file = "data_unitcost_cfs2017.csv" # unit cost by commodity from CFS 2017 (all zones combined)
prod_by_zone_file = "producer_value_fraction_by_faf.csv" # Total production value by FAF zone from CFS 2017
SCTG_group_file = "SCTG_Groups_revised.csv" # Commodity type to group lookup (pre-defined)

# load inputs
firms = read_csv(os.path.join(file_path, output_dir, synthetic_firms_no_location_file), low_memory=False) # 8,396, 679 FIRMS
mesozone_faf_lookup = read_csv(os.path.join(file_path, input_dir, zonal_id_file))
c_n6_n6io_sctg = read_csv(os.path.join(file_path, parameter_dir, c_n6_n6io_sctg_file))
for_prod = read_csv(os.path.join(file_path, parameter_dir, foreign_prod_file))

io = read_csv(os.path.join(file_path, parameter_dir, BEA_io_2017_file))
unitcost = read_csv(os.path.join(file_path, parameter_dir, agg_unit_cost_file))

producer_value_fraction_by_location = \
    read_csv(os.path.join(file_path, parameter_dir, prod_by_zone_file))

sctg_lookup = read_csv(os.path.join(file_path, parameter_dir, SCTG_group_file))

# define constant
foreignprodcostfactor = 0.9     # producer cost factor for foreign produers (applied to unit costs) 
wholesalecostfactor = 1.2     # markup factor for wholesalers (applied to unit costs)

# define output
io_summary_file = "io_summary_revised.csv" # i-o table in long format, without zero values
synthetic_wholesaler_file = "synthetic_wholesaler.csv" # synthetic wholesaler (serve as both buyer and supplier)
synthetic_producer_file = "synthetic_producers.csv" # synthetic producer
synthetic_producer_by_sctg_filehead = "prods_sctg" # file head for synthetic producer by sctg
io_filtered_file = "data_2017io_filtered.csv" # processed I-O table, after dropping wholesale transaction

# define result directory
result_dir = os.path.join(file_path, output_dir)



# <codecell>
########################################################
#### step 2 - Creation of Producers database ###########
########################################################

# All agents that produce some SCTG commodity become potential producers
# wholesales are generated separately below

producers = firms.loc[firms['Commodity_SCTG'] > 0] 
producers = producers.loc[producers['Industry_NAICS6_Make'].str[:2] != '42']
# 424,120 producers, exclude wholesale

# convert I-O to long table
io = pd.melt(io, id_vars=['make'], 
        var_name='Industry_NAICS6_Use', value_name='ProVal')

io = io.reset_index()

io = io.rename(columns = {'make': 'Industry_NAICS6_Make'})
io = io.drop(columns = 'index')
io.loc[:, 'Industry_NAICS6_Use'] = io.loc[:, 'Industry_NAICS6_Use'].str[1:7]

io = io.loc[io['ProVal']>0]
io.to_csv(os.path.join(file_path, parameter_dir, io_summary_file), index = False) 

 

# <codecell>
########################################################
#### step 3 - Creation of wholesale producers ##########
########################################################
 
# generate wholesale production
to_wholesale = io.loc[io['Industry_NAICS6_Use'].str[:2] == "42"] # 1383 rows, 3 variables

c_n6_n6io_sctg = c_n6_n6io_sctg.loc[c_n6_n6io_sctg['Commodity_SCTG'] > 0] # keep industries that produce commodity (not service)
naics_by_sctg_fration = \
    c_n6_n6io_sctg.drop_duplicates(subset = ['Commodity_SCTG', 'Industry_NAICS6_Make', 'Proportion'], keep = 'first')
 
# rescaling production fraction
naics_by_sctg_fration.loc[:, 'Proportion'] = \
    naics_by_sctg_fration.loc[:, 'Proportion'] / \
        naics_by_sctg_fration.groupby(['Industry_NAICS6_Make'])['Proportion'].transform('sum') 


# for selected wholesale industry, define the industry of commodity supplier

to_wholesale_by_sctg = \
    pd.merge(to_wholesale, 
             naics_by_sctg_fration, 
             on = "Industry_NAICS6_Make", how = 'inner') # 954 * 5

to_wholesale_by_sctg.loc[:, 'ProVal'] = \
    to_wholesale_by_sctg.loc[:, 'ProVal'] * to_wholesale_by_sctg.loc[:, 'Proportion']

to_wholesale_by_sctg = \
    to_wholesale_by_sctg[['Industry_NAICS6_Make', 'Industry_NAICS6_Use', 'ProVal', 'Commodity_SCTG', 'Proportion']]


# define wholesale industry as supplier

from_wholesale = io.loc[io['Industry_NAICS6_Make'].str[:2] == "42"] # 859 rows
from_wholesale = from_wholesale.rename(columns = {'ProVal': 'ProValFromWhl'})

wholesale_flow = \
  pd.merge(to_wholesale_by_sctg, from_wholesale, 
        left_on = 'Industry_NAICS6_Use', 
        right_on = 'Industry_NAICS6_Make', how = 'outer') #size = 82508 * 8
 # <codecell>  
  
whlprod =  from_wholesale.loc[:, 'ProValFromWhl'].sum() # this is the margin = sales - cost of good sold
whlcons =  to_wholesale_by_sctg.loc[:, 'ProVal'].sum()

wholesale_flow.loc[:, 'ProValPctUse'] = wholesale_flow.loc[:, 'ProVal']/ \
wholesale_flow.groupby(['Industry_NAICS6_Make_y', 'Industry_NAICS6_Use_y'])['ProVal'].transform('sum')

wholesalecostfactor = (whlcons + whlprod) / whlcons # assume zero inventory -> all commodity purchased will be sold

wholesale_flow.loc[:, 'CellValue'] = wholesale_flow.loc[:, 'ProValPctUse'] * wholesale_flow.loc[:, 'ProValFromWhl'] 

wholesale_flow.loc[:, 'CellValue'] *= whlcons/whlprod # scale value to cost of good
wholesale_flow.loc[:, 'CellValue'] *= wholesalecostfactor # add margin
wholesale_flow.loc[:, 'CellValue'] = np.round(wholesale_flow.loc[:, 'CellValue'], 0)
wholesale_flow = wholesale_flow.loc[wholesale_flow['CellValue'] > 0]
print(wholesale_flow.loc[:, 'CellValue'].sum())

wholesale_flow = \
    wholesale_flow[['Industry_NAICS6_Make_x', 'Industry_NAICS6_Use_y', 'Commodity_SCTG', 'Industry_NAICS6_Make_y', 'CellValue']]

wholesale_flow.columns =['Industry_NAICS6_Make', 'Industry_NAICS6_Use', 'SCTG', 'NAICS_whl', 'ProValWhl']    

# 21,336 rows


iowhl = wholesale_flow.groupby(['Industry_NAICS6_Make', 'Industry_NAICS6_Use'])[['ProValWhl']].sum()
iowhl = iowhl.reset_index()
print(iowhl.loc[:, 'ProValWhl'].sum())
# 10533 row

  
 
 #remove wholesale records from io table
io_no_wholesale = \
  io.loc[(io['Industry_NAICS6_Use'].str[:2] != "42") & (io['Industry_NAICS6_Make'].str[:2] != "42")]

io_with_wholesale = pd.merge(io_no_wholesale, iowhl,
                           on = ["Industry_NAICS6_Make", "Industry_NAICS6_Use"],
                           how = 'outer')
# some industry only transact with wholesaler, make sure those transactions are captured using outer join
io_with_wholesale.fillna(0, inplace=True)
io_with_wholesale.loc[:, 'ProVal_with_whl'] = \
    io_with_wholesale.loc[:, 'ProVal'] + io_with_wholesale.loc[:, 'ProValWhl'] 

 # <codecell>    
#add the wholesales with the correct capacities in value and tons
#to both producer and consumer tables
wholesalers = firms.loc[firms['Industry_NAICS6_Make'].str[:2] == "42",] 
# 413,532 wholesaler simulated

whlval = wholesale_flow.groupby(['NAICS_whl', 'SCTG'])[['ProValWhl']].sum()
whlval = whlval.reset_index()
whlval.columns = ['Industry_NAICS6_Make', 'Commodity_SCTG', 'ProVal']
# 270 rows
list_of_wholesaler_naics = whlval['Industry_NAICS6_Make'].unique()
wholesalers = wholesalers.drop(columns = ['Commodity_SCTG'])
wholesalers_with_sctg = None

for whl_naics in list_of_wholesaler_naics:
    print(whl_naics)
    con_firms = wholesalers.loc[wholesalers['Industry_NAICS6_Make'] == whl_naics]
    sample_size = len(con_firms)
    con_io = whlval.loc[whlval['Industry_NAICS6_Make'] == whl_naics]
    con_io.loc[:, 'probability'] = con_io.loc[:, 'ProVal'] / con_io.loc[:, 'ProVal'].sum()
    sample_naics_make = con_io.sample(n = sample_size, replace=True, weights = con_io['probability'],random_state=1)
    sample_naics_make = sample_naics_make.reset_index()
    con_firms = pd.concat([con_firms.reset_index(), sample_naics_make[['Commodity_SCTG']]], axis = 1)
    wholesalers_with_sctg = pd.concat([wholesalers_with_sctg, con_firms])

# 308235 wholesalers in the output that sell commodities

 # <codecell> 
 
# assign production for each wholesaler
wholesale_emp = \
    wholesalers_with_sctg.groupby(['Industry_NAICS6_Make', 'Commodity_SCTG', 'FAFZONE'])[['Emp']].sum()

wholesale_emp = wholesale_emp.reset_index()
whlval_with_loc = \
  pd.merge(wholesale_emp, whlval, 
  on = ["Industry_NAICS6_Make", 'Commodity_SCTG'], how = 'left')
  
producer_value_fraction_by_location = \
    producer_value_fraction_by_location.rename(columns = {'FAF':'FAFZONE'})
whlval_with_loc = \
    pd.merge(whlval_with_loc,
             producer_value_fraction_by_location[['Commodity_SCTG', 'FAFZONE', 'value_fraction']],
             on = ["Commodity_SCTG", "FAFZONE"], how = 'left') # 31,656

whlval_with_loc.loc[:, 'value_fraction'] = \
    whlval_with_loc.loc[:, 'value_fraction'] / \
        whlval_with_loc.groupby(['Industry_NAICS6_Make', 'Commodity_SCTG'])['value_fraction'].transform('sum')

whlval_with_loc.loc[:, 'ProVal'] = \
    whlval_with_loc.loc[:, 'ProVal'] * whlval_with_loc.loc[:, 'value_fraction'] 
whlval_with_loc.loc[:, 'ValEmp'] = \
    whlval_with_loc.loc[:, 'ProVal'] / whlval_with_loc.loc[:, 'Emp'] 

wholesalers_with_value = \
  pd.merge(wholesalers_with_sctg, 
           whlval_with_loc[['Industry_NAICS6_Make', 'ValEmp', 'Commodity_SCTG', 'FAFZONE']], 
           on = ['Industry_NAICS6_Make', "Commodity_SCTG", 'FAFZONE'],
           how = 'left') #merge the value per employee back on to businesses

wholesalers_with_value.loc[:, 'ProdVal'] = \
    wholesalers_with_value.loc[:, 'ValEmp']  * wholesalers_with_value.loc[:, 'Emp'] #calculate production value for each establishment, 

#======= this markes as the end of wholesaler generation ============#

#issues - need to be marked as a wholesales using the NAICS code
#but need to be tagged with the correct make/use commodity seperate from their NAICS code
#this should be easy for the consumer side, check possible for the producers side


# <codecell>
########################################################
#### step 4 - Creation of non-wholesale producers ######
########################################################

producer_emp = \
    producers.groupby(['Industry_NAICS6_Make', 'Commodity_SCTG', 'FAFZONE'])[['Emp']].sum()

producer_emp = producer_emp.reset_index()    

# 27085 * 4
