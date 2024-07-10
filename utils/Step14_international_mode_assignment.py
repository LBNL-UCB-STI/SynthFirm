#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul  1 11:22:28 2024

@author: xiaodanxu
"""

import pandas as pd
import os
import numpy as np
from pandas import read_csv
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings("ignore")

print("Start international shipment generation...")

########################################################
#### step 1 - configure environment and load inputs ####
########################################################

# load model config temporarily here
scenario_name = 'BayArea'
out_scenario_name = 'BayArea'
file_path = '/Users/xiaodanxu/Documents/SynthFirm.nosync'
parameter_dir = 'SynthFirm_parameters'
input_dir = 'inputs_' + scenario_name
output_dir = 'outputs_' + out_scenario_name

c_n6_n6io_sctg_file = 'corresp_naics6_n6io_sctg_revised.csv'
# synthetic_firms_no_location_file = "synthetic_firms.csv" 
zonal_id_file = "zonal_id_lookup_final.csv" # zonal ID lookup table 
SCTG_group_file = 'SCTG_Groups_revised.csv'
int_mode_choice_file = 'freight_mode_choice_4alt_international.csv'
distance_travel_skim_file = 'combined_travel_time_skim.csv'

int_mode_choice_param = \
    read_csv(os.path.join(file_path, parameter_dir, int_mode_choice_file))
list_of_alternative = int_mode_choice_param['Alternative'].tolist()

distance_travel_time_skim = \
    read_csv(os.path.join(file_path, parameter_dir, distance_travel_skim_file), sep = ',')

# international flow

import_file = 'import_od.csv'
export_file = 'export_od.csv'

import_output = read_csv(os.path.join(file_path, output_dir, import_file))
export_output = read_csv(os.path.join(file_path, output_dir, export_file))

# prepare mode choice specifications
mode_choice_spec = {} 
lb_to_ton = 0.0005
mode_choice_spec['lb_to_ton'] = lb_to_ton

NAICS_wholesale = [42]
mode_choice_spec['NAICS_wholesale'] = NAICS_wholesale

NAICS_mfr = [31, 32, 33]
mode_choice_spec['NAICS_mfr'] = NAICS_mfr

NAICS_mgt = [55]
mode_choice_spec['NAICS_mgt'] = NAICS_mgt

NAICS_retail = [44, 45]
mode_choice_spec['NAICS_retail'] = NAICS_retail

NAICS_info = [51]
mode_choice_spec['NAICS_info'] = NAICS_info 

NAICS_mining = [21]
mode_choice_spec['NAICS_mining'] = NAICS_mining 

NAICS_tw = [49]
mode_choice_spec['NAICS_tw'] = NAICS_tw      

weight_bin = [0, 0.075, 0.75, 15, 22.5, 100000]
mode_choice_spec['weight_bin'] = weight_bin   

weight_bin_label = [1, 2, 3, 4, 5]
mode_choice_spec['weight_bin_label'] = weight_bin_label  

# <codecell>
########################################################
#### step 2 - prepare mode choice inputs ###############
########################################################
import_output["bundle_id"] = import_output.index + 1
export_output["bundle_id"] = export_output.index + 1

import_attr = ['bundle_id', 'CBP Port Location', 'FAF', 'is_airport', 'CFS_CODE', 'CFS_NAME',
       'dms_dest', 'SCTG_Code', 'TruckLoad', 'ship_count', 'value_2017', 'value_density', 'SCTG_Group']
export_attr = ['bundle_id', 'CBP Port Location', 'FAF', 'is_airport', 'CFS_CODE', 'CFS_NAME',
       'dms_orig', 'SCTG_Code', 'TruckLoad', 'ship_count', 'value_2017', 'value_density', 'SCTG_Group']

import_output_with_mode = pd.concat([import_output, 
                           pd.DataFrame(columns = list_of_alternative)],
                          sort=False) # append mode choice
import_output_with_mode.loc[:, list_of_alternative] = 1
import_output_with_mode = pd.melt(import_output_with_mode, 
                                  id_vars = import_attr,
                                  value_vars = list_of_alternative,
                                  var_name = 'Alternative', 
                                  value_name = 'constant')
import_output_with_mode = import_output_with_mode.reset_index()

export_output_with_mode = pd.concat([export_output, 
                           pd.DataFrame(columns = list_of_alternative)],
                          sort=False) # append mode choice
export_output_with_mode.loc[:, list_of_alternative] = 1
export_output_with_mode = pd.melt(export_output_with_mode, 
                                  id_vars = export_attr,
                                  value_vars = list_of_alternative,
                                  var_name = 'Alternative', 
                                  value_name = 'constant')
export_output_with_mode = export_output_with_mode.reset_index()

# <codecell>

def int_choice_model_variable_generator(data, mode_choice_spec, 
                                        distance_travel_time_skim,
                                        is_import = True):     # generate variables for mode choice model #####

    # weight bin
    data.loc[:, 'weight_bin'] = pd.cut(data.loc[:, 'TruckLoad'], 
                                       bins = mode_choice_spec['weight_bin'], 
                                       labels = mode_choice_spec['weight_bin_label'], 
                                       right = True,
                                       include_lowest = True)
    data.loc[:, 'weight_bin'] = data.loc[:, 'weight_bin'].astype(str)
    data.loc[:, 'weight_bin'] = data.loc[:, 'weight_bin'].astype(int)
    
    data.loc[:, 'weight_bin_1'] = 0
    data.loc[data['weight_bin'] == 1, 'weight_bin_1'] = 1
    data.loc[:, 'weight_bin_2'] = 0
    data.loc[data['weight_bin'] == 2, 'weight_bin_2'] = 1
    data.loc[:, 'weight_bin_3'] = 0
    data.loc[data['weight_bin'] == 3, 'weight_bin_3'] = 1
    data.loc[:, 'weight_bin_4'] = 0
    data.loc[data['weight_bin'] == 4, 'weight_bin_4'] = 1
    data.loc[:, 'weight_bin_5'] = 0
    data.loc[data['weight_bin'] == 5, 'weight_bin_5'] = 1
    # data.loc[:, 'weight_bin'] = pd.factorize(data.loc[:, 'weight_bin'])[0]
    # print(data['weight_bin'].dtype)
    # print(data[['SCTG_Code', 'TruckLoad', 'ship_count', 'weight_bin']].head(5))
    # dummy variables for commodity
    data.loc[:, 'Bulk_val'] = 1 * (data.loc[:, 'SCTG_Group'] == 1) + \
        0 * (data.loc[:, 'SCTG_Group'] != 1)
    data.loc[:, 'Fuel_fert_val'] = 1 * (data.loc[:, 'SCTG_Group'] == 2) + \
        0 * (data.loc[:, 'SCTG_Group'] != 2)
    data.loc[:, 'Interm_food_val'] = 1 * (data.loc[:, 'SCTG_Group'] == 3) + \
        0 * (data.loc[:, 'SCTG_Group'] != 3) 
    data.loc[:, 'mfr_good_val'] = 1 * (data.loc[:, 'SCTG_Group'] == 4) + \
        0 * (data.loc[:, 'SCTG_Group'] != 4)   
            
    # assign value density
    # data = pd.merge(data, value_density_lookup, on = 'SCTG_Group', how = 'left')
    data.loc[:, 'value_density'] = data.loc[:, 'value_density'] * \
        mode_choice_spec['lb_to_ton']
    
    data.loc[:, 'country_A_val'] = 1 * (data.loc[:, 'CFS_CODE'] == 'A') + \
            0 * (data.loc[:, 'CFS_CODE'] != 'A')   
    data.loc[:, 'country_C_val'] = 1 * (data.loc[:, 'CFS_CODE'] == 'C') + \
                0 * (data.loc[:, 'CFS_CODE'] != 'C') 
    data.loc[:, 'country_E_val'] = 1 * (data.loc[:, 'CFS_CODE'] == 'E') + \
                    0 * (data.loc[:, 'CFS_CODE'] != 'E') 
    data.loc[:, 'country_M_val'] = 1 * (data.loc[:, 'CFS_CODE'] == 'M') + \
                    0 * (data.loc[:, 'CFS_CODE'] != 'M') 
    data.loc[:, 'country_S_val'] = 1 * (data.loc[:, 'CFS_CODE'] == 'S') + \
                    0 * (data.loc[:, 'CFS_CODE'] != 'S') 
    
    ##### placeholder for generating alternative specific variables (travel time & cost) ######
    if is_import == True:
        data = pd.merge(data, distance_travel_time_skim, 
                        left_on = ['FAF', 'dms_dest', 'Alternative'],
                         right_on = ['orig_FAFID', 'dest_FAFID', 'Alternative'], 
                         how = 'left')

    else:
        data = pd.merge(data, distance_travel_time_skim, 
                        left_on = ['dms_orig', 'FAF', 'Alternative'],
                         right_on = ['orig_FAFID', 'dest_FAFID', 'Alternative'], 
                         how = 'left')
    ###### assign mode availability ######
    data.loc[:, 'mode_available'] = 1
    data.loc[(data['Alternative'] == 'Air') & (data['TruckLoad'] > 410), 'mode_available'] = 0 
    data.loc[(data['Alternative'] == 'Air') & (data['is_airport'] == 0), 'mode_available'] = 0 
    data.loc[(data['Alternative'] == 'Parcel') & (data['TruckLoad'] > 0.075), 'mode_available'] = 0  
    # data.loc[(data['Alternative'] == 'Private Truck') & (data['Distance'] > 500), 'mode_available'] = 0 
    data.loc[(data['Distance'].isna()) | (data['Travel_time'].isna()), 'mode_available'] = 0         
    return(data) 

import_output_with_mode = \
    int_choice_model_variable_generator(import_output_with_mode, mode_choice_spec, 
                                        distance_travel_time_skim,
                                        is_import = True)

export_output_with_mode = \
    int_choice_model_variable_generator(export_output_with_mode, mode_choice_spec, 
                                        distance_travel_time_skim,
                                        is_import = False)
    
# <codecell>

# check output distance distribution
sns.histplot(data = import_output_with_mode, x = 'Distance', 
             weights = 'ship_count', bins = 30, alpha = 0.5)
plt.show()
sns.histplot(data = export_output_with_mode, x = 'Distance', 
             weights = 'ship_count', bins = 30, alpha = 0.5)
plt.show()


# <codecell>
########################################################
#### step 2 - assign international modes ###############
########################################################

def int_mode_choice_utility_generator(data, mode_choice_param, list_of_alternative): # compute utility and probability by mode
    data = pd.merge(data, mode_choice_param, on = 'Alternative', how = 'left')
    # print(data['weight_bin'].dtype)
    data.loc[:, 'Utility'] = data.loc[:, 'constant'] * data.loc[:, 'Const'] + \
    data.loc[:, 'weight_bin_2'] * data.loc[:, 'Weight_2'] + \
    data.loc[:, 'weight_bin_3'] * data.loc[:, 'Weight_3'] + \
    data.loc[:, 'weight_bin_4'] * data.loc[:, 'Weight_4'] + \
    data.loc[:, 'weight_bin_5'] * data.loc[:, 'Weight_5'] + \
    data.loc[:, 'Distance'] * data.loc[:, 'Dist'] + \
    data.loc[:, 'Bulk_val'] * data.loc[:, 'Bulk'] + \
    data.loc[:, 'Fuel_fert_val'] * data.loc[:, 'Fuel_fert'] + \
    data.loc[:, 'Interm_food_val'] * data.loc[:, 'Interm_food'] + \
    data.loc[:, 'mfr_good_val'] * data.loc[:, 'Mfr_good'] + \
    data.loc[:, 'value_density'] * data.loc[:, 'Val_den'] + \
    data.loc[:, 'country_A_val'] * data.loc[:, 'CNTRY_A'] + \
    data.loc[:, 'country_C_val'] * data.loc[:, 'CNTRY_C'] + \
    data.loc[:, 'country_M_val'] * data.loc[:, 'CNTRY_M'] + \
    data.loc[:, 'country_S_val'] * data.loc[:, 'CNTRY_S'] 
    # utility= sum of (variable * parameter)
        
        #will add new items once we have the coeff
    data.loc[:, 'Utility'].fillna(0, inplace = True)   
    data.loc[:, 'Utility'] = data.loc[:, 'Utility'].astype(float)

    # data.loc[:, 'Utility_exp'] = np.exp(data.loc[:, 'Utility'])
    data.loc[:, 'Utility_exp'] = np.exp(data.loc[:, 'Utility'].to_numpy().astype(np.float32))
    data.loc[:, 'Utility_exp'] = data.loc[:, 'Utility_exp'] * data.loc[:, 'mode_available']
    global data_to_check
    data_to_check = data
    
    mode_choice_results = data.pivot(values='Utility_exp', index='bundle_id', 
                                     columns='Alternative')
    mode_choice_results["sum"] = mode_choice_results.sum(axis=1)
    mode_choice_results.loc[:, list_of_alternative] = mode_choice_results.loc[:, list_of_alternative].divide(mode_choice_results.loc[:, "sum"], axis="index")
    mode_choice_results = mode_choice_results.reset_index()   
    return(mode_choice_results)

print('start mode choice generation')
import_mode_choice_results = \
    int_mode_choice_utility_generator(import_output_with_mode, int_mode_choice_param, list_of_alternative)    
import_mode_choice_results.drop(columns = 'sum', inplace = True)

import_output_mode_assigned = pd.merge(import_output, 
                                        import_mode_choice_results, 
                                        on = 'bundle_id', how = 'left')

export_mode_choice_results = \
    int_mode_choice_utility_generator(export_output_with_mode, int_mode_choice_param, list_of_alternative)
export_mode_choice_results.drop(columns = 'sum', inplace = True)
export_output_mode_assigned = pd.merge(export_output, 
                                        export_mode_choice_results, 
                                        on = 'bundle_id', how = 'left')  
mode_attr = []
for alt in list_of_alternative:
    output_attr = alt + '_count'
    mode_attr.append(output_attr)
    import_output_mode_assigned.loc[:, output_attr] = \
    import_output_mode_assigned.loc[:, alt] * import_output_mode_assigned.loc[:, 'ship_count']
    import_output_mode_assigned.loc[:, output_attr] = np.round(import_output_mode_assigned.loc[:, output_attr], 0)
    
    export_output_mode_assigned.loc[:, output_attr] = \
        export_output_mode_assigned.loc[:, alt] * export_output_mode_assigned.loc[:, 'ship_count']
    export_output_mode_assigned.loc[:, output_attr] = np.round(export_output_mode_assigned.loc[:, output_attr], 0)
# import_output_mode_assigned.loc[:, list_of_alternative] = \
#     np.round(import_output_mode_assigned.loc[:, list_of_alternative].multiply(import_output_mode_assigned.loc[:, 'ship_count']), 0)

# 
# <codecell>
# perform all-or-nothing assignment for count less than 5
import_output_mode_assigned.loc[:, 'max_prob'] = \
    import_output_mode_assigned.loc[:, list_of_alternative].max(axis = 1)
    
export_output_mode_assigned.loc[:, 'max_prob'] = \
    export_output_mode_assigned.loc[:, list_of_alternative].max(axis = 1)

select_rows_import = (import_output_mode_assigned['ship_count']<= 5)
select_rows_export = (export_output_mode_assigned['ship_count']<= 5)
for alt in list_of_alternative:
    import_output_mode_assigned.loc[:, alt] = \
        0 * (import_output_mode_assigned.loc[:, alt]< import_output_mode_assigned.loc[:, 'max_prob']) + \
            1 * (import_output_mode_assigned.loc[:, alt] == import_output_mode_assigned.loc[:, 'max_prob'])
    
    export_output_mode_assigned.loc[:, alt] = \
        0 * (export_output_mode_assigned.loc[:, alt]< export_output_mode_assigned.loc[:, 'max_prob']) + \
            1 * (export_output_mode_assigned.loc[:, alt] == export_output_mode_assigned.loc[:, 'max_prob'])
    
    output_attr = alt + '_count'
    import_output_mode_assigned.loc[select_rows_import, output_attr] = \
    import_output_mode_assigned.loc[select_rows_import, alt] * import_output_mode_assigned.loc[select_rows_import, 'ship_count']
    import_output_mode_assigned.loc[select_rows_import, output_attr] = \
        np.round(import_output_mode_assigned.loc[select_rows_import, output_attr], 0)
    
    export_output_mode_assigned.loc[select_rows_export, output_attr] = \
        export_output_mode_assigned.loc[select_rows_export, alt] * export_output_mode_assigned.loc[select_rows_export, 'ship_count']
    export_output_mode_assigned.loc[select_rows_export, output_attr] = \
        np.round(export_output_mode_assigned.loc[select_rows_export, output_attr], 0)

# <codecell>
# reformat data table, with one mode per row

import_attr = ['CBP Port Location', 'FAF', 'is_airport', 'CFS_CODE', 'CFS_NAME',
       'dms_dest', 'SCTG_Code', 'TruckLoad', 'value_2017',
       'value_density', 'SCTG_Group', 'bundle_id']
export_attr = ['CBP Port Location', 'FAF', 'is_airport', 'CFS_CODE', 'CFS_NAME',
       'dms_orig', 'SCTG_Code', 'TruckLoad', 'value_2017',
       'value_density', 'SCTG_Group', 'bundle_id']

import_output_with_mode = pd.melt(import_output_mode_assigned, 
                                  id_vars = import_attr,
                                  value_vars = mode_attr,
                                  var_name = 'mode_choice', 
                                  value_name = 'shipments')
import_output_with_mode = import_output_with_mode.reset_index()
import_output_with_mode.loc[:, 'mode_choice'] = \
    import_output_with_mode.loc[:, 'mode_choice'].str.split('_').str[0]
import_output_with_mode = \
    import_output_with_mode.loc[import_output_with_mode['shipments'] >0]
import_output_with_mode = import_output_with_mode.drop(columns = ['index'])   

export_output_with_mode = pd.melt(export_output_mode_assigned, 
                                  id_vars = export_attr,
                                  value_vars = mode_attr,
                                  var_name = 'mode_choice', 
                                  value_name = 'shipments')
export_output_with_mode = export_output_with_mode.reset_index()

export_output_with_mode.loc[:, 'mode_choice'] = \
    export_output_with_mode.loc[:, 'mode_choice'].str.split('_').str[0]
export_output_with_mode = \
    export_output_with_mode.loc[export_output_with_mode['shipments'] >0]
export_output_with_mode = export_output_with_mode.drop(columns = ['index'])  

# <codecell>

# append distance skim
distance_skim = \
    distance_travel_time_skim[['orig_FAFID', 'dest_FAFID', 'Distance', 'Alternative']]

import_output_with_mode_skim = pd.merge(import_output_with_mode,
                                        distance_skim,                                          
                                        left_on = ['FAF', 'dms_dest', 'mode_choice'],
                                        right_on = ['orig_FAFID', 'dest_FAFID', 'Alternative'], 
                                        how = 'left')

import_output_with_mode_skim.drop(columns = ['orig_FAFID', 'dest_FAFID', 'Alternative'],
                                  inplace = True)
export_output_with_mode_skim = pd.merge(export_output_with_mode,
                                        distance_skim,
                                        left_on = ['dms_orig', 'FAF', 'mode_choice'],
                                        right_on = ['orig_FAFID', 'dest_FAFID', 'Alternative'], 
                                        how = 'left')

export_output_with_mode_skim.drop(columns = ['orig_FAFID', 'dest_FAFID', 'Alternative'],
                                  inplace = True)


# check mode split by count and weight
import_output_with_mode_skim.loc[:,'total_weight'] = \
    import_output_with_mode_skim.loc[:,'TruckLoad'] * \
        import_output_with_mode_skim.loc[:,'shipments'] /1000
  
# in 1000 tons
export_output_with_mode_skim.loc[:,'total_weight'] = \
    export_output_with_mode_skim.loc[:,'TruckLoad'] * \
        export_output_with_mode_skim.loc[:,'shipments'] /1000
# in 1000 tons        
import_mode_split = \
    import_output_with_mode_skim.groupby(['mode_choice'])[['total_weight']].sum()
import_mode_split = import_mode_split.reset_index()
import_mode_split.loc[:, 'direction'] = 'import'
export_mode_split = \
    export_output_with_mode_skim.groupby(['mode_choice'])[['total_weight']].sum()
export_mode_split = export_mode_split.reset_index()
export_mode_split.loc[:, 'direction'] = 'export'
combined_mode_split = pd.concat([import_mode_split, export_mode_split])
combined_mode_split.loc[:, 'fraction'] = \
    combined_mode_split.loc[:, 'total_weight'] / \
        combined_mode_split.groupby('direction')['total_weight'].transform('sum')
combined_mode_split.to_csv(os.path.join(file_path, input_dir, 'port', 'modeled_mode_split.csv'),
                           index = False)
# <codecell>
# writing output
# import_output_mode_assigned = \
#     import_output_mode_assigned.drop(columns = list_of_alternative)
# import_output_mode_assigned = \
#         import_output_mode_assigned.drop(columns = ['max_prob'])
        
# export_output_mode_assigned = \
#     export_output_mode_assigned.drop(columns = list_of_alternative)
# export_output_mode_assigned = \
#         export_output_mode_assigned.drop(columns = ['max_prob'])

int_out_dir = os.path.join(file_path, output_dir, 'international')
if not os.path.exists(int_out_dir):
    os.makedirs(int_out_dir)

import_output_file = 'import_OD_with_mode.csv'
import_output_mode_assigned.to_csv(os.path.join(int_out_dir, import_output_file))

export_output_file = 'export_OD_with_mode.csv'
export_output_mode_assigned.to_csv(os.path.join(int_out_dir, export_output_file))
# import_output_mode_assigned.loc[:, list_of_alternative] = \
#     np.round(import_output_mode_assigned.loc[:, list_of_alternative].mul(import_output_mode_assigned.loc[:, 'ship_count']), 0)        