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



########################################################
#### step 1 - configure environment and load inputs ####
########################################################

# load model config temporarily here
# scenario_name = 'Seattle'
# out_scenario_name = 'Seattle'
# file_path = '/Users/xiaodanxu/Documents/SynthFirm.nosync'
# parameter_dir = 'SynthFirm_parameters'
# input_dir = 'inputs_' + scenario_name
# output_dir = 'outputs_' + out_scenario_name

# # c_n6_n6io_sctg_file = 'corresp_naics6_n6io_sctg_revised.csv'
# # synthetic_firms_no_location_file = "synthetic_firms.csv" 
# # zonal_id_file = "zonal_id_lookup_final.csv" # zonal ID lookup table 
# # sctg_group_file = 'SCTG_Groups_revised.csv'
# # int_mode_choice_file = 'freight_mode_choice_4alt_international_seacal.csv' # using this for seattle
# int_mode_choice_file = 'freight_mode_choice_4alt_international_seacal.csv'
# distance_travel_skim_file = 'combined_travel_time_skim.csv'
# import_file = 'import_od.csv'
# export_file = 'export_od.csv'
# import_mode_file = 'import_OD_with_mode.csv'
# export_mode_file = 'export_OD_with_mode.csv'

def int_choice_model_variable_generator(data, mode_choice_spec, 
                                        distance_travel_time_skim,
                                        is_import = True):     
    # generate variables for mode choice model #####

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

def int_mode_choice_utility_generator(data, mode_choice_param, 
                                      list_of_alternative): 
    
    # compute utility and probability by mode
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

def process_b2b_flow_summary(modeled_summary, is_import, 
                             region_code=None, by_cbg=False):
    
    # Define aggregation variables
    agg_var = ["orig_FAFID", "orig_FAFNAME", 
               "dest_FAFID", "dest_FAFNAME", 
               "Commodity_SCTG", "SCTG_Group", "mode_choice"]
    
    if by_cbg:
        agg_var.insert(0, "MESOZONE")
    
    sum_var = ['tmiles', 'ShipmentLoad', 'count']
    
    # Group by the defined variables and sum
    modeled_agg = modeled_summary.groupby(agg_var)[sum_var].sum().reset_index()
    
    # Set import/export specific columns
    if is_import:
        modeled_agg.loc[:, 'outbound'] = 1
        modeled_agg.loc[:, 'inbound'] = 0
        modeled_agg.loc[:, 'Source'] = 'Import'
    else:
        modeled_agg.loc[:, 'inbound'] = 1
        modeled_agg.loc[:, 'outbound'] = 0
        modeled_agg.loc[:, 'Source'] = 'Export'
    
    # Calculate Distance
    modeled_agg.loc[:, 'Distance'] = modeled_agg.loc[:, 'tmiles'] / 1000 / modeled_agg.loc[:, 'ShipmentLoad']
    
    # Adjust for region code if provided
    if region_code is not None:
        if is_import:
            modeled_agg.loc[modeled_agg.loc[:, 'dest_FAFID'].isin(region_code), 'inbound'] = 1
        else:
            modeled_agg.loc[modeled_agg.loc[:, 'orig_FAFID'].isin(region_code), 'outbound'] = 1
    
    return modeled_agg
    
def international_mode_choice(int_mode_choice_file, distance_travel_skim_file,
                              import_od, export_od, import_mode_file, export_mode_file,
                              mode_choice_spec, output_path, mesozone_to_faf_file,
                              international_summary_file,
                              international_summary_zone_file, region_code = None):
    print("Start international mode choice...")
    int_mode_choice_param = \
        read_csv( int_mode_choice_file)
    list_of_alternative = int_mode_choice_param['Alternative'].tolist()
    
    distance_travel_time_skim = \
        read_csv(distance_travel_skim_file, sep = ',')
    
    # international flow   
    import_output = read_csv(import_od)
    export_output = read_csv(export_od)
        
    # mesozone id lookup --> for result summary
    mesozone_lookup = read_csv(mesozone_to_faf_file, sep = ',')
    faf_name_lookup = mesozone_lookup[['FAFID', 'FAFNAME']]
    faf_name_lookup = faf_name_lookup.drop_duplicates(keep = 'first')
    
    ########################################################
    #### step 2 - prepare mode choice inputs ###############
    ########################################################
    
    # excluding OOS shipment bundles from mode choice
    # import fuel and transportation equipment does not goes to mode choice
    import_output = import_output.loc[~import_output['SCTG_Code'].isin([16, 37])]
    # export transportation equipment does not goes to mode choice
    export_output = export_output.loc[~export_output['SCTG_Code'].isin([37])]
    
    
    import_output["bundle_id"] = import_output.index + 1
    export_output["bundle_id"] = export_output.index + 1
    
    import_attr = ['bundle_id', 'PORTID', 'CBP Port Location', 
                   'FAF', 'CBPZONE', 'MESOZONE', 'TYPE', 
                   'is_airport', 'CFS_CODE', 'CFS_NAME',
                   'dms_dest', 'SCTG_Code', 'TruckLoad', 
                   'ship_count', 'value_2017', 'value_density', 'SCTG_Group']
    export_attr = ['bundle_id', 'PORTID', 'CBP Port Location', 
                   'FAF', 'CBPZONE', 'MESOZONE', 'TYPE',
                   'is_airport', 'CFS_CODE', 'CFS_NAME',
                   'dms_orig', 'SCTG_Code', 'TruckLoad', 
                   'ship_count', 'value_2017', 'value_density', 'SCTG_Group']
    
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
    
    # generate explanatory variables for mode choice    
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
    # sns.histplot(data = import_output_with_mode, x = 'Distance', 
    #              weights = 'ship_count', bins = 30, alpha = 0.5)
    # plt.show()
    # sns.histplot(data = export_output_with_mode, x = 'Distance', 
    #              weights = 'ship_count', bins = 30, alpha = 0.5)
    # plt.show()
    
    
    # <codecell>
    ########################################################
    #### step 2 - assign international modes ###############
    ########################################################
    
    
    print('Start mode choice generation')
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
    
    import_attr = ['PORTID', 'CBP Port Location', 'FAF', 'CBPZONE', 'MESOZONE', 'TYPE', 
                   'is_airport', 'CFS_CODE', 'CFS_NAME', 'dms_dest', 'SCTG_Code', 
                   'TruckLoad', 'value_2017', 'value_density', 'SCTG_Group', 'bundle_id']
    export_attr = ['PORTID', 'CBP Port Location', 'FAF', 'CBPZONE', 'MESOZONE', 'TYPE', 
                   'is_airport', 'CFS_CODE', 'CFS_NAME', 'dms_orig', 'SCTG_Code', 
                   'TruckLoad', 'value_2017', 'value_density', 'SCTG_Group', 'bundle_id']
    
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
    
    # add tmile
    import_output_with_mode_skim.loc[:,'tmiles'] = \
        import_output_with_mode_skim.loc[:,'total_weight'] * 1000 * \
            import_output_with_mode_skim.loc[:,'Distance']
      
    # in 1000 tons
    export_output_with_mode_skim.loc[:,'total_weight'] = \
        export_output_with_mode_skim.loc[:,'TruckLoad'] * \
            export_output_with_mode_skim.loc[:,'shipments'] /1000
            
    export_output_with_mode_skim.loc[:,'tmiles'] = \
        export_output_with_mode_skim.loc[:,'total_weight'] * 1000 * \
            export_output_with_mode_skim.loc[:,'Distance']            


    # <codecell>
    
    #############################################################
    #### step 3 - writing summary statistics for validation #####
    #############################################################

    # generating mode split in 1000 tons   
     
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
            
    ### B2B flow summary ###
    
    modeled_import_summary = pd.merge(import_output_with_mode_skim, faf_name_lookup,
                                  left_on = 'FAF', right_on = 'FAFID', how = 'left')
    modeled_import_summary.rename(columns = {'FAFNAME':'orig_FAFNAME'},inplace = True)
    modeled_import_summary.drop(columns = ['FAFID'], inplace = True)
    
    modeled_import_summary = pd.merge(modeled_import_summary, faf_name_lookup,
                                  left_on = 'dms_dest', right_on = 'FAFID', how = 'left')
    modeled_import_summary.rename(columns = {'FAFNAME':'dest_FAFNAME'},inplace = True)
    modeled_import_summary.drop(columns = ['FAFID'], inplace = True)
    
    modeled_import_summary.rename(columns = {'FAF': 'orig_FAFID', 
                                         'dms_dest': 'dest_FAFID',
                                         'SCTG_Code': 'Commodity_SCTG',
                                         'total_weight': 'ShipmentLoad',
                                         "shipments": "count"}, inplace = True)
    
    ### B2B flow summary by faf zone
    modeled_export_summary = pd.merge(export_output_with_mode_skim, faf_name_lookup,
                                  left_on = 'FAF', right_on = 'FAFID', how = 'left')
    modeled_export_summary.rename(columns = {'FAFNAME':'dest_FAFNAME'},inplace = True)
    modeled_export_summary.drop(columns = ['FAFID'], inplace = True)
    
    modeled_export_summary = pd.merge(modeled_export_summary, faf_name_lookup,
                                  left_on = 'dms_orig', right_on = 'FAFID', how = 'left')
    modeled_export_summary.rename(columns = {'FAFNAME':'orig_FAFNAME'},inplace = True)
    modeled_export_summary.drop(columns = ['FAFID'], inplace = True)
    
    modeled_export_summary.rename(columns = {'FAF': 'dest_FAFID', 
                                         'dms_orig': 'orig_FAFID',
                                         'SCTG_Code': 'Commodity_SCTG',      
                                         'total_weight': 'ShipmentLoad',
                                         'shipments': "count"}, inplace = True)
    
    print('Total import in region (1000 ton):')
    print(modeled_import_summary.ShipmentLoad.sum())
    print('Total export in region (1000 ton):')
    print(modeled_export_summary.ShipmentLoad.sum())
    
    # Usage example:
    # Assuming modeled_import_summary and modeled_export_summary are defined DataFrames

    combined_modeled_OD_agg = pd.concat([
        process_b2b_flow_summary(modeled_import_summary, is_import=True, region_code=region_code),
        process_b2b_flow_summary(modeled_export_summary, is_import=False, region_code=region_code)
    ])
    
    combined_modeled_OD_agg_zone = pd.concat([
        process_b2b_flow_summary(modeled_import_summary, is_import=True, region_code=region_code, by_cbg=True),
        process_b2b_flow_summary(modeled_export_summary, is_import=False, region_code=region_code, by_cbg=True)
    ])
    print('Total international flow in region (in 1000 ton):')
    print(combined_modeled_OD_agg.ShipmentLoad.sum())
    # print(combined_modeled_OD_agg_zone.ShipmentLoad.sum())
    
    combined_modeled_OD_agg.to_csv(os.path.join(output_path, international_summary_file), 
                                   sep = ',', index = False)
    combined_modeled_OD_agg_zone.to_csv(os.path.join(output_path, international_summary_zone_file), 
                                   sep = ',', index = False)
    
    # <codecell>
    # writing output

    
    int_out_dir = os.path.join(output_path, 'international')
    
    if not os.path.exists(int_out_dir):
        os.makedirs(int_out_dir)
    combined_mode_split.to_csv(os.path.join(int_out_dir, 'modeled_mode_split.csv'),
                               index = False)    
    
    # format data before writing output

    #                'TruckLoad', 'value_2017', 'value_density', 'SCTG_Group', 'bundle_id'
    data_format_import = {'PORTID': np.int64, 
                   'CBP Port Location':'string', 
                   'FAF':'int', 
                   'CBPZONE': np.int64, 
                   'MESOZONE': np.int64, 
                   'TYPE': 'string', 
                    'is_airport': 'int', 
                    'CFS_CODE': 'string', 
                    'CFS_NAME': 'string', 
                    'dms_dest': 'int', 
                    'SCTG_Code': 'int',
                    'TruckLoad': 'float',
                    'shipments': np.int64,
                    'value_2017': 'float',
                    'value_density': 'float',
                    'SCTG_Group': 'int',
                    'total_weight': 'float',
                    'mode_choice': 'string',
                    'Distance': 'float'
                    }
    import_output_with_mode_skim = import_output_with_mode_skim.astype(data_format_import)
    import_output_with_mode_skim.to_csv(import_mode_file,
                               index = False)
    
    data_format_export = {'PORTID': np.int64, 
                   'CBP Port Location':'string', 
                   'FAF':'int', 
                   'CBPZONE': np.int64, 
                   'MESOZONE': np.int64, 
                   'TYPE': 'string', 
                    'is_airport': 'int', 
                    'CFS_CODE': 'string', 
                    'CFS_NAME': 'string', 
                    'dms_orig': 'int', 
                    'SCTG_Code': 'int',
                    'TruckLoad': 'float',
                    'shipments': np.int64,
                    'value_2017': 'float',
                    'value_density': 'float',
                    'SCTG_Group': 'int',
                    'total_weight': 'float',
                    'mode_choice': 'string',
                    'Distance': 'float'
                    }
    export_output_with_mode_skim = export_output_with_mode_skim.astype(data_format_export)
    export_output_with_mode_skim.to_csv(export_mode_file,
                               index = False)
    # import_output_mode_assigned.loc[:, list_of_alternative] = \
#     np.round(import_output_mode_assigned.loc[:, list_of_alternative].mul(import_output_mode_assigned.loc[:, 'ship_count']), 0)        