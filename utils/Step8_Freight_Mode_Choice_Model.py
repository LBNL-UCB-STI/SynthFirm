#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug  5 11:30:01 2021

@author: xiaodanxu
"""
from pandas import read_csv
import pandas as pd
import numpy as np
# import constants_sf as c
import os
import warnings
from multiprocessing import Pool

warnings.filterwarnings('ignore')
#import math

def choice_model_variable_generator(data, mode_choice_spec, distance_travel_time_skim):     # generate variables for mode choice model #####

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
    # print(data[['TruckLoad', 'weight_bin']].head(5))
    # dummy variables for commodity
    data.loc[:, 'Bulk_val'] = 1 * (data.loc[:, 'SCTG_Group'] == 1) + \
        0 * (data.loc[:, 'SCTG_Group'] != 1)
    data.loc[:, 'Fuel_fert_val'] = 1 * (data.loc[:, 'SCTG_Group'] == 2) + \
        0 * (data.loc[:, 'SCTG_Group'] != 2)
    data.loc[:, 'Interm_food_val'] = 1 * (data.loc[:, 'SCTG_Group'] == 3) + \
        0 * (data.loc[:, 'SCTG_Group'] != 3) 
    data.loc[:, 'mfr_good_val'] = 1 * (data.loc[:, 'SCTG_Group'] == 4) + \
        0 * (data.loc[:, 'SCTG_Group'] != 4)   
    
    # dummy variables for industry type 
    data.loc[:, 'Wholesale_val'] = 1 * (data.loc[:, 'NAICS_code'].isin(mode_choice_spec['NAICS_wholesale'])) + \
        0 * (~data.loc[:, 'NAICS_code'].isin(mode_choice_spec['NAICS_wholesale']))  
    data.loc[:, 'Mfr_val'] = 1 * (data.loc[:, 'NAICS_code'].isin(mode_choice_spec['NAICS_mfr'])) + \
        0 * (~data.loc[:, 'NAICS_code'].isin(mode_choice_spec['NAICS_mfr'])) 
    data.loc[:, 'Mgt_val'] = 1 * (data.loc[:, 'NAICS_code'].isin(mode_choice_spec['NAICS_mgt'])) + \
        0 * (~data.loc[:, 'NAICS_code'].isin(mode_choice_spec['NAICS_mgt'])) 
    data.loc[:, 'Retail_val'] = 1 * (data.loc[:, 'NAICS_code'].isin(mode_choice_spec['NAICS_retail'])) + \
        0 * (~data.loc[:, 'NAICS_code'].isin(mode_choice_spec['NAICS_retail']))
    data.loc[:, 'Info_val'] = 1 * (data.loc[:, 'NAICS_code'].isin(mode_choice_spec['NAICS_info'])) + \
        0 * (~data.loc[:, 'NAICS_code'].isin(mode_choice_spec['NAICS_info']))
    data.loc[:, 'TW_val'] = 1 * (data.loc[:, 'NAICS_code'].isin(mode_choice_spec['NAICS_tw'])) + \
        0 * (~data.loc[:, 'NAICS_code'].isin(mode_choice_spec['NAICS_tw']))           
    # assign value density
    # data = pd.merge(data, value_density_lookup, on = 'SCTG_Group', how = 'left')
    data.loc[:, 'value_density'] = data.loc[:, 'UnitCost'] * mode_choice_spec['lb_to_ton']
    ##### placeholder for generating alternative specific variables (travel time & cost) ######
    data = pd.merge(data, distance_travel_time_skim, 
                     on = ['orig_FAFID', 'dest_FAFID', 'Alternative'], how = 'left')
    data.loc[:, 'Cost_val'] = 0
    # 1. rail cost
    data.loc[data['Alternative'] == 'Rail/IMX', 'Cost_val'] = mode_choice_spec['rail_unit_cost'] * data.loc[data['Alternative'] == 'Rail/IMX', 'TruckLoad'] * \
        data.loc[data['Alternative'] == 'Rail/IMX','Distance']
    data.loc[(data['Alternative'] == 'Rail/IMX') & (data['Cost_val'] < mode_choice_spec['rail_min_cost']), 'Cost_val'] = mode_choice_spec['rail_min_cost']
    # 2. air cost
    data.loc[data['Alternative'] == 'Air', 'Cost_val'] = mode_choice_spec['air_min_cost'] + \
        mode_choice_spec['air_unit_cost'] * (data.loc[data['Alternative'] == 'Air','TruckLoad'] > 0.05) * \
        (data.loc[data['Alternative'] == 'Air','TruckLoad'] - 0.05) / mode_choice_spec['lb_to_ton']
    # 3. truck cost
    truck_criteria = (data['Alternative'] == 'Private Truck') | (data['Alternative'] == 'For-hire Truck')
    weight_criteria_low = (data['TruckLoad'] < 0.075)
    weight_criteria_medium = (data['TruckLoad'] >= 0.075) & (data['TruckLoad'] < 0.75)
    weight_criteria_high = (data['TruckLoad'] >= 0.75) 
    data.loc[truck_criteria & weight_criteria_low, 'Cost_val'] = mode_choice_spec['truck_unit_cost_sm'] * data.loc[truck_criteria & weight_criteria_low,'TruckLoad'] * \
        data.loc[truck_criteria & weight_criteria_low, 'Distance']
    data.loc[truck_criteria & weight_criteria_medium, 'Cost_val'] = mode_choice_spec['truck_unit_cost_md'] * data.loc[truck_criteria & weight_criteria_medium,'TruckLoad'] * \
        data.loc[truck_criteria & weight_criteria_medium, 'Distance']
    data.loc[truck_criteria & weight_criteria_high, 'Cost_val'] = mode_choice_spec['truck_unit_cost_lg'] * data.loc[truck_criteria & weight_criteria_high,'TruckLoad'] * \
        data.loc[truck_criteria * weight_criteria_high, 'Distance']   
    data.loc[truck_criteria & (data['Cost_val'] < 10), 'Cost_val'] = mode_choice_spec['truck_min_cost']
    
    # 4. parcel
    parcel_criteria_1 = (data['Alternative'] == 'Parcel') & (data['TruckLoad'] <= 0.15)
    parcel_criteria_2 = (data['Alternative'] == 'Parcel') & (data['TruckLoad'] > 0.15)
    data.loc[parcel_criteria_1, 'Cost_val'] =  \
        np.exp(mode_choice_spec['parcel_cost_coeff_a'] + mode_choice_spec['parcel_cost_coeff_b'] * data.loc[parcel_criteria_1, 'TruckLoad'] / mode_choice_spec['lb_to_ton'])
    data.loc[parcel_criteria_2, 'Cost_val'] = mode_choice_spec['parcel_max_cost'] # set shipping cost upper bound
    # 4. parcel cost
    
    ###### assign mode availability ######
    data.loc[:, 'mode_available'] = 1
    data.loc[(data['Alternative'] == 'Air') & (data['TruckLoad'] > 550), 'mode_available'] = 0 
    data.loc[(data['Alternative'] == 'Parcel') & (data['TruckLoad'] > 0.15), 'mode_available'] = 0  
    data.loc[(data['Alternative'] == 'Private Truck') & (data['Distance'] > 500), 'mode_available'] = 0 
    data.loc[(data['Distance'].isna()) | (data['Travel_time'].isna()), 'mode_available'] = 0         
    return(data)        

def mode_choice_utility_generator(data, mode_choice_param, list_of_alternative): # compute utility and probability by mode
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
    data.loc[:, 'Travel_time'] * data.loc[:, 'Ttime'] + \
    data.loc[:, 'Cost_val'] * data.loc[:, 'Cost'] + \
    data.loc[:, 'Wholesale_val'] * data.loc[:, 'Ind_ws'] + \
    data.loc[:, 'Mfr_val'] * data.loc[:, 'Ind_mfr'] + \
    data.loc[:, 'Mgt_val'] * data.loc[:, 'Ind_mgt'] + \
    data.loc[:, 'Retail_val'] * data.loc[:, 'Ind_retail'] + \
    data.loc[:, 'Info_val'] * data.loc[:, 'Ind_info'] + \
    data.loc[:, 'TW_val'] * data.loc[:, 'Ind_tw'] 
    # utility= sum of (variable * parameter)
        
        #will add new items once we have the coeff
    data.loc[:, 'Utility'].fillna(0, inplace = True)   
    data.loc[:, 'Utility'] = data.loc[:, 'Utility'].astype(float)

    # data.loc[:, 'Utility_exp'] = np.exp(data.loc[:, 'Utility'])
    data.loc[:, 'Utility_exp'] = np.exp(data.loc[:, 'Utility'].to_numpy().astype(np.float32))
    data.loc[:, 'Utility_exp'] = data.loc[:, 'Utility_exp'] * data.loc[:, 'mode_available']
    global data_to_check
    data_to_check = data
    
    mode_choice_results = data.pivot(values='Utility_exp', index='shipment_id', columns='Alternative')
    mode_choice_results["sum"] = mode_choice_results.sum(axis=1)
    mode_choice_results.loc[:, list_of_alternative] = mode_choice_results.loc[:, list_of_alternative].divide(mode_choice_results.loc[:, "sum"], axis="index")
    mode_choice_results = mode_choice_results.reset_index()   
    return(mode_choice_results)
        
def mode_choice_simulator(alternatives, probabilities, nsize = 1): # Simulate mode choice
    try:
        choice = np.random.choice(alternatives, size = nsize, p = probabilities)       
        choice = choice[0]
    except ValueError:
        choice = 'Other'
    # else:
    #     choice = 'Other'
    # print(choice)
    return(choice)

def split_dataframe(df, chunk_size = 10 ** 6): 
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    return chunks
# change to data dir

def process_chunk(args):
    modeled_OD_by_sctg, i, allparams = args
    mesozone_lookup, distance_travel_time_skim,list_of_alternative,file_name, mode_choice_spec_glb,mode_choice_param,output_dir, sctg=allparams

    print('process chunk id ' + str(i))
    # print('total shipment in this batch ' + str(len(modeled_OD_by_sctg)))
    # select domestic shipment

    modeled_OD_by_sctg['NAICS_code'] = modeled_OD_by_sctg.SellerNAICS.astype(str).str[:2] # generate 2-digit NAICS code
    modeled_OD_by_sctg['NAICS_code'] = modeled_OD_by_sctg['NAICS_code'].astype(int)
    modeled_OD_by_sctg["shipment_id"] = modeled_OD_by_sctg.index + 1 # generate shipment ID for matching
    modeled_OD_by_sctg = pd.concat([modeled_OD_by_sctg, 
                                     pd.DataFrame(columns = list_of_alternative)], 
                                    sort=False) # append mode choice
    modeled_OD_by_sctg.loc[:, list_of_alternative] = 1
 
     
     # convert data to long format, generate variables for mode choice
    modeled_OD_by_sctg_long = pd.melt(modeled_OD_by_sctg, id_vars=['BuyerID', 'BuyerZone', 'BuyerNAICS', 'SellerID', 'SellerZone',
        'SellerNAICS', 'TruckLoad', 'Commodity_SCTG', 'SCTG_Group', 'NAICS_code', 'shipment_id', 'orig_FAFID', 'dest_FAFID', 'UnitCost'], 
         value_vars=list_of_alternative,
         var_name='Alternative', value_name='constant')  # convert wide dataframe to long       
     # modeled_OD_by_sctg_long.loc[modeled_OD_by_sctg_long['TruckLoad'] > c.max_shipment_load, 'TruckLoad'] = c.max_shipment_load

     ##### generate variables for mode choice model #####     
    modeled_OD_by_sctg_long = \
         choice_model_variable_generator(modeled_OD_by_sctg_long, mode_choice_spec_glb, distance_travel_time_skim)  

     ##### compute utilities and probabilities #####
    print('start mode choice generation')
    mode_choice_results = \
        mode_choice_utility_generator(modeled_OD_by_sctg_long, mode_choice_param, list_of_alternative)        

    mode_choice_results.loc[:, 'mode_choice'] = mode_choice_results.apply(
    lambda row: mode_choice_simulator(list_of_alternative, row[list_of_alternative].to_numpy().astype(np.float32)),axis=1)
    # mode_choice_results.to_csv('mode_choice_to_check.csv')
    mode_choice_results.loc[:, 'Other'] = 0
    mode_choice_results.loc[mode_choice_results['mode_choice'] == 'Other', 'Other'] = 1
    mode_choice_results.loc[mode_choice_results['mode_choice'] != 'Other', 'probability'] = mode_choice_results.apply(
    lambda row: row[row['mode_choice']], axis=1)
     
    mode_choice_results = mode_choice_results.loc[:, ['shipment_id', 'mode_choice','probability']]
     
    print('start writing output')
 
    modeled_OD_by_sctg = pd.merge(modeled_OD_by_sctg, mode_choice_results, on = 'shipment_id', how = 'left')
    modeled_OD_by_sctg = pd.merge(modeled_OD_by_sctg, distance_travel_time_skim, left_on = ['orig_FAFID', 'dest_FAFID', 'mode_choice'], 
                                   right_on = ['orig_FAFID', 'dest_FAFID', 'Alternative'], how = 'left')
    modeled_OD_by_sctg = modeled_OD_by_sctg[['BuyerID', 'BuyerZone', 
                                              'BuyerNAICS', 'SellerID', 'SellerZone',
                                              'SellerNAICS', 'TruckLoad', 'Commodity_SCTG', 'SCTG_Group', 'NAICS_code', 
                                              'shipment_id', 'orig_FAFID', 'dest_FAFID', 'mode_choice', 
                                              'probability', 'Distance', 'Travel_time']]
    int_var = ['BuyerID', 'BuyerZone', 'SellerID', 'SellerZone', 'Commodity_SCTG', 'SCTG_Group', 'NAICS_code', 'shipment_id',
    'orig_FAFID', 'dest_FAFID']
    modeled_OD_by_sctg.loc[:, int_var] = modeled_OD_by_sctg.loc[:, int_var].astype(int)
    float_var = ['TruckLoad', 'probability', 'Distance', 'Travel_time']
    modeled_OD_by_sctg.loc[:, float_var] = modeled_OD_by_sctg.loc[:, float_var].astype(float)
    # i += 1
    # combined_modeled_OD_by_sctg = pd.concat([combined_modeled_OD_by_sctg, modeled_OD_by_sctg])
    # break
    # cut_off_point = 1000 * c.max_ton_lookup[sctg]
    # combined_modeled_OD_by_sctg.loc[combined_modeled_OD_by_sctg['TruckLoad'] > cut_off_point, 'TruckLoad'] = cut_off_point


    ##### writing output ######
    # combined_modeled_OD_by_sctg.to_csv(c.input_dir + 'sample_mode_choice.csv')
    mc_out_dir = os.path.join(output_dir, sctg)
    if not os.path.exists(mc_out_dir):
        os.makedirs(mc_out_dir)
    out_file_name = 'reassigned_' + file_name + '_' + str(i) + '.zip'
        # combined_modeled_OD_by_sctg.to_csv(c.input_dir + 'sample_mode_choice.csv')
    modeled_OD_by_sctg.to_csv(os.path.join(mc_out_dir, out_file_name)) # writing output

# os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')
#define parameter file names
def mode_choice_model(mode_choice_param_file, mesozone_to_faf_file, 
                      distance_travel_skim_file, mode_choice_spec,
                      output_path, number_of_processes):
    
    print('Assign mode choice to each shipment...')
    # load parameter
    mode_choice_spec_glb = mode_choice_spec
    mode_choice_param = read_csv(mode_choice_param_file, sep = ',')
    mesozone_lookup = read_csv(mesozone_to_faf_file, sep = ',')
    # value_density_lookup = read_csv(c.param_dir + c.value_density_file, sep = ',')
    distance_travel_time_skim = read_csv(distance_travel_skim_file, sep = ',')
    list_of_alternative = mode_choice_param['Alternative'].tolist()
    # chunk_size = 10 ** 5  # process large data by chunk
    output_dir = output_path
    ###### Functions used for mode choice variable generation and simulation #####
    
        
    # aggregated output data
    # mode_choide_by_commodity = None
    chunk_size = 10 ** 6
    njob = 0
    ###### processing OD flow and generate mode choice ##########
    for k in range(5):
        sctg = 'sctg' + str(k + 1)
        # if sctg != 'sctg1':
        #     continue
        # sctg_id = int(sctg[4])
        file_start = 'shipment_' + sctg
        filelist = [file for file in os.listdir(output_dir) if file.startswith(file_start)]
        i = 0
        for f in filelist:
            print(f)
            all_modeled_OD_by_sctg = read_csv(os.path.join(output_dir, f), low_memory=False) 
            all_modeled_OD_by_sctg.reset_index(inplace = True)
            chunks_of_flows = split_dataframe(all_modeled_OD_by_sctg, chunk_size)
        # OD_file_to_load = sctg + '_OD.csv'
            file_name = sctg + '_OD'
            # all_modeled_OD_by_sctg = pd.concat([read_csv(os.path.join(output_dir, f), low_memory=False) for f in filelist ])
            # all_modeled_OD_by_sctg.reset_index(inplace = True)
            # chunks_of_flows = split_dataframe(all_modeled_OD_by_sctg, chunk_size)
            # # OD_file_to_load = sctg + '_OD.csv'
            # file_name = sctg + '_OD'

            # parallel run mode choice
            allparams= mesozone_lookup, distance_travel_time_skim,list_of_alternative,file_name, mode_choice_spec_glb,mode_choice_param,output_dir, sctg 

            jobs=[]
            for modeled_OD_by_sctg in chunks_of_flows:
                jobs.append( (modeled_OD_by_sctg, i, allparams))
                i+=1
    
            print(len(jobs))
            
            njob+=len(jobs)
            pl=Pool(processes=number_of_processes if number_of_processes > 0 else None)
            pl.map(process_chunk, jobs)
            # process_chunk(jobs[0])
        # break
        # for modeled_OD_by_sctg in chunks_of_flows:
        #     print('process chunk id ' + str(i))        
        #     # select domestic shipment
         
        #     modeled_OD_by_sctg['NAICS_code'] = modeled_OD_by_sctg.SellerNAICS.astype(str).str[:2] # generate 2-digit NAICS code
        #     modeled_OD_by_sctg['NAICS_code'] = modeled_OD_by_sctg['NAICS_code'].astype(int)
        #     modeled_OD_by_sctg["shipment_id"] = modeled_OD_by_sctg.index + 1 # generate shipment ID for matching
        #     modeled_OD_by_sctg = pd.concat([modeled_OD_by_sctg, 
        #                                     pd.DataFrame(columns = list_of_alternative)], 
        #                                    sort=False) # append mode choice
        #     modeled_OD_by_sctg.loc[:, list_of_alternative] = 1
    
            
        #     # convert data to long format, generate variables for mode choice
        #     modeled_OD_by_sctg_long = pd.melt(modeled_OD_by_sctg, id_vars=['BuyerID', 'BuyerZone', 'BuyerNAICS', 'SellerID', 'SellerZone',
        #        'SellerNAICS', 'TruckLoad', 'SCTG_Group', 'NAICS_code', 'shipment_id', 'orig_FAFID', 'dest_FAFID', 'UnitCost'], 
        #         value_vars=list_of_alternative,
        #         var_name='Alternative', value_name='constant')  # convert wide dataframe to long       
        #     # modeled_OD_by_sctg_long.loc[modeled_OD_by_sctg_long['TruckLoad'] > c.max_shipment_load, 'TruckLoad'] = c.max_shipment_load

        #     ##### generate variables for mode choice model #####     
        #     modeled_OD_by_sctg_long = \
        #         choice_model_variable_generator(modeled_OD_by_sctg_long, mode_choice_spec, distance_travel_time_skim)  

        #     ##### compute utilities and probabilities #####
        #     print('start mode choice generation')
        #     mode_choice_results = \
        #         mode_choice_utility_generator(modeled_OD_by_sctg_long, mode_choice_param, list_of_alternative)        
            
        #     mode_choice_results.loc[:, 'mode_choice'] = mode_choice_results.apply(
        #     lambda row: mode_choice_simulator(list_of_alternative, row[list_of_alternative]),axis=1)
        #     mode_choice_results.loc[:, 'Other'] = 0
        #     mode_choice_results.loc[mode_choice_results['mode_choice'] == 'Other', 'Other'] = 1
        #     mode_choice_results.loc[mode_choice_results['mode_choice'] != 'Other', 'probability'] = mode_choice_results.apply(
        #     lambda row: row[row['mode_choice']], axis=1)
            
        #     mode_choice_results = mode_choice_results.loc[:, ['shipment_id', 'mode_choice','probability']]
            
        #     print('start writing output')
    
        #     modeled_OD_by_sctg = pd.merge(modeled_OD_by_sctg, mode_choice_results, on = 'shipment_id', how = 'left')
        #     modeled_OD_by_sctg = pd.merge(modeled_OD_by_sctg, distance_travel_time_skim, left_on = ['orig_FAFID', 'dest_FAFID', 'mode_choice'], 
        #                                   right_on = ['orig_FAFID', 'dest_FAFID', 'Alternative'], how = 'left')
        #     modeled_OD_by_sctg = modeled_OD_by_sctg[['BuyerID', 'BuyerZone', 
        #                                              'BuyerNAICS', 'SellerID', 'SellerZone',
        #                                              'SellerNAICS', 'TruckLoad', 'SCTG_Group', 'NAICS_code', 
        #                                              'shipment_id', 'orig_FAFID', 'dest_FAFID', 'mode_choice', 
        #                                              'probability', 'Distance', 'Travel_time']]
        #     int_var = ['BuyerID', 'BuyerZone', 'SellerID', 'SellerZone', 'SCTG_Group', 'NAICS_code', 'shipment_id',
        #    'orig_FAFID', 'dest_FAFID']
        #     modeled_OD_by_sctg.loc[:, int_var] = modeled_OD_by_sctg.loc[:, int_var].astype(int)
        #     float_var = ['TruckLoad', 'probability', 'Distance', 'Travel_time']
        #     modeled_OD_by_sctg.loc[:, float_var] = modeled_OD_by_sctg.loc[:, float_var].astype(float)
        #     i += 1
        #     # combined_modeled_OD_by_sctg = pd.concat([combined_modeled_OD_by_sctg, modeled_OD_by_sctg])
        #     # break
        #     # cut_off_point = 1000 * c.max_ton_lookup[sctg]
        #     # combined_modeled_OD_by_sctg.loc[combined_modeled_OD_by_sctg['TruckLoad'] > cut_off_point, 'TruckLoad'] = cut_off_point
    
    
        #     ##### writing output ######
        #     mc_out_dir = os.path.join(output_dir, sctg)
        #     if not os.path.exists(mc_out_dir):
        #         os.makedirs(mc_out_dir)
        #     out_file_name = 'reassigned_' + file_name + '_' + str(i) + '.zip'
        #     # combined_modeled_OD_by_sctg.to_csv(c.input_dir + 'sample_mode_choice.csv')
        #     modeled_OD_by_sctg.to_csv(os.path.join(mc_out_dir, out_file_name)) # writing output
        #     break
        # break
    print('end of mode choice generation')
    print('-----------------------------')
    return
 