#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug  5 11:30:01 2021

@author: xiaodanxu
"""
from pandas import read_csv
import pandas as pd
import numpy as np
# import constants as c
import os, sys
import warnings
import shutil
from multiprocessing import Pool
warnings.filterwarnings('ignore')
#import math

# change to data dir

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')
#define parameter file names
mode_choice_param_file = 'freight_mode_choice_7alt_austin.csv'


# load parameter
mode_choice_param = read_csv('Parameter/' + mode_choice_param_file, sep = ',')
# print(mode_choice_param)
mesozone_lookup = read_csv('Parameter/' + 'zonal_id_lookup_final.csv', sep = ',')
# value_density_lookup = read_csv(c.param_dir + c.value_density_file, sep = ',')
distance_travel_time_skim = read_csv('Parameter/' + 'combined_travel_time_skim_7alt.csv', sep = ',')
op_cost_by_scenario = read_csv('Parameter/' + 'opcost_sensitivity_analysis.csv', sep = ',')
list_of_alternative = mode_choice_param['Alternative'].tolist()
chunk_size = 10 ** 6  # process large data by chunk

weight_bin = [0, 0.075, 0.75, 15, 22.5, 30000]
weight_bin_label = [1, 2, 3, 4, 5]
output_dir = 'outputs_aus_2050/'
lb_to_ton = 1/2000
NAICS_wholesale = [42]
NAICS_mfr = [31, 32, 33]
NAICS_mgt = [55]
NAICS_retail = [44, 45]
NAICS_info = [51]
NAICS_mining = [21]
NAICS_tw = [49]
list_of_sctg_group = ['sctg1', 'sctg2', 'sctg3', 'sctg4', 'sctg5']

###### Functions used for mode choice variable generation and simulation #####

def choice_model_variable_generator(data, fuel_cost, elec_cost, rail_cost):     # generate variables for mode choice model #####
    
    # weight bin
    data.loc[:, 'weight_bin'] = pd.cut(data.loc[:, 'TruckLoad'], bins = weight_bin, 
                                       labels = weight_bin_label, right = True,
                                       include_lowest = True)
    data.loc[:, 'weight_bin'] = data.loc[:, 'weight_bin'].astype(int)
    # data.loc[:, 'weight_bin_1'] = 0
    # data.loc[data['weight_bin'] == 1, 'weight_bin_1'] = 1
    # data.loc[:, 'weight_bin_2'] = 0
    # data.loc[data['weight_bin'] == 2, 'weight_bin_2'] = 1
    # data.loc[:, 'weight_bin_3'] = 0
    # data.loc[data['weight_bin'] == 3, 'weight_bin_3'] = 1
    # data.loc[:, 'weight_bin_4'] = 0
    # data.loc[data['weight_bin'] == 4, 'weight_bin_4'] = 1
    # data.loc[:, 'weight_bin_5'] = 0
    # data.loc[data['weight_bin'] == 5, 'weight_bin_5'] = 1
    
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
    data.loc[:, 'Wholesale_val'] = 1 * (data.loc[:, 'NAICS_code'].isin(NAICS_wholesale)) + \
        0 * (~data.loc[:, 'NAICS_code'].isin(NAICS_wholesale))  
    data.loc[:, 'Mfr_val'] = 1 * (data.loc[:, 'NAICS_code'].isin(NAICS_mfr)) + \
        0 * (~data.loc[:, 'NAICS_code'].isin(NAICS_mfr)) 
    data.loc[:, 'Mgt_val'] = 1 * (data.loc[:, 'NAICS_code'].isin(NAICS_mgt)) + \
        0 * (~data.loc[:, 'NAICS_code'].isin(NAICS_mgt)) 
    data.loc[:, 'Retail_val'] = 1 * (data.loc[:, 'NAICS_code'].isin(NAICS_retail)) + \
        0 * (~data.loc[:, 'NAICS_code'].isin(NAICS_retail))
    data.loc[:, 'Info_val'] = 1 * (data.loc[:, 'NAICS_code'].isin(NAICS_info)) + \
        0 * (~data.loc[:, 'NAICS_code'].isin(NAICS_info))
    data.loc[:, 'TW_val'] = 1 * (data.loc[:, 'NAICS_code'].isin(NAICS_tw)) + \
        0 * (~data.loc[:, 'NAICS_code'].isin(NAICS_tw))           
    # assign value density
    # data = pd.merge(data, value_density_lookup, on = 'SCTG_Group', how = 'left')
    data.loc[:, 'value_density'] = data.loc[:, 'UnitCost'] * lb_to_ton
    ##### placeholder for generating alternative specific variables (travel time & cost) ######
    data = pd.merge(data, distance_travel_time_skim, 
                     on = ['orig_FAFID', 'dest_FAFID', 'Alternative'], how = 'left')
    data.loc[:, 'Cost_val'] = 0
    # 1. rail cost
    data.loc[data['Alternative'] == 'Rail/IMX', 'Cost_val'] = rail_cost * data.loc[data['Alternative'] == 'Rail/IMX', 'TruckLoad'] * \
        data.loc[data['Alternative'] == 'Rail/IMX','Distance']
    data.loc[(data['Alternative'] == 'Rail/IMX') & (data['Cost_val'] < 200), 'Cost_val'] = 200
    # 2. air cost
    data.loc[data['Alternative'] == 'Air', 'Cost_val'] = 55 + 1.08 * (data.loc[data['Alternative'] == 'Air','TruckLoad'] > 0.05) * \
        (data.loc[data['Alternative'] == 'Air','TruckLoad'] - 0.05) / lb_to_ton
    # 3. truck cost
    truck_modes = ['Private Truck Fuel', 'Private Truck Elec',
                           'For-hire Truck Fuel', 'For-hire Truck Elec']
    data.loc[data['Alternative'] == 'Private Truck Fuel', 'Cost_val'] = fuel_cost * \
        data.loc[data['Alternative'] == 'Private Truck Fuel', 'Distance']
    data.loc[data['Alternative'] == 'Private Truck Elec', 'Cost_val'] = elec_cost * \
        data.loc[data['Alternative'] == 'Private Truck Elec', 'Distance']
    data.loc[data['Alternative'] == 'For-hire Truck Fuel', 'Cost_val'] = fuel_cost * \
        data.loc[data['Alternative'] == 'For-hire Truck Fuel', 'Distance']  
    data.loc[data['Alternative'] == 'For-hire Truck Elec', 'Cost_val'] = elec_cost * \
        data.loc[data['Alternative'] == 'For-hire Truck Elec', 'Distance']   
    data.loc[(data['Alternative'].isin(truck_modes)) & (data['Cost_val'] < 10), 'Cost_val'] = 10
    
    # 4. parcel
    data.loc[data['Alternative'] == 'Parcel', 'Cost_val'] =  3.58 + 0.015 * data.loc[data['Alternative'] == 'Parcel', 'TruckLoad'] / lb_to_ton
    # 4. parcel cost
    
    ###### assign mode availability ######
    data.loc[:, 'mode_available'] = 1
    data.loc[(data['Alternative'] == 'Air') & (data['TruckLoad'] > 7.5), 'mode_available'] = 0 
    data.loc[(data['Alternative'] == 'Parcel') & (data['TruckLoad'] > 0.15), 'mode_available'] = 0  
    data.loc[(data['Alternative'] == 'Private Truck Fuel') & (data['Distance'] > 500), 'mode_available'] = 0 
    data.loc[(data['Alternative'] == 'Private Truck Elec') & (data['Distance'] > 500), 'mode_available'] = 0 
    data.loc[(data['Distance'].isna()) | (data['Travel_time'].isna()), 'mode_available'] = 0         
    return(data)        

def mode_choice_utility_generator(data, mode_choice_param): # compute utility and probability by mode
    data = pd.merge(data, mode_choice_param, on = 'Alternative', how = 'left')
    data.loc[:, 'Utility'] = data.loc[:, 'constant'] * data.loc[:, 'Const'] + \
    data.loc[:, 'weight_bin'] * data.loc[:, 'Weight'] + \
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
    data.loc[:, 'Utility_exp'] = np.exp(data.loc[:, 'Utility'])
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

def process_chunk(args):
    modeled_OD_by_sctg, i, fuel_cost, elec_cost, rail_cost, output_path, sctg, file_name = args

    print('process chunk id ' + str(i))
    # select domestic shipment

    modeled_OD_by_sctg['NAICS_code'] = modeled_OD_by_sctg.SellerNAICS.astype(str).str[:2] # generate 2-digit NAICS code
    modeled_OD_by_sctg['NAICS_code'] = modeled_OD_by_sctg['NAICS_code'].astype(int)
    modeled_OD_by_sctg["shipment_id"] = modeled_OD_by_sctg.index + 1 # generate shipment ID for matching
    modeled_OD_by_sctg = pd.concat([modeled_OD_by_sctg, pd.DataFrame(columns = list_of_alternative)], sort=False) # append mode choice
    modeled_OD_by_sctg.loc[:, list_of_alternative] = 1


    # convert data to long format, generate variables for mode choice
    modeled_OD_by_sctg_long = pd.melt(modeled_OD_by_sctg, id_vars=['BuyerID', 'BuyerZone', 'BuyerNAICS', 'SellerID', 'SellerZone',
        'SellerNAICS', 'TruckLoad', 'SCTG_Group', 'NAICS_code', 'shipment_id', 'orig_FAFID', 'dest_FAFID', 'UnitCost'],
        value_vars=list_of_alternative,
        var_name='Alternative', value_name='constant')  # convert wide dataframe to long
    # modeled_OD_by_sctg_long.loc[modeled_OD_by_sctg_long['TruckLoad'] > c.max_shipment_load, 'TruckLoad'] = c.max_shipment_load
    ##### generate variables for mode choice model #####
    modeled_OD_by_sctg_long = choice_model_variable_generator(modeled_OD_by_sctg_long, fuel_cost, elec_cost, rail_cost)
    ##### compute utilities and probabilities #####
    print('start mode choice generation')
    mode_choice_results = mode_choice_utility_generator(modeled_OD_by_sctg_long, mode_choice_param)
    mode_choice_results.loc[:, 'mode_choice'] = mode_choice_results.apply(
    lambda row: mode_choice_simulator(list_of_alternative, row[list_of_alternative]),axis=1)
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
                                                'SellerNAICS', 'TruckLoad', 'SCTG_Group', 'NAICS_code',
                                                'shipment_id', 'orig_FAFID', 'dest_FAFID', 'mode_choice',
                                                'probability', 'Distance', 'Travel_time']]
    int_var = ['BuyerID', 'BuyerZone', 'SellerID', 'SellerZone', 'SCTG_Group', 'NAICS_code', 'shipment_id',
    'orig_FAFID', 'dest_FAFID']
    modeled_OD_by_sctg.loc[:, int_var] = modeled_OD_by_sctg.loc[:, int_var].astype(int)
    float_var = ['TruckLoad', 'probability', 'Distance', 'Travel_time']
    modeled_OD_by_sctg.loc[:, float_var] = modeled_OD_by_sctg.loc[:, float_var].astype(float)
    i += 1
    # combined_modeled_OD_by_sctg = pd.concat([combined_modeled_OD_by_sctg, modeled_OD_by_sctg])
    # break
    # cut_off_point = 1000 * c.max_ton_lookup[sctg]
    # combined_modeled_OD_by_sctg.loc[combined_modeled_OD_by_sctg['TruckLoad'] > cut_off_point, 'TruckLoad'] = cut_off_point


    ##### writing output ######
    # combined_modeled_OD_by_sctg.to_csv(c.input_dir + 'sample_mode_choice.csv')
    final_path = output_path + '/' + sctg
    isExist = os.path.exists(final_path)
    if not isExist:
        os.makedirs(final_path)
            
               # Create a new directory because it does not exist
               
    modeled_OD_by_sctg.to_csv(final_path + '/reassigned_' + file_name + '_' + str(i) + '.zip') # writing output

def main():
    # aggregated output data
    # mode_choide_by_commodity = None
    chunk_size = 10 ** 6
    njob=0
    fuel_scenario = ['HOP', 'Ref']
    elec_scenario = ['p2', 'p4', 'p6', 'p8', 'p10']
    for fs in fuel_scenario:
        for es in elec_scenario:
            print('processing scenario ' + fs + ' for fuel and ' + es + ' for elec')
            scenario_name = fs + '_' + es
            output_path = output_dir + scenario_name
            # Check whether the specified path exists or not
            isExist = os.path.exists(output_path)
            if not isExist:
            
               # Create a new directory because it does not exist
               os.makedirs(output_path)
    
            row_index = (op_cost_by_scenario['Diesel_Scenario'] == fs) & \
                        (op_cost_by_scenario['Elec_Scenario'] == es)
            fuel_cost = np.round(op_cost_by_scenario.loc[row_index, 'Diesel_TC'], 2) 
            elec_cost = np.round(op_cost_by_scenario.loc[row_index, 'Elec_TC'], 2) 
            rail_cost = np.round(op_cost_by_scenario.loc[row_index, 'Rail_TC'], 2)                         
    ###### processing OD flow and generate mode choice ##########
            for sctg in list_of_sctg_group:
                if sys.argv[-1].startswith("sctg"):
                    if sctg!=sys.argv[-1]: continue
                print(sctg)
                # sctg_id = int(sctg[4])
                file_start = 'shipment_' + sctg
                filelist = sorted([file for file in os.listdir(output_dir) if file.startswith(file_start)])
                i = 0
                print(len(filelist))
                for f in filelist:
                    print(f)
                    all_modeled_OD_by_sctg = read_csv(output_dir + f, low_memory=False) 
                    all_modeled_OD_by_sctg.reset_index(inplace = True)
                    chunks_of_flows = split_dataframe(all_modeled_OD_by_sctg, chunk_size)
                    # OD_file_to_load = sctg + '_OD.csv'
                    file_name = sctg + '_OD'
                    jobs=[]
                    for modeled_OD_by_sctg in chunks_of_flows:
                        jobs.append( (modeled_OD_by_sctg, i, fuel_cost, elec_cost, rail_cost, output_path, sctg, file_name))
                        i+=1
            
                    print(len(jobs))
                    njob+=len(jobs)
                    pl=Pool(4)
                    pl.map(process_chunk, jobs)
                    # for j in jobs: process_chunk(j)
            
                    # shutil.move(output_dir + f, output_path + f)
        #             break
        #         break
        #     break
        # break
         
        print("total jobs", njob)

if __name__ == '__main__':
    main()