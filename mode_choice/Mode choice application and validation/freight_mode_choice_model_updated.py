#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug  5 11:30:01 2021

@author: xiaodanxu
"""
from pandas import read_csv
import pandas as pd
import numpy as np
import constants as c
#import math

#define parameter file names
mode_choice_param_file = 'freight_mode_choice_5alt_calib.csv'


# load parameter
mode_choice_param = read_csv(c.param_dir + mode_choice_param_file, sep = ',')
mesozone_lookup = read_csv(c.param_dir + c.mesozone_id_lookup_file, sep = ',')
value_density_lookup = read_csv(c.param_dir + c.value_density_file, sep = ',')
distance_travel_time_skim = read_csv(c.param_dir + c.distance_travel_skim_file, sep = ',')
list_of_alternative = mode_choice_param['Alternative'].tolist()
chunk_size = 10 ** 6  # process large data by chunk

###### Functions used for mode choice variable generation and simulation #####

def model_od_processor(data, mesozone_lookup):   # assign OD FAF zone ID
    data = pd.merge(data, mesozone_lookup, left_on = 'SellerZone', right_on = 'MESOZONE', how = 'left')
    data = data.rename(columns={"GEOID": "orig_GEOID", "CBPZONE": "orig_CBPZONE", 
                                                "MESOZONE":"orig_MESOZONE", "FAFID":"orig_FAFID",
                                               "FAFNAME":"orig_FAFNAME"})
    data = pd.merge(data, mesozone_lookup, left_on = 'BuyerZone', right_on = 'MESOZONE', how = 'left')
    data = data.rename(columns={"GEOID": "dest_GEOID", "CBPZONE": "dest_CBPZONE", 
                                                "MESOZONE":"dest_MESOZONE", "FAFID":"dest_FAFID",
                                               "FAFNAME":"dest_FAFNAME"})
    data = data.dropna(subset = ['orig_FAFID', 'dest_FAFID']) # remove shipment outside US
    data.loc[:, 'in_study_area'] = 0
    data.loc[:, 'in_study_area'] = 1 * (data.loc[:, 'orig_FAFID'].isin(c.bay_area_region_code) | \
                                        data.loc[:, 'dest_FAFID'].isin(c.bay_area_region_code))
    return(data)

def choice_model_variable_generator(data):     # generate variables for mode choice model #####
    
    # log weight
    data.loc[:, 'Log_weight'] = np.log(data.loc[:, 'TruckLoad'])
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
    data.loc[:, 'Wholesale_val'] = 1 * (data.loc[:, 'NAICS_code'].isin(c.NAICS_wholesale)) + \
        0 * (~data.loc[:, 'NAICS_code'].isin(c.NAICS_wholesale))  
    data.loc[:, 'Mfr_val'] = 1 * (data.loc[:, 'NAICS_code'].isin(c.NAICS_mfr)) + \
        0 * (~data.loc[:, 'NAICS_code'].isin(c.NAICS_mfr)) 
    data.loc[:, 'Mgt_val'] = 1 * (data.loc[:, 'NAICS_code'].isin(c.NAICS_mgt)) + \
        0 * (~data.loc[:, 'NAICS_code'].isin(c.NAICS_mgt)) 
    data.loc[:, 'Retail_val'] = 1 * (data.loc[:, 'NAICS_code'].isin(c.NAICS_retail)) + \
        0 * (~data.loc[:, 'NAICS_code'].isin(c.NAICS_retail))
    data.loc[:, 'Info_val'] = 1 * (data.loc[:, 'NAICS_code'].isin(c.NAICS_info)) + \
        0 * (~data.loc[:, 'NAICS_code'].isin(c.NAICS_info))
    data.loc[:, 'Mining_val'] = 1 * (data.loc[:, 'NAICS_code'].isin(c.NAICS_mining)) + \
        0 * (~data.loc[:, 'NAICS_code'].isin(c.NAICS_mining))           
    # assign value density
    data = pd.merge(data, value_density_lookup, on = 'SCTG_Group', how = 'left')

    ##### placeholder for generating alternative specific variables (travel time & cost) ######
    data = pd.merge(data, distance_travel_time_skim, on = ['orig_FAFID', 'dest_FAFID', 'Alternative'], how = 'left')
    data.loc[:, 'Cost_val'] = 0
    # 1. rail cost
    data.loc[data['Alternative'] == 'Rail/IMX', 'Cost_val'] = 0.039 * data.loc[data['Alternative'] == 'Rail/IMX', 'TruckLoad'] * \
        data.loc[data['Alternative'] == 'Rail/IMX','Distance']
    data.loc[(data['Alternative'] == 'Rail/IMX') & (data['Cost_val'] < 200), 'Cost_val'] = 200
    # 2. air cost
    data.loc[data['Alternative'] == 'Air', 'Cost_val'] = 55 + 1.08 * (data.loc[data['Alternative'] == 'Air','TruckLoad'] > 0.05) * \
        (data.loc[data['Alternative'] == 'Air','TruckLoad'] - 0.05) / c.lb_to_ton
    # 3. truck cost
    truck_criteria = (data['Alternative'] == 'Private Truck') | (data['Alternative'] == 'For-hire Truck')
    weight_criteria_low = (data['TruckLoad'] < 0.075)
    weight_criteria_medium = (data['TruckLoad'] >= 0.075) & (data['TruckLoad'] < 0.75)
    weight_criteria_high = (data['TruckLoad'] >= 0.75) 
    data.loc[truck_criteria & weight_criteria_low, 'Cost_val'] = 2.83 * data.loc[truck_criteria & weight_criteria_low,'TruckLoad'] * \
        data.loc[truck_criteria & weight_criteria_low, 'Distance']
    data.loc[truck_criteria & weight_criteria_medium, 'Cost_val'] = 0.5 * data.loc[truck_criteria & weight_criteria_medium,'TruckLoad'] * \
        data.loc[truck_criteria & weight_criteria_medium, 'Distance']
    data.loc[truck_criteria & weight_criteria_high, 'Cost_val'] = 0.18 * data.loc[truck_criteria & weight_criteria_high,'TruckLoad'] * \
        data.loc[truck_criteria * weight_criteria_high, 'Distance']   
    data.loc[truck_criteria & (data['Cost_val'] < 10), 'Cost_val'] = 10
    
    # 4. parcel
    data.loc[data['Alternative'] == 'Parcel', 'Cost_val'] =  3.58 + 0.015 * data.loc[data['Alternative'] == 'Parcel', 'TruckLoad'] / c.lb_to_ton
    # 4. parcel cost
    
    ###### assign mode availability ######
    data.loc[:, 'mode_available'] = 1
    # data.loc[(data['Alternative'] == 'Air') & (data['TruckLoad'] > 2.328), 'mode_available'] = 0 
    # data.loc[(data['Alternative'] == 'Parcel') & (data['TruckLoad'] > 0.075), 'mode_available'] = 0  
    # data.loc[(data['Alternative'] == 'Private Truck') & (data['Distance'] > 425), 'mode_available'] = 0 
    data.loc[(data['Distance'].isna()) | (data['Travel_time'].isna()), 'mode_available'] = 0         
    return(data)        

def mode_choice_utility_generator(data, mode_choice_param): # compute utility and probability by mode
    data = pd.merge(data, mode_choice_param, on = 'Alternative', how = 'left')
    data.loc[:, 'Utility'] = data.loc[:, 'constant'] * data.loc[:, 'Const'] + \
    data.loc[:, 'Log_weight'] * data.loc[:, 'Weight'] + \
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
    data.loc[:, 'Mining_val'] * data.loc[:, 'Ind_mining'] 
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

# aggregated output data
mode_choide_by_commodity = None


###### processing OD flow and generate mode choice ##########
for sctg in c.list_of_sctg_group:
    print(sctg)
    # if sctg != 'sctg1':
    #     continue
    sctg_id = int(sctg[4])
    OD_file_to_load = sctg + '_OD.csv'
    file_name = sctg + '_OD'
    combined_modeled_OD_by_sctg = None
    i = 0
    for modeled_OD_by_sctg in read_csv(c.input_dir + OD_file_to_load, sep = ',', chunksize = chunk_size, low_memory = False):
        print('process chunk id ' + str(i))        
        # select domestic shipment
        domestic_zones = mesozone_lookup['MESOZONE'].unique()
        modeled_OD_by_sctg = modeled_OD_by_sctg.loc[modeled_OD_by_sctg['SellerZone'].isin(domestic_zones)]
        modeled_OD_by_sctg = modeled_OD_by_sctg.loc[modeled_OD_by_sctg['BuyerZone'].isin(domestic_zones)]
        if len(modeled_OD_by_sctg) == 0:
            continue
        modeled_OD_by_sctg.loc[:, 'TruckLoad'] *= c.lb_to_ton  #convert to ton, match the unit in mode choice spec        
        modeled_OD_by_sctg['NAICS_code'] = modeled_OD_by_sctg.SellerNAICS.astype(str).str[:2] # generate 2-digit NAICS code
        modeled_OD_by_sctg['NAICS_code'] = modeled_OD_by_sctg['NAICS_code'].astype(int)
        modeled_OD_by_sctg["shipment_id"] = modeled_OD_by_sctg.index + 1 # generate shipment ID for matching
        modeled_OD_by_sctg = pd.concat([modeled_OD_by_sctg, pd.DataFrame(columns = list_of_alternative)], sort=False) # append mode choice
        modeled_OD_by_sctg.loc[:, list_of_alternative] = 1
        modeled_OD_by_sctg = model_od_processor(modeled_OD_by_sctg, mesozone_lookup)
        
        # convert data to long format, generate variables for mode choice
        modeled_OD_by_sctg_long = pd.melt(modeled_OD_by_sctg, id_vars=['BuyerID', 'BuyerZone', 'BuyerNAICS', 'SellerID', 'SellerZone',
           'SellerNAICS', 'TruckLoad', 'SCTG_Group', 'NAICS_code', 'shipment_id', 'orig_FAFID', 'dest_FAFID'], value_vars=list_of_alternative,
            var_name='Alternative', value_name='constant')  # convert wide dataframe to long       
        # modeled_OD_by_sctg_long.loc[modeled_OD_by_sctg_long['TruckLoad'] > c.max_shipment_load, 'TruckLoad'] = c.max_shipment_load
        ##### generate variables for mode choice model #####             
        modeled_OD_by_sctg_long = choice_model_variable_generator(modeled_OD_by_sctg_long)  
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
        modeled_OD_by_sctg = modeled_OD_by_sctg.loc[:, ['BuyerID', 'BuyerZone', 'BuyerNAICS', 'SellerID', 'SellerZone',
            'SellerNAICS', 'TruckLoad', 'SCTG_Group', 'NAICS_code', 
            'shipment_id', 'orig_FAFID', 'dest_FAFID', 'in_study_area']]
        modeled_OD_by_sctg = pd.merge(modeled_OD_by_sctg, mode_choice_results, on = 'shipment_id', how = 'left')
        modeled_OD_by_sctg = pd.merge(modeled_OD_by_sctg, distance_travel_time_skim, left_on = ['orig_FAFID', 'dest_FAFID', 'mode_choice'], 
                                      right_on = ['orig_FAFID', 'dest_FAFID', 'Alternative'], how = 'left')
        # combined_modeled_OD_by_sctg = pd.concat([combined_modeled_OD_by_sctg, modeled_OD_by_sctg])
        # break
        # cut_off_point = 1000 * c.max_ton_lookup[sctg]
        # combined_modeled_OD_by_sctg.loc[combined_modeled_OD_by_sctg['TruckLoad'] > cut_off_point, 'TruckLoad'] = cut_off_point


        ##### writing output ######
        # combined_modeled_OD_by_sctg.to_csv(c.input_dir + 'sample_mode_choice.csv')
        modeled_OD_by_sctg.to_csv(c.input_dir + sctg + '/reassigned_' + file_name + '_' + str(i) + '.zip', compression = 'zip') # writing output
        
        # truck_OD_by_sctg = combined_modeled_OD_by_sctg.loc[combined_modeled_OD_by_sctg['mode_choice'].isin(['For-hire Truck', 'Private Truck'])]
        # truck_OD_by_sctg.to_csv(c.input_dir + 'truck_only_' + OD_file_to_load, sep = ',', index = False) # writing truck only results for FRISM
        
        ##### generate summary table #####
        modeled_OD_by_sctg = modeled_OD_by_sctg.dropna()
        modeled_OD_by_sctg.loc[:, 'in_study_area'] = modeled_OD_by_sctg.loc[:, 'in_study_area'].astype(str)
        modeled_OD_by_sctg.loc[:, 'SCTG_Group'] = modeled_OD_by_sctg.loc[:, 'SCTG_Group'].astype(str)
        
        mode_choice_aggregated = modeled_OD_by_sctg.groupby(['mode_choice', 'SCTG_Group', 'in_study_area'])[['shipment_id']].count()
        mode_choice_aggregated = mode_choice_aggregated.reset_index()
        modeled_OD_by_sctg.loc[:, 'tonmile'] = modeled_OD_by_sctg.loc[:, 'TruckLoad'] * modeled_OD_by_sctg.loc[:, 'Distance']
        distance_and_weight_aggregated = modeled_OD_by_sctg.groupby(['mode_choice', 'SCTG_Group', 'in_study_area'])[['TruckLoad', 'tonmile']].sum()
        max_distance_aggregated = modeled_OD_by_sctg.groupby(['mode_choice', 'SCTG_Group', 'in_study_area'])[['Distance']].max()
        max_distance_aggregated.columns = ['max distance']
        max_distance_aggregated = max_distance_aggregated.reset_index()
        distance_and_weight_aggregated = distance_and_weight_aggregated.reset_index()
        
        mode_choice_aggregated = pd.merge(mode_choice_aggregated, distance_and_weight_aggregated, 
                                          on = ['mode_choice', 'SCTG_Group', 'in_study_area'], how = 'left')
        mode_choice_aggregated = pd.merge(mode_choice_aggregated, max_distance_aggregated, 
                                          on = ['mode_choice', 'SCTG_Group', 'in_study_area'], how = 'left')
        mode_choice_aggregated.loc[:, 'Distance'] =  mode_choice_aggregated.loc[:, 'tonmile']/ mode_choice_aggregated.loc[:, 'TruckLoad']
        mode_choice_aggregated.loc[:, 'SCTG_Group'] = c.sctg_def[sctg]
        mode_choice_aggregated.loc[:, 'chunk_id'] = i
        mode_choide_by_commodity = pd.concat([mode_choide_by_commodity, mode_choice_aggregated])
        i += 1
        # break
mode_choide_by_commodity.to_csv(c.output_dir + 'mode_choice_calibration_new_parameter.csv')