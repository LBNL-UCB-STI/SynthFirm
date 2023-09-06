#!/usr/bin/env python
# Xiaodan Xu 08-22-2023
import argparse
from pandas import read_csv
import pandas as pd
import numpy as np
import os
import gc
import warnings
import configparser
from sklearn.utils import shuffle
import rpy2
# import rpy2.robjects as robjects
import subprocess

# import SynthFirm modules
from utils.Step6_Supplier_Selection import supplier_selection
from utils.Step7_Shipment_Size_Generation import shipment_size_generation
from utils.Step8_Freight_Mode_Choice_Model import mode_choice_model
from utils.Step9_Post_Process_B2B_Flow import post_mode_choice

warnings.filterwarnings("ignore")

def main():
    des = """
SynthFirm Business-to-business (B2B) flow generation"
    """
    parser = argparse.ArgumentParser(description=des)
    parser.add_argument("--config", type = str, help = "config file name", default= 'SynthFirm.conf')
    # parser.add_argument("--param1", type=str,help="111", default="abc.aaa")
    # parser.add_argument("--verbose", action='store_true', help="print more stuff")
    options = parser.parse_args()

    # if options.verbose:
    #     print("MeowMeowMeow~~~~")
        
    
    # print(des)

    # load config
    conf_file = options.config
    config = configparser.ConfigParser()
    config.read(conf_file)
    # print(config['ENVIRONMENT']['file_path'])
    scenario_name = config['ENVIRONMENT']['scenario_name']
    out_scenario_name = config['ENVIRONMENT']['out_scenario_name']
    file_path = config['ENVIRONMENT']['file_path']
    parameter_dir = config['ENVIRONMENT']['parameter_path']
    input_dir = 'inputs_' + scenario_name
    output_dir = 'outputs_' + out_scenario_name

    input_path = os.path.join(file_path, input_dir)
    output_path = os.path.join(file_path, output_dir)
    param_path = os.path.join(file_path, parameter_dir)
    
    # Get the defined synthFirm regions
    region_code_str = config['ENVIRONMENT']['region_code']
    region_code = [int(num) for num in region_code_str.split(',')]
    # print(region_code_str)

    # load module to run

    run_firm_generation = config.getboolean('ENVIRONMENT', 'enable_firm_generation') 
    if run_firm_generation:
        print('including synthetic firm generation in the pipeline...')
        
    run_supplier_selection = config.getboolean('ENVIRONMENT', 'enable_supplier_selection') 
    if run_supplier_selection:
        print('including supplier selection in the pipeline...')
    
    run_size_generation = config.getboolean('ENVIRONMENT', 'enable_size_generation') 
    if run_size_generation:
        print('including shipment size generation in the pipeline...')
    
    run_mode_choice = config.getboolean('ENVIRONMENT', 'enable_mode_choice') 
    if run_mode_choice:
        print('including mode choice model in the pipeline...')
    
    run_post_analysis = config.getboolean('ENVIRONMENT', 'enable_post_analysis') 
    if run_post_analysis:
        print('including post analysis in the pipeline...')
    
    run_fleet_generation = config.getboolean('ENVIRONMENT', 'enable_fleet_generation') 
    if run_fleet_generation:
        print('including fleet generation in the pipeline...')
    # load inputs    

    mesozone_to_faf_file = os.path.join(input_path, config['INPUTS']['mesozone_to_faf_file'])
    shipment_by_distance_file = os.path.join(param_path, config['INPUTS']['shipment_by_distance_bin_file'])    
    shipment_distance_lookup_file = os.path.join(param_path, config['INPUTS']['shipment_distance_lookup_file'])
    cost_by_location_file = os.path.join(param_path, config['INPUTS']['cost_by_location_file'])
    
    producer_file = os.path.join(output_path, config['INPUTS']['producer_file'])
    consumer_file = os.path.join(output_path, config['INPUTS']['consumer_file'])

    cfs_to_faf_file = os.path.join(param_path, config['INPUTS']['cfs_to_faf_file'])
    max_load_per_shipment_file = os.path.join(param_path, config['INPUTS']['max_load_per_shipment_file'])
    sctg_group_file = os.path.join(param_path, config['INPUTS']['sctg_group_file'])
    supplier_selection_param_file = os.path.join(param_path, config['INPUTS']['supplier_selection_param_file'])
    mode_choice_param_file = os.path.join(input_path, config['INPUTS']['mode_choice_param_file'])
    distance_travel_skim_file = os.path.join(param_path, config['INPUTS']['distance_travel_skim_file'])

    # prepare mode choice specifications
    mode_choice_spec = {} 
    lb_to_ton = float(config['CONSTANTS']['lb_to_ton'])
    mode_choice_spec['lb_to_ton'] = lb_to_ton
    
    NAICS_wholesale_str = config['CONSTANTS']['NAICS_wholesale']
    NAICS_wholesale = [int(num) for num in NAICS_wholesale_str.split(',')]
    mode_choice_spec['NAICS_wholesale'] = NAICS_wholesale
    
    NAICS_mfr_str = config['CONSTANTS']['NAICS_mfr']
    NAICS_mfr = [int(num) for num in NAICS_mfr_str.split(',')]
    mode_choice_spec['NAICS_mfr'] = NAICS_mfr
    
    NAICS_mgt_str = config['CONSTANTS']['NAICS_mgt']
    NAICS_mgt = [int(num) for num in NAICS_mgt_str.split(',')]
    mode_choice_spec['NAICS_mgt'] = NAICS_mgt
    
    NAICS_retail_str = config['CONSTANTS']['NAICS_retail']
    NAICS_retail = [int(num) for num in NAICS_retail_str.split(',')]
    mode_choice_spec['NAICS_retail'] = NAICS_retail

    
    NAICS_info_str = config['CONSTANTS']['NAICS_info']
    NAICS_info = [int(num) for num in NAICS_info_str.split(',')]
    mode_choice_spec['NAICS_info'] = NAICS_info 
    
    NAICS_mining_str = config['CONSTANTS']['NAICS_mining']
    NAICS_mining = [int(num) for num in NAICS_mining_str.split(',')]
    mode_choice_spec['NAICS_mining'] = NAICS_mining 

    NAICS_tw_str = config['CONSTANTS']['NAICS_tw']
    NAICS_tw = [int(num) for num in NAICS_tw_str.split(',')]
    mode_choice_spec['NAICS_tw'] = NAICS_tw      
    
    weight_bin_str = config['CONSTANTS']['weight_bin']
    weight_bin = [float(num) for num in weight_bin_str.split(',')]
    mode_choice_spec['weight_bin'] = weight_bin   
    
    weight_bin_label_str = config['CONSTANTS']['weight_bin_label']
    weight_bin_label = [int(num) for num in weight_bin_label_str.split(',')]
    mode_choice_spec['weight_bin_label'] = weight_bin_label  
    
    # print(mode_choice_spec)
    print('SynthFirm run for ' + scenario_name + ' start!')
    print('----------------------------------------------')

    ##### Step 1 to 5 -  synthetic firm generation
    if run_firm_generation:
        # robjects.r.source("/utils/run_firm_generation_master_R.R", encoding="utf-8")
        subprocess.call ("Rscript --vanilla utils/run_firm_generation_master_R.R", shell=True)

    ##### Step 6 -  supplier selection         
    if run_supplier_selection:
        supplier_selection(mesozone_to_faf_file, shipment_by_distance_file,
                            shipment_distance_lookup_file, cost_by_location_file,
                            producer_file, consumer_file, cfs_to_faf_file,
                            max_load_per_shipment_file, sctg_group_file,
                            supplier_selection_param_file, output_path)

    ##### Step 7 - shipment size generation
    if run_size_generation:
        shipment_size_generation(mesozone_to_faf_file, max_load_per_shipment_file, 
                                  region_code, output_path)
    
    ##### Step 8 - mode choice generation
    if run_mode_choice:
        mode_choice_model(mode_choice_param_file, mesozone_to_faf_file, 
                          distance_travel_skim_file, mode_choice_spec,
                          output_path)
        
    ##### Step 9 - post mode choice analysis and result summary
    if run_post_analysis:
        post_mode_choice(sctg_group_file, mesozone_to_faf_file, 
                     output_path, region_code)
    
    print('SynthFirm run for ' + scenario_name + ' finished!')
    print('All outputs are under ' + output_path)
    print('-------------------------------------------------')
    return


if __name__ == '__main__':
	main()

