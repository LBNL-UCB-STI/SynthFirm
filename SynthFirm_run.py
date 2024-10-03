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
# import rpy2
# import rpy2.robjects as robjects
import subprocess

# import SynthFirm modules
from utils.Step1_Firm_Generation import synthetic_firm_generation
from utils.Step2_Producer_Generation import producer_generation
from utils.Step3_Consumer_Generation import consumer_generation
from utils.Step4_production_and_consumption_forecast import prod_cons_demand_forecast
from utils.Step5_Firm_Location_Generation import firm_location_generation
from utils.Step6_Supplier_Selection import supplier_selection
from utils.Step7_Shipment_Size_Generation import shipment_size_generation
from utils.Step8_Freight_Mode_Choice_Model import mode_choice_model
from utils.Step9_Post_Process_B2B_Flow import post_mode_choice
from utils.Step13_international_shipment import international_demand_generation
from utils.Step14_international_mode_assignment import international_mode_choice
from utils.Step15_international_B2B_flow_generator import domestic_receiver_assignment

warnings.filterwarnings("ignore")

def main():
    des = """
    SynthFirm Business-to-business (B2B) flow generation"
    """
    parser = argparse.ArgumentParser(description=des)
    parser.add_argument("--config", type = str, help = "config file name", default= 'configs/Seattle_2030.conf')
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
    number_of_processes = config['ENVIRONMENT'].get('number_of_processes')
    number_of_processes = int(number_of_processes) if number_of_processes else 0
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
        
    run_producer_consumer_generation = config.getboolean('ENVIRONMENT', 'enable_producer_consumer_generation')
    if run_producer_consumer_generation:
        print('including synthetic producer/consumer generation in the pipeline...')
    
    run_demand_forecast = config.getboolean('ENVIRONMENT', 'enable_demand_forecast')
    # check if this is a forecast run
    if run_demand_forecast:
        forecast_year = config['ENVIRONMENT']['forecast_year']
        #print(print(type(forecast_year)))
        print('including demand forecast in the pipeline and forecast year is ' + forecast_year + '...')
        

    enable_firm_loc_generation = config.getboolean('ENVIRONMENT', 'enable_firm_loc_generation')
    if enable_firm_loc_generation:
        print('including firm location generation in the pipeline...')
        
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
        
    run_international_flow = config.getboolean('ENVIRONMENT', 'enable_international_flow') 
    if run_international_flow:
            print('including international flow generation in the pipeline...')
    
    # load inputs  
    
    # inputs/outputs first appear in firm synthesizer
    cbp_file = os.path.join(input_path, config['INPUTS']['cbp_file'])
    mzemp_file = os.path.join(input_path, config['INPUTS']['mzemp_file'])
    
    c_n6_n6io_sctg_file = os.path.join(param_path, config['PARAMETERS']['c_n6_n6io_sctg_file'])
    employment_per_firm_file = os.path.join(param_path, config['PARAMETERS']['employment_per_firm_file'])
    employment_per_firm_gapfill_file = os.path.join(param_path, config['PARAMETERS']['employment_per_firm_gapfill_file'])
    
    synthetic_firms_no_location_file = os.path.join(output_path, config['OUTPUTS']['synthetic_firms_no_location_file'])

    
    # inputs/outputs first appear in producer and consumer generation
    BEA_io_2017_file = os.path.join(param_path, config['PARAMETERS']['BEA_io_2017_file'])
    agg_unit_cost_file = os.path.join(param_path, config['PARAMETERS']['agg_unit_cost_file'])
    prod_by_zone_file = os.path.join(param_path, config['PARAMETERS']['prod_by_zone_file'])
    cons_by_zone_file = os.path.join(param_path, config['PARAMETERS']['cons_by_zone_file'])
    sctg_group_file = os.path.join(param_path, config['PARAMETERS']['sctg_group_file'])
    
    io_summary_file = os.path.join(output_path, config['OUTPUTS']['io_summary_file'])
    wholesaler_file = os.path.join(output_path, config['OUTPUTS']['wholesaler_file'])
    producer_file = os.path.join(output_path, config['OUTPUTS']['producer_file'])
    producer_by_sctg_filehead = os.path.join(output_path, config['OUTPUTS']['producer_by_sctg_filehead'])
    consumer_file = os.path.join(output_path, config['OUTPUTS']['consumer_file'])
    consumer_by_sctg_filehead = os.path.join(output_path, config['OUTPUTS']['consumer_by_sctg_filehead'])
    sample_consumer_file = os.path.join(output_path, config['OUTPUTS']['sample_consumer_file'])
    io_filtered_file = os.path.join(output_path, config['OUTPUTS']['io_filtered_file'])
    
    mesozone_to_faf_file = os.path.join(input_path, config['INPUTS']['mesozone_to_faf_file'])
    shipment_by_distance_file = os.path.join(param_path, config['PARAMETERS']['shipment_by_distance_bin_file'])    
    shipment_distance_lookup_file = os.path.join(param_path, config['PARAMETERS']['shipment_distance_lookup_file'])
    cost_by_location_file = os.path.join(param_path, config['PARAMETERS']['cost_by_location_file'])
    
    # inputs/outputs first appear in demand forecast 
    if run_demand_forecast:
        prod_forecast_name = config['PARAMETERS']['prod_forecast_filehead'] + forecast_year + '.csv'
        prod_forecast_file = os.path.join(param_path, prod_forecast_name)
        cons_forecast_name = config['PARAMETERS']['cons_forecast_filehead'] + forecast_year + '.csv'
        cons_forecast_file = os.path.join(param_path, cons_forecast_name)
    #inputs/outputs first appear in firm location generation
    spatial_boundary_file_fileend = config['INPUTS']['spatial_boundary_file_fileend']
    spatial_boundary_file_name = scenario_name + spatial_boundary_file_fileend
    spatial_boundary_file = os.path.join(input_path, spatial_boundary_file_name)
    synthetic_firms_with_location_file = os.path.join(output_path, 
                                                      config['OUTPUTS']['synthetic_firms_with_location_file'])
    zonal_output_fileend = config['OUTPUTS']['zonal_output_fileend']
    zonal_output_file = os.path.join(output_path, scenario_name + zonal_output_fileend)

    cfs_to_faf_file = os.path.join(param_path, config['PARAMETERS']['cfs_to_faf_file'])
    max_load_per_shipment_file = os.path.join(param_path, config['PARAMETERS']['max_load_per_shipment_file'])
    
    supplier_selection_param_file = os.path.join(param_path, config['PARAMETERS']['supplier_selection_param_file'])
    mode_choice_param_file = os.path.join(input_path, config['INPUTS']['mode_choice_param_file'])
    distance_travel_skim_file = os.path.join(param_path, config['PARAMETERS']['distance_travel_skim_file'])
    
    # input/output appear in international flow
    need_domestic_adjustment = config.getboolean('INPUTS', 'need_domestic_adjustment') 
    if need_domestic_adjustment:
        location_from_str = config['INPUTS']['location_from']
        location_from = [int(num) for num in location_from_str.split(',')]
        location_to_str = config['INPUTS']['location_to']
        location_to = [int(num) for num in location_to_str.split(',')]
    regional_import_file = os.path.join(input_path, 'port', config['INPUTS']['regional_import_file'])
    regional_export_file = os.path.join(input_path, 'port', config['INPUTS']['regional_export_file'])
    port_level_import_file = os.path.join(input_path, 'port', config['INPUTS']['port_level_import_file'])
    port_level_export_file = os.path.join(input_path, 'port', config['INPUTS']['port_level_export_file'])
    int_shipment_size_file = os.path.join(param_path,  config['PARAMETERS']['int_shipment_size_file'])
    sctg_by_port_file = os.path.join(param_path, config['PARAMETERS']['sctg_by_port_file'])
    
    import_od = os.path.join(output_path, config['OUTPUTS']['import_od'])
    export_od = os.path.join(output_path, config['OUTPUTS']['export_od'])
    
    int_mode_choice_file = os.path.join(input_path, config['INPUTS']['int_mode_choice_file'])
    import_mode_file = os.path.join(output_path, 'international', config['OUTPUTS']['import_mode_file'])
    export_mode_file = os.path.join(output_path, 'international', config['OUTPUTS']['export_mode_file'])
    
    export_with_firm_file = os.path.join(output_path, 'international', config['OUTPUTS']['export_with_firm_file'])
    import_with_firm_file = os.path.join(output_path, 'international', config['OUTPUTS']['import_with_firm_file'])
    
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
    
    # rail_unit_cost_per_tonmile = 0.039
    # rail_min_cost = 200
    # air_unit_cost_per_lb = 1.08
    # air_min_cost = 55
    # truck_unit_cost_per_tonmile_sm = 2.83
    # truck_unit_cost_per_tonmile_md = 0.5
    # truck_unit_cost_per_tonmile_lg = 0.18
    # truck_min_cost = 10
    # parcel_cost_coeff_a = 3.58
    # parcel_cost_coeff_b = 0.015
    # parcel_cost_max = 1000
    rail_unit_cost_per_tonmile = float(config['MC_CONSTANTS']['rail_unit_cost_per_tonmile'])
    mode_choice_spec['rail_unit_cost'] = rail_unit_cost_per_tonmile
    
    rail_min_cost = float(config['MC_CONSTANTS']['rail_min_cost'])
    mode_choice_spec['rail_min_cost'] = rail_min_cost
    
    air_unit_cost_per_lb = float(config['MC_CONSTANTS']['air_unit_cost_per_lb'])
    mode_choice_spec['air_unit_cost'] = air_unit_cost_per_lb
    
    air_min_cost = float(config['MC_CONSTANTS']['air_min_cost'])
    mode_choice_spec['air_min_cost'] = air_min_cost
    
    truck_unit_cost_per_tonmile_sm = float(config['MC_CONSTANTS']['truck_unit_cost_per_tonmile_sm'])
    mode_choice_spec['truck_unit_cost_sm'] = truck_unit_cost_per_tonmile_sm
    
    truck_unit_cost_per_tonmile_md = float(config['MC_CONSTANTS']['truck_unit_cost_per_tonmile_md'])
    mode_choice_spec['truck_unit_cost_md'] = truck_unit_cost_per_tonmile_md
    
    truck_unit_cost_per_tonmile_lg = float(config['MC_CONSTANTS']['truck_unit_cost_per_tonmile_lg'])
    mode_choice_spec['truck_unit_cost_lg'] = truck_unit_cost_per_tonmile_lg
    
    truck_min_cost = float(config['MC_CONSTANTS']['truck_min_cost'])
    mode_choice_spec['truck_min_cost'] = truck_min_cost
    
    parcel_cost_coeff_a = float(config['MC_CONSTANTS']['parcel_cost_coeff_a'])
    mode_choice_spec['parcel_cost_coeff_a'] = parcel_cost_coeff_a
    
    parcel_cost_coeff_b = float(config['MC_CONSTANTS']['parcel_cost_coeff_b'])
    mode_choice_spec['parcel_cost_coeff_b'] = parcel_cost_coeff_b
    
    parcel_max_cost = float(config['MC_CONSTANTS']['parcel_max_cost'])
    mode_choice_spec['parcel_max_cost'] = parcel_max_cost
    
    # print(mode_choice_spec)
    print('SynthFirm run for ' + scenario_name + ' start!')
    print('----------------------------------------------')

    ##### Step 1 -  synthetic firm generation
    if run_firm_generation:
        
        # subprocess.call ("Rscript --vanilla utils/run_firm_generation_master_R.R", shell=True)
        synthetic_firm_generation(cbp_file, mzemp_file, c_n6_n6io_sctg_file, 
                                  employment_per_firm_file, employment_per_firm_gapfill_file, 
                                  synthetic_firms_no_location_file, output_path)

    ##### Steps 2 and 3 -  synthetic producer and consumer generation        
    if run_producer_consumer_generation:
        # producer generation
        
        # wholesale cost factor is the ratio between wholesale output/input (1+revenue/cost)
        wholesalecostfactor = producer_generation(c_n6_n6io_sctg_file, synthetic_firms_no_location_file,
                                mesozone_to_faf_file, BEA_io_2017_file, agg_unit_cost_file,
                                prod_by_zone_file, sctg_group_file, io_summary_file,
                                wholesaler_file, producer_file, producer_by_sctg_filehead,
                                io_filtered_file, output_path)
        # consumer generation
        consumer_generation(synthetic_firms_no_location_file, mesozone_to_faf_file,
                                c_n6_n6io_sctg_file, agg_unit_cost_file, cons_by_zone_file,
                                sctg_group_file, wholesaler_file,
                                producer_file, io_filtered_file, consumer_file,
                                sample_consumer_file, consumer_by_sctg_filehead, 
                                wholesalecostfactor, output_path)
    
    ##### Steps 4 (optional) -  run demand forecast       
    if run_demand_forecast:
        prod_cons_demand_forecast(forecast_year, synthetic_firms_no_location_file,
                                      producer_file, consumer_file, prod_forecast_file,
                                      cons_forecast_file, mesozone_to_faf_file, sctg_group_file,
                                      consumer_by_sctg_filehead, output_path)
    
    ##### Step 5 -  synthetic firm location generation
    if enable_firm_loc_generation:
        firm_location_generation(synthetic_firms_no_location_file,
                                     synthetic_firms_with_location_file,
                                     zonal_output_file,
                                     spatial_boundary_file, output_path)
    
    
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
                          output_path, number_of_processes)
        
    ##### Step 9 - post mode choice analysis and result summary
    if run_post_analysis:
        post_mode_choice(sctg_group_file, mesozone_to_faf_file, 
                     output_path, region_code)
    

    
    ###### placeholder for validation and fleet generation
    
    ###### Step 13 -- international shipment
    if run_international_flow:
        
        # international commodity flow
        if need_domestic_adjustment:
            print('Use international flow generation with destination adjustment...')
            
            international_demand_generation(c_n6_n6io_sctg_file, sctg_by_port_file,
                                                sctg_group_file, int_shipment_size_file,
                                                regional_import_file, regional_export_file, 
                                                port_level_import_file, port_level_export_file,
                                                need_domestic_adjustment, import_od, export_od, 
                                                output_path, 
                                                location_from, location_to)
        else: 
            print('Use international flow generation without destination adjustment...')
            international_demand_generation(c_n6_n6io_sctg_file, sctg_by_port_file,
                                                sctg_group_file, int_shipment_size_file,
                                                regional_import_file, regional_export_file, 
                                                port_level_import_file, port_level_export_file,
                                                need_domestic_adjustment, import_od, export_od, 
                                                output_path)
            
        # international mode choice
        international_mode_choice(int_mode_choice_file, distance_travel_skim_file,
                                  import_od, export_od, import_mode_file, export_mode_file,
                                  mode_choice_spec, output_path)
        
        # domestic receiver assignment
        domestic_receiver_assignment(consumer_file, producer_file, mesozone_to_faf_file,
                                 sctg_group_file, import_mode_file, export_mode_file,
                                 export_with_firm_file, 
                                 import_with_firm_file, output_path)
            
            
    print('SynthFirm run for ' + scenario_name + ' finished!')
    print('All outputs are under ' + output_path)
    print('-------------------------------------------------')
    return
if __name__ == '__main__':
	main()

