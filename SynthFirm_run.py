#!/usr/bin/env python
# Xiaodan Xu 08-22-2023
import argparse
import os
import warnings
import configparser
import sys
import datetime
import utils.visualkit as vk

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
from utils.Step11_firm_fleet_generation import firm_fleet_generator
from utils.Step12_firm_fleet_adjustment_post_mc import firm_fleet_generator_post_mc
from utils.Step13_international_shipment import international_demand_generation
from utils.Step14_international_mode_assignment import international_mode_choice
from utils.Step15_international_B2B_flow_generator import domestic_receiver_assignment
from utils.firm_emp_validation import validate_firm_employment
from utils.commodity_flow_validation import validate_commodity_flow

warnings.filterwarnings("ignore")

# define the class for logging statement
class Logger:
    def __init__(self, logfile):
        self.terminal = sys.stdout
        self.log = open(logfile, "a", encoding="utf-8")

    def write(self, message):
        if message.strip():  # avoid empty lines
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            formatted_message = f"[{timestamp}] {message}"
            self.terminal.write(formatted_message)
            self.log.write(formatted_message)
        else:
            self.terminal.write(message)
            self.log.write(message)

    def flush(self):
        self.terminal.flush()
        self.log.flush()
            

def main():
    des = """
    SynthFirm Business-to-business (B2B) flow generation"
    """
    parser = argparse.ArgumentParser(description=des)

    parser.add_argument("--config", type = str, help = "config file name", 
                        default= 'configs/national_base.conf')

    # parser.add_argument("--param1", type=str,help="111", default="abc.aaa")
    # parser.add_argument("--verbose", action='store_true', help="print more stuff")
    options = parser.parse_args()

    # if options.verbose:
    #     print("MeowMeowMeow~~~~")
        

    # load config
    # print(options.config)
    # f=open(options.config,'r')
    # l=f.readlines()
    # print(l)
    # f.close()
    conf_file = options.config
    config = configparser.ConfigParser()
    config.read(conf_file)
    # print(list(config.keys()))
    print(config['ENVIRONMENT']['file_path'])
    scenario_name = config['ENVIRONMENT']['scenario_name']
    out_scenario_name = config['ENVIRONMENT']['out_scenario_name']
    file_path = config['ENVIRONMENT']['file_path']
    parameter_dir = config['ENVIRONMENT']['parameter_path']
    number_of_processes = config['ENVIRONMENT'].get('number_of_processes')
    number_of_processes = int(number_of_processes) if number_of_processes else 0
    input_dir = 'inputs_' + scenario_name
    output_dir = 'outputs_' + out_scenario_name
    plot_dir = 'plots_' + out_scenario_name

    input_path = os.path.join(file_path, input_dir)
    output_path = os.path.join(file_path, output_dir)
    param_path = os.path.join(file_path, parameter_dir)
    plot_path = os.path.join(file_path, plot_dir)
    
    # preparing run log
    log_filename = datetime.datetime.now().strftime(out_scenario_name + "_run_%Y%m%d.log")
    log_path = os.path.join(output_path, 'log')
    if not os.path.exists(log_path):
        os.mkdir(log_path)
    else:
      print("Log directory exists!")
    logfile = os.path.join(log_path, log_filename)
    sys.stdout = Logger(logfile)
    
    # Get the defined synthFirm regions
    regional_analysis = config.getboolean('ENVIRONMENT', 'regional_analysis') 
    forecast_analysis = config.getboolean('ENVIRONMENT', 'forecast_analysis') 
    port_analysis = config.getboolean('ENVIRONMENT', 'port_analysis') 
    
    if regional_analysis:
        
        region_code_str = config['ENVIRONMENT']['region_code']
        region_code = [int(num) for num in region_code_str.split(',')]
        print(f'Run regional model with selected FAF zones = {region_code}')
        
        focus_region_str = config['VALIDATION']['focus_region']
        focus_region = [int(num) for num in focus_region_str.split(',')]
        print(f'Run model validation with selected FAF zones = {focus_region}')
    else:
        region_code = None
        focus_region = None
        print('Run national-scale model')
    
    if forecast_analysis:
        forecast_year = config['ENVIRONMENT']['forecast_year']
        print(f'Run demand forecast with year = {forecast_year}')
    else:
        forecast_year = '2017'
        print('Run base year 2017 results')


    

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
    
    run_model_validation = config.getboolean('ENVIRONMENT', 'enable_model_validation')
    if run_model_validation:
            print('including model validation in the pipeline...')
            
    # define if regional calibration is needed
    need_regional_calibration = config.getboolean('ENVIRONMENT', 'need_regional_calibration') 
    
    if need_regional_calibration:
        regional_variable_str = config['ENVIRONMENT']['regional_variable']
        regional_variable = [var for var in regional_variable_str.split(',')]
    # load inputs  
    
    # inputs/outputs first appear in firm synthesizer
    cbp_file = os.path.join(input_path, config['INPUTS']['cbp_file'])
    mzemp_file = os.path.join(input_path, config['INPUTS']['mzemp_file'])
    mesozone_to_faf_file = os.path.join(input_path, config['INPUTS']['mesozone_to_faf_file'])
    c_n6_n6io_sctg_file = os.path.join(param_path, config['PARAMETERS']['c_n6_n6io_sctg_file'])
    employment_per_firm_file = os.path.join(param_path, config['PARAMETERS']['employment_per_firm_file'])
    employment_per_firm_gapfill_file = os.path.join(param_path, config['PARAMETERS']['employment_per_firm_gapfill_file'])
    zip_to_tract_file = os.path.join(param_path, config['PARAMETERS']['zip_to_tract_file'])
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
    

    # inputs/outputs first appear in demand forecast (optional)
    if forecast_analysis:
        prod_forecast_name = config['PARAMETERS']['prod_forecast_filehead'] + forecast_year + '.csv'
        prod_forecast_file = os.path.join(param_path, prod_forecast_name)
        cons_forecast_name = config['PARAMETERS']['cons_forecast_filehead'] + forecast_year + '.csv'
        cons_forecast_file = os.path.join(param_path, cons_forecast_name)
    
    # inputs/outputs first appear in firm location generation
    spatial_boundary_file_fileend = config['INPUTS']['spatial_boundary_file_fileend']
    spatial_boundary_file_name = scenario_name + spatial_boundary_file_fileend
    spatial_boundary_file = os.path.join(input_path, spatial_boundary_file_name)
    synthetic_firms_with_location_file = os.path.join(output_path, 
                                                      config['OUTPUTS']['synthetic_firms_with_location_file'])
    zonal_output_fileend = config['OUTPUTS']['zonal_output_fileend']
    zonal_output_file = os.path.join(output_path, scenario_name + zonal_output_fileend)


    # inputs/outputs first appear in supplier selection
    
    shipment_by_distance_file = os.path.join(param_path, config['PARAMETERS']['shipment_by_distance_bin_file'])    
    shipment_distance_lookup_file = os.path.join(param_path, config['PARAMETERS']['shipment_distance_lookup_file'])
    cost_by_location_file = os.path.join(param_path, config['PARAMETERS']['cost_by_location_file'])
    supplier_selection_param_file = os.path.join(param_path, config['PARAMETERS']['supplier_selection_param_file'])
    
    
    # inputs/outputs first appear in shipment size
    cfs_to_faf_file = os.path.join(param_path, config['PARAMETERS']['cfs_to_faf_file'])
    max_load_per_shipment_file = os.path.join(param_path, config['PARAMETERS']['max_load_per_shipment_file'])
    
    # inputs/outputs first appear in mode choice
    mode_choice_param_file = os.path.join(input_path, config['INPUTS']['mode_choice_param_file'])
    distance_travel_skim_file = os.path.join(param_path, config['PARAMETERS']['distance_travel_skim_file'])
        
    # input/outputs first appear in domestic post analysis
    domestic_summary_file = os.path.join(output_path, 
                                         config['OUTPUTS']['domestic_summary_file'])
    domestic_summary_zone_file = os.path.join(output_path, 
                                         config['OUTPUTS']['domestic_summary_zone_file'])

    # input/output appear in fleet generation (optional)
    
    # scenario-specific inputs
    
    
    if run_fleet_generation:
        fleet_year = config['FLEET_IO']['fleet_year']
        fleet_name = config['FLEET_IO']['fleet_name']
        regulations = config['FLEET_IO']['regulations']
        fleet_scenario_name = fleet_name + ' & ' + regulations
        
        # generic inputs
        private_fleet_file = os.path.join(param_path, 'fleet', config['FLEET_IO']['private_fleet_file'])
        for_hire_fleet_file = os.path.join(param_path, 'fleet', config['FLEET_IO']['for_hire_fleet_file'])
        cargo_type_distribution_file = os.path.join(param_path,'fleet',  config['FLEET_IO']['cargo_type_distribution_file'])
        state_fips_lookup_file = os.path.join(param_path, config['FLEET_IO']['state_fips_lookup_file'])
        
        # scenario-dependent inputs
        private_fuel_mix_file = os.path.join(param_path, 'fleet', config['FLEET_IO']['private_fuel_mix_file'])
        hire_fuel_mix_file = os.path.join(param_path, 'fleet', config['FLEET_IO']['hire_fuel_mix_file'])
        lease_fuel_mix_file = os.path.join(param_path, 'fleet', config['FLEET_IO']['lease_fuel_mix_file'])
        private_stock_file = os.path.join(param_path, 'fleet', config['FLEET_IO']['private_stock_file'])
        hire_stock_file = os.path.join(param_path, 'fleet', config['FLEET_IO']['hire_stock_file'])
        lease_stock_file = os.path.join(param_path, 'fleet', config['FLEET_IO']['lease_stock_file'])
        ev_availability_file = os.path.join(param_path, 'fleet', config['FLEET_IO']['ev_availability_file'])
        
        # fleet outputs
        firms_with_fleet_file = os.path.join(output_path, fleet_year, fleet_scenario_name,
                                                       config['FLEET_IO']['firms_with_fleet_file'])
        carriers_with_fleet_file = os.path.join(output_path, fleet_year, fleet_scenario_name,
                                                       config['FLEET_IO']['carriers_with_fleet_file'])
        leasing_with_fleet_file = os.path.join(output_path, fleet_year, fleet_scenario_name,
                                                       config['FLEET_IO']['leasing_with_fleet_file'])
        firms_with_fleet_mc_adj_files = os.path.join(output_path, fleet_year, fleet_scenario_name,
                                                       config['FLEET_IO']['firms_with_fleet_mc_adj_files'])

    # input/output appear in port analysis (optional)
    
    # those files are needed across module (international + validation)
    if port_analysis:
        international_summary_file = os.path.join(output_path, 
                                             config['OUTPUTS']['international_summary_file'])
        international_summary_zone_file = os.path.join(output_path, 
                                             config['OUTPUTS']['international_summary_zone_file'])

    # those files are only needed for international
    if run_international_flow: 
    # input/output appear in international flow
        need_domestic_adjustment = config.getboolean('INPUTS', 'need_domestic_adjustment') 
        if need_domestic_adjustment:
            location_from_str = config['INPUTS']['location_from']
            location_from = [int(num) for num in location_from_str.split(',')]
            location_to_str = config['INPUTS']['location_to']
            location_to = [int(num) for num in location_to_str.split(',')]
        if forecast_analysis and forecast_year != '2017':
            import_forecast_factor = os.path.join(param_path,\
                                                  config['PARAMETERS']['import_forecast_filehead'] + forecast_year + '.csv')
            export_forecast_factor = os.path.join(param_path,\
                                                  config['PARAMETERS']['export_forecast_filehead'] + forecast_year + '.csv')
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
    
    # input/output appear in model validation
    lehd_file = os.path.join(param_path, config['VALIDATION']['lehd_file'])
    us_county_map_file = os.path.join(param_path, config['VALIDATION']['us_county_map_file'])
    faf_data_file = os.path.join(param_path, config['VALIDATION']['faf_data_file'])
    cfs_data_file = os.path.join(param_path, config['VALIDATION']['cfs_data_file'])
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
        synthetic_firm_generation(cbp_file, mzemp_file, mesozone_to_faf_file, c_n6_n6io_sctg_file, 
                                  employment_per_firm_file, employment_per_firm_gapfill_file, 
                                  zip_to_tract_file, synthetic_firms_no_location_file, output_path)

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
                                      mesozone_to_faf_file,
                                      zonal_output_file,
                                      spatial_boundary_file, output_path, number_of_processes)
        
    
    
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
                     output_path, domestic_summary_file, 
                     domestic_summary_zone_file, region_code)
    

    
    ###### placeholder for model calibration 

    ##### Step 11/12 - generate firm-level fleet before and after mode choice
    
    if run_fleet_generation:
        if need_regional_calibration:
            print('Adding calibrated variables under fleet generation')
            firm_fleet_generator(int(fleet_year), fleet_name, regulations,
                                     synthetic_firms_with_location_file, private_fleet_file,
                                     for_hire_fleet_file, cargo_type_distribution_file, state_fips_lookup_file,
                                     private_fuel_mix_file, hire_fuel_mix_file, lease_fuel_mix_file,
                                     private_stock_file, hire_stock_file, lease_stock_file,
                                     firms_with_fleet_file, carriers_with_fleet_file, leasing_with_fleet_file, 
                                     ev_availability_file, output_path, 
                                     need_regional_calibration, regional_variable)

            
            firm_fleet_generator_post_mc(int(fleet_year), fleet_name, regulations, synthetic_firms_with_location_file,
                                     private_fleet_file, private_fuel_mix_file, ev_availability_file, 
                                     firms_with_fleet_file, firms_with_fleet_mc_adj_files, output_path, 
                                     need_regional_calibration, regional_variable)
        else: 
            print('No calibrated variables under fleet generation')
            firm_fleet_generator(int(fleet_year), fleet_name, regulations,
                                     synthetic_firms_with_location_file, private_fleet_file,
                                     for_hire_fleet_file, cargo_type_distribution_file, state_fips_lookup_file,
                                     private_fuel_mix_file, hire_fuel_mix_file, lease_fuel_mix_file,
                                     private_stock_file, hire_stock_file, lease_stock_file,
                                     firms_with_fleet_file, carriers_with_fleet_file, leasing_with_fleet_file, 
                                     ev_availability_file, output_path, need_regional_calibration)

            
            firm_fleet_generator_post_mc(int(fleet_year), fleet_name, regulations, synthetic_firms_with_location_file,
                                     private_fleet_file, private_fuel_mix_file, ev_availability_file, 
                                     firms_with_fleet_file, firms_with_fleet_mc_adj_files, output_path, need_regional_calibration)
           
    ###### Steps 13-15 -- international flow, mode choice and shipment generation
    
    if run_international_flow:
        
        # international commodity flow
        if need_domestic_adjustment:
            print('Use international flow generation with destination adjustment...')
            if forecast_analysis and forecast_year != '2017':
                international_demand_generation(c_n6_n6io_sctg_file, sctg_by_port_file,
                                                    sctg_group_file, int_shipment_size_file,
                                                    regional_import_file, regional_export_file, 
                                                    port_level_import_file, port_level_export_file,
                                                    need_domestic_adjustment, import_od, export_od, 
                                                    output_path, forecast_year, 
                                                    import_forecast_factor,
                                                    export_forecast_factor,
                                                    location_from, location_to)
                # full list of inputs
            else:
                international_demand_generation(c_n6_n6io_sctg_file, sctg_by_port_file,
                                                    sctg_group_file, int_shipment_size_file,
                                                    regional_import_file, regional_export_file, 
                                                    port_level_import_file, port_level_export_file,
                                                    need_domestic_adjustment, import_od, export_od, 
                                                    output_path, 
                                                    location_from = location_from, 
                                                    location_to =location_to)
                # skipping forecast
        else:
            print('Use international flow generation without destination adjustment...')
            if forecast_analysis and forecast_year != '2017':
                international_demand_generation(c_n6_n6io_sctg_file, sctg_by_port_file,
                                                    sctg_group_file, int_shipment_size_file,
                                                    regional_import_file, regional_export_file, 
                                                    port_level_import_file, port_level_export_file,
                                                    need_domestic_adjustment, import_od, export_od, 
                                                    output_path, forecast_year, 
                                                    import_forecast_factor,
                                                    export_forecast_factor)
            else:
                international_demand_generation(c_n6_n6io_sctg_file, sctg_by_port_file,
                                                sctg_group_file, int_shipment_size_file,
                                                regional_import_file, regional_export_file, 
                                                port_level_import_file, port_level_export_file,
                                                need_domestic_adjustment, import_od, export_od, 
                                                output_path)
            
        # international mode choice
        if regional_analysis:
            international_mode_choice(int_mode_choice_file, distance_travel_skim_file,
                                      import_od, export_od, import_mode_file, export_mode_file,
                                      mode_choice_spec, output_path, mesozone_to_faf_file,
                                      international_summary_file,
                                      international_summary_zone_file, region_code)
        else:
            international_mode_choice(int_mode_choice_file, distance_travel_skim_file,
                                      import_od, export_od, import_mode_file, export_mode_file,
                                      mode_choice_spec, output_path, mesozone_to_faf_file,
                                      international_summary_file,
                                      international_summary_zone_file)
        
        # domestic receiver assignment
        domestic_receiver_assignment(consumer_file, producer_file, mesozone_to_faf_file,
                                 sctg_group_file, import_mode_file, export_mode_file,
                                 export_with_firm_file, 
                                 import_with_firm_file, output_path)
            
    ###### Final step -- model validation 
    if run_model_validation: 
        if regional_analysis: # include region code and focus zone
            if port_analysis: # include international summary
                print('Validate regional results with international flow.')
                validate_firm_employment(synthetic_firms_with_location_file,
                                         spatial_boundary_file,
                                         mesozone_to_faf_file, 
                                         domestic_summary_zone_file, 
                                         lehd_file, us_county_map_file, plot_path, 
                                         region_code, focus_region,
                                         port_analysis,                                          
                                         international_summary_zone_file) 
                
                validate_commodity_flow(sctg_group_file,
                                            faf_data_file, cfs_data_file,
                                            domestic_summary_file,
                                            forecast_year, plot_path, 
                                            region_code, focus_region,                     
                                            port_analysis, 
                                            international_summary_file)
                
            else:
                print('Validate regional results without international flow.')
                validate_firm_employment(synthetic_firms_with_location_file,
                                         spatial_boundary_file,
                                         mesozone_to_faf_file, 
                                         domestic_summary_zone_file, 
                                         lehd_file, us_county_map_file, plot_path, 
                                         region_code, focus_region, port_analysis)
                
                validate_commodity_flow(sctg_group_file,
                                            faf_data_file, cfs_data_file,
                                            domestic_summary_file,
                                            forecast_year, plot_path, 
                                            region_code, focus_region)
        else: # run national validation (without ports)
            print('Validate national results without international flow.')
            validate_firm_employment(synthetic_firms_with_location_file,
                                      spatial_boundary_file,
                                      mesozone_to_faf_file, 
                                      domestic_summary_zone_file, 
                                      lehd_file, us_county_map_file, plot_path)
            
            validate_commodity_flow(sctg_group_file,
                                    faf_data_file, cfs_data_file,
                                    domestic_summary_file,
                                    forecast_year, plot_path)
                
    print('SynthFirm run for ' + scenario_name + ' finished!')
    print('All outputs are under ' + output_path)
    print('-------------------------------------------------')
    
    # logfile.close()
    return
if __name__ == '__main__':
	main()

