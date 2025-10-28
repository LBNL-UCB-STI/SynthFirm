*** Copyright Notice ***

Firm Synthesizer and Supply-chain Simulator (SynthFirm) Copyright (c) 2024, The Regents of the University of California, through Lawrence Berkeley National Laboratory (subject to receipt of any required approvals from the U.S. Dept. of Energy). All rights reserved.

If you have questions about your rights to use or distribute this software,
please contact Berkeley Lab's Intellectual Property Office at
IPO@lbl.gov.

NOTICE.  This Software was developed under funding from the U.S. Department
of Energy and the U.S. Government consequently retains certain rights.  As
such, the U.S. Government has been granted for itself and others acting on
its behalf a paid-up, nonexclusive, irrevocable, worldwide license in the
Software to reproduce, distribute copies to the public, prepare derivative 
works, and perform publicly and display publicly, and to permit others to do so.



# SynthFirm Tutorial
<p> <b>A quick overview of running SynthFirm for PSRC region</b> </p>
<p> <b>Contact</b>: Xiaodan Xu, Ph.D.  (XiaodanXu@lbl.gov) </p>
<p> <b>Updates (Aug 14, 2023)</b>: documented synthetic firm, producer and consumer generation </p>
<p> <b>Updates (Aug 22, 2023)</b>: documented B2B flow generation </p>
<p> <b>Updates (Aug 29, 2023)</b>: added firm generation into SynthFirm pipeline and uploaded input data </p>
<p> <b>Updates (Oct 14, 2024)</b>: update documentation to reflect V2.0 improvements and regional implementation capabilities </p>
<p> <b>Updates (Oct 14, 2025)</b>: update documentation to reflect V2.1 improvements, including full national run capabilities, national fleet generation and forecast, and enhanced regional modeling/validation capabilities </p>
<p> <b>Updates (Oct 23, 2025)</b>: update documentation for PSRC application </p>
<p> <b>Updates (Oct 28, 2025)</b>: update data link for PSRC application </p>

## Task 0 -- Input data generation ##
* Please refer to this [input generation guide](input_generation/Readme.md) to prepare inputs for selected region and scenario
  * For FAF region, please refer to the [FAF5 website](https://faf.ornl.gov/faf5/) and search 'FAF5 Zones - 2017 CFS Geography Shapefile' for definition.
* Following instructions to prepare inputs needed for the selected region, or use pre-generated input files: https://doi.org/10.5281/zenodo.17469395
* Make sure Python3 is accessible through bash/terminal.  You can check the status of Python using the following scripts:
    ```
    Python3 --version
    ```

## Task 1 -- Prepare configuration file ##

### 1.1 -- Define run types and environment variables ###
* Pre-defined configuration files for PSRC baseline and 2050 forecast can be found under the [configuration folder](configs).
  * [PSRC baseline configuration](configs/Seattle_base_psrc.conf)
  * [PSRC 2050 forecast configuration](configs/Seattle_2050_psrc.conf)

* The specific model run type and steps are defined below, with an example of PSRC baseline:
  * define model execution environment:

    ```
    [ENVIRONMENT]
    file_path = /Users/xiaodanxu/Documents/SynthFirm.nosync # path to project data
    
    scenario_name = Seattle # scenario name must be consistent with input generation to allow for models searching for the I-O paths
    out_scenario_name = Seattle_base  # scenario name for output, can be different from input scenario name, but must be consistent with firm generation configs
    parameter_path = SynthFirm_parameters # parameter directory
    number_of_processes = 4
    # number of cores to be used for parallel computing, zero means all the available cores
    ```

  * Define the current run type (the input files vary by types of run, which will be elaborated below):
  
    ```
    regional_analysis = yes # this specification defines if SynthFirm is executed at regional or national scale, yes for regional, and no for national
    
    region_code = 531, 532, 539, 411 # if 'regional_analysis = yes', please specify the FAF regions for the current run
    
    forecast_analysis = yes # this specification defines if future year run is needed. If yes, users also need to include 'forecast_year' under environment. If 'forecast_analysis = no', a base year 2017 run will be executed.

    forecast_year = 2017 # if 'forecast_analysis = yes', specify the future year for projection. If 'forecast_analysis = yes' and 'forecast_year = 2017', a calibration run is enabled to ensure the production/consuption equals to FAF5 values. 
    
    port_analysis = yes # this specification defines if international flow is included. If yes, international flow inputs are needed and 'enable_international_flow' below can be set as yes. If no, international flow inputs are optional and 'enable_international_flow' below can only be no. 

    need_regional_calibration = yes # this specification defines if regional data will be used for model calibration, which is defaulted to 'yes' in PSRC case. The region-specific inputs will be defined in the 'CALIBRATION' section of the config file
    ```
  
  * Select the modules that are needed (must complete all of them in the following order, but can run one module at a time):

    ```
    enable_psrc_pre_calibration = yes
    enable_firm_generation = yes
    enable_producer_consumer_generation = yes
    enable_demand_forecast = yes # can only be turned on if 'forecast_analysis = yes'
    enable_firm_loc_generation = yes
    enable_psrc_post_run_calibration = yes
    enable_supplier_selection = yes
    enable_size_generation = yes
    enable_mode_choice = yes
    enable_post_analysis = yes
    enable_fleet_generation = yes
    enable_international_flow = yes # can only be turned on if 'port_analysis = yes'
    enable_model_validation = yes
    ```


### 1.2 -- Define input and output files ###

* If the model is executed for a specific MPOs, additional spatial variables can be added for crosswalk with MPO models, such as the following case applied for PSRC:

    ```
    [CALIBRATION]
    uncalibrated_mzemp_file = data_mesozone_emprankings.csv # SynthFirm employment data before PSRC calibration, once calibrated, write output to 'mzemp_file' item below
    psrc_parcel_file = PSRC/landuse/2018/v3.0_RTP/parcels_urbansim.txt # PSRC parcel-level data
    soundcast_db_file = PSRC/db/soundcast_inputs.db # PSRC soundcast input for defining geography
    geography_table_name = parcel_2018_geography # table name for 2018 geography under DB
    cleaned_parcel_file = parcel_data_2018.csv # output name for cleaned parcel data
    regional_variable = ParcelID,TAZ # additional zonal attributes to be included in the SynthFirm output
    ```

* For the selected run, fill in the input file names:
  
    ```
    [INPUTS]
    # below are mandatory inputs for any types of run
    cbp_file = data_emp_cbp_imputed.csv # CBP firm and employment file
    mzemp_file = data_mesozone_emprankings_2018.csv # CALIBRATED employment ranking at CBG level
    mesozone_to_faf_file = zonal_id_lookup_final.csv # mesozone-FAFID-GEOID crosswalk
    mode_choice_param_file = freight_mode_choice_parameter.csv # mode choice parameter
    spatial_boundary_file_fileend = _freight.geojson # geometry file of the run
  
    # below are optional international shipment inputs
    regional_import_file = FAF_regional_import_{analysis_year}.csv #FAF import value and tonnage for the region and selected year
    regional_export_file = FAF_regional_export_{analysis_year}.csv #FAF export value and tonnage for the region and selected year
    port_level_import_file = port_level_import.csv # USATO port-level import value for the region 
    port_level_export_file = port_level_export.csv # USATO port-level export value for the region
    int_mode_choice_file = freight_mode_choice_4alt_international_sfbcal.csv
    
    # below are additional specifications for international flow, where you can reallocate domestic destinations in FAF5 from outside region to inside the region, to avoid unnecessary inter-regional flow (e.g., Los Angeles export shipped via Port of Oakland). This function can be turned off if 'need_domestic_adjustment = no' and drop out the 'location_from' and 'location_to' variables.  
    need_domestic_adjustment = no
    
    ```
  
* Fill in the parameter file names:
    ```
    [PARAMETERS]
    # below are mandatory parameters for any types of run
    
    # parameters for firm, producer, consumer generation
    c_n6_n6io_sctg_file = corresp_naics6_n6io_sctg_revised.csv # NAICS-SCTG crosswalk file
    employment_per_firm_file = employment_by_firm_size_naics.csv # employment per firm estimate 
    employment_per_firm_gapfill_file = employment_by_firm_size_gapfill.csv # aggregated employment per firm for initiate the model 
    BEA_io_2017_file = data_2017io_revised_USE_value_added.csv # input-output table
    agg_unit_cost_file = data_unitcost_psrc_calib.csv # unit cost of commodity, calibrated to match load in PSRC region
    prod_by_zone_file = producer_value_fraction_by_faf.csv # regional allocation factor for production
    cons_by_zone_file = consumer_value_fraction_by_faf.csv # regional allocation factor for consumption
  
    # parameters for supplier selection  
    shipment_by_distance_bin_file = fraction_of_shipment_by_distance_bin.csv # fraction of shipment by distance bin for supplier selection 
    shipment_distance_lookup_file = CFS2017_routed_distance_matrix.csv # generic travel distance matrix for supplier selection 
    cost_by_location_file = data_unitcost_by_zone_cfs2017.csv # unit cost by SCTG and region for supplier selection
    supplier_selection_param_file = supplier_selection_parameter.csv # supplier selection model parameter
    
    # parameters for shipment size and mode choice simulation  
    cfs_to_faf_file = CFS_FAF_LOOKUP.csv # CFS to faf crosswalk
    max_load_per_shipment_file = max_load_per_shipment_80percent.csv # shipment size by SCTG
    sctg_group_file = SCTG_Groups_revised_V2.csv # SCTG code to SCTG group definition
    distance_travel_skim_file = combined_travel_time_skim.csv # travel distance and time skim by mode
    
    # optional parameters for forecast analysis
    prod_forecast_filehead = total_commodity_production_ # domestic production filehead (forecast year defined under environment section)
    cons_forecast_filehead = total_commodity_attraction_ # domestic consumption filehead (forecast year defined under environment section)
    
    # optional parameters for port analysis
    int_shipment_size_file = international_shipment_size. # international shipment size
    sctg_by_port_file = commodity_to_port_constraint.csv # commodity constraints by port type
    
    # optional parameters for forecast analysis and port analysis
    import_forecast_filehead = factor_import_ # international import projection filehead (forecast year defined under environment section)
    export_forecast_filehead = factor_export_ # international export projection filehead (forecast year defined under environment section)
    ```
    
  
* Fill in the output file names:
    ```
    [OUTPUTS]
    # below are generic output files for all types of runs
    synthetic_firms_no_location_file = synthetic_firms.csv #synthetic firms without lat/lon
    io_summary_file = io_summary_revised.csv #  input-output summary for quality checking
    wholesaler_file = synthetic_wholesaler.csv # synthetic wholesalers
  
    
    io_filtered_file = data_2017io_filtered.csv # selected input-output values
    producer_file = synthetic_producers.csv # synthetic producers (all SCTG groups)
    producer_by_sctg_filehead = prods_sctg # producer file by SCTG group
    consumer_file = synthetic_consumers.csv # synthetic consumers (all SCTG groups)
    sample_consumer_file = sample_synthetic_consumers.csv # sample consumers for troubleshooting 
    consumer_by_sctg_filehead = consumers_sctg # consumer file by SCTG group
    synthetic_firms_with_location_file = synthetic_firms_with_location.csv #synthetic firms with lat/lon, and additional spatial variables for MPO runs
    zonal_output_fileend = _freight_no_island.geojson # clipped geometry file for plotting
    domestic_summary_file = domestic_b2b_flow_summary.csv # domestic commodity flow summary file by FAF zone
    domestic_summary_zone_file = domestic_b2b_flow_summary_mesozone.csv # domestic commodity flow summary file by mesozone
    
    
    # optional international shipment outputs
    import_od = import_od.csv # import flow by OD FAF zone
    export_od = export_od.csv # export flow by OD FAF zone
    import_mode_file = import_OD_with_mode.csv # import flow with mode assignment
    export_mode_file = export_OD_with_mode.csv # export flow with mode assignment
    export_with_firm_file = export_OD_with_seller.csv # export flow with seller
    import_with_firm_file = import_OD_with_buyer.csv # import flow with buyer
    international_summary_file = international_b2b_flow_summary.csv  # international commodity flow summary file by FAF zone
    international_summary_zone_file = international_b2b_flow_summary_mesozone.csv # international commodity flow summary file by mesozone
    ```
  
### 1.3 -- Define constant variables ###

* For mode choice model, the cost inputs can be adjusted under this section (in 2017 dollar):

    ```
    [CONSTANTS]
    # below are constants for general model run
    lb_to_ton = 0.0005
    NAICS_wholesale = 42
    NAICS_mfr = 31, 32, 33
    NAICS_mgt = 55
    NAICS_retail = 44, 45
    NAICS_info = 51
    NAICS_mining = 21
    NAICS_tw = 49
    weight_bin = 0, 0.075, 0.75, 15, 22.5, 100000
    weight_bin_label = 1, 2, 3, 4, 5
    
    [MC_CONSTANTS]
    # below are mode choice (MC) specific constant variables
    rail_unit_cost_per_tonmile = 0.039
    rail_min_cost = 200
    air_unit_cost_per_lb = 1.08
    air_min_cost = 55
    truck_unit_cost_per_tonmile_sm = 2.83
    truck_unit_cost_per_tonmile_md = 0.5
    truck_unit_cost_per_tonmile_lg = 0.18
    truck_min_cost = 10
    parcel_cost_coeff_a = 3.58
    parcel_cost_coeff_b = 0.015
    parcel_max_cost = 1000
    ```

### 1.4 -- Define fleet specifications ###  

* For fleet generation, you can also configure the scenarios here:
  
    ```
    [FLEET_IO]
    fleet_year = 2018 # years of truck fleet
    fleet_name = Ref_highp6 # fuel price scenario, see the information below for definition
    regulations = ACC and ACT # if consider EV mandate from ACC and ACT rules, choose from 'ACC and ACT' and 'no ACC and ACT'
    
    private_fleet_file = veh_per_emp_by_state.csv # vehicle per employement for fleet size calculation
    for_hire_fleet_file = FMCSA_truck_count_by_state_size.csv # registered for-hire truck fleet size by state for fleet size calculation
    cargo_type_distribution_file = probability_of_cargo_group.csv # probability of cargo type for carrier cargo assignment 
    state_fips_lookup_file = us-state-ansi-fips.csv # state fips code
    ev_availability_file = synthfirm_ev_availability.csv # ev powertrain availability by vehicle type
    private_fuel_mix_file = private_fuel_mix_scenario.csv # fuel mix for private fleet, by state, year and scenario
    hire_fuel_mix_file = hire_fuel_mix_scenario.csv # fuel mix of for-hire fleet, by year and scenario
    lease_fuel_mix_file = lease_fuel_mix_scenario.csv # fuel mix of for-leasing fleet, by year and scenario
    private_stock_file = private_stock_projection.csv # future year private truck stock projection
    hire_stock_file = hire_stock_projection.csv # future year for-hire truck stock projection
    lease_stock_file = lease_stock_projection.csv # future year for-lease truck stock projection
    
    # below are fleet-specific output
    firms_with_fleet_file = synthetic_firms_with_fleet.csv  # synthetic firms with fleet assigned
    carriers_with_fleet_file = synthetic_carriers.csv # synthetic carriers
    leasing_with_fleet_file = synthetic_leasing_company.csv # synthetic truck leasing firms
    firms_with_fleet_mc_adj_files = synthetic_firms_with_fleet_mc_adjusted.csv # synthetic firms with fleet assigned and payload capacity adjusted to match shipping demand
    ```
* The fuel price scenario definition can be found under [opcost_sensitivity_analysis](docs/opcost_sensitivity_analysis.csv)
 
### 1.5 -- Config model validation ###  
* For model validation, you can also configure the specifications here:
  
    ``` 
    [VALIDATION]
    
    lehd_file = US_naics.csv # LEHD employment
    us_county_map_file = US_counties.geojson # US county shapefile 
    faf_data_file = FAF5.3.csv # FAF5 data for validation
    cfs_data_file = CFS2017_stats_by_zone.csv # aggregated CFS2017 flow for base year validation
    
    #optional inputs for regional analysis
    focus_region = 64 #select zoom-in regions, must be selected from the 'region_code' variable above 
    ```
* Finish preparing configure file!
  
## Task 2 -- Run synthetic firm and B2B flow generation ##


* Run selected SynthFirm modules:
  * Open system Terminal/Shell, change directory to where the SynthFirm tool is located
  * Run [customized SynthFirm model for PSRC](SynthFirm_run_PSRC.py):

    ```
    python SynthFirm_run_PSRC.py --config 'configs/Seattle_base_psrc.conf'
    ```
  
  * Check output following the prompt on screen
  * The log file will be created for each run under the output directory, with file name 'Seattle_base_run_{date}.log'
  
* You are done, cheers!

