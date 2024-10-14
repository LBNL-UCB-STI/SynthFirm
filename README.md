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
<p>A quick overview of running SynthFirm for a selected region </p>
<p> Contact: Xiaodan Xu, Ph.D.  (XiaodanXu@lbl.gov) </p>
<p> Updates (Aug 14, 2023): documented synthetic firm, producer and consumer generation </p>
<p> Updates (Aug 22, 2023): documented B2B flow generation </p>
<p> Updates (Aug 29, 2023): added firm generation into SynthFirm pipeline and uploaded input data </p>
<p> Updates (Oct 14, 2024): update documentation to reflect V2.0 improvements </p>

## Task 0 -- Input data generation and environment setup ##
* Please refer to this [input generation guide](input_generation/Readme.md) to prepare inputs for selected region
* Following instructions to prepare inputs needed for the selected region, or use pre-generated input files under [input_data](input_data)
  * Pre-generated baseline inputs from [San Francisco Bay Area](input_data/inputs_BayArea.zip) 
  * Pre-generated baseline inputs from [Austin Region](input_data/Inputs_Austin.zip)
  * Pre-generated baseline inputs from [Seattle Region](input_data/Inputs_Seattle.zip)
* Make sure Python3 is accessible through bash.  You can check the status of Python using the following scripts:
    ```
    Python3 --version
    ```

## Task 1 -- Prepare configuration file ##
* Define input path and files under the [Python configure file](SynthFirm.conf), with current inputs set up for San Francisco Bay Area
  * Fill in project information following this example:

    ```
    [ENVIRONMENT]
    file_path = /Users/xiaodanxu/Documents/SynthFirm.nosync # path to project data
    
    scenario_name = BayArea # scenario name, must be consistent with input generation to allow for models searching for the I-O paths
    
    out_scenario_name = BayArea # scenario name for output, can be different from input scenario name, but must be consistent with firm generation configs
    
    parameter_path = SynthFirm_parameters # parameter directory
    
    region_code = 62, 64, 65, 69 # list of FAF zone from the study region, for more information about the zonal id, please reference this guide: https://faf.ornl.gov/faf5/data/FAF5%20User%20Guide.pdf
    
    number_of_processes = 2 # number of cores to be used for parallel computing, zero means all the available cores
    ```

  * Select the modules that are needed (must complete all of them in the following order, but can run one module at a time):

    ```
    enable_firm_generation = yes
    enable_producer_consumer_generation = no
    enable_demand_forecast = no # this is an optional step for demand forecast
    enable_firm_loc_generation = no
    enable_supplier_selection = no
    enable_size_generation = no
    enable_mode_choice = no
    enable_post_analysis = no
    enable_fleet_generation = no
    enable_international_flow = no # this is an optional step for international demand generation
    ```

  * Fill in the input file names:
  
  ```
  [INPUTS]
  # below are mandatory inputs
  cbp_file = data_emp_cbp_imputed.csv
  mzemp_file = data_mesozone_emprankings.csv
  mesozone_to_faf_file = zonal_id_lookup_final.csv
  mode_choice_param_file = freight_mode_choice_parameter.csv
  spatial_boundary_file_fileend = _freight.geojson
  
  # below are optional international shipment inputs
  regional_import_file = FAF_regional_import.csv
  regional_export_file = FAF_regional_export.csv
  port_level_import_file = port_level_import.csv
  port_level_export_file = port_level_export.csv
  need_domestic_adjustment = yes
  # optional zonal inputs when there is a need to reallocate destinations
  location_from = 61, 63
  location_to = 62, 64, 65, 69
  int_mode_choice_file = freight_mode_choice_4alt_international_sfbcal.csv
  ```
  
  * Fill in the parameter file names:
  ```
  [PARAMETERS]
  c_n6_n6io_sctg_file = corresp_naics6_n6io_sctg_revised.csv
  employment_per_firm_file = employment_by_firm_size_naics.csv
  employment_per_firm_gapfill_file = employment_by_firm_size_gapfill.csv
  BEA_io_2017_file = data_2017io_revised_USE_value_added.csv
  agg_unit_cost_file = data_unitcost_cfs2017.csv
  prod_by_zone_file = producer_value_fraction_by_faf.csv
  cons_by_zone_file = consumer_value_fraction_by_faf.csv

  shipment_by_distance_bin_file = fraction_of_shipment_by_distance_bin.csv
  shipment_distance_lookup_file = CFS2017_routed_distance_matrix.csv
  cost_by_location_file = data_unitcost_by_zone_cfs2017.csv
  cfs_to_faf_file = CFS_FAF_LOOKUP.csv
  max_load_per_shipment_file = max_load_per_shipment_80percent.csv
  sctg_group_file = SCTG_Groups_revised.csv
  supplier_selection_param_file = supplier_selection_parameter.csv
  distance_travel_skim_file = combined_travel_time_skim.csv
  
  # optional paramter for international shipments
  int_shipment_size_file = international_shipment_size.csv
  sctg_by_port_file = commodity_to_port_constraint.csv
  ```
  
  * Fill in the output file names:
  ```
  [OUTPUTS]
  synthetic_firms_no_location_file = synthetic_firms.csv
  io_summary_file = io_summary_revised.csv
  wholesaler_file = synthetic_wholesaler.csv

  producer_by_sctg_filehead = prods_sctg
  io_filtered_file = data_2017io_filtered.csv
  producer_file = synthetic_producers.csv
  consumer_file = synthetic_consumers.csv
  sample_consumer_file = sample_synthetic_consumers.csv
  consumer_by_sctg_filehead = consumers_sctg
  synthetic_firms_with_location_file = synthetic_firms_with_location.csv
  zonal_output_fileend = _freight_no_island.geojson
  import_od = import_od.csv
  export_od = export_od.csv
  
  # below are optional international shipment outputs
  import_mode_file = import_OD_with_mode.csv
  export_mode_file = export_OD_with_mode.csv
  export_with_firm_file = export_OD_with_seller.csv
  import_with_firm_file = import_OD_with_buyer.csv
  ```
  
  * For mode choice model, the cost inputs can be adjusted under this section (in 2017 dollar):
  
  ```
  [MC_CONSTANTS]
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
  
  * For fleet generation, you can also configure the scenarios here (currently under revision and subject to change in V3.0):
  
  ```
  [FLEET_IO]
  private_fleet_file = fleet/CA_private_fleet_size_distribution.csv
  for_hire_fleet_file = fleet/CA_for_hire_fleet_size_distribution.csv
  for_lease_fleet_file = fleet/CA_for_lease_fleet_size_distribution.csv
  cargo_type_distribution_file = fleet/probability_of_cargo_group.csv
  state_fips_lookup_file = us-state-ansi-fips.csv
  fleet_year = 2018
  fleet_name = Ref_highp6
  national_fleet_composition_file = TDA_vehicle_stock.csv
  vehicle_type_by_state_file = fleet_composition_by_state.csv
  ev_availability_file = EV_availability.csv
  firms_with_fleet_file = synthetic_firms_with_fleet.csv
  carriers_with_fleet_file = synthetic_carriers.csv
  leasing_with_fleet_file = synthetic_leasing_company.csv
  firms_with_fleet_mc_adj_files = synthetic_firms_with_fleet_mc_adjusted.csv
  ```
  * For demand forecast scenario setup, please follow this [Seattle template](configs/Seattle_2030.conf)
  * Finish preparing configure file!
  
## Task 2 -- Run synthetic firm and B2B flow generation ##


* Run selected SynthFirm modules:
  * Open system Terminal/Shell, change directory to where the SynthFirm tool is located
  * Run [SynthFirm model](SynthFirm_run.py):

    ```
    python SynthFirm_run.py --config 'SynthFirm.conf'
    ```
  * Check output following the prompt on screen
  
* You are done, cheers!

