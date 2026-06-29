*** Copyright Notice ***

Firm Synthesizer and Supply-chain Simulator (SynthFirm) Copyright (c) 2024- 2025, The Regents of the University of California, through Lawrence Berkeley National Laboratory (subject to receipt of any required approvals from the U.S. Dept. of Energy). All rights reserved.

If you have questions about your rights to use or distribute this software,
please contact Berkeley Lab's Intellectual Property Office at
IPO@lbl.gov.

NOTICE.  This Software was developed under funding from the U.S. Department
of Energy and the U.S. Government consequently retains certain rights.  As
such, the U.S. Government has been granted for itself and others acting on
its behalf a paid-up, nonexclusive, irrevocable, worldwide license in the
Software to reproduce, distribute copies to the public, prepare derivative
works, and perform publicly and display publicly, and to permit others to do so.


****************************


# SynthFirm Tutorial
<p> <b>A quick overview of running SynthFirm for a selected region</b> </p>
<p> <b>Contact</b>: Xiaodan Xu, Ph.D.  (XiaodanXu@lbl.gov) </p>
<p> <b>Updates (Aug 14, 2023)</b>: documented synthetic firm, producer and consumer generation </p>
<p> <b>Updates (Aug 22, 2023)</b>: documented B2B flow generation </p>
<p> <b>Updates (Aug 29, 2023)</b>: added firm generation into SynthFirm pipeline and uploaded input data </p>
<p> <b>Updates (Oct 14, 2024)</b>: update documentation to reflect V2.0 improvements and regional implementation capabilities </p>
<p> <b>Updates (Oct 14, 2025)</b>: update documentation to reflect V2.1 improvements, including full national run capabilities, national fleet generation and forecast, and enhanced regional modeling/validation capabilities </p>
<p> <b>Updates (Nov 5, 2025)</b>: Adding input and configuration for Austin and national model release </p>

## Task 0 -- Input data generation ##
* Please refer to this [input generation guide](input_generation/Readme.md) to prepare inputs for selected region and scenario
  * For FAF region, please refer to the [FAF5 website](https://faf.ornl.gov/faf5/) and search 'FAF5 Zones - 2017 CFS Geography Shapefile' for definition.
* Following instructions to prepare inputs needed for the selected region, or use pre-generated input files: https://doi.org/10.5281/zenodo.17583820
* Make sure Python3 is accessible through bash/terminal.  You can check the status of Python using the following scripts:
    ```
    Python3 --version
    ```

## Task 1 -- Prepare configuration file ##

### 1.1 -- Define run types and environment variables ###
* Define input path and files under the [Python configure file](SynthFirm.conf), with current inputs set up for San Francisco Bay Area
  * Fill in project information following this example:

    ```
    [ENVIRONMENT]
    file_path = . # path to project data; relative paths are resolved from the config file location
    
    scenario_name = BayArea # scenario name must be consistent with input generation to allow for models searching for the I-O paths
    out_scenario_name = BayArea  # scenario name for output, can be different from input scenario name, but must be consistent with firm generation configs
    parameter_path = SynthFirm_parameters # parameter directory
    number_of_processes = 2 
    # number of cores to be used for parallel computing, zero means all the available cores
    ```

  * Keep downloaded data outside the Git checkout. Machine-specific config
    files can live beside that data. `file_path` should point to the data root
    that contains `inputs_<scenario>`, `outputs_<scenario>`,
    `plots_<scenario>`, and the parameter directory. It may be an absolute
    path, a `~` or environment-variable path, or a relative path. Relative
    paths are resolved from the directory that contains the config file rather
    than from the shell's current working directory. A local Austin test can use
    this layout:

    ```text
    /path/to/SynthFirm-data/
      Austin_local.conf
      inputs_Austin/
      SynthFirm_parameters/
    ```

    For a local run, copy a checked-in config such as `configs/Austin_base.conf`
    to the data root as `Austin_local.conf`, update `scenario_name`,
    `out_scenario_name`, `parameter_path`, and the step flags as needed, then
    use `file_path = .`. Run it with an absolute path to the local config:

    ```bash
    python SynthFirm_run.py --config /path/to/SynthFirm-data/Austin_local.conf
    ```

    Leave `Austin_local.conf` and other configs with local absolute paths out of
    version control.

  * Define the current run type (the input files vary by types of run, which will be elaborated below):
  
    ```
    regional_analysis = yes # this specification defines if SynthFirm is executed at regional or national scale, yes for regional, and no for national
    
    region_code = 62, 64, 65, 69 # if 'regional_analysis = yes', please specify the FAF regions for the current run
    
    forecast_analysis = yes # this specification defines if future year run is needed. If yes, users also need to include 'forecast_year' under environment. If 'forecast_analysis = no', a base year 2017 run will be executed.

    forecast_year = 2017 # if 'forecast_analysis = yes', specify the future year for projection. If 'forecast_analysis = yes' and 'forecast_year = 2017', a calibration run is enabled to ensure the production/consuption equals to FAF5 values. 
    
    port_analysis = yes # this specification defines if international flow is included. If yes, international flow inputs are needed and 'enable_international_flow' below can be set as yes. If no, international flow inputs are optional and 'enable_international_flow' below can only be no. 
    ```
  
  * Select the modules that are needed (must complete all of them in the following order, but can run one module at a time):

    ```
    enable_firm_generation = yes
    enable_producer_consumer_generation = no
    enable_demand_forecast = no # can only be turned on if 'forecast_analysis = yes'
    enable_firm_loc_generation = no
    enable_supplier_selection = no
    enable_size_generation = no
    enable_mode_choice = no
    enable_post_analysis = no
    enable_fleet_generation = no
    enable_international_flow = no # can only be turned on if 'port_analysis = yes'
    enable_model_validation = no
    ```

  * Finally, if the model is executed for general purpose, not a MPO run, the user need to specify the following variables:
    
    ```
    need_regional_calibration = no
    ```
    
  * If the model is executed for a specific MPOs, additional spatial variables can be added for crosswalk with MPO models, such as the following case applied for PSRC:
  
    ```
    need_regional_calibration = yes
    regional_variable = ParcelID,TAZ
    ```

### 1.2 -- Define input and output files ###

* For the selected run, fill in the input file names:
  
    ```
    [INPUTS]
    # below are mandatory inputs for any types of run
    cbp_file = data_emp_cbp_imputed.csv # CBP firm and employment file
    mzemp_file = data_mesozone_emprankings.csv # in-region employment ranking at CBG level
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
    need_domestic_adjustment = yes
    # optional zonal inputs when there is a need to reallocate destinations
    location_from = 61, 63
    location_to = 62, 64, 65, 69
    
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
    agg_unit_cost_file = data_unitcost_cfs2017.csv # unit cost of commodity
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
  * Run [SynthFirm model](SynthFirm_run.py):

    ```
    python SynthFirm_run.py --config 'SynthFirm.conf'
    ```

    Local machine-specific configs can live outside the repository, or under
    `configs/` with `local` in the file name so they are ignored by Git.

### Consist provenance tracking

This integration is the first Consist wiring for SynthFirm. Consist records the
inputs, config, and outputs for selected model steps so a run can be inspected
afterward without reconstructing the file flow by hand. The current integration
tracks Step 1 firm generation, Step 2 producer generation, Step 3 consumer
generation, and Step 4 demand forecasting. Later enabled model steps still run
normally, but they are not yet recorded as individual Consist steps.

The intent is to establish a small, concrete template for the rest of the
pipeline. The tracked steps show how to declare real file inputs, attach
important output artifacts, group multi-file outputs with `OutputSet`, and add
schema metadata where it is useful. The parsed SynthFirm config is stored as
Consist run config rather than as a normal input artifact.

Step 2 also writes `wholesale_cost_factor.csv`, a small one-row artifact with
the wholesale adjustment calculated during producer generation. Step 3 reads
that file instead of receiving the value through Python state. This keeps the
handoff visible in Consist: the cost factor is a normal Step 2 output and a
normal Step 3 input.

Each script execution creates a Consist scenario header tagged
`full-execution`, with Steps 1-4 recorded as child runs under that scenario.
Step 4 is useful as a small configuration example: `forecast_year` is recorded
as run config because it changes the forecast calculation and the forecast
input files used by that step.
By default, Consist writes state under the data root named by
`ENVIRONMENT.file_path`, not under an individual scenario output directory. This
keeps local serial runs in one provenance database:

```text
<data_root>/database/runs
<data_root>/database/archive
<data_root>/database/provenance.duckdb
```

The run log prints a pasteable `consist shell --trust-db --db-path ...` command
for the active database. These default paths can be overridden with
`SYNTHFIRM_CONSIST_RUN_DIR` and `SYNTHFIRM_CONSIST_DB_PATH`.
Tracked steps use Consist cache reuse with
`cache_hydration="inputs-missing"` and `validate_materialized_inputs=True`.
That means Consist may skip a step when the declared inputs, config, and code
identity match a previous completed run. If a later cache miss needs an earlier
version of a file that SynthFirm has overwritten, Consist can restore the
archived version when its recorded full-content hash proves the live file is
stale. The tracked-step cache epoch is set to `2` so runs created before this
archive-aware policy are not reused accidentally.

After each tracked step, SynthFirm asks Consist to archive the declared
single-file outputs under `<data_root>/database/archive`. This is what makes the
Step 4 same-path forecast pattern recoverable: the baseline `synthetic_firms`,
`producer`, and `consumer` files are copied to a recovery root before Step 4
overwrites those live filenames with forecasted versions. Supporting tracked
outputs such as `wholesale_cost_factor.csv` are archived the same way so later
cache misses can recover the exact Step 2 handoff file. Output sets are still
recorded as Consist `OutputSet` artifacts, but this first cache-aware pass only
archives the main single-file outputs.

Recorded artifact paths use Consist mounts. Files under `ENVIRONMENT.file_path`
are recorded as `data://...`, and files under the SynthFirm checkout are
recorded as `code://...`. When inspecting on the same machine, `--trust-db`
lets the CLI use the stored mount roots. On another machine, pass explicit
mounts such as `--mount data=/path/to/SynthFirm-data`.

Inspect recorded runs with the Consist CLI:

```bash
consist runs --db-path <data_root>/database/provenance.duckdb
consist show <run_id> --db-path <data_root>/database/provenance.duckdb
consist artifacts <run_id> --db-path <data_root>/database/provenance.duckdb
consist lineage <artifact_key> --db-path <data_root>/database/provenance.duckdb
```

SynthFirm enables Consist file-schema profiling for the tracked steps. During a
run, Consist captures lightweight schemas for tabular inputs and outputs such as
`synthetic_firms`, `producer`, and `consumer`. That makes the observed schema
available immediately after the run:

```bash
consist artifacts <run_id> --db-path <data_root>/database/provenance.duckdb
consist schema export --artifact-id <artifact_id> --db-path <data_root>/database/provenance.duckdb --out schemas/<schema_name>.py
```

Use `artifacts <run_id>` to find the artifact ID for the specific output you
want to export. Exporting by artifact ID avoids ambiguity when similar artifact
keys appear in multiple steps.

The repository also includes curated schema classes in
`utils/consist_schemas.py`. These started from Austin run schema stubs and add
column descriptions plus conservative relationships for the main Step 1-4
outputs. `utils.consist_tracking.create_consist_tracker` registers those
schemas with the Consist tracker so they are available for Consist views. The
Step 1-4 Consist specs attach the schemas to the main declared outputs with
`ArtifactSpec`, including `synthetic_firms`, `producer`, `wholesaler`,
`consumer`, forecasted firm/producer/consumer outputs, and the
producer/consumer SCTG output sets. Untyped inputs and secondary outputs still
rely on automatic file-schema profiling.

To promote another artifact to a first-class schema:

1. Run the step once with profiling enabled.
2. Use `consist artifacts <run_id>` or the interactive shell to find the
   artifact.
3. Export or inspect a stub with `consist schema export --artifact-id ...` or
   `schema_stub @<n>`.
4. Add the reviewed SQLModel class to `utils/consist_schemas.py`, preserving
   the observed CSV column names and adding only relationships or descriptions
   that are clear from the model.
5. Add the class to `SYNTHFIRM_CONSIST_SCHEMAS`.
6. Attach it to the relevant `ArtifactSpec` or `OutputSet` in
   `utils/consist_tracking.py`.

Doing this gives the artifact a stable schema name, documented columns, and
explicit relationships for Consist views and downstream run inspection. It also
makes shared run archives easier to interpret because the important outputs
carry more structure than a filename and an observed CSV profile.

For interactive inspection, open the shell and run `artifacts <run_id>`, then
`schema_profile @<n>` or `schema_stub @<n>` for the artifact reference you want
to inspect:

```bash
consist shell --trust-db --db-path <data_root>/database/provenance.duckdb
```

Input hydration runs only when a step has a cache miss and needs to execute. A
full all-hit replay can remain metadata-only, so recreating deleted terminal
files after every step cache-hits should be handled with an explicit Consist
output hydration or export step.
