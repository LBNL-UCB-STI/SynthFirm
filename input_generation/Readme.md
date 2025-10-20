# SynthFirm Input Data Generation
<p>This folder include scripts that are used to generate SynthFirm inputs from raw data sources under various geographic resolution </p>
<p> Contact: Xiaodan Xu, Ph.D.  (XiaodanXu@lbl.gov) </p>
<p> Updates (May 8, 2023): documented firm and employment data generation </p>
<p> Updates (May 11, 2023): documented CFS processing, supplier selection, I-O projection, and map generation </p>
<p> Updates (Aug 08, 2023): documented firm fleet generation </p>
<p> Updates (Oct 14, 2024): update documents to reflect V2 changes and improve readability </p>
<p> Updates (Oct 20, 2025): update documents for national fleet generation, international flow inputs and other input enhancements </p>

**List of Modules**
<!--ts-->
* [National-level input generation ](#a---national-level-input-generation)
* [Study area input generation](#b---study-area-input-generation)
* [Fleet generation](#c---national-fleet-input-generation)
* [International flow](#d---international-flow-input-generation)

<!--te-->

## A - National-level input generation 

## The inputs are generated at national-scale regardless of study area

### step a.1 - processing CFS zone in [cfs_to_faf_map_generation.R](cfs_to_faf_map_generation.R):
* Using input geographic data from CFS2017 (https://www.census.gov/programs-surveys/cfs/technical-documentation/geographies.html) and CFS2017-FAF5 mapping (https://faf.ornl.gov/faf5/)
* Combine CFS county data with CFS county to metro area lookup table
* Combine CFS metro area with FAF zone
* Produce county shapefile with CFS county, metro area and FAF zone (CFS2017_areas.geojson)
  
### step a.2 - generate initial employment count per firm by firm size at national level in [employment_count_by_firm.R](employment_count_by_firm.R):
* Require CENSUS employment data by firm size (https://www.census.gov/programs-surveys/susb.html)
* Produce average employment per firm by firm size group  'SynthFirm_parameters/employment_by_firm_size_gapfill.csv'
* Produce average employment per firm by firm size group and industries 'SynthFirm_parameters/employment_by_firm_size_naics.csv' 
* While these two tables are not directly associated with synthetic firm output due to IPF fitting, it is needed to initiate firm synthesize and provide initial employment values.

### step a.3 - project BEA I-O table for baseline year in [BEA_IO_processor.py](BEA_IO_processor.py):
* Require BEA I-O use data from multiple years as inputs
  * 2012 I-O with 405 industries (also contains industry mapping): https://docs.google.com/viewer?url=https%3A%2F%2Fapps.bea.gov%2Findustry%2Fxls%2Fio-annual%2FUse_SUT_Framework_2007_2012_DET.xlsx
  * 2017 I-O with 71 industries:https://apps.bea.gov/iTable/?reqid=150&step=3&isuri=1&table_list=6009&categories=io (modify year to baseline = 2017)
* For 2017 I-O, in addition to industry-specific values, it is critical to include added value (last row) during production.  This value indicates the value of commodity gained during production and paid by consumers.  Without this the final I-O will be significantly under-estimated. BEA:The sum of the entries in a column is that industry’s output.
* Project 2012 I-O value to 2017 industry total, to obtain I-O values by all 405 industries
* Produce projected baseline I-O data as output 'SynthFirm_parameters/data_2017io_revised_USE_value_added.csv'

### step a.4 - collect commodity flow distributions from CFS data in [CFS_distribution_generator.py](CFS_distribution_generator.py):
* Require CFS 2017 public use data as input (cfs-2017-puf-csv.csv):https://www.census.gov/data/datasets/2017/econ/cfs/historical-datasets.html
* Require several predefined lookup tables:
  * industy and commodity look up table (from MAG model): corresp_naics6_n6io_sctg_revised.csv
  * CFS and FAF zone lookup from FAF5 document: CFS_FAF_LOOKUP.csv (downloaded from FAF5 website under 'Related Information': https://faf.ornl.gov/faf5/)
  * commodity SCTG group definition: SCTG_Groups_revised_V2.csv (defined by research team)
* Produce the following 5 outputs:
  * Unit cost by commodity: SynthFirm_parameters/data_unitcost_cfs2017.csv
  * Unit cost by CFS zone and commodity: SynthFirm_parameters/data_unitcost_by_zone_cfs2017.csv
  * Shipment size with pre-defined percentile cut-off criteria: SynthFirm_parameters/max_load_per_shipment_{X}percent.csv -> x is pre-defined quantile value to select max load by sctg 
  * Production/consumption value allocation factor by FAF zone: SynthFirm_parameters/producer_value_fraction_by_faf.csv AND SynthFirm_parameters/consumer_value_fraction_by_faf.csv
  * Seasonal load allocation factor by SCTG group for national freight trip generation (future work): seasonal_allocation_factor_by_sctg.csv
  
### step a.5 - estimate supplier selection model parameter in [supplier_selection_model_estimation.py](supplier_selection_model_estimation.py):
* Require CFS 2017 imputed data as input (CFS2017_national_forML_short.csv)
* Require unit cost estimation by zone and sctg from step a.4 (data_unitcost_by_zone_cfs2017)
* Require several predefined lookup tables:
  * CFS and FAF zone lookup from FAF5 document: CFS_FAF_LOOKUP.csv (downloaded from FAF5 website under 'Related Information': https://faf.ornl.gov/faf5/)
  * commodity SCTG group definition: SCTG_Groups_revised.csv (defined by research team)
* Perform MNL model using CFS O-D data for supplier selection
* Produce the following 5 outputs:
  * Avg. routed distance matrix from CFS observation: CFS2017_routed_distance_matrix.csv
  * Estimated model parameters: supplier_selection_national_sctg{sctg_group_id}.html


### step a.6 - generate demand forecast inputs in [demand_forecast_input_generation.py](demand_forecast_input_generation.py):
* Require FAF5 and SCTG group definition (SCTG_Groups_revised.csv)
* Require CFS and FAF zone lookup from FAF5 document: CFS_FAF_LOOKUP.csv (downloaded from FAF5 website under 'Related Information': https://faf.ornl.gov/faf5/)
* Generate total production and consumption by FAF zone, SCTG and forecast year:
  * SynthFirm_parameters/total_commodity_production_{forecast_year}.csv
  * SynthFirm_parameters/total_commodity_consumption_{forecast_year}.csv
* Generate unit cost by year from FAF5
  * SynthFirm_parameters/data_unitcost_by_zone_faf{forecast_year}.csv
* Generate demand growth rate for international shipping (by SCTG and year):
  * SynthFirm_parameters/factor_import_{forecast_year}.csv
  * SynthFirm_parameters/factor_export_{forecast_year}.csv

### step a.7 - collecting employment data from Census API in [Census.R](Census.R):
* No input data required (need Census API key) and set default regional coverage to U.S.
* produce employment count by census block group and industry (US_naics.csv)
* Produce census block group shapefile for entire U.S. (US_bg.geojson)

### step a.8 - generating aggregated commodity flow for validation [cfs_validation_processor.py](cfs_validation_processor.py):
* Require CFS 2017 public use data as input (cfs-2017-puf-csv.csv):https://www.census.gov/data/datasets/2017/econ/cfs/historical-datasets.html
* Require several predefined lookup tables:
  * CFS and FAF zone lookup from FAF5 document: CFS_FAF_LOOKUP.csv (downloaded from FAF5 website under 'Related Information': https://faf.ornl.gov/faf5/)
  * commodity SCTG group definition: SCTG_Groups_revised_V2.csv (defined by research team)
* Produce aggregated commodity flow by origin and destination zones, commodity type and mode ('CFS2017_stats_by_zone.csv') for model validation

  
## B - study area input generation

## Inputs that need to be executed for study region

### step b.1 - collect CBP employment data using [FAFZone.R](FAFZone.R):
* Require CFS to FAF crosswalk map from step a.1 (CFS2017_areas.geojson) and CFS zone to FAF zone lookup table (CFS_FAF_LOOKUP.csv) as input (downloaded from FAF5 website under 'Related Information': https://faf.ornl.gov/faf5/)
* Require CBP and ZBP (industry detail and total) complete county file from census.gov (sample: https://www.census.gov/data/datasets/2017/econ/cbp/2017-cbp.html)
* Produce county, zipcode and FAF zone lookup table within study area ({region_name}_FAFCNTY.csv)
* Produce county/zipcode establishment file before imputation (data_emp_cbp.csv)
	
### step b.2 - impute firms and employment for missing industry in [FAFZones_no_cbp.R](FAFZones_no_cbp.R) (using firm data outside CBP for gap filling)
* Require department of labor employment data for imputing firm count and employment of missing industries in CBP (https://www.bls.gov/cew/downloadable-data-files.htm)
* Require outputs {region_name}_FAFCNTY.csv and data_emp_cbp.csv from step b.2 
* Produce imputed firm and employment output data_emp_cbp_imputed.csv

### step b.3 - generate CBG tier and ranking for each industry in [cbp_emprank.py](cbp_emprank.py)
* Require the following zonal shapefile and lookup tables:
  * Require FAF5 zonal shapefile (FAF5Zones.geojson) from step a.1
  * Require census block group shapefile (US_bg.geojson) from step a.7
  * Require FAF zone county lookup table ({region_name}_FAFCNTY.csv) from step b.1
* Require national employment data (US_naics.csv) from step a.7 - Census.R
* Produce ranking of employment by industry within each MESOZONE (data_mesozone_emprankings.csv)
* Produce mesozone - census GEOID lookup table (zonal_id_lookup_final.csv)
* Produce shapefile of model spatial resolution: {region_name}_freight.geojson
* Produce centroids of modeled zones: {region_name}_freight_centroids.geojson

### step b.4 - mode choice model estimation and application
* For mode choice model estimation, please refer to the [mode choice folder](../mode_choice) for more information
* The detailed methodology is documented in [Xu et al., 2024 paper](https://doi.org/10.3389/ffutr.2024.1339273)
* For the travel skim generation that supports mode choice application, all the scripts are under [skim generation](Skim generation/)
  * Highway routed distance skim using 2021 NHS (National Highway Systems): [highway_od_distance_clean_ver.R](Skim generation/highway_od_distance_clean_ver.R)
  * Rail/IMX routed distance skim using 2021 NTAD: [rail_od_distance_clean_ver.R](Skim generation/rail_od_distance_clean_ver.R)
  * Travel distance and time estimation for all modes: [freight_travel_time_skim_generation.ipynb](Skim generation/freight_travel_time_skim_generation.ipynb)

## C - national fleet input generation

## C.1 - base-year fleet generation (require proprietary registration data)

### step c.1.1 - processing 2022 Experian vehicle registration data in [national_baseline_fleet_from_experian.py](fleet_generation/national_baseline_fleet_from_experian.py):

* Require national vehicle registration data from NREL (POC: Alicia Birky: Alicia.Birky@nrel.gov) -- the data is aggregated by county, vehicle class (2-8), body type, sales type, model year, NAICS code and vehicle count.
* Require multiple [2021 US VIUS](https://www.census.gov/programs-surveys/vius.html) estimated parameters for data attributes imputation:
  * 2a/2b truck split by body type ('class_2a2b_by_body_type.csv')
  * fraction of commercial vehicles by class ('commercial_use_by_class.csv')
  * commercial truck count by state and class for result validation ('VIUS_truck_count_by_state.csv')
* Produce cleaned truck registration data with SynthFirm classes assigned ('cleaned_experian_data_national.csv')

### step c.1.2 - generating SynthFirm fleet inputs using cleaned Experian data [synthfirm_fleet_inputs_experian.py](fleet_generation/synthfirm_fleet_inputs_experian.py):

* Require cleaned Experian data from step c.1.1: 'cleaned_experian_data_national.csv'
* Require national employment data by industry from step a.7: 'US_naics.csv'
* Require additional [2021 US VIUS](https://www.census.gov/programs-surveys/vius.html) estimated parameters for data attributes imputation:
  * Require class 1 and 2 ratio file for imputing class 1 truck count ('class1_ratio_vius.csv')
* Produce fleet per employment estimate by state and industry, and class 1 truck count imputed ('veh_per_emp_by_state.csv')
* Produce the base-year fleet attributes for future-year projection:
  * truck age distribution: 'experian_national_age_distribution.csv'
  * fuel mix by SynthFirm truck class: 'experian_national_fuel_distribution.csv'

### step c.1.3 - generating carrier fleet characteristics using FMCSA carrier census data [fmcsa_data_processor.py](fleet_generation/fmcsa_data_processor.py)
* Require [2024 FMCSA carrier census data](https://data.transportation.gov/Trucking-and-Motorcoaches/Motor-Carrier-Registrations-Census-Files/4a2k-zf79/about_data)
* Clean the data to select carrier fleet in the U.S. and assign commodity group to each carrier
* Produce truck count by state and fleet size group: 'FMCSA_truck_count_by_state_size.csv'
* Produce probability of carriers that transport certain commodity group in operation: 'probability_of_cargo_group.csv'

## C.2 - future-year fleet generation (require proprietary NREL's TITAN project result)

### step c.2.1 - processing market shares of alternative vehicle technologies from NREL TITAN analysis and regional EV mandates [fleet_forecast_from_TDA.py](fleet_forecast/fleet_forecast_from_TDA.py):
* Require the following inputs:
  * Require state-level MD/HD split from NREL: 'MDHDbyState.csv'
  * Require national fleet projection from NREL TITAN analysis (proprietary, methodology documented in [Brooker et al., 2021](https://docs.nrel.gov/docs/fy21osti/79617.pdf))
  * Require baseline fleet growth and turnover rate from MOVES4 for LDTs (methodology documented in [Xu, et al., 2025 paper](https://doi.org/10.1080/15568318.2025.2566755))
* Produce SynthFirm fleet growth and turnover rate, combining TITAN and MOVES4 fleet projection: 'SynthFirm_pop_growth_and_turnover_rate.csv'
* Produce SynthFirm vehicle market share of electric trucks by model year: 'synthfirm_ev_market.csv'
* Produce conditional probability of EV powertrains (BEV, PHEV and FCEV) by vehicle classes and model year: ''synthfirm_ev_availability.csv''

### step c.2.2 - project future-year age distribution (and fraction of new sale) [synthfirm_rate_based_fleet_projection.py](fleet_forecast/synthfirm_rate_based_fleet_projection.py)
* Require SynthFirm fleet growth and turnover rate from step c.2.1: 'SynthFirm_pop_growth_and_turnover_rate.csv'
* Require base year vehicle age distribution from step c.1.2: 'experian_national_age_distribution.csv'
* Require MOVES5 vehicle scrappage rate: 'MOVES5_RMAR_and_Survival_rate.csv'
* Perform fleet projection using a probability-based approach, adding new sale and retiring older vehicles for every projection year (methodology documented in [Xu, et al., 2025 paper](https://doi.org/10.1080/15568318.2025.2566755))
* Produce projected fleet stock by age until 2050:
  * Vehicle stock for private fleet: 'synthfirm_age_distribution_private.csv'
  * Vehicle stock for for-hire fleet: 'synthfirm_age_distribution_hire.csv'
  * Vehicle stock for leasing fleet: 'synthfirm_age_distribution_lease.csv'

### step c.2.3 - project future-year fleet composition by fuel type [synthfirm_compile_fleet_inputs.py](fleet_forecast/synthfirm_compile_fleet_inputs.py)
* Require SynthFirm vehicle market share of electric trucks by model year from step c.2.1: 'synthfirm_ev_market.csv'
* Require projected fleet stock by age until 2050, for all sectors from step c.2.2
* Generate fleet stock size per forecast year and fuel mix by forecast year, combining EV market share by model year and age distribution in each year
* Produce fleet stock size by year and sector:
  * private fleet stock: 'private_stock_projection.csv'
  * for-hire fleet stock (for fleet size initialization, and final results will be calibrated to truck count from FMCSA registration data):  'hire_stock_projection.csv'
  * leasing stock: 'lease_stock_projection.csv'
* Produce fuel mix by by year and sector:
  * Fuel mix for private fleet: 'private_fuel_mix_scenario.csv'
  * Fuel mix for for-hire fleet: 'hire_fuel_mix_scenario.csv'
  * Fuel mix for leasing fleet: 'lease_fuel_mix_scenario.csv'

## D - international flow input generation

### step d.1 - estimate international shipment size [cfs_international_shipment_size.py](port_inputs/cfs_international_shipment_size.py)
* Require CFS 2017 public use data as input (cfs-2017-puf-csv.csv):https://www.census.gov/data/datasets/2017/econ/cfs/historical-datasets.html
* Select export shipments and estimate median shipment size (tons/package) by commodity type and international trade partner. Impute shipment size for countries with missing values using all-region shipment size.
* Produce international shipment size: 'international_shipment_size.csv'

### step d.2 - port location generation [port_location_processor.py](port_inputs/port_location_processor.py)
* Require 2017 EPA NEI port and airport data (https://drive.google.com/drive/folders/1tQViUa6b_GAVxgK4hG60ZuWDlSzS-omw)
* Require port entry location from U.S. Customs and Border Protection (US CBP) (https://www.cbp.gov/about/contact/ports), and their GPS locations are summarized on [Wikipedia](https://en.wikipedia.org/wiki/List_of_Canada%E2%80%93United_States_border_crossings)
* Require port code and mode availability from US CBP: https://www.cbp.gov/document/guidance/ace-appendix-d-export-port-codes
* Extract port location (lat/lon) and geometry within study area, for airports, ports (sea and river), and border crossing.
* Produce port location file:'port_location_CA_WA_OR_MA.geojson'

### step d.3 - regional international flow generation [international_flow_from_faf_usato.py](port_inputs/international_flow_from_faf_usato.py)
* Require port-level import/export value from [Census USA trade](https://usatrade.census.gov/) for ports within study area, login credential needed
* Require regional international flow from [FAF5](https://faf.ornl.gov/faf5/)
* Require spatial definition of ports and region:
  * Port location from step d.2: 'port_location_CA_WA_OR_MA.geojson'
  * Region spatial definition from step b.3: '{region_name}_freight.geojson'
* Select port-level and regional international flow from Census data and FAF5 for analysis year
* Map port location to mesozone within the region
* Produce port-level flow output:
  * Port-level imports:'port_level_import.csv'
  * Port-level exports:'port_level_export.csv'
* Produce regional port flow by O-D, commodity type and foreign trade region:
  * Regional imports: 'FAF_regional_import_(analysis_year}.csv'
  * Regional exports: 'FAF_regional_export_(analysis_year}.csv'
* Produce port location file within study area: 'port_location_in_region.geojson'

### step d.4 - international mode choice model
* TO BE ADDED

