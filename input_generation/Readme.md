# SynthFirm Input Data Generation
<p>This folder include scripts that are used to generate SynthFirm inputs from raw data sources under various geographic resolution </p>
<p> Contact: Xiaodan Xu, Ph.D.  (XiaodanXu@lbl.gov) </p>
<p> Updates (May 8, 2023): documented firm and employment data generation </p>
<p> Updates (May 11, 2023): documented CFS processing, supplier selection, I-O projection, and map generation </p>
<p> Updates (Aug 08, 2023): documented firm fleet generation </p>
<p> Updates (Oct 14, 2024): update documents to reflect V2 changes and improve readability </p>

**List of Modules**
<!--ts-->
* [National-level input generation ](#a---national-level-input-generation)
* [Study area input generation](#b---study-area-input-generation)
* [Travel demand](#c---demand-attributes)
* [Land use](#d---land-use)

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
  
## B - study area input generation

## Inputs that need to be executed for study region

### step b.1 - collecting data from Census API in [Census.R](Census.R): 
* No input data required (need Census API key)
* produce employment count by census block group and industry ({state}_naics.csv)
* Produce census block group shape file within selected states ({state}_bg.geojson)

### step b.2 - collect CBP employment data using [FAFZone.R](FAFZone.R): 
* Require CFS to FAF crosswalk map from step a.1 (CFS2017_areas.geojson) and CFS zone to FAF zone lookup table (CFS_FAF_LOOKUP.csv) as input (downloaded from FAF5 website under 'Related Information': https://faf.ornl.gov/faf5/)
* Require CBP and ZBP (industry detail and total) complete county file from census.gov (sample: https://www.census.gov/data/datasets/2017/econ/cbp/2017-cbp.html)
* Produce county, zipcode and FAF zone lookup table within study area ({region_name}_FAFCNTY.csv)
* Produce county/zipcode establishment file before imputation (data_emp_cbp.csv)
	
### step b.3 - impute firms and employment for missing industry in [FAFZones_no_cbp.R](FAFZones_no_cbp.R) (using firm data outside CBP for gap filling)
* Require department of labor employment data for imputing firm count and employment of missing industries in CBP (https://www.bls.gov/cew/downloadable-data-files.htm)
* Require outputs {region_name}_FAFCNTY.csv and data_emp_cbp.csv from step b.2 
* Produce imputed firm and employment output data_emp_cbp_imputed.csv

### step b.4 - generate CBG tier and ranking for each industry in [CBP_EMPRANKS.R](CBP_EMPRANKS.R)
* Require FAF zone county lookup table ({region_name}_FAFCNTY.csv) from step b.2- FAFZone.R 
* Require state employment data ({state}_naics.csv) from step b.1 - Census.R
* Produce ranking of employment by industry within each MESOZONE (data_mesozone_emprankings.csv)
* Produce mesozone - census GEOID lookup table (MESOZONE_GEOID_LOOKUP.csv)

### step b.5 generate shapefile of the study area and rest of the U.S. in [FreightGIS.R](FreightGIS.R):
* Require the following zonal shapefile and lookup tables: 
  * Require FAF5 zonal shapefile (FAF5Zones.geojson) from step a.1
  * Require state census block group shapefile ({state}_bg.geojson) from step b.1
  * Require FAF zone - county look up file {region_name}_FAFCNTY.csv from step b.2
  * Require mesozone - census GEOID look up file MESOZONE_GEOID_LOOKUP.csv from step b.4
* Produce shapefile of model spatial resolution: {region_name}_freight.geojson
* Produce centroids of modeled zones: {region_name}_freight_centroids.geojson

### step b.6 generate final mesoscopic zonal lookup table in [zonal_data_processor.py](zonal_data_processor.py):
* Require the following zonal shapefile and lookup tables: 
  * Require CFS and FAF zone lookup from FAF5 document: CFS_FAF_LOOKUP.csv from step a.5
  * Require FAF zone - county look up file {region_name}_FAFCNTY.csv from step b.2
  * Require shapefile of model spatial resolution: {region_name}_freight.geojson from step b.5
* Produce mesoscopic zonal lookup table: zonal_id_lookup_final.csv

## c.firm fleet generation (using private data sources, under folder: fleet_generation)

### step c.1 processing IHS vehicle registration data from 2014 in [vehicle_registration_data_processor.py](fleet_generation/vehicle_registration_data_processor.py):
* Require the following inputs: 
  * Require state-level vehicle registration data from NREL (POC: Alicia Birky: Alicia.Birky@nrel.gov)
* Produce private, for-hire and for-leasing fleet distribution (MD/HD) for selected state:
  * Private fleet: '{state}_private_fleet_size_distribution.csv'
  * For-hire fleet: '{state}_for_hire_fleet_size_distribution.csv'
  * For-leasing fleet: '{state}_for_lease_fleet_size_distribution.csv'
  
### step c.2 processing cargo type from 2022 FMCSA Carrier Census data in [carrier_cargo_type_analysis.py](fleet_generation/carrier_cargo_type_analysis.py):
* Require the following inputs: 
  * Require FMCSA Census data (requested from: https://ask.fmcsa.dot.gov/app/mcmiscatalog/d_census_mcmis_doc#top)
* Produce probability of cargo groups for each for-hire carrier:
  * 'probability_of_cargo_group.csv'
  
### step c.3 processing state-by-state fleet projection from NREL TDA analysis in [national_fleet_generation_TDA.py](fleet_generation/national_fleet_generation_TDA.py):
* Require the following inputs: 
  * Require state-level MD/HD split from NREL: 'MDHDbyState.csv'
  * Require national fleet projection from NREL TDA analysis: 'TDA_results/BEAMFreightSensitivity_HOPmod.xlsx'
  * Manually define list of scenarios from NREL TDA analysis
* Produce fleet stock and distributions: 
  * national vehicle stock by year: {scenario}/TDA_vehicle_stock.csv'
  * state-level vehicle composition by year: {scenario}/fleet_composition_by_state.csv'
  * EV powertrain distribution by vehicle type: {scenario}/EV_availability.csv'
