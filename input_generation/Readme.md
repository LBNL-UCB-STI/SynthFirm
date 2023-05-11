# SynthFirm Input Data Generation
<p>This folder include scripts that are used to generate SynthFirm inputs from raw data sources under various geographic resolution </p>
<p> Contact: Xiaodan Xu, Ph.D.  (XiaodanXu@lbl.gov) </p>
<p> Update (May 8, 2023): documented firm and employment data generation </p>

## a. national-level input generation (regardless of study area)

### step a.1 - processing CFS zone (cfs_to_faf_map_generation.R):
* Using input geographic data from CFS2017 (https://www.census.gov/programs-surveys/cfs/technical-documentation/geographies.html) and CFS2017-FAF5 mapping (https://faf.ornl.gov/faf5/)
* Combine CFS county data with CFS county to metro area lookup table
* Combine CFS metro area with FAF zone
* Produce county shapefile with CFS county, metro area and FAF zone (CFS2017_areas.geojson)
  
### step a.2 - generate initial employment count per firm by firm size at national level (employment_count_by_firm.R):
* Require CENSUS employment data by firm size (https://www.census.gov/programs-surveys/susb.html)
* Produce average employment per firm by firm size group  'employment_by_firm_size_gapfill.csv'
* Produce average employment per firm by firm size group and industries 'employment_by_firm_size_naics.csv' 
* While these two tables are not directly associated with synthetic firm output due to IPF fitting, it is needed to initiate firm synthesize and provide initial employment values.

## step a.3 - project BEA I-O table for baseline year (BEA_IO_processor.py)
* Require BEA I-O use data from multiple years as inputs
  * 2012 I-O with 405 industries (also contains industry mapping): https://docs.google.com/viewer?url=https%3A%2F%2Fapps.bea.gov%2Findustry%2Fxls%2Fio-annual%2FUse_SUT_Framework_2007_2012_DET.xlsx
  * 2017 I-O with 71 industries:https://apps.bea.gov/iTable/?reqid=150&step=3&isuri=1&table_list=6009&categories=io (modify year to baseline = 2017)
* For 2017 I-O, in addition to industry-specific values, it is critical to include added value (last row) during production.  This value indicates the value of commodity gained during production and paid by consumers.  Without this the final I-O will be significantly under-estimated. BEA:The sum of the entries in a column is that industry’s output.
* Project 2012 I-O value to 2017 industry total, to obtain I-O values by all 405 industries
* Produce projected baseline I-O data as output 'data_2017io_revised_USE_value_added.csv'

## step a.4 - collect commodity flow distributions from CFS data (CFS_distribution_generator.py)
* Require CFS 2017 public use data as input (cfs-2017-puf-csv.csv):https://www.census.gov/data/datasets/2017/econ/cfs/historical-datasets.html
* Require several predefined lookup tables:
  * industy and commodity look up table (from MAG model): corresp_naics6_n6io_sctg_revised.csv
  * CFS and FAF zone lookup from FAF5 document: CFS_FAF_LOOKUP.csv (downloaded from FAF5 website under 'Related Information': https://faf.ornl.gov/faf5/)
  * commodity SCTG group definition: SCTG_Groups_revised.csv (defined by research team)
* Produce the following 5 outputs:
  * Unit cost by commodity: data_unitcost_cfs2017.csv
  * Unit cost by CFS zone and commodity: data_unitcost_by_zone_cfs2017.csv
  * Shipment size with pre-defined percentile cut-off criteria: max_load_per_shipment_{X}percent.csv -> x is pre-defined quantile value to select max load by sctg 
  * Production/consumption value allocation factor by FAF zone: producer_value_fraction_by_faf.csv AND consumer_value_fraction_by_faf.csv
  * Seasonal load allocation factor by SCTG group for national freight trip generation (future work): seasonal_allocation_factor_by_sctg.csv
  
## step a.5 - estimate supplier selection model parameter (supplier_selection_model_estimation.py)
* Require CFS 2017 imputed data as input (CFS2017_national_forML_short.csv)
* Require unit cost estimation by zone and sctg from step a.4 (data_unitcost_by_zone_cfs2017)
* Require several predefined lookup tables:
  * CFS and FAF zone lookup from FAF5 document: CFS_FAF_LOOKUP.csv (downloaded from FAF5 website under 'Related Information': https://faf.ornl.gov/faf5/)
  * commodity SCTG group definition: SCTG_Groups_revised.csv (defined by research team)
* Perform MNL model using CFS O-D data for supplier selection
* Produce the following 5 outputs:
  * Avg. routed distance matrix from CFS observation: CFS2017_routed_distance_matrix.csv
  * Estimated model parameters: supplier_selection_national_sctg{sctg_group_id}.html



## b.study area input generation (need to specify study region)

### step b.1 - collecting data from Census API (Census.R): 
* No input data required (need Census API key)
* produce employment count by census block group and industry ({state}_naics.csv)
* Produce census block group shape file within selected states ({state}_bg.geojson)

### step b.2 - collect employment data using FAFZone.R (using CBP): 
* Require CFS to FAF crosswalk map from step a.1 (CFS2017_areas.geojson) and CFS zone to FAF zone lookup table (CFS_FAF_LOOKUP.csv) as input (downloaded from FAF5 website under 'Related Information': https://faf.ornl.gov/faf5/)
* Require CBP complete county file from census.gov (sample: https://www.census.gov/data/datasets/2017/econ/cbp/2017-cbp.html)
* Produce county and FAF zone lookup table within study area ({region_name}_FAFCNTY.csv)
* Produce county establishment file before imputation (data_emp_cbp.csv)
	
### step b.3 - impute firms and employment for missing industry FAFZones_no_cbp.R (using firm data outside CBP for gap filling)
* Require department of labor employment data for imputing firm count and employment of missing industries in CBP (https://www.bls.gov/cew/downloadable-data-files.htm)
* Require outputs {region_name}_FAFCNTY.csv and data_emp_cbp.csv from step b.2 
* Produce imputed firm and employment output data_emp_cbp_imputed.csv

### step b.4 - generate CBG tier and ranking for each industry CBP_EMPRANKS.R
* Require FAF zone county lookup table ({region_name}_FAFCNTY.csv) from step b.2- FAFZone.R 
* Require state employment data ({state}_naics.csv) from step b.1 - Census.R
* Produce ranking of employment by industry within each MESOZONE (data_mesozone_emprankings.csv)
* Produce mesozone - census GEOID lookup table (MESOZONE_GEOID_LOOKUP.csv)

### step b.5 generate shapefile of the study area and rest of the U.S. (FreightGIS.R)
* Require the following zonal shapefile and lookup tables: 
  * Require FAF5 zonal shapefile (FAF5Zones.geojson) from step a.1
  * Require state census block group shapefile ({state}_bg.geojson) from step b.1
  * Require FAF zone - county look up file {region_name}_FAFCNTY.csv from step b.2
  * Require mesozone - census GEOID look up file MESOZONE_GEOID_LOOKUP.csv from step b.4
* Produce shapefile of model spatial resolution: {region_name}_freight.geojson
* Produce centroids of modeled zones: {region_name}_freight_centroids.geojson
