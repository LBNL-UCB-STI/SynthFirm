# SynthFirm Input Data Generation
<p>This folder include scripts that are used to generate SynthFirm inputs from raw data sources under various geographic resolution </p>
<p> Contact: Xiaodan Xu, Ph.D.  XiaodanXu@lbl.gov </p>
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
* Require outputs {region_name}_FAFCNTY.csv and data_emp_cbp.csvfrom step b.2 
* Produce imputed firm and employment output data_emp_cbp_imputed.csv

### step b.4 - generate CBG tier and ranking for each industry CBP_EMPRANKS.R
* Require FAF zone county lookup table ({region_name}_FAFCNTY.csv) from step b.2- FAFZone.R 
* Require state employment data ({state}_naics.csv) from step b.1 - Census.R
* Produce ranking of employment by industry within each MESOZONE (data_mesozone_emprankings.csv)

## uncleaned part below

BEA_IO_processor.py
* Require BEA I-O data from multiple years as inputs
* Produce balanced I-O data as output 'data_2017io_revised.csv'

FreightGIS.R
* generate shapefile of the study area and rest of the U.S.

DistMat.R
* No longer needed (replaced with Skim generation under mode choice)
