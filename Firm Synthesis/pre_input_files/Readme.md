Census.R: 

	- No input data required
	
	- produce employment count by census block group and industry (ca_naics.csv)
	
	- Produce census block group shape file within selected states (ca_bg.geojson)

FAFZone.R (using CBP) and FAFZones_no_cbp.R (using firm data outside CBP for gap filling)

	- Require CFS_AREA (CFS_Areas.geojson) and CFS_FAF_LOOKUP (CFS_FAF_LOOKUP.csv) as input (no need to update when transferring scenarios)
	
	- Require CBP complete county file from census.gov (sample: https://www.census.gov/data/datasets/2017/econ/cbp/2017-cbp.html)
	- Require department of labor employment data (https://www.bls.gov/cew/downloadable-data-files.htm)
	
	- Produce county and FAF zone lookup table within study area (SFBay_FAFCNTY.csv)
	
	- Produce county establishment file (data_emp_cbp.csv)

CBP_EMPRANKS.R

	- Require FAF zone county lookup table (SFBay_FAFCNTY.csv) from FAFZone.R 
	
	- Require state employment data (ca_naics.csv) from Census.R
	
	- Produce ranking of employment by industry within each MESOZONE (data_mesozone_emprankings.csv)

employment_count_by_firm.R
	- Require CENSUS employment data by firm size (https://www.census.gov/programs-surveys/susb.html)
	- Produce average employment per firm by firm size group and industry'employment_by_firm_size_naics.csv'
	- Produce average employment per firm by firm size group for industries without observed data 'employment_by_firm_size_naics.csv'


BEA_IO_processor.py
	-Require BEA I-O data from multiple years as inputs
	- Produce balanced I-O data as output 'data_2017io_revised.csv'

FreightGIS.R

	- generate shapefile of the study area and rest of the U.S.

DistMat.R

	- No longer needed (replaced with Skim generation under mode choice)
