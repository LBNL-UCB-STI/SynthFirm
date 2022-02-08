Census.R: 

	- No input data required
	
	- produce employment count by census block group and industry (ca_naics.csv)
	
	- Produce census block group shape file within selected states (ca_bg.geojson)

FAFZone.R

	- Require CFS_AREA (CFS_Areas.geojson) and CFS_FAF_LOOKUP (CFS_FAF_LOOKUP.csv) as input (no need to update when transferring scenarios)
	
	- Require CBP complete county file from census.gov (sample: https://www.census.gov/data/datasets/2017/econ/cbp/2017-cbp.html)
	
	- Produce county and FAF zone lookup table within study area (SFBay_FAFCNTY.csv)
	
	- Produce county establishment file (data_emp_cbp.csv)

CBP_EMPRANKS.R

	- Require FAF zone county lookup table (SFBay_FAFCNTY.csv) from FAFZone.R 
	
	- Require state employment data (ca_naics.csv) from Census.R
	
	- Produce ranking of employment by industry within each MESOZONE (data_mesozone_emprankings.csv)

FreightGIS.R

	- Not really needed

DistMat.R

	- No longer needed (replaced with Skim generation under mode choice)
