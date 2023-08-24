# Config file to run SynthFirm firm generation and 

## define path to project directory
path2file <- "/Users/xiaodanxu/Documents/SynthFirm.nosync" # path to project folder
number_of_cores <- 8 # register number of cores for location generation

## define scenario name
scenario <- 'BayArea' # scenario name, the same as input generation process

## define model inputs 

cbp_file <- "data_emp_cbp_imputed.csv" # county-level employment data
mzemp_file <-"data_mesozone_emprankings.csv" # CBG-level employment ranking
zonal_id_file <- "zonal_id_lookup_final.csv" # zonal ID lookup table 

## define model parameters
c_n6_n6io_sctg_file <-"corresp_naics6_n6io_sctg_revised.csv" # industry to commodity lookup table (pre-defined)
employment_per_firm_file <- "employment_by_firm_size_naics.csv" # average employment by firm size group and industry
employment_per_firm_gapfill_file <- "employment_by_firm_size_gapfill.csv" # average employment by firm (all industry combined)
foreign_prod_file <- "data_foreign_prod.csv" # total foreign production (pre-defined from prior studies)
foreign_cons_file <- "data_foreign_cons.csv" # total foreign consumption (pre-defined from prior studies)
BEA_io_2017_file <- "data_2017io_revised_USE_value_added.csv" # final scaled BEA I-O use table
agg_unit_cost_file <- "data_unitcost_cfs2017.csv" # unit cost by commodity from CFS 2017 (all zones combined)
prod_by_zone_file <- "producer_value_fraction_by_faf.csv" # Total production value by FAF zone from CFS 2017
cons_by_zone_file <- "consumer_value_fraction_by_faf.csv" # Total consumption value  by FAF zone from CFS 2017
SCTG_group_file <- "SCTG_Groups_revised.csv" # Commodity type to group lookup (pre-defined)

# define constant
foreignprodcostfactor <- 0.9     # producer cost factor for foreign produers (applied to unit costs) 
wholesalecostfactor    <- 1.2     # markup factor for wholesalers (applied to unit costs)

## define output name
synthetic_firms_no_location_file <- "synthetic_firms.csv" # synthetic firm output (before location assignment)
io_summary_file <- "io_summary_revised.csv" # i-o table in long format, without zero values
synthetic_wholesaler_file <- "synthetic_wholesaler.csv" # synthetic wholesaler (serve as both buyer and supplier)
synthetic_producer_file <- "synthetic_producers.csv" # synthetic producer
synthetic_producer_by_sctg_filehead <- "prods_sctg" # file head for synthetic producer by sctg
io_filtered_file <- "data_2017io_filtered.csv" # processed I-O table, after dropping wholesale transaction
synthetic_consumer_file <- "synthetic_consumers.csv" # synthetic consumer
sample_synthetic_consumer_file <- "sample_synthetic_consumers.csv" # sample synthetic consumer (for results checking)
synthetic_consumer_by_sctg_filehead <- "consumers_sctg" # file head for synthetic consumer by sctg
synthetic_firms_with_location_file <- "synthetic_firms_with_location.csv" # synthetic firm output (after location assignment)

