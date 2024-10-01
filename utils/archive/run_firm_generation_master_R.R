
rm(list = ls())
path2code <- '.'
setwd(path2code)
source('utils/step0_SynthFirm_starter.R') # load packages
# source('scenario/scenario_variables.R')  # load environmental variable

# step 1 -- synthetic firm generation
source('utils/config.R')  # load input settings 
source('utils/Step1_Firm_Generation.R')
rm(list = ls())

# step 2 -- synthetic producer (including wholesaler) generation
source('utils/config.R')  # load input settings 
source('utils/Step2_Producer_Generation.R')
rm(list = ls())

# step 3 -- synthetic consumer generation
source('utils/config.R')  # load input settings 
source('utils/Step3_Consumer_Generation.R')
rm(list = ls())

# step 4 -- firm location generation
source('utils/config.R')  # load input settings 
source('utils/Step4_Firm_Location_Generation.R')
rm(list = ls())

# step 5 -- summarize firm generation statistics
source('utils/config.R')  # load input settings 
source('utils/Step5_Result_Summary.R')
rm(list = ls())

