
rm(list = ls())
path2code <- '/Users/xiaodanxu/Documents/GitHub/SynthFirm/utils/'
setwd(path2code)
source('step0_SynthFirm_starter.R') # load packages
# source('scenario/scenario_variables.R')  # load environmental variable

# step 1 -- synthetic firm generation
source('config.R')  # load input settings 
source('Step1_Firm_Generation.R')
rm(list = ls())

# step 2 -- synthetic producer (including wholesaler) generation
source('config.R')  # load input settings 
source('Step2_Producer_Generation.R')
rm(list = ls())

# step 3 -- synthetic consumer generation
source('config.R')  # load input settings 
source('Step3_Consumer_Generation.R')
rm(list = ls())

# step 4 -- firm location generation
source('config.R')  # load input settings 
source('Step4_Firm_Location_Generation.R')
rm(list = ls())

# step 5 -- summarize firm generation statistics
source('config.R')  # load input settings 
source('Step5_Result_Summary.R')
rm(list = ls())

