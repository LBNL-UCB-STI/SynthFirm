#################################################################################
rm(list = ls())
options(scipen = '10')
#################################################################################

#---------------------------------------------------------------------
#Define Model/Scenario Control Variables/Inputs/Packages/Steps
#---------------------------------------------------------------------
basedir <-
  "/Users/xiaodanxu/Documents/GitHub/SynthFirm/Firm Synthesis"
setwd(basedir)
scenario <- "baseline"

source("./scripts/0_rFreight_Functions.R")

#define the components that comprise the model: name, titles, scripts
steps <-
  c(
    "firmsyn",
    "pmg",
    "pmgcon",
    "pmgout",
    "daysamp",
    "whouse",
    "vehtour",
    "stopseq",
    "stopdur",
    "tourtod",
    "preptt"
  )
steptitles <-
  c(
    "Firm Synthesis",
    "Procurement Market Games",
    "PMG Controller",
    "PMG Outputs",
    "Daily Sample",
    "Warehouse Allocations",
    "Vehicle Choice and Tour Pattern",
    "Stop Sequence",
    "Stop Duration",
    "Time of Day",
    "Prepare Trip Table"
  )
stepscripts <-
  c(
    "01_Firm_Synthesis.R",
    "02_Procurement_Markets.R",
    "03_PMG_Controller.R",
    "04_PMG_Outputs.R",
    "05_Daily_Sample.R",
    "06_Warehouse_Allocation_CMAP.R",
    "07_Vehicle_Choice_Tour_Pattern_CMAP.R",
    "08_Stop_Sequence_CMAP.R",
    "09_Stop_Duration.R",
    "10_Time_of_Day.R",
    "11_Prepare_Trip_Table_CMAP.R"
  )

#create the model list to hold that information, load packages, make model step lists
model <- startModel(
  basedir = basedir,
  scenarioname = scenario,
  packages = c(
    "dplyr",
    "dtplyr",
    "data.table",
    "bit64",
    "reshape",
    "reshape2",
    "ggplot2",
    "fastcluster"
  ),
  steps = steps,
  steptitles = steptitles,
  stepscripts = stepscripts
)
#rm(basedir,scenario,steps,steptitles,stepscripts)

#Load file paths to model inputs, outputs, and workspaces
#source("./scripts/0_File_Locations.R")

#-----------------------------------------------------------------------------------
#Run model steps
#-----------------------------------------------------------------------------------

source(model$stepscripts[1])
#source(model$stepscripts[2])

save(
  list = c("model", model$steps),
  file = file.path(model$outputdir, "modellists.Rdata")
)
