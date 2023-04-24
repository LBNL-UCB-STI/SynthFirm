
#-----------------------------------------------------------------------------------
#Define file paths for inputs, outputs, workspaces and log files
#-----------------------------------------------------------------------------------
print("Defining file names for inputs, outputs, workspaces and log files")

#-----------------------------------------------------------------------------------
#Inputs
#-----------------------------------------------------------------------------------

##1 Firm Synthesis Inputs
firmsyn[["inputs"]] <- within(firmsyn[["inputs"]], {
  c_n6_n6io_sctg                  <-     "corresp_naics6_n6io_sctg.csv"              #Correspondence between NAICS 6-digit, I/O NAICS, and SCTG
  cbp                             <-     "data_emp_cbp.csv"              #CBP data file
  for_prod                        <-     "data_foreign_prod.csv"         #Foreign producers
  for_cons                        <-     "data_foreign_cons.csv"         #foreign consumers
  io                              <-     "data_2010io.csv"
  unitcost                        <-     "data_unitcost.csv"
  prefweights                     <-     "data_firm_pref_weights.csv"
  mzemp                           <-     "data_mesozone_emprankings.csv"      #Industry rankings data by mesozone based on employment
})
  
#-----------------------------------------------------------------------------------
#Workspaces
#-----------------------------------------------------------------------------------

model[["workspace"]]                        <-   "Model.RData"
firmsyn[["workspace"]]                      <-   "Step1_FirmSynthesis.RData"
pmg[["workspace"]]                          <-   "Step2_ProcurementMarkets.RData"
pmgcon[["workspace"]]                       <-   "Step3_PMGController.RData"
pmgout[["workspace"]]                       <-   "Step4_PMGOutputs.RData"
daysamp[["workspace"]]                      <-   "Step5_DailySample.RData"
whouse[["workspace"]]                       <-   "Step6_WarehouseAllocation.RData"
vehtour[["workspace"]]                      <-   "Step7_VehicleChoiceTourPattern.RData"
stopseq[["workspace"]]                      <-   "Step8_StopSequence.RData"
stopdur[["workspace"]]                      <-   "Step9_StopDuration.RData"
tourtod[["workspace"]]                      <-   "Step10_TimeofDay.RData"
preptt[["workspace"]]                       <-   "Step11_TripTable.RData"

#-----------------------------------------------------------------------------------
#Runtimes and Log File Locations
#-----------------------------------------------------------------------------------
model[["logs"]] <- list()
model[["logs"]] <- within(model[["logs"]], {
  Step_RunTimes                               <-   "RunTimes.csv"
  Main_Log                                    <-   "Main_Log.txt"
  Profile_Log                                 <-   "Profile.out"
  Profile_Summary                             <-   "Profile_Summary.txt"
})


