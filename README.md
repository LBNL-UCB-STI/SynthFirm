*** Copyright Notice ***

Firm Synthesizer and Supply-chain Simulator (SynthFirm) Copyright (c) 2024, The Regents of the University of California, through Lawrence Berkeley National Laboratory (subject to receipt of any required approvals from the U.S. Dept. of Energy). All rights reserved.

If you have questions about your rights to use or distribute this software,
please contact Berkeley Lab's Intellectual Property Office at
IPO@lbl.gov.

NOTICE.  This Software was developed under funding from the U.S. Department
of Energy and the U.S. Government consequently retains certain rights.  As
such, the U.S. Government has been granted for itself and others acting on
its behalf a paid-up, nonexclusive, irrevocable, worldwide license in the
Software to reproduce, distribute copies to the public, prepare derivative 
works, and perform publicly and display publicly, and to permit others to do so.



# SynthFirm Tutorial
<p>A quick overview of running SynthFirm for a selected region </p>
<p> Contact: Xiaodan Xu, Ph.D.  (XiaodanXu@lbl.gov) </p>
<p> Updates (Aug 14, 2023): documented synthetic firm, producer and consumer generation </p>
<p> Updates (Aug 22, 2023): documented B2B flow generation </p>
<p> Updates (Aug 29, 2023): added firm generation into SynthFirm pipeline and uploaded input data </p>

## Task 0 -- Input data generation and environment setup ##
* Please refer to this [input generation guide](input_generation/Readme.md) to prepare inputs for selected region
* Following instructions to prepare inputs needed for the selected region (or use data files shared by the team, POC: XiaodanXu@lbl.gov)
* Using pre-generated baseline inputs from [San Francisco Bay Area](input_data/inputs_BayArea.zip) and [Austin Area](input_data/Inputs_Austin.zip)
* Make sure Python and R are accessible through bash.  You can check the status of Python and R using the following scripts:
    ```
    Python3 --version
    ```
    and,
    ```
    R --version
    ```

## Task 1 -- Synthetic firm generation ##
* Define input path and files under the [R configure file](utils/config.R), with current inputs set up for San Francisco Bay Area

## Task 2 -- Synthetic B2B flow generation ##
* Define input path and files under the [Python configure file](SynthFirm.conf), with current inputs set up for San Francisco Bay Area
  * Fill in project information following this example:

    ```
    file_path = /Users/xiaodanxu/Documents/SynthFirm.nosync # path to project data
    scenario_name = BayArea # scenario name, must be consistent with firm generation to allow for models searching for the I-O paths
    out_scenario_name = BayArea # scenario name for output, can be different from input scenario name, but must be consistent with firm generation configs
    parameter_path = SynthFirm_parameters # parameter directory
    region_code = 62, 64, 65, 69 # list of FAF zone from the study region, for more information about the zonal id, please reference this guide: https://faf.ornl.gov/faf5/data/FAF5%20User%20Guide.pdf
    ```

  * Select the modules that are needed (must complete all of them in the following order, but can run one module at a time):

    ```
    enable_firm_generation = no # synthetic firm, producer and buyer generation
    enable_supplier_selection = no # supplier selection
    enable_size_generation = no # shipment size generation
    enable_mode_choice = no # mode choice model
    enable_post_analysis = yes # post mode choice analysis (truck flow and summary statistics)
    enable_fleet_generation = no # fleet generation (DO NOT ACTIVATE, NOT YET READY)
    ```

  * Fill in rest of the input file names (and do not change file structure defined in synthetic firm generation)
  * Finish preparing configure file!

* Run selected SynthFirm modules:
  * Open system Terminal/Shell, change directory to where the SynthFirm tool is located
  * Run [SynthFirm model](SynthFirm_run.py):

    ```
    python SynthFirm_run.py --config 'SynthFirm.conf'
    ```
  * The firm generation step is executed in R using [R master file](utils/run_firm_generation_master_R.R), users need to make sure the right path to project is defined in this file:
  ```
  path2code <- '/Users/xiaodanxu/Documents/GitHub/SynthFirm/utils/'
  setwd(path2code)
  ```
  * The rest steps are done in Python using values defined in the [configure file](SynthFirm.conf)
  * Check output following the prompt on screen
* You are done, cheers!

