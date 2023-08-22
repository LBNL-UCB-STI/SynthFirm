# SynthFirm Tutorial
<p>A quick overview of running SynthFirm for a selected region </p>
<p> Contact: Xiaodan Xu, Ph.D.  (XiaodanXu@lbl.gov) </p>
<p> Updates (Aug 14, 2023): documented synthetic firm, producer and consumer generation </p>


## data generation ##
* Please refer to this [input generation guide](input_generation/Readme.md) to prepare inputs for selected region
* Following instructions to prepare inputs needed for the selected region (or use data files shared by the team, POC: XiaodanXu@lbl.gov)

## Synthetic firm generation ##
* Define input path and files under the [configure file](utils/config.R)
* Open master run file in [R master file](utils/run_firm_generation_master_R.R) and define the path to project

```
path2code <- '/Users/xiaodanxu/Documents/GitHub/SynthFirm/utils/'
setwd(path2code)
```

* Run through the rest of code in R (will eventually merge this part into SynthFirm_run.py!)

## Synthetic B2B flow generation ##
* Define input path and files under the [configure file](SynthFirm.conf)
  * Fill in project information following this example:

    ```
    file_path = /Users/xiaodanxu/Documents/SynthFirm.nosync # path to project data
    scenario_name = BayArea # scenario name, must be consistent with firm generation to allow for models searching for the I-O paths
    parameter_path = SynthFirm_parameters # parameter directory
    region_code = 62, 64, 65, 69 # list of FAF zone from the study region, for more information about the zonal id, please reference this guide: https://faf.ornl.gov/faf5/data/FAF5%20User%20Guide.pdf

    ```

  * select the modules that are needed:

    ```
    enable_supplier_selection = no # supplier selection
    enable_size_generation = no # shipment size generation
    enable_mode_choice = no # mode choice model
    enable_post_analysis = yes # post mode choice analysis (truck flow and summary statistics)
    enable_fleet_generation = no # fleet generation (DO NOT ACTIVATE, NOT YET READY)
    ```

  * fill in rest of the input file names (and do not change file structure defined in synthetic firm generation)
  * finish preparing configure file!

* Run selected SynthFirm modules:
  * In terminal/Shell, direct to where the SynthFirm tool is located
  * Run [SynthFirm model](SynthFirm_run.py):

    ```
    python SynthFirm_run.py --config 'SynthFirm.conf'
    ```

  * Check output following the prompt on screen
* You are done, cheers!


