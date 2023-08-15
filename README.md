# SynthFirm Tutorial
<p>A quick overview of running SynthFirm for a selected region </p>
<p> Contact: Xiaodan Xu, Ph.D.  (XiaodanXu@lbl.gov) </p>
<p> Updates (Aug 14, 2023): documented synthetic firm, producer and consumer generation </p>


## data generation ##
* Please refer to [a relative link](input_generation/Readme.md) to prepare inputs for selected region
* Following instructions to prepare inputs needed for the selected region (or use data files shared by the team, POC: XiaodanXu@lbl.gov)

## Synthetic firm generation ##
* define input path and files under the configure file [a relative link](utils/config.R)
* open [a relative link](utils/run_firm_generation_master_R.R) and define the path to project
'''
path2code <- '/Users/xiaodanxu/Documents/GitHub/SynthFirm/utils/'
setwd(path2code)
'''
* run through the rest of code in R (will eventually create a run_model.sh to replace this!)