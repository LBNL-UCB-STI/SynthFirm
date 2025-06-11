
# Fleet generation and emission analysis
<p> <b>Supplementary code and analysis for paper 'Improving Commercial Truck Fleet Composition in Emission Modeling using 2021 US VIUS Data' </b> </p>
<p> <b>Contact</b>: Xiaodan Xu, Ph.D.  (XiaodanXu@lbl.gov) </p>
<p> <b>Updates (June 11, 2025)</b>: code uploaded, cleaned and data uploaded </p>
<p> Dataset can be downloaded: 10.5281/zenodo.15643435 </p>

## Part 1 -- VIUS data cleaning and vehicle type assignment ##
* Corresponding to Section 2.1 of the paper
* Download VIUS PUF file and data dictionary from BTS website:https://www.census.gov/data/datasets/2021/econ/vius/2021-vius-puf.html
* Perform data cleaning and variable generation using [VIUS2021_commercial fleet generation_clean](VIUS2021_commercial fleet generation_clean.ipynb)
 * Writing VIUS data output with generated attributes: 'vius_2021_com_crosswalk_20240624.csv'


## Part 2 -- Baseline fleets generation ##

### 2.1 -- MOVES baseline fleet generation ###
* Corresponding to Section 2.2.1 of the paper
* Run the MOVES fleet generation code [baseline_fleet_generation_from_moves](baseline_fleet_generation_from_moves.py) 
* Producing MOVES fleet output file: 'MOVES_VMT_fraction_with_fuel_com_only.csv'

### 2.2 -- VIUS baseline fleet generation ###
* Corresponding to Section 2.2.2 of the paper
* Run the VIUS fleet generation code [baseline_fleet_generation_from_vius](baseline_fleet_generation_from_vius.py)
* Relying on output from 2.1 for age distribution imputation (age > 23 yr)
* Producing VIUS fleet output file: ''VIUS_VMT_fraction_with_fuel_com_only.csv'

## Part 3 -- Fleet forecast ###

### 3.1 -- rate-based fleet turnover ###
* Corresponding to Section 2.3.1 of the paper
  * Vehicle population turnover from MOVES base fleet: [rate_based_fleet_projection_moves](rate_based_fleet_projection_moves.py)
  * Vehicle population turnover from VIUS base fleet:
  [rate_based_fleet_projection_vius](rate_based_fleet_projection_vius.py)
  * VMT composition from projected MOVES fleet:
  [vmt_fraction_forecast_moves](vmt_fraction_forecast_moves.py)
  * VMT composition from projected VIUS fleet:
  [vmt_fraction_forecast_vius](vmt_fraction_forecast_vius.py)


### 3.2 -- fuel mix generation ###
* Corresponding to Section 2.3.2 of the paper
* The raw TITAN results cannot be shared due to data restrictions. The AVFT (alternative vehicle fuel type) generation code can be accessed [AVFT_from_TDA](AVFT_from_TDA.py), and the results are available under the 'Parameter/turnover' folder under shared data (Note: TDA is the previous project name acronym for TITAN project)
* The scenario description can be found under file [opcost_sensitivity_analysis](parameters/opcost_sensitivity_analysis.csv)
* The AVFT results are combined with VMT distributions from 3.1 above using script [compile_fleet_distribution_forecast](compile_fleet_distribution_forecast.py)


## Part 4 -- Fleet composition comparison ###
* Corresponding to Section 3 of the paper
* The base year fleet compositions are compared under [vius_moves_fleet_comparison](vius_moves_fleet_comparison.ipynb)
* The future fleet results are generated in 3.2 above

## Part 5 -- Emission comparison ###
* Corresponding to Section 4 of the paper
* The base year emission results are calculated and compared under [compare_emission_moves_vius_hpms_ver](compare_emission_moves_vius_hpms_ver.py)
* The future year emission results are calculated and compared under [compare_emission_forecast](compare_emission_forecast.py)





