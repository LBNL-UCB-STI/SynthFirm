
# Model Estimation for Baseline MNL and Advanced MNL

This folder contains the data and codes used to estimate the multinomial logit model results presented in Table 2 and 3 in "Teaching Freight Mode Choice Models New Tricks Using Interpretable Machine Learning Methods".


## Data File Description

'cfs_2017.csv' is the 2017 Commodity Flow Survey (CFS) public use file.
The raw CFS 2017 public use data can also be downloaded online:
https://www.census.gov/data/datasets/2017/econ/cfs/historical-datasets.html
click 'CFS 2017 PUF CSV [118.8 MB]' and the download will start
## Code File Description

* ['CFS2017_Austin_dataprep_for_biogeme.ipynb'](CFS2017_Austin_dataprep_for_biogeme.ipynb) includes data cleaning and variable generation steps to prepare the dataset for biogeme.

* ['CFS2017_biogeme_MNL_Austin_MLpaper.ipynb'](CFS2017_biogeme_MNL_Austin_MLpaper.ipynb) specifies both the baseline and advanced MNL models and generates estimation results using Biogeme package. 

## Useful Links

[2017 Commodity Flow Survey (CFS)](https://www.census.gov/data/datasets/2017/econ/cfs/historical-datasets.html)

[Biogeme Documentation](https://biogeme.epfl.ch/sphinx/index.html)