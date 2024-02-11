
# Model Estimation for Baseline MNL and Advanced MNL

## Contact: Xiaodan Xu (XiaodanXu@lbl.gov) and Hung-Chia Yang (hcyang@lbl.gov)

**Overview:** This folder contains the data and codes used to estimate the multinomial logit model (MNL) results, Machine Learning (ML) models, and SHAP interpretations in **"Teaching Freight Mode Choice Models New Tricks Using Interpretable Machine Learning Methods"**.


# Data File Description

**'cfs_2017.csv'** used in data preparation step is the 2017 Commodity Flow Survey (CFS) public use file.
The raw CFS 2017 public use data can also be downloaded online:
https://www.census.gov/data/datasets/2017/econ/cfs/historical-datasets.html
click 'CFS 2017 PUF CSV [118.8 MB]' and the download will start

# Code File Description

## Main modules

*  **MNL and ML data preparation:** ['CFS2017_Austin_dataprep_for_biogeme.ipynb'](CFS2017_Austin_dataprep_for_biogeme.ipynb) includes data cleaning and variable generation steps to prepare the dataset for MNL and ML estimation.

* **MNL model specification and estimation:** ['CFS2017_biogeme_MNL_Austin_MLpaper_clean.ipynb'](CFS2017_biogeme_MNL_Austin_MLpaper_clean.ipynb) specifies both the baseline and advanced MNL models and generates estimation results using Biogeme package. 

* **ML model training and SHAP interpretations:** ['MachineLearningMethod_2017_training.ipynb'](MachineLearningMethod_2017_training.ipynb) trains various machine learning models and generate SHAP intepretations for best-performing ML models.

## Data exploration and results visualization

* **Mode split visualization:** ['plot_sample_size_pie.ipynb'](plot_sample_size_pie.ipynb) provides mode split visualizations for Austin freight mode choice.

* **Exploratory data analysis (EDA):** ['mode_choice_explore_austin.ipynb'](mode_choice_explore_austin.ipynb) provides EDA results for Austin freight mode choice.

* **Performance measures (with output from main modules):** ['accuracy_plot.ipynb'](accuracy_plot.ipynb) provides performance measures for MNL and ML models.

## Useful Links

[2017 Commodity Flow Survey (CFS)](https://www.census.gov/data/datasets/2017/econ/cfs/historical-datasets.html)

[Biogeme Documentation](https://biogeme.epfl.ch/sphinx/index.html)

[SHAP Documentation](https://shap.readthedocs.io/en/latest/index.html)