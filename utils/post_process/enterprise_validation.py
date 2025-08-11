# -*- coding: utf-8 -*-
"""
Created on Mon Aug 11 10:55:42 2025

@author: xiaodanxu
"""

import pandas as pd
from pandas import read_csv
import os
from os import listdir
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from sklearn.metrics import root_mean_squared_error 
from sklearn.metrics import r2_score
import warnings
warnings.filterwarnings("ignore")

scenario_name = 'national'
out_scenario_name = 'national'
file_path = 'C:\SynthFirm'
parameter_dir = 'SynthFirm_parameters'
number_of_processes = 4
input_dir = 'inputs_' + scenario_name
output_path = 'outputs_' + out_scenario_name

os.chdir(file_path)
susb_file = os.path.join(parameter_dir, 'SUSB_msa_3digitnaics_2016.csv')
# county_to_msa_file = os.path.join(parameter_dir, 'county_msa_crosswalk.csv')
firm_enterprise_file = os.path.join(output_path, 'synthetic_enterprise.csv')

susb_data = read_csv(susb_file)
enterprise_df = read_csv(firm_enterprise_file)

# <codecell>
def plot_emp_comparison_scatter(df, x_col, y_col, agg_level,
                        plot_filepath, alpha = 0.3):
    # Calculate metrics
    rmse_emp = root_mean_squared_error(df[x_col], df[y_col])
    r2_emp = r2_score(df[x_col], df[y_col])
    rmse_emp = np.round(rmse_emp, 1)
    r2_emp = np.round(r2_emp, 2)
    
    # Print sums and metrics
    print('The total reported and modeled employment:')
    print(df[x_col].sum(), df[y_col].sum())
    print('The RMSE and R2 values:')
    print(rmse_emp, r2_emp)
        
    # Determine axis limits with a 5% margin
    max_val = max(df[x_col].quantile(0.999), df[y_col].quantile(0.999))
    axis_max = max_val * 1.05
    
    # Plot
    sns.lmplot(
        data=df, x=x_col, y=y_col, 
        height=4.5, aspect=1.2, 
        line_kws={'color': 'grey'}, 
        scatter_kws={'alpha':alpha}
    )
    plt.xlim([0, axis_max])
    plt.ylim([0, axis_max])
    plt.xlabel(x_col)
    plt.ylabel(y_col)
    plt.title(f'Results by {agg_level}, $R^{{2}}$ = {r2_emp} , RMSE = {rmse_emp}', fontsize=14)
    
    # Save
    plt.savefig(plot_filepath, dpi=200, bbox_inches='tight')
    plt.show()

# <codecell>
# check results by industry

enterprise_df = enterprise_df.loc[enterprise_df['INSUSB'] == 1]
modeled_emp_by_naics = \
enterprise_df.groupby('NAICS')[['emp_per_est']].sum().reset_index()
modeled_ent_by_naics = \
    enterprise_df.groupby('NAICS')[['FirmID', 'BusID']].count().reset_index()
modeled_ent_by_naics = pd.merge(modeled_ent_by_naics, modeled_emp_by_naics,
                                on = 'NAICS', how = 'left')

modeled_ent_by_naics.columns = ['NAICS', 'FIRM_Model', 'ESTB_Model', 'EMPL_Model']

susb_ent_by_naics = \
    susb_data.groupby('NAICS')[['FIRM', 'ESTB', 'EMPL']].sum().reset_index()

susb_ent_by_naics.columns = ['NAICS', 'FIRM_SUSB', 'ESTB_SUSB', 'EMPL_SUSB']

ent_by_naics_comparison = pd.merge(susb_ent_by_naics, modeled_ent_by_naics,
                                   on = 'NAICS', how = 'left')
   
plot_path = 'plots_' + out_scenario_name 
plot_filepath = os.path.join(plot_path, 'susb_firm_count_by_naics_validation.png')    
plot_emp_comparison_scatter(ent_by_naics_comparison, 
                            'FIRM_SUSB', 'FIRM_Model', 'NAICS',
                            plot_filepath, alpha = 1)

plot_filepath = os.path.join(plot_path, 'susb_bus_count_by_naics_validation.png')    
plot_emp_comparison_scatter(ent_by_naics_comparison, 
                            'ESTB_SUSB', 'ESTB_Model', 'NAICS',
                            plot_filepath, alpha = 1)

plot_filepath = os.path.join(plot_path, 'susb_emp_count_by_naics_validation.png')    
plot_emp_comparison_scatter(ent_by_naics_comparison, 
                            'EMPL_SUSB', 'EMPL_Model', 'NAICS',
                            plot_filepath, alpha = 1)

# <codecell>
# check results by MSA

# enterprise_df = enterprise_df.loc[enterprise_df['INSUSB'] == 1]
susb_data.loc[:, 'MSA Code'] =\
    'C' + susb_data.loc[:, 'MSA'].astype(str).str[0:4]
    
modeled_emp_by_msa = \
enterprise_df.groupby('MSA Code')[['emp_per_est']].sum().reset_index()
modeled_ent_by_msa = \
    enterprise_df.groupby('MSA Code')[['FirmID', 'BusID']].count().reset_index()
modeled_ent_by_msa = pd.merge(modeled_ent_by_msa, modeled_emp_by_msa,
                                on = 'MSA Code', how = 'left')

modeled_ent_by_msa.columns = ['MSA Code', 'FIRM_Model', 'ESTB_Model', 'EMPL_Model']

susb_ent_by_msa = \
    susb_data.groupby('MSA Code')[['FIRM', 'ESTB', 'EMPL']].sum().reset_index()

susb_ent_by_msa.columns = ['MSA Code', 'FIRM_SUSB', 'ESTB_SUSB', 'EMPL_SUSB']

ent_by_msa_comparison = pd.merge(susb_ent_by_msa, modeled_ent_by_msa,
                                   on = 'MSA Code', how = 'left')
   
plot_path = 'plots_' + out_scenario_name 
plot_filepath = os.path.join(plot_path, 'susb_firm_count_by_msa_validation.png')    
plot_emp_comparison_scatter(ent_by_msa_comparison, 
                            'FIRM_SUSB', 'FIRM_Model', 'NAICS',
                            plot_filepath, alpha = 1)

plot_filepath = os.path.join(plot_path, 'susb_bus_count_by_msa_validation.png')    
plot_emp_comparison_scatter(ent_by_msa_comparison, 
                            'ESTB_SUSB', 'ESTB_Model', 'NAICS',
                            plot_filepath, alpha = 1)

plot_filepath = os.path.join(plot_path, 'susb_emp_count_by_msa_validation.png')    
plot_emp_comparison_scatter(ent_by_msa_comparison, 
                            'EMPL_SUSB', 'EMPL_Model', 'NAICS',
                            plot_filepath, alpha = 1)