#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb  5 10:17:52 2025

@author: xiaodanxu
"""

import pandas as pd
import sqlite3
# import geopandas as gps
# import matplotlib.pyplot as plt
# import seaborn as sns
import warnings
# import os
from pandas import read_csv
import numpy as np

warnings.filterwarnings("ignore")

def psrc_employment_calibration(psrc_parcel_file, soundcast_db_file, 
                                geography_table_name, uncalibrated_mzemp_file, 
                                cleaned_parcel_file, mzemp_file):
    
    print('Start calibrating employment counts within PSRC region!')
    # define synthfirm parameters
    # data_path = '/Users/xiaodanxu/Documents/SynthFirm.nosync'
    # analysis_year = '2050'
    # region_name = 'Seattle'
    # os.chdir(data_path)
    
    ### PSRC inputs ###
    # path to PSRC data
    # input_dir = 'RawData/PSRC/' # need to update this to where the PSRC data is located
    # output_dir = 'Inputs_' + region_name
    
    # PSRC parcel data
    # parcel_file = 'rtp_2050_updated_income/parcels_urbansim.txt'
    
    # soundcast DB
    # db_file = 'soundcast_inputs.db'
    # from_file = 'parcel_2018_geography' # this is hard coded for now, may need to replace in the future
    
    ### SynthFirm inputs ###
    # employment by zone #
    # synthfirm_empranking_file = 'data_mesozone_emprankings.csv'
    # baseline_parcel_path = os.path.join(input_dir, 'landuse', analysis_year, parcel_file)
    psrc_parcels = pd.read_csv(psrc_parcel_file, sep = ' ')
    
    ## start data loading
    # soundcast_input_db = os.path.join(input_dir, 'db', db_file)
    db_con = sqlite3.connect(soundcast_db_file)
    
    # emp_ranking_file = os.path.join(uncalibrated_mzemp_file)
    emp_ranking = read_csv(uncalibrated_mzemp_file)
    
    db_cur = db_con.cursor()
    
    
    parcel_geography = pd.read_sql_query("SELECT * FROM " + geography_table_name, db_con)
    
    parcel_geography = \
    parcel_geography[['ParcelID','CityName', 'Census2010Block',
           'Census2010BlockGroup', 'Census2010Tract', 'FAZID', 'taz_p', 
                      'District', 'district_name', 'CountyName', 'TAZ', 
                      'BaseYear', 'GEOID10', 'place_name']]
    
    
    # <codecell>
    
    # merge geography and create parcel employment data
    
    parcel_geography.loc[:, 'Census2010BlockGroup'] = \
    parcel_geography.loc[:, 'Census2010BlockGroup'].astype(int).astype(str).str.zfill(12)
    parcel_geography.loc[:, 'State'] = \
    parcel_geography.loc[:, 'Census2010BlockGroup'].str[0:2] 
    parcel_geography.loc[:, 'County'] = \
    parcel_geography.loc[:, 'Census2010BlockGroup'].str[2:5]
    parcel_geography.loc[:, 'FIPS'] = \
    parcel_geography.loc[:, 'Census2010BlockGroup'].str[0:5] 
    
    
    # parcel_geography.to_csv(os.path.join(output_dir, 'parcel_geography_' + analysis_year + '.csv'),
    #                         index = False)
    
    psrc_parcels = pd.merge(psrc_parcels,
                            parcel_geography,
                            left_on = ['parcelid', 'taz_p'],
                            right_on = ['ParcelID', 'taz_p'],
                            how = 'left')
    
    psrc_parcels_job_only = psrc_parcels.loc[psrc_parcels['emptot_p'] > 0]
    print(len(psrc_parcels_job_only))
    psrc_parcels_job_only.to_csv(cleaned_parcel_file, index = False)
    
    # <codecell>
    
    # updating employment using parcel data
    emp_attr = ['empedu_p', 'empfoo_p', 'empgov_p', 'empind_p', 'empmed_p',
           'empofc_p', 'empoth_p', 'empret_p', 'empsvc_p', 'emptot_p']
    
    agg_var = ['CountyName', 'Census2010BlockGroup', 'BaseYear']
    psrc_emp_by_cbg = psrc_parcels_job_only.groupby(agg_var)[emp_attr].sum()
    psrc_emp_by_cbg = psrc_emp_by_cbg.reset_index()
    print(len(psrc_emp_by_cbg))
    print(psrc_emp_by_cbg.emptot_p.sum())
    
    psrc_sector = ['PSRC_Education', 'PSRC_Government', 'PSRC_Industrial', 'PSRC_Medical',
                  'PSRC_Other', 'PSRC_Retail', 'PSRC_Office', 'PSRC_Service']
    
    psrc_emp_by_cbg.loc[:, 'PSRC_Education'] = psrc_emp_by_cbg.loc[:, 'empedu_p']
    psrc_emp_by_cbg.loc[:, 'PSRC_Government'] = psrc_emp_by_cbg.loc[:, 'empgov_p']
    psrc_emp_by_cbg.loc[:, 'PSRC_Industrial'] = psrc_emp_by_cbg.loc[:, 'empind_p']
    psrc_emp_by_cbg.loc[:, 'PSRC_Medical'] = psrc_emp_by_cbg.loc[:, 'empmed_p']
    psrc_emp_by_cbg.loc[:, 'PSRC_Other'] = psrc_emp_by_cbg.loc[:, 'empoth_p']
    psrc_emp_by_cbg.loc[:, 'PSRC_Retail'] = psrc_emp_by_cbg.loc[:, 'empret_p']
    psrc_emp_by_cbg.loc[:, 'PSRC_Office'] = psrc_emp_by_cbg.loc[:, 'empofc_p']
    psrc_emp_by_cbg.loc[:, 'PSRC_Service'] = psrc_emp_by_cbg.loc[:, 'empfoo_p'] + \
    psrc_emp_by_cbg.loc[:, 'empsvc_p']
    
    # <codecell>
    
    # adjust employment within PSRC boundary
    missing_geoid = 530530729073
    mesozone_ids = parcel_geography.Census2010BlockGroup.unique()
    
    mesozone_ids = [int(element) for element in mesozone_ids]
    mesozone_ids.append(missing_geoid)
    print(len(mesozone_ids))
    
    emp_ranking.loc[:, 'MESOZONE'] = emp_ranking.loc[:, 'MESOZONE'].astype(int)
    emp_ranking_to_adj = emp_ranking.loc[emp_ranking['MESOZONE'].isin(mesozone_ids)]
    emp_ranking_no_adj = emp_ranking.loc[~emp_ranking['MESOZONE'].isin(mesozone_ids)]
    
    emp_ranking_to_adj.fillna(0, inplace = True)
    
    psrc_emp_long = pd.melt(psrc_emp_by_cbg, id_vars=['Census2010BlockGroup', 'BaseYear'],
                            value_vars=psrc_sector, var_name='industry', value_name='PSRC_emp')
    
    emp_ranking_long = pd.melt(emp_ranking_to_adj, id_vars=['MESOZONE', 'COUNTY'],
                            var_name='NAICS', value_name='LEHD_emp')
    
    
    # formatting industry names
    emp_ranking_long.loc[:, 'NAICS'] = emp_ranking_long.loc[:, 'NAICS'].str.split('rank').str[1]
    print(emp_ranking_long['NAICS'].unique())
    psrc_emp_long.loc[:,'industry'] = psrc_emp_long.loc[:,'industry'].str.split('_').str[1]
    print(psrc_emp_long['industry'].unique())
    
    industry_mapping = {
        'Other': ['11', '21', '23'],
        'Industrial': ['3133', '22', '42', '4849'],
        'Retail': ['4445'],
        'Office': ['51', '52', '53', '54', '55', '56'],
        'Education': ['61'],
        'Medical': ['62'],
        'Service': ['71', '72', '81'],
        'Government': ['92']
    }
    
    emp_ranking_long.loc[:, 'industry'] = np.nan
    for col, values in industry_mapping.items():
        emp_ranking_long.loc[emp_ranking_long['NAICS'].isin(values), 'industry'] = col
        
    # <codecell>
    
    # generating fraction of emp by naics among each industry
    
    psrc_emp_long.rename(columns = {'Census2010BlockGroup': 'MESOZONE'}, inplace = True)
    psrc_emp_long.loc[:, 'MESOZONE'] = psrc_emp_long.loc[:, 'MESOZONE'].astype(int)
    
    emp_ranking_long = pd.merge(emp_ranking_long, psrc_emp_long,
                                on = ['MESOZONE', 'industry'], how = 'left')
    
    
    print('total employment from PSRC data:')
    print(psrc_emp_long.loc[:, 'PSRC_emp'].sum())
    emp_ranking_long.loc[:, 'PSRC_emp'].fillna(0, inplace = True)
    
    
    frac_among_ind = emp_ranking_long.groupby(['industry', 'NAICS'])['LEHD_emp'].sum()
    frac_among_ind = frac_among_ind.reset_index()
    frac_among_ind.loc[:, 'fraction'] = frac_among_ind.loc[:, 'LEHD_emp']/ \
        frac_among_ind.groupby('industry')['LEHD_emp'].transform('sum')
    frac_among_ind.rename(columns = {'LEHD_emp': 'emp_by_naics'}, inplace = True)
    
    emp_ranking_long = pd.merge(emp_ranking_long, frac_among_ind,
                                on = ['industry', 'NAICS'], how = 'left')
    
    # <codecell>
    
    # Define imputation method, variable 'imp_flag'
    emp_ranking_long.loc[:, 'emp_adj'] = emp_ranking_long.loc[:, 'LEHD_emp']
    
    emp_ranking_long.loc[:, 'LEHD_emp_sum'] = \
        emp_ranking_long.groupby(['MESOZONE', 'industry'])['LEHD_emp'].transform('sum')
        
    emp_ranking_long.loc[:, 'imp_flag'] = 1 # scaling employment if both LEHD and PSRC data are non-zero
     
    criteria_1 = (emp_ranking_long['LEHD_emp_sum'] == 0) & (emp_ranking_long['PSRC_emp'] == 0)
    emp_ranking_long.loc[criteria_1, 'imp_flag'] = 0 # no adjustment ==> all zero
    
    criteria_2 = (emp_ranking_long['LEHD_emp_sum'] > 0) & (emp_ranking_long['PSRC_emp'] == 0)
    emp_ranking_long.loc[criteria_2, 'imp_flag'] = 2 # wipe out LEHD emp
    
    criteria_3 = (emp_ranking_long['LEHD_emp_sum'] == 0) & (emp_ranking_long['PSRC_emp'] > 0)
    emp_ranking_long.loc[criteria_3, 'imp_flag'] = 3 # assign PSRC emp and distribute across NAICS
    
    adj_threshold = 0.001
    psrc_total = psrc_emp_long.loc[:, 'PSRC_emp'].sum()
    diff_ratio = abs(emp_ranking_long.loc[:, 'emp_adj'].sum()/psrc_total - 1)
    
    iterator = 1
    while diff_ratio > adj_threshold:
        emp_ranking_long.loc[:, 'LEHD_emp_sum'] = \
            emp_ranking_long.groupby(['MESOZONE', 'industry'])['emp_adj'].transform('sum')
            
        print('this is the iteration number ' + str(iterator))
        print('total employment from LEHD data (before adjustment):')
        print(emp_ranking_long.loc[:, 'emp_adj'].sum())
        
        emp_ranking_long.loc[:, 'adj_factor'] = \
            emp_ranking_long.loc[:, 'PSRC_emp'] / emp_ranking_long.loc[:, 'LEHD_emp_sum'] 
        
        # wipe out emp if PSRC_emp is zero    
        emp_ranking_long.loc[emp_ranking_long['imp_flag']==2, 'emp_adj'] = 0
        
        # distribute PSRC employment if LEHD employment is zero    
        emp_ranking_long.loc[emp_ranking_long['imp_flag']==3, 'emp_adj'] = \
            emp_ranking_long.loc[emp_ranking_long['imp_flag']==3,'PSRC_emp'] * \
                emp_ranking_long.loc[emp_ranking_long['imp_flag']==3,'fraction']
                
        # scale lehd employment if both sets are none zero   
        emp_ranking_long.loc[emp_ranking_long['imp_flag']==1, 'emp_adj'] = \
            emp_ranking_long.loc[emp_ranking_long['imp_flag']==1,'emp_adj'] * \
                emp_ranking_long.loc[emp_ranking_long['imp_flag']==1,'adj_factor']
        emp_ranking_long.loc[:, 'emp_adj'] = np.round(emp_ranking_long.loc[:, 'emp_adj'], 0)  
         
        print('total employment from LEHD data (after adjustment):')
        print(emp_ranking_long.loc[:, 'emp_adj'].sum())
        diff_ratio = abs(emp_ranking_long.loc[:, 'emp_adj'].sum()/psrc_total - 1)
        
        # print(diff_ratio)
        iterator += 1
    
    # <codecell>
    
    # convert data back to emp ranking
    emp_ranking_adjusted = emp_ranking_long[['MESOZONE', 'COUNTY', 'NAICS', 'emp_adj']]
    emp_ranking_adjusted.loc[:, 'NAICS'] = \
        'rank' + emp_ranking_adjusted.loc[:, 'NAICS']
    
    emp_ranking_adjusted = pd.pivot_table(emp_ranking_adjusted, 
                                          index = ['MESOZONE', 'COUNTY'],
                                          columns = 'NAICS', 
                                          values = 'emp_adj',
                                          aggfunc= 'sum')
    
    emp_ranking_adjusted = emp_ranking_adjusted.reset_index()
    emp_ranking_output = pd.concat([emp_ranking_adjusted, emp_ranking_no_adj])
    
    # output_file = os.path.join(output_dir,'data_mesozone_emprankings_2050.csv')
    emp_ranking_output.to_csv(mzemp_file, index = False)
    
