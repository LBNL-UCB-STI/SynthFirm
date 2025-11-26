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

def boston_employment_calibration(taz_file,
                                 boston_employment_ma_2019 , boston_employment_ma_2050,
                                 boston_employment_nhri_2020, boston_employment_nhri_2050,
                                 uncalibrated_mzemp_file,
                                 mzemp_file):
    
    print('Start calibrating employment counts within Boston region!')

    #####################################################################
    #Prepare BOSTON File
    boston_employment_ma_2019 = read_csv(boston_employment_ma_2019)
    boston_employment_nhri_2020 = read_csv(boston_employment_nhri_2020)

    # emp_ranking_file = os.path.join(uncalibrated_mzemp_file)
    emp_ranking = read_csv(uncalibrated_mzemp_file)

    #Stack Emp files
    print("MA shape:", boston_employment_ma_2019.shape)
    print("NHRI shape:", boston_employment_nhri_2020.shape)
    print("MA columns:", boston_employment_ma_2019.columns.tolist())
    print("NHRI columns:", boston_employment_nhri_2020.columns.tolist())
    boston_blocks = pd.concat([boston_employment_ma_2019, boston_employment_nhri_2020], ignore_index=True)
    print("Combined shape:", boston_blocks.shape)

    #Get Block Group ID
    boston_blocks["block_id"] = boston_blocks["block_id"].astype(str)
    boston_blocks["block_group"] = boston_blocks["block_id"].str[:12]

    #Aggregate from Group ID to Block Group ID
    group_cols = [
        '1_constr', '2_eduhlth', '3_finance', '4_public', '5_info',
        '6_ret_leis', '7_manu', '8_other', '9_profbus', '10_ttu',
        'total_jobs', 'total_households'
    ]
    boston_bg = boston_blocks.groupby("block_group", as_index=False)[group_cols].sum()
    print("Block groups:", boston_bg.shape)

    # Create MESOZONE (used by SynthFirm)
    boston_bg["MESOZONE"] = boston_bg["block_group"].astype(int)

    # Extract state FIPS (first 2 digits)
    boston_bg["state_fips"] = boston_bg["block_group"].str[:2]

    # Basic totals check
    emp_cols = [
        '1_constr','2_eduhlth','3_finance','4_public','5_info',
        '6_ret_leis','7_manu','8_other','9_profbus','10_ttu',
        'total_jobs','total_households'
    ]

    print("Total jobs:", boston_bg["total_jobs"].sum())
    print("Total households:", boston_bg["total_households"].sum())

    # --- Boston sector columns (10 categories from your employment files)
    boston_sector_cols = [
        '1_constr',
        '2_eduhlth',
        '3_finance',
        '4_public',
        '5_info',
        '6_ret_leis',
        '7_manu',
        '8_other',
        '9_profbus',
        '10_ttu'
    ]

    # Observed Boston employment in LONG format:
    # one row per MESOZONE × industry
    boston_emp_long = boston_bg.melt(
        id_vars=["MESOZONE", "block_group", "state_fips"],
        value_vars=boston_sector_cols,
        var_name="industry",
        value_name="BOSTON_emp"
    )

    print("Boston long shape:", boston_emp_long.shape)

    ###############################################################
    #Prepare LEHD File

    # --- Restrict mzemp to MESOZONEs in the Boston study area
    emp_ranking["MESOZONE"] = emp_ranking["MESOZONE"].astype(int)

    mesozone_ids = boston_bg["MESOZONE"].unique().tolist()
    print("Number of MESOZONEs in Boston BG:", len(mesozone_ids))

    emp_ranking_to_adj = emp_ranking.loc[emp_ranking["MESOZONE"].isin(mesozone_ids)].copy()
    emp_ranking_no_adj = emp_ranking.loc[~emp_ranking["MESOZONE"].isin(mesozone_ids)].copy()

    print("emp_ranking_to_adj shape:", emp_ranking_to_adj.shape)
    print("emp_ranking_no_adj shape:", emp_ranking_no_adj.shape)

    # --- Melt mzemp to LONG format (NAICS-level employment)
    emp_ranking_long = pd.melt(
        emp_ranking_to_adj,
        id_vars=["MESOZONE", "COUNTY"],
        var_name="NAICS",
        value_name="LEHD_emp"
    )

    print("emp_ranking_long shape:", emp_ranking_long.shape)
    print("total LEHD jobs:", emp_ranking_long["LEHD_emp"].sum())

    # --- Extract NAICS code from rankXX (strip 'rank')
    emp_ranking_long["NAICS_code"] = emp_ranking_long["NAICS"].str.replace("rank", "", regex=False)

    # --- Convert NAICS code to Boston industry
    naics_to_boston = {
        "11":   "8_other",
        "21":   "8_other",
        "22":   "8_other",
        "23":   "1_constr",
        "3133": "7_manu",
        "42":   "9_profbus",
        "4445": "6_ret_leis",
        "4849": "10_ttu",
        "51":   "5_info",
        "52":   "3_finance",
        "53":   "3_finance",
        "54":   "9_profbus",
        "55":   "9_profbus",
        "56":   "9_profbus",
        "61":   "2_eduhlth",
        "62":   "2_eduhlth",
        "71":   "6_ret_leis",
        "72":   "6_ret_leis",
        "81":   "8_other",
        "92":   "4_public",
    }

    emp_ranking_long["industry"] = emp_ranking_long["NAICS_code"].map(naics_to_boston)
    # Sanity check: no NAICS left unmapped
    missing_mask = emp_ranking_long["industry"].isna()

    if missing_mask.any():
        print("WARNING: some NAICS not mapped:", emp_ranking_long.loc[missing_mask, "NAICS_code"].unique())
    else:
        print("All NAICS successfully mapped to Boston sectors.")
        print("Mapped industries:", sorted(emp_ranking_long["industry"].unique()))

    print("Unique industries from NAICS after mapping:", emp_ranking_long["industry"].unique())

    ###############################################################
    #Merge LEHD and Boston Files

    # --- Merge LEHD-based NAICS employment with observed Boston employment by industry
    emp_ranking_long = emp_ranking_long.merge(
        boston_emp_long[["MESOZONE", "industry", "BOSTON_emp"]],
        on=["MESOZONE", "industry"],
        how="left"
    )

    print("Merged shape:", emp_ranking_long.shape)

    emp_ranking_long.loc[:, 'LEHD_emp'].fillna(0, inplace = True)
    emp_ranking_long.loc[:, 'BOSTON_emp'].fillna(0, inplace = True)
    boston_total = boston_emp_long["BOSTON_emp"].sum()
    lehd_total = emp_ranking_long["LEHD_emp"].sum()

    # Compute total LEHD employment by (industry, NAICS) across all zones
    frac_among_ind = (
        emp_ranking_long
        .groupby(["industry", "NAICS"], as_index=False)["LEHD_emp"]
        .sum()
    )

    # Fraction of LEHD_emp each NAICS contributes within its industry
    frac_among_ind["fraction"] = (
        frac_among_ind["LEHD_emp"] /
        frac_among_ind.groupby("industry")["LEHD_emp"].transform("sum")
    )

    frac_among_ind = frac_among_ind.rename(columns={"LEHD_emp": "emp_by_naics"})

    # Attach fraction back to the long table
    emp_ranking_long = emp_ranking_long.merge(
        frac_among_ind[["industry", "NAICS", "fraction"]],
        on=["industry", "NAICS"],
        how="left"
    )

    # If an industry has zero LEHD everywhere, fraction will be NaN → set to 0
    emp_ranking_long["fraction"] = emp_ranking_long["fraction"].fillna(0)

    # Start from LEHD as the initial adjusted employment
    emp_ranking_long["emp_adj"] = emp_ranking_long["LEHD_emp"]

    # Total LEHD employment by MESOZONE × industry (using current emp_adj)
    emp_ranking_long["LEHD_emp_sum"] = (
        emp_ranking_long
        .groupby(["MESOZONE", "industry"])["LEHD_emp"]
        .transform("sum")
    )

    # Default: case 1 = both LEHD and Boston positive → we will scale
    emp_ranking_long["imp_flag"] = 1

    # Case 0: both zero → nothing to do
    criteria_0 = (emp_ranking_long["LEHD_emp_sum"] == 0) & (emp_ranking_long["BOSTON_emp"] == 0)
    emp_ranking_long.loc[criteria_0, "imp_flag"] = 0

    # Case 2: LEHD > 0, Boston = 0 → wipe LEHD to zero
    criteria_2 = (emp_ranking_long["LEHD_emp_sum"] > 0) & (emp_ranking_long["BOSTON_emp"] == 0)
    emp_ranking_long.loc[criteria_2, "imp_flag"] = 2

    # Case 3: LEHD = 0, Boston > 0 → we will *create* employment, using NAICS fractions
    criteria_3 = (emp_ranking_long["LEHD_emp_sum"] == 0) & (emp_ranking_long["BOSTON_emp"] > 0)
    emp_ranking_long.loc[criteria_3, "imp_flag"] = 3


    adj_threshold = 0.001  # 0.1% tolerance on total jobs
    diff_ratio = abs(emp_ranking_long["emp_adj"].sum() / boston_total - 1)

    iterator = 1
    while diff_ratio > adj_threshold:
        print(f"Calibration iteration {iterator}")
        print("  Total emp before iteration:", emp_ranking_long["emp_adj"].sum())

        # Recompute sum by MESOZONE × industry using current emp_adj
        emp_ranking_long["LEHD_emp_sum"] = (
            emp_ranking_long
            .groupby(["MESOZONE", "industry"])["emp_adj"]
            .transform("sum")
        )

        # Scaling factor for case 1 (both LEHD and Boston > 0)
        # Avoid division by zero: where LEHD_emp_sum = 0, adj_factor stays 0
        emp_ranking_long["adj_factor"] = 0.0
        mask_case1 = emp_ranking_long["imp_flag"] == 1
        emp_ranking_long.loc[mask_case1, "adj_factor"] = (
            emp_ranking_long.loc[mask_case1, "BOSTON_emp"] /
            emp_ranking_long.loc[mask_case1, "LEHD_emp_sum"].replace(0, np.nan)
        ).fillna(0)

        # Case 2: wipe out LEHD (Boston wants 0)
        emp_ranking_long.loc[emp_ranking_long["imp_flag"] == 2, "emp_adj"] = 0

        # Case 3: create jobs according to fraction of NAICS within industry
        mask_case3 = emp_ranking_long["imp_flag"] == 3
        emp_ranking_long.loc[mask_case3, "emp_adj"] = (
            emp_ranking_long.loc[mask_case3, "BOSTON_emp"] *
            emp_ranking_long.loc[mask_case3, "fraction"]
        )

        # Case 1: scale existing LEHD to hit Boston totals
        emp_ranking_long.loc[mask_case1, "emp_adj"] = (
            emp_ranking_long.loc[mask_case1, "emp_adj"] *
            emp_ranking_long.loc[mask_case1, "adj_factor"]
        )

        # Round to integer jobs
        emp_ranking_long["emp_adj"] = np.round(emp_ranking_long["emp_adj"], 0)

        print("  Total emp after iteration:", emp_ranking_long["emp_adj"].sum())

        diff_ratio = abs(emp_ranking_long["emp_adj"].sum() / boston_total - 1)
        print("  diff_ratio:", diff_ratio)

        iterator += 1










    
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
    
