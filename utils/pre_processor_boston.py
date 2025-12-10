#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb  5 10:17:52 2025

@author: xiaodanxu
"""

import pandas as pd
import sqlite3
import geopandas as gpd
# import matplotlib.pyplot as plt
# import seaborn as sns
import warnings
from pygris import block_groups
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
    boston_blocks = pd.concat([boston_employment_ma_2019, boston_employment_nhri_2020], ignore_index=True).drop_duplicates()
    print("Combined shape:", boston_blocks.shape)

    ma_ids = set(boston_employment_ma_2019['block_id'])
    nhri_ids = set(boston_employment_nhri_2020['block_id'])
    duplicate_block_ids = ma_ids.intersection(nhri_ids)
    print("Number of repeated block IDs:", len(duplicate_block_ids))
    print("Sample:", list(duplicate_block_ids)[:10])

    boston_blocks["block_id"] = boston_blocks["block_id"].astype(str)
    boston_blocks["len"] = boston_blocks["block_id"].str.len()
    print("Total jobs:", boston_blocks["total_jobs"].sum())

    boston_blocks_15  = boston_blocks[boston_blocks["len"]==15].copy()
    boston_blocks_6   = boston_blocks[boston_blocks["len"]==6].copy()
    print("  15-digit rows:", len(boston_blocks_15))
    print("  6-digit (TAZ) rows:", len(boston_blocks_6))
    boston_blocks_15 = boston_blocks_15.drop(columns=["len"])
    boston_blocks_6 = boston_blocks_6.drop(columns=["len"])

    group_cols = [
        '1_constr', '2_eduhlth', '3_finance', '4_public', '5_info',
        '6_ret_leis', '7_manu', '8_other', '9_profbus', '10_ttu',
        'total_jobs', 'total_households'
    ]

    #Get Block Group ID
    boston_blocks_15["block_group"] = boston_blocks_15["block_id"].str[:12]
    boston_blocks_15 = boston_blocks_15.groupby("block_group", as_index=False)[group_cols].sum()
    boston_blocks_15 = boston_blocks_15.rename(columns={"block_group":"MESOZONE"})
    print("  Total Block groups from block ids:", len(boston_blocks_15))

    boston_blocks_6["taz_id"] = boston_blocks_6["block_id"].astype(int)
    taz_emp = boston_blocks_6.groupby("taz_id", as_index=False)[group_cols].sum()

    taz = gpd.read_file(taz_file)
    taz["taz_id"] = taz["taz_id"].astype(int)
    taz = taz.merge(taz_emp, on="taz_id", how="inner")

    print('taz associated to unique taz_id form 6-digit Boston codes', len(taz))

    # Load block groups from pygris
    print("  Loading block groups via pygris...")
    ma_bg = block_groups("MA", year=2018)
    nh_bg = block_groups("NH", year=2018)
    ri_bg = block_groups("RI", year=2018)
    bg = pd.concat([ma_bg,nh_bg,ri_bg])[["GEOID","geometry"]]

    # Reproject
    taz = taz.to_crs("EPSG:26919")
    bg  = bg.to_crs("EPSG:26919")

    # Overlay
    print("  Computing BG–TAZ intersections...")
    inter = gpd.overlay(bg, taz[["taz_id","geometry"]+group_cols], how="intersection")
    inter["overlap"] = inter.area

    a = taz[["taz_id","geometry"]].copy()
    a["taz_area"] = a.area
    inter = inter.merge(a[["taz_id","taz_area"]], on="taz_id")
    inter["w"] = inter["overlap"] / inter["taz_area"]

    # Weighted employment
    for c in group_cols:
        inter[c] = inter[c] * inter["w"]

    bg_from_taz = inter.groupby("GEOID", as_index=False)[group_cols].sum()
    bg_from_taz = bg_from_taz.rename(columns={"GEOID":"MESOZONE"})
    print(" Total Block groups from TAZ ids:", len(bg_from_taz))

    print("Combining block groups from MA + NH/RI...")
    boston_bg = pd.concat([boston_blocks_15, bg_from_taz], ignore_index=True)
    boston_bg = boston_bg.groupby("MESOZONE", as_index=False)[group_cols].sum()
    print("Total Block groups:", len(boston_bg))
    boston_bg[group_cols] = boston_bg[group_cols].round(0).astype(int)
    # Extract state FIPS (first 2 digits)
    boston_bg["state_fips"] = boston_bg["MESOZONE"].str[:2]
    # Create MESOZONE (used by SynthFirm)
    boston_bg["MESOZONE"] = boston_bg["MESOZONE"].astype(int)

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
        id_vars=["MESOZONE",  "state_fips"],
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
    print("total LEHD jobs to adjust:", emp_ranking_long["LEHD_emp"].sum())

    # --- Extract NAICS code from rankXX (strip 'rank')
    emp_ranking_long["NAICS_code"] = emp_ranking_long["NAICS"].str.replace("rank", "", regex=False)

    # --- Convert NAICS code to Boston industry
    naics_to_boston = {
        "11":   "8_other",
        "21":   "8_other",
        "22":   "10_ttu",
        "23":   "1_constr",
        "3133": "7_manu",
        "31": "7_manu",
        "32": "7_manu",
        "33": "7_manu",
        "42":   "10_ttu",
        "4445": "6_ret_leis",
        "44": "6_ret_leis",
        "45": "6_ret_leis",
        "4849": "10_ttu",
        "48": "10_ttu",
        "49": "10_ttu",
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
        "99":   "8_other",
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
    boston_total = emp_ranking_long.groupby(["MESOZONE", "industry"])["BOSTON_emp"].first().sum()
    lehd_total = emp_ranking_long["LEHD_emp"].sum()
    print(f"Total Boston observed jobs (all MESOZONE × industry) after the merge with LEHD block groups: {boston_total}" )
    print(f"Total LEHD jobs in Boston MESOZONEs (raw) {lehd_total}:")

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
    max_iter = 20
    while diff_ratio > adj_threshold  and iterator <= max_iter:
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



    # Keep only the fields we need
    emp_ranking_adjusted = emp_ranking_long[["MESOZONE", "COUNTY", "NAICS", "emp_adj"]].copy()

    # Pivot back to wide format: one row per MESOZONE × COUNTY, columns = NAICS (rankXX)
    emp_ranking_adjusted = (
        emp_ranking_adjusted
        .pivot_table(
            index=["MESOZONE", "COUNTY"],
            columns="NAICS",
            values="emp_adj",
            aggfunc="sum"
        )
        .reset_index()
    )

    # Columns after pivot are something like ['MESOZONE', 'COUNTY', 'rank11', 'rank21', ...]
    # Merge with zones outside Boston that we did not calibrate
    emp_ranking_output = pd.concat(
        [emp_ranking_adjusted, emp_ranking_no_adj],
        ignore_index=True
    )

    # Save calibrated mzemp file for the rest of SynthFirm
    emp_ranking_output.to_csv(mzemp_file, index=False)
    print("Calibrated mzemp written to:", mzemp_file)

