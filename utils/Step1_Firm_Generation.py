#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 16 10:29:16 2024

@author: xiaodanxu
"""

import pandas as pd
import os
import numpy as np
from pandas import read_csv
import warnings
import time
from collections import defaultdict
import matplotlib.pyplot as plt
import geopandas as gpd



########################################################
#### step 1 - configure environment and load inputs ####
########################################################

# scenario_name = 'Seattle'
# out_scenario_name = 'Seattle'
# file_path = '/Users/xiaodanxu/Documents/SynthFirm.nosync'
# parameter_dir = 'SynthFirm_parameters'
# number_of_processes = 4
# input_dir = 'inputs_' + scenario_name
# output_path = 'outputs_' + out_scenario_name

# os.chdir(file_path)    
# cbp_file = os.path.join(input_dir, 'data_emp_cbp_imputed.csv')
# mzemp_file = os.path.join(input_dir, 'data_mesozone_emprankings.csv')
# mesozone_to_faf_file = os.path.join(input_dir, 'zonal_id_lookup_final.csv')

# c_n6_n6io_sctg_file = os.path.join(parameter_dir, 'corresp_naics6_n6io_sctg_revised.csv')
# employment_per_firm_file = os.path.join(parameter_dir, 'employment_by_firm_size_naics.csv')
# employment_per_firm_gapfill_file = os.path.join(parameter_dir, 'employment_by_firm_size_gapfill.csv')
# zip_to_tract_file = os.path.join(parameter_dir, 'ZIP_TRACT_LOOKUP_2016.csv')
# synthetic_firms_no_location_file = os.path.join(output_path, 'synthetic_firms.csv')

def synthetic_firm_generation(cbp_file, mzemp_file, mesozone_to_faf_file, 
                              c_n6_n6io_sctg_file, employment_per_firm_file,
                              employment_per_firm_gapfill_file, zip_to_tract_file,
                              synthetic_firms_no_location_file, output_path,
                              assign_enterprises, susb_file='', costar_file = '', county_to_msa_file='',
                              naics_crosswalk_file='', firm_enterprise_file='', plot_path='', us_county_map_file=''):
    print("Start synthetic firm generation...")
    # load model inputs
    cbp = read_csv(cbp_file)
    mzemp = read_csv(mzemp_file)
    mesozone_to_faf = read_csv(mesozone_to_faf_file)
    # mzemp = mzemp.drop(columns = ['cnty_id'])
    c_n6_n6io_sctg = read_csv(c_n6_n6io_sctg_file)
    employment_per_firm = read_csv(employment_per_firm_file)
    employment_per_firm_gapfill = read_csv(employment_per_firm_gapfill_file)
    zip_to_tract_crosswalk = read_csv(zip_to_tract_file)
    
    # create result directory if not exist
    
    if not os.path.exists(output_path):
        os.mkdir(output_path)
    else:
      print("Output directory exists!")
        
        
        # <codecell>
        
        ########################################################
        #### step 2 - Enumerate list of firms and workers ######
        ########################################################
        
    print("Enumerating Firms...")
    criteria = (cbp.loc[:, 'employment'] < cbp.loc[:, 'establishment'])
    cbp.loc[criteria, 'employment'] = cbp.loc[criteria, 'establishment']
    
    
    # drop invalid record (if any)
    cbp = cbp.dropna(subset=['Industry_NAICS6_CBP', 'FAFZONE', 'CBPZONE'])
    
    cbp_by_industry = pd.merge(cbp, c_n6_n6io_sctg, 
                                on = 'Industry_NAICS6_CBP', 
                                how = 'left')
    
    cbp_by_industry.loc[:, 'n2'] = \
        cbp_by_industry.loc[:, 'Industry_NAICS6_CBP'].astype(str).str[0:2]
    cbp_by_industry.loc[:, 'n4'] = \
        cbp_by_industry.loc[:, 'Industry_NAICS6_CBP'].astype(str).str[0:4]
    
    cbp_long = pd.melt(cbp_by_industry, 
                        id_vars=["Industry_NAICS6_CBP", "CBPZONE", "FAFZONE", "COUNTY","ZIPCODE",
                                 "Industry_NAICS6_Make", "Commodity_SCTG", "n2", "n4"], 
                        value_vars= ['e1', 'e2', 'e3', 'e4', 'e5', 'e6', 'e7'],
                        var_name='esizecat', value_name='est')
    
    cbp_long.loc[:, 'esizecat'] = cbp_long.loc[:, 'esizecat'].str[1:2].astype(int)
    cbp_long = cbp_long.loc[cbp_long['est'] > 0]
    
    employment_per_firm_short = employment_per_firm[['NAICS', 'size_group', 'emp_per_est']]
    cbp_long = pd.merge(cbp_long, employment_per_firm_short, 
                        left_on = ['Industry_NAICS6_CBP', 'esizecat'],
                        right_on = ['NAICS', 'size_group'], how = 'left')
    
    cbp_long_to_fill = cbp_long.loc[cbp_long['emp_per_est'].isna()]
    cbp_long_no_fill = cbp_long.loc[~cbp_long['emp_per_est'].isna()]
    
    employment_per_firm_gapfill = \
        employment_per_firm_gapfill[['size_group', 'emp_per_est']]
    cbp_long_to_fill = cbp_long_to_fill.drop(columns = ['size_group', 'emp_per_est'])
    cbp_long_to_fill = pd.merge(cbp_long_to_fill, employment_per_firm_gapfill,
                                left_on = 'esizecat', right_on = 'size_group',
                                how = 'left')
    
    cbp_long = pd.concat([cbp_long_no_fill, cbp_long_to_fill])
    
    firms = pd.DataFrame(cbp_long.values.repeat(cbp_long.est, axis=0), 
                          columns=cbp_long.columns)
    
    firms.loc[:, 'BusID'] = firms.reset_index().index + 1
    # 8,396,679 firms nationwide
    
    # <codecell>
    
    # adjust employment
    
    emp_obs = \
        cbp.groupby(['Industry_NAICS6_CBP','FAFZONE', 'CBPZONE'])[['employment']].sum() 
    emp_obs.columns = ['emp_obs']
    emp_obs = emp_obs.reset_index()
    
    emp_sim = \
        firms.groupby(['Industry_NAICS6_CBP','FAFZONE', 'CBPZONE'])[['emp_per_est']].sum()
    emp_sim.columns = ['emp_sim']
    emp_sim = emp_sim.reset_index()
    
    emp_adj = pd.merge( emp_obs, emp_sim, 
                        on = ['Industry_NAICS6_CBP', 'CBPZONE', 'FAFZONE'],
                        how = 'left')
    
    emp_adj.loc[:, 'emp_adj'] = \
        emp_adj.loc[:, 'emp_obs'] / emp_adj.loc[:, 'emp_sim']
        
    emp_adj = emp_adj[['Industry_NAICS6_CBP', 'CBPZONE', 'FAFZONE', 'emp_adj']]
    
    firms = pd.merge(firms, emp_adj, 
                      on = ['Industry_NAICS6_CBP', 'CBPZONE', 'FAFZONE'],
                      how = 'left')
    firms.loc[:, 'emp_per_est'] *= firms.loc[:, 'emp_adj']
    # firms.loc[:, 'emp_per_est'] =np.round(firms.loc[:, 'emp_per_est'].astype(float), 2)
    
    # validate employment
    total_employment_est = firms.loc[:, 'emp_per_est'].sum()
    
    print('Total number of input firm is:')
    print(cbp_long.loc[:, 'est'].sum())
    print('Total number of output firm is:')
    print(len(firms))
    
    print('Total number of input employment is:')
    print(cbp.loc[:, 'employment'].sum())
    print('Total number of modeled employment before LEHD scaling:')
    print(total_employment_est)

    print('Firms:')
    print(firms.head())
    # <codecell>


    ##################################################
    #### step 2.1 - Assign enterprise information ######
    ##################################################
    if assign_enterprises:

        print('Assign Enterprises...')

        #SUSB
        # This file comes from /Users/cpoliziani/Documents/repo/SynthFirm/input_generation/national/susb_costar_firm_and_est_data.ipynb
        susb_data = read_csv(susb_file)

        susb_data.loc[:, 'MSA Code'] ='C' + susb_data.loc[:, 'MSA'].astype(str).str[0:4]
        susb_data['NAICS'] = susb_data['NAICS'].astype(str)
        susb_data.rename(columns = {'NAICS': 'n3'}, inplace = True)
        susb_data.loc[:, 'susb_emp_per_est'] = susb_data.loc[:, 'EMPL']/ susb_data.loc[:, 'ESTB']

        print(f'# of enterprises from SUSB Data: {susb_data.FIRM.sum()}')
        print(f'# of firms from SUSB Data: {susb_data.ESTB.sum()}')
        print(f'# of total employees from SUSB: {susb_data.EMPL.sum()}')

        #CoStar
        # This file comes from /Users/cpoliziani/Documents/repo/SynthFirm/input_generation/national/susb_costar_firm_and_est_data.ipynb

        costar_data = read_csv(costar_file)
        costar_data['naics6d'] = costar_data['naics6d'].astype(int).astype(str)
        costar_data['naics3'] = costar_data['naics3'].astype(str)
        costar_data.rename(columns = {'naics3': 'n3'}, inplace = True)

        if naics_crosswalk_file:
            naics_xwalk = read_csv(naics_crosswalk_file)
            # Columns: NAICS2022, [title], NAICS2017, [title]
            col_2022 = naics_xwalk.columns[0]  # 'NAICS2022'
            col_2017 = naics_xwalk.columns[2]  # 'NAICS2017'
            naics_xwalk['n3_2022'] = naics_xwalk[col_2022].astype(str).str[:3]
            naics_xwalk['n3_2017'] = naics_xwalk[col_2017].astype(str).str[:3]

            # Keep unique 3-digit pairs, drop where they're identical
            n3_map = (naics_xwalk[['n3_2022', 'n3_2017']]
                      .drop_duplicates()
                      .query('n3_2022 != n3_2017'))
            # If a 2022 code maps to multiple 2017 codes, take the first (most common)
            n3_map = n3_map.drop_duplicates(subset='n3_2022', keep='first')
            naics_map_dict = dict(zip(n3_map['n3_2022'], n3_map['n3_2017']))

            n_before = costar_data['n3'].nunique()
            costar_data['n3_original'] = costar_data['n3']
            costar_data['n3'] = costar_data['n3'].map(lambda x: naics_map_dict.get(x, x))
            n_remapped = (costar_data['n3'] != costar_data['n3_original']).sum()
            print(f'NAICS crosswalk: {len(naics_map_dict)} 3-digit codes remapped')
            print(f'  CoStar rows remapped: {n_remapped:,} / {len(costar_data):,}')
            print(f'  Unique n3 before: {n_before}, after: {costar_data.n3.nunique()}')
        else:
            print('No NAICS crosswalk provided, skipping CoStar NAICS remapping')

        print(f'# of enterprises GT5 from Costar Data: {len(costar_data.firm_id.unique())}')
        print(f'# of firms from GT5 Costar Data: {costar_data.N.sum()}')
        print(f'# of total employees from GT5 Costar Data: {costar_data.employees.sum()}')

        #SynthFirm
        firms.loc[:, 'n3'] = firms.loc[:, 'Industry_NAICS6_CBP'].astype(str).str[0:3]
        county_to_msa = read_csv(county_to_msa_file)
        county_to_msa.rename(columns = {'County Code': 'CBPZONE'},
                             inplace = True)
        county_to_msa_short = county_to_msa[['CBPZONE', 'MSA Code']]
        firms['CBPZONE'] = firms['CBPZONE'].astype(int)
        firms = pd.merge(firms, county_to_msa, on = 'CBPZONE',
                         how = 'left')

        print(f'# of generated firms from synthfirm: {len(firms)}')
        print(f'# of total employees from synthfirm: {firms.emp_per_est.sum()}')

#         #Here i need to enrich the firm file with SUSB data for problabil assign of enterprises (also use employ)
#
#
#         pairs = []
#         for r in costar_data.iloc[:30].itertuples(index=False):
#             FAF_select = r.FAF_Zone
#             n6_select = r.naics6d
#             n3_select = r.n3
#             N_select = r.N
#
#             pool = firms[(firms["FAFZONE"] == FAF_select) & (firms["n3"] == n3_select) & (~firms["BusID"].isin(pd.concat(pairs)["BusID"]) if pairs else True)]
#             print(f'# of possible Synthfirm firms = {len(pool)}')
#             #Here i need to insert the probabilistic assignment
#             pick = pool.sample(n=min(r.N, len(pool)), replace=False, random_state=1)
#             pairs.append(pick[["BusID"]].assign(CoStarEntpID=r.firm_id))
#
#         pairs = pd.concat(pairs, ignore_index=True)          # BusID -> FirmID
#         firms  = firms.merge(pairs, on="BusID", how="left")
#         firms["CoStarEntpID"] = firms["CoStarEntpID"].fillna(0).astype(np.int64)
# #         print(firms)
#         pairs.to_csv(firm_enterprise_file, index = False)

        # --- Build SUSB target lookup: (MSA Code, n3) -> enterprise count ---
        susb_target = dict(zip(
            zip(susb_data['MSA Code'], susb_data['n3']),
            susb_data['FIRM'].astype(int)
        ))

        # --- Pre-build firm index: (FAFZONE, n3) -> arrays + availability mask ---
        firms_for_index = firms[['BusID', 'FAFZONE', 'n3', 'MSA Code']].copy()
        firms_for_index['FAFZONE'] = firms_for_index['FAFZONE'].astype(int)

        firm_index = {}
        for (faf, n3_val), grp in firms_for_index.groupby(['FAFZONE', 'n3'], sort=False):
            firm_index[(faf, n3_val)] = {
                'busids':    grp['BusID'].values.copy(),
                'msas':      grp['MSA Code'].values.copy(),
                'available': np.ones(len(grp), dtype=bool)
            }
        print(f'Firm index built: {len(firm_index):,} (FAFZONE, n3) buckets')

        # --- Sort CoStar: largest enterprises first (hardest to place) ---
        costar_sorted = costar_data.sort_values('employees_total', ascending=False).reset_index(drop=True)

        # --- Running state: start from total SynthFirm establishments per (MSA, n3) ---
        current_assigned = defaultdict(int)
        for (msa, n3_val), cnt in firms_for_index.groupby(['MSA Code', 'n3']).size().items():
            if pd.notna(msa):
                current_assigned[(msa, str(n3_val))] = int(cnt)

        pairs_list = []
        n_unmatched = 0
        total_rows = len(costar_sorted)
        # --- Main probabilistic assignment loop ---
        print(f'Starting probabilistic assignment for {total_rows:,} CoStar rows...')
        n_no_bucket = 0       # CoStar row has no matching (FAF, n3) bucket at all
        n_pool_empty = 0      # bucket exists but all firms already taken
        n_partial = 0         # got some firms but fewer than requested N
        t0 = time.time()

        diag_no_bucket = []    # (FAF, n3, N_requested)
        diag_pool_empty = []   # (FAF, n3, N_requested, original_pool_size)
        diag_partial = []      # (FAF, n3, N_requested, n_picked, avail_left, firm_id)

        for i, r in enumerate(costar_sorted.itertuples(index=False)):
            if i % 10000 == 0:
                elapsed = time.time() - t0
                print(f'  [{i:,}/{total_rows:,}] pairs: {len(pairs_list):,} | '
                      f'unmatched N: {n_unmatched:,} | elapsed: {elapsed:.0f}s')

            key = (int(r.FAF_Zone), str(r.n3))
            if key not in firm_index:
                n_unmatched += int(r.N)
                n_no_bucket += 1
                diag_no_bucket.append({'FAF_Zone': int(r.FAF_Zone), 'n3': str(r.n3),
                                       'N_requested': int(r.N), 'firm_id': int(r.firm_id)})
                continue

            bucket = firm_index[key]
            total_bucket_size = len(bucket['busids'])
            avail_idx = np.where(bucket['available'])[0]
            if len(avail_idx) == 0:
                n_unmatched += int(r.N)
                n_pool_empty += 1
                diag_pool_empty.append({'FAF_Zone': int(r.FAF_Zone), 'n3': str(r.n3),
                                        'N_requested': int(r.N), 'firm_id': int(r.firm_id),
                                        'original_pool_size': total_bucket_size})
                continue

            avail_busids = bucket['busids'][avail_idx]
            avail_msas   = bucket['msas'][avail_idx]
            n3_val       = str(r.n3)

            weights = np.array([
                max(0, current_assigned[(msa, n3_val)] - susb_target.get((msa, n3_val), 0))

                if pd.notna(msa) else 0
                for msa in avail_msas
            ], dtype=float)

            if weights.sum() == 0:
                weights = np.ones(len(avail_busids), dtype=float)

            positive_mask = weights > 0
            n_positive = positive_mask.sum()
            n_pick = min(int(r.N), n_positive)

            if n_pick == 0:
                n_unmatched += int(r.N)
                n_pool_empty += 1
                diag_pool_empty.append({'FAF_Zone': int(r.FAF_Zone), 'n3': str(r.n3),
                                        'N_requested': int(r.N), 'firm_id': int(r.firm_id),
                                        'original_pool_size': total_bucket_size})
                continue

            if n_pick < int(r.N):
                n_partial += 1
                n_unmatched += int(r.N) - n_pick
                diag_partial.append({'FAF_Zone': int(r.FAF_Zone), 'n3': str(r.n3),
                                     'N_requested': int(r.N), 'N_picked': n_pick,
                                     'avail_remaining': len(avail_idx),
                                     'firm_id': int(r.firm_id),
                                     'original_pool_size': total_bucket_size})

            pos_indices = np.where(positive_mask)[0]
            pos_weights = weights[pos_indices]
            pos_weights /= pos_weights.sum()

            chosen_in_pos = np.random.choice(len(pos_indices), size=n_pick, replace=False, p=pos_weights)
            chosen = pos_indices[chosen_in_pos]

            msa_counts = defaultdict(int)
            for ci in chosen:
                bucket_pos = avail_idx[ci]
                bucket['available'][bucket_pos] = False
                msa = avail_msas[ci]
                if pd.notna(msa):
                    msa_counts[msa] += 1
                pairs_list.append({'BusID': int(avail_busids[ci]), 'CoStarEntpID': int(r.firm_id)})
            # Subtract (N_picked - 1) per MSA: 1 firm stays as unique, rest are enterprise estabs
            for msa, cnt in msa_counts.items():
                current_assigned[(msa, n3_val)] -= max(0, cnt - 1)

        # --- Diagnostic DataFrames ---
        df_no_bucket  = pd.DataFrame(diag_no_bucket)  if diag_no_bucket  else pd.DataFrame()
        df_pool_empty = pd.DataFrame(diag_pool_empty) if diag_pool_empty else pd.DataFrame()
        df_partial    = pd.DataFrame(diag_partial)    if diag_partial    else pd.DataFrame()

        print(f'\n--- Failure Diagnosis ---')

        if len(df_no_bucket) > 0:
            print(f'\n  NO BUCKET (FAF/n3 combo not in SynthFirm): {len(df_no_bucket):,} rows, '
                  f'{df_no_bucket.N_requested.sum():,} estabs')
            top_nb = df_no_bucket.groupby(['FAF_Zone','n3'])['N_requested'].agg(['sum','count']).nlargest(10,'sum')
            print(f'  Top 10 missing (FAF, n3) by N_requested:')
            print(top_nb.to_string())

        if len(df_pool_empty) > 0:
            print(f'\n  POOL EXHAUSTED (bucket existed but ran out): {len(df_pool_empty):,} rows, '
                  f'{df_pool_empty.N_requested.sum():,} estabs')
            top_pe = df_pool_empty.groupby(['FAF_Zone','n3']).agg(
                N_requested=('N_requested','sum'),
                rows=('N_requested','count'),
                orig_pool=('original_pool_size','first')
            ).nlargest(10,'N_requested')
            print(f'  Top 10 exhausted (FAF, n3) — requested vs original pool:')
            print(top_pe.to_string())

        if len(df_partial) > 0:
            print(f'\n  PARTIAL ASSIGNMENT: {len(df_partial):,} rows, '
                  f'got {df_partial.N_picked.sum():,} / requested {df_partial.N_requested.sum():,}')
            top_pa = df_partial.groupby(['FAF_Zone','n3']).agg(
                N_requested=('N_requested','sum'),
                N_picked=('N_picked','sum'),
                orig_pool=('original_pool_size','first')
            ).nlargest(10,'N_requested')
            top_pa['deficit'] = top_pa['N_requested'] - top_pa['N_picked']
            print(f'  Top 10 partial (FAF, n3):')
            print(top_pa.to_string())

        # Save diagnostics to CSV
        df_no_bucket.to_csv(os.path.join(output_path, 'diag_no_bucket.csv'), index=False)
        df_pool_empty.to_csv(os.path.join(output_path, 'diag_pool_empty.csv'), index=False)
        df_partial.to_csv(os.path.join(output_path, 'diag_partial.csv'), index=False)
        print(f'\n  Diagnostic CSVs saved to {output_path}')

        elapsed = time.time() - t0
        print(f'\n--- Assignment Summary ---')
        print(f'  Total CoStar rows processed:  {total_rows:,}')
        print(f'  Total pairs assigned:         {len(pairs_list):,}')
        print(f'  Unmatched establishments (N): {n_unmatched:,}')
        print(f'    - No matching bucket:       {n_no_bucket:,} rows')
        print(f'    - Pool exhausted:           {n_pool_empty:,} rows')
        print(f'    - Partial assignment:        {n_partial:,} rows')
        print(f'  Elapsed time:                 {elapsed:.1f}s')

        pairs = pd.DataFrame(pairs_list)
        firms = firms.merge(pairs, on='BusID', how='left')
        firms['CoStarEntpID'] = firms['CoStarEntpID'].fillna(0).astype(np.int64)
        pairs.to_csv(firm_enterprise_file, index=False)

        # ============================================================
        # --- VALIDATION ---
        # ============================================================
        print('\n' + '='*60)
        print('VALIDATION')
        print('='*60)

        pairs = pd.DataFrame(pairs_list)
        assigned_firms = firms.loc[firms['CoStarEntpID'] > 0,
                                   ['MSA Code', 'n3', 'CoStarEntpID', 'FAFZONE']].copy()

        # ---- Build geometries (once) ----
        faf_geo = None
        msa_geo = None
        if us_county_map_file and os.path.exists(us_county_map_file):
            counties = gpd.read_file(us_county_map_file)
            counties['FIPS'] = counties['STATEFP'] + counties['COUNTYFP']

            # FAF geometry (national): county -> FAF from cbp
            county_faf = cbp[['CBPZONE', 'FAFZONE']].drop_duplicates()
            county_faf['FIPS'] = county_faf['CBPZONE'].astype(int).astype(str).str.zfill(5)
            county_faf['FAFZONE'] = county_faf['FAFZONE'].astype(int)
            counties_faf = counties.merge(county_faf[['FIPS', 'FAFZONE']], on='FIPS', how='inner')
            faf_geo = counties_faf.dissolve(by='FAFZONE', as_index=False)

            # MSA geometry (study area)
            cty_msa = pd.read_csv(county_to_msa_file, dtype=str)
            study_counties = mzemp.dropna(subset=['COUNTY'])['COUNTY'].astype(int).unique()
            # county_to_msa was renamed CBPZONE and is int
            study_msas = county_to_msa.loc[county_to_msa['CBPZONE'].astype(int).isin(study_counties), 'MSA Code'].unique()

            print(f'  Study counties: {len(study_counties)}, Study MSAs: {len(study_msas)}')

            counties_msa = counties.merge(cty_msa[['County Code', 'MSA Code']],
                                          left_on='FIPS', right_on='County Code', how='inner')
            counties_msa = counties_msa[counties_msa['MSA Code'].isin(study_msas)]
            msa_geo = counties_msa.dissolve(by='MSA Code', as_index=False)

            print(f'  Geometries: {len(faf_geo)} FAF zones, {len(msa_geo)} MSAs (study area)')

        # ============================================================
        # BLOCK 1: CoStar (GT5 enterprises, national, per FAF + NAICS3)
        # ============================================================
        print('\n--- CoStar Validation (GT5 Enterprises) ---')

        # 1a. Enterprise coverage
        costar_requested = costar_data.groupby('firm_id')['N'].sum().reset_index()
        costar_requested.columns = ['CoStarEntpID', 'N_requested']
        costar_got = pairs.groupby('CoStarEntpID').size().reset_index(name='N_assigned')
        costar_match = pd.merge(costar_requested, costar_got, on='CoStarEntpID', how='left')
        costar_match['N_assigned'] = costar_match['N_assigned'].fillna(0).astype(int)
        costar_match['N_missing'] = costar_match['N_requested'] - costar_match['N_assigned']
        costar_match['match_rate'] = np.where(costar_match['N_requested'] > 0,
                                              costar_match['N_assigned'] / costar_match['N_requested'], 0)
        n_fully = (costar_match['N_missing'] == 0).sum()
        n_partial_cs = ((costar_match['N_assigned'] > 0) & (costar_match['N_missing'] > 0)).sum()
        n_zero = (costar_match['N_assigned'] == 0).sum()

        print(f'  Fully assigned:     {n_fully:,} / {len(costar_match):,}')
        print(f'  Partially assigned: {n_partial_cs:,}')
        print(f'  Not assigned:       {n_zero:,}')
        print(f'  Overall match rate: {costar_match.N_assigned.sum() / max(1, costar_match.N_requested.sum()) * 100:.1f}%')

        # 1b. Per-FAF unique enterprises
        costar_ent_faf = (costar_data.groupby('FAF_Zone')['firm_id'].nunique()
                          .reset_index().rename(columns={'FAF_Zone': 'FAFZONE', 'firm_id': 'ENT_costar'}))
        modeled_ent_faf = (assigned_firms.groupby('FAFZONE')['CoStarEntpID'].nunique()
                           .reset_index().rename(columns={'CoStarEntpID': 'ENT_modeled'}))
        costar_ent_faf['FAFZONE'] = costar_ent_faf['FAFZONE'].astype(int)
        modeled_ent_faf['FAFZONE'] = modeled_ent_faf['FAFZONE'].astype(int)
        val_ent_faf = pd.merge(costar_ent_faf, modeled_ent_faf, on='FAFZONE', how='outer')
        val_ent_faf = val_ent_faf.fillna(0).astype({'ENT_costar': int, 'ENT_modeled': int})
        val_ent_faf['ent_ratio'] = np.where(val_ent_faf['ENT_costar'] > 0,
                                            val_ent_faf['ENT_modeled'] / val_ent_faf['ENT_costar'], 0)

        # 1c. Per-NAICS3 unique enterprises
        costar_ent_n3 = (costar_data.groupby('n3')['firm_id'].nunique()
                         .reset_index().rename(columns={'firm_id': 'ENT_costar'}))
        modeled_ent_n3 = (assigned_firms.groupby('n3')['CoStarEntpID'].nunique()
                          .reset_index().rename(columns={'CoStarEntpID': 'ENT_modeled'}))
        val_ent_n3 = pd.merge(costar_ent_n3, modeled_ent_n3, on='n3', how='outer')
        val_ent_n3 = val_ent_n3.fillna(0).astype({'ENT_costar': int, 'ENT_modeled': int})
        val_ent_n3['ent_ratio'] = np.where(val_ent_n3['ENT_costar'] > 0,
                                           val_ent_n3['ENT_modeled'] / val_ent_n3['ENT_costar'], 0)

        costar_match.to_csv(os.path.join(output_path, 'validation_costar_enterprise.csv'), index=False)
        val_ent_faf.to_csv(os.path.join(output_path, 'validation_costar_by_faf.csv'), index=False)
        val_ent_n3.to_csv(os.path.join(output_path, 'validation_costar_by_naics3.csv'), index=False)

        # CoStar plots
        if plot_path:
            fig, axes = plt.subplots(2, 3, figsize=(20, 12))

            # Requested vs Assigned
            ax = axes[0, 0]
            ax.scatter(costar_match['N_requested'], costar_match['N_assigned'], alpha=0.3, s=10)
            lim = costar_match['N_requested'].max() * 1.05
            ax.plot([0, lim], [0, lim], 'r--', lw=1)
            ax.set_xlabel('Requested Estabs')
            ax.set_ylabel('Assigned Estabs')
            ax.set_title('Per Enterprise: Requested vs Assigned')

            # Match rate distribution
            ax = axes[0, 1]
            ax.hist(costar_match['match_rate'], bins=50, edgecolor='black', alpha=0.7)
            ax.axvline(1.0, color='r', linestyle='--', lw=1)
            ax.set_xlabel('Match Rate')
            ax.set_ylabel('Count')
            ax.set_title('Match Rate Distribution')

            # Pie chart
            ax = axes[0, 2]
            sizes = [n_fully, n_partial_cs, n_zero]
            labels = [f'Full\n({n_fully:,})', f'Partial\n({n_partial_cs:,})', f'None\n({n_zero:,})']
            ax.pie(sizes, labels=labels, colors=['#2ecc71', '#f39c12', '#e74c3c'],
                   autopct='%1.1f%%', startangle=90)
            ax.set_title('Assignment Status')

            # Unique enterprises per FAF
            ax = axes[1, 0]
            lim = max(val_ent_faf['ENT_costar'].max(), val_ent_faf['ENT_modeled'].max()) * 1.05
            ax.scatter(val_ent_faf['ENT_costar'], val_ent_faf['ENT_modeled'], alpha=0.6, s=30)
            ax.plot([0, lim], [0, lim], 'r--', lw=1)
            ax.set_xlabel('CoStar GT5 Enterprises per FAF')
            ax.set_ylabel('Modeled Enterprises per FAF')
            ax.set_title('Unique Enterprises by FAF Zone')

            # Unique enterprises per NAICS3
            ax = axes[1, 1]
            lim = max(val_ent_n3['ENT_costar'].max(), val_ent_n3['ENT_modeled'].max()) * 1.05
            ax.scatter(val_ent_n3['ENT_costar'], val_ent_n3['ENT_modeled'], alpha=0.6, s=30)
            ax.plot([0, lim], [0, lim], 'r--', lw=1)
            ax.set_xlabel('CoStar GT5 Enterprises per NAICS3')
            ax.set_ylabel('Modeled Enterprises per NAICS3')
            ax.set_title('Unique Enterprises by NAICS3')

            # NAICS3 bar chart
            ax = axes[1, 2]
            vn3 = val_ent_n3.sort_values('n3')
            x = np.arange(len(vn3))
            w = 0.35
            ax.bar(x - w/2, vn3['ENT_costar'], w, label='CoStar GT5', alpha=0.8)
            ax.bar(x + w/2, vn3['ENT_modeled'], w, label='Modeled', alpha=0.8)
            ax.set_xticks(x)
            ax.set_xticklabels(vn3['n3'], rotation=90, fontsize=6)
            ax.set_ylabel('Unique Enterprises')
            ax.set_title('Enterprises by NAICS3')
            ax.legend()

            plt.suptitle('CoStar GT5 Enterprise Validation', fontsize=16)
            plt.tight_layout()
            plt.savefig(os.path.join(plot_path, 'validation_costar.png'), dpi=200, bbox_inches='tight')
            plt.close()
            print('  CoStar plots saved')

            # CoStar FAF map (national)
            if faf_geo is not None:
                faf_map = faf_geo.merge(val_ent_faf, on='FAFZONE', how='inner')
                fig, axes = plt.subplots(1, 2, figsize=(20, 8))

                ax = axes[0]
                faf_map.plot(column='ENT_modeled', ax=ax, legend=True,
                             cmap='YlOrRd', edgecolor='grey', linewidth=0.3,
                             legend_kwds={'label': 'Modeled Enterprises', 'shrink': 0.5})
                ax.set_title('Modeled GT5 Enterprises per FAF Zone')
                ax.set_axis_off()

                ax = axes[1]
                faf_map.plot(column='ent_ratio', ax=ax, legend=True,
                             cmap='RdYlGn', edgecolor='grey', linewidth=0.3,
                             vmin=0, vmax=1.5,
                             legend_kwds={'label': 'Modeled / CoStar', 'shrink': 0.5})
                ax.set_title('GT5 Enterprise Coverage Ratio per FAF Zone')
                ax.set_axis_off()

                plt.suptitle('CoStar GT5 Enterprise Validation Maps', fontsize=16)
                plt.tight_layout()
                plt.savefig(os.path.join(plot_path, 'validation_costar_faf_map.png'), dpi=200, bbox_inches='tight')
                plt.close()
                print('  CoStar FAF map saved')

        # ============================================================
        # BLOCK 2: SUSB Validation (study area only, per MSA + NAICS3)
        # ============================================================
        print('\n--- SUSB Validation (Study Area) ---')

        # Merge pairs onto firms, filter to study area
        firms_val = firms_for_index[['BusID', 'MSA Code', 'n3']].copy()
        firms_val['n3'] = firms_val['n3'].astype(str)
        study_counties = mzemp.dropna(subset=['COUNTY'])['COUNTY'].astype(int).unique()
        study_msas = county_to_msa.loc[county_to_msa['CBPZONE'].isin(study_counties), 'MSA Code'].unique()
        firms_val = firms_val[firms_val['MSA Code'].isin(study_msas)]

        if len(pairs) > 0:
            firms_val = firms_val.merge(pairs, on='BusID', how='left')
        else:
            firms_val['CoStarEntpID'] = np.nan

        # SUSB reference (study area only)
        susb_bkt = susb_data[['MSA Code', 'n3', 'ESTB', 'FIRM']].copy()
        susb_bkt['n3'] = susb_bkt['n3'].astype(str)
        susb_bkt.rename(columns={'ESTB': 'ESTB_SUSB', 'FIRM': 'FIRM_SUSB'}, inplace=True)
        susb_bkt = susb_bkt[susb_bkt['MSA Code'].isin(study_msas)]

        # Count unique firms per bucket
        def _count_unique_firms(g):
            return g['CoStarEntpID'].dropna().nunique() + g['CoStarEntpID'].isna().sum()

        # Per (MSA, n3): establishments
        synth_estab = firms_val.groupby(['MSA Code', 'n3']).size().reset_index(name='ESTB_SYNTH')
        comp_estab = synth_estab.merge(susb_bkt, on=['MSA Code', 'n3'], how='outer')
        comp_estab['estab_ratio'] = comp_estab['ESTB_SYNTH'] / comp_estab['ESTB_SUSB']

        # Per (MSA, n3): unique firms
        synth_firm = (firms_val.groupby(['MSA Code', 'n3'])
                      .apply(_count_unique_firms).reset_index(name='FIRM_SYNTH'))
        comp_firm = synth_firm.merge(susb_bkt[['MSA Code', 'n3', 'FIRM_SUSB']],
                                     on=['MSA Code', 'n3'], how='outer')
        comp_firm['firm_ratio'] = comp_firm['FIRM_SYNTH'] / comp_firm['FIRM_SUSB']

        me = comp_estab.dropna(subset=['ESTB_SYNTH', 'ESTB_SUSB']).query('ESTB_SUSB > 0')
        mf = comp_firm.dropna(subset=['FIRM_SYNTH', 'FIRM_SUSB']).query('FIRM_SUSB > 0')

        print(f'  Estab ratio  - mean: {me["estab_ratio"].mean():.3f}  median: {me["estab_ratio"].median():.3f}')
        print(f'  Firm ratio   - mean: {mf["firm_ratio"].mean():.3f}  median: {mf["firm_ratio"].median():.3f}')

#         # Per NAICS3 (aggregated across study area MSAs)
#         synth_n3_estab = firms_val.groupby('n3').size().reset_index(name='ESTB_SYNTH')
#         synth_n3_firm = (firms_val.groupby('n3').apply(_count_unique_firms)
#                          .reset_index(name='FIRM_SYNTH'))
#         susb_n3 = susb_bkt.groupby('n3').agg(
#             ESTB_SUSB=('ESTB_SUSB', 'sum'), FIRM_SUSB=('FIRM_SUSB', 'sum')).reset_index()
#         comp_n3 = (synth_n3_estab.merge(susb_n3, on='n3', how='outer')
#                    .merge(synth_n3_firm, on='n3', how='outer'))
#         comp_n3['estab_ratio'] = comp_n3['ESTB_SYNTH'] / comp_n3['ESTB_SUSB']
#         comp_n3['firm_ratio'] = comp_n3['FIRM_SYNTH'] / comp_n3['FIRM_SUSB']

        # Save CSVs
        val_all = comp_estab.merge(
            comp_firm[['MSA Code', 'n3', 'FIRM_SYNTH', 'FIRM_SUSB', 'firm_ratio']],
            on=['MSA Code', 'n3'], how='outer')
        val_all.to_csv(os.path.join(output_path, 'validation_susb.csv'), index=False)
#         comp_n3.to_csv(os.path.join(output_path, 'validation_susb_by_naics3.csv'), index=False)
        print('  CSVs saved')

        # SUSB plots
        if plot_path:
            fig, axes = plt.subplots(1, 2, figsize=(16, 14))

            # Scatter: estabs per (MSA, n3) bucket
            ax = axes[0, 0]
            ax.scatter(me['ESTB_SUSB'], me['ESTB_SYNTH'], alpha=0.3, s=10)
            lim = max(me['ESTB_SUSB'].max(), me['ESTB_SYNTH'].max()) * 1.05
            ax.plot([0, lim], [0, lim], 'r--', lw=1)
            ax.set_xlabel('SUSB ESTB')
            ax.set_ylabel('SynthFirm Estabs')
            ax.set_title('Establishments per (MSA, NAICS3)')

            # Scatter: unique firms per (MSA, n3) bucket
            ax = axes[0, 1]
            ax.scatter(mf['FIRM_SUSB'], mf['FIRM_SYNTH'], alpha=0.3, s=10)
            lim = max(mf['FIRM_SUSB'].max(), mf['FIRM_SYNTH'].max()) * 1.05
            ax.plot([0, lim], [0, lim], 'r--', lw=1)
            ax.set_xlabel('SUSB FIRM')
            ax.set_ylabel('SynthFirm Unique Firms')
            ax.set_title('Unique Firms per (MSA, NAICS3)')

#             # NAICS3 bars: establishments
#             mn3 = comp_n3.dropna(subset=['ESTB_SYNTH', 'ESTB_SUSB']).query('ESTB_SUSB > 0').sort_values('n3')
#             x = np.arange(len(mn3))
#             w = 0.35
#
#             ax = axes[1, 0]
#             ax.bar(x - w/2, mn3['ESTB_SYNTH'], w, label='SynthFirm', alpha=0.8)
#             ax.bar(x + w/2, mn3['ESTB_SUSB'], w, label='SUSB', alpha=0.8)
#             ax.set_xticks(x)
#             ax.set_xticklabels(mn3['n3'], rotation=90, fontsize=6)
#             ax.set_ylabel('Establishments')
#             ax.set_title('Establishments by NAICS3')
#             ax.legend()
#
#             # NAICS3 bars: unique firms
#             ax = axes[1, 1]
#             mn3f = comp_n3.dropna(subset=['FIRM_SYNTH', 'FIRM_SUSB']).query('FIRM_SUSB > 0').sort_values('n3')
#             xf = np.arange(len(mn3f))
#             ax.bar(xf - w/2, mn3f['FIRM_SYNTH'], w, label='SynthFirm', alpha=0.8)
#             ax.bar(xf + w/2, mn3f['FIRM_SUSB'], w, label='SUSB', alpha=0.8)
#             ax.set_xticks(xf)
#             ax.set_xticklabels(mn3f['n3'], rotation=90, fontsize=6)
#             ax.set_ylabel('Unique Firms')
#             ax.set_title('Unique Firms by NAICS3')
#             ax.legend()

            plt.suptitle('SUSB Validation (Study Area)', fontsize=16)
            plt.tight_layout()
            plt.savefig(os.path.join(plot_path, 'validation_susb.png'), dpi=200, bbox_inches='tight')
            plt.close()
            print('  SUSB plots saved')

            # SUSB MSA maps (study area)
            if msa_geo is not None:
                msa_estab = (me.groupby('MSA Code')
                             .agg(ESTB_SYNTH=('ESTB_SYNTH', 'sum'), ESTB_SUSB=('ESTB_SUSB', 'sum'))
                             .reset_index())
                msa_estab['estab_ratio'] = msa_estab['ESTB_SYNTH'] / msa_estab['ESTB_SUSB']

                msa_firm = (mf.groupby('MSA Code')
                            .agg(FIRM_SYNTH=('FIRM_SYNTH', 'sum'), FIRM_SUSB=('FIRM_SUSB', 'sum'))
                            .reset_index())
                msa_firm['firm_ratio'] = msa_firm['FIRM_SYNTH'] / msa_firm['FIRM_SUSB']

                fig, axes = plt.subplots(1, 2, figsize=(20, 8))

                ax = axes[0]
                msa_geo.merge(msa_estab, on='MSA Code', how='inner').plot(
                    column='estab_ratio', ax=ax, legend=True, cmap='RdYlGn',
                    edgecolor='grey', linewidth=0.3, vmin=0.5, vmax=1.5,
                    legend_kwds={'label': 'SynthFirm / SUSB ESTB', 'shrink': 0.5})
                ax.set_title('Establishment Ratio per MSA')
                ax.set_axis_off()

                ax = axes[1]
                msa_geo.merge(msa_firm, on='MSA Code', how='inner').plot(
                    column='firm_ratio', ax=ax, legend=True, cmap='RdYlGn',
                    edgecolor='grey', linewidth=0.3, vmin=0.5, vmax=1.5,
                    legend_kwds={'label': 'SynthFirm / SUSB FIRM', 'shrink': 0.5})
                ax.set_title('Unique Firm Ratio per MSA')
                ax.set_axis_off()

                plt.suptitle('SUSB Validation Maps (Study Area)', fontsize=16)
                plt.tight_layout()
                plt.savefig(os.path.join(plot_path, 'validation_susb_maps.png'), dpi=200, bbox_inches='tight')
                plt.close()
                print('  SUSB maps saved')

    ########################################################################
    # Step 3 - Allocating commodity and location for each establishment ####
    ########################################################################
    
    
    #### scale employment to align with LEHD total (by NAICS 2-DIGIT code)
    # pre-process employment ranking
    mesozone_to_cbpzone = mesozone_to_faf[['MESOZONE', 'CBPZONE']]
    lehd_emp_for_scaling = pd.merge(mzemp, mesozone_to_cbpzone, on = 'MESOZONE')
    
    emp_colnames = ["rank11",
      "rank21",
      "rank22",
      "rank23",
      "rank3133",
      "rank42",
      "rank4445",
      "rank4849",
      "rank51",
      "rank52",
      "rank53",
      "rank54",
      "rank55",
      "rank56",
      "rank61",
      "rank62",
      "rank71",
      "rank72",
      "rank81",
      "rank92"]
    
    lehd_emp_for_scaling.loc[:, emp_colnames] = lehd_emp_for_scaling.loc[:, emp_colnames].fillna(0)
    lehd_emp_for_scaling = lehd_emp_for_scaling.groupby('CBPZONE')[emp_colnames].sum()
    lehd_emp_for_scaling = lehd_emp_for_scaling.reset_index()
    lehd_emp_for_scaling = pd.melt(lehd_emp_for_scaling, id_vars = ["CBPZONE"],
                                   value_vars = emp_colnames,
                                      var_name= 'industry', value_name='emp_lehd')
    
    lehd_emp_for_scaling = lehd_emp_for_scaling.reset_index()
    lehd_emp_for_scaling.loc[:, 'industry'] = \
        lehd_emp_for_scaling.loc[:, 'industry'].str.split('rank').str[1]
        
    
    print('Total LEHD employment:')
    print(lehd_emp_for_scaling.emp_lehd.sum())
    # develop firm emp scaling factor
    firms.loc[:, 'industry'] = firms.loc[:, 'n2']
    firms.loc[firms['industry'].isin(["31", "32", "33"]), 'industry'] = "3133"
    firms.loc[firms['industry'].isin(["44", "45", "4A"]), 'industry'] = "4445"
    firms.loc[firms['industry'].isin(["48", "49"]), 'industry'] = "4849"
    firms.loc[firms['industry'].isin(["S0"]), 'industry'] = "92"
    # print(firms['industry'].unique())
    
    emp_sim = \
        firms.groupby(['industry', 'CBPZONE'])[['emp_per_est']].sum()
    emp_sim.columns = ['emp_sim']
    emp_sim = emp_sim.reset_index()
    
    emp_adj = pd.merge( lehd_emp_for_scaling, emp_sim, 
                        on = ['industry', 'CBPZONE'],
                        how = 'left')
    
    emp_adj.loc[:, 'emp_adj'] = \
        emp_adj.loc[:, 'emp_lehd'] / emp_adj.loc[:, 'emp_sim']
    # emp_adj = emp_adj.fillna(0)
    # if CBP < LEHD, do not adjust employment -> to keep employments from imputation step
    emp_adj.loc[emp_adj['emp_adj']<1, 'emp_adj'] = 1
    
    emp_adj = emp_adj[['industry', 'CBPZONE', 'emp_adj']]
    firms.drop(columns = ['emp_adj'], inplace = True)
    firms = pd.merge(firms, emp_adj, 
                      on = ['industry', 'CBPZONE'],
                      how = 'left')
    
    firms.loc[:, 'emp_adj'].fillna(1, inplace = True)
    firms.loc[:, 'emp_per_est'] *= firms.loc[:, 'emp_adj']
    # firms.loc[:, 'emp_per_est'] =np.round(firms.loc[:, 'emp_per_est'].astype(float), 2)
    
    # validate employment
    total_employment_est = firms.loc[:, 'emp_per_est'].sum()
    
    print('Total number of of modeled employment before LEHD scaling:')
    print(total_employment_est)
    
    # <codecell>
    
    # separate firms inside/outside study reion
    firms['ZIPCODE'] = firms['ZIPCODE'].astype(np.int64)
    emp_ranking_in_boundary = mzemp.dropna(subset = ['COUNTY'])
    emp_ranking_in_boundary = \
        emp_ranking_in_boundary.rename(columns = {"COUNTY": 'CBPZONE'})
    cbpzone_in_region = emp_ranking_in_boundary.CBPZONE.unique()
    
    
    essential_attr = ['CBPZONE', 'FAFZONE',	'esizecat', 'Industry_NAICS6_Make', 'COUNTY', 'ZIPCODE',
                    'Commodity_SCTG', 'emp_per_est', 'BusID', 'industry']
    if assign_enterprises:
        essential_attr.append('CoStarEntpID')

    firms_out_boundary = \
        firms.loc[~firms['CBPZONE'].isin(cbpzone_in_region), essential_attr]
    
    firms_in_boundary = \
        firms.loc[firms['CBPZONE'].isin(cbpzone_in_region), essential_attr]
    
    
    print('Number of firms outside study area:')
    print(len(firms_out_boundary))
    
    print('Number of firms within study area:')
    print(len(firms_in_boundary))
    
    # <codecell>
    
    # generate mesozone ID for firms outside boundary
    firms_out_boundary = pd.merge(firms_out_boundary,
                                  mesozone_to_cbpzone,
                                  on = 'CBPZONE', how = 'left')
    
    firms_out_boundary = \
        firms_out_boundary.groupby(essential_attr).sample(1, replace = False, random_state = 1)
    
    # assign mesozone for firms within boundary
    # <codecell>
    # separate firms with/without zipcode
    firms_in_boundary_nozip = firms_in_boundary.loc[firms_in_boundary['ZIPCODE'] == 99999]
    firms_in_boundary_withzip = firms_in_boundary.loc[firms_in_boundary['ZIPCODE'] != 99999]
    
    # create tract ID from CBG id
    emp_ranking_in_boundary.loc[:, 'MESOZONE'] = \
        emp_ranking_in_boundary.loc[:, 'MESOZONE'].astype(np.int64).astype(str).str.zfill(12)
    emp_ranking_in_boundary.loc[:, 'geoid'] = \
        emp_ranking_in_boundary.loc[:, 'MESOZONE'].str[0:11]
    
    # <codecell>
    zip_to_tract_crosswalk['zip'] = zip_to_tract_crosswalk['zip'].astype(np.int64)
    zip_to_tract_crosswalk.loc[:, 'geoid'] = \
        zip_to_tract_crosswalk.loc[:, 'geoid'].astype(np.int64).astype(str).str.zfill(11)
        
    emp_ranking_in_boundary = pd.melt(emp_ranking_in_boundary, 
                                      id_vars = ['CBPZONE', 'MESOZONE', 'geoid'],
                                      value_vars = emp_colnames,
                                      var_name= 'industry', value_name='emp_lehd')
    
    emp_ranking_in_boundary = emp_ranking_in_boundary.reset_index()
    emp_ranking_in_boundary = \
        emp_ranking_in_boundary.loc[emp_ranking_in_boundary['emp_lehd']>0]
    # if a CBG has 0 employment for selected industry, it is not a valid candidate
    # therefore, CBGs with missing ranking is dropped
    emp_ranking_in_boundary.loc[:, 'industry'] = \
        emp_ranking_in_boundary.loc[:, 'industry'].str.split('rank').str[1]
        
    print('Total LEHD employment within study area:')
    print(emp_ranking_in_boundary.emp_lehd.sum())
    
    # <codecell>
    
    # assign mesozone to firms with zip code ID
    
    industries = emp_ranking_in_boundary.loc[:, 'industry'].unique()
    zip_to_tract_crosswalk.rename(columns = {'zip': 'ZIPCODE'}, inplace = True)
    zip_to_tract_crosswalk.drop(columns = ['bus_ratio', 'fraction'], inplace = True)
    # print(len(firms_in_boundary_withzip))
    
    # <codecell>
    firms_out_withzip = None
    
    nozip_col = firms_in_boundary_nozip.columns
    for ind in industries:
    
        firms_to_assign = \
            firms_in_boundary_withzip.loc[firms_in_boundary_withzip['industry'] == ind]
        print('numbers of firms to assign from industry = ' + str(ind))
        print(len(firms_to_assign.BusID.unique()))
        
        firms_to_assign = pd.merge(firms_to_assign, zip_to_tract_crosswalk,
                                   on = 'ZIPCODE', how = 'left')
        firms_to_assign = pd.merge(firms_to_assign, emp_ranking_in_boundary,
                                   on = ['CBPZONE', 'geoid', 'industry'], how = 'left')
        
        # find firms that do not have valid mesozone in the CBP region
        firms_in_boundary_nozip_add = firms_to_assign.loc[firms_to_assign['emp_lehd'].isna()]
        firms_in_boundary_nozip_add = firms_in_boundary_nozip_add[nozip_col]
        firms_in_boundary_nozip_add.drop_duplicates(keep = 'first', inplace = True)
    
            
        firms_to_assign = firms_to_assign.dropna(subset = ['emp_lehd'])
        bus_ids = firms_to_assign.BusID.unique()
        firms_in_boundary_nozip_add = \
            firms_in_boundary_nozip_add[~firms_in_boundary_nozip_add['BusID'].isin(bus_ids)]
        # print('firms without valid CBG in Zip code:')
        # print(len(firms_in_boundary_nozip_add))
        firms_in_boundary_nozip = \
            pd.concat([firms_in_boundary_nozip, firms_in_boundary_nozip_add])
        if firms_to_assign is not None:
            if len(firms_to_assign) > 0:
        # print(len(firms_to_assign.BusID.unique()))
        
        # Sometimes, LODES report 0 employment in a county, while firm data as non-zero
        # may attributed to imputation for non-payroll workers
        # fill minimum ranking for all zones as no information is available for the ranking
        
                firms_to_assign = \
                    firms_to_assign.groupby(essential_attr).sample(1,
                                                                    weights = firms_to_assign['emp_lehd'],
                                                                    replace = False, random_state = 1)
                firms_to_assign = \
                    firms_to_assign.drop(columns = ['index', 'industry', 'emp_lehd'])
                firms_to_assign.loc[:, 'MESOZONE'].fillna(method = 'ffill', inplace = True)
                firms_to_assign.loc[:, 'MESOZONE'].fillna(method = 'bfill', inplace = True)
                
                firms_out_withzip = pd.concat([firms_out_withzip, firms_to_assign])
        
        # break
    print('Firms in region with valid zip:')
    print(len(firms_out_withzip))
    
    print('Firms in region without valid zip:')
    print(len(firms_in_boundary_nozip))
    
    # <codecell>
    
    # assign mesozone to firms without zip code
    firms_out_nozip = None
    final_missing = None
    
    def split_dataframe(df, chunk_size = 10 ** 5): 
        chunks = list()
        num_chunks = len(df) // chunk_size + 1
        for i in range(num_chunks):
            chunks.append(df[i*chunk_size:(i+1)*chunk_size])
        return chunks
    
    for ind in industries:
    
        firms_to_assign = firms_in_boundary_nozip.loc[firms_in_boundary_nozip['industry'] == ind]
        print('Numbers of firms without ZIP CODE to assign from industry = ' + str(ind))
        print(len(firms_to_assign.BusID.unique()))
        chunks = split_dataframe(firms_to_assign, 10000)
        post_firms_to_assign = None
        for chunk in chunks:
    
            chunk = pd.merge(chunk, emp_ranking_in_boundary,
                                       on = ['CBPZONE', 'industry'], how = 'left')
            
            chunk_missing = chunk.loc[chunk['emp_lehd'].isna()]
            chunk_missing = chunk_missing[nozip_col]
            chunk_missing.drop_duplicates(keep = 'first', inplace = True)
    
                
            chunk = chunk.dropna(subset = ['emp_lehd'])
            bus_ids = chunk.BusID.unique()
            chunk_missing = \
                chunk_missing[~chunk_missing['BusID'].isin(bus_ids)]
            final_missing = pd.concat([final_missing, chunk_missing])
    
            chunk = chunk.dropna(subset = ['emp_lehd'])
            chunk = \
                chunk.groupby(essential_attr).sample(1,
                                                     weights = chunk['emp_lehd'],
                                                     replace = False, random_state = 1)
            chunk = \
                chunk.drop(columns = ['index', 'industry', 'emp_lehd'])
            chunk.loc[:, 'MESOZONE'].fillna(method = 'ffill', inplace = True)
            chunk.loc[:, 'MESOZONE'].fillna(method = 'bfill', inplace = True)
            post_firms_to_assign = pd.concat([post_firms_to_assign, chunk])
        firms_out_nozip = pd.concat([firms_out_nozip, post_firms_to_assign])
        
        # break
    # print(len(firms_out_nozip))
    
    
    # <codecell> 
    
    # impute last chunk of missing --> county has no lehd emp by industry, so drop industry
    final_missing.drop(columns = ['industry'], inplace = True)
    final_missing = pd.merge(final_missing, emp_ranking_in_boundary,
                               on = ['CBPZONE'], how = 'left')
    
    final_missing = final_missing.dropna(subset = ['emp_lehd']) 
    essential_attr = ['CBPZONE', 'FAFZONE',	'esizecat', 'Industry_NAICS6_Make', 'COUNTY', 'ZIPCODE',
                    'Commodity_SCTG', 'emp_per_est', 'BusID']
    if assign_enterprises:
        essential_attr.append('CoStarEntpID')
    final_missing = \
                final_missing.groupby(essential_attr).sample(1,
                                                     weights = final_missing['emp_lehd'],
                                                     replace = False, random_state = 1)
    final_missing.drop(columns = ['index', 'industry', 'emp_lehd'], inplace = True)
    
    
        
    # <codecell>
    
    ####################################################
    # Step 4 - final formatting and writing outputs ####
    ####################################################
    
    firms = pd.concat([firms_out_boundary, firms_out_withzip, firms_out_nozip, final_missing])
    
    print('number of firms before writing output:')
    print(len(firms))
        
    output_attr = ['CBPZONE', 'FAFZONE',	'esizecat', 'Industry_NAICS6_Make',
                    'Commodity_SCTG', 'emp_per_est', 'BusID', 'MESOZONE', 'ZIPCODE']
    if assign_enterprises:
        output_attr.append('CoStarEntpID')
    
    firms = firms[output_attr]
    firms = firms.rename(columns = {'emp_per_est': 'Emp'})

    # apply the right data format before exporting    
    firms = firms.astype({
    'CBPZONE': np.int64,
    'FAFZONE': np.int64,
    'esizecat': np.int64, 
    'Industry_NAICS6_Make': 'string',
    'Commodity_SCTG': np.int64,
    'Emp': 'float',
    'BusID': np.int64, 
    'MESOZONE': np.int64, 
    'ZIPCODE': np.int64
    })
    if assign_enterprises:
        firms['CoStarEntpID'] = firms['CoStarEntpID'].astype(np.int64)
    firms.to_csv(synthetic_firms_no_location_file, index = False)
    print('Synthetic firms without location attributes generated!')
    print('Current output located at ' + synthetic_firms_no_location_file)