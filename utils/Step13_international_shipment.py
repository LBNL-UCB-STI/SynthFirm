#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun  6 13:59:01 2024

@author: xiaodanxu
"""

import pandas as pd
import os
import numpy as np
from pandas import read_csv
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings("ignore")



########################################################
#### step 1 - configure environment and load inputs ####
########################################################

# load model config temporarily here
# scenario_name = 'Seattle'
# out_scenario_name = 'Seattle'
# file_path = '/Users/xiaodanxu/Documents/SynthFirm.nosync'
# parameter_dir = 'SynthFirm_parameters'
# input_dir = 'inputs_' + scenario_name
# output_dir = 'outputs_' + out_scenario_name

# c_n6_n6io_sctg_file = 'corresp_naics6_n6io_sctg_revised.csv'
# # synthetic_firms_no_location_file = "synthetic_firms.csv" 
# zonal_id_file = "zonal_id_lookup_final.csv" # zonal ID lookup table 
# # international flow
# regional_import_file = 'port/FAF_regional_import.csv'
# regional_export_file = 'port/FAF_regional_export.csv'
# port_level_import_file = 'port/port_level_import.csv'
# port_level_export_file = 'port/port_level_export.csv'
# sctg_group_file = 'SCTG_Groups_revised.csv'
# int_shipment_size_file = 'international_shipment_size.csv'
# sctg_by_port_file = 'commodity_to_port_constraint.csv'

    # need_domestic_adjustment = 1 # 1 - yes, 0 - no
    # location_from = [61, 63]
    # location_to = [62, 64, 65, 69]
    


def international_demand_generation(c_n6_n6io_sctg_file, sctg_by_port_file,
                                    sctg_group_file, int_shipment_size_file,
                                    regional_import_file, regional_export_file, 
                                    port_level_import_file, port_level_export_file,
                                    need_domestic_adjustment, import_od, export_od,
                                    output_path, 
                                    location_from = None, location_to = None):
    print("Start international shipment generation...")
    # mesozone_faf_lookup = read_csv(zonal_id_file)
    c_n6_n6io_sctg = read_csv(c_n6_n6io_sctg_file)
    sctg_lookup = read_csv(sctg_group_file)
    int_shipment_size = read_csv(int_shipment_size_file)
    sctg_by_port_constraint = read_csv( sctg_by_port_file)
    
    regional_import = read_csv(regional_import_file)
    regional_export = read_csv(regional_export_file)
    port_level_import = read_csv(port_level_import_file)
    port_level_export = read_csv(port_level_export_file)
    
    # pre-select data
    sctg_lookup_short = sctg_lookup[['SCTG_Code', 'SCTG_Group']]
    int_shipment_size_short = \
        int_shipment_size[['SCTG', 'CFS_CODE', 'median_weight_ton']]  
    int_shipment_size_short = \
        int_shipment_size_short.rename(columns = {'SCTG': 'SCTG_Code'})
    
    # split fuel and non-automobile transport equipment
    port_level_import_fuel = port_level_import.loc[port_level_import['HS Code'] == 27]
    port_level_import_te = port_level_import.loc[port_level_import['HS Code'] == 88]
    port_level_import = port_level_import.loc[~port_level_import['HS Code'].isin([27,88])]
    
    port_level_export_te = port_level_export.loc[port_level_export['HS Code'] == 88]
    port_level_export = port_level_export.loc[~port_level_export['HS Code'].isin([88])]
    
    regional_import_fuel = regional_import.loc[regional_import['sctg2'] == 16]
    regional_import_te = regional_import.loc[regional_import['sctg2'] == 37]
    regional_import = regional_import.loc[~regional_import['sctg2'].isin([16, 37])]
    
    regional_export_te = regional_export.loc[regional_export['sctg2'] == 37]
    regional_export = regional_export.loc[~regional_export['sctg2'].isin([37])]
    
    calibrated_sctg = 37
    calibrated_value = 200
    
    ########################################################
    #### step 1 - Scale regional import-export flow ########
    ########################################################
    
    def scale_demand(faf_data, usato_data, usato_value, output_attr): 
        faf_scaling = \
            faf_data.groupby(['CFS_CODE', 'CFS_NAME'])['value_2017'].sum()
        faf_scaling = faf_scaling.reset_index()
        faf_scaling = \
        faf_scaling.rename(columns = {'value_2017':'region total'})    
        usato_scaling = \
            usato_data.groupby(['CFS_CODE', 'CFS_NAME'])[[usato_value]].sum()
        usato_scaling.columns = ['usato value']
        usato_scaling.loc[:, 'usato value'] /= 10 ** 6 # convert value to million
        usato_scaling = usato_scaling.reset_index()
        regional_scaling = pd.merge(usato_scaling, faf_scaling,
                                  on = ['CFS_CODE', 'CFS_NAME'], how = 'left')
        regional_scaling.loc[:, output_attr] = \
            regional_scaling.loc[:, 'usato value']/ \
            regional_scaling.loc[:, 'region total']
        regional_scaling = \
            regional_scaling[['CFS_CODE', 'CFS_NAME', output_attr]]
        return(regional_scaling)
    
    regional_import_scaling = scale_demand(regional_import, port_level_import,
                                           'Customs Value (Gen) ($US)', 'import_frac')
    regional_import_scaling_te = scale_demand(regional_import_te, port_level_import_te,
                                           'Customs Value (Gen) ($US)', 'import_frac')

    regional_export_scaling = scale_demand(regional_export, port_level_export,
                                           'Total Exports Value ($US)', 'export_frac')
    regional_export_scaling_te = scale_demand(regional_export_te, port_level_export_te,
                                           'Total Exports Value ($US)', 'export_frac')
    
    
    # <codecell>
    
    ########################################################
    #### step 2 - Generate list of shipments ###############
    ########################################################
    # regional_import_scaling = \
    #     regional_import_scaling[['CFS_CODE', 'CFS_NAME', 'import_frac']]
    # regional_export_scaling = \
    #     regional_export_scaling[['CFS_CODE', 'CFS_NAME', 'export_frac']]

            
    def assign_weight_to_flow(regional_flow, scaling_factor, int_shipment_size, scale_attr, trim_tail = False):
        regional_flow = \
                regional_flow.rename(columns = {'sctg2': 'SCTG_Code'})
        regional_flow_scaled = pd.merge(regional_flow, scaling_factor,
                                          on = ['CFS_CODE', 'CFS_NAME'], how = 'left') 
        regional_flow_scaled.loc[:, 'tons_2017'] *= regional_flow_scaled.loc[:, scale_attr]   
        regional_flow_scaled.loc[:, 'value_2017'] *= regional_flow_scaled.loc[:, scale_attr]   
        regional_flow_scaled = pd.merge(regional_flow_scaled,
                                           int_shipment_size,
                                           on = ['SCTG_Code', 'CFS_CODE'], how = 'left')
        regional_flow_scaled.loc[:, "ship_count"] = \
            regional_flow_scaled.loc[:, "tons_2017"] * 1000 / \
                    regional_flow_scaled.loc[:, "median_weight_ton"] 
        regional_flow_scaled.loc[:, "ship_count"] = \
            np.round(regional_flow_scaled.loc[:, "ship_count"], 0)
            
        regional_flow_scaled.loc[regional_flow_scaled["ship_count"] < 1, "ship_count"] = 1
        
        # import count can be really large, trim off the long tail
        if trim_tail:
            cut_off = np.round(regional_flow_scaled["ship_count"].quantile(0.999), 0)
            regional_flow_scaled.loc[regional_flow_scaled["ship_count"] >= cut_off, "ship_count"] = cut_off      
        return(regional_flow_scaled)  
    
    regional_import_by_size = assign_weight_to_flow(regional_import, 
                                                    regional_import_scaling,
                                                    int_shipment_size_short, 
                                                    'import_frac', trim_tail = True)
    regional_export_by_size = assign_weight_to_flow(regional_export, 
                                                    regional_export_scaling,
                                                    int_shipment_size_short, 
                                                    'export_frac', trim_tail = False)
    # calibrate sctg weight (for airplane)

    int_shipment_size_cal = int_shipment_size_short.copy()
    int_shipment_size_cal.loc[int_shipment_size_cal['SCTG_Code'] == calibrated_sctg, 
                              'median_weight_ton'] = calibrated_value
    
    regional_import_by_size_te = assign_weight_to_flow(regional_import_te, 
                                                    regional_import_scaling_te,
                                                    int_shipment_size_cal, 
                                                    'import_frac', trim_tail = False)
    regional_export_by_size_te = assign_weight_to_flow(regional_export_te, 
                                                    regional_export_scaling_te,
                                                    int_shipment_size_cal, 
                                                    'export_frac', trim_tail = False)

    # append shipment size
                
    print('Total import shipments before scaling:')
    print(regional_import_by_size.loc[:, "ship_count"].sum())
    
    print('Total import value before scaling:')
    print(regional_import_by_size.loc[:, "value_2017"].sum())
    
    print('Total import tonnage before scaling:')
    print(regional_import_by_size.loc[:, "tons_2017"].sum() * 1000)
    
    print('Total export shipments before scaling:')
    print(regional_export_by_size.loc[:, "ship_count"].sum())
    
    print('Total export value before scaling:')
    print(regional_export_by_size.loc[:, "value_2017"].sum())
    
    print('Total export tonnage before scaling:')
    print(regional_export_by_size.loc[:, "tons_2017"].sum() * 1000)
    
    # <codecell>
    
    ########################################################
    #### step 3 - Assign airport flow first (less sctg) ####
    ########################################################
    
    # import by port
    
    var_to_group = ['PORTID', 'CBP Port Location', 'FAF', 'CBPZONE', 'MESOZONE', 'TYPE', 
                    'is_airport', 'CFS_CODE', 'CFS_NAME']
    import_by_port = port_level_import.groupby(var_to_group)[['Customs Value (Gen) ($US)']].sum()
    import_by_port.columns = ['import value']
    import_by_port.loc[:, 'import value'] /= 10 ** 6 # convert value to million
    import_by_port = import_by_port.reset_index()
    
    import_by_port_te = port_level_import_te.groupby(var_to_group)[['Customs Value (Gen) ($US)']].sum()
    import_by_port_te.columns = ['import value']
    import_by_port_te.loc[:, 'import value'] /= 10 ** 6 # convert value to million
    import_by_port_te = import_by_port_te.reset_index()
    
    # export by port
    export_by_port = port_level_export.groupby(var_to_group)[['Total Exports Value ($US)']].sum()
    export_by_port.columns = ['export value']
    export_by_port.loc[:, 'export value'] /= 10 ** 6 # convert value to million
    export_by_port = export_by_port.reset_index()
    
    export_by_port_te = port_level_export_te.groupby(var_to_group)[['Total Exports Value ($US)']].sum()
    export_by_port_te.columns = ['export value']
    export_by_port_te.loc[:, 'export value'] /= 10 ** 6 # convert value to million
    export_by_port_te = export_by_port_te.reset_index()
    
    # split airport/other port
    import_by_airport = import_by_port.loc[import_by_port['TYPE'] == 'Airport']
    import_by_other_port = import_by_port.loc[import_by_port['TYPE'] != 'Airport']
    
    export_by_airport = export_by_port.loc[export_by_port['TYPE'] == 'Airport']
    export_by_other_port = export_by_port.loc[export_by_port['TYPE'] != 'Airport']
    
    # select faf values with SCTG accessible at airport
    
    sctg_with_ap_access = \
        sctg_by_port_constraint.loc[sctg_by_port_constraint['Airport'] == 1, 'SCTG'].unique()
    
    faf_import_ap_only = \
        regional_import_by_size.loc[regional_import_by_size['SCTG_Code'].isin(sctg_with_ap_access)]
    faf_export_ap_only = \
            regional_export_by_size.loc[regional_export_by_size['SCTG_Code'].isin(sctg_with_ap_access)]
    
    def port_allocation_factor(faf_data, flow_by_port, input_attr, output_attr):
        faf_data = faf_data.groupby(['CFS_CODE', 'CFS_NAME'])['value_2017'].sum()
        faf_data = faf_data.reset_index()
        faf_data = \
            faf_data.rename(columns = {'value_2017':'sector total'})
        flow_by_port = pd.merge(flow_by_port, faf_data,
                                  on = ['CFS_CODE', 'CFS_NAME'], how = 'left')
        flow_by_port.loc[:, output_attr] = flow_by_port.loc[:, input_attr]/ \
            flow_by_port.loc[:, 'sector total']
        return(flow_by_port)
    
    import_by_airport = port_allocation_factor(faf_import_ap_only, 
                                               import_by_airport, 'import value', 'import_frac')
    export_by_airport = port_allocation_factor(faf_export_ap_only, 
                                               export_by_airport, 'export value', 'export_frac')
    
    import_by_port_te = port_allocation_factor(regional_import_by_size_te, 
                                               import_by_port_te, 'import value', 'import_frac')
    export_by_port_te = port_allocation_factor(regional_export_by_size_te, 
                                               export_by_port_te, 'export value', 'export_frac')
    print('range of import adj. factors')
    print(import_by_airport['import_frac'].min(), import_by_airport['import_frac'].max())
    # export_by_airport['export_frac'].hist()
    print('range of export adj. factors')
    print(export_by_airport['export_frac'].min(), export_by_airport['export_frac'].max())
    
    
    # <codecell>
    def adjust_demand(faf_data, adjustment_factor, multiplier, agg_var = None):
        if agg_var is not None:
            """Adjust international demand"""
            faf_data = faf_data.groupby(agg_var)[['tons_2017', 'value_2017', 'ship_count']].sum()
            faf_data = faf_data.reset_index()
            faf_data = \
            faf_data.loc[faf_data['value_2017'] > 0]
        
        flow_by_port_to_adj = pd.merge(adjustment_factor, faf_data,  
                                       on= ['CFS_CODE', 'CFS_NAME'], 
                                       how='left')
        
        
        flow_by_port_to_adj.loc[:, 'tons_2017'] *= flow_by_port_to_adj.loc[:, multiplier]
        flow_by_port_to_adj.loc[:, 'value_2017'] *= flow_by_port_to_adj.loc[:, multiplier]
        flow_by_port_to_adj.loc[:, 'ship_count'] *= flow_by_port_to_adj.loc[:, multiplier]
        # print(flow_by_port_to_adj['ship_count'].sum())
        flow_by_port_to_adj = flow_by_port_to_adj.loc[flow_by_port_to_adj['ship_count'] >= 1]
        flow_by_port_to_adj.loc[:, 'ship_count'] = np.round(flow_by_port_to_adj.loc[:, 'ship_count'], 0)
        
        flow_by_port_to_adj.loc[:, 'value_density'] = \
            flow_by_port_to_adj.loc[:, 'value_2017'] / flow_by_port_to_adj.loc[:, 'tons_2017'] * 1000
        # $/ton
        flow_by_port_to_adj.loc[:, "TruckLoad"] = flow_by_port_to_adj.loc[:, "tons_2017"] * 1000  / \
                    flow_by_port_to_adj.loc[:, "ship_count"] # unit is ton
        # ton            
        return flow_by_port_to_adj
    
    # assign destination to airport import
    agg_var_in = ['CFS_CODE', 'CFS_NAME', 'dms_dest', 'SCTG_Code']
    import_by_airport_by_dest = adjust_demand(faf_import_ap_only, import_by_airport, 
                                              'import_frac', agg_var_in)
    import_by_port_te = adjust_demand(regional_import_by_size_te, import_by_port_te, 
                                              'import_frac', agg_var_in)
    # $/ton
    
    print('airport import shipment after disaggregation:')
    print(import_by_airport_by_dest['ship_count'].sum()) 
    
    print('airport import value (million $) after disaggregation:')
    print(import_by_airport_by_dest['value_2017'].sum())    
    
    print('airport import tonnage after disaggregation:')
    print(import_by_airport_by_dest['tons_2017'].sum() * 1000)  
    
    
    # FAF value grouped by foreign country, domestic destination and sctg for entire region
    agg_var_out = ['CFS_CODE', 'CFS_NAME', 'dms_orig', 'SCTG_Code']
    export_by_airport_by_orig = adjust_demand(faf_export_ap_only, export_by_airport, 
                                              'export_frac', agg_var_out)
    export_by_port_te = adjust_demand(regional_export_by_size_te, export_by_port_te, 
                                              'export_frac', agg_var_out)
    print('airport export shipment after disaggregation:')
    print(export_by_airport_by_orig['ship_count'].sum())  
    
    print('airport export value (million $) after disaggregation:')
    print(export_by_airport_by_orig['value_2017'].sum()) 
    
    print('airport export tonnage after disaggregation:')
    print(export_by_airport_by_orig['tons_2017'].sum() * 1000)  
    
    # <codecell>
    
    
    ########################################################
    #### step 5 - Assign rest of flow to other port ########
    ########################################################
    def exclude_ap_demand(faf_data, airport_flow, agg_var):
        """Adjust international demand"""
        faf_data_agg = faf_data.groupby(agg_var)[['tons_2017', 'value_2017', 'ship_count']].sum()
        faf_data_agg = faf_data_agg.reset_index()
        faf_data_agg = \
        faf_data_agg.loc[faf_data_agg['value_2017'] > 0]
        
        airport_flow = \
            airport_flow.groupby(agg_var)[['value_2017']].sum()
        airport_flow = airport_flow.reset_index()
        airport_flow = \
        airport_flow.rename(columns = {'value_2017': 'values_airport'})
        
        flow_by_port_to_adj = pd.merge(faf_data_agg, airport_flow, 
                                       on= agg_var, 
                                       how='left')
        
        flow_by_port_to_adj = flow_by_port_to_adj.fillna(0)
        flow_by_port_to_adj.loc[:, 'value_other'] = \
            flow_by_port_to_adj.loc[:, 'value_2017'] - \
                flow_by_port_to_adj.loc[:, 'values_airport']
        
        flow_by_port_to_adj.loc[:, 'scaling_factor'] = \
            flow_by_port_to_adj.loc[:, 'value_other'] / \
                flow_by_port_to_adj.loc[:, 'value_2017']
        
        flow_by_port_to_adj.loc[:, 'tons_2017'] *= flow_by_port_to_adj.loc[:,'scaling_factor']
        flow_by_port_to_adj.loc[:, 'value_2017'] *= flow_by_port_to_adj.loc[:, 'scaling_factor']
        flow_by_port_to_adj.loc[:, 'ship_count'] *= flow_by_port_to_adj.loc[:, 'scaling_factor']
        flow_by_port_to_adj.drop(columns = ['values_airport','value_other'], inplace = True)                 
        return flow_by_port_to_adj
    
    # adjust import to assign
    remain_imports_to_assign = exclude_ap_demand(regional_import_by_size, import_by_airport_by_dest, 
                                                   agg_var_in)
  
    # adjust export to assign
    remain_exports_to_assign = exclude_ap_demand(regional_export_by_size, export_by_airport_by_orig, 
                                                   agg_var_out)
    
    import_by_other_port = port_allocation_factor(remain_imports_to_assign, 
                                               import_by_other_port, 'import value', 'import_frac')
    export_by_other_port = port_allocation_factor(remain_exports_to_assign, 
                                               export_by_other_port, 'export value', 'export_frac')    
    # import_by_other_port['import_frac'].hist()
    print('range of import adj. factors')
    print(import_by_other_port['import_frac'].min(), 
          import_by_other_port['import_frac'].max())
    # export_by_other_port['export_frac'].hist()
    print('range of export adj. factors')
    print(export_by_other_port['export_frac'].min(), 
          export_by_other_port['export_frac'].max())
    
    # <codecell>
    # assign destination to other ports import
    import_by_other_port_by_dest = adjust_demand(remain_imports_to_assign, 
                                                 import_by_other_port, 
                                                 'import_frac')

    
    
    
    print('other import shipment after disaggregation:')
    print(import_by_other_port_by_dest['ship_count'].sum()) 
    
    print('other import value (million $) after disaggregation:')
    print(import_by_other_port_by_dest['value_2017'].sum())    
    
    print('other import tonnage after disaggregation:')
    print(import_by_other_port_by_dest['tons_2017'].sum() * 1000)  
    
    export_by_other_port_by_orig = adjust_demand(remain_exports_to_assign, 
                                                 export_by_other_port, 
                                                 'export_frac')    

    
    print('other export shipment after disaggregation:')
    print(export_by_other_port_by_orig['ship_count'].sum())  
    
    print('other export value (million $) after disaggregation:')
    print(export_by_other_port_by_orig['value_2017'].sum()) 
    
    print('other export tonnage after disaggregation:')
    print(export_by_other_port_by_orig['tons_2017'].sum() * 1000)  
    
      
    # <codecell>
    
    import_attr = ['PORTID', 'CBP Port Location', 'FAF', 'CBPZONE', 'MESOZONE', 'TYPE', 
                   'is_airport', 'CFS_CODE', 'CFS_NAME', 'dms_dest', 'SCTG_Code', 
                   'TruckLoad', 'ship_count', 'value_2017', 'value_density']
    export_attr = ['PORTID', 'CBP Port Location', 'FAF', 'CBPZONE', 'MESOZONE', 'TYPE', 
                   'is_airport', 'CFS_CODE', 'CFS_NAME','dms_orig', 'SCTG_Code', 
                   'TruckLoad', 'ship_count', 'value_2017', 'value_density']
    
    import_output_airport = import_by_airport_by_dest[import_attr]
    import_output_other_port = import_by_other_port_by_dest[import_attr]
    import_by_port_te = import_by_port_te[import_attr]
    import_by_port_by_dest = pd.concat([import_output_airport,
                                        import_output_other_port,
                                        import_by_port_te])
    
    
    export_output_airport = export_by_airport_by_orig[export_attr]
    export_output_other_port = export_by_other_port_by_orig[export_attr]
    export_by_port_te = export_by_port_te[export_attr]
    export_by_port_by_orig = pd.concat([export_output_airport,
                                        export_output_other_port,
                                        export_by_port_te])
    
    
    
    # <codecell>
    
    
    
    ########################################################
    #### step 5 - Adjust FAF destination assignment ########
    ########################################################
    
    
    if need_domestic_adjustment:
        included_locations = location_from + location_to
        # shift import demand
        import_by_port_by_dest_to_adj = \
            import_by_port_by_dest.loc[import_by_port_by_dest['dms_dest'].isin(location_from)]
        import_by_port_by_dest_no_adj = \
            import_by_port_by_dest.loc[import_by_port_by_dest['dms_dest'].isin(location_to)]
        import_by_port_by_dest_remaining = \
            import_by_port_by_dest.loc[~import_by_port_by_dest['dms_dest'].isin(included_locations)]
        
        import_by_port_by_dest_to_adj.drop(columns = ['dms_dest'], inplace = True)
        import_relocation_factor = \
            import_by_port_by_dest_no_adj.groupby(['dms_dest', 'SCTG_Code'])[['value_2017']].sum()
        import_relocation_factor = import_relocation_factor.reset_index()
        import_relocation_factor.loc[:, 'Portion'] = \
            import_relocation_factor.loc[:, 'value_2017'] / \
                import_relocation_factor.groupby(['SCTG_Code'])['value_2017'].transform('sum')
        
        import_relocation_factor.drop(columns = 'value_2017', inplace = True)
        import_by_port_by_dest_to_adj = pd.merge(import_by_port_by_dest_to_adj,
                                                 import_relocation_factor, 
                                                 on = 'SCTG_Code', how = 'left')
        import_by_port_by_dest_to_adj.loc[:, 'ship_count'] = \
            import_by_port_by_dest_to_adj.loc[:, 'ship_count'] * import_by_port_by_dest_to_adj.loc[:, 'Portion']
        import_by_port_by_dest_to_adj.loc[:, 'value_2017'] = \
            import_by_port_by_dest_to_adj.loc[:, 'value_2017'] * import_by_port_by_dest_to_adj.loc[:, 'Portion']
        import_by_port_by_dest_to_adj.drop(columns = 'Portion', inplace = True)
        
        import_by_port_by_dest_adjusted = pd.concat([import_by_port_by_dest_to_adj, 
                                                      import_by_port_by_dest_no_adj])
        
        grouping_var = ['PORTID', 'CBP Port Location', 'FAF', 'CBPZONE', 'MESOZONE', 'TYPE', 
                        'is_airport', 'CFS_CODE', 'CFS_NAME', 'dms_dest', 'SCTG_Code']
        import_by_port_by_dest_adjusted.loc[:, 'tons_2017'] = \
            import_by_port_by_dest_adjusted.loc[:, 'ship_count'] * \
                import_by_port_by_dest_adjusted.loc[:, 'TruckLoad'] / 1000
        import_by_port_by_dest_adjusted = \
            import_by_port_by_dest_adjusted.groupby(grouping_var)[['value_2017', 'tons_2017', 'ship_count']].sum()
        import_by_port_by_dest_adjusted = import_by_port_by_dest_adjusted.reset_index()
        import_by_port_by_dest_adjusted.loc[:, "ship_count"] = \
            np.round(import_by_port_by_dest_adjusted.loc[:, "ship_count"], 0)
            
        import_by_port_by_dest_adjusted.loc[import_by_port_by_dest_adjusted["ship_count"] < 1, "ship_count"] = 1
        import_by_port_by_dest_adjusted.loc[:, 'value_density'] = \
            import_by_port_by_dest_adjusted.loc[:, 'value_2017'] / import_by_port_by_dest_adjusted.loc[:, 'tons_2017'] * 1000
        import_by_port_by_dest_adjusted.loc[:, "TruckLoad"] = import_by_port_by_dest_adjusted.loc[:, "tons_2017"] * 1000  / \
                    import_by_port_by_dest_adjusted.loc[:, "ship_count"] # unit is ton
        import_by_port_by_dest_adjusted = import_by_port_by_dest_adjusted[import_attr]  
        import_by_port_by_dest = pd.concat([import_by_port_by_dest_adjusted,
                                            import_by_port_by_dest_remaining])
        
        
        # shift export demand
        export_by_port_by_orig_to_adj = \
            export_by_port_by_orig.loc[export_by_port_by_orig['dms_orig'].isin(location_from)]
        export_by_port_by_orig_no_adj = \
            export_by_port_by_orig.loc[export_by_port_by_orig['dms_orig'].isin(location_to)]
        export_by_port_by_orig_remaining = \
            export_by_port_by_orig.loc[~export_by_port_by_orig['dms_orig'].isin(included_locations)]
        
        export_by_port_by_orig_to_adj.drop(columns = ['dms_orig'], inplace = True)
        export_relocation_factor = \
            export_by_port_by_orig_no_adj.groupby(['dms_orig', 'SCTG_Code'])[['value_2017']].sum()
        export_relocation_factor = export_relocation_factor.reset_index()
        export_relocation_factor.loc[:, 'Portion'] = \
            export_relocation_factor.loc[:, 'value_2017'] / \
                export_relocation_factor.groupby(['SCTG_Code'])['value_2017'].transform('sum')
        
        export_relocation_factor.drop(columns = 'value_2017', inplace = True)
        export_by_port_by_orig_to_adj = pd.merge(export_by_port_by_orig_to_adj,
                                                 export_relocation_factor, 
                                                 on = 'SCTG_Code', how = 'left')
        export_by_port_by_orig_to_adj.loc[:, 'ship_count'] = \
            export_by_port_by_orig_to_adj.loc[:, 'ship_count'] * export_by_port_by_orig_to_adj.loc[:, 'Portion']
        export_by_port_by_orig_to_adj.loc[:, 'value_2017'] = \
            export_by_port_by_orig_to_adj.loc[:, 'value_2017'] * export_by_port_by_orig_to_adj.loc[:, 'Portion']
        export_by_port_by_orig_to_adj.drop(columns = 'Portion', inplace = True)
        
        export_by_port_by_orig_adjusted = pd.concat([export_by_port_by_orig_to_adj, 
                                                      export_by_port_by_orig_no_adj])
        
        grouping_var = ['PORTID', 'CBP Port Location', 'FAF', 'CBPZONE', 'MESOZONE', 'TYPE', 
                        'is_airport', 'CFS_CODE', 'CFS_NAME', 'dms_orig', 'SCTG_Code']
        export_by_port_by_orig_adjusted.loc[:, 'tons_2017'] = \
            export_by_port_by_orig_adjusted.loc[:, 'ship_count'] * \
                export_by_port_by_orig_adjusted.loc[:, 'TruckLoad'] / 1000
        export_by_port_by_orig_adjusted = \
            export_by_port_by_orig_adjusted.groupby(grouping_var)[['value_2017', 'tons_2017', 'ship_count']].sum()
        export_by_port_by_orig_adjusted = export_by_port_by_orig_adjusted.reset_index()
        export_by_port_by_orig_adjusted.loc[:, "ship_count"] = \
            np.round(export_by_port_by_orig_adjusted.loc[:, "ship_count"], 0)
            
        export_by_port_by_orig_adjusted.loc[export_by_port_by_orig_adjusted["ship_count"] < 1, "ship_count"] = 1
        export_by_port_by_orig_adjusted.loc[:, 'value_density'] = \
            export_by_port_by_orig_adjusted.loc[:, 'value_2017'] / export_by_port_by_orig_adjusted.loc[:, 'tons_2017'] * 1000
        export_by_port_by_orig_adjusted.loc[:, "TruckLoad"] = export_by_port_by_orig_adjusted.loc[:, "tons_2017"] * 1000  / \
                    export_by_port_by_orig_adjusted.loc[:, "ship_count"] # unit is ton
        export_by_port_by_orig_adjusted = export_by_port_by_orig_adjusted[export_attr]  
        export_by_port_by_orig = pd.concat([export_by_port_by_orig_adjusted,
                                            export_by_port_by_orig_remaining])
        
    # <codecell>
    
    # output_path = os.path.join(file_path, output_dir)
    import_by_port_by_dest = pd.merge(import_by_port_by_dest,
                                       sctg_lookup_short,
                                       on = ['SCTG_Code'], how = 'left')
    export_by_port_by_orig = pd.merge(export_by_port_by_orig,
                                       sctg_lookup_short,
                                       on = ['SCTG_Code'], how = 'left')
    
    import_by_port_by_dest.to_csv(os.path.join(output_path, import_od), index = False)
    export_by_port_by_orig.to_csv(os.path.join(output_path, export_od), index = False)
    
    print('Total import shipments after scaling:')
    print(import_by_port_by_dest.loc[:, "ship_count"].sum())
    
    print('Total export shipments after scaling:')
    print(export_by_port_by_orig.loc[:, "ship_count"].sum())