#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  4 10:20:28 2024

@author: xiaodanxu
"""

import pandas as pd
import os
import numpy as np
from pandas import read_csv
import warnings
warnings.filterwarnings("ignore")



########################################################
#### step 1 - configure environment and load inputs ####
########################################################


# scenario_name = 'Seattle'
# out_scenario_name = 'Seattle'
# file_path = '/Users/xiaodanxu/Documents/SynthFirm.nosync'
# parameter_dir = 'SynthFirm_parameters'
# input_dir = 'inputs_' + scenario_name
# output_dir = 'outputs_' + out_scenario_name

# specifications that has been defined before
c_n6_n6io_sctg_file = 'corresp_naics6_n6io_sctg_revised.csv'
synthetic_firms_no_location_file = "synthetic_firms.csv" 

# specifications that are new to this code
mesozone_to_faf_file = "zonal_id_lookup_final.csv" # zonal ID lookup table 
# foreign_prod_file = "data_foreign_prod.csv" # total foreign production (pre-defined from prior studies)
BEA_io_2017_file = "data_2017io_revised_USE_value_added.csv" # final scaled BEA I-O use table
agg_unit_cost_file = "data_unitcost_calib.csv" # unit cost by commodity from CFS 2017 (all zones combined)
prod_by_zone_file = "producer_value_fraction_by_faf.csv" # Total production value by FAF zone from CFS 2017
sctg_group_file = "SCTG_Groups_revised.csv" # Commodity type to group lookup (pre-defined)

io_summary_file = "io_summary_revised.csv" # i-o table in long format, without zero values
wholesaler_file = "synthetic_wholesaler.csv" # synthetic wholesaler (serve as both buyer and supplier)
producer_file = "synthetic_producers.csv" # synthetic producer
producer_by_sctg_filehead = "prods_sctg" # file head for synthetic producer by sctg
io_filtered_file = "data_2017io_filtered.csv" # processed I-O table, after dropping wholesale transaction

def producer_generation(c_n6_n6io_sctg_file, synthetic_firms_no_location_file,
                        mesozone_to_faf_file, BEA_io_2017_file, agg_unit_cost_file,
                        prod_by_zone_file, sctg_group_file, io_summary_file,
                        wholesaler_file, producer_file, producer_by_sctg_filehead,
                        io_filtered_file, output_path):
    print("Generating synthetic producers...")
# load inputs
    firms = read_csv(synthetic_firms_no_location_file, low_memory=False) # 8,396, 679 FIRMS
    mesozone_faf_lookup = read_csv(mesozone_to_faf_file)
    c_n6_n6io_sctg = read_csv(c_n6_n6io_sctg_file)
    # for_prod = read_csv(os.path.join(file_path, parameter_dir, foreign_prod_file))
    
    io = read_csv(BEA_io_2017_file)
    unitcost = read_csv(agg_unit_cost_file)
    
    producer_value_fraction_by_location = \
        read_csv(prod_by_zone_file)
    
    sctg_lookup = read_csv(sctg_group_file)
    
    # define constant
    # foreignprodcostfactor = 0.9     # producer cost factor for foreign producers (applied to unit costs) 
    # wholesalecostfactor = 1.2     # markup factor for wholesalers (applied to unit costs)
    
    # define output
    
    
    # define result directory
    # result_dir = os.path.join(file_path, output_dir)
    
    # <codecell>
    ########################################################
    #### step 2 - Creation of Producers database ###########
    ########################################################
    
    # All agents that produce some SCTG commodity become potential producers
    # wholesales are generated separately below
    
    producers = firms.loc[firms['Commodity_SCTG'] > 0] 
    producers = producers.loc[producers['Industry_NAICS6_Make'].str[:2] != '42']
    # 424,120 producers, exclude wholesale
    
    # convert I-O to long table
    io = pd.melt(io, id_vars=['make'], 
            var_name='Industry_NAICS6_Use', value_name='ProVal')
    
    io = io.reset_index()
    
    io = io.rename(columns = {'make': 'Industry_NAICS6_Make'})
    io = io.drop(columns = 'index')
    io.loc[:, 'Industry_NAICS6_Use'] = io.loc[:, 'Industry_NAICS6_Use'].str[1:7]
    
    io = io.loc[io['ProVal']>0]
    io.to_csv(io_summary_file, index = False) 
    
     
    
    # <codecell>
    ########################################################
    #### step 3 - Creation of wholesale producers ##########
    ########################################################
     
    # generate wholesale production
    to_wholesale = io.loc[io['Industry_NAICS6_Use'].str[:2] == "42"] # 1383 rows, 3 variables
    
    c_n6_n6io_sctg = c_n6_n6io_sctg.loc[c_n6_n6io_sctg['Commodity_SCTG'] > 0] # keep industries that produce commodity (not service)
    naics_by_sctg_fration = \
        c_n6_n6io_sctg.drop_duplicates(subset = ['Commodity_SCTG', 'Industry_NAICS6_Make', 'Proportion'], keep = 'first')
     
    # rescaling production fraction
    naics_by_sctg_fration.loc[:, 'Proportion'] = \
        naics_by_sctg_fration.loc[:, 'Proportion'] / \
            naics_by_sctg_fration.groupby(['Industry_NAICS6_Make'])['Proportion'].transform('sum') 
    
    
    # for selected wholesale industry, define the industry of commodity supplier
    
    to_wholesale_by_sctg = \
        pd.merge(to_wholesale, 
                 naics_by_sctg_fration, 
                 on = "Industry_NAICS6_Make", how = 'inner') # 954 * 5
    
    to_wholesale_by_sctg.loc[:, 'ProVal'] = \
        to_wholesale_by_sctg.loc[:, 'ProVal'] * to_wholesale_by_sctg.loc[:, 'Proportion']
    
    to_wholesale_by_sctg = \
        to_wholesale_by_sctg[['Industry_NAICS6_Make', 'Industry_NAICS6_Use', 'ProVal', 'Commodity_SCTG', 'Proportion']]
    
    
    # define wholesale industry as supplier
    
    from_wholesale = io.loc[io['Industry_NAICS6_Make'].str[:2] == "42"] # 859 rows
    from_wholesale = from_wholesale.rename(columns = {'ProVal': 'ProValFromWhl'})
    
    wholesale_flow = \
      pd.merge(to_wholesale_by_sctg, from_wholesale, 
            left_on = 'Industry_NAICS6_Use', 
            right_on = 'Industry_NAICS6_Make', how = 'outer') #size = 82508 * 8
     # <codecell>  
      
    whlprod =  from_wholesale.loc[:, 'ProValFromWhl'].sum() # this is the margin = sales - cost of good sold
    whlcons =  to_wholesale_by_sctg.loc[:, 'ProVal'].sum()
    
    wholesale_flow.loc[:, 'ProValPctUse'] = wholesale_flow.loc[:, 'ProVal']/ \
    wholesale_flow.groupby(['Industry_NAICS6_Make_y', 'Industry_NAICS6_Use_y'])['ProVal'].transform('sum')
    
    wholesalecostfactor = (whlcons + whlprod) / whlcons # assume zero inventory -> all commodity purchased will be sold
    print(wholesalecostfactor)
    wholesale_flow.loc[:, 'CellValue'] = wholesale_flow.loc[:, 'ProValPctUse'] * wholesale_flow.loc[:, 'ProValFromWhl'] 
    
    wholesale_flow.loc[:, 'CellValue'] *= whlcons/whlprod # scale value to cost of good
    wholesale_flow.loc[:, 'CellValue'] *= wholesalecostfactor # add margin
    wholesale_flow.loc[:, 'CellValue'] = np.round(wholesale_flow.loc[:, 'CellValue'], 0)
    wholesale_flow = wholesale_flow.loc[wholesale_flow['CellValue'] > 0]
    # print(wholesale_flow.loc[:, 'CellValue'].sum())
    
    wholesale_flow = \
        wholesale_flow[['Industry_NAICS6_Make_x', 'Industry_NAICS6_Use_y', 'Commodity_SCTG', 'Industry_NAICS6_Make_y', 'CellValue']]
    
    wholesale_flow.columns =['Industry_NAICS6_Make', 'Industry_NAICS6_Use', 'SCTG', 'NAICS_whl', 'ProValWhl']    
    
    # 21,336 rows
    
    
    iowhl = wholesale_flow.groupby(['Industry_NAICS6_Make', 'Industry_NAICS6_Use'])[['ProValWhl']].sum()
    iowhl = iowhl.reset_index()
    # print(iowhl.loc[:, 'ProValWhl'].sum())
    # 10533 row
    
     
     #remove wholesale records from io table
    io_no_wholesale = \
      io.loc[(io['Industry_NAICS6_Use'].str[:2] != "42") & (io['Industry_NAICS6_Make'].str[:2] != "42")]
    
    io_with_wholesale = pd.merge(io_no_wholesale, iowhl,
                               on = ["Industry_NAICS6_Make", "Industry_NAICS6_Use"],
                               how = 'outer')
    # some industry only transact with wholesaler, make sure those transactions are captured using outer join
    io_with_wholesale.fillna(0, inplace=True)
    io_with_wholesale.loc[:, 'ProVal_with_whl'] = \
        io_with_wholesale.loc[:, 'ProVal'] + io_with_wholesale.loc[:, 'ProValWhl'] / wholesalecostfactor
    
    
    from_wholesale = io_with_wholesale['Industry_NAICS6_Make'].str[:2] == "42"
    io_with_wholesale.loc[from_wholesale, 'ProVal_with_whl'] = \
        io_with_wholesale.loc[from_wholesale, 'ProVal'] + \
            io_with_wholesale.loc[from_wholesale, 'ProValWhl']
    
    
     # <codecell>    
    #add the wholesales with the correct capacities in value and tons
    #to both producer and consumer tables
    wholesalers = firms.loc[firms['Industry_NAICS6_Make'].str[:2] == "42",] 
    # 413,532 wholesaler simulated
    
    whlval = wholesale_flow.groupby(['Industry_NAICS6_Make', 'NAICS_whl', 'SCTG'])[['ProValWhl']].sum()
    whlval = whlval.reset_index()
    whlval.columns = ['InputCommodity', 'Industry_NAICS6_Make', 'Commodity_SCTG', 'ProVal']
    # 270 rows
    list_of_wholesaler_naics = whlval['Industry_NAICS6_Make'].unique()
    wholesalers = wholesalers.drop(columns = ['Commodity_SCTG'])
    wholesalers_with_sctg = None
    
    for whl_naics in list_of_wholesaler_naics:
        # print(whl_naics)
        con_firms = wholesalers.loc[wholesalers['Industry_NAICS6_Make'] == whl_naics]
        sample_size = len(con_firms)
        con_io = whlval.loc[whlval['Industry_NAICS6_Make'] == whl_naics]
        con_io.loc[:, 'probability'] = con_io.loc[:, 'ProVal'] / con_io.loc[:, 'ProVal'].sum()
        sample_naics_make = con_io.sample(n = sample_size, replace=True, weights = con_io['probability'],random_state=1)
        sample_naics_make = sample_naics_make.reset_index()
        con_firms = pd.concat([con_firms.reset_index(), sample_naics_make[['InputCommodity', 'Commodity_SCTG']]], axis = 1)
        wholesalers_with_sctg = pd.concat([wholesalers_with_sctg, con_firms])
    
    # 308235 wholesalers in the output that sell commodities
    
     # <codecell> 
     
    # assign production for each wholesaler
    wholesale_emp = \
        wholesalers_with_sctg.groupby(['Industry_NAICS6_Make', 'Commodity_SCTG', 'FAFZONE'])[['Emp']].sum()
    
    wholesale_emp = wholesale_emp.reset_index()
    
    whlval = wholesale_flow.groupby(['NAICS_whl', 'SCTG'])[['ProValWhl']].sum()
    whlval = whlval.reset_index()
    whlval.columns = ['Industry_NAICS6_Make', 'Commodity_SCTG', 'ProVal']
    
    whlval_with_loc = \
      pd.merge(wholesale_emp, whlval, 
      on = ["Industry_NAICS6_Make", 'Commodity_SCTG'], how = 'inner')
     
    
    producer_value_fraction_by_location = \
        producer_value_fraction_by_location.rename(columns = {'FAF':'FAFZONE'})
    whlval_with_loc = \
        pd.merge(whlval_with_loc,
                 producer_value_fraction_by_location[['Commodity_SCTG', 'FAFZONE', 'value_fraction']],
                 on = ["Commodity_SCTG", "FAFZONE"], how = 'inner') # 21,811
    
    whlval_with_loc.loc[:, 'value_fraction'] = \
        whlval_with_loc.loc[:, 'value_fraction'] / \
            whlval_with_loc.groupby(['Industry_NAICS6_Make', 'Commodity_SCTG'])['value_fraction'].transform('sum')
    
    whlval_with_loc.loc[:, 'ProVal'] = \
        whlval_with_loc.loc[:, 'ProVal'] * whlval_with_loc.loc[:, 'value_fraction'] 
    whlval_with_loc.loc[:, 'ValEmp'] = \
        whlval_with_loc.loc[:, 'ProVal'] / whlval_with_loc.loc[:, 'Emp'] 
    
    wholesalers_with_value = \
      pd.merge(wholesalers_with_sctg, 
               whlval_with_loc[['Industry_NAICS6_Make', 'ValEmp', 'Commodity_SCTG', 'FAFZONE']], 
               on = ['Industry_NAICS6_Make', "Commodity_SCTG", 'FAFZONE'],
               how = 'inner') #merge the value per employee back on to businesses
    
    wholesalers_with_value.loc[:, 'ProdVal'] = \
        wholesalers_with_value.loc[:, 'ValEmp']  * wholesalers_with_value.loc[:, 'Emp'] #calculate production value for each establishment, 
    
    # 305,935 wholesaler --> 
    # if a zone doesn't have observed wholesale shipment from CFS, the seller will be removed
    
    #======= this markes as the end of wholesaler generation ============#
    
    #issues - need to be marked as a wholesales using the NAICS code
    #but need to be tagged with the correct make/use commodity seperate from their NAICS code
    #this should be easy for the consumer side, check possible for the producers side
    
    
    # <codecell>
    ########################################################
    #### step 4 - Creation of non-wholesale producers ######
    ########################################################
    
    producer_emp = \
        producers.groupby(['Industry_NAICS6_Make', 'Commodity_SCTG', 'FAFZONE'])[['Emp']].sum()
    
    producer_emp = producer_emp.reset_index()    
    
    io_with_wholesale_agg = \
        io_with_wholesale.groupby(['Industry_NAICS6_Make'])[['ProVal_with_whl']].sum()
    io_with_wholesale_agg = io_with_wholesale_agg.reset_index()
    
    prodval = \
      pd.merge(producer_emp, io_with_wholesale_agg, 
               on = "Industry_NAICS6_Make", how = 'inner')
    # 27085 * 5
    
    prodval_with_loc = \
        pd.merge(prodval, 
                 producer_value_fraction_by_location, 
                 on = ["Commodity_SCTG", "FAFZONE"], how = 'inner') # 27,085
    
    prodval_with_loc = \
        prodval_with_loc.rename(columns = {'ProVal_with_whl': 'ProVal'})
    
    prodval_with_loc['value_fraction'].fillna(0, inplace = True)
    
    prodval_with_loc.loc[:, 'value_fraction'] = \
        prodval_with_loc.loc[:, 'value_fraction'] / \
            prodval_with_loc.groupby(['Industry_NAICS6_Make', 'Commodity_SCTG'])['value_fraction'].transform('sum')
    
    
    prodval_with_loc.loc[:, 'ProVal'] = \
        prodval_with_loc.loc[:, 'ProVal'] * prodval_with_loc.loc[:, 'value_fraction'] 
    
    prodval_with_loc.loc[:, 'ValEmp'] = \
        prodval_with_loc.loc[:, 'ProVal'] / prodval_with_loc.loc[:, 'Emp'] 
    
    
    producers_with_value = \
      pd.merge(producers, 
               prodval_with_loc[['Industry_NAICS6_Make', 'Commodity_SCTG', 'FAFZONE', 'ValEmp']], 
               on = ['Industry_NAICS6_Make', 'Commodity_SCTG', 'FAFZONE'],
               how = 'inner') # 388,808 producers
    
    producers_with_value.loc[:, 'ProdVal'] = \
        producers_with_value.loc[:, 'ValEmp'] * producers_with_value.loc[:, 'Emp']
    
    # total production = 12,759,317
    
    #sum(producers$ProdVal)
    # [1] 12,585,516
    
    # <codecell>
    ###################################################################
    #### step 5 - adding foreign producers (will drop in future) ######
    ###################################################################
    
    # io_total = io.groupby(['Industry_NAICS6_Make'])[['ProVal']].sum()
    # io_total = io_total.reset_index()
    
    # producer_emp = \
    #     producers.groupby(['Industry_NAICS6_Make'])[['Emp']].sum()
    # producer_emp = producer_emp.reset_index()
    
    # for_prod = for_prod.rename(columns = {'Commodity_NAICS6': 'Industry_NAICS6_CBP'})
    # for_prod = \
    #   pd.merge(for_prod, 
    #         c_n6_n6io_sctg[['Industry_NAICS6_CBP', 'Industry_NAICS6_Make', 'Commodity_SCTG']], 
    #         on = "Industry_NAICS6_CBP", how = 'left') #Merge in the I/O NAICS codes and SCTG codes, size = 27786 * 8
    # for_prod = for_prod.loc[for_prod['Commodity_SCTG'] > 0]
    
    
    
    # for_prod.loc[:, 'ProdVal'] = for_prod.loc[:, 'USImpVal']/ (10 ** 6) # convert to million dollars
    
    # for_prod_agg = \
    # for_prod.groupby(['Industry_NAICS6_Make', 'CBPZONE', 'FAFZONE', 'Commodity_SCTG'])[['ProdVal']].sum()
    # for_prod_agg = for_prod_agg.reset_index()
    
    # prodval = \
    #   pd.merge(producer_emp, io_total,
    #            on = "Industry_NAICS6_Make", how = 'left')
      
      
    # prodval.loc[:, 'ValEmp'] = prodval.loc[:, 'ProVal'] / prodval.loc[:, 'Emp'] 
    # #production value per employee (in Million of Dollars)
    
    # for_prod_agg = \
    #   pd.merge(for_prod_agg, prodval[['Industry_NAICS6_Make', 'ValEmp']], 
    #            on = "Industry_NAICS6_Make", how = 'left') #
      
    # for_prod_agg['ValEmp'].fillna(0, inplace = True)
    
    # for_prod_agg.loc[:, 'ValEmp'] = foreignprodcostfactor
    
    # for_prod_agg.loc[:, 'Emp'] = \
    # np.round(for_prod_agg.loc[:, 'ProdVal']/ for_prod_agg.loc[:, 'ValEmp'], 0)
    
    
    # emp_bins = [0, 19, 99, 499, 1000, 2499, 4999, for_prod_agg.loc[:, 'Emp'].max()]
    # emp_bin_label = [1, 2, 3, 4, 5, 6, 7]
    
    # for_prod_agg.loc[for_prod_agg['Emp']< 1, 'Emp'] = 1
    # for_prod_agg.loc[:, 'esizecat'] = pd.cut(for_prod_agg.loc[:, 'Emp'], 
    #                                          bins = emp_bins, 
    #                                          right=True, 
    #                                          labels=emp_bin_label)
    
    
    #producers' output in POUNDS (units costs converted to pounds)
    
    # for_prod_agg = pd.merge(for_prod_agg, unitcost, 
    #                         on = "Commodity_SCTG", how = 'left')
    
    
    # #update unit cost using foreign producer adjustment
    # for_prod_agg.loc[:, 'UnitCost'] *= foreignprodcostfactor
    # for_prod_agg.loc[:,  'ProdCap'] = \
    #     for_prod_agg.loc[:,'ProdVal'] * (10**6) / for_prod_agg.loc[:, 'UnitCost'] # ProdVal was in $M, ProdCap in pound
        
    
    # capacity_ub = 5 * 10 ** 8 # define upper bound capacity per firm in pound
    # for_prod_agg.loc[:, 'est'] = 1
    # for_prod_agg.loc[for_prod_agg['ProdCap'] > capacity_ub, 'est'] = \
    #     np.ceil(for_prod_agg.loc[for_prod_agg['ProdCap'] > capacity_ub, 'ProdCap']/capacity_ub)
    
    # for_prod_agg.loc[for_prod_agg['est']> 1, 'ProdVal'] = \
    #     for_prod_agg.loc[for_prod_agg['est']> 1, 'ProdVal'] / for_prod_agg.loc[for_prod_agg['est']> 1, 'est'] # update ProdVal for multiple firms
        
    # for_prod_agg.loc[for_prod_agg['est']> 1, 'ProdCap'] = \
    #     for_prod_agg.loc[for_prod_agg['est']> 1, 'ProdCap'] / for_prod_agg.loc[for_prod_agg['est']> 1, 'est'] # update ProdCap for multiple firms
    
    # <codecell>
    
    # for_prod_rep = pd.DataFrame(np.repeat(for_prod_agg.values, for_prod_agg.est, axis=0))
    # for_prod_rep.columns = for_prod_agg.columns
    # # 24,554 foreign firms
    # for_prod_rep = for_prod_rep.drop(columns = ['est'])
    
    # for_prod_rep.loc[:, 'MESOZONE'] = for_prod_rep.loc[:, 'CBPZONE'] + 30000
    # max_id = int(firms.loc[:, 'BusID'].max())
    # for_prod_rep.loc[:, 'BusID'] = max_id + for_prod_rep.reset_index().index + 1
    # size = 24,554 * 13
    
    
    #################################################
    #### step 6 - finalize producer generation ######
    #################################################
    unitcost.loc[:, 'UnitCost'] /=  2000
    producers_with_value = pd.merge(producers_with_value, unitcost, 
                              on = "Commodity_SCTG", how = 'left')
    
    
    producers_with_value.loc[:, 'ProdCap'] = \
        producers_with_value.loc[:, 'ProdVal'] * (10**6) / \
            producers_with_value.loc[:, 'UnitCost'] 
    # ProdVal was in $M
    
    wholesalers_with_value = pd.merge(wholesalers_with_value, 
                                      unitcost, 
                                      on = "Commodity_SCTG",
                                      how = 'left')
    # factor up unitcost to reflect wholesalers margin
    wholesalers_with_value.loc[:, 'ProdCap'] = \
        wholesalers_with_value.loc[:, 'ProdVal'] * (10**6) / \
            wholesalers_with_value.loc[:, 'UnitCost'] 
    # ProdVal was in $M
    
    # <codecell>
    
    output_var = ["SellerID", "Zone", "NAICS", "SCTG", "Size", "OutputCapacitylb", "NonTransportUnitCost"]
    
    # combine producers
    producers_output = \
        producers_with_value[["BusID", "MESOZONE", "Industry_NAICS6_Make", "Commodity_SCTG", "Emp", "ProdCap", "UnitCost"]]
    # for_prod_output = \
    #     for_prod_rep[["BusID", "MESOZONE", "Industry_NAICS6_Make", "Commodity_SCTG", "Emp", "ProdCap", "UnitCost"]]
    
    # producers_output = pd.concat([producers_output, for_prod_output])
    # producers_output.column = output_var
    
    # size = 445,266 * 7
    
    # Add in wholesalers to producers
    wholesalers_output = \
        wholesalers_with_value[["BusID", "MESOZONE", "Industry_NAICS6_Make", 'Commodity_SCTG', "Emp", "ProdCap", "UnitCost"]]
    # size = 308,235 * 7
    
    # wholesalers_output.columns = output_var
    producers_output = pd.concat([producers_output, wholesalers_output], axis = 0)
    # size = 753,501 firms
    renaming_dict = {"BusID": "SellerID",
                     "MESOZONE": "Zone", 
                     "Industry_NAICS6_Make": "NAICS",
                     # "Commodity_SCTG": "SCTG",
                     "Emp": "Size",
                     "ProdCap": "OutputCapacitylb",
                     "UnitCost": "NonTransportUnitCost"}
    producers_output = producers_output.rename(columns = renaming_dict)
    wholesalers_with_value = wholesalers_with_value.rename(columns = renaming_dict)
    # writing output
    print('Total number of producers:')
    print(len(producers_output))
    
    print('Total number of wholesalers (among producers):')
    print(len(wholesalers_output))
    
    wholesalers_with_value.to_csv(wholesaler_file, index = False)
    producers_output.to_csv(producer_file, index = False)
    io_with_wholesale.to_csv(io_filtered_file, index = False)
    
    sctg_lookup_sel = sctg_lookup[['SCTG_Code', 'SCTG_Group', 'SCTG_Name']] 
    sctg_lookup_sel = sctg_lookup_sel.rename(columns = {'SCTG_Code': 'Commodity_SCTG'})
    producers_output = pd.merge(producers_output, 
                                sctg_lookup_sel, 
                                on = "Commodity_SCTG", 
                                how = 'left')
    # <codecell>
    for i in range(5):
      print("Processing SCTG Group " + str(i+1))
      
      g1_prods = producers_output.loc[producers_output['SCTG_Group'] == i+1] 
      g1_prods = g1_prods[['SCTG_Group', 'Commodity_SCTG', 'SellerID', 'Zone', 'NAICS', 'Size', 'OutputCapacitylb', 'NonTransportUnitCost']]
      g1_prods.to_csv(producer_by_sctg_filehead + str(i+1) + ".csv", index = False)
     
    
    print('Producer generation is done!')