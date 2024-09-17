#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 12 10:34:47 2024

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
# c_n6_n6io_sctg_file = 'corresp_naics6_n6io_sctg_revised.csv'
# synthetic_firms_no_location_file = "synthetic_firms.csv" 
# mesozone_to_faf_file = "zonal_id_lookup_final.csv" # zonal ID lookup table 
# agg_unit_cost_file = "data_unitcost_calib.csv" # unit cost by commodity from CFS 2017 (all zones combined)
# SCTG_group_file = "SCTG_Groups_revised.csv" # Commodity type to group lookup (pre-defined)
# synthetic_wholesaler_file = "synthetic_wholesaler.csv" # synthetic wholesaler (serve as both buyer and supplier)
# synthetic_producer_file = "synthetic_producers.csv" # synthetic producer
# io_filtered_file = "data_2017io_filtered.csv" # processed I-O table, after dropping wholesale transaction

# # specifications that are new to this code
# # foreign_cons_file = "data_foreign_cons.csv" # total foreign consumption (pre-defined from prior studies)
# cons_by_zone_file = "consumer_value_fraction_by_faf.csv" # Total consumption value  by FAF zone from CFS 2017

# # define output
# consumer_file = "synthetic_consumers.csv" # synthetic consumer
# sample_consumer_file = "sample_synthetic_consumers.csv" # sample synthetic consumer (for results checking)
# consumer_by_sctg_filehead = "consumers_sctg" 

def consumer_generation(synthetic_firms_no_location_file, mesozone_to_faf_file,
                        c_n6_n6io_sctg_file, agg_unit_cost_file, cons_by_zone_file,
                        sctg_group_file, wholesaler_file,
                        producer_file, io_filtered_file, consumer_file,
                        sample_consumer_file, consumer_by_sctg_filehead, output_path):
    print("Generating synthetic consumers...")
    # load inputs
    firms = read_csv(synthetic_firms_no_location_file, low_memory=False) # 8,396, 679 FIRMS
    mesozone_faf_lookup = read_csv(mesozone_to_faf_file)
    c_n6_n6io_sctg = read_csv(c_n6_n6io_sctg_file)
    unitcost = read_csv(agg_unit_cost_file)
    
    # for_cons = read_csv(os.path.join(file_path, parameter_dir, foreign_cons_file))
    consumer_value_fraction_by_location = \
        read_csv(cons_by_zone_file)
    sctg_lookup = read_csv(sctg_group_file)
    
    wholesalers = read_csv(wholesaler_file, low_memory=False) 
    producers = read_csv(producer_file, low_memory=False) 
    io = read_csv(io_filtered_file, low_memory=False) 
    
    # define constant
    wholesalecostfactor = 1.376 # in the future, replace this value with output from step 2
    
    
    # <codecell>
    
    ########################################################
    #### step 2 - Creation of consumer database ############
    ########################################################
    
    list_of_supply_industry = \
        producers.loc[producers['NAICS'].str[:2] != "42", 'NAICS'].unique()
    io_filered = io.loc[io['Industry_NAICS6_Make'].isin(list_of_supply_industry)] #focus on just producers of transported commodities
    io_filered = io_filered.loc[io_filered['Industry_NAICS6_Use'].str[:2] != "42"]
    list_of_consumer_naics = io_filered.loc[:, 'Industry_NAICS6_Use'].unique()
    
    consumers = None 
    # exclude wholesale consumer --> their input industry and commodity has been regenerated
    for con_naics in list_of_consumer_naics:
        # print(con_naics)
        con_firms = firms.loc[firms['Industry_NAICS6_Make'] == con_naics]
        con_firms = con_firms.rename(columns = {'Industry_NAICS6_Make': 'Industry_NAICS6_Use'})
        sample_size = len(con_firms)
        con_io = io_filered.loc[io_filered['Industry_NAICS6_Use'] == con_naics]
        con_io.loc[:, 'probability'] = con_io.loc[:, 'ProVal_with_whl'] / con_io['ProVal_with_whl'].sum()
        sample_naics_make = con_io.sample(n = sample_size, replace=True, weights = con_io['probability'],random_state=1)
        sample_naics_make = sample_naics_make.reset_index()
        con_firms = pd.concat([con_firms.reset_index(), sample_naics_make[['Industry_NAICS6_Make']]], axis = 1)
        consumers = pd.concat([consumers, con_firms])
        # randomly assign producer industry, so each consumer only purchases from one selected industry
    
    
    consumers = consumers.rename(columns = {'Commodity_SCTG': 'Buyer.SCTG'})
    #7,669,815 consumers
    
    # <codecell>
    
    # format NAICS industry-commodity crosswalk
    # c_n6_n6io_sctg = c_n6_n6io_sctg.loc[c_n6_n6io_sctg['Commodity_SCTG'] > 0] # keep industries that produce commodity (not service)
    naics_by_sctg_fration = \
        c_n6_n6io_sctg.drop_duplicates(subset = ['Commodity_SCTG', 'Industry_NAICS6_Make', 'Proportion'], keep = 'first')
    
    naics_by_sctg_fration = naics_by_sctg_fration.loc[naics_by_sctg_fration['Commodity_SCTG'] > 0 ]    
    # rescaling production fraction
    naics_by_sctg_fration.loc[:, 'Proportion'] = \
        naics_by_sctg_fration.loc[:, 'Proportion'] / \
            naics_by_sctg_fration.groupby(['Industry_NAICS6_Make'])['Proportion'].transform('sum')
            
    
    emp = consumers.groupby(['Industry_NAICS6_Use', 'Industry_NAICS6_Make', 'FAFZONE'])[['Emp']].sum() 
    emp = emp.reset_index()
    emp_with_io = pd.merge(emp, io_filered, 
                           on = ["Industry_NAICS6_Use", "Industry_NAICS6_Make"],
                           how = 'inner')
    emp_with_io = pd.merge(emp_with_io, naics_by_sctg_fration, 
                           on = "Industry_NAICS6_Make", how = 'left')
    
    emp_with_io.loc[:, 'ProVal'] = \
        emp_with_io.loc[:, 'ProVal_with_whl'] * emp_with_io.loc[:, 'Proportion']
    
    consumer_value_fraction_by_location = \
        consumer_value_fraction_by_location.rename(columns = {'FAF': 'FAFZONE'})
    
    io_with_loc = pd.merge(emp_with_io,
                           consumer_value_fraction_by_location[['FAFZONE', 'Commodity_SCTG', 'value_fraction']], 
                           on = ["Commodity_SCTG", 'FAFZONE'], how = 'inner')
    
    #generate consumption value per employee
    io_with_loc.loc[:, 'value_fraction_scaled'] = \
        io_with_loc.loc[:, 'value_fraction'] / \
            io_with_loc.groupby(["Industry_NAICS6_Use", 'Industry_NAICS6_Make', 'Commodity_SCTG'])['value_fraction'].transform('sum') 
    
    io_with_loc.loc[:, 'ProVal'] = io_with_loc.loc[:, 'ProVal'] * io_with_loc.loc[:, 'value_fraction_scaled']
    io_with_loc.loc[:, 'ValEmp'] = io_with_loc.loc[:, 'ProVal'] / io_with_loc.loc[:, 'Emp']
    #production value per employee (in Million of Dollars)
    # 599,163 rows
    
    # <codecell>
    
    #Merge suppliers with I-O table list to create a consumers dataset
    io_with_loc = \
        io_with_loc[['Industry_NAICS6_Use', 'Industry_NAICS6_Make', 'Commodity_SCTG', \
                     'FAFZONE', 'ValEmp']]
        
    consumers = consumers[['MESOZONE', 'Industry_NAICS6_Use', 'Industry_NAICS6_Make', \
                           'Buyer.SCTG', 'FAFZONE', 'BusID', 'Emp']]
        
    consumers = \
      pd.merge(io_with_loc, 
            consumers,
            on = ['Industry_NAICS6_Use', 'Industry_NAICS6_Make', 'FAFZONE'], how = 'left') 
      
    consumers.loc[:, 'ConVal'] = consumers.loc[:, 'ValEmp'] * consumers.loc[:, 'Emp']
    
    #8,627,725 consumers * SCTG
    
    # <codecell>
    # Calculate the purchase amount and convert to tons needed - this is production value
    
    # Convert purchase value from $M to POUNDS
    unitcost.loc[:, 'UnitCost'] /=  2000
    consumers = pd.merge(consumers, unitcost,
                         on = "Commodity_SCTG", how = 'left')
    
    consumers.loc[:,  'PurchaseAmountlb'] = \
        consumers.loc[:,'ConVal'] * (10**6) / consumers.loc[:, 'UnitCost'] 
        
    
    consumers = consumers.drop(columns = ["ValEmp", "UnitCost"]) # Remove extra fields
    
    # <codecell>
    ########################################################
    #### step 3 - Generate foreign consumer ################
    ########################################################
    
    # for_cons= for_cons.rename(columns = {"Commodity_NAICS6": "Industry_NAICS6_CBP"})
    # for_cons = pd.merge(for_cons, 
    #                     c_n6_n6io_sctg[['Industry_NAICS6_CBP', 'Industry_NAICS6_Make']].drop_duplicates(keep = 'first'), 
    #                     on = "Industry_NAICS6_CBP", how = 'left') #Merge in the I/O NAICS codes and SCTG codes
    
    
    # for_cons = for_cons.rename(columns = {'Industry_NAICS6_Make': 'Industry_NAICS6_Use'})
    
    
    # for_cons = for_cons.dropna()
    # list_of_for_consumer_naics = for_cons['Industry_NAICS6_Use'].unique()
    
    # # assign random producer for each consumer
    # for_consumers = None
    # for con_naics in list_of_for_consumer_naics:
    #   # print(con_naics)
    #     con_firms = for_cons.loc[for_cons['Industry_NAICS6_Use'] == con_naics]
    #     con_firms = con_firms.rename(columns = {'Industry_NAICS6_Make': 'Industry_NAICS6_Use'})
    #     sample_size = len(con_firms)
    #     con_io = io.loc[io['Industry_NAICS6_Use'] == con_naics]
    #     con_io.loc[:, 'probability'] = con_io.loc[:, 'ProVal_with_whl'] / con_io['ProVal_with_whl'].sum()
    #     sample_naics_make = con_io.sample(n = sample_size, replace=True, weights = con_io['probability'],random_state=1)
    #     sample_naics_make = sample_naics_make.reset_index()
    #     con_firms = pd.concat([con_firms.reset_index(), sample_naics_make[['Industry_NAICS6_Make']]], axis = 1)
    #     for_consumers = pd.concat([for_consumers, con_firms])
    #   # break
    
    
    # for_consumers = pd.merge(for_consumers, 
    #                          c_n6_n6io_sctg[['Industry_NAICS6_Make', 'Commodity_SCTG', 'Proportion']].drop_duplicates(keep = 'first'), 
    #                          on = "Industry_NAICS6_Make", how = 'left') #Merge in the I/O NAICS codes and SCTG codes
    
    # for_consumers.loc[:, 'USExpVal'] = \
    #     for_consumers.loc[:, 'USExpVal'] * for_consumers.loc[:, 'Proportion']
        
    # # <codecell>
    
    # for_consumers = for_consumers.loc[for_consumers['Commodity_SCTG'] > 0]
    
    # group_var = ['Industry_NAICS6_Make', 'Industry_NAICS6_Use', 'CBPZONE', 'FAFZONE', 'Commodity_SCTG']
    # for_consumption_value = for_consumers.groupby(group_var)[['USExpVal']].sum()
    # for_consumption_value.loc[:, 'ConVal'] = for_consumption_value.loc[:, 'USExpVal'] / (10 ** 6)
    
    # for_consumption_value = for_consumption_value.reset_index()
    # # million dollars
    
    # for_consumption_value = pd.merge(for_consumption_value, unitcost, 
    #                         on = "Commodity_SCTG", how = 'left')
    
    # #update unit cost using foreign producer adjustment
    # for_consumption_value.loc[:,  'PurchaseAmountlb'] = \
    #     for_consumption_value.loc[:,'ConVal'] * (10**6) / \
    #         for_consumption_value.loc[:, 'UnitCost'] # ProdVal was in $M, ProdCap in pound
            
        
    # capacity_ub = 5 * 10 ** 8 # define upper bound capacity per firm in pound
    # for_consumption_value.loc[:, 'est'] = 1
    # cap_filter = (for_consumption_value['PurchaseAmountlb'] > capacity_ub)
    # for_consumption_value.loc[cap_filter, 'est'] = \
    #     np.ceil(for_consumption_value.loc[cap_filter, 'PurchaseAmountlb']/capacity_ub)
    
    # dup_filter = (for_consumption_value['est']> 1)
    
    # for_consumption_value.loc[dup_filter, 'ConVal'] = \
    #     for_consumption_value.loc[dup_filter, 'ConVal'] / \
    #         for_consumption_value.loc[dup_filter, 'est'] # update ProdVal for multiple firms
        
    # for_consumption_value.loc[dup_filter, 'PurchaseAmountlb'] = \
    #     for_consumption_value.loc[dup_filter, 'PurchaseAmountlb'] / \
    #         for_consumption_value.loc[dup_filter, 'est'] # update ProdCap for multiple firms
    
     
    # for_cons_rep = pd.DataFrame(np.repeat(for_consumption_value.values, 
    #                                       for_consumption_value.est, axis=0))
    # for_cons_rep.columns = for_consumption_value.columns
    # # 24,554 foreign firms
    # for_cons_rep = for_cons_rep.drop(columns = ['est'])
    
    # for_cons_rep.loc[:, 'MESOZONE'] = for_cons_rep.loc[:, 'CBPZONE'] + 30000
    # max_id = int(producers.loc[:, 'SellerID'].max())
    # for_cons_rep.loc[:, 'BusID'] = max_id + for_cons_rep.reset_index().index + 1
    # for_cons_rep.loc[:, 'Buyer.SCTG'] = 0  #don't know buyer industry
    # for_cons_rep.loc[:, 'Emp'] = 0 #don't know employment of buying firm
    # for_cons_rep = for_cons_rep.drop(columns = ["CBPZONE", "FAFZONE", 'USExpVal', "UnitCost"])
    #42,675 foreign consumers
    
    # <codecell>
    ########################################################
    #### step 4 - generate consumer output #################
    ########################################################
    consumers = consumers.drop(columns = ['FAFZONE'])
    # consumers_out = pd.concat([consumers, for_cons_rep]) #8,171,491
    
    rename_dict = {"BusID": "BuyerID",
        "MESOZONE": "Zone",
        "Industry_NAICS6_Make": "InputCommodity",
        "Industry_NAICS6_Use": "NAICS",
        "Emp": "Size"}
    
    consumers = consumers.rename(columns = rename_dict)
    
    # 8,674,432 consumers
    
    
    # process wholesale consumer
    wholesalers.loc[:, 'ConVal'] = \
    wholesalers.loc[:, 'OutputCapacitylb'] * wholesalers.loc[:, 'NonTransportUnitCost'] / wholesalecostfactor / (10 ** 6)
    #to million $
    
    wholesalers = wholesalers.drop(columns = ['NonTransportUnitCost'])
    
    wholesalers = wholesalers.rename(columns = {'SellerID': 'BuyerID',
                                                'OutputCapacitylb': 'PurchaseAmountlb'})
    
    wholesalers.loc[:, 'Buyer.SCTG'] = wholesalers.loc[:, 'Commodity_SCTG']
    # wholesalers.loc[:, 'Commodity_SCTG'] = wholesalers.loc[:, 'SCTG']
    
    wholesalers_out = wholesalers[['NAICS', 'InputCommodity', 'Commodity_SCTG', 
                                   'Zone', 'Buyer.SCTG',
                                   'BuyerID', 'Size', 'ConVal', 'PurchaseAmountlb']]
     #size = 308,235 * 9
    
    
    consumers_out = pd.concat([consumers, wholesalers_out])
    sample_consumers = consumers_out.loc[consumers_out['BuyerID'] <= 100]
    
    print('Total number of consumers is:')
    print(len(consumers_out))
    # 8,968,556
    
    consumers_out.to_csv(os.path.join(output_path, consumer_file), index = False)
    sample_consumers.to_csv(os.path.join(output_path, sample_consumer_file), index = False)
    # <codecell>
    
    sctg_lookup_sel = sctg_lookup[['SCTG_Code', 'SCTG_Group', 'SCTG_Name']] 
    sctg_lookup_sel = sctg_lookup_sel.rename(columns = {'SCTG_Code': 'Commodity_SCTG'})
    consumers_out = pd.merge(consumers_out,
                             sctg_lookup_sel, 
                             on = "Commodity_SCTG",
                             how = 'left')
    # <codecell>
    
    
    for i in range(5):
      print("Processing SCTG Group " + str(i+1))
      
      g1_cons = consumers_out.loc[consumers_out['SCTG_Group'] == i+1] 
      g1_cons = g1_cons[['SCTG_Group', 'Commodity_SCTG', 'BuyerID', 'Zone', 'NAICS', 'InputCommodity', 'PurchaseAmountlb']]
      g1_cons.to_csv(os.path.join(output_path, consumer_by_sctg_filehead + str(i+1) + ".csv"), index = False)
     
    print('Consumer generation is done!')