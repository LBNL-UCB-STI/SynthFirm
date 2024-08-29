#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 17 10:11:06 2024

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

print("Start international B2B flow assignment...")

########################################################
#### step 1 - configure environment and load inputs ####
########################################################

# load model config temporarily here
scenario_name = 'BayArea'
out_scenario_name = 'BayArea'
file_path = '/Users/xiaodanxu/Documents/SynthFirm.nosync'
parameter_dir = 'SynthFirm_parameters'
input_dir = 'inputs_' + scenario_name
output_dir = 'outputs_' + out_scenario_name

import_output_file = 'import_OD_with_mode.csv'
export_output_file = 'export_OD_with_mode.csv'

int_out_dir = os.path.join(file_path, output_dir, 'international')
import_output_with_mode = read_csv(os.path.join(int_out_dir, import_output_file))
export_output_with_mode = read_csv(os.path.join(int_out_dir, export_output_file))

producer_file = 'synthetic_producers.csv'
consumer_file = 'synthetic_consumers.csv'
mesozone_to_faf_file = "zonal_id_lookup_final.csv" # zonal ID lookup table 
SCTG_group_file = "SCTG_Groups_revised.csv" # Commodity type to group lookup (pre-defined)

domestic_consumer = read_csv(os.path.join(file_path, output_dir, consumer_file))
domestic_producer = read_csv(os.path.join(file_path, output_dir, producer_file))
mesozone_to_faf_lookup = read_csv(os.path.join(file_path, input_dir, mesozone_to_faf_file))
sctg_lookup = read_csv(os.path.join(file_path, parameter_dir, SCTG_group_file))

payload_capacity = {'Class 4-6 Vocational': 4,
                   'Class 7&8 Tractor': 18,
                   'Class 7&8 Vocational': 18}

# define output file
export_with_firm_file = 'export_OD_with_seller.csv'
import_with_firm_file = 'import_OD_with_buyer.csv'

# assign SCTG to producer/seller
domestic_producer = pd.merge(domestic_producer, sctg_lookup,
                             left_on = 'Commodity_SCTG',
                             right_on = 'SCTG_Code', how = 'left')
# assign SCTG to consumer/buyer
domestic_consumer = pd.merge(domestic_consumer, sctg_lookup,
                             left_on = 'Commodity_SCTG',
                             right_on = 'SCTG_Code', how = 'left')
# <codecell>

########################################################
#### step 2 - export B2B flow from producers ###########
########################################################
veh_type_to_assign = 'Class 7&8 Tractor'
mode_to_select = 'For-hire Truck'
payload_frac_thres = 1
capacity_to_use = payload_capacity[veh_type_to_assign]

export_output_truck_only = \
    export_output_with_mode.loc[export_output_with_mode['mode_choice'] == mode_to_select]

export_output_truck_only.loc[:, 'total_weight'] *= 1000 # convert back to ton
export_output_truck_only.loc[:, 'load_frac'] = \
    export_output_truck_only.loc[:, 'total_weight'] / capacity_to_use
export_output_truck_only.loc[:, 'low_load'] = 0
export_output_truck_only.loc[export_output_truck_only['load_frac'] <= payload_frac_thres, 'low_load'] = 1

print(export_output_truck_only.total_weight.min())
print(export_output_truck_only.total_weight.max())
print(export_output_truck_only.groupby('low_load')[['shipments']].sum())

export_output_truck_only.loc[:, 'sample_size'] = \
    np.round(export_output_truck_only.loc[:, 'load_frac'], 0)
export_output_truck_only.loc[export_output_truck_only['load_frac']<= payload_frac_thres, 'sample_size'] = 1

truckload_criteria = (export_output_truck_only['shipments'] < export_output_truck_only['sample_size'])
export_output_truck_only.loc[truckload_criteria, 'sample_size'] = \
    export_output_truck_only.loc[truckload_criteria, 'shipments']

export_output_truck_only.loc[:, 'TruckLoad'] = \
export_output_truck_only.loc[:, 'total_weight'] /export_output_truck_only.loc[:, 'sample_size']

export_output_truck_only.loc[:, 'shipments'] = \
export_output_truck_only.loc[:, 'shipments'] /export_output_truck_only.loc[:, 'sample_size']

export_truck_shipments = pd.DataFrame(np.repeat(export_output_truck_only.values, 
                                            export_output_truck_only.sample_size, axis=0))
export_truck_shipments.columns = export_output_truck_only.columns

# convert shipment to integer
export_truck_shipments.loc[:, 'total_weight'] = \
    export_truck_shipments.loc[:, 'TruckLoad'] * export_truck_shipments.loc[:, 'shipments']
export_truck_shipments.loc[:, 'shipments'] = np.round(export_truck_shipments.loc[:, 'shipments'].astype(float),0)
export_truck_shipments.loc[:, 'TruckLoad'] = \
export_truck_shipments.loc[:, 'total_weight'] /export_truck_shipments.loc[:, 'shipments']

export_truck_shipments.drop(columns = ['Unnamed: 0', 'value_2017', 'value_density', 
                                   'bundle_id', 'total_weight',
                                   'load_frac', 'low_load', 'sample_size'],
                            inplace = True)



# <codecell>
export_truck_shipments["bundle_id"] = export_truck_shipments.index + 1 
# assign origin firms (producers)
mesozone_to_faf_sel = mesozone_to_faf_lookup[['MESOZONE', 'FAFID']]

domestic_producer_to_match = domestic_producer[['SellerID', 'Zone', 'NAICS', 
                                                'SCTG_Code', 'SCTG_Group', 'Size']]
domestic_producer_to_match = pd.merge(domestic_producer_to_match,
                                      mesozone_to_faf_sel,
                                      left_on = 'Zone', right_on = 'MESOZONE', how = 'left')
domestic_producer_to_match.drop(columns = ['MESOZONE'], inplace = True)
domestic_producer_to_match.columns = ['SellerID', 'SellerZone', 'SellerNAICS',
                                      'SCTG_Code', 'SCTG_Group', 'Size', 'dms_orig']

def split_dataframe(df, chunk_size = 10000): 
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    return chunks

# generate random destination choice sets
chunksize = 10000
# trip_generation = trip_generation.loc[trip_generation['TripGeneration'] > 0]
chunks_of_shipments = split_dataframe(export_truck_shipments, chunksize)
export_truck_shipment_assigned = None
export_truck_shipment_failed = None
i = 0

existing_attr = export_truck_shipments.columns.tolist()

for chunk in chunks_of_shipments:
    print('processing batch ' + str(i))
    producer_to_match = domestic_producer_to_match.sample(frac = 0.2) # reduce size for sampling
    chunk_export = pd.merge(chunk, producer_to_match,
                            on = ['dms_orig', 'SCTG_Code', 'SCTG_Group'], how = 'left')
    failed_shipment = \
    chunk_export.loc[chunk_export['SellerZone'].isna()]
    failed_shipment = failed_shipment[existing_attr]
    chunk_export = chunk_export.dropna()
    # chunk_attraction.loc[:, 'importance'] = 1 /((chunk_attraction.loc[:, 'distance'] + 2) ** power_coeff)
    # chunk_attraction.loc[chunk_attraction['importance'] < 0.0001, 'importance'] = 0.0001
    chunk_export = chunk_export.groupby(existing_attr).sample(n = 1, 
                                                             weights = chunk_export['Size'],
                                                             replace = True, 
                                                             random_state = 1)
    export_truck_shipment_assigned = pd.concat([export_truck_shipment_assigned,
                                                chunk_export])
    export_truck_shipment_failed = pd.concat([export_truck_shipment_failed,
                                              failed_shipment])
    i += 1
    # break
# <codecell>
# assign failed ones (drop SCTG)
producer_to_match = domestic_producer_to_match.sample(frac = 0.5) # reduce size for sampling
print(len(export_truck_shipment_failed))
producer_to_match.drop(columns = ['SCTG_Code'], inplace = True) # ignore SCTG from producers
export_truck_shipment_reassign = pd.merge(export_truck_shipment_failed, 
                                        producer_to_match,
                                        on = ['dms_orig', 'SCTG_Group'], 
                                        how = 'left')
failed_shipment = \
export_truck_shipment_reassign.loc[export_truck_shipment_reassign['SellerZone'].isna()]
failed_shipment = failed_shipment[existing_attr]
export_truck_shipment_reassign.dropna(inplace = True)
export_truck_shipment_reassign = \
    export_truck_shipment_reassign.groupby(existing_attr).sample(n = 1, 
                                                             weights = export_truck_shipment_reassign['Size'],
                                                             replace = True, 
                                                             random_state = 1)
print(len(export_truck_shipment_reassign))
export_truck_shipment_assigned = pd.concat([export_truck_shipment_assigned,
                                            export_truck_shipment_reassign])

# final imputation -- drop all SCTG
producer_to_match = domestic_producer_to_match.sample(frac = 0.1) # reduce size for sampling
print(len(failed_shipment))
producer_to_match.drop(columns = ['SCTG_Code', 'SCTG_Group'], inplace = True) # ignore SCTG from producers
export_truck_shipment_reassign = pd.merge(failed_shipment, 
                                        producer_to_match,
                                        on = ['dms_orig'], 
                                        how = 'left')
failed_shipment = \
export_truck_shipment_reassign.loc[export_truck_shipment_reassign['SellerZone'].isna()]
failed_shipment = failed_shipment[existing_attr]
export_truck_shipment_reassign.dropna(inplace = True)
export_truck_shipment_reassign = \
    export_truck_shipment_reassign.groupby(existing_attr).sample(n = 1, 
                                                             weights = export_truck_shipment_reassign['Size'],
                                                             replace = True, 
                                                             random_state = 1)
print(len(export_truck_shipment_reassign))

export_truck_shipment_assigned = pd.concat([export_truck_shipment_assigned,
                                            export_truck_shipment_reassign])


# <codecell>
########################################################
#### step 3 - import B2B flow to consumers #############
########################################################

import_output_truck_only = \
    import_output_with_mode.loc[import_output_with_mode['mode_choice'] == mode_to_select]

import_output_truck_only.loc[:, 'total_weight'] *= 1000 # convert back to ton
import_output_truck_only.loc[:, 'load_frac'] = \
    import_output_truck_only.loc[:, 'total_weight'] / capacity_to_use
import_output_truck_only.loc[:, 'low_load'] = 0
import_output_truck_only.loc[import_output_truck_only['load_frac'] <= payload_frac_thres, 'low_load'] = 1

print(import_output_truck_only.total_weight.min())
print(import_output_truck_only.total_weight.max())
print(import_output_truck_only.groupby('low_load')[['shipments']].sum())

import_output_truck_only.loc[:, 'sample_size'] = \
    np.round(import_output_truck_only.loc[:, 'load_frac'], 0)
import_output_truck_only.loc[import_output_truck_only['load_frac']<= payload_frac_thres, 'sample_size'] = 1
truckload_criteria = (import_output_truck_only['shipments'] < import_output_truck_only['sample_size'])

import_output_truck_only.loc[truckload_criteria, 'sample_size'] = \
    import_output_truck_only.loc[truckload_criteria, 'shipments']

import_output_truck_only.loc[:, 'TruckLoad'] = \
import_output_truck_only.loc[:, 'total_weight'] /import_output_truck_only.loc[:, 'sample_size']

import_output_truck_only.loc[:, 'shipments'] = \
import_output_truck_only.loc[:, 'shipments'] /import_output_truck_only.loc[:, 'sample_size']

import_truck_shipments = pd.DataFrame(np.repeat(import_output_truck_only.values, 
                                            import_output_truck_only.sample_size, axis=0))

import_truck_shipments.columns = import_output_truck_only.columns

# convert shipment to integer
import_truck_shipments.loc[:, 'total_weight'] = \
    import_truck_shipments.loc[:, 'TruckLoad'] * import_truck_shipments.loc[:, 'shipments']
import_truck_shipments.loc[:, 'shipments'] = \
    np.round(import_truck_shipments.loc[:, 'shipments'].astype(float),0)
import_truck_shipments.loc[:, 'TruckLoad'] = \
import_truck_shipments.loc[:, 'total_weight'] /import_truck_shipments.loc[:, 'shipments']

import_truck_shipments.drop(columns = ['Unnamed: 0', 'value_2017', 'value_density', 
                                    'bundle_id', 'total_weight',
                                    'load_frac', 'low_load', 'sample_size'],
                            inplace = True)


# <codecell>
import_truck_shipments["bundle_id"] = import_truck_shipments.index + 1 
# assign origin firms (producers)
mesozone_to_faf_sel = mesozone_to_faf_lookup[['MESOZONE', 'FAFID']]

domestic_consumer_to_match = domestic_consumer[['BuyerID', 'Zone', 'NAICS', 
                                                'SCTG_Code', 'SCTG_Group', 'Size']]
domestic_consumer_to_match = pd.merge(domestic_consumer_to_match,
                                      mesozone_to_faf_sel,
                                      left_on = 'Zone', right_on = 'MESOZONE', how = 'left')
domestic_consumer_to_match.drop(columns = ['MESOZONE'], inplace = True)
domestic_consumer_to_match.columns = ['BuyerID', 'BuyerZone', 'BuyerNAICS',
                                      'SCTG_Code', 'SCTG_Group', 'Size', 'dms_dest']

chunks_of_shipments = split_dataframe(import_truck_shipments, chunksize)
import_truck_shipment_assigned = None
import_truck_shipment_failed = None
i = 0

existing_attr = import_truck_shipments.columns.tolist()

for chunk in chunks_of_shipments:
    print('processing batch ' + str(i))
    consumer_to_match = domestic_consumer_to_match.sample(frac = 0.01) # reduce size for sampling
    chunk_import = pd.merge(chunk, consumer_to_match,
                            on = ['dms_dest', 'SCTG_Code', 'SCTG_Group'], how = 'left')
    failed_shipment = \
    chunk_import.loc[chunk_import['BuyerZone'].isna()]
    failed_shipment = failed_shipment[existing_attr]
    chunk_import = chunk_import.dropna()
    # chunk_attraction.loc[:, 'importance'] = 1 /((chunk_attraction.loc[:, 'distance'] + 2) ** power_coeff)
    # chunk_attraction.loc[chunk_attraction['importance'] < 0.0001, 'importance'] = 0.0001
    chunk_import = chunk_import.groupby(existing_attr).sample(n = 1, 
                                                             weights = chunk_import['Size'],
                                                             replace = True, 
                                                             random_state = 1)
    import_truck_shipment_assigned = pd.concat([import_truck_shipment_assigned,
                                                chunk_import])
    import_truck_shipment_failed = pd.concat([import_truck_shipment_failed,
                                              failed_shipment])
    i += 1
    # break

# <codecell>
# assign failed ones (drop SCTG)
consumer_to_match = domestic_consumer_to_match.sample(frac = 0.05) # reduce size for sampling
print(len(import_truck_shipment_failed))
consumer_to_match.drop(columns = ['SCTG_Code'], inplace = True) # ignore SCTG from producers
import_truck_shipment_reassign = pd.merge(import_truck_shipment_failed, 
                                        consumer_to_match,
                                        on = ['dms_dest', 'SCTG_Group'], 
                                        how = 'left')
failed_shipment = \
import_truck_shipment_reassign.loc[import_truck_shipment_reassign['BuyerZone'].isna()]
failed_shipment = failed_shipment[existing_attr]
import_truck_shipment_reassign.dropna(inplace = True)
import_truck_shipment_reassign = \
    import_truck_shipment_reassign.groupby(existing_attr).sample(n = 1, 
                                                             weights = import_truck_shipment_reassign['Size'],
                                                             replace = True, 
                                                             random_state = 1)
print(len(import_truck_shipment_reassign))
import_truck_shipment_assigned = pd.concat([import_truck_shipment_assigned,
                                            import_truck_shipment_reassign])

# final imputation -- drop all SCTG
consumer_to_match = domestic_consumer_to_match.sample(frac = 0.05) # reduce size for sampling
print(len(failed_shipment))
consumer_to_match.drop(columns = ['SCTG_Code', 'SCTG_Group'], inplace = True) # ignore SCTG from producers
import_truck_shipment_reassign = pd.merge(failed_shipment, 
                                        consumer_to_match,
                                        on = ['dms_dest'], 
                                        how = 'left')
failed_shipment = \
import_truck_shipment_reassign.loc[import_truck_shipment_reassign['BuyerZone'].isna()]
failed_shipment = failed_shipment[existing_attr]
import_truck_shipment_reassign.dropna(inplace = True)
import_truck_shipment_reassign = \
    import_truck_shipment_reassign.groupby(existing_attr).sample(n = 1, 
                                                              weights = import_truck_shipment_reassign['Size'],
                                                              replace = True, 
                                                              random_state = 1)
print(len(import_truck_shipment_reassign))

import_truck_shipment_assigned = pd.concat([import_truck_shipment_assigned,
                                            import_truck_shipment_reassign])

# write export output
# <codecell>

########################################################
############ step 4 - writing output ###################
########################################################

# write export output
export_truck_shipment_assigned.drop(columns = ['Size'], inplace = True)
export_truck_shipment_assigned.loc[:, 'veh_type']= 'Diesel ' + veh_type_to_assign
export_truck_shipment_assigned = \
    export_truck_shipment_assigned.rename(columns = {'MESOZONE': 'PORTZONE',
                                                     'SCTG_Code': 'Commodity_SCTG'})

export_truck_shipment_assigned.to_csv(os.path.join(int_out_dir, export_with_firm_file), index = False)

import_truck_shipment_assigned.loc[:, 'veh_type']= 'Diesel ' + veh_type_to_assign
import_truck_shipment_assigned.drop(columns = ['Size'], inplace = True)
import_truck_shipment_assigned = \
    import_truck_shipment_assigned.rename(columns = {'MESOZONE': 'PORTZONE',
                                                     'SCTG_Code': 'Commodity_SCTG'})

import_truck_shipment_assigned.to_csv(os.path.join(int_out_dir, import_with_firm_file), index = False)