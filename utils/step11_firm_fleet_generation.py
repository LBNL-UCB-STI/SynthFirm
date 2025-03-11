#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 11 13:48:31 2025

@author: xiaodanxu
"""

import pandas as pd
import os
import numpy as np
from pandas import read_csv



########################################################
#### step 1 - configure environment and load inputs ####
########################################################

scenario_name = 'Seattle'
out_scenario_name = 'Seattle'
file_path = '/Users/xiaodanxu/Documents/SynthFirm.nosync'
parameter_dir = 'SynthFirm_parameters'
number_of_processes = 4
input_dir = 'inputs_' + scenario_name
output_path = 'outputs_' + out_scenario_name

synthetic_firms_with_location_file = 'synthetic_firms_with_location.csv'
private_fleet_file = 'fleet/veh_per_emp_by_state.csv'
for_hire_fleet_file = 'fleet/FMCSA_truck_count_by_state_size.csv'
cargo_type_distribution_file = 'fleet/probability_of_cargo_group.csv'

fleet_year = 2018
fleet_name = 'Base fuel, 1.0 * elec'
regulations = 'ACC and ACT'

ev_availability_file = 'synthfirm_ev_availability.csv'
firms_with_fleet_file = 'synthetic_firms_with_fleet.csv'
carriers_with_fleet_file = 'synthetic_carriers.csv'
leasing_with_fleet_file = 'synthetic_leasing_company.csv'
firms_with_fleet_mc_adj_files = 'synthetic_firms_with_fleet_mc_adjusted.csv'

