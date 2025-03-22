#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 21 22:41:44 2025

@author: xiaodanxu
"""

import pandas as pd
import os
import math
import random
import numpy as np


import warnings
warnings.filterwarnings("ignore")

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')

# <codecell>
# load input tables

region_df = pd.read_csv('RawData/CFS/CFS2017_national_forML_short.csv')
# CFS2017 data with imputation 
factor_df = pd.read_csv('RawData/CFS/data_unitcost_by_zone_cfs2017.csv')
# unit cost from CFS 2017
sctg_group = pd.read_csv('RawData/SCTG_Groups_revised.csv')
# pre-defined sctg group
cfs_to_faf_lookup = pd.read_csv('RawData/CFS_FAF_LOOKUP.csv')
# cfs and faf zone crosswalk
faf_gc_distance = pd.read_csv('RawData/FAF_od_gc_distance.csv')
faf_routed_distance = pd.read_csv('RawData/FAF_od_highway_distance.csv')
# <codecell>