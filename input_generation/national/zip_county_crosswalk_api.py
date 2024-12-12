#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 15 13:36:12 2023

@author: xiaodanxu
"""

import pandas as pd
import requests
import os
import numpy as np

os.chdir('/Users/xiaodanxu/Documents/SynthFirm.nosync')
# return a Pandas Dataframe of HUD USPS Crosswalk values

# Note that type is set to 1 which will return values for the ZIP to Tract file and query is set to VA which will return Zip Codes in Virginia
url = "https://www.huduser.gov/hudapi/public/usps?type=2&query=All&year=2016"
token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6IjNkZWNjZmE2MDY5MGQ4YjU3NTBjZWVkOTFlNDk3M2E3NTUwYTdhZjA1NmUxZWRlNjNmMWUzYWZiOTIxNjc1YTJlZjdjYjVhMGY3ZmM2M2QyIn0.eyJhdWQiOiI2IiwianRpIjoiM2RlY2NmYTYwNjkwZDhiNTc1MGNlZWQ5MWU0OTczYTc1NTBhN2FmMDU2ZTFlZGU2M2YxZTNhZmI5MjE2NzVhMmVmN2NiNWEwZjdmYzYzZDIiLCJpYXQiOjE2OTQ4MTAyNjAsIm5iZiI6MTY5NDgxMDI2MCwiZXhwIjoyMDEwNDI5NDYwLCJzdWIiOiI1ODc2OSIsInNjb3BlcyI6W119.D0ks3vKCvsM2KTXJ1tyRhjgJZRPAzgHtLxLmJujiqYHm7Y0G2Yjx-QrrtMPMXvkTlaL_b9xsQwd8Z6TdBZtwHg"
headers = {"Authorization": "Bearer {0}".format(token)}

response = requests.get(url, headers = headers)

if response.status_code != 200:
	print ("Failure, see status code: {0}".format(response.status_code))
else: 
	df = pd.DataFrame(response.json()["data"]["results"])	
# 	print(df)

df = df[['zip', 'geoid', 'bus_ratio']]    
print('numbers of zip code in dataset:')
print(len(df.zip.unique()))     
# <codecell>



df.loc[:, 'fraction'] = df.loc[:, 'bus_ratio']/ \
    df.groupby('zip')['bus_ratio'].transform('sum')
df.fillna(1, inplace = True)
# make sure the output year is the same as in the URL


df.to_csv('RawData/CBP/ZIP_COUNTY_LOOKUP_2016.csv', index = False)
# <codecell>

# DON'T RUN THIS FOR TRACT LEVEL
df = df.sort_values(by = 'fraction', ascending = False)
df_nodup = df.drop_duplicates(subset = 'zip', keep = 'first')
print('numbers of zip code in dataset:')
print(len(df_nodup.zip.unique()))

df_nodup.to_csv('RawData/CBP/ZIP_COUNTY_LOOKUP_2016_clean.csv', index = False)