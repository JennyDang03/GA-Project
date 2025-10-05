# -*- coding: utf-8 -*-
"""
Created on Tue Mar  7 17:16:27 2023

@author: mathe
"""

import urllib.parse
import pandas as pd
import numpy as np

df = pd.read_stata("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/ESTBAN-CAIXA/address_normalized_cleaned.dta")
df.head(10)
df["encoded_address"] = df["address"].apply(urllib.parse.quote)

header = ["unique_location_id", "encoded_address"]
df.to_csv("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/CSV/geocoding/address_normalized_cleaned_url.csv",header=False,index=False, columns = header)
####################################################



df = pd.read_stata("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/aux_address/aux_address_cleaned.dta")
df.head(10)
#df["encoded_address"] = df["address"].apply(urllib.parse.quote)
df_sample = df[0:10]

# Shuffle the rows of the DataFrame with a seed value of 42
df = df.sample(frac=1, random_state=42).reset_index(drop=True)

# Split the DataFrame into groups of 30,000 rows each
num_groups = int(np.ceil(len(df) / 30000))
groups = np.array_split(df, num_groups)

header = ["index", "address"]
file_address = "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/CSV/aux_address/30k_files/"
# Export each group to a separate CSV file
for i, group in enumerate(groups):
    group.to_csv(file_address+"aux_address"+str(i)+".csv",header=False,index=False, columns = header)
    
 