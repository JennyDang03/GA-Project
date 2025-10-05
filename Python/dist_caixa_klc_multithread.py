# -*- coding: utf-8 -*-
"""
Created on Wed Mar 15 11:47:36 2023

@author: mathe
"""

import pandas as pd
import geopy.distance
import concurrent.futures

import time
start = time.time()

#from concurrent.futures import ThreadPoolExecutor, as_completed
#from multiprocessing import Pool

df1_file = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\address_normalized_cleaned_url_google2.dta'
#df1_file = "/home/mcs038/Documents/Pix_regressions/stata/address_normalized_cleaned_url_google2.dta"

df_weight_file = 'C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/dist_caixa/distance_rank_weight.dta'
#df_weight_file = "/home/mcs038/Documents/Pix_regressions/stata/distance_rank_weight.dta"

df2_file = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\aux_address\aux_address_partial_results7_super_cleaned.dta'
#df2_file = "/home/mcs038/Documents/Pix_regressions/stata/aux_address_partial_results7_super_cleaned.dta"

df3_file = 'C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/dist_caixa/dist_caixa.dta'
#df3_file = "/home/mcs038/Documents/Pix_regressions/stata/dist_caixa.dta"

# Load and clean the first dataset -> Banks locations
df1 = pd.read_stata(df1_file)
df1 = df1[['latitude', 'longitude', 'bank']]
df1= df1[0:60]
# Generate Weights
df_weight = pd.read_stata(df_weight_file)

# Load and clean the second dataset
df2 = pd.read_stata(df2_file)
df2 = df2[['latitude', 'longitude', 'index']]
df2 = df2[0:600]
# Create a new dataframe with the index and expected_distance columns
df3 = pd.DataFrame(index=df2['index'].astype(int), columns=['expected_distance'])

#------------------------------------------------------------------------------
def calculate_distances(row2):
    distances = []
    bank = []
    for index1, row1 in df1.iterrows():
        distances.append(geopy.distance.distance((row1['latitude'], row1['longitude']), (row2['latitude'], row2['longitude'])).km)
        bank.append(row1['bank'])
    # Find the smallest variable in distances in which bank == "Caixa". Save it to df3["dist_caixa"]
    caixa_distances = [distances[i] for i in range(len(distances)) if bank[i] == 'Caixa']
    df3.at[row2['index'], 'dist_caixa'] = min(caixa_distances) if len(caixa_distances) > 0 else None
    
    # Find the smallest variable in distances in which bank != "Caixa". Save it to df3["dist_otherbank"]
    otherbank_distances = [distances[i] for i in range(len(distances)) if bank[i] != 'Caixa']
    df3.at[row2['index'], 'dist_otherbank'] = min(otherbank_distances) if len(otherbank_distances) > 0 else None
    
    # Find the smallest variable in distances. Save it to df3["dist_bank"]
    bank_distances = [distances[i] for i in range(len(distances))]
    df3.at[row2['index'], 'dist_bank'] = min(bank_distances) if len(bank_distances) > 0 else None
    
    # Rank the values in distances from 1 to len(df1). Find out the weight for each rank and multiply it to the distance
    weighted_distances = []
    for rank, weight in df_weight[['distance_rank', 'weight']].values:
        if rank <= len(distances):
            weighted_distances.append(weight * sorted(distances)[int(rank) - 1])
    df3.at[row2['index'], 'expected_distance'] = sum(weighted_distances) if len(weighted_distances) > 0 else None

# Define a function that will process each chunk of rows
def process_chunk(chunk):
    for _, row2 in chunk.iterrows():
        calculate_distances(row2)

# Split df2 into chunks
n = 64  # Number of chunks
chunks = [df2[i:i+n] for i in range(0, len(df2), n)]

# Use a ThreadPoolExecutor to process the chunks in parallel
with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
    executor.map(process_chunk, chunks)

# Convert the "expected_distance" column to number before exporting
df3['expected_distance'] = pd.to_numeric(df3["expected_distance"])
df3['dist_caixa'] = pd.to_numeric(df3["dist_caixa"])
df3['dist_bank'] = pd.to_numeric(df3["dist_bank"])
df3['dist_otherbank'] = pd.to_numeric(df3["dist_otherbank"])
df3.to_stata(df3_file)

end = time.time()
print(end - start)