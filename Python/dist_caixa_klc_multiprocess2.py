# -*- coding: utf-8 -*-
"""
Created on Thu Mar 16 14:13:00 2023

@author: mathe
"""

import pandas as pd
import geopy.distance
import multiprocessing
import time
start = time.time()

df1_file = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\address_normalized_cleaned_url_google2.dta'
#df1_file = "/home/mcs038/Documents/Pix_regressions/stata/address_normalized_cleaned_url_google2.dta"

df_weight_file = 'C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/dist_caixa/distance_rank_weight.dta'
#df_weight_file = "/home/mcs038/Documents/Pix_regressions/stata/distance_rank_weight.dta"

df2_file = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\aux_address\aux_address_partial_results7_super_cleaned.dta'
#df2_file = "/home/mcs038/Documents/Pix_regressions/stata/aux_address_partial_results7_super_cleaned.dta"

df3_file = 'C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/dist_caixa/dist_caixa_multiprocess.dta'
#df3_file = "/home/mcs038/Documents/Pix_regressions/stata/dist_caixa_multiprocess.dta"

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
df3 = pd.DataFrame(index=df2['index'].astype(int))
df3['dist_caixa'] = None
df3['dist_otherbank'] = None
df3['dist_bank'] = None
df3['expected_distance'] = None

# Function to calculate distances for a given row in df2
def calculate_distances(row2):
    distances = []
    bank = []
    for index1, row1 in df1.iterrows():
        distances.append(geopy.distance.distance((row1['latitude'], row1['longitude']), (row2['latitude'], row2['longitude'])).km)
        bank.append(row1['bank'])
    caixa_distances = [distances[i] for i in range(len(distances)) if bank[i] == 'Caixa']
    return min(caixa_distances) if len(caixa_distances) > 0 else None


# Define the number of processes to use
num_processes = multiprocessing.cpu_count()

# Split df2 into chunks to be processed in parallel
chunks = [df2[i:i+num_processes] for i in range(0, len(df2), num_processes)]

# Create a pool of worker processes
pool = multiprocessing.Pool(processes=num_processes)

# Map the calculation function to the chunks of data
results = pool.map(calculate_distances, chunks)

# Combine the results into a single dataframe
df3['dist_caixa'] = pd.concat(results)

# Convert the "expected_distance" column to number before exporting
df3['dist_caixa'] = pd.to_numeric(df3["dist_caixa"])
df3.to_stata(df3_file)

end = time.time()
print(end - start)
