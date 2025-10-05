# -*- coding: utf-8 -*-
"""
Created on Thu Mar 16 14:08:16 2023

@author: mathe
"""

import pandas as pd
import geopy.distance
import multiprocessing

# Set the number of processes to use
num_processes = 4

#df1_file = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\address_normalized_cleaned_url_google2.dta'
df1_file = "/home/mcs038/Documents/Pix_regressions/stata/address_normalized_cleaned_url_google2.dta"

#df_weight_file = 'C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/dist_caixa/distance_rank_weight.dta'
df_weight_file = "/home/mcs038/Documents/Pix_regressions/stata/distance_rank_weight.dta"

#df2_file = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\aux_address\aux_address_partial_results7_super_cleaned.dta'
df2_file = "/home/mcs038/Documents/Pix_regressions/stata/aux_address_partial_results7_super_cleaned.dta"

#df3_file = 'C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/dist_caixa/dist_caixa.dta'
df3_file = "/home/mcs038/Documents/Pix_regressions/stata/dist_caixa.dta"

# Load and clean the first dataset -> Banks locations
df1 = pd.read_stata(df1_file)
df1 = df1[['latitude', 'longitude', 'bank']]

# Generate Weights
df_weight = pd.read_stata(df_weight_file)

# Load and clean the second dataset
df2 = pd.read_stata(df2_file)
df2 = df2[['latitude', 'longitude', 'index']]

# Split the second dataset into chunks
chunks = [df2.loc[i:i+int(len(df2)/num_processes)-1, :] for i in range(0, len(df2), int(len(df2)/num_processes))]

# Define a function to be executed by each process
def process_chunk(chunk):
    # Create a new dataframe with the index and expected_distance columns
    df3 = pd.DataFrame(index=chunk['index'].astype(int))
    df3['dist_caixa'] = None
    df3['dist_otherbank'] = None
    df3['dist_bank'] = None
    df3['expected_distance'] = None

    # Calculate the geodistance between the two datasets
    for index2, row2 in chunk.iterrows():
        distances = []
        bank = []
        for index1, row1 in df1.iterrows():
            distances.append(geopy.distance.distance((row1['latitude'], row1['longitude']), (row2['latitude'], row2['longitude'])).km)
            bank.append(row1['bank'])
        # Find the smallest variable in distances in which bank == "Caixa". Save it to df3["dist_caixa"]
        caixa_distances = [distances[i] for i in range(len(distances)) if bank[i] == 'Caixa']
        df3.at[row2['index'], 'dist_caixa'] = min(caixa_distances) if len(caixa_distances) > 0 else None

        # Find the smallest variable in distances in which bank != "
