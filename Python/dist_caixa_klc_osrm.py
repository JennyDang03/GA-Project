# -*- coding: utf-8 -*-
"""
Created on Tue May  2 14:07:58 2023

@author: mathe
"""


# Let's calculate distances!

"""
import numpy as np
import math
import tqdm
# y is 66078 (Number of banks)
# n is 25890 (Number of Caixas)
y = len(df1)
#n = len(df1["bank"]=="Caixa")
n = len(df1.loc[df1["bank"] == "Caixa"])

# create a list of integers from 1 to y for distance_rank
rank = list(range(1, y+1))

# create a list of weights using a list comprehension
weights = [math.comb(y-r-1, n-1) / math.comb(y, n) for r in tqdm.tqdm(range(y))]

# create a DataFrame with distance_rank and weight columns
df_weight = pd.DataFrame({'distance_rank': rank, 'weight': weights})

print(df_weight['weight'].sum())

df_weight.to_stata(df_weight_file, write_index=False)
"""


import pandas as pd
import geopy.distance


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

# Create a new dataframe with the index and expected_distance columns
df3 = pd.DataFrame(index=df2['index'].astype(int))
df3['dist_caixa'] = None
df3['dist_otherbank'] = None
df3['dist_bank'] = None
df3['expected_distance'] = None

# Calculate the geodistance between the two datasets
for index2, row2 in df2.iterrows():
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

# Convert the "expected_distance" column to number before exporting
df3['expected_distance'] = pd.to_numeric(df3["expected_distance"])
df3['dist_caixa'] = pd.to_numeric(df3["dist_caixa"])
df3['dist_bank'] = pd.to_numeric(df3["dist_bank"])
df3['dist_otherbank'] = pd.to_numeric(df3["dist_otherbank"])
df3.to_stata(df3_file)