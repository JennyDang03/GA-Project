# -*- coding: utf-8 -*-
"""
Created on Thu Mar 16 14:09:02 2023

@author: mathe
"""
#Code for chat gpt
import pandas as pd
import geopy.distance


df1_file = "/home/mcs038/Documents/Pix_regressions/stata/address_normalized_cleaned_url_google2.dta"
df_weight_file = "/home/mcs038/Documents/Pix_regressions/stata/distance_rank_weight.dta"
df2_file = "/home/mcs038/Documents/Pix_regressions/stata/aux_address_partial_results7_super_cleaned.dta"
df3_file = "/home/mcs038/Documents/Pix_regressions/stata/dist_caixa.dta"

df1 = pd.read_stata(df1_file)
df_weight = pd.read_stata(df_weight_file)
df2 = pd.read_stata(df2_file)

df3 = pd.DataFrame(index=df2['index'].astype(int))
df3['dist_caixa'] = None

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
    

# Convert the "expected_distance" column to number before exporting
df3['dist_caixa'] = pd.to_numeric(df3["dist_caixa"])
df3.to_stata(df3_file)