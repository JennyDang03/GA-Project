# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 15:56:37 2023

@author: mathe
"""

# Let's calculate distances!

import pandas as pd
import geopy.distance

# load and clean the first dataset
df1 = pd.read_stata(r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\address_normalized_cleaned_url_google2.dta')
df1 = df1[['latitude', 'longitude', 'bank', 'type']]
df1.columns = ['latitude1', 'longitude1', 'bank', 'type']
#df1['id1'] = df1.index + 1
# maybe I should put the ID
# save the cleaned dataset
df1.to_stata(r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\temp\allbanks1.dta', write_index=False)

# load and clean the second dataset
df2 = pd.read_stata(r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\aux_address\aux_address_partial_results7_super_cleaned.dta')
df2 = df2[df2['confidence'] >= 10]
df2 = df2[['latitude', 'longitude', 'index', 'index0']]
#df2['cpf'] = df2.index + 1
#df2['id2'] = df2.index + 1

# calculate the geodistance between the two datasets
distances = []
for index1, row1 in df1.iterrows():
    for index2, row2 in df2.iterrows():
        distances.append(geopy.distance.distance((row1['latitude1'], row1['longitude1']), (row2['latitude'], row2['longitude'])).km)

# add the distances to the second dataset and sort by distance
df2['d_allbanks'] = distances
df2 = df2.sort_values(by=['index', 'd_allbanks'])

# add a column for the distance rank
df2['distance_rank'] = df2.groupby('index')['d_allbanks'].rank()



###############################################################################
import pandas as pd
import geopy.distance
import numpy as np
import math
import tqdm

# load and clean the first dataset -> Banks locations
df1 = pd.read_stata(r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\address_normalized_cleaned_url_google2.dta')
df1 = df1[['latitude', 'longitude', 'bank', 'type']]
df1.columns = ['latitude1', 'longitude1', 'bank', 'type']
df1.to_stata(r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\temp\allbanks1.dta', write_index=False)


#Generate y-i choses n-1 over y choses n -------------------------------------
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
df_weight.to_stata('C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/aux_address/distance_rank_weight.dta', write_index=False)
# -----------------------------------------------------------------------------


# load and clean the second dataset -> Individuals Location
df2 = pd.read_stata(r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\aux_address\aux_address_partial_results7_super_cleaned.dta')

# First, lets sample
#df2 = df2[df2['confidence'] >= 10]
df2 = df2[0:20]

df2 = df2[['latitude', 'longitude', 'index', 'index0']]

# Split the DataFrame into groups of 30,000 rows each
num_groups = int(np.ceil(len(df2) / 10))
groups = np.array_split(df2, num_groups)

# create a dummy variable to join the two datasets
df1['key'] = 1
# Save address
file_address = 'C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/temp/'
for i, group in enumerate(groups):
    # create a dummy variable to join the two datasets
    group['key'] = 1
    # join the two datasets using a Cartesian product
    df3 = pd.merge(df1, group, on='key')

    # calculate the geodistance between the two datasets
    df3['d_allbanks'] = df3.apply(lambda x: geopy.distance.distance((x['latitude1'], x['longitude1']), (x['latitude'], x['longitude'])).km, axis=1)

    ###group.to_csv(file_address+"aux_address"+str(i)+".csv",header=False,index=False, columns = header)
    
    # sort by distance and add a column for the distance rank
    df3 = df3.sort_values(by=['index', 'd_allbanks'])
    df3['distance_rank'] = df3.groupby('index')['d_allbanks'].rank()
    
    # extract the relevant columns and save the results
    group = df3[['latitude', 'longitude', 'index', 'index0', 'd_allbanks', 'distance_rank']]
    
    #group.to_stata(file_address+"aux_address_sample"+str(i), write_index=False)
    
        
