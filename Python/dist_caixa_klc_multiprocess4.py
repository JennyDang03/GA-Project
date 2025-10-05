# -*- coding: utf-8 -*-
"""
Created on Thu Mar 16 14:20:19 2023

@author: mathe
"""

import pandas as pd
import geopy.distance
import multiprocessing as mp
#from multiprocessing import Pool, Manager
import time
start = time.time()

number_of_workers = 24
geo_filter = 0.5

def calculate_distance(row1, row2):
    return geopy.distance.distance((row1['latitude'], row1['longitude']), (row2['latitude'], row2['longitude'])).km


def calculate_distances(df1, df2, df_weight):
    pool = mp.Pool(number_of_workers)
    results = []
    for index2, row2 in df2.iterrows():
        distances = []
        bank = []
        geo_filter = 0.1
        df1_filtered = df1.loc[(abs(df1['latitude'] - row2['latitude']) <= geo_filter) & (abs(df1['longitude'] - row2['longitude']) <= geo_filter)]
        caixa = df1_filtered[df1_filtered['bank'] == 'Caixa']
        
        while len(df1_filtered) < 100 or len(caixa) == 10:
            geo_filter += 0.1
            df1_filtered = df1.loc[(abs(df1['latitude'] - row2['latitude']) <= geo_filter) & (abs(df1['longitude'] - row2['longitude']) <= geo_filter)]
            caixa = df1_filtered[df1_filtered['bank'] == 'Caixa']
                
        for index1, row1 in df1_filtered.iterrows():
            distances.append(pool.apply(calculate_distance, args=(row1, row2)))
            bank.append(row1['bank'])
        # Find the smallest variable in distances in which bank == "Caixa". Save it to df3["dist_caixa"]
        caixa_distances = [distances[i] for i in range(len(distances)) if bank[i] == 'Caixa']
        dist_caixa = min(caixa_distances) if len(caixa_distances) > 0 else None
        
        otherbank_distances = [distances[i] for i in range(len(distances)) if bank[i] != 'Caixa']
        dist_otherbank = min(otherbank_distances) if len(otherbank_distances) > 0 else None
        
        bank_distances = [distances[i] for i in range(len(distances))]
        dist_bank = min(bank_distances) if len(bank_distances) > 0 else None
        
        weighted_distances = []
        for rank, weight in df_weight[['distance_rank', 'weight']].values:
            if rank <= len(distances):
                weighted_distances.append(weight * sorted(distances)[int(rank) - 1])
        expected_distance = sum(weighted_distances) if len(weighted_distances) > 0 else None
        
        results.append((row2['index'], dist_caixa, dist_otherbank, dist_bank, expected_distance))
               
    pool.close()
    pool.join()
    df3 = pd.DataFrame(results, columns=['index', 'dist_caixa', 'dist_otherbank', 'dist_bank', 'expected_distance'])
    return df3.set_index('index')

if __name__ == '__main__':
    df1_file = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\address_normalized_cleaned_url_google2.dta'
    df1_file = "/home/mcs038/Documents/Pix_regressions/stata/address_normalized_cleaned_url_google2.dta"

    df_weight_file = 'C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/dist_caixa/distance_rank_weight.dta'
    df_weight_file = "/home/mcs038/Documents/Pix_regressions/stata/distance_rank_weight.dta"

    df2_file = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\aux_address\aux_address_partial_results7_super_cleaned.dta'
    df2_file = "/home/mcs038/Documents/Pix_regressions/stata/aux_address_partial_results7_super_cleaned.dta"

    df3_file = 'C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/dist_caixa/dist_caixa_multiprocess5.dta'
    df3_file = "/home/mcs038/Documents/Pix_regressions/stata/dist_caixa_multiprocess.dta"

    # Load and clean the first dataset -> Banks locations
    df1 = pd.read_stata(df1_file)
    df1 = df1[['latitude', 'longitude', 'bank']]

    # Generate Weights
    df_weight = pd.read_stata(df_weight_file)

    # Load and clean the second dataset
    df2 = pd.read_stata(df2_file)
    df2 = df2[['latitude', 'longitude', 'index']]
    #df2 = df2[0:20]
    df3 = calculate_distances(df1, df2, df_weight)

    # Convert the "expected_distance" column to number before exporting
    df3['expected_distance'] = pd.to_numeric(df3["expected_distance"])
    df3['dist_caixa'] = pd.to_numeric(df3["dist_caixa"])
    df3['dist_bank'] = pd.to_numeric(df3["dist_bank"])
    df3['dist_otherbank'] = pd.to_numeric(df3["dist_otherbank"])
    df3.to_stata(df3_file)
    
    end = time.time()
    print(end-start)