# -*- coding: utf-8 -*-
"""
Created on Thu Mar 16 15:22:47 2023

@author: mathe
"""
import pandas as pd
#Sample

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
df1 = df1[0:600]
df1_file_sample = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\bank_address_sample.dta'
df1.to_stata(df1_file_sample)

# Load and clean the second dataset
df2 = pd.read_stata(df2_file)
df2 = df2[['latitude', 'longitude', 'index']]
df2 = df2[0:600]
df2_file_sample = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\aux_address\people_address_sample.dta'
df2.to_stata(df2_file_sample)
