# -*- coding: utf-8 -*-
"""
Created on Thu May 25 09:43:06 2023

@author: mathe
"""
import ee
import geemap.foliumap as geemap
import pandas as pd
import numpy as np

#import geemap
#import webbrowser
#from datetime import datetime

#import seaborn as sns
#import matplotlib.pyplot as plt
#import numpy as np

#map_file_path = r'C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output/map_nightlights.html'


#df1_file = 'C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/ESTBAN-CAIXA/address_normalized_cleaned_url_google2.dta'
#df1_file = "/home/mcs038/Documents/Pix_regressions/stata/address_normalized_cleaned_url_google2.dta"
df2_file = 'C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/aux_address/aux_address_partial_results7_super_cleaned.dta'
#df2_file = "/home/mcs038/Documents/Pix_regressions/stata/aux_address_partial_results7_super_cleaned.dta"

#df_radiance_file = 'C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/radiance.dta'

df2_radiance_file = 'C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/'
#df2_radiance_file = "/home/mcs038/Documents/Pix_regressions/stata/radiance_people.dta"


df2 = pd.read_stata(df2_file)
#df2 = df2[['latitude', 'longitude', 'index']]

# Load and clean the first dataset -> Banks locations
#df1 = pd.read_stata(df1_file)
#df1 = df1[['latitude', 'longitude', 'bank']]

#df1_caixa =  df1[(df1.bank == 'Caixa')]
#df1_caixa = df1_caixa[0:100]
#df1_n_caixa =  df1[(df1.bank != 'Caixa')]
#df1_n_caixa = df1_n_caixa[0:300]

try:
        ee.Initialize()
except Exception as e:
        ee.Authenticate()
        ee.Initialize()

center_lat = -14.235004
center_lon = -51.92528
zoomlevel=4.5

##########################################################################
# VIIRS
# VIIRS Stray Light Corrected Nighttime Day/Night Band Composites Version 1

# get 2019 image, we're using the "avg_rad" band
viirs2019 = ee.ImageCollection("NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG").filterDate("2019-01-01","2019-12-31").select('avg_rad').median()


### -> Lets get elevation of each person. 
# Import the USGS ground elevation image.
elv = ee.Image('USGS/SRTMGL1_003')
scale = 1000  # scale in meters



# Determine the number of splits you want
num_splits = 5

# Split df2 into multiple dataframes
df2_splits = np.array_split(df2, num_splits)

# Process and save each split dataframe
for i, df_split in enumerate(df2_splits):
    df_split_test = df_split[0:5]
    radiance_people = []
    elevation_people = []
    list_coor = df_split_test[['latitude', 'longitude']].values.tolist()

    for j, coor in enumerate(list_coor):
        print(f"Processing split {i+1}/{num_splits}, coordinate {j+1}/{len(list_coor)}")
        aoi = ee.Geometry.Point([coor[1], coor[0]])
        arr = geemap.ee_to_numpy(viirs2019, region=aoi)
        radiance = np.mean(arr)
        radiance_people.append(radiance)

        elevation = elv.sample(aoi, scale).first().get('elevation').getInfo()
        elevation_people.append(elevation)

    df_split_test["radiance"] = radiance_people
    df_split_test["elevation"] = elevation_people
    
    filename = f"radiance_people_{i}.dta"
    output_path = df2_radiance_file+filename
    
    df_split_test.to_stata(output_path)


# Try with ee.GeometryMultipoint

# aoi = ee.Geometry.MultiPoint([i[1], i[0]])
# arr = geemap.ee_to_numpy(viirs2019, region=aoi)

# This can also get the ground elevation!!!!!!!!!!!!


