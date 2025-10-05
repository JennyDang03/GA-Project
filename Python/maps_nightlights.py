# -*- coding: utf-8 -*-
"""
Created on Fri May 19 16:45:39 2023

@author: mathe
"""



import ee
#import geemap
import geemap.foliumap as geemap
import pandas as pd
import webbrowser
from datetime import datetime

#import seaborn as sns
#import matplotlib.pyplot as plt
import numpy as np

map_file_path = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output\map_nightlights.html'
df1_file = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\address_normalized_cleaned_url_google2.dta'
#df1_file = "/home/mcs038/Documents/Pix_regressions/stata/address_normalized_cleaned_url_google2.dta"
df2_file = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\aux_address\aux_address_partial_results7_super_cleaned.dta'
#df2_file = "/home/mcs038/Documents/Pix_regressions/stata/aux_address_partial_results7_super_cleaned.dta"
df_radiance_file = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\radiance.dta'
df2_radiance_file = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\radiance_people.dta'

df2 = pd.read_stata(df2_file)
df2 = df2[['latitude', 'longitude', 'index']]

# Load and clean the first dataset -> Banks locations
df1 = pd.read_stata(df1_file)
df1 = df1[['latitude', 'longitude', 'bank']]

df1_caixa =  df1[(df1.bank == 'Caixa')]
#df1_caixa = df1_caixa[0:100]
df1_n_caixa =  df1[(df1.bank != 'Caixa')]
#df1_n_caixa = df1_n_caixa[0:300]
# Load and clean the second dataset

try:
        ee.Initialize()
except Exception as e:
        ee.Authenticate()
        ee.Initialize()

center_lat = -14.235004
center_lon = -51.92528
zoomlevel=4.5


##########################################################################
# DMSP

dmsp92id = "NOAA/DMSP-OLS/NIGHTTIME_LIGHTS/F101992"
dmsp2013id = "NOAA/DMSP-OLS/NIGHTTIME_LIGHTS/F182013"

# create an ee object for our 1992 image
# note that for DMSP, there is only one band, so we dont need to worry about selecting a band.
dmsp92 = ee.Image(dmsp92id)
dmsp2013 = ee.Image(dmsp2013id)

# initialize another map add this image as a layer to our map object
# and call the layer: "DMSP NTL 1992"
Map = geemap.Map(center=[center_lat,center_lon], zoom=zoomlevel)

# add our alternate basemap
Map.add_basemap("CartoDB.Positron") ## if you put the wrong name, all possible names appear

# add our 1992 (and remember to create a mask and change opacity to 75%)
Map.addLayer(dmsp2013.mask(dmsp2013), name='DMSP NTL 2013', opacity=0.75)

Map.addLayerControl()
Map.save(map_file_path)
webbrowser.open(map_file_path)


##########################################################################
# 1992 vs 2013

# generate tile layers from the ee image objects, masking and changing opacity to 75%
dmsp92_tile = geemap.ee_tile_layer(dmsp92.mask(dmsp92), {}, 'DMSP NTL 1992', opacity=0.75)
dmsp2013_tile = geemap.ee_tile_layer(dmsp2013.mask(dmsp2013), {}, 'DMSP NTL 2013', opacity=0.75)

# initial map object centered on Abuja
Map2 = geemap.Map(center=[center_lat,center_lon], zoom=zoomlevel)
# add our alternate basemap
Map2.add_basemap("CartoDB.Positron")
# use .split_map function to create split panels
Map2.split_map(left_layer=dmsp92_tile, right_layer=dmsp2013_tile)


Map2.addLayerControl()
Map2
Map2.save(map_file_path)
webbrowser.open(map_file_path)

##########################################################################
#get dates - not important
dmsp = ee.ImageCollection("NOAA/DMSP-OLS/NIGHTTIME_LIGHTS")
# DMSP OLS: Nighttime Lights Time Series Version 4, Defense Meteorological Program Operational Linescan System
print(f"There are {dmsp.size().getInfo()} images in this collection.")
def get_date_range(img_collection):
    imgrange = img_collection.reduceColumns(ee.Reducer.minMax(), ["system:time_start"])
    start = ee.Date(imgrange.get('min')).getInfo()['value']
    end = ee.Date(imgrange.get('max')).getInfo()['value']

    start = datetime.utcfromtimestamp(start/1000).strftime('%Y-%m-%d %H:%M:%S')
    end = datetime.utcfromtimestamp(end/1000).strftime('%Y-%m-%d %H:%M:%S')
    print(f"Date range: {start, end}")
get_date_range(dmsp)

##########################################################################
# VIIRS
# VIIRS Stray Light Corrected Nighttime Day/Night Band Composites Version 1

# get December image, we're using the "avg_rad" band
viirs2019_12 = ee.ImageCollection("NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG").filterDate("2019-12-01","2019-12-31").select('avg_rad').median()

# get 2019 image, we're using the "avg_rad" band
viirs2019 = ee.ImageCollection("NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG").filterDate("2019-01-01","2019-12-31").select('avg_rad').median()

Map = geemap.Map(center=[center_lat,center_lon], zoom=zoomlevel)
Map.add_basemap("CartoDB.Positron")
Map.addLayer(viirs2019, {}, "VIIRS-DNB", opacity=0.33)
#Map.addLayer(viirs2019.mask(viirs2019), {}, "VIIRS-DNB", opacity=1)

# How to Isolate Brazil from other countries:
# I would need to use a simpler version of the boundaries of Brazil for this to work
# Brazils map is to complex for google right now
#"C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/CSV/Brazil_shapefile2/brazil_administrative.shp"
#C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/CSV/Brazil_shapefile/BRA_adm0.shp
#ee_object = geemap.shp_to_ee("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/CSV/Brazil_shapefile/BRA_adm0.shp")
#Map.addLayer(ee_object, {}, 'Brazil')

Map.addLayerControl()
Map.save(map_file_path)
webbrowser.open(map_file_path)


##########################################################################

df1_caixa =  df1[(df1.bank == 'Caixa')]
df1_n_caixa =  df1[(df1.bank != 'Caixa')]

#df1_caixa_test =  df1_caixa[0:10]
#df1_n_caixa_test =  df1_n_caixa[0:10]


# This gets a number for the radiance: it is a numpy array
radiance_caixa = []
list_coor = df1_caixa[['bank','latitude','longitude']].values.tolist()
for i in list_coor:
    print(i)
    aoi = ee.Geometry.Point([i[2], i[1]]).buffer(1000);
    arr = geemap.ee_to_numpy(viirs2019, region=aoi)
    radiance = np.mean(arr)
    radiance_caixa.append(radiance)

radiance_n_caixa = []        
list_coor = df1_n_caixa[['bank','latitude','longitude']].values.tolist()
for i in list_coor:
    print(i)
    aoi = ee.Geometry.Point([i[2], i[1]]).buffer(1000);
    arr = geemap.ee_to_numpy(viirs2019, region=aoi)
    radiance = np.mean(arr)
    radiance_n_caixa.append(radiance)

# Create the DataFrame
data = {
    'caixa': [0] * len(radiance_n_caixa) + [1] * len(radiance_caixa),
    'radiance': radiance_n_caixa + radiance_caixa
}
df = pd.DataFrame(data)

df['latitude'] = df1_n_caixa['latitude'].tolist() + df1_caixa['latitude'].tolist()
df['longitude'] = df1_n_caixa['longitude'].tolist() + df1_caixa['longitude'].tolist() 
df['unique_location_id'] = df1_n_caixa['unique_location_id'].tolist() + df1_caixa['unique_location_id'].tolist()
df['state'] = df1_n_caixa['state'].tolist() + df1_caixa['state'].tolist()
df['mun_name'] = df1_n_caixa['mun_name'].tolist() + df1_caixa['mun_name'].tolist()

# Save the DataFrame to a Stata file
df.to_stata(df_radiance_file)





# do Hypothesis test 
from scipy.stats import ttest_ind
statistic, pvalue = ttest_ind(radiance_caixa, radiance_n_caixa)

# Print the results
print("T-test results:")
print("Statistic:", statistic)
print("p-value:", pvalue)



##########################################################################
df2_file = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\aux_address\aux_address_partial_results7_super_cleaned.dta'
#df2_file = "/home/mcs038/Documents/Pix_regressions/stata/aux_address_partial_results7_super_cleaned.dta"
df2_radiance_file = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\radiance_people.dta'

df2 = pd.read_stata(df2_file)
#df2 = df2[0:20]


# This gets a number for the radiance: it is a numpy array
radiance_people = []
list_coor = df2[['latitude','longitude']].values.tolist()
for i in list_coor:
    print(i)
    aoi = ee.Geometry.Point([i[1], i[0]]) # .buffer(1000);
    arr = geemap.ee_to_numpy(viirs2019, region=aoi)
    radiance = arr[0,0]
    radiance_people.append(radiance)

df2["radiance"] = radiance_people

# Save the DataFrame to a Stata file
df2.to_stata(df2_radiance_file)








##########################################################################
# Multiprocessing



import multiprocessing

# Set the number of processes to use
num_processes = multiprocessing.cpu_count()

df2_file = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\aux_address\aux_address_partial_results7_super_cleaned.dta'
df2_radiance_file = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\radiance_people.dta'

df2 = pd.read_stata(df2_file)
df2 = df2[0:100]


# Define the function to compute radiance for a single coordinate
def compute_radiance(coordinate):
    lat, lon = coordinate
    aoi = ee.Geometry.Point(lon, lat)
    arr = geemap.ee_to_numpy(viirs2019, region=aoi)
    radiance = arr[0, 0]
    return radiance

# Define the main function for processing the dataframe
def process_dataframe(df):
    # Create a pool of worker processes
    pool = multiprocessing.Pool()

    # Map the compute_radiance function to each coordinate in parallel
    radiance_people = pool.map(compute_radiance, df[['latitude', 'longitude']].values.tolist())

    # Close the pool of worker processes
    pool.close()
    pool.join()

    # Add the radiance column to the dataframe
    df['radiance'] = radiance_people

    return df

# Read the dataframe from the Stata file
df2_file = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\aux_address\aux_address_partial_results7_super_cleaned.dta'
df2_radiance_file = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\radiance_people.dta'
df2 = pd.read_stata(df2_file)

# Process the dataframe
df2_processed = process_dataframe(df2)

# Save the processed dataframe to a Stata file
df2_processed.to_stata(df2_radiance_file)




###########################################################################

########## SEEE THIS!!!!!!!!!!!

# get the geometry for Timor-Leste from GEE's tagged datasets
tls = ee.Feature(ee.FeatureCollection("FAO/GAUL/2015/level0").filter(ee.Filter.eq('ADM0_NAME', 'Brazil')).first()).geometry()

# clip our VIIRS image to Timor-Leste
ntl_tls = viirs2019.clip(tls)

# initialize our map and center it on Timor-Leste
map1 = geemap.Map()
map1.centerObject(tls, zoom=4)

map1.add_basemap("CartoDB.Positron")

map1.addLayer(ntl_tls, {}, "VIIRS-DNB 2019", opacity=0.33)
#map1.addLayer(ntl_tls.mask(ntl_tls), {}, "VIIRS-DNB", opacity=1)


#map1.addLayer(ntl_tls, {}, "VIIRS-DNB 2019")
map1.addLayerControl()
map1.save(map_file_path)
webbrowser.open(map_file_path)















