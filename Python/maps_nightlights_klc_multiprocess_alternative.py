# -*- coding: utf-8 -*-
"""
Created on Sat Jun 17 13:34:31 2023

@author: mathe
"""

# Another type of multiprocess that I will check if it is faster

import ee
import geemap.foliumap as geemap
import pandas as pd
import numpy as np
import multiprocessing as mp
import time
start = time.time()


# Define the processing function
def process_coor(df2_split):
    #print(coor[1])
    try:
            ee.Initialize()
    except Exception as e:
            ee.Authenticate()
            ee.Initialize()
    viirs2019 = ee.ImageCollection("NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG").filterDate("2019-01-01","2019-12-31").select('avg_rad').median()
    elv = ee.Image('USGS/SRTMGL1_003')    
    scale = 1000 # scale in meters
    
    radiance_people = []
    elevation_people = []
    list_coor = df2_split[['latitude', 'longitude']].values.tolist()
    for j, coor in enumerate(list_coor):
        #print(f"Processing split {i+1}/{num_splits}, coordinate {j+1}/{len(list_coor)}")
        aoi = ee.Geometry.Point([coor[1], coor[0]])
        arr = geemap.ee_to_numpy(viirs2019, region=aoi)
        radiance = np.mean(arr)
        elevation = elv.sample(aoi, scale).first().get('elevation').getInfo()
        
        radiance_people.append(radiance)
        elevation_people.append(elevation)

    df2_split["radiance"] = radiance_people
    df2_split["elevation"] = elevation_people

    return df2_split




if __name__ == '__main__':
    df2_file = 'C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/aux_address/aux_address_partial_results7_super_cleaned.dta'
    #df2_file = "/home/mcs038/Documents/Pix_regressions/stata/aux_address_partial_results7_super_cleaned.dta"
    
    df3_file = 'C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/radiance_elevation_people.dta'
    #df3_file = "/home/mcs038/Documents/Pix_regressions/stata/radiance_elevation_people.dta"
    try:
            ee.Initialize()
    except Exception as e:
            ee.Authenticate()
            ee.Initialize()
    # VIIRS
    # VIIRS Stray Light Corrected Nighttime Day/Night Band Composites Version 1
    # get 2019 image, we're using the "avg_rad" band
    viirs2019 = ee.ImageCollection("NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG").filterDate("2019-01-01","2019-12-31").select('avg_rad').median()
    
    ### -> Lets get elevation of each person. 
    # Import the USGS ground elevation image.
    elv = ee.Image('USGS/SRTMGL1_003')
    
    ##########################################################################
    
    df2 = pd.read_stata(df2_file)
    df2 = df2[0:320]
    number_of_workers = 16
    scale = 1000  # scale in meters
    
    # Split df2 into multiple dataframes
    df2_splits = np.array_split(df2, number_of_workers)

    # Create a pool of worker processes
    pool = mp.Pool(number_of_workers)
    results = pool.map(process_coor, df2_splits)
    
    pool.close()
    pool.join()
    
    df3 = pd.concat(results)
    df3.to_stata(df3_file)
    # Reset the index of the combined dataframe
    #df3 = df3.reset_index(drop=True)
    
    end = time.time()
    print(end-start)