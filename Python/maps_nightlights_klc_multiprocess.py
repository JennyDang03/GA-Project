# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 15:22:22 2023

@author: mathe
"""

import ee
import geemap.foliumap as geemap
import pandas as pd
import numpy as np
import multiprocessing as mp
import time
start = time.time()


# Define the processing function
def process_coor(coor):
    print(coor[1])
    try:
            ee.Initialize()
    except Exception as e:
            ee.Authenticate()
            ee.Initialize()
    viirs2019 = ee.ImageCollection("NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG").filterDate("2019-01-01","2019-12-31").select('avg_rad').median()
    elv = ee.Image('USGS/SRTMGL1_003')    
    scale = 1000 # scale in meters
    aoi = ee.Geometry.Point([coor[1], coor[0]])
    arr = geemap.ee_to_numpy(viirs2019, region=aoi)
    radiance = np.mean(arr)
    elevation = elv.sample(aoi, scale).first().get('elevation').getInfo()
    return (radiance, elevation)

if __name__ == '__main__':
    df2_file = 'C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/aux_address/aux_address_partial_results7_super_cleaned.dta'
    #df2_file = "/home/mcs038/Documents/Pix_regressions/stata/aux_address_partial_results7_super_cleaned.dta"
    
    dta_folder = 'C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/'
    #dta_folder = "/home/mcs038/Documents/Pix_regressions/stata/"

    
    ##########################################################################
    
    df2 = pd.read_stata(df2_file)
    df2 = df2[0:320]
    number_of_workers = 16
    scale = 1000  # scale in meters
    
    list_coor = df2[['latitude', 'longitude']].values.tolist()
    
    # Create a pool of worker processes
    pool = mp.Pool(number_of_workers)
    results = pool.map(process_coor, list_coor)
    # [(coor, viirs2019, elv, scale) for _, coor in enumerate(list_coor)]
    
    # This works:
    #results = pool.map(process_coor, list_coor)
    #Now lets make something more fun/faster. Lets split df2 into number_of_workers and do a for loop inside the function so we can save time by initializing less
    
    pool.close()
    pool.join()
    
    df3 = pd.DataFrame(results, columns=['radiance', 'elevation'])
    
    end = time.time()
    print(end-start)