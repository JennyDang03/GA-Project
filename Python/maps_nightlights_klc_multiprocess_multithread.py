# -*- coding: utf-8 -*-
"""
Created on Mon Jun 19 08:39:53 2023

@author: mathe
"""
import pandas as pd
import numpy as np
import multiprocessing as mp
#import concurrent.futures as cf
import ee
import geemap.foliumap as geemap
import time
start = time.time()


def process_coordinate(coor):
    viirs2019 = ee.ImageCollection("NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG").filterDate("2019-01-01","2019-12-31").select('avg_rad').median()
    elv = ee.Image('USGS/SRTMGL1_003')
    scale = 1000
    aoi = ee.Geometry.Point([coor[1], coor[0]])
    arr = geemap.ee_to_numpy(viirs2019, region=aoi)
    radiance = np.mean(arr)
    elevation = elv.sample(aoi, scale).first().get('elevation').getInfo()
    return radiance, elevation

def process_coor(df2_split):
    try:
        ee.Initialize()
    except Exception as e:
        ee.Authenticate()
        ee.Initialize()
    list_coor = df2_split[['latitude', 'longitude']].values.tolist()
    
    #NOT MULTITHREADING #######################################################
    radiance_people = []
    elevation_people = []
    viirs2019 = ee.ImageCollection("NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG").filterDate("2019-01-01","2019-12-31").select('avg_rad').median()
    elv = ee.Image('USGS/SRTMGL1_003')
    scale = 1000
    for j, coor in enumerate(list_coor):
        #print(f"Processing split {i+1}/{num_splits}, coordinate {j+1}/{len(list_coor)}")
        aoi = ee.Geometry.Point([coor[1], coor[0]])
        arr = geemap.ee_to_numpy(viirs2019, region=aoi)
        try:
            radiance = np.mean(arr)
        except Exception as e:
            radiance = None  # Assign an empty value to radiance_people in case of any error
        try:
            elevation = elv.sample(aoi, scale).first().get('elevation').getInfo()
        except Exception as e:
            elevation = None
            
        radiance_people.append(radiance)
        elevation_people.append(elevation)
    
    #MULTITHREADING ###########################################################
    # I took out the multithreading because google had a limit on downloads per minute
    #with cf.ThreadPoolExecutor() as executor:
    #    results = list(executor.map(process_coordinate, list_coor))
    #radiance_people, elevation_people = zip(*results)
    #### [(coor, viirs2019, elv, scale) for _, coor in enumerate(list_coor)] - example
    
    ##################################################
    df2_split["radiance"] = radiance_people
    df2_split["elevation"] = elevation_people
    return df2_split

if __name__ == '__main__':
    df2_file = 'C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/aux_address/aux_address_partial_results7_super_cleaned.dta'
    df2_file = "/home/mcs038/Documents/Pix_regressions/stata/aux_address_partial_results7_super_cleaned.dta"
    
    df3_file = 'C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/radiance_elevation_people.dta'
    df3_file = "/home/mcs038/Documents/Pix_regressions/stata/radiance_elevation_people.dta"
    
    dta_folder = 'C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/'
    dta_folder = "/home/mcs038/Documents/Pix_regressions/stata/"

    try:
        ee.Initialize()
    except Exception as e:
        ee.Authenticate()
        ee.Initialize()

    viirs2019 = ee.ImageCollection("NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG").filterDate("2019-01-01","2019-12-31").select('avg_rad').median()
    elv = ee.Image('USGS/SRTMGL1_003')
    
    df2 = pd.read_stata(df2_file)
    #df2 = df2[0:320]
    backup_split_number = 20 #This is in case the process breaks down in the middle
    number_of_workers = 24
    scale = 1000  # scale in meters
    
    

    #### Lets test if the exceeding quota of google is making many results empty or just taking a bit more time to load everything. 
    #df2 = df2[0:64000]
    
    
    backup_splits = np.array_split(df2, backup_split_number)
    for i, backup_split in enumerate(backup_splits):
        print(f"Starting split {i+1} of {backup_split_number}")
        if i+1 >=0: # if the process breaks in the middle, adjust here
            df2_splits = np.array_split(backup_split, number_of_workers)
            pool = mp.Pool(number_of_workers)
            results = pool.map(process_coor, df2_splits)
            pool.close()
            pool.join()
            df3 = pd.concat(results)
            file_name = f"radiance_elevation_people{i+1}.dta"
            df3_subfile = dta_folder + file_name
            df3.to_stata(df3_subfile)
    
    df3_final = pd.DataFrame()
    #append backup splits
    for i in range(1,backup_split_number+1):
        print(f"Starting to append split {i+1} of {backup_split_number}")
        file_name = f"radiance_elevation_people{i}.dta"
        df3_subfile = dta_folder + file_name
        df3_sub= pd.read_stata(df3_subfile)
        df3_final = df3_final.append(df3_sub)
        
    df3_final = df3_final.drop('level_0', axis=1)
    df3_final.to_stata(df3_file)
    
    end = time.time()
    print(end-start)