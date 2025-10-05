# -*- coding: utf-8 -*-
"""
Created on Tue Sep 26 18:15:04 2023

@author: mathe
"""

# Create maps of Floods!

import pandas as pd


import ee
#import geemap
import geemap.foliumap as geemap
import webbrowser
from datetime import datetime

#import seaborn as sns
#import matplotlib.pyplot as plt
import numpy as np



#This file has data on floods for each municipality monthly since 2013
df_flood_monthly_file = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\natural_disasters_monthly_filled_flood.dta'
df_flood_monthly = pd.read_stata(df_flood_monthly_file)

# Get information on the municipality. Which state, ...



# Collapse the data by id_municipio (sum) number_disasters
# Then do map
# maybe keep only 2018 to 2022. 

center_lat = -14.235004
center_lon = -51.92528
zoomlevel=4.5

# initialize another map add this image as a layer to our map object
# and call the layer: "DMSP NTL 1992"
Map = geemap.Map(center=[center_lat,center_lon], zoom=zoomlevel)

# add our alternate basemap
Map.add_basemap("CartoDB.Positron") ## if you put the wrong name, all possible names appear
