# -*- coding: utf-8 -*-
"""
Created on Thu May  4 14:29:55 2023

@author: mathe
"""

import folium
import pandas as pd
import webbrowser
#import geopandas as gpd
from branca.element import Figure


map_file_path = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output\map.html'
df1_file = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\address_normalized_cleaned_url_google2.dta'
#df1_file = "/home/mcs038/Documents/Pix_regressions/stata/address_normalized_cleaned_url_google2.dta"
df2_file = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\aux_address\aux_address_partial_results7_super_cleaned.dta'
#df2_file = "/home/mcs038/Documents/Pix_regressions/stata/aux_address_partial_results7_super_cleaned.dta"
df2 = pd.read_stata(df2_file)
df2 = df2[['latitude', 'longitude', 'index']]

# Load and clean the first dataset -> Banks locations
df1 = pd.read_stata(df1_file)
df1 = df1[['latitude', 'longitude', 'bank']]

df1_caixa =  df1[(df1.bank == 'Caixa')]
#df1_caixa = df1_caixa[0:1000]
df1_n_caixa =  df1[(df1.bank != 'Caixa')]
#df1_n_caixa = df1_n_caixa[0:3000]
# Load and clean the second dataset

#location=(-14.235004, -51.92528)
map_new = folium.Map(location=(-14.235004, -51.92528), zoom_start=4.5, tiles="cartodb positron")
#
list_coor = df1_caixa[['bank','latitude','longitude']].values.tolist()
for i in list_coor:
    #map_new.add_child(folium.Marker(location=[i[1],i[2]],popup=i[0],icon=folium.Icon(color='red',icon="fa-sharp fa-regular fa-grip-dots", icon_size=(10,10), shadowSize = (0,0))))
    folium.Circle(location=[i[1],i[2]], color='red',fill=True, radius=10,opacity=1,fill_opacity=1).add_to(map_new)
map_new.save(map_file_path)
        
list_coor = df1_n_caixa[['bank','latitude','longitude']].values.tolist()
for i in list_coor:
    #map_new.add_child(folium.Marker(location=[i[1],i[2]],popup=i[0],icon=folium.Icon(color='green',icon="fa-sharp fa-regular fa-grip-dots", icon_size=(10,10), shadowSize = (0,0))))
    folium.Circle(location=[i[1],i[2]], color='green',fill=True, radius=10,opacity=1,fill_opacity=1).add_to(map_new)
map_new.save(map_file_path)

webbrowser.open(map_file_path)

fig=Figure(width=550,height=350)
fig.add_child(map_new)
map_new

