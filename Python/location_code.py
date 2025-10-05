# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
from geopy.geocoders import Nominatim
import numpy as np
from geopy.extra.rate_limiter import RateLimiter

# Import DATA

# Change from Brazil to Brasil
#Put language in portuguese

df = pd.read_stata("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/ESTBAN-CAIXA/address_normalized.dta")
df.head(10)


# Run code to get latitude longitude
geolocator = Nominatim(user_agent="appname2")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

#df['location_address2'] = df['address2'].apply(geocode)
#df.to_csv("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/ESTBAN-CAIXA/address_normalized_latlon.csv")

# Unique values:
    
# Address1:
unique_array_address1 = df["address1"].unique()
latlon_address1 = pd.DataFrame(unique_array_address1, columns=["address1"])
latlon_address1['location_address1'] = latlon_address1['address1'].apply(geocode)
latlon_address1['point_address1'] = latlon_address1['location_address1'].apply(lambda loc: tuple(loc.point) if loc else None)
latlon_address1.to_csv("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/ESTBAN-CAIXA/latlon_address1_update.csv")

df = pd.merge(df,latlon_address1,how="right",on=["address1"])

# Address2:
unique_array_address2 = df["address2"].unique()
latlon_address2 = pd.DataFrame(unique_array_address2, columns=["address2"])
latlon_address2['location_address2'] = latlon_address2['address2'].apply(geocode)
latlon_address2['point_address2'] = latlon_address2['location_address2'].apply(lambda loc: tuple(loc.point) if loc else None)
latlon_address2.to_csv("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/ESTBAN-CAIXA/latlon_address2.csv")

df = pd.merge(df,latlon_address2,how="right",on=["address2"])

# CEP:
unique_array_cep = df["cep"].unique()
latlon_cep = pd.DataFrame(unique_array_address2, columns=["cep"])
latlon_cep['location_cep'] = latlon_address2['cep'].apply(geocode)
latlon_cep['point_cep'] = latlon_address2['location_cep'].apply(lambda loc: tuple(loc.point) if loc else None)
latlon_cep.to_csv("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/ESTBAN-CAIXA/latlon_cep.csv")

df = pd.merge(df,latlon_cep,how="right",on=["cep"])


df.to_csv("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/ESTBAN-CAIXA/address_normalized_latlon.csv")


    
    
#####################################
# Another way
#####################################

geolocator = Nominatim(user_agent="appname2")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

## Cep ########################################################################
unique_array = df["cep"].unique()
latlon = pd.DataFrame(unique_array, columns=["cep"])
length = len(unique_array)
lat_cep = np.empty(length, dtype=object)
lat_cep[:] = np.nan
lon_cep = np.empty(length, dtype=object)
lon_cep[:] = np.nan

# for i in range(length):
for i in range(18981,length):
    location = geolocator.geocode(unique_array[i])
    if location is None:
        continue
    else:
        lat_cep[i] = location.latitude
        lon_cep[i] = location.longitude
else: print("Done with cep!")
latlon["lat_cep"] = lat_cep
latlon["lon_cep"] = lon_cep
latlon.to_csv("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/ESTBAN-CAIXA/latloncep.dta")

## Address1 ###################################################################
unique_array_address1 = df["address1"].unique()

latlon_address1 = pd.DataFrame(unique_array_address1, columns=["address1"])
length_address1 = len(unique_array_address1)
lat_address1 = np.empty(length_address1, dtype=object)
lat_address1[:] = np.nan
lon_address1 = np.empty(length_address1, dtype=object)
lon_address1[:] = np.nan

# for i in range(length_address1):
for i in range(2434,length_address1):
    location = geocode(unique_array_address1[i])
    #location = geolocator.geocode(unique_array_address1[i])
    if location is None:
        continue
    else:
        lat_address1[i] = location.latitude
        lon_address1[i] = location.longitude
else: print("Done with address1!")
latlon_address1["lat_address1"] = lat_address1
latlon_address1["lon_address1"] = lon_address1
latlon_address1.to_csv("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/ESTBAN-CAIXA/latlon_address1_update.csv")

## Address2 ###################################################################
unique_array_address2 = df["address2"].unique()

latlon_address2 = pd.DataFrame(unique_array_address2, columns=["address2"])
length_address2 = len(unique_array_address2)
lat_address2 = np.empty(length_address2, dtype=object)
lat_address2[:] = np.nan
lon_address2 = np.empty(length_address2, dtype=object)
lon_address2[:] = np.nan

# for i in range(length_address1):
for i in range(length_address2):
    location = geolocator.geocode(unique_array_address2[i])
    if location is None:
        continue
    else:
        lat_address2[i] = location.latitude
        lon_address2[i] = location.longitude
else: print("Done with address2!")
latlon_address2["lat_address2"] = lat_address2
latlon_address2["lon_address2"] = lon_address2
latlon_address2.to_csv("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/ESTBAN-CAIXA/latlon_address2.csv")


## MERGE ######################################################################
df = pd.merge(df,latlon,how="right",on=["cep"])
df = pd.merge(df,latlon_address1,how="right",on=["address1"])
df = pd.merge(df,latlon_address1,how="right",on=["address2"])
df.to_csv("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/ESTBAN-CAIXA/address_normalized_latlon.csv")



    
#####################################
# Another way
#####################################




import arcpy  
from arcpy import env  
env.workspace = "N:\Reference\GIS\State\Project\Address Locator"  
geocoder = env.workspace + "\State_Addresses_Create_Addr"  
for y in range(1,43):  
    zippo = "Projects_" + str(y)  
    outfile = r'N:\Project_Geocode_Results_{0}.shp'.format(y) 
    fncgeo = 'Street Apartment VISIBLE NONE; ZIP {0} VISIBLE'.format(zippo)  
    arcpy.GeocodeAddresses_geocoding("Geocode_Table", geocoder, fncgeo, outfile) 



import requests

response = requests.get('https://maps.googleapis.com/maps/api/geocode/json?address=1600+Amphitheatre+Parkway,+Mountain+View,+CA')

resp_json_payload = response.json()

print(resp_json_payload['results'][0]['geometry']['location'])




import requests
import urllib.parse

address = 'Shivaji Nagar, Bangalore, KA 560001'
url = 'https://nominatim.openstreetmap.org/search/' + urllib.parse.quote(address) +'?format=json'

response = requests.get(url).json()
print(response[0]["lat"])
print(response[0]["lon"])


#####################################
# Another way
#####################################


from opencage.geocoder import OpenCageGeocode
from opencage.geocoder import InvalidInputError, RateLimitExceededError, UnknownError

key = "22b98c5cf0394fbaa417974bf8c5fd4c"
geocoder = OpenCageGeocode(key)


address='1108 ROSS CLARK CIRCLE,DOTHAN,HOUSTON,AL'
result = geocoder.geocode(address, no_annotations="1")  
result[0]['geometry']



####
#Needs to do this to unique values

# Import DATA
df = pd.read_stata("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/ESTBAN-CAIXA/address_normalized.dta")
df.head(10)

addresses = df["address1"].values.tolist()
addresses_100 = addresses[0:100]
key = "22b98c5cf0394fbaa417974bf8c5fd4c"
geocoder = OpenCageGeocode(key)
lat_address1 = []
lon_address1 = []
for address in addresses_100: 
    result = geocoder.geocode(address, no_annotations="1")  
    
    if result and len(result):  
        longitude  = result[0]["geometry"]["lng"]  
        latitude = result[0]["geometry"]["lat"] 
    else:  
        longitude = ""  
        latitude = ""  
    
    lat_address1.append(latitude) 
    lon_address1.append(longitude)

df["lat_address1"] = lat_address1
df["lon_address1"] = lon_address1
df.head(10)






#####################################
# Create Map
#####################################





import folium
#import webbrowser

################
map = folium.Map(location=[bike_station_locations.Latitude.mean(), bike_station_locations.Longitude.mean()], zoom_start=14, control_scale=True

for index, location_info in bike_station_locations.iterrows():
    folium.Marker([location_info["Latitude"], location_info["Longitude"]], popup=location_info["Name"]).add_to(map)
###################
    
    
# -14.2400732 -53.1805017 - Brazil's location

folium_map = folium.Map(location=[-14.2400732,-53.1805017],zoom_start=4.4,tiles="CartoDB dark_matter")


FastMarkerCluster(data[[‘latitudes’, ‘longitudes’]].values.tolist()).add_to(folium_map)
folium.LayerControl().add_to(folium_map) for row in final.iterrows():
    row=row[1]
    folium.CircleMarker(location=(row["latitudes"],
                                  row["longitudes"]),
                        radius= 10,
                        color="#007849",
                        popup=row[‘Facility_Name’],
                        fill=False).add_to(folium_map)
    
folium_map

folium_map.save("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output/maps/test1_brazil.html")

webbrowser.open_tab("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output/maps/test1_brazil.html")


















"""












#make Brasil tag

# put commas instead of -

#try the structured address later

q=<query>

street=<housenumber> <streetname>
city=<city>
county=<county>
state=<state>
country=<country>
postalcode=<postalcode>


q=""&limit=1

for i in range(2433,length_address1):
    location = geolocator.geocode(unique_array_address1[i])
    if location is None:
        continue
    elif type(location) == "geopy.location.Location":
        lat_address1[i] = location.latitude
        lon_address1[i] = location.longitude
    else:
        print(i)
        break
else: print("Done with address1!")







## Address 1:
    # Make sure the loop goes over the unique CEPs

    # Create unique cep df

    # Run the code
    
    # merge back

    
for ind in range(10):
    location = geolocator.geocode(df["address1"][ind])
    #print((location.latitude, location.longitude))
    #print(location.address)
    
else: print("Done with address1!")
#print(location.raw)


location = geolocator.geocode("72940000")
print(location.address)
# Return CSV or dta




####################




location = geolocator.reverse("52.509669, 13.376294")
print(location.address)
print((location.latitude, location.longitude))
print(location.raw)





from pycep_correios import get_address_from_cep, WebService, exceptions

address = get_address_from_cep('61658710', webservice=WebService.APICEP)

print(address)
location = geolocator.geocode(address)
print(location.address)
print((location.latitude, location.longitude))

try:

    address = get_address_from_cep('37503-130', webservice=WebService.APICEP)

except exceptions.InvalidCEP as eic:
    print(eic)

except exceptions.CEPNotFound as ecnf:
    print(ecnf)

except exceptions.ConnectionError as errc:
    print(errc)

except exceptions.Timeout as errt:
    print(errt)

except exceptions.HTTPError as errh:
    print(errh)

except exceptions.BaseException as e:
    print(e)

"""