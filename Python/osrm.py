# -*- coding: utf-8 -*-
"""
Created on Wed Apr 26 16:35:29 2023

@author: mathe
"""
import osrm

points = [(-33.45017046193167,-70.65281867980957),
          (-33.45239047269638,-70.65300107002258),
          (-33.453867464504555,-70.65277576446533)]

result = osrm.match(points, steps=False, overview="simplified")

result = osrm.simple_route(
                      [21.0566163803209,42.004088575972], [20.9574645547597, 41.5286973392856],
                      output='route', overview="full", geometry='wkt')


import osrm

client = osrm.Client(host='http://localhost:5000')

response = client.route(
    coordinates=[[-74.0056, 40.6197], [-74.0034, 40.6333]],
    overview=osrm.overview.full)

print(response)