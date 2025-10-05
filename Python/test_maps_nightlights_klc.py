# -*- coding: utf-8 -*-
"""
Created on Wed Jun 14 13:29:51 2023

@author: mathe
"""

#Test

import ee
ee.Authenticate()
ee.Initialize()
print(ee.Image("NASA/NASADEM_HGT/001").get("title").getInfo())
