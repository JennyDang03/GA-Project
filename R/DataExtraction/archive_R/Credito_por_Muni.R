################################################################################

# Credito_por_Muni.R
# Input:  './queries/Credito_Por_Muni_PF.sql'
#         './queries/Credito_Por_Muni_PJ.sql'
# Output: "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/Credito_Muni_PF_", as.character(currentYEAR), ".csv"
#         "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/Credito_Muni_PJ_", as.character(currentYEAR), ".csv"
# y:  

# The goal: 

# To do: 

################################################################################

rm(list = ls())
options(download.file.method = "wininet")
library(readr)
library(stringr)
library(odbc)
library(dplyr)
library(tidyr)
library(RODBC)
library(futile.logger)
library(data.table)
library(bit64)
library(haven)
setwd("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer")
source("./functions/parametrizeQuery.R")
source("./functions/extractTeradataData.R")

# Global settings ---------------------------------------------------------
# Hashmap -----------------------------------------------------------------

# Query -------------------------------------------------------------------
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

##Coletar dados de crédito
standardQuery <- read_file('./queries/Credito_Por_Muni_PF.sql', 
                           locale = locale(encoding = "latin1"))

 START_YEAR = 2018 
 END_YEAR   = 2022
 currentYEAR = START_YEAR

while(currentYEAR <= END_YEAR) {

 START_DATE = currentYEAR * 100 + 1 
 END_DATE   = currentYEAR * 100 + 12
 currentDate = START_DATE 
 
 if(exists("allData")) {
   rm(allData)
 }

 
while(currentDate <= END_DATE) {
  
  flog.info("Extracting data for %d...", currentDate)
  parameters = data.table(from = "@selectedDate", to = currentDate)
  
  parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
  
  data <- as.data.table(dbGetQuery(connection, parametrizedQuery))
  
  if(exists("allData")) {
    allData = rbindlist(l = list(allData, data), use.names = T)
  } else {
    allData = data
  }
  
  currentDate = ifelse(test = currentDate %% 100 >= 12, 
                       yes = (round(currentDate / 100) + 1) * 100 + 1, 
                       no = currentDate + 1)  

}
  
  # Save the un-identified data
  fwrite(x = allData, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/Credito_Muni_PF_", 
                                    as.character(currentYEAR), ".csv"))
  
  currentYEAR = currentYEAR + 1 

}  

################ CREDITO PJ #############
 # Query -------------------------------------------------------------------
 DATA_SOURCE_NAME = "teradata-t"
 connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
 dbSendQuery(connection,"set role all")
 
 ##Coletar dados de crédito
 standardQuery <- read_file('./queries/Credito_Por_Muni_PJ.sql', 
                            locale = locale(encoding = "latin1"))
 
 START_YEAR = 2018 
 END_YEAR   = 2022
 currentYEAR = START_YEAR
 
 while(currentYEAR <= END_YEAR) {
   
   START_DATE = currentYEAR * 100 + 1 
   END_DATE   = currentYEAR * 100 + 12
   currentDate = START_DATE 
   
   if(exists("allData")) {
     rm(allData)
   }
   
   
   while(currentDate <= END_DATE) {
     
     flog.info("Extracting data for %d...", currentDate)
     parameters = data.table(from = "@selectedDate", to = currentDate)
     
     parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
     
     data <- as.data.table(dbGetQuery(connection, parametrizedQuery))
     
     if(exists("allData")) {
       allData = rbindlist(l = list(allData, data), use.names = T)
     } else {
       allData = data
     }
     
     currentDate = ifelse(test = currentDate %% 100 >= 12, 
                          yes = (round(currentDate / 100) + 1) * 100 + 1, 
                          no = currentDate + 1)  
     
   }
   
   # Save the un-identified data
   fwrite(x = allData, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/Credito_Muni_PJ_", 
                                     as.character(currentYEAR), ".csv"))
   
   currentYEAR = currentYEAR + 1 
   
 }  
 
 
 
 dbDisconnect(connection)


