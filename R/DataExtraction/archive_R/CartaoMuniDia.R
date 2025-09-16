
################################################################################
# CartaoMuniDia.R
# Input:  './queries/CartaoDebitoMuniDia.sql'
#         './queries/CartaoCreditoMuniDia.sql'
# Output: "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/CartaoDebitoMuni_", as.character(currentYEAR), ".csv"
#         "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/CartaoCreditoMuni_", as.character(currentYEAR), ".csv"
# y:   

# The goal: 

# To do: 

################################################################################


# DADOS CARTAO DE DEBITO E CREDITO POR MUNICIPIO
options(download.file.method = "wininet")
rm(list = ls())
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

# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

# Query -------------------------------------------------------------------
##Coletar dados de cartao de debito
standardQuery <- read_file('./queries/CartaoDebitoMuniDia.sql', 
                           locale = locale(encoding = "latin1"))

START_YEAR = 2018
END_YEAR   = 2022
currentYEAR = START_YEAR

if(exists("allData")) {
  rm(allData)
}

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
    
    #data = as.data.table(extractTeradataData(parametrizedQuery))
    data <- as.data.table(dbGetQuery(connection, parametrizedQuery))
    
    
    if(nrow(data) > 0)
    {
        if(exists("allData")) {
          allData = rbindlist(l = list(allData, data), use.names = T)
        } else {
          allData = data
        }
    }
    currentDate = ifelse(test = currentDate %% 100 >= 12, 
                         yes = (round(currentDate / 100) + 1) * 100 + 1, 
                         no = currentDate + 1)  
    
  }


  # Save all the data
  if (dim(allData)[1] > 0) {
  fwrite(x = allData, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/CartaoDebitoMuni_", as.character(currentYEAR), ".csv"))
  }
  
  # Save the data by year
  currentYEAR = currentYEAR + 1 
  
}  

##########################################
##Coletar dados de cartao de CREDITO
##########################################

standardQuery <- read_file('./queries/CartaoCreditoMuniDia.sql', 
                           locale = locale(encoding = "latin1"))

START_YEAR = 2018
END_YEAR   = 2022
currentYEAR = START_YEAR

if(exists("allData")) {
  rm(allData)
}

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
    
    #data = as.data.table(extractTeradataData(parametrizedQuery))
    data <- as.data.table(dbGetQuery(connection, parametrizedQuery))
    
    if(nrow(data) > 0)
    {
      if(exists("allData")) {
        allData = rbindlist(l = list(allData, data), use.names = T)
      } else {
        allData = data
      }
    }
    currentDate = ifelse(test = currentDate %% 100 >= 12, 
                         yes = (round(currentDate / 100) + 1) * 100 + 1, 
                         no = currentDate + 1)  
    
  }
  
  
  # Save all the data
  if (dim(allData)[1] > 0) {
  fwrite(x = allData, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/CartaoCreditoMuni_", as.character(currentYEAR), ".csv"))
  }
  
  # Save the data by year
  currentYEAR = currentYEAR + 1 
  
}  

dbDisconnect(connection)

