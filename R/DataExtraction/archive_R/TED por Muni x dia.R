################################################################################
# TED por Muni x dia.R
# Input:  './queries/STR - muni x dia - Intramuni.sql'
#         './queries/SITRAF - muni x dia - INTRA.sql'
#         './queries/SITRAF - muni x dia - QTD_CLI PAG.sql'
#         './queries/SITRAF - muni x dia - QTD_CLI REC.sql'
#         './queries/STR - muni x dia - QTD_CLI PAG.sql'
#         './queries/STR - muni x dia - QTD_CLI REC.sql'
#         
# Output: "./results/TED_muni_dia_intra_", as.character(currentYEAR), ".csv"
#         "./results/TED_muni_dia_intra_SITRAF_", as.character(currentYEAR), ".csv"
#         "./results/TED_muni_dia_QTDCLI_PAG_SITRAF", as.character(currentYEAR), ".csv"
#         "./results/TED_muni_dia_QTDCLI_REC_SITRAF", as.character(currentYEAR), ".csv"
#         "./results/TED_muni_dia_QTDCLI_PAG_STR", as.character(currentYEAR), ".csv"
#         "./results/TED_muni_dia_QTDCLI_REC_STR", as.character(currentYEAR), ".csv"
# y:  

# The goal: 

# To do: 

################################################################################

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

setwd("//sbcdf060/depep$/DEPEPCOPEF/TEDslicer")

source("./functions/parametrizeQuery.R")
source("./functions/extractSQLServerData.R")

# Global settings ---------------------------------------------------------
# Hashmap -----------------------------------------------------------------

#mapBankCNPJ = fread(input = "./data/hash_bank_cnpj.csv")
#mapBankConglomerate = fread(input = "./data/hash_bank_conglomerate.csv")
#mapIndividuals = fread(input = "./data/hash_persons.csv")
# mapFirms = fread("./data/hash_firms.csv")
# mapFirms8 = unique(mapFirms[, .(CNPJ8, CNPJ8_HASH)])
# rm(mapFirms)

# Query -------------------------------------------------------------------

##Coletar dados de TED por municipio por dia, e por tipo de pessoa
##  1) Primeiro pega TEDs do STR intramunicipio
##  2) Depois pega TEDs do Sitraf
##  3) Qtd clientes por muni pagador Sitraf
##  4) Qtd clientes por muni recebedor Sitraf

 

 
 #################################################
 ## 1)   Coletar dados de TED INTRA-municipio - STR
 #################################################
 
 standardQuery <- read_file('./queries/STR - muni x dia - Intramuni.sql', 
                            locale = locale(encoding = "latin1"))
 
 START_YEAR = 2022 
 END_YEAR   = 2022
 currentYEAR = START_YEAR
 
 while(currentYEAR <= END_YEAR) {
    
    if(exists("allData")) {
       rm(allData)
    }
    
    
    flog.info("Extracting data for %d...", currentYEAR)
    parameters = data.table(from = "@selectedYEAR", to = currentYEAR)
    
    parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
    
    data = as.data.table(extractSQLServerData(parametrizedQuery))
    
    if(exists("allData")) {
       allData = rbindlist(l = list(allData, data), use.names = T)
    } else {
       allData = data
    }
    
    # Save the data
    fwrite(x = allData, file = paste0("./results/TED_muni_dia_intra_", 
                                      as.character(currentYEAR), ".csv"))
    
    currentYEAR = currentYEAR + 1 
    
 }  
 
 ################## SITRAF ####################
 ####################################################
 ##   2) Coletar dados de TED INTRA-municipio - SITRAF
 ####################################################
 
 standardQuery <- read_file('./queries/SITRAF - muni x dia - INTRA.sql', 
                            locale = locale(encoding = "latin1"))
 
 START_YEAR = 2022 
 END_YEAR   = 2022
 currentYEAR = START_YEAR
 
 while(currentYEAR <= END_YEAR) {
    
    if(exists("allData")) {
       rm(allData)
    }
    
    
    flog.info("Extracting INTRA-municipio - SITRAF data for %d...", currentYEAR)
    parameters = data.table(from = "@selectedYEAR", to = currentYEAR)
    
    parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
    
    data = as.data.table(extractSQLServerData(parametrizedQuery))
    
    if(exists("allData")) {
       allData = rbindlist(l = list(allData, data), use.names = T)
    } else {
       allData = data
    }
    
    # Save the data
    fwrite(x = allData, file = paste0("./results/TED_muni_dia_intra_SITRAF_", 
                                      as.character(currentYEAR), ".csv"))
    
    currentYEAR = currentYEAR + 1 
    
 }  


################## QTD CLIENTES - #######################
#######################################################################
##########   3) Coletar dados Qtd Clientes PAGADOR TED - SITRAF
#######################################################################
 
 standardQuery <- read_file('./queries/SITRAF - muni x dia - QTD_CLI PAG.sql', 
                            locale = locale(encoding = "latin1"))
 
 START_YEAR = 2018 
 END_YEAR   = 2022
 currentYEAR = START_YEAR
 
 while(currentYEAR <= END_YEAR) {
   
   if(exists("allData")) {
     rm(allData)
   }
   
   
   flog.info("Extracting data for %d...", currentYEAR)
   parameters = data.table(from = "@selectedYEAR", to = currentYEAR)
   
   parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
   
   data = as.data.table(extractSQLServerData(parametrizedQuery))
   
   if(exists("allData")) {
     allData = rbindlist(l = list(allData, data), use.names = T)
   } else {
     allData = data
   }
   
   # Save the data
   fwrite(x = allData, file = paste0("./results/TED_muni_dia_QTDCLI_PAG_SITRAF", 
                                     as.character(currentYEAR), ".csv"))
   
   currentYEAR = currentYEAR + 1 
   
 }  
 
 
 ##########################################################
 ##  4) Coletar dados Qtd Clientes RECEBEDOR TED - SITRAF
 ##########################################################
 
 standardQuery <- read_file('./queries/SITRAF - muni x dia - QTD_CLI REC.sql', 
                            locale = locale(encoding = "latin1"))
 
 START_YEAR = 2018 
 END_YEAR   = 2022
 currentYEAR = START_YEAR
 
 while(currentYEAR <= END_YEAR) {
   
   if(exists("allData")) {
     rm(allData)
   }
   
   
   flog.info("Extracting data for %d...", currentYEAR)
   parameters = data.table(from = "@selectedYEAR", to = currentYEAR)
   
   parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
   
   data = as.data.table(extractSQLServerData(parametrizedQuery))
   
   if(exists("allData")) {
     allData = rbindlist(l = list(allData, data), use.names = T)
   } else {
     allData = data
   }
   
   # Save the data
   fwrite(x = allData, file = paste0("./results/TED_muni_dia_QTDCLI_REC_SITRAF", 
                                     as.character(currentYEAR), ".csv"))
   
   currentYEAR = currentYEAR + 1 
   
 }  
 
 #######################################################################
 ##########   5) Coletar dados Qtd Clientes PAGADOR TED - STR
 #######################################################################
 
 standardQuery <- read_file('./queries/STR - muni x dia - QTD_CLI PAG.sql', 
                            locale = locale(encoding = "latin1"))
 
 START_YEAR = 2018 
 END_YEAR   = 2022
 currentYEAR = START_YEAR
 
 while(currentYEAR <= END_YEAR) {
   
   if(exists("allData")) {
     rm(allData)
   }
   
   
   flog.info("Extracting data for %d...", currentYEAR)
   parameters = data.table(from = "@selectedYEAR", to = currentYEAR)
   
   parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
   
   data = as.data.table(extractSQLServerData(parametrizedQuery))
   
   if(exists("allData")) {
     allData = rbindlist(l = list(allData, data), use.names = T)
   } else {
     allData = data
   }
   
   # Save the data
   fwrite(x = allData, file = paste0("./results/TED_muni_dia_QTDCLI_PAG_STR", 
                                     as.character(currentYEAR), ".csv"))
   
   currentYEAR = currentYEAR + 1 
   
 }  

 
 #######################################################################
 ##########   6) Coletar dados Qtd Clientes PAGADOR TED - STR
 #######################################################################
 
 standardQuery <- read_file('./queries/STR - muni x dia - QTD_CLI REC.sql', 
                            locale = locale(encoding = "latin1"))
 
 START_YEAR = 2018 
 END_YEAR   = 2022
 currentYEAR = START_YEAR
 
 while(currentYEAR <= END_YEAR) {
   
   if(exists("allData")) {
     rm(allData)
   }
   
   
   flog.info("Extracting data for %d...", currentYEAR)
   parameters = data.table(from = "@selectedYEAR", to = currentYEAR)
   
   parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
   
   data = as.data.table(extractSQLServerData(parametrizedQuery))
   
   if(exists("allData")) {
     allData = rbindlist(l = list(allData, data), use.names = T)
   } else {
     allData = data
   }
   
   # Save the data
   fwrite(x = allData, file = paste0("./results/TED_muni_dia_QTDCLI_REC_STR", 
                                     as.character(currentYEAR), ".csv"))
   
   currentYEAR = currentYEAR + 1 
   
 }  
 
 
  