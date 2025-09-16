################################################################################
# CCS_Muni_IF_estoque.R
# Input:  "./queries/", CCS_Muni_IF_PF_estoque, ".sql"
#         
# Output: "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/",CCS_Muni_IF_PF_estoque, as.character(currentYEAR*100+mes), ".csv"
#         
# y:  

# The goal: 

# To do: Needs PJ

################################################################################


# CCS por municipio x IF x mes
# Estoque mensal de contas abertas por municipio x instituicao

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
library(lubridate)
setwd("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer")
source("./functions/parametrizeQuery.R")
source("./functions/extractTeradataDataSEDE.R")

# Global settings ---------------------------------------------------------
# Hashmap -----------------------------------------------------------------
mapCNPJ8 = fread("./data/hash_firms8_2022.csv")

# Query -------------------------------------------------------------------

################# LOOP ################
    tipo <- c("CCS_Muni_IF_PF_estoque")
    #tipo <- c("CCS_Muni_IF_PJ_estoque","CCS_Muni_IF_PF_estoque")

# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

 ## Coletar dados CCS para cada tipo acima
 for(j in 1:length(tipo)) {
     
   paste0("./queries/", tipo[j], ".sql")
   standardQuery <- read_file(paste0("./queries/", tipo[j], ".sql"), 
                              locale = locale(encoding = "latin1"))
   
   START_YEAR = 2018
   END_YEAR   = 2022
   currentYEAR = START_YEAR
   
   while(currentYEAR <= END_YEAR) {
     
     mes = 1
     
     if(exists("allData")) {
       rm(allData)
     }
  
     while(mes <= 12) {
       
       flog.info("Extracting data for %s...", paste(currentYEAR,mes,sep="-") )
       
       data_ini = paste(toString(currentYEAR) ,formatC(mes, width=2, flag="0"), "01", sep="-")

       parameters = data.table(from = "@selectedDate", to = data_ini)
       parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
       
       data <- as.data.table(dbGetQuery(connection, parametrizedQuery))  
 
       # ANONIMIZACAO DA IF
       data$instituicao = as.integer64(data$instituicao)
       data = merge(x = data, y = mapCNPJ8,
                    by.x = "instituicao", by.y = "CNPJ8",
                    all.x = T)
       
       # Check numero de nulls
       flog.info(sum(is.na (data$CNPJ8_HASH)))
       
       # Drop identified columns and keep hashed ones ----------------------------
       data$instituicao = data$CNPJ8_HASH
       data$CNPJ8_HASH       = NULL

       # Save the aggregated data for each week
       fwrite(x = data, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/",tipo[j], as.character(currentYEAR*100+mes), ".csv"))
  
       mes = mes + 1
    }  # End loop por mes
     currentYEAR = currentYEAR + 1 
   }  # End loop por ano
 } # end of tipo loop   
 
 
 
 