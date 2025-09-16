################################################################################
# BoletosMuni.R
# Input:  './queries/Boleto_PF.sql'
#         './queries/Boleto_PJ.sql'
#         './queries/Boleto_PF_Qtd_Cli_Pagador.sql'
#         './queries/Boleto_PJ_Qtd_Cli_Pagador.sql'
#         './queries/Boleto_PJ_Qtd_Cli_Recebedor.sql'
# Output: "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/Boleto_MUNI_PF_", as.character(currentYEAR*100+mes), ".csv"
#         "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/Boleto_MUNI_PJ_", as.character(currentYEAR*100+mes), ".csv"
#         "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/Boleto_MUNI_PF_QTD_CLI_", as.character(currentYEAR*100+mes), ".csv"
#         "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/Boleto_MUNI_PJ_QTD_CLI_", as.character(currentYEAR*100+mes), ".csv"
#         "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/Boleto_MUNI_PJ_QTD_CLI_REC_", as.character(currentYEAR*100+mes), ".csv"

# y:  

# The goal: 

# To do: 

################################################################################


# Boletos por municipio - PF e PJ
options(download.file.method = "wininet")
rm(list = ls())
if(!require(data.table)){install.packages("data.table")}
if(!require(readr)){install.packages("readr")}
if(!require(stringr)){install.packages("stringr")}
if(!require(odbc)){install.packages("odbc")}
if(!require(dplyr)){install.packages("dplyr")}
if(!require(tidyr)){install.packages("tidyr")}
if(!require(RODBC)){install.packages("RODBC")}
if(!require(futile.logger)){install.packages("futile.logger")}
if(!require(bit64)){install.packages("bit64")}
if(!require(haven)){install.packages("haven")}

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
source("./functions/extractTeradataDataSEDE.R")

# Global settings ---------------------------------------------------------
# Hashmap -----------------------------------------------------------------
#mapCNPJ14 = fread("./data/hash_firms14_2022.csv")

# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

### 1)  POR MUNICIPIO DO PAGADOR PF, MEIO E CANAL - QTD E VOLUME
### 2)  POR MUNICIPIO DO PAGADOR PJ, MEIO E CANAL - QTD E VOLUME
### 3)  MUNICIPIO DO PAGADOR PF - QTD DE CLI
### 4)  MUNICIPIO DO PAGADOR PJ - QTD DE CLI
### 5)  MUNICIPIO DO RECEBEDOR PJ - QTD DE CLI


# Query -------------------------------------------------------------------
### 1)  POR MUNICIPIO DO PAGADOR PF, MEIO E CANAL - QTD E VOLUME
##Coletar dados PIX
standardQuery <- read_file('./queries/Boleto_PF.sql', 
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
  
  if (mes == 12) {
    mes_seg = 1
    currentYEARseg = currentYEAR + 1
  } else {
    mes_seg = mes + 1
    currentYEARseg = currentYEAR
  }

  data_ini = paste(toString(currentYEAR) ,formatC(mes, width=2, flag="0"), "01", sep="-")
  #data_ini = paste("'", data_ini, "'")

  data_fim = paste(toString(currentYEARseg) ,formatC(mes_seg, width=2, flag="0"), "01", sep="-")

  parameters = data.table(from = "@selectedDateSTART", to = data_ini)
  parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
  
  parameters = data.table(from = "@selectedDateEND", to = data_fim)
  parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)

  data <- as.data.table(dbGetQuery(connection, parametrizedQuery))  

  # Save the aggregated muni data for each month
  fwrite(x = data, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/Boleto_MUNI_PF_", 
                                    as.character(currentYEAR*100+mes), ".csv"))

  mes = mes + 1

}

  currentYEAR = currentYEAR + 1 

}  

 flog.info("Extracting data - BOLETO PJ MUNI PAGADOR")
 
 ### 2)  POR MUNICIPIO DO PAGADOR PJ, MEIO E CANAL - QTD E VOLUME
 
 standardQuery <- read_file('./queries/Boleto_PJ.sql', 
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
     
     if (mes == 12) {
       mes_seg = 1
       currentYEARseg = currentYEAR + 1
     } else {
       mes_seg = mes + 1
       currentYEARseg = currentYEAR
     }
     
     data_ini = paste(toString(currentYEAR) ,formatC(mes, width=2, flag="0"), "01", sep="-")
     #data_ini = paste("'", data_ini, "'")
     
     data_fim = paste(toString(currentYEARseg) ,formatC(mes_seg, width=2, flag="0"), "01", sep="-")
     
     parameters = data.table(from = "@selectedDateSTART", to = data_ini)
     parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
     
     parameters = data.table(from = "@selectedDateEND", to = data_fim)
     parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
     
     data <- as.data.table(dbGetQuery(connection, parametrizedQuery))
     
     # Save the aggregated muni data for each month
     fwrite(x = data, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/Boleto_MUNI_PJ_", 
                                    as.character(currentYEAR*100+mes), ".csv"))
     
     mes = mes + 1
     
   }
   
   currentYEAR = currentYEAR + 1 
   
 }  
 

 ### 3)  POR MUNICIPIO DO PAGADOR PF - QTD CLIENTES
 
 flog.info("Extracting data - BOLETO PF MUNI PAGADOR - QTD CLIENTES")
 
 standardQuery <- read_file('./queries/Boleto_PF_Qtd_Cli_Pagador.sql', 
                            locale = locale(encoding = "latin1"))
 
 START_YEAR = 2020
 END_YEAR   = 2022
 currentYEAR = START_YEAR
 

 while(currentYEAR <= END_YEAR) {
   
   mes = 1
   
   if(exists("allData")) {
     rm(allData)
   }
   
   
   while(mes <= 12) {
     
     flog.info("Extracting data for %s...", paste(currentYEAR,mes,sep="-") )
     
     if (mes == 12) {
       mes_seg = 1
       currentYEARseg = currentYEAR + 1
     } else {
       mes_seg = mes + 1
       currentYEARseg = currentYEAR
     }
     
     data_ini = paste(toString(currentYEAR) ,formatC(mes, width=2, flag="0"), "01", sep="-")
     #data_ini = paste("'", data_ini, "'")
     
     data_fim = paste(toString(currentYEARseg) ,formatC(mes_seg, width=2, flag="0"), "01", sep="-")
     
     parameters = data.table(from = "@selectedDateSTART", to = data_ini)
     parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
     
     parameters = data.table(from = "@selectedDateEND", to = data_fim)
     parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
     
     data <- as.data.table(dbGetQuery(connection, parametrizedQuery))
     
     # Save the aggregated muni data for each month
     fwrite(x = data, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/Boleto_MUNI_PF_QTD_CLI_", 
                                    as.character(currentYEAR*100+mes), ".csv"))
     
     mes = mes + 1
     
   }
   
   currentYEAR = currentYEAR + 1 
   
 }  
 
 ### 4)  POR MUNICIPIO DO PAGADOR PJ - QTD CLIENTES
 
 flog.info("Extracting data - BOLETO PJ MUNI PAGADOR - QTD CLIENTES")
 
 standardQuery <- read_file('./queries/Boleto_PJ_Qtd_Cli_Pagador.sql', 
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
     
     if (mes == 12) {
       mes_seg = 1
       currentYEARseg = currentYEAR + 1
     } else {
       mes_seg = mes + 1
       currentYEARseg = currentYEAR
     }
     
     data_ini = paste(toString(currentYEAR) ,formatC(mes, width=2, flag="0"), "01", sep="-")
     #data_ini = paste("'", data_ini, "'")
     
     data_fim = paste(toString(currentYEARseg) ,formatC(mes_seg, width=2, flag="0"), "01", sep="-")
     
     parameters = data.table(from = "@selectedDateSTART", to = data_ini)
     parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
     
     parameters = data.table(from = "@selectedDateEND", to = data_fim)
     parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
     
     data <- as.data.table(dbGetQuery(connection, parametrizedQuery))
     
     # Save the aggregated muni data for each month
     fwrite(x = data, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/Boleto_MUNI_PJ_QTD_CLI_", 
                                    as.character(currentYEAR*100+mes), ".csv"))
     
     mes = mes + 1
     
   }
   
   currentYEAR = currentYEAR + 1 
   
 }  
 
 ### 5)  MUNICIPIO DO RECEBEDOR PJ - QTD DE CLI
 flog.info("Extracting data - BOLETO PJ MUNI RECEBEDOR - QTD CLIENTES")
 
 standardQuery <- read_file('./queries/Boleto_PJ_Qtd_Cli_Recebedor.sql', 
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
     
     if (mes == 12) {
       mes_seg = 1
       currentYEARseg = currentYEAR + 1
     } else {
       mes_seg = mes + 1
       currentYEARseg = currentYEAR
     }
     
     data_ini = paste(toString(currentYEAR) ,formatC(mes, width=2, flag="0"), "01", sep="-")
     #data_ini = paste("'", data_ini, "'")
     
     data_fim = paste(toString(currentYEARseg) ,formatC(mes_seg, width=2, flag="0"), "01", sep="-")
     
     parameters = data.table(from = "@selectedDateSTART", to = data_ini)
     parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
     
     parameters = data.table(from = "@selectedDateEND", to = data_fim)
     parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
     
     data <- as.data.table(dbGetQuery(connection, parametrizedQuery))
     
     # Save the aggregated muni data for each month
     fwrite(x = data, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/Boleto_MUNI_PJ_QTD_CLI_REC_", 
                                    as.character(currentYEAR*100+mes), ".csv"))
     
     mes = mes + 1
     
   }
   
   currentYEAR = currentYEAR + 1 
   
 }  
 
 dbDisconnect(connection)
 
 
 
 
