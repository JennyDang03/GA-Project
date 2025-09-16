################################################################################
# PixMuniBanco.R
# Input:  './queries/PixMuniBancoPAG.sql'
#         './queries/PixMuniBancoREC.sql'
#         './queries/Pix_IntraMuni_Banco_PAG.sql'
#         './queries/Pix_IntraMuni_Banco_REC.sql'
#         './queries/Pix_IntraMuni_Banco_qtd_PAG.sql'
#         './queries/Pix_IntraMuni_Banco_qtd_REC.sql'
#         './queries/PixMuniBancoSELF.sql'

# Output: "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/PIX_MUNI_IF_", as.character(currentYEAR*100+mes), ".csv"
#         "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/PIX_MUNI_IF_REC_", as.character(currentYEAR*100+mes), ".csv"
#         "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/PIX_INTRA_IF_PAG", as.character(currentYEAR*100+mes), ".csv"
#         "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/PIX_INTRA_IF_REC", as.character(currentYEAR*100+mes), ".csv"
#         "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/PIX_INTRA_IF_QTD_PAG", as.character(currentYEAR*100+mes), ".csv"
#         "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/PIX_INTRA_IF_QTD_REC", as.character(currentYEAR*100+mes), ".csv"
#         "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/PIX_SELF_MUNI_IF_", as.character(currentYEAR*100+mes), ".csv"

# y:  

# The goal: 

# To do: 

################################################################################


# Pix por municipio x Banco x tipo de pessoa
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
library(optiRum)
setwd("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer")
source("./functions/parametrizeQuery.R")
source("./functions/extractTeradataDataSEDE.R")

# Global settings ---------------------------------------------------------
# Hashmap -----------------------------------------------------------------
#mapCNPJ14 = fread("./data/hash_firms14_2022.csv")
mapCNPJ8 = fread("./data/hash_firms8_2022.csv")

# Query -------------------------------------------------------------------
##Coletar dados PIX
## Codigo Dividido em 5 partes:
## 1) Por municipio x banco do Pagador x tipo pessoa pagador
## 2) Por municipio x banco do Recebedor x tipo pessoa recebedor
## 3) INTRA Municipio x Banco do Pagador x tipo pessoa pagador
## 4) INTRA Municipio x Banco do Recebedor x tipo pessoa recebedor
## 5) QTD CLIENTES - INTRA Municipio x Banco do Pagador x Tipo pessoa Pagador #####
## 6) QTD CLIENTES - INTRA Municipio x Banco do Recebedor x Tipo pessoa Recebedor #####
## 7) PIX PARA SI MESMO: Municipio x Banco Creditado x Banco Debitado x tipo pessoa

## 1) Por Municipio x Banco do Pagador x tipo pessoa pagador
standardQuery <- read_file('./queries/PixMuniBancoPAG.sql', 
                           locale = locale(encoding = "latin1"))

 START_YEAR = 2022 
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
  data_ini = paste("'", data_ini, "'")

  data_fim = paste(toString(currentYEARseg) ,formatC(mes_seg, width=2, flag="0"), "01", sep="-")
  data_fim = paste("'", data_fim, "'")

  parameters = data.table(from = "@selectedDateSTART", to = data_ini)
  parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
  
  parameters = data.table(from = "@selectedDateEND", to = data_fim)
  parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)

  data = as.data.table(extractTeradataDataSEDE(parametrizedQuery))
  data$IF_PAG = as.integer64(trunc(data$IF_PAG/1000000))
  data = merge(x = data, y = mapCNPJ8,
                  by.x = "IF_PAG", by.y = "CNPJ8",
                  all.x = T)
  
  # Check numero de nulls
  flog.info(sum(is.na (data$CNPJ8_HASH)))
  
  # Drop identified columns and keep hashed ones ----------------------------
  data$IF_PAG = data$CNPJ8_HASH
  data$CNPJ8_HASH       = NULL
  
  # Save the aggregated data for each month
  fwrite(x = data, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/PIX_MUNI_IF_", 
                                    as.character(currentYEAR*100+mes), ".csv"))

  mes = mes + 1

}

  currentYEAR = currentYEAR + 1 

}  
 
 
 ##################################################################
 ## 2) Por municipio x banco do Recebedor x tipo pessoa Recebedor 
 ##################################################################
 
 
 standardQuery <- read_file('./queries/PixMuniBancoREC.sql', 
                            locale = locale(encoding = "latin1"))
 
 START_YEAR = 2022 
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
     data_ini = paste("'", data_ini, "'")
     
     data_fim = paste(toString(currentYEARseg) ,formatC(mes_seg, width=2, flag="0"), "01", sep="-")
     data_fim = paste("'", data_fim, "'")
     
     parameters = data.table(from = "@selectedDateSTART", to = data_ini)
     parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
     
     parameters = data.table(from = "@selectedDateEND", to = data_fim)
     parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
     
     data = as.data.table(extractTeradataDataSEDE(parametrizedQuery))
     
     data$IF_REC = as.integer64(trunc(data$IF_REC/1000000))
     data = merge(x = data, y = mapCNPJ8,
                  by.x = "IF_REC", by.y = "CNPJ8",
                  all.x = T)
     
     # Check numero de nulls
     flog.info(sum(is.na (data$CNPJ8_HASH)))
     
     # Drop identified columns and keep hashed ones ----------------------------
     data$IF_REC = data$CNPJ8_HASH
     data$CNPJ8_HASH       = NULL
     
     # Save the aggregated data for each month
     fwrite(x = data, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/PIX_MUNI_IF_REC_", 
                                    as.character(currentYEAR*100+mes), ".csv"))
     
     mes = mes + 1
     
   }
   
   currentYEAR = currentYEAR + 1 
   
 }  
 
 
 ####################################################################
 ## 3) INTRA Municipio x Banco do Pagador x Tipo pessoa Pagador #####
 ####################################################################
 
 standardQuery <- read_file('./queries/Pix_IntraMuni_Banco_PAG.sql', 
                            locale = locale(encoding = "latin1"))
 
 START_YEAR = 2022 
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
     data_ini = paste("'", data_ini, "'")
     
     data_fim = paste(toString(currentYEARseg) ,formatC(mes_seg, width=2, flag="0"), "01", sep="-")
     data_fim = paste("'", data_fim, "'")
     
     parameters = data.table(from = "@selectedDateSTART", to = data_ini)
     parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
     
     parameters = data.table(from = "@selectedDateEND", to = data_fim)
     parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
     
     data = as.data.table(extractTeradataDataSEDE(parametrizedQuery))
     data$IF_PAG = as.integer64(trunc(data$IF_PAG/1000000))
     data = merge(x = data, y = mapCNPJ8,
                  by.x = "IF_PAG", by.y = "CNPJ8",
                  all.x = T)
     
     # Check numero de nulls
     flog.info(sum(is.na (data$CNPJ8_HASH)))
     
     # Drop identified columns and keep hashed ones ----------------------------
     data$IF_PAG = data$CNPJ8_HASH
     data$CNPJ8_HASH       = NULL
     
     # Save the aggregated data for each month
     fwrite(x = data, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/PIX_INTRA_IF_PAG", 
                                    as.character(currentYEAR*100+mes), ".csv"))
     
     mes = mes + 1
     
   }
   
   currentYEAR = currentYEAR + 1 
   
 }  
 
 
 ########################################################################
 ## 4) INTRA Municipio x Banco do Recebedor x Tipo pessoa Recebedor #####
 ########################################################################
 
 standardQuery <- read_file('./queries/Pix_IntraMuni_Banco_REC.sql', 
                            locale = locale(encoding = "latin1"))
 
 START_YEAR = 2022 
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
     data_ini = paste("'", data_ini, "'")
     
     data_fim = paste(toString(currentYEARseg) ,formatC(mes_seg, width=2, flag="0"), "01", sep="-")
     data_fim = paste("'", data_fim, "'")
     
     parameters = data.table(from = "@selectedDateSTART", to = data_ini)
     parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
     
     parameters = data.table(from = "@selectedDateEND", to = data_fim)
     parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
     
     data = as.data.table(extractTeradataDataSEDE(parametrizedQuery))
     data$IF_REC = as.integer64(trunc(data$IF_REC/1000000))
     data = merge(x = data, y = mapCNPJ8,
                  by.x = "IF_REC", by.y = "CNPJ8",
                  all.x = T)
     
     # Check numero de nulls
     flog.info(sum(is.na (data$CNPJ8_HASH)))
     
     # Drop identified columns and keep hashed ones ----------------------------
     data$IF_REC = data$CNPJ8_HASH
     data$CNPJ8_HASH       = NULL
     
     # Save the aggregated data for each month
     fwrite(x = data, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/PIX_INTRA_IF_REC", 
                                    as.character(currentYEAR*100+mes), ".csv"))
     
     mes = mes + 1
     
   }
   
   currentYEAR = currentYEAR + 1 
   
 }  
 
 
####### AGORA PEGA AS QUANTIDAES DE CLIENTES UNICOS POR RECEBEDOR E PAGADOR **** 
 ####################################################################
 ## 5) INTRA Municipio x Banco do Pagador x Tipo pessoa Pagador #####
 ####################################################################
 
 standardQuery <- read_file('./queries/Pix_IntraMuni_Banco_qtd_PAG.sql', 
                            locale = locale(encoding = "latin1"))
 
 START_YEAR = 2020 
 END_YEAR   = 2022
 currentYEAR = START_YEAR
 
 
 while(currentYEAR <= END_YEAR) {
   
   mes = 1
   if (currentYEAR==2020) {
     mes = 11
   }
   
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
     data_ini = paste("'", data_ini, "'")
     
     data_fim = paste(toString(currentYEARseg) ,formatC(mes_seg, width=2, flag="0"), "01", sep="-")
     data_fim = paste("'", data_fim, "'")
     
     parameters = data.table(from = "@selectedDateSTART", to = data_ini)
     parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
     
     parameters = data.table(from = "@selectedDateEND", to = data_fim)
     parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
     
     data = as.data.table(extractTeradataDataSEDE(parametrizedQuery))
     data$IF_PAG = as.integer64(trunc(data$IF_PAG/1000000))
     data = merge(x = data, y = mapCNPJ8,
                  by.x = "IF_PAG", by.y = "CNPJ8",
                  all.x = T)
     
     # Check numero de nulls
     flog.info(sum(is.na (data$CNPJ8_HASH)))
     
     # Drop identified columns and keep hashed ones ----------------------------
     data$IF_PAG = data$CNPJ8_HASH
     data$CNPJ8_HASH       = NULL
     
     # Save the aggregated data for each month
     fwrite(x = data, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/PIX_INTRA_IF_QTD_PAG", 
                                    as.character(currentYEAR*100+mes), ".csv"))
     
     mes = mes + 1
     
   }
   
   currentYEAR = currentYEAR + 1 
   
 }  
 
 
 ########################################################################
 ## 6) INTRA Municipio x Banco do Recebedor x Tipo pessoa Recebedor #####
 #### QUANTIDADE DO RECEBEDOR
 ########################################################################
 
 standardQuery <- read_file('./queries/Pix_IntraMuni_Banco_qtd_REC.sql', 
                            locale = locale(encoding = "latin1"))
 
 START_YEAR = 2020 
 END_YEAR   = 2022
 currentYEAR = START_YEAR
 
 
 while(currentYEAR <= END_YEAR) {
   
   mes = 1
   if (currentYEAR==2020) {
    mes = 11
   }
   
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
     data_ini = paste("'", data_ini, "'")
     
     data_fim = paste(toString(currentYEARseg) ,formatC(mes_seg, width=2, flag="0"), "01", sep="-")
     data_fim = paste("'", data_fim, "'")
     
     parameters = data.table(from = "@selectedDateSTART", to = data_ini)
     parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
     
     parameters = data.table(from = "@selectedDateEND", to = data_fim)
     parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
     
     data = as.data.table(extractTeradataDataSEDE(parametrizedQuery))
     data$IF_REC = as.integer64(trunc(data$IF_REC/1000000))
     data = merge(x = data, y = mapCNPJ8,
                  by.x = "IF_REC", by.y = "CNPJ8",
                  all.x = T)
     
     # Check numero de nulls
     flog.info(sum(is.na (data$CNPJ8_HASH)))
     
     # Drop identified columns and keep hashed ones ----------------------------
     data$IF_REC = data$CNPJ8_HASH
     data$CNPJ8_HASH       = NULL
     
     # Save the aggregated data for each month
     fwrite(x = data, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/PIX_INTRA_IF_QTD_REC", 
                                    as.character(currentYEAR*100+mes), ".csv"))
     
     mes = mes + 1
     
   }
   
   currentYEAR = currentYEAR + 1 
   
 }  
 
 ## 7) PIX PARA SI MESMO
 # DB Settings
 DATA_SOURCE_NAME = "teradata-t"
 connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
 dbSendQuery(connection,"set role all")
 
 standardQuery <- read_file('./queries/PixMuniBancoSELF.sql', 
                            locale = locale(encoding = "latin1"))
 
 START_YEAR = 2021 
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
     data_ini = paste("'", data_ini, "'")
     
     data_fim = paste(toString(currentYEARseg) ,formatC(mes_seg, width=2, flag="0"), "01", sep="-")
     data_fim = paste("'", data_fim, "'")
     
     parameters = data.table(from = "@selectedDateSTART", to = data_ini)
     parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
     
     parameters = data.table(from = "@selectedDateEND", to = data_fim)
     parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
     
     data <- as.data.table(dbGetQuery(connection, parametrizedQuery))  
     
     #### ANONIMIZACAO IF PAGADORA ####
     data$IF_PAG = trunc(as.integer64(data$IF_PAG)/1000000)
     data = merge(x = data, y = mapCNPJ8,
                  by.x = "IF_PAG", by.y = "CNPJ8",
                  all.x = T)
     
     # Check numero de nulls
     flog.info(sum(is.na (data$CNPJ8_HASH)))
     
     # Drop identified columns and keep hashed ones ----------------------------
     data$IF_PAG = data$CNPJ8_HASH
     data$CNPJ8_HASH       = NULL
 
     #### ANONIMIZACAO IF RECEBEDORA ####
     data$IF_REC = trunc(as.integer64(data$IF_REC)/1000000)
     data = merge(x = data, y = mapCNPJ8,
                  by.x = "IF_REC", by.y = "CNPJ8",
                  all.x = T)
     
     # Check numero de nulls
     flog.info(sum(is.na (data$CNPJ8_HASH)))
     
     # Drop identified columns and keep hashed ones ----------------------------
     data$IF_REC = data$CNPJ8_HASH
     data$CNPJ8_HASH       = NULL     

     # Save the aggregated data for each month
     fwrite(x = data, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/PIX_SELF_MUNI_IF_", 
                                    as.character(currentYEAR*100+mes), ".csv"))
     
     mes = mes + 1
     
   }
   
   currentYEAR = currentYEAR + 1 
   
 }  
 
 
 