# PIX Por individuo (PJ e PJ) e mes
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
if(!require(gdata)){install.packages("gdata")}

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
library(gdata)

setwd("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer")
source("./functions/parametrizeQuery.R")
source("./functions/extractTeradataDataSEDE.R")

# Global settings ---------------------------------------------------------
# Hashmap -----------------------------------------------------------------
mapCNPJ8 = fread("./data/hash_CNPJ8_DE.csv")
mapIndividuals = fread("./data/hash_persons_DE.csv")

# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

### 1)  POR PAGADOR PIX E MES
### 2)  POR RECEBEDOR PIX E MES

# Query -------------------------------------------------------------------
### 1)  POR PAGADOR PIX E MES
##Coletar dados PIX
standardQuery <- read_file('./queries/Pix_mes_por_ind_pag.sql', 
                           locale = locale(encoding = "latin1"))

 START_YEAR = 2020
 END_YEAR   = 2020
 currentYEAR = START_YEAR

while(currentYEAR <= END_YEAR) {

 mes = 11
 
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

  ################ ANONIMIZACAO IDs ###############
  ### 1) Firmas
  ### 2) Pessoas fisicas
  #################################################
  
  ##########################################
  # 1) Des-identificacao firmas  
  ##########################################
  
  firmas = data[data$tipo_pessoa == 2, ] 
  firmas$id = trunc(as.integer64(firmas$id)/1000000)
  firmas = merge(x = firmas, y = mapCNPJ8,
                 by.x = "id", by.y = "CNPJ8",
                 all.x = T)
  
  # Check numero de nulls
  flog.info(sum(is.na (firmas$CNPJ8_HASH)))
  
  # Drop identified columns and keep hashed ones ----------------------------
  firmas$id = firmas$CNPJ8_HASH
  firmas$CNPJ8_HASH       = NULL
  
  ###############################################
  # 2) Des-identificacao pessoa fisica
  ###############################################
  data = data[data$tipo_pessoa == 1, ] 
  data$id = trunc(as.integer64(trim(data$id))/100)
  data = merge(x = data, y = mapIndividuals,
               by.x = "id", by.y = "cpf",
               all.x = T)
  
  # Check numero de nulls
  flog.info(sum(is.na (data$cpf_hash)))
  
  # Drop identified columns and keep hashed ones ----------------------------
  data$id = data$cpf_hash
  data$cpf_hash       = NULL
  
  ### FIM DA ANONIMIZACAO #####
  ############################################################
  
  data <- rbindlist(list(data, firmas)) 
  
  # Save the un-identified data for each month
  fwrite(x = data, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/PIX_MES_PAG_", 
                                    as.character(currentYEAR*100+mes), ".csv"))

  mes = mes + 1

}

  currentYEAR = currentYEAR + 1 

}  


 ### 2)  POR RECEBEDOR PIX E MES
 
 standardQuery <- read_file('./queries/Pix_mes_por_ind_rec.sql', 
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
     #data_ini = paste("'", data_ini, "'")
     
     data_fim = paste(toString(currentYEARseg) ,formatC(mes_seg, width=2, flag="0"), "01", sep="-")
     
     parameters = data.table(from = "@selectedDateSTART", to = data_ini)
     parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
     
     parameters = data.table(from = "@selectedDateEND", to = data_fim)
     parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
     
     data <- as.data.table(dbGetQuery(connection, parametrizedQuery))  
     
     ################ ANONIMIZACAO IDs ###############
     ### 1) Firmas
     ### 2) Pessoas fisicas
     #################################################
     
     ##########################################
     # 1) Des-identificacao firmas  
     ##########################################
     
     firmas = data[data$tipo_pessoa == 2, ] 
     firmas$id = trunc(as.integer64(firmas$id)/1000000)
     firmas = merge(x = firmas, y = mapCNPJ8,
                    by.x = "id", by.y = "CNPJ8",
                    all.x = T)
     
     # Check numero de nulls
     flog.info(sum(is.na (firmas$CNPJ8_HASH)))
     
     # Drop identified columns and keep hashed ones ----------------------------
     firmas$id = firmas$CNPJ8_HASH
     firmas$CNPJ8_HASH       = NULL
     
     ###############################################
     # 2) Des-identificacao pessoa fisica
     ###############################################
     data = data[data$tipo_pessoa == 1, ] 
     data$id = trunc(as.integer64(trim(data$id))/100)
     data = merge(x = data, y = mapIndividuals,
                  by.x = "id", by.y = "cpf",
                  all.x = T)
     
     # Check numero de nulls
     flog.info(sum(is.na (data$cpf_hash)))
     
     # Drop identified columns and keep hashed ones ----------------------------
     data$id = data$cpf_hash
     data$cpf_hash       = NULL
     
     ### FIM DA ANONIMIZACAO #####
     ############################################################
     
     data <- rbindlist(list(data, firmas)) 
     
     # Save the un-identified data for each month
     fwrite(x = data, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/PIX_MES_REC_", 
                                    as.character(currentYEAR*100+mes), ".csv"))
     
     mes = mes + 1
     
   }
   
   currentYEAR = currentYEAR + 1 
   
 }  
 
 
 
 
 dbDisconnect(connection)
 
 
 
 
