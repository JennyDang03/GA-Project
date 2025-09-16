# Pix - municipios CEARA
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
mapCNPJ14 = fread("./data/hash_firms14_2022.csv")
#mapCNPJ8 = fread("./data/hash_CNPJ8_DE.csv")
mapCNPJ8 = fread("./data/hash_firms8_2022.csv")
mapCPF = fread("./data/hash_persons_DE.csv")

# Query -------------------------------------------------------------------
##Coletar dados PIX do Estado do Ceará

standardQuery <- read_file('./queries/PixMuniCE.sql', 
                           locale = locale(encoding = "latin1"))

for (tipo_recebedor in 1:2) {
for (tipo_pagador in 1:2) {  

   START_YEAR = 2020 
   END_YEAR   = 2021
   currentYEAR = START_YEAR
  
  # Loop pelos anos 
  while(currentYEAR <= END_YEAR) {
  
   mes = 1
   if (currentYEAR == 2020) {
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
  
    parameters = data.table(from = "@TIPOREC", to = tipo_recebedor)
    parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
    
    parameters = data.table(from = "@TIPOPAG", to = tipo_pagador)
    parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)    
    
    data = as.data.table(extractTeradataDataSEDE(parametrizedQuery))
  
  ################ ANONIMIZACAO IFs ###############
  ### 1) IF pagadora
  ### 2) IF recebedora
  #################################################
    
  ##########################################
  # 1) Des-identificacao dados IF pagadora  
  ##########################################
    
    data$IF_PAG = as.integer64(trunc(data$IF_PAG/1000000))
    data = merge(x = data, y = mapCNPJ8,
                    by.x = "IF_PAG", by.y = "CNPJ8",
                    all.x = T)
    
    # Check numero de nulls
    flog.info(sum(is.na (data$CNPJ8_HASH)))
    
    # Drop identified columns and keep hashed ones ----------------------------
    data$IF_PAG = data$CNPJ8_HASH
    data$CNPJ8_HASH       = NULL
  
    ###############################################
    # 2) Des-identificacao dados IF Recebedora
    ###############################################
    data$IF_REC = as.integer64(trunc(data$IF_REC/1000000))
    data = merge(x = data, y = mapCNPJ8,
                 by.x = "IF_REC", by.y = "CNPJ8",
                 all.x = T)
    
    # Check numero de nulls
    flog.info(sum(is.na(data$CNPJ8_HASH)))
    
    # Drop identified columns and keep hashed ones ----------------------------
    data$IF_REC = data$CNPJ8_HASH
    data$CNPJ8_HASH       = NULL
    
    ################ ANONIMIZACAO RECEBEDOR #############
    ################ CASO SEJA EMPRESA ##################
    if (tipo_recebedor == 2) { 
    data$PES_NU_CPF_CNPJ_RECEBEDOR = as.integer64(data$PES_NU_CPF_CNPJ_RECEBEDOR)
    data = merge(x = data, y = mapCNPJ14,
                 by.x = "PES_NU_CPF_CNPJ_RECEBEDOR", by.y = "CNPJ14",
                 all.x = T)
    
    # Check numero de nulls
    flog.info(sum(is.na (data$CNPJ14_HASH)))
    
    # Drop identified columns and keep hashed ones ----------------------------
    data$PES_NU_CPF_CNPJ_RECEBEDOR = data$CNPJ14_HASH
    data$CNPJ14_HASH       = NULL
    }
    ################ CASO SEJA PF ##################
    if (tipo_recebedor == 1) { 
      data$PES_NU_CPF_CNPJ_RECEBEDOR = as.integer64(trunc(data$PES_NU_CPF_CNPJ_RECEBEDOR/100))
      data = merge(x = data, y = mapCPF,
                   by.x = "PES_NU_CPF_CNPJ_RECEBEDOR", by.y = "cpf",
                   all.x = T)
      
      # Check numero de nulls
      flog.info(sum(is.na (data$cpf_hash)))
      
      # Drop identified columns and keep hashed ones ----------------------------
      data$PES_NU_CPF_CNPJ_RECEBEDOR = data$cpf_hash
      data$cpf_hash       = NULL
    }    
    
    
    ################ ANONIMIZACAO PAGADOR ###############
    ################ CASO SEJA EMPRESA ##################
    if (tipo_pagador == 2) { 
      data$PES_NU_CPF_CNPJ_PAGADOR = as.integer64(data$PES_NU_CPF_CNPJ_PAGADOR)
      data = merge(x = data, y = mapCNPJ14,
                   by.x = "PES_NU_CPF_CNPJ_PAGADOR", by.y = "CNPJ14",
                   all.x = T)
      
      # Check numero de nulls
      flog.info(sum(is.na (data$CNPJ14_HASH)))
      
      # Drop identified columns and keep hashed ones ----------------------------
      data$PES_NU_CPF_CNPJ_PAGADOR = data$CNPJ14_HASH
      data$CNPJ14_HASH       = NULL
    }
    ################ CASO SEJA PF ##################
    if (tipo_pagador == 1) { 
      data$PES_NU_CPF_CNPJ_PAGADOR = as.integer64(trunc(data$PES_NU_CPF_CNPJ_PAGADOR/100))
      data = merge(x = data, y = mapCPF,
                   by.x = "PES_NU_CPF_CNPJ_PAGADOR", by.y = "cpf",
                   all.x = T)
      
      # Check numero de nulls
      flog.info(sum(is.na (data$cpf_hash)))
      
      # Drop identified columns and keep hashed ones ----------------------------
      data$PES_NU_CPF_CNPJ_PAGADOR = data$cpf_hash
      data$cpf_hash       = NULL
    }  
    
    ### FIM DA ANONIMIZACAO #####
    ############################################################
    # Save the aggregated data for each month
    fwrite(x = data, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/PIX_MUNI_CE_", 
                                      as.character(currentYEAR*100+mes), "_", as.character(tipo_recebedor),"_", as.character(tipo_pagador)  ,".csv"))
   
    mes = mes + 1
  
  } # Loop pelos meses
    currentYEAR = currentYEAR + 1 
  } # Loop pelos anos 
} # Loop Tipo recebedor
} # Loop Tipo pagador


