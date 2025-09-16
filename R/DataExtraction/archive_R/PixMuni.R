################################################################################
# PIXMuniAggreg.R
# Input:  './queries/PIXMuni.sql'

# Output: "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/PIX_", as.character(currentYEAR), ".csv"

# y:  

# The goal: 

# To do: 

################################################################################

# Pix por municipio

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
source("./functions/parametrizeQuery.R")
source("./functions/extractTeradataData.R")

# Global settings ---------------------------------------------------------
# Hashmap -----------------------------------------------------------------

mapBankCNPJ = fread(input = "./data/hash_bank_cnpj.csv")
mapBankConglomerate = fread(input = "./data/hash_bank_conglomerate.csv")
mapIndividuals = fread(input = "./data/hash_persons.csv")
mapFirms = fread("./data/hash_firms.csv")
mapFirms8 = unique(mapFirms[, .(CNPJ8, CNPJ8_HASH)])
rm(mapFirms)

# Query -------------------------------------------------------------------
##Coletar dados PIX
standardQuery <- read_file('./queries/PIXMuni.sql', 
                           locale = locale(encoding = "latin1"))

 START_YEAR = 2021 
 END_YEAR   = 2021
 currentYEAR = START_YEAR


while(currentYEAR <= END_YEAR) {

 mes = 1
 
 if(exists("allData")) {
   rm(allData)
 }
 
  
while(mes <= 12) {
  
  flog.info("Extracting data for %d...", paste(currentYEAR,mes,sep="-") )
  
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


  data = as.data.table(extractTeradataData(parametrizedQuery))
  
  if(exists("allData")) {
    allData = rbindlist(l = list(allData, data), use.names = T)
  } else {
    allData = data
  }
  
  mes = mes + 1

}

 # cria campos separados para CPF e CNPJ
 allData[TIPO_REC==1, CPF_REC :=PES_NU_CPF_CNPJ_RECEBEDOR]
 allData[TIPO_REC==2, CNPJ_REC :=PES_NU_CPF_CNPJ_RECEBEDOR]
 allData[TIPO_PAG==1, CPF_PAG :=PES_NU_CPF_CNPJ_PAGADOR]
 allData[TIPO_PAG==2, CNPJ_PAG :=PES_NU_CPF_CNPJ_PAGADOR]
 
 # Hash FIRMA Recebedoras ---------------------------------------------------
  allData = merge(x = allData, y = mapFirms8,
                 by.x = "CNPJ_REC", by.y = "CNPJ8",
                 all.x = T)
  # Drop identified columns and keep hashed ones ----------------------------
 allData$CNPJ_REC  = allData$CNPJ8_HASH
 allData$CNPJ8_HASH    = NULL
 
 # Hash FIRMA Pagadoras ----------------------------------------------------
  allData = merge(x = allData, y = mapFirms8,
                 by.x = "CNPJ_PAG", by.y = "CNPJ8",
                 all.x = T)
 # Drop identified columns and keep hashed ones ----------------------------
 allData$CNPJ_PAG  = allData$CNPJ8_HASH
 allData$CNPJ8_HASH    = NULL
 
 
 
 # Hash Individuos Recebedores ---------------------------------------------
 allData$CPF_REC = as.integer64(allData$CPF_REC)
 allData = merge(allData, mapIndividuals,
                 by.x = "CPF_REC", by.y = "cpf", all.x = T)
 
 # Drop identified columns and keep hashed ones ----------------------------
 allData$CPF_REC  = allData$cpf_hash
 allData$cpf_hash     = NULL
 
 # Hash Individuos Pagadores ---------------------------------------------
 allData$CPF_PAG = as.integer64(allData$CPF_PAG)
 allData = merge(allData, mapIndividuals,
                 by.x = "CPF_PAG", by.y = "cpf", all.x = T)
 
 # Drop identified columns and keep hashed ones ----------------------------
 allData$CPF_PAG  = allData$cpf_hash
 allData$cpf_hash     = NULL
 
 
  # Save the un-identified data
  #fwrite(x = allData, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/PIX_", 
  #                                  as.character(currentYEAR), ".csv"))
  
  currentYEAR = currentYEAR + 1 

}  
  

