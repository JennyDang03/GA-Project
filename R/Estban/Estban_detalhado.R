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
mapCNPJ8 = fread("./data/hash_firms8_2022.csv")

# Query -------------------------------------------------------------------
##Coletar dados de crédito
standardQuery <- read_file('./queries/Estban_detalhado.sql', 
                           locale = locale(encoding = "latin1"))

 START_YEAR = 2019 
 END_YEAR   = 2022
 currentYEAR = START_YEAR

 if(exists("allData")) {
   rm(allData)
 }
 
 
while(currentYEAR <= END_YEAR) {

 START_DATE = currentYEAR * 100 + 1 
 END_DATE   = currentYEAR * 100 + 12
 currentDate = START_DATE 
 
 
while(currentDate <= END_DATE) {
  
  flog.info("Extracting data for %d...", currentDate)
  parameters = data.table(from = "@selectedDate", to = currentDate)
  
  parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
  
  data = as.data.table(extractTeradataData(parametrizedQuery))
  
  if(exists("allData")) {
    allData = rbindlist(l = list(allData, data), use.names = T)
  } else {
    allData = data
  }
  
  currentDate = ifelse(test = currentDate %% 100 >= 12, 
                       yes = (round(currentDate / 100) + 1) * 100 + 1, 
                       no = currentDate + 1)  


}
 
  currentYEAR = currentYEAR + 1 

}  

 
 # Hash banks --------------------------------------------------------------
 allData$CNPJ = as.integer64( allData$CNPJ )
 allData = merge(x = allData, y = mapCNPJ8,
              by.x = "CNPJ", by.y = "CNPJ8",
              all.x = T)
 
 # Check numero de nulls
 flog.info(sum(is.na (allData$CNPJ8_HASH)))
 
 # Drop identified columns and keep hashed ones ----------------------------
 allData$CNPJ = allData$CNPJ8_HASH
 allData$CNPJ8_HASH       = NULL


 # Save all data anonimized
 fwrite(x = allData, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/ESTBAN_DETA.csv"))


