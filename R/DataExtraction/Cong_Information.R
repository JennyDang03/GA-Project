# Caracteristicas dos Conglomerados e IFs
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
library(gdata)
library(lubridate)
library(arrow)

setwd("//sbcdf176/PIX_Matheus$/R/DataExtraction")
source("parametrizeQuery.R")
source("extractTeradataData.R")
source("extractSQLServerData.R")


# Global settings ---------------------------------------------------------
# Hashmap -----------------------------------------------------------------
mapCNPJ8 = fread("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/data/hash_CNPJ8_DE.csv")
path_data <- "//sbcdf176/PIX_Matheus$/DadosOriginais/"

# Query -------------------------------------------------------------------
##Coletar dados das caracteristicas das IFs e seus conglomerados
## das tabelas do SCR no Teradata

standardQuery <- read_file('Cong_Info_SCR.sql', 
                           locale = locale(encoding = "latin1"))

 START_YEAR = 2019 
 END_YEAR   = 2022
 currentYEAR = START_YEAR

while(currentYEAR <= END_YEAR) {


 if(exists("allData")) {
   rm(allData)
 }
 
  timeid = currentYEAR * 100 + 12
  parameters = data.table(from = "@selectedDate", to = timeid)
  parametrizedQuery <- parametrizeQuery(standardQuery, parameters)

  data = as.data.table(extractTeradataData(parametrizedQuery))
 
  # Criar coluna com flag de digbank
  #myvar <- c('digbank')
  #data[, c(myvar):=0]
  #data[CNPJ8_IF==31872495, digbank:=1] # C6 nativo digital
  #data[CNPJ8_IF==10664513 , digbank:=1]    #Agibank
  #data[CNPJ8_IF==71027866 & (time_id>=201909), digbank:=1]    # BS2
  #data[CNPJ8_IF==416968 , digbank:=1]      # Inter
  #data[CNPJ8_IF==92894922 , digbank:=1]    # Original
  
  data$CNPJ8_IF = as.integer64(data$CNPJ8_IF)
  data = merge(x = data, y = mapCNPJ8,
                  by.x = "CNPJ8_IF", by.y = "CNPJ8",
                  all.x = T)
  
  # Check numero de nulls
  flog.info(sum(is.na (data$CNPJ8_HASH)))
  
  # Drop identified columns and keep hashed ones ----------------------------
  data$CNPJ8_IF = data$CNPJ8_HASH
  data$CNPJ8_HASH       = NULL
  
  # cria variavel binario dizendo que pertence a conglomerado
  myvar <- c('belong_cong')
  data[, c(myvar):=substr(cod_cong_prud, 1, 1) ]
  data[, belong_cong := as.numeric(str_replace(belong_cong, "C", "-1"))]
  data[belong_cong >=0 , belong_cong := 0]
  data[belong_cong <0 , belong_cong := 1]
  data[belong_cong ==0 , cod_cong_prud := NA]
  
  
  # Save the aggregated data for each month
  #fwrite(x = data, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/Cong_SCR_", 
  #                                  as.character(currentYEAR*100+12), ".csv"))

  write_parquet(data, sink = paste0(path_data, "Cong_SCR_", as.character(currentYEAR), ".parquet"))
  
  currentYEAR = currentYEAR + 1 

}  

# Pega no Unicad aqueles que não estão no SCR

 standardQuery <- read_file('Cong_Info_UNICAD.sql', 
                            locale = locale(encoding = "latin1"))
 
 START_YEAR = 2019 
 END_YEAR   = 2022
 currentYEAR = START_YEAR
 mes = 12
 
 while(currentYEAR <= END_YEAR) {
   
   
   if(exists("allData")) {
     rm(allData)
   }
   
   timeid = currentYEAR * 100 + mes
   parameters = data.table(from = "@selectedDate", to = timeid)
   parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
   
   data = as.data.table(extractSQLServerData(parametrizedQuery))
   
   # Criar coluna com flag de digbank
   #myvar <- c('digbank')
   #data[, c(myvar):=0]
   #data[CNPJ8_IF==31872495, digbank:=1] # C6 nativo digital
   #data[CNPJ8_IF==10664513 , digbank:=1]    #Agibank
   #data[CNPJ8_IF==71027866 & (time_id>=201909), digbank:=1]    # BS2
   #data[CNPJ8_IF==416968 , digbank:=1]      # Inter
   #data[CNPJ8_IF==92894922 , digbank:=1]    # Original

   data$CNPJ8_IF = as.integer64(data$CNPJ8_IF)
   data = merge(x = data, y = mapCNPJ8,
                by.x = "CNPJ8_IF", by.y = "CNPJ8",
                all.x = T)
   
   # Check numero de nulls
   flog.info(sum(is.na (data$CNPJ8_HASH)))
   
   # Drop identified columns and keep hashed ones ----------------------------
   data$CNPJ8_IF = data$CNPJ8_HASH
   data$CNPJ8_HASH       = NULL
   
   # Save the aggregated data for each month
   #fwrite(x = data, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/CaracInstUnicad_", 
#                                  as.character(currentYEAR*100+mes), ".csv"))
   
   write_parquet(data, sink = paste0(path_data, "Cong_Unicad_", as.character(currentYEAR), ".parquet"))
   
   
   
   currentYEAR = currentYEAR + 1 
   
 }  
 
 