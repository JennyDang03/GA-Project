# CCS por municipio x IF x Semana
# Estoque semanal de contas abertas por municipio x instituicao

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
if(!require(optiRum)){install.packages("optiRum")}
library(optiRum)
setwd("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer")
source("./functions/parametrizeQuery.R")
source("./functions/extractTeradataDataSEDE.R")

# Global settings ---------------------------------------------------------
# Hashmap -----------------------------------------------------------------
mapCNPJ8 = fread("./data/hash_firms8_2022.csv")

# Query -------------------------------------------------------------------

 ################# LOOP ################

    #tipo <- c("CCS_Muni_IF_PF_estoque")
    tipo <- c("CCS_Muni_IF_PJ_estoque","CCS_Muni_IF_PF_estoque")

 ## Coletar dados CCS para cada tipo acima

 for(j in 1:length(tipo)) {
     
   paste0("./queries/", tipo[j], ".sql")
   standardQuery <- read_file(paste0("./queries/", tipo[j], ".sql"), 
                              locale = locale(encoding = "latin1"))
   
   WEEKS = seq(mdy("1/1/2018"), mdy("7/31/2022"), 7)
    
 
   for(i in 142:length(WEEKS)  ) {         # Head of for-loop

     if(exists("allData")) {
       rm(allData)
     }
       
       flog.info("Extracting data for %s...", paste(WEEKS[i],sep="-") )
       
       parameters = data.table(from = "@selectedDate", to = WEEKS[i] )
       parametrizedQuery <- parametrizeQuery(standardQuery, parameters)

       data = as.data.table(extractTeradataDataSEDE(parametrizedQuery))
 
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
       if (dim(data)[1] > 0) {
       fwrite(x = data, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/", 
                                      tipo[j], i, ".csv"))
       }
  


   }  # End loop por semana WEEK
   
 } # end of tipo loop   
 
 
 
 