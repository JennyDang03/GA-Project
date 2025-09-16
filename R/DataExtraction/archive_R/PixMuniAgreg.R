################################################################################
# PIXMuniAggreg.R
# Input:  "./queries/", tipo[i], ".sql"
#         tipo = "PIX_INTRAMUNI", "PIX_MUNI_Outflow", "PIX_MUNI_Inflow", "PIXMuniRECAgg", "PIXMuniPAGAgg", "PIX_Muni_idade_Pag","PIX_Muni_idade_Rec", "PIX_Muni_Self"
# Output: "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/",tipo[i],  as.character(currentYEAR*100+mes), ".csv"
# y:  

# The goal: 

# To do: 

################################################################################

# Pix por municipio 
rm(list = ls())
options(download.file.method = "wininet")
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

# Query -------------------------------------------------------------------
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

 ################# LOOP ################
  #tipo <- c("PIX_MUNI_Outflow", "PIX_MUNI_Inflow")
  #tipo <- c("PIX_INTRAMUNI", "PIX_MUNI_Outflow", "PIX_MUNI_Inflow", "PIXMuniRECAgg", "PIXMuniPAGAgg")
  #tipo <- c("PIX_INTRAMUNI", "PIX_MUNI_Outflow", "PIX_MUNI_Inflow")
  #tipo <- c("PIX_Muni_idade_Pag","PIX_Muni_idade_Rec")
  tipo <- c("PIX_Muni_Self") 

 ## Coletar dados PIX para cada tipo acima
 for(i in 1:length(tipo)) {
     
   paste0("./queries/", tipo[i], ".sql")
   standardQuery <- read_file(paste0("./queries/", tipo[i], ".sql"), 
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
       
       flog.info("Extracting data for %s...", paste(tipo[i], currentYEAR,mes,sep="-") )
       
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
       
       #data = as.data.table(extractTeradataDataSEDE(parametrizedQuery))
       data <- as.data.table(dbGetQuery(connection, parametrizedQuery))
       
       # Save the aggregated data for each month
       if (dim(data)[1] > 0) {
       fwrite(x = data, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/",tipo[i],  
                                      as.character(currentYEAR*100+mes), ".csv"))
       }
       
       mes = mes + 1
       
     }
     
     currentYEAR = currentYEAR + 1 
     
   }  
   
 } # end of tipo loop   
 
 
 
 