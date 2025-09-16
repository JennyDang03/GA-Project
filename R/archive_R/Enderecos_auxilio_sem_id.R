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
source("./functions/extractTeradataData.R")

#######################################
############### PF ####################
#######################################

# Query -------------------------------------------------------------------
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

##Coletar dados de credito

#standardQuery <- read_file('./queries/Todos_enderecos_aux_emerg.sql', locale = locale(encoding = "latin1"))
standardQuery <- read_file('./queries/Todos_enderecos_aux_emerg_amostra_muni.sql', locale = locale(encoding = "latin1"))

data <- as.data.table(dbGetQuery(connection, standardQuery))

# Create index:
data$index <- seq.int(nrow(data))

# Salva dados 
fwrite(x = data, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/Todos_enderecos_aux_emerg_amostra_muni.csv"))



 
 