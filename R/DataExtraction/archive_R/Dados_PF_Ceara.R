# Cadastro Receita - municipios CEARA
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
mapCPF = fread("./data/hash_persons_DE.csv")

# Query -------------------------------------------------------------------
##Coletar dados da Receita de pessoas fisicas residentes no Estado do Ceará

standardQuery <- read_file('./queries/Dados_PF_Ceara.sql', 
                           locale = locale(encoding = "latin1"))

data = as.data.table(extractTeradataDataSEDE(standardQuery))

# ANONIMIZACAO CPF
data$id = as.integer64(data$id)
data = merge(x = data, y = mapCPF,
             by.x = "id", by.y = "cpf",
             all.x = T)
# Check numero de nulls
flog.info(sum(is.na (data$cpf_hash)))

# Drop identified columns and keep hashed ones ----------------------------
data$id = data$cpf_hash
data$cpf_hash       = NULL      

fwrite(x = data, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/Dados_PF_CE.csv"))





