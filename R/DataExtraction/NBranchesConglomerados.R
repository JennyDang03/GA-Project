# Caracteristicas dos Conglomerados e IFs
options(download.file.method = "wininet")

rm(list = ls())
library(readr)
library(stringr)
library(dplyr)
library(tidyr)
library(futile.logger)
library(data.table)
library("arrow")
library(readxl)
path_data <- "//sbcdf176/PIX_Matheus$/DadosOriginais/"

# Global settings ---------------------------------------------------------
# Hashmap -----------------------------------------------------------------
mapCNPJ8 = fread("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/data/hash_CNPJ8_DE.csv")

data <- read_excel("//sbcdf176/PIX_Matheus$/DadosOriginais/bank - number of  branches.xlsx", 
                                      sheet = "branches", range = "A1:B522")

data = merge(x = data, y = mapCNPJ8,
                  by.x = "cnpj", by.y = "CNPJ8",
                  all.x = T)
  
  # Check numero de nulls
  flog.info(sum(is.na (data$CNPJ8_HASH)))
  
  # Drop identified columns and keep hashed ones ----------------------------
  data$cnpj = data$CNPJ8_HASH
  data$CNPJ8_HASH       = NULL
  
   # Save unidentified data
   write_parquet(data, sink = paste0(path_data, "NBranchesCongLevel", ".parquet"))
   #temp <- read_parquet(paste0(path_data, "NBranchesCongLevel", ".parquet"), as_tibble = TRUE)
   

 