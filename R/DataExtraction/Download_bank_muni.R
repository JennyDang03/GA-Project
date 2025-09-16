#Download_bank_muni.R

################################################################################
options(download.file.method = "wininet")
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
library("arrow")
library(stargazer)
library(lubridate)
library(fixest)
library(ggplot2)

path_main <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/"
path_main <- "//sbcdf176/PIX_Matheus$/"

path_query <- paste0(path_main, "R/DataExtraction/")
path_data <- paste0(path_main, "DadosOriginais/")
path_dta <- paste0(path_main, "Stata/dta/")
path_output <- paste0(path_main, "Output/")
log_path <- paste0(path_main, "Stata/log/")
dta_path <- paste0(path_main, "Stata/dta/")
output_path <- paste0(path_main, "Output/")
origdata_path <- paste0(path_main, "DadosOriginais/")
R_path <- paste0(path_main, "R/")

# Global settings ---------------------------------------------------------
setwd("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer")

# Hashmap -----------------------------------------------------------------
mapCNPJ8 = fread("./data/hash_CNPJ8_DE.csv")
mapIndividuals = fread("./data/hash_persons_DE.csv")
#mapCNPJ8 = fread("./data/hash_firms8_2022.csv") # Old version
#mapCNPJ14 = fread("./data/hash_firms14_2022.csv") # we are not going to use 14 cnpj (establishment), we will use 8 cnpj (firm)

# Whats the muni list?
mun_convert <- read_dta(paste0(path_dta, "municipios.dta"))
mun_convert <- mun_convert %>%
  select(MUN_CD, MUN_CD_CADMU, MUN_CD_IBGE, MUN_NM, MUN_NM_NAO_FORMATADO) %>%
  rename(muni_cd = MUN_CD_CADMU,
         id_municipio = MUN_CD_IBGE,
         id_municipio_receita = MUN_CD,
         muni_nm = MUN_NM,
         muni_nm_nao_formatado = MUN_NM_NAO_FORMATADO)
mun_convert <- data.table(mun_convert)
mun_convert <- mun_convert %>%
  mutate(id_municipio = as.integer(id_municipio),
         muni_cd = as.integer(muni_cd),
         id_municipio_receita = as.integer(id_municipio_receita))
any(duplicated(mun_convert$id_municipio) | duplicated(mun_convert$muni_cd) | duplicated(mun_convert$id_municipio_receita))
setDT(mun_convert)
temp <- read_dta(file.path(dta_path, "mun_fe.dta"))
temp <- temp %>% select(muni_cd,pop2022)
mun_convert <- merge(mun_convert, temp, by= c("muni_cd"), all.x = TRUE, all.y = TRUE)
setorder(mun_convert, pop2022)
# Functions -------------------------------------------------------------------
source("./functions/parametrizeQuery.R")
source(paste0(R_path,"/functions/stata_week_number.R"))
source(paste0(R_path,"/functions/gen_week_list.R"))
source(paste0(R_path,"/functions/gen_ano_mes_list.R"))
source(paste0(R_path,"/functions/stata_month_number.R"))

# Query -------------------------------------------------------------------
run_pix_bank_muni <- 0 # 
run_TED_bank_muni <- 0 #worked




# ------------------------------------------------------------------------------
# Pix_Muni_Bank_users - Done! Checked!
# ------------------------------------------------------------------------------
# Variables: week, muni_cd, tipo, bank, value_send, trans_send, send_users, value_send_w, 
#                                       value_rec, trans_rec, rec_users, value_rec_w

# When we separate by type of bank, we are doing a sum or a an average of users? because, that differs if the number of banks grow. But that is not so concerning.
# I think, what I did to get users was to sum rec with send. (same for transactions and value) -> it is fine. 

if(run_Pix_Muni_Bank == 1){
# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

tipo <- c("Pix_Muni_Bank_rec", "Pix_Muni_Bank_send", "Pix_Muni_Bank_self_rec","Pix_Muni_Bank_self_send")
loop_week <- gen_week_list(2023, 2023)
week_number_list <- lapply(loop_week, stata_week_number)
#### 

for(i in 1:length(tipo)) {
  data <- data.table()
  for (j in 1:(length(loop_week) - 1)) {
    
    print(Sys.time())
    cat("Extracting data for", tipo[i], "...\n")
    standardQuery <- read_file(paste0(path_query, tipo[i], ".sql"), 
                               locale = locale(encoding = "latin1"))
    
    data_ini <- format(loop_week[[j]], "%Y-%m-%d")
    data_fim <- format(loop_week[[j + 1]], "%Y-%m-%d")
    data_ini <- paste("'", data_ini, "'")
    data_fim <- paste("'", data_fim, "'")
    cat("The week starts at", data_ini, "and ends at", data_fim, "\n")
    
    parameters = data.table(from = "@selectedDateSTART", to = data_ini)
    parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
    parameters = data.table(from = "@selectedDateEND", to = data_fim)
    parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
    parameters = data.table(from = "@WEEK", to = week_number_list[j])
    parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
    
    # Append the new data to the existing data table
    data <- rbindlist(list(data, as.data.table(dbGetQuery(connection, parametrizedQuery)))) 
  } #end of Loop j through weeks
  
  # Variables: week, muni_cd, tipo, bank, value_send, trans_send, send_users, value_send_w, value_rec, trans_rec, rec_users, value_rec_w
  
  # Deletes those with muni_cd < 0
  data <- data[data$muni_cd >= 0, ]
  
  ################ BANK ANONIMIZATION ###############
  
  data$bank = trunc(as.integer64(data$bank)/1000000)
  
  data = merge(x = data, y = mapCNPJ8,
               by.x = "bank", by.y = "CNPJ8",
               all.x = T)
  
  # Check numero de nulls
  flog.info(sum(is.na (data$CNPJ8_HASH)))
  
  # Drop identified columns and keep hashed ones ----------------------------
  data$bank = data$CNPJ8_HASH
  data$CNPJ8_HASH       = NULL
  
  ### FIM DA ANONIMIZACAO #####
  
  # Merge with old data if it exists 
  if (file.exists(paste0(path_data, tipo[i], ".parquet"))) {
    # rbindlist data and old data
    old_data <- read_parquet(paste0(path_data, tipo[i], ".parquet"))
    data <- rbindlist(list(data, old_data))
    # Delete repeated rows
    data <- unique(data)
    # Saves old data just in case
    write_parquet(old_data, sink = paste0(path_data, tipo[i], "_old.parquet"))
    rm(old_data)
  }
  write_parquet(data, sink = paste0(path_data, tipo[i], ".parquet"))

} #end of Loop i through tipo[i]
rm(data)
}