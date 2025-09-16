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
source("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/functions/parametrizeQuery.R")
source(paste0(R_path,"/functions/stata_week_number.R"))
source(paste0(R_path,"/functions/gen_week_list.R"))
source(paste0(R_path,"/functions/gen_ano_mes_list.R"))
source(paste0(R_path,"/functions/stata_month_number.R"))

# Query -------------------------------------------------------------------

run_Pix_ind_sample <- 0 #
run_Boleto_ind_sample <- 0 # 
run_Card_ind_sample <- 0 # 

run_Credito_ind_sample <- 0 # 
run_CCS_ind_sample <- 0 # - Runs for a long time

run_Pix_ind_sample_month <- 1 #
run_TED_ind_sample_STR <- 1 #
# ------------------------------------------------------------------------------
# Pix_ind_sample
# ------------------------------------------------------------------------------
# Variables: week, id, tipo, value_send, trans_send, value_rec, trans_rec, value_self, trans_self

if(run_Pix_ind_sample == 1){
# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

tipo <- c("Pix_ind_sample_PJ")#, "Pix_ind_sample_PF")
loop_week <- gen_week_list(2020, 2022)
loop_week <- loop_week[-(1:45)]
week_number_list <- lapply(loop_week, stata_week_number)
#### 
# To Test
#loop_week <- loop_week[(1:2)]
#week_number_list <- lapply(loop_week, stata_week_number)
###
tryCatch({
for(i in 1:length(tipo)) {
  data <- data.table()
  for (j in 1:(length(loop_week) - 1)) {
    print(Sys.time())
    cat("Extracting data for", tipo[i], "...\n")
    standardQuery <- read_file(paste0(path_query, tipo[i], ".sql"), 
                               locale = locale(encoding = "latin1"))
    
    data_ini <- format(loop_week[[j]], "%Y-%m-%d")
    data_fim <- format(loop_week[[j + 1]], "%Y-%m-%d")
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

    ################ ANONIMIZACAO IDs ###############
    ### 1) Firmas
    ### 2) Pessoas fisicas
    #################################################

    ##########################################
    # 1) Des-identificacao firmas
    ##########################################
    if(tipo[i]=="Pix_ind_sample_PJ"){
      #data = data[data$tipo == 2, ]
      data$id = trunc(as.integer64(data$id)/1000000)
      data = merge(x = data, y = mapCNPJ8,
                  by.x = "id", by.y = "CNPJ8",
                  all.x = T)
      # data$id = as.integer64(data$id)
      # data = merge(x = data, y = mapCNPJ14,
      #              by.x = "id", by.y = "CNPJ14",
      #              all.x = T)
    # Check numero de nulls
    flog.info(sum(is.na (data$CNPJ8_HASH)))
    #flog.info(sum(is.na (data$CNPJ14_HASH)))

    # Drop identified columns and keep hashed ones ----------------------------
    data$id = data$CNPJ8_HASH
    data$CNPJ8_HASH       = NULL
    #data$id = data$CNPJ14_HASH
    #data$CNPJ14_HASH       = NULL
    }
    ###############################################
    # 2) Des-identificacao pessoa fisica
    ###############################################
    if(tipo[i]=="Pix_ind_sample_PF"){
    #data = data[data$tipo == 1, ]
    data$id = trunc(as.integer64(trim(data$id))/100) # IS THIS CORRECT?
    data = merge(x = data, y = mapIndividuals,
                by.x = "id", by.y = "cpf",
                all.x = T)

    # Check numero de nulls
    flog.info(sum(is.na (data$cpf_hash)))

    # Drop identified columns and keep hashed ones ----------------------------
    data$id = data$cpf_hash
    data$cpf_hash       = NULL

    ### FIM DA ANONIMIZACAO #####
    #data <- rbindlist(list(data, firmas))
    }
    ############################################################

  # Merge with old data if it exists 
  # if (file.exists(paste0(path_data, tipo[i], ".parquet"))) {
  #   # rbindlist data and old data
  #   old_data <- read_parquet(paste0(path_data, tipo[i], ".parquet"))
  #   data <- rbindlist(list(data, old_data))
  #   # Delete repeated rows
  #   data <- unique(data)
  #   # Saves old data just in case
  #   write_parquet(old_data, sink = paste0(path_data, tipo[i], "_old.parquet"))
  #   rm(old_data)
  # }
  # 
  write_parquet(data, sink = paste0(path_data, tipo[i], ".parquet"))
}  #end of Loop i through tipo[i]

}, error = function(e) {
  print(e)
  print("Error in the SQL: Pix_ind_sample")
})
}


# ------------------------------------------------------------------------------
# Boleto_ind_sample
# ------------------------------------------------------------------------------
# Variables: week, id, id_municipio_receita, tipo, value_send, trans_send, value_rec, trans_rec, value_self, trans_self

if(run_Boleto_ind_sample == 1){
# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

tipo <- c("Boleto_ind_sample_PJ")#, "Boleto_ind_sample_PF")
loop_week <- gen_week_list(2019, 2022)
#loop_week <- loop_week[-(1:43)] # Boleto starts only in the end of 2018.
week_number_list <- lapply(loop_week, stata_week_number)
#### 
# To Test
#loop_week <- loop_week[(1:2)]
#week_number_list <- lapply(loop_week, stata_week_number)
###
tryCatch({
for(i in 1:length(tipo)) {
  data <- data.table()

  for (j in 1:(length(loop_week) - 1)) {
    print(Sys.time())
    cat("Extracting data for", tipo[i], "...\n")
    standardQuery <- read_file(paste0(path_query, tipo[i], ".sql"), 
                               locale = locale(encoding = "latin1"))
    
    data_ini <- format(loop_week[[j]], "%Y-%m-%d")
    data_fim <- format(loop_week[[j + 1]], "%Y-%m-%d")
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

  ################ ANONIMIZACAO IDs ###############
  ### 1) Firmas
  ### 2) Pessoas fisicas
  #################################################
  
  ##########################################
  # 1) Des-identificacao firmas
  ##########################################
  if(tipo[i]=="Boleto_ind_sample_PJ"){
    #data = data[data$tipo == 2, ]
    data$id = trunc(as.integer64(data$id)/1000000)
    data = merge(x = data, y = mapCNPJ8,
                 by.x = "id", by.y = "CNPJ8",
                 all.x = T)
    # data$id = as.integer64(data$id)
    # data = merge(x = data, y = mapCNPJ14,
    #              by.x = "id", by.y = "CNPJ14",
    #              all.x = T)
    # Check numero de nulls
    flog.info(sum(is.na (data$CNPJ8_HASH)))
    #flog.info(sum(is.na (data$CNPJ14_HASH)))
    
    # Drop identified columns and keep hashed ones ----------------------------
    data$id = data$CNPJ8_HASH
    data$CNPJ8_HASH       = NULL
    #data$id = data$CNPJ14_HASH
    #data$CNPJ14_HASH       = NULL
  }
  ###############################################
  # 2) Des-identificacao pessoa fisica
  ###############################################
  if(tipo[i]=="Boleto_ind_sample_PF"){
    #data = data[data$tipo == 1, ]
    data$id = trunc(as.integer64(trim(data$id))/100) # IS THIS CORRECT?
    data = merge(x = data, y = mapIndividuals,
                 by.x = "id", by.y = "cpf",
                 all.x = T)
    
    # Check numero de nulls
    flog.info(sum(is.na (data$cpf_hash)))
    
    # Drop identified columns and keep hashed ones ----------------------------
    data$id = data$cpf_hash
    data$cpf_hash       = NULL
    
    ### FIM DA ANONIMIZACAO #####
    #data <- rbindlist(list(data, firmas))
  }
  ############################################################
  
  # Merge with old data if it exists 
  # if (file.exists(paste0(path_data, tipo[i], ".parquet"))) {
  #   # rbindlist data and old data
  #   old_data <- read_parquet(paste0(path_data, tipo[i], ".parquet"))
  #   data <- rbindlist(list(data, old_data))
  #   # Delete repeated rows
  #   data <- unique(data)
  #   # Saves old data just in case
  #   write_parquet(old_data, sink = paste0(path_data, tipo[i], "_old.parquet"))
  #   rm(old_data)
  # }
  
  write_parquet(data, sink = paste0(path_data, tipo[i], ".parquet"))
}  #end of Loop i through tipo[i]

}, error = function(e) {
  print(e)
  print("Error in the SQL: Boleto_ind_sample")
})
}

# ------------------------------------------------------------------------------
# Card_ind_sample
# ------------------------------------------------------------------------------
# Variables: week, id, id_municipio_receita, tipo, value_credit, trans_credit, value_debit, trans_debit

if(run_Card_ind_sample == 1){
# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

tipo <- c("Card_ind_sample_PJ")#, "Card_ind_sample_PF")
loop_week <- gen_week_list(2019, 2022)
week_number_list <- lapply(loop_week, stata_week_number)
#### 
# To Test
#loop_week <- loop_week[(1:2)]
#week_number_list <- lapply(loop_week, stata_week_number)
###
tryCatch({
for(i in 1:length(tipo)) {
  data <- data.table()
  for (j in 1:(length(loop_week) - 1)) {
    print(Sys.time())
    cat("Extracting data for", tipo[i], "...\n")
    standardQuery <- read_file(paste0(path_query, tipo[i], ".sql"), 
                               locale = locale(encoding = "latin1"))
    
    data_ini <- format(loop_week[[j]], "%Y-%m-%d")
    data_fim <- format(loop_week[[j + 1]], "%Y-%m-%d")
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
 
  ################ ANONIMIZACAO IDs ###############
  ### 1) Firmas
  ### 2) Pessoas fisicas
  #################################################
  
  ##########################################
  # 1) Des-identificacao firmas
  ##########################################
  if(tipo[i]=="Card_ind_sample_PJ"){
    #data = data[data$tipo == 2, ]
    data$id = trunc(as.integer64(data$id)/1000000)
    data = merge(x = data, y = mapCNPJ8,
                 by.x = "id", by.y = "CNPJ8",
                 all.x = T)
    # data$id = as.integer64(data$id)
    # data = merge(x = data, y = mapCNPJ14,
    #              by.x = "id", by.y = "CNPJ14",
    #              all.x = T)
    # Check numero de nulls
    flog.info(sum(is.na (data$CNPJ8_HASH)))
    #flog.info(sum(is.na (data$CNPJ14_HASH)))
    
    # Drop identified columns and keep hashed ones ----------------------------
    data$id = data$CNPJ8_HASH
    data$CNPJ8_HASH       = NULL
    #data$id = data$CNPJ14_HASH
    #data$CNPJ14_HASH       = NULL
  }
  ###############################################
  # 2) Des-identificacao pessoa fisica
  ###############################################
  if(tipo[i]=="Card_ind_sample_PF"){
    #data = data[data$tipo == 1, ]
    data$id = trunc(as.integer64(trim(data$id))/100) # IS THIS CORRECT?
    data = merge(x = data, y = mapIndividuals,
                 by.x = "id", by.y = "cpf",
                 all.x = T)
    
    # Check numero de nulls
    flog.info(sum(is.na (data$cpf_hash)))
    
    # Drop identified columns and keep hashed ones ----------------------------
    data$id = data$cpf_hash
    data$cpf_hash       = NULL
    
    ### FIM DA ANONIMIZACAO #####
    #data <- rbindlist(list(data, firmas))
  }
  ############################################################
  

  # Merge with old data if it exists 
  # if (file.exists(paste0(path_data, tipo[i], ".parquet"))) {
  #   # rbindlist data and old data
  #   old_data <- read_parquet(paste0(path_data, tipo[i], ".parquet"))
  #   data <- rbindlist(list(data, old_data))
  #   # Delete repeated rows
  #   data <- unique(data)
  #   # Saves old data just in case
  #   write_parquet(old_data, sink = paste0(path_data, tipo[i], "_old.parquet"))
  #   rm(old_data)
  # }
  
  write_parquet(data, sink = paste0(path_data, tipo[i], ".parquet"))
}  #end of Loop i through tipo[i]

}, error = function(e) {
  print(e)
  print("Error in the SQL: Card_ind_sample")
})
}

# ------------------------------------------------------------------------------
# Credito_ind_sample
# ------------------------------------------------------------------------------
# Variables: Variables: time_id, id, tipo, bank, new_users, new_users_if, new_users_cg, valor, valor_ativo, qtd

if(run_Credito_ind_sample == 1){
# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

#tipo <- c("Credito_ind_sample_PJ", "Credito_ind_sample_PF")
tipo <- c("Credito_ind_sample_PF")

loop_ano_mes <- gen_ano_mes_list(2018,2023)
month_number_list <- lapply(loop_ano_mes, stata_month_number)

###
# To Test
#loop_ano_mes <- loop_ano_mes[(1:2)]
#month_number_list <- lapply(loop_ano_mes, stata_month_number)
###
tryCatch({
for(i in 1:length(tipo)) {
  data <- data.table()
  cat("Extracting data for", tipo[i], "...\n")
  for (j in 1:(length(loop_ano_mes) - 1)) {
    print(Sys.time())
    standardQuery <- read_file(paste0(path_query, tipo[i], ".sql"), 
                               locale = locale(encoding = "latin1"))

    cat("The month starts at", loop_ano_mes[[j]], "\n")
    parameters <- data.table(from = "@selectedDate", to = as.numeric(loop_ano_mes[[j]]))
    parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
    parameters <- data.table(from = "@TIME_ID", to = as.numeric(month_number_list[[j]]))
    parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)

    # Append the new data to the existing data table
    data <- rbindlist(list(data, as.data.table(dbGetQuery(connection, parametrizedQuery)))) 
  } #end of Loop j through weeks
  # Variables: Variables: time_id, id, tipo, bank, new_users, new_users_if, new_users_cg, valor, valor_ativo, qtd

    ################ ANONIMIZACAO IDs ###############
    ### 0) Bancos
    ### 1) Firmas
    ### 2) Pessoas fisicas
    #################################################
    
    ##########################################
    # 0) Des-identificacao Bancos
    ##########################################
    #data$bank = trunc(as.integer64(data$bank)/1000000)
    data$bank = as.integer64(data$bank) #########################################################################
    data = merge(x = data, y = mapCNPJ8,
                by.x = "bank", by.y = "CNPJ8",
                all.x = T)

    # Check numero de nulls
    flog.info(sum(is.na(data$CNPJ8_HASH)))

    # Drop identified columns and keep hashed ones
    data$bank = data$CNPJ8_HASH
    data$CNPJ8_HASH       = NULL

    
    
    ##########################################
    # 1) Des-identificacao firmas
    ##########################################
    if(tipo[i]=="Credito_ind_sample_PJ"){
      data$id = as.integer64(data$id)
      data = merge(x = data, y = mapCNPJ8,
                  by.x = "id", by.y = "CNPJ8",
                  all.x = T)

    # Check numero de nulls
    flog.info(sum(is.na (data$CNPJ8_HASH)))

    # Drop identified columns and keep hashed ones 
    data$id = data$CNPJ8_HASH
    data$CNPJ8_HASH       = NULL
    }
    ###############################################
    # 2) Des-identificacao pessoa fisica
    ###############################################
    if(tipo[i]=="Credito_ind_sample_PF"){
    data$id = trunc(as.integer64(trim(data$id))/100)
    data = merge(x = data, y = mapIndividuals,
                by.x = "id", by.y = "cpf",
                all.x = T)

    # Check numero de nulls
    flog.info(sum(is.na (data$cpf_hash)))

    # Drop identified columns and keep hashed ones 
    data$id = data$cpf_hash
    data$cpf_hash       = NULL
    }
    ### FIM DA ANONIMIZACAO #####

  # Merge with old data if it exists 
  #if (file.exists(paste0(path_data, tipo[i], ".parquet"))) {
  #  # rbindlist data and old data
  #  old_data <- read_parquet(paste0(path_data, tipo[i], ".parquet"))
  #  data <- rbindlist(list(data, old_data))
  #  # Delete repeated rows
  #  data <- unique(data)
  #  # Saves old data just in case
  #  write_parquet(old_data, sink = paste0(path_data, tipo[i], "_old.parquet"))
  #  rm(old_data)
  #}
  
  write_parquet(data, sink = paste0(path_data, tipo[i], ".parquet"))
}  #end of Loop i through tipo[i]
rm(data)
}, error = function(e) {
  print(e)
  print("Error in the SQL: Credito_ind_sample")
})
}

# ------------------------------------------------------------------------------
# CCS_ind_sample
# ------------------------------------------------------------------------------
# Variables: id, tipo, bank, dia_inicio, dia_fim
# anonimize the banks!

if(run_CCS_ind_sample == 1){
# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

tipo <- c("CCS_ind_sample_PJ", "CCS_ind_sample_PF")
tryCatch({
for(i in 1:length(tipo)) {
  data <- data.table()
  print(Sys.time())
  cat("Extracting data for", tipo[i], "...\n")
  standardQuery <- read_file(paste0(path_query, tipo[i], ".sql"), 
                              locale = locale(encoding = "latin1"))
  
  # Append the new data to the existing data table
  data <- as.data.table(dbGetQuery(connection, standardQuery))

  # Variables: id, tipo, bank, dia_inicio, dia_fim

    ################ ANONIMIZACAO IDs ###############
    ### 0) Bancos
    ### 1) Firmas
    ### 2) Pessoas fisicas
    #################################################
    
    ##########################################
    # 0) Des-identificacao Bancos
    ##########################################
    #data$bank = trunc(as.integer64(data$bank)/1000000)
    data$bank = as.integer64(data$bank) ##############################################################
    data = merge(x = data, y = mapCNPJ8,
                by.x = "bank", by.y = "CNPJ8",
                all.x = T)

    # Check numero de nulls
    flog.info(sum(is.na(data$CNPJ8_HASH)))

    # Drop identified columns and keep hashed ones
    data$bank = data$CNPJ8_HASH
    data$CNPJ8_HASH       = NULL
    
    ##########################################
    # 1) Des-identificacao firmas
    ##########################################
    if(tipo[i]=="CCS_ind_sample_PJ"){
      data$id = trunc(as.integer64(data$id)/1000000)
      data = merge(x = data, y = mapCNPJ8,
                  by.x = "id", by.y = "CNPJ8",
                  all.x = T)

    # Check numero de nulls
    flog.info(sum(is.na (data$CNPJ8_HASH)))

    # Drop identified columns and keep hashed ones 
    data$id = data$CNPJ8_HASH
    data$CNPJ8_HASH       = NULL
    }
    ###############################################
    # 2) Des-identificacao pessoa fisica
    ###############################################
    if(tipo[i]=="CCS_ind_sample_PF"){
    data$id = trunc(as.integer64(trim(data$id))/100)
    data = merge(x = data, y = mapIndividuals,
                by.x = "id", by.y = "cpf",
                all.x = T)

    # Check numero de nulls
    flog.info(sum(is.na (data$cpf_hash)))

    # Drop identified columns and keep hashed ones 
    data$id = data$cpf_hash
    data$cpf_hash       = NULL
    }
    ### FIM DA ANONIMIZACAO #####

  write_parquet(data, sink = paste0(path_data, tipo[i], ".parquet"))
}  #end of Loop i through tipo[i]

}, error = function(e) {
  print(e)
  print("Error in the SQL: CCS_ind_sample")
})
}


################# BANKS ################
# FOR EACH BANK, ACCOUNTS, CREDITO, USE OF PIX, BOLETO, TED, CARD. 

# Pix, boleto, ted, card, credito, ccs


# ------------------------------------------------------------------------------
# Pix_ind_sample_month
# ------------------------------------------------------------------------------
# Variables: Variables: time_id, id, tipo, value_send, trans_send, value_rec, trans_rec, value_self, trans_self

if(run_Pix_ind_sample_month == 1){
# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

tipo <- c("Pix_ind_sample_month_PJ", "Pix_ind_sample_month_PF")
loop_ano_mes <- gen_ano_mes_list(2018,2023)
month_number_list <- lapply(loop_ano_mes, stata_month_number)

###
# To Test
#loop_ano_mes <- loop_ano_mes[(1:2)]
#month_number_list <- lapply(loop_ano_mes, stata_month_number)
###
tryCatch({
for(i in 1:length(tipo)) {
  data <- data.table()
  cat("Extracting data for", tipo[i], "...\n")
  for (j in 1:(length(loop_ano_mes) - 1)) {
    print(Sys.time())
    standardQuery <- read_file(paste0(path_query, tipo[i], ".sql"), 
                               locale = locale(encoding = "latin1"))

    cat("The month starts at", loop_ano_mes[[j]], "\n")
    
    data_ini <- as.Date(paste0(substr(loop_ano_mes[[j]], 1, 4), "-", substr(loop_ano_mes[[j]], 5, 6), "-01"))
    data_ini <- format(data_ini, "%Y-%m-%d")
    data_fim <- as.Date(paste0(substr(loop_ano_mes[[j + 1]], 1, 4), "-", substr(loop_ano_mes[[j + 1]], 5, 6), "-01"))
    data_fim <- format(data_fim, "%Y-%m-%d")
    
    cat("The month starts at", data_ini, "and ends at", data_fim, "\n")
    
    parameters = data.table(from = "@selectedDateSTART", to = data_ini)
    parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
    parameters = data.table(from = "@selectedDateEND", to = data_fim)
    parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
    parameters = data.table(from = "@TIME_ID", to = as.numeric(month_number_list[[j]]))
    parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
    
    # Append the new data to the existing data table
    data <- rbindlist(list(data, as.data.table(dbGetQuery(connection, parametrizedQuery)))) 

  } #end of Loop j through months
  # Variables: Variables: time_id, id, id_municipio_receita

  
  ################ ANONIMIZACAO IDs ###############
  ### 1) Firmas
  ### 2) Pessoas fisicas
  #################################################
  
  ##########################################
  # 1) Des-identificacao firmas
  ##########################################
  if(tipo[i]=="Pix_ind_sample_month_PJ"){
    #data = data[data$tipo == 2, ]
    data$id = trunc(as.integer64(data$id)/1000000)
    data = merge(x = data, y = mapCNPJ8,
                 by.x = "id", by.y = "CNPJ8",
                 all.x = T)

    # Check numero de nulls
    flog.info(sum(is.na (data$CNPJ8_HASH)))
    #flog.info(sum(is.na (data$CNPJ14_HASH)))
    
    # Drop identified columns and keep hashed ones
    data$id = data$CNPJ8_HASH
    data$CNPJ8_HASH       = NULL
  }
  ###############################################
  # 2) Des-identificacao pessoa fisica
  ###############################################
  if(tipo[i]=="Pix_ind_sample_month_PF"){
    data$id = trunc(as.integer64(trim(data$id))/100) # IS THIS CORRECT?
    data = merge(x = data, y = mapIndividuals,
                 by.x = "id", by.y = "cpf",
                 all.x = T)
    
    # Check numero de nulls
    flog.info(sum(is.na (data$cpf_hash)))
    
    # Drop identified columns and keep hashed ones
    data$id = data$cpf_hash
    data$cpf_hash       = NULL
  }
  ############################################################
  
  write_parquet(data, sink = paste0(path_data, tipo[i], ".parquet"))
}  #end of Loop i through tipo[i]
rm(data)
}, error = function(e) {
  print(e)
  print("Error in the SQL: Pix_ind_sample_month")
})
}
  
# ------------------------------------------------------------------------------



# ------------------------------------------------------------------------------
# TED_ind_sample_STR
# ------------------------------------------------------------------------------

# Variables: week, id, tipo, value_send, trans_send, value_rec, trans_rec, value_self, trans_self

if(run_TED_ind_sample_STR == 1){
  # DB Settings
  #DATA_SOURCE_NAME = "TED"
  #connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
  DATA_SOURCE_NAME = "teradata-t"
  connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
  dbSendQuery(connection,"set role all")
  
  tipo <- c("TED_ind_sample_STR_PJ")
  loop_week <- gen_week_list(2019, 2022)
  week_number_list <- lapply(loop_week, stata_week_number)
  #### 
  # To Test
  #loop_week <- loop_week[(1:2)]
  #week_number_list <- lapply(loop_week, stata_week_number)
  ###
  tryCatch({
    for(i in 1:length(tipo)) {
      data <- data.table()
      for (j in 1:(length(loop_week) - 1)) {
        print(Sys.time())
        cat("Extracting data for", tipo[i], "...\n")
        standardQuery <- read_file(paste0(path_query, tipo[i], ".sql"), 
                                   locale = locale(encoding = "latin1"))
        
        data_ini <- format(loop_week[[j]], "%Y-%m-%d")
        data_fim <- format(loop_week[[j + 1]], "%Y-%m-%d")
        cat("The week starts at", data_ini, "and ends at", data_fim, "\n")
        
        parameters = data.table(from = "@selectedDateSTART", to = data_ini)
        parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
        parameters = data.table(from = "@selectedDateEND", to = data_fim)
        parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
        parameters = data.table(from = "@WEEK", to = week_number_list[j])
        parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
        
        #data <- rbindlist(list(data, as.data.table(dbGetQuery(connection, parametrizedQuery)))) 
        
        data <- as.data.table(dbGetQuery(connection, parametrizedQuery))
        
        
        ################ ANONIMIZACAO IDs ###############
        ### 1) Firmas
        ### 2) Pessoas fisicas
        #################################################
        
        ##########################################
        # 1) Des-identificacao firmas
        ##########################################
        if(tipo[i]=="TED_ind_sample_STR_PJ"){
          data$id = trunc(as.integer64(data$id)/1000000)
          data = merge(x = data, y = mapCNPJ8,
                       by.x = "id", by.y = "CNPJ8",
                       all.x = T)
          # Check numero de nulls
          flog.info(sum(is.na (data$CNPJ8_HASH)))
          # Drop identified columns and keep hashed ones
          data$id = data$CNPJ8_HASH
          data$CNPJ8_HASH       = NULL
        }
        ###############################################
        # 2) Des-identificacao pessoa fisica
        ###############################################
        if(tipo[i]=="TED_ind_sample_STR_PF"){
          data$id = trunc(as.integer64(trim(data$id))/100)
          data = merge(x = data, y = mapIndividuals,
                       by.x = "id", by.y = "cpf",
                       all.x = T)
          # Check numero de nulls
          flog.info(sum(is.na (data$cpf_hash)))
          # Drop identified columns and keep hashed ones
          data$id = data$cpf_hash
          data$cpf_hash       = NULL
        }
        ### FIM DA ANONIMIZACAO #####
        ############################################################
        
        
        
        write_parquet(data, sink = paste0(path_data, tipo[i], "_teradata_", format(loop_week[[j]], "%Y-%m-%d"), ".parquet"))
      } 
    } 
  }, error = function(e) {
    print(e)
    print("Error in the SQL: TED_ind_sample_STR")
  })
  
  tipo <- c("TED_ind_sample_STR_PJ")
  loop_week <- gen_week_list(2019, 2022)
  week_number_list <- lapply(loop_week, stata_week_number)
  for(i in 1:length(tipo)) {
    data <- data.table()
    for (j in (length(loop_week) - 1):1) {
      data_week <- read_parquet(paste0(path_data, tipo[i], "_teradata_", format(loop_week[[j]], "%Y-%m-%d"), ".parquet"))
      data <- rbindlist(list(data, data_week))
    }
    write_parquet(data, sink = paste0(path_data, tipo[i], ".parquet"))
  }
}

