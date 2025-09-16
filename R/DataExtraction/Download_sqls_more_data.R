# Once we download data from a time period and we want to add more data
# Download_sqls_more_data.R

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
run_Pix_Muni_user_month <- 1

run_Pix_Muni_flow <- 0 #worked
run_Pix_Muni_Bank <- 0 #worked
run_CCS_Muni_IF <- 0 #worked
run_CCS_first_account <- 0 #worked
run_Pix_Muni_user <- 0 #worked
run_CCS_Muni_stock <- 0 #worked
run_Card_rec <- 0 #worked
run_Boleto <- 0 # worked
run_Credito <- 0 # worked, took a long time. 

run_Card_adoption <- 0 # Worked, it was fast. do it all
run_Boleto_adoption <- 0 # Exclude dead people, exclude dead firms. Seems to work but take hours and needs to be broken down. 
run_Pix_adoption <- 0 # Seems to work but take hours and needs to be broken down. 

################# MUNICIPAL ################
# ------------------------------------------------------------------------------
# Pix_Muni_user_month
# ------------------------------------------------------------------------------

if(run_Pix_Muni_user_month == 1){
# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

# Variables: time_id, muni_cd, tipo, senders, receivers, users

tipo <- c("Pix_Muni_user_month")

loop_ano_mes <- gen_ano_mes_list(2020, 2023)
loop_ano_mes <- loop_ano_mes[-(1:10)]
month_number_list <- lapply(loop_ano_mes, stata_month_number)
tryCatch({
  for(i in 1:length(tipo)) {
    data <- data.table()
    for (j in 1:(length(loop_ano_mes) - 1)) {
      print(Sys.time())
      cat("Extracting data for", tipo[i], "...\n")
      standardQuery <- read_file(paste0(path_query, tipo[i], ".sql"), 
                                 locale = locale(encoding = "latin1"))

      data_ini <- as.Date(paste0(loop_ano_mes[[j]], "01"), format = "%Y%m%d")
      data_fim <- as.Date(paste0(loop_ano_mes[[j+1]], "01"), format = "%Y%m%d")
      data_ini <- paste("'", data_ini, "'")
      data_fim <- paste("'", data_fim, "'")
      cat("The month starts at", data_ini, "and ends at", data_fim, "\n")
      
      parameters = data.table(from = "@selectedDateSTART", to = data_ini)
      parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
      parameters = data.table(from = "@selectedDateEND", to = data_fim)
      parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
      parameters = data.table(from = "@TIME_ID", to = as.numeric(month_number_list[[j]]))
      parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)

      # Append the new data to the existing data table
      data <- rbindlist(list(data, as.data.table(dbGetQuery(connection, parametrizedQuery)))) 
    } #end of Loop j through weeks
    
    # Deletes those with muni_cd < 0
    data <- data[data$muni_cd >= 0, ]
    
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
    
  }  #end of Loop i through tipo[i]
  rm(data)
}, error = function(e) {
  print(e)
  print("Error in the SQL: Pix_Muni_user_month")
})
}

# ------------------------------------------------------------------------------
# Credito
# ------------------------------------------------------------------------------

# Add limite de credito!

# Idea, do Credito_Banks. For each municipality, see if types of banks grew their number of new operations.

# PF Variables: time_id, id_municipio, new_users, new_users_if, new_users_cg, valor, valor_ativo, users, qtd, banks, valor_w, users_w, qtd_w, valor_cartao, users_cartao, qtd_cartao
# PJ Variables: time_id, id_municipio, new_users, new_users_if, new_users_cg, valor, valor_ativo, users, qtd, banks, valor_cartao, users_cartao, qtd_cartao


if(run_Credito == 1){
  # DB Settings
  DATA_SOURCE_NAME = "teradata-t"
  connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
  dbSendQuery(connection,"set role all")
  
  tipo <- c("Credito_Muni_PF", "Credito_Muni_PJ")
  loop_ano_mes <- gen_ano_mes_list(2019,2022)
  month_number_list <- lapply(loop_ano_mes, stata_month_number)
  
  #loop_week <- gen_week_list(2019,2022)
  #week_number_list <- lapply(loop_week, stata_week_number)

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
        parameters = data.table(from = "@TIME_ID", to = as.numeric(month_number_list[[j]]))
        parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
        
        # Append the new data to the existing data table
        data <- rbindlist(list(data, as.data.table(dbGetQuery(connection, parametrizedQuery))))
      }
      # Deletes those with id_municipio < 0
      data <- data[data$id_municipio >= 0, ]
      
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
    }
    rm(data)
  }, error = function(e) {
    print(e)
    print("Error in the SQL: Credito")
  })
}

# Add limite de credito! The problem is that I can only download new people. so, it is too hard to download it. 


# ------------------------------------------------------------------------------
# Pix_Muni_flow - Done! Checked!
# ------------------------------------------------------------------------------
# Variables: flow_code, week, muni_cd, sender_type, receiver_type, senders, receivers, valor, trans, valor_w

if(run_Pix_Muni_flow == 1){
# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

tipo <- c("Pix_Muni_self", "Pix_Muni_inflow","Pix_Muni_outflow","Pix_Muni_intra")
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
  # Deletes those with muni_cd < 0
  data <- data[data$muni_cd >= 0, ]
  
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
}  #end of Loop i through tipo[i]
rm(data)
}

# ------------------------------------------------------------------------------
# Pix_Muni_Bank - Done! Checked!
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

# ------------------------------------------------------------------------------ 
# CCS_Muni_IF - Done! Checked!
# ------------------------------------------------------------------------------
# Variables: week, id_municipio_receita, tipo, bank, stock

if(run_CCS_Muni_IF == 1){
# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

#tipo <- c("CCS_Muni_IF_opening_PF", "CCS_Muni_IF_opening_PJ", "CCS_Muni_IF_closing_PF", "CCS_Muni_IF_closing_PJ", "CCS_Muni_IF_stock_PF", "CCS_Muni_IF_stock_PJ")
tipo <- c("CCS_Muni_IF_stock_PF", "CCS_Muni_IF_stock_PJ")

loop_week <- gen_week_list(2018, 2023)
week_number_list <- lapply(loop_week, stata_week_number)

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
    
    # Note that I am getting the number of accounts by the end of the week
    parameters = data.table(from = "@selectedDateSTART", to = data_ini)
    parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
    parameters = data.table(from = "@selectedDateEND", to = data_fim)
    parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
    parameters = data.table(from = "@WEEK", to = week_number_list[j])
    parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
    
    # Append the new data to the existing data table
    data <- rbindlist(list(data, as.data.table(dbGetQuery(connection, parametrizedQuery))))
  }#end of Loop j through weeks
  
  ### Now, lets clean it. ### 
  # Variables: week, id_municipio_receita, tipo, bank, opening, stock, closing
  # Variables: week, id_municipio_receita, tipo, bank, stock
  # Deletes those with id_municipio_receita < 0
  data <- data[data$id_municipio_receita >= 0, ]
  
  ################ BANK ANONIMIZATION ###############
  
  #data$bank = trunc(as.integer64(data$bank)/1000000) # We only need to divide it if the CNPJ is 14 digits. 
  data$bank = as.integer64(data$bank)
  
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
  if (file.exists(paste0(path_data, tipo[i], "_v2.parquet"))) {
    # rbindlist data and old data
    old_data <- read_parquet(paste0(path_data, tipo[i], "_v2.parquet"))
    data <- rbindlist(list(data, old_data))
    # Delete repeated rows
    data <- unique(data)
    # Saves old data just in case
    write_parquet(old_data, sink = paste0(path_data, tipo[i], "_v2_old.parquet"))
    rm(old_data)
  }
  
  write_parquet(data, sink = paste0(path_data, tipo[i], "_v2.parquet"))
}
rm(data)
}

# ------------------------------------------------------------------------------
# CCS_first_account - Done! Checked!
# ------------------------------------------------------------------------------
# Variables: dia, id_municipio_receita, tipo, first_account

if(run_CCS_first_account == 1){
# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

tipo <- c("CCS_first_account_PF", "CCS_first_account_PJ")

for(i in 1:length(tipo)) {
  print(Sys.time())
  cat("Extracting data for", tipo[i], "...\n")
  data <- data.table()
  standardQuery <- read_file(paste0(path_query, tipo[i], ".sql"), 
                             locale = locale(encoding = "latin1"))
  data <- as.data.table(dbGetQuery(connection, standardQuery))
  
  # Deletes those with id_municipio_receita < 0
  data <- data[data$id_municipio_receita >= 0, ]
  
  write_parquet(data, sink = paste0(path_data, tipo[i], "_v2.parquet"))
}
rm(data)
}

# ------------------------------------------------------------------------------
# CCS_Muni_stock - Done! Checked!
# ------------------------------------------------------------------------------

# Variables: week, id_municipio_receita, tipo, muni_stock, muni_stock_w, banked_pop

if(run_CCS_Muni_stock == 1){
# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

tipo <- c("CCS_Muni_stock_PF", "CCS_Muni_stock_PJ")
loop_week <- gen_week_list(2018, 2023)
week_number_list <- lapply(loop_week, stata_week_number)

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
    
    # Note that I am getting the number of accounts by the end of the week
    parameters = data.table(from = "@selectedDateEND", to = data_fim)
    parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
    parameters = data.table(from = "@WEEK", to = week_number_list[j])
    parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
    
    # Append the new data to the existing data table
    data <- rbindlist(list(data, as.data.table(dbGetQuery(connection, parametrizedQuery))))
  }#end of Loop j through weeks
  
  # Deletes those with id_municipio_receita < 0
  data <- data[data$id_municipio_receita >= 0, ]
  
  # Merge with old data if it exists 
  if (file.exists(paste0(path_data, tipo[i], "_v2.parquet"))) {
    # rbindlist data and old data
    old_data <- read_parquet(paste0(path_data, tipo[i], "_v2.parquet"))
    data <- rbindlist(list(data, old_data))
    # Delete repeated rows
    data <- unique(data)
    # Saves old data just in case
    write_parquet(old_data, sink = paste0(path_data, tipo[i], "_old_v2.parquet"))
    rm(old_data)
  }
  
  write_parquet(data, sink = paste0(path_data, tipo[i], "_v2.parquet"))
}
rm(data)
}

# ------------------------------------------------------------------------------
# Pix_Muni_user - Done! Checked!
# ------------------------------------------------------------------------------

if(run_Pix_Muni_user == 1){
# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

# Variables: week, muni_cd, tipo, users

tipo <- c("Pix_Muni_user")
loop_week <- gen_week_list(2023, 2023)
#loop_week <- loop_week[-(1:45)]
week_number_list <- lapply(loop_week, stata_week_number)

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
  
  # Deletes those with muni_cd < 0
  data <- data[data$muni_cd >= 0, ]
  
  # Merge with old data if it exists 
  if (file.exists(paste0(path_data, tipo[i], ".parquet"))) {
    # rbindlist data and old data
    old_data <- read_parquet(paste0(path_data, tipo[i], ".parquet"))
    data <- rbindlist(list(data, old_data))
    # Delete repeated rows
    data <- unique(data)
    rm(old_data)
  }
  
  write_parquet(data, sink = paste0(path_data, tipo[i], ".parquet"))
  
}  #end of Loop i through tipo[i]
rm(data)

# Safe solution ----------------------------------------------------------------
# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

# Variables: week, muni_cd, tipo, senders, receivers

tipo <- c("Pix_Muni_user_rec", "Pix_Muni_user_send")
loop_week <- gen_week_list(2020,2023)
loop_week <- loop_week[-(1:45)]
week_number_list <- lapply(loop_week, stata_week_number)

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
  
  # Deletes those with muni_cd < 0
  data <- data[data$muni_cd >= 0, ]
  
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
  
}  #end of Loop i through tipo[i]
rm(data)

# New solution to try. ---------------------------------------------------------
#this is just for test if sql works.
# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

# Variables: week, muni_cd, tipo, senders, receivers, users

# RODAR
tipo <- c("Pix_Muni_user_v2")
loop_week <- gen_week_list(2020,2020) # Putting very little just to test it out. 
loop_week <- loop_week[-(1:45)]
week_number_list <- lapply(loop_week, stata_week_number)
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
    
    # Deletes those with muni_cd < 0
    data <- data[data$muni_cd >= 0, ]
    
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
    
  }  #end of Loop i through tipo[i]
  rm(data)
}, error = function(e) {
  print(e)
  print("Error in the SQL: Pix_Muni_user_v2")
})
}

# ------------------------------------------------------------------------------
# Card_rec - Done!
# ------------------------------------------------------------------------------
# the code is not necessarily better than the older one. The older one got transactions per day. 
# Questions:

# How to get bank code??
# Ideas: Winsorize cartao

# Variables: week, id_municipio, tipo, receivers, valor
if(run_Card_rec == 1){
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

tipo <- c("Debit_card_rec", "Credit_card_rec")
loop_week <- gen_week_list(2018,2023)
week_number_list <- lapply(loop_week, stata_week_number)
###
# To Test
#loop_week <- loop_week[(1:10)]
#week_number_list <- lapply(loop_week, stata_week_number)
###
tryCatch({
for(i in 1:length(tipo)) {
  data <- data.table()
  cat("Extracting data for", tipo[i], "...\n")
  for (j in 1:(length(loop_week) - 1)) {
    print(Sys.time())
    
    standardQuery <- read_file(paste0(path_query, tipo[i], ".sql"), 
                               locale = locale(encoding = "latin1"))
    
    data_ini <- format(loop_week[[j]], "%Y-%m-%d")
    data_fim <- format(loop_week[[j + 1]], "%Y-%m-%d")
    cat("The week starts at", data_ini, "and ends at", data_fim, "\n")
    
    parameters <- data.table(from = "@selectedDateSTART", to = data_ini)
    parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
    parameters <- data.table(from = "@selectedDateEND", to = data_fim)
    parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
    #parameters <- data.table(from = "@WEEK", to = as.numeric(week_number_list[[j]])) # - I though the older method was wrong but it worked fine. 
    parameters = data.table(from = "@WEEK", to = week_number_list[j])
    parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
    
    # Append the new data to the existing data table
    data <- rbindlist(list(data, as.data.table(dbGetQuery(connection, parametrizedQuery)))) 
  } #end of Loop j through weeks
  
  # Deletes those with id_municipio < 0
  data <- data[data$id_municipio >= 0, ]
  
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
  
}  #end of Loop i through tipo[i]
rm(data)
}, error = function(e) {
  print(e)
  print("Error in the SQL: Card_rec")
})
}

# ------------------------------------------------------------------------------
# Card_adoption - test worked
# ------------------------------------------------------------------------------

# Variables: time_id, tipo, id_municipio, adopters, adopters_credit, adopters_debit
if(run_Card_adoption == 1){
# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

tipo <- c("Card_adoption")
mun_list <- mun_convert
####
# Test
#mun_list <- mun_convert[1:2,]
# Since we are ordering the municipalities, we can do it in chunks.
  # Note that we are using id_municipio here, not muni_cd. Thats because the credit card data is in the id_municipio format.
###
tryCatch({
for(i in 1:length(tipo)) {
  data <- data.table()
  # Note that we are using id_municipio here, not muni_cd. Thats because the credit card data is in the id_municipio format.
  for(j in 1:nrow(mun_list)) { 
    m <- as.integer(mun_list[j, "id_municipio"])
    print(Sys.time())
    cat("Extracting data for municipality:", m, "...\n")
    standardQuery <- read_file(paste0(path_query, tipo[i], ".sql"), 
                               locale = locale(encoding = "latin1"))
    parameters <- data.table(from = "@MUNI_CD_LOOP", to = m)
    parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
    # Append the new data to the existing data table
    data <- rbindlist(list(data, as.data.table(dbGetQuery(connection, parametrizedQuery)))) 
  }
  
  # Deletes those with id_municipio < 0
  data <- data[data$id_municipio >= 0, ]
  
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
}
rm(data)
}, error = function(e) {
  print(e)
  print("Error in the SQL: Card_adoption")
})
}

# ------------------------------------------------------------------------------
# Boleto - Done!
# ------------------------------------------------------------------------------

# Questions:
# How to get bank code??
# winsorize boleto, and ted

# week,id_municipio_receita,tipo,receivers,trans_rec, valor_rec
# week, id_municipio_receita, tipo, senders, trans_send, valor_send

# RFB.MUN_CD AS id_municipio_receita

if(run_Boleto == 1){
# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

tipo <- c("Boleto_rec", "Boleto_send")
loop_week <- gen_week_list(2018,2023)
week_number_list <- lapply(loop_week, stata_week_number)

###
# To Test
#loop_week <- loop_week[(1:10)]
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
  
  # Deletes those with id_municipio_receita < 0
  data <- data[data$id_municipio_receita >= 0, ]
  
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
  
}  #end of Loop i through tipo[i]
rm(data)
}, error = function(e) {
  print(e)
  print("Error in the SQL: Boleto")
})
}

# ------------------------------------------------------------------------------
# Boleto_adoption - test took too long - break it down.
# ------------------------------------------------------------------------------

if(run_Boleto_adoption == 1){
# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

tipo <- c("Boleto_adoption_PJ","Boleto_adoption_PF")
# loop through municipality? 
####
# Test
mun_list <- mun_convert[1:2,]
# Since we are ordering the municipalities, we can do it in chunks.
# PIX.MUN_CD = @MUNI_CD_LOOP
###
tryCatch({
for(i in 1:length(tipo)) {
  data <- data.table()
  for(j in 1:nrow(mun_list)) { 
    m <- as.integer(mun_list[j, "id_municipio_receita"])
    print(Sys.time())
    cat("Extracting data for municipality:", m, "...\n")
    standardQuery <- read_file(paste0(path_query, tipo[i], ".sql"), 
                               locale = locale(encoding = "latin1"))
    parameters = data.table(from = "@MUNI_CD_LOOP", to = m)
    parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
    # Append the new data to the existing data table
    data <- rbindlist(list(data, as.data.table(dbGetQuery(connection, parametrizedQuery)))) 
  }
  
  # Deletes those with id_municipio_receita < 0
  data <- data[data$id_municipio_receita >= 0, ]
  
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
}
rm(data)
}, error = function(e) {
  print(e)
  print("Error in the SQL: Boleto_adoption")
})
}


# ------------------------------------------------------------------------------
# Pix_adoption <---- Whole thing - loop though munis  - Pix_adoption.sql  - test took too long - break it down. 
# ------------------------------------------------------------------------------

# Variables: time_id, muni_cd, tipo, adopters

if(run_Pix_adoption == 1){
# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

tipo <- c("Pix_adoption_PJ","Pix_adoption_PF")

####
# Test
mun_list <- mun_convert[1:2,]
# Since we are ordering the municipalities, we can do it in chunks.
# CLI_PAG.MUN_CD = @MUNI_CD_LOOP
###
tryCatch({
for(i in 1:length(tipo)) {
  data <- data.table()
  for(j in 1:nrow(mun_list)) {
    m <- as.integer(mun_list[j, "muni_cd"])
    print(Sys.time())
    cat("Extracting data for municipality:", m, "...\n")

    standardQuery <- read_file(paste0(path_query, tipo[i], ".sql"),
                               locale = locale(encoding = "latin1"))

    parameters = data.table(from = "@MUNI_CD_LOOP", to = m)
    parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
    # Append the new data to the existing data table
    data <- rbindlist(list(data, as.data.table(dbGetQuery(connection, parametrizedQuery))))
  }
  # Deletes those with muni_cd < 0
  data <- data[data$muni_cd >= 0, ]

  # Merge with old data if it exists
  if(file.exists(paste0(path_data, tipo[i], ".parquet"))) {
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
}
rm(data)
}, error = function(e) {
  print(e)
  print("Error in the SQL: Adoption_Pix_loop")
})
}


# # ------------------------------------------------------------------------------
# # Ideas
# # ------------------------------------------------------------------------------

# # Only thing that matters is number of users!
# # get number of unique users of tech in the place (plus transactions and value)

# # log trans, log vol, users send, users rec. (maybe users self)


# # it seems very simple to get the info from samples. 
# # Then, all the graphs can be produced later. 


# # ------------------------------------------------------------------------------




# # Credito_por_Muni.R, BoletosMuni.R, TED por Muni x dia.R, CartaoMuniDia.R

# # Cartao Ideas:
# # Winsorize cartao; vendas
# # Adocao, quando foi a primeira vez que aquele cnpj usou o cartao?
# # Can be done for sample of firms as well

# # Credito
# # Linha de credito
# # numero de cartoes de credito
# # Adocao, quando foi a primeira vez que teve credito?

# #--

# # Pix 
# # adoption for sample firms
# # adoption for all firms and ind, need to loop though muni

# # TED
# # There is something wrong with the results of TED
# # I think the cleaning was done wrong, especially in the quantity of people/firms
# # we can also do the same we did for pix. 
# # inflow, ..., P2P, ...
# # Maybe we get bank flows as well. 

# # Boleto
# # The same things we do for Pix, we can do for boleto. 
# # Inflow,...,  P2P,...
# # Maybe we get the bank flows as well. 

# # We can then sum boleto, ted and pix. Show the effect on the whole economy.

# #-- Do all codes for individuals
