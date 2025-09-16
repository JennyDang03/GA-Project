#Download_sqls

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

# Functions -------------------------------------------------------------------
source("./functions/parametrizeQuery.R")
source("./functions/extractTeradataDataSEDE.R")
source(paste0(R_path,"/functions/stata_week_number.R"))
source(paste0(R_path,"/functions/gen_week_list.R"))

# Settings -------------------------------------------------------------------
# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

# Query -------------------------------------------------------------------

################# MUNICIPAL ################

# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# Pix_Muni_flow
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# Variables: flow_code, week, muni_cd, sender_type, receiver_type, senders, receivers, valor, trans, valor_w

# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

tipo <- c("Pix_Muni_self", "Pix_Muni_inflow","Pix_Muni_outflow","Pix_Muni_intra")
loop_week <- gen_week_list(2020,2022)
loop_week <- loop_week[-(1:45)]
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
  
  write_parquet(data, sink = paste0(path_data, tipo[i], ".parquet"))
  
}  #end of Loop i through tipo[i]
rm(data)


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# Pix_Muni_Bank
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# Variables: week, muni_cd, tipo, bank, value_send, trans_send, send_users, value_send_w, value_rec, trans_rec, rec_users, value_rec_w

# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

tipo <- c("Pix_Muni_Bank_rec", "Pix_Muni_Bank_send", "Pix_Muni_Bank_self_rec","Pix_Muni_Bank_self_send")
loop_week <- gen_week_list(2020,2022)
loop_week <- loop_week[-(1:45)]
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
  
  write_parquet(data, sink = paste0(path_data, tipo[i], ".parquet"))
  
} #end of Loop i through tipo[i]
rm(data)

# ------------------------------------------------------------------------------
# CCS_Muni_IF 
# ------------------------------------------------------------------------------
# Variables: week, muni_cd, tipo, bank, opening, stock, closing

# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

tipo <- c("CCS_Muni_IF_opening_PF", "CCS_Muni_IF_opening_PJ", "CCS_Muni_IF_closing_PF", "CCS_Muni_IF_closing_PJ", "CCS_Muni_IF_stock_PF", "CCS_Muni_IF_stock_PJ")
#tipo <- c("CCS_Muni_IF_opening_PJ", "CCS_Muni_IF_closing_PJ",  "CCS_Muni_IF_stock_PJ")


loop_week <- gen_week_list(2018,2022)
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
    #data_ini <- paste("'", data_ini, "'")
    #data_fim <- paste("'", data_fim, "'")
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
  # Variables: week, muni_cd, tipo, bank, opening, stock, closing
  
  # Deletes those with muni_cd < 0
  data <- data[data$muni_cd >= 0, ]
  
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
  write_parquet(data, sink = paste0(path_data, tipo[i], ".parquet"))
}



# ------------------------------------------------------------------------------
# CCS_Muni_stock
# ------------------------------------------------------------------------------
# Variables: week, muni_cd, tipo, muni_stock, muni_stock_w, banked_pop

# Demora muito - 6 min por semana. Fazer depois. 

# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

tipo <- c("CCS_Muni_stock_PF", "CCS_Muni_stock_PJ")

loop_week <- gen_week_list(2018,2022)
week_number_list <- lapply(loop_week, stata_week_number)

#### 
#For test:
#loop_week <- gen_week_list(2018,2018)
#loop_week <- loop_week[(1:10)]
#week_number_list <- lapply(loop_week, stata_week_number)
####

for(i in 2:length(tipo)) {
  data <- data.table()
  for (j in 1:(length(loop_week) - 1)) {
    print(Sys.time())
    cat("Extracting data for", tipo[i], "...\n")
    standardQuery <- read_file(paste0(path_query, tipo[i], ".sql"), 
                               locale = locale(encoding = "latin1"))
    
    data_ini <- format(loop_week[[j]], "%Y-%m-%d")
    data_fim <- format(loop_week[[j + 1]], "%Y-%m-%d")
    #data_ini <- paste("'", data_ini, "'") # on Pix database we need those lines, but for CCS, we dont
    #data_fim <- paste("'", data_fim, "'")
    cat("The week starts at", data_ini, "and ends at", data_fim, "\n")
    
    # Note that I am getting the number of accounts by the end of the week
    parameters = data.table(from = "@selectedDateEND", to = data_fim)
    parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
    parameters = data.table(from = "@WEEK", to = week_number_list[j])
    parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
    
    # Append the new data to the existing data table
    data <- rbindlist(list(data, as.data.table(dbGetQuery(connection, parametrizedQuery))))
  }#end of Loop j through weeks
  
  # Deletes those with muni_cd < 0
  data <- data[data$muni_cd >= 0, ]
  
  write_parquet(data, sink = paste0(path_data, tipo[i], ".parquet"))
}
rm(data)

# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# Pix_Muni_user
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------


# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

# Variables: week, muni_cd, tipo, senders, receivers, users

tipo <- c("Pix_Muni_user")
loop_week <- gen_week_list(2020,2022)
loop_week <- loop_week[-(1:45)]
week_number_list <- lapply(loop_week, stata_week_number)
#### 
#For test:
# loop_week <- gen_week_list(2020,2020)
# loop_week <- loop_week[-(1:45)]
# week_number_list <- lapply(loop_week, stata_week_number)
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
  
  write_parquet(data, sink = paste0(path_data, tipo[i], ".parquet"))
  
}  #end of Loop i through tipo[i]
rm(data)

# ------------------------------------------------------------------------------
# CCS_first_account
# ------------------------------------------------------------------------------
# Variables: day, muni_cd, tipo, first_account

# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

tipo <- c("CCS_first_account_PF", "CCS_first_account_PJ")
#tipo <- c("CCS_first_account_PJ")


for(i in 1:length(tipo)) {
  print(Sys.time())
  data <- data.table()
  standardQuery <- read_file(paste0(path_query, tipo[i], ".sql"), 
                             locale = locale(encoding = "latin1"))
  data <- as.data.table(dbGetQuery(connection, standardQuery))
  
  # Deletes those with muni_cd < 0
  data <- data[data$muni_cd >= 0, ]
  
  write_parquet(data, sink = paste0(path_data, tipo[i], ".parquet"))
}

# ------------------------------------------------------------------------------
# Adoption_Pix
# ------------------------------------------------------------------------------
# Variables: day, muni_cd, tipo, rec_adopters, send_adopters, self_adopters, adopters

# Adoption for individuals is too hard -> adoption for sample people
# adoption for firms later


# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

tipo <- c("adoption_self_ind")
#tipo <- c("adoption_rec_ind", "adoption_send_ind") 

for(i in 1:length(tipo)) {
  print(Sys.time())
  data <- data.table()
  standardQuery <- read_file(paste0(path_query, tipo[i], ".sql"), 
                             locale = locale(encoding = "latin1"))
  data <- as.data.table(dbGetQuery(connection, standardQuery))
  
  # Deletes those with muni_cd < 0
  data <- data[data$muni_cd >= 0, ]
  
  write_parquet(data, sink = paste0(path_data, tipo[i], ".parquet"))
}


# ------------------------------------------------------------------------------
# Ideas
# ------------------------------------------------------------------------------


# Credito_por_Muni.R, BoletosMuni.R, TED por Muni x dia.R, CartaoMuniDia.R

# CCS BANK ACCOUNT IDEAS:
# easy to get the first ever bank account. Just minimize date initial
# Also, Get a flag for having an account before Pix. 

# Cartao Ideas:
# Winsorize cartao; vendas
# Adocao, quando foi a primeira vez que aquele cnpj usou o cartao?
# Can be done for sample of firms as well

# Credito
# Linha de credito
# numero de cartoes de credito

#--

# Pix 
# adoption for firms! Adoption for individuals is too hard. 
# adoption for sample people

# TED
# There is something wrong with the results of TED
# I think the cleaning was done wrong, especially in the quantity of people/firms
# we can also do the same we did for pix. 
# inflow, ..., P2P, ...
# Maybe we get bank flows as well. 

# Boleto
# The same things we do for Pix, we can do for boleto. 
# Inflow,...,  P2P,...
# Maybe we get the bank flows as well. 

# We can then sum boleto, ted and pix. Show the effect on the whole economy.

#-- Do all codes for individuals





# ------------------------------------------------------------------------------
# Pix_flow_new_ind
# ------------------------------------------------------------------------------
# Variables: flow_code, week, muni_cd, id, id_type, sender_type, receiver_type, senders, receivers, valor, trans, valor_w

tipo <- c("Pix_self_new_ind", "Pix_inflow_new_ind","Pix_outflow_new_ind","Pix_intra1_new_ind","Pix_intra2_new_ind")

loop_month <- gen_month_list(2020,2022)
month_number_list <- lapply(loop_month, stata_month_number)

#### EXAMPLE FOR JOSE TO TEST:
loop_month <- loop_week[10:13]
month_number_list <- lapply(loop_month, stata_month_number)
#### 






# tipo <- c("CCS_Muni_IF_new", "CCS_Muni_new") # Accounts of the END of the month
# for(i in 1:length(tipo)) {
#   paste0("./queries/", tipo[i], ".sql")
#   standardQuery <- read_file(paste0("./queries/", tipo[i], ".sql"), 
#                              locale = locale(encoding = "latin1"))
#   START_YEAR = 2018 
#   END_YEAR   = 2022
#   currentYEAR = START_YEAR
#   while(currentYEAR <= END_YEAR) {
#     mes = 1
#     if(exists("allData")) {
#       rm(allData)
#     }
#     while(mes <= 12) {
#       flog.info("Extracting data for %s...", paste(tipo[i], currentYEAR,mes,sep="-") )
#       if (mes == 12) {
#         mes_seg = 1
#         currentYEARseg = currentYEAR + 1
#       } else {
#         mes_seg = mes + 1
#         currentYEARseg = currentYEAR
#       }
#       
#       data_fim = paste(toString(currentYEARseg) ,formatC(mes_seg, width=2, flag="0"), "01", sep="-")
#       data_fim = paste("'", data_fim, "'")
#       parameters = data.table(from = "@selectedDate", to = data_fim)
#       parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
#       data = as.data.table(extractTeradataDataSEDE(parametrizedQuery))
#       
#       # Save the aggregated data for each month
#       if (dim(data)[1] > 0) {
#         fwrite(x = data, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/",tipo[i],  
#                                        as.character(currentYEAR*100+mes), ".csv"))
#       }
#       mes = mes + 1
#     }
#     currentYEAR = currentYEAR + 1 
#   }  
# } # end of tipo loop   
# 
# ################# INDIVIDUAL ################
# tipo <- c("Pix_flow_new_ind")
# for(i in 1:length(tipo)) {
#   paste0("./queries/", tipo[i], ".sql")
#   standardQuery <- read_file(paste0("./queries/", tipo[i], ".sql"), 
#                              locale = locale(encoding = "latin1"))
#   START_YEAR = 2020 
#   END_YEAR   = 2022
#   currentYEAR = START_YEAR
#   while(currentYEAR <= END_YEAR) {
#     mes = 1
#     if(exists("allData")) {
#       rm(allData)
#     }
#     while(mes <= 12) {
#       flog.info("Extracting data for %s...", paste(tipo[i], currentYEAR,mes,sep="-") )
#       if (mes == 12) {
#         mes_seg = 1
#         currentYEARseg = currentYEAR + 1
#       } else {
#         mes_seg = mes + 1
#         currentYEARseg = currentYEAR
#       }
#       data_ini = paste(toString(currentYEAR), formatC(mes, width=2, flag="0"), "01", sep="-")
#       data_ini = paste("'", data_ini, "'")
#       data_fim = paste(toString(currentYEARseg) ,formatC(mes_seg, width=2, flag="0"), "01", sep="-")
#       data_fim = paste("'", data_fim, "'")
#       
#       parameters = data.table(from = "@selectedDateSTART", to = data_ini)
#       parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
#       parameters = data.table(from = "@selectedDateEND", to = data_fim)
#       parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
#       
#       data = as.data.table(extractTeradataDataSEDE(parametrizedQuery))
#       
#       ################ ANONIMIZACAO IDs ###############
#       ### 1) Firmas
#       ### 2) Pessoas fisicas
#       #################################################
#       
#       ##########################################
#       # 1) Des-identificacao firmas  
#       ##########################################
#       
#       firmas = data[data$id_tipo == 2, ] 
#       firmas$id = trunc(as.integer64(firmas$id)/1000000)
#       firmas = merge(x = firmas, y = mapCNPJ8,
#                      by.x = "id", by.y = "CNPJ8",
#                      all.x = T)
#       
#       # Check numero de nulls
#       flog.info(sum(is.na (firmas$CNPJ8_HASH)))
#       
#       # Drop identified columns and keep hashed ones ----------------------------
#       firmas$id = firmas$CNPJ8_HASH
#       firmas$CNPJ8_HASH       = NULL
#       
#       ###############################################
#       # 2) Des-identificacao pessoa fisica
#       ###############################################
#       data = data[data$id_tipo == 1, ] 
#       data$id = trunc(as.integer64(trim(data$id))/100)
#       data = merge(x = data, y = mapIndividuals,
#                    by.x = "id", by.y = "cpf",
#                    all.x = T)
#       
#       # Check numero de nulls
#       flog.info(sum(is.na (data$cpf_hash)))
#       
#       # Drop identified columns and keep hashed ones ----------------------------
#       data$id = data$cpf_hash
#       data$cpf_hash       = NULL
#       
#       ### FIM DA ANONIMIZACAO #####
#       ############################################################
#       
#       data <- rbindlist(list(data, firmas)) 
#       
#       # Save the aggregated data for each month
#       if (dim(data)[1] > 0) {
#         fwrite(x = data, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/",tipo[i],  
#                                        as.character(currentYEAR*100+mes), ".csv"))
#       }
#       mes = mes + 1
#     }
#     currentYEAR = currentYEAR + 1 
#   }  
# } # end of tipo loop   



# 
# START_YEAR = 2020 
# END_YEAR   = 2022
# currentYEAR = START_YEAR
# 
# # Loop through weeks!
# 
# 
# while(currentYEAR <= END_YEAR) {
#   mes = 1
#   if(exists("allData")) {
#     rm(allData)
#   }
#   while(mes <= 12) {
#     if (mes == 12) {
#       mes_seg = 1
#       currentYEARseg = currentYEAR + 1
#     } else {
#       mes_seg = mes + 1
#       currentYEARseg = currentYEAR
#     }
#     
#     # data <- data.table()
#     for(i in 1:length(tipo)) {
#       flog.info("Extracting data for %s...", paste(tipo[i], currentYEAR,mes,sep="-") )
#       paste0("./queries/", tipo[i], ".sql")
#       standardQuery <- read_file(paste0("./queries/", tipo[i], ".sql"), 
#                                  locale = locale(encoding = "latin1"))
#       
#       data_ini = paste(toString(currentYEAR), formatC(mes, width=2, flag="0"), "01", sep="-")
#       data_ini = paste("'", data_ini, "'")
#       data_fim = paste(toString(currentYEARseg) ,formatC(mes_seg, width=2, flag="0"), "01", sep="-")
#       data_fim = paste("'", data_fim, "'")
#       
#       parameters = data.table(from = "@selectedDateSTART", to = data_ini)
#       parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
#       parameters = data.table(from = "@selectedDateEND", to = data_fim)
#       parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
#       
#       # Append the new data to the existing data table
#       data <- rbindlist(list(data, as.data.table(extractTeradataDataSEDE(parametrizedQuery))))
#       
#     } # end of tipo loop   
#     # # Save the aggregated data for each month
#     # if (dim(data)[1] > 0) {
#     #   fwrite(x = data, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/",filename,  
#     #                                  as.character(currentYEAR*100+mes), ".csv"))
#     # }
#     mes = mes + 1
#   }
#   currentYEAR = currentYEAR + 1 
# }  
# 
# ### Now, lets clean it. ### 
# 
# # Fix the day. Example: "2020-01-01"
# data$day <- as.Date(data$day)
# 
# # Fix the week
# data <- data %>% 
#   mutate(week = stata_week_number(day))
# 
# # Collapse data
# data <- data %>%
#   group_by(week, muni_cd, sender_type, receiver_type, flow_code) %>%
#   summarize(
#     value = sum(value),
#     trans = sum(trans),
#     value_w = sum(value_w),
#     senders = sum(senders),
#     receivers = sum(receivers)
#   ) %>%
#   mutate(
#     lvalue = log1p(value),
#     ltrans = log1p(trans),
#     lvalue_w = log1p(value_w),
#     lsenders = log1p(senders),
#     lreceivers = log1p(receivers)
#   )
# # Checks for duplicate
# 
# # Deletes those with muni_cd < 0
# 
# # Needs to fill the data. Some muni_cd have weeks with no transactions for example. 
# 
#   # substitute some of the missing for zeros (only intra has users variable)
# 
# # Add Flood -> keep only date_flood. keep only matches. 
# flood_data <- read_dta("\\sbcdf176\PIX_Matheus$\Stata\dta\flood_weekly_2020_2022.dta")
# data <- data %>%
#   left_join(flood_data, by = c("muni_cd", "week"))
# 
# 
# # Download data
# write_dta(data, filename)
# 
# 
# 
# 
# #dbDisconnect(connection)
# 
# 
# 



# Backup

# tipo <- c("Pix_flow_new", "bank_flow_new", "bank_flow_self_new")
# for(i in 1:length(tipo)) {
#   paste0("./queries/", tipo[i], ".sql")
#   standardQuery <- read_file(paste0("./queries/", tipo[i], ".sql"), 
#                              locale = locale(encoding = "latin1"))
#   START_YEAR = 2020 
#   END_YEAR   = 2022
#   currentYEAR = START_YEAR
#   while(currentYEAR <= END_YEAR) {
#     mes = 1
#     if(exists("allData")) {
#       rm(allData)
#     }
#     while(mes <= 12) {
#       flog.info("Extracting data for %s...", paste(tipo[i], currentYEAR,mes,sep="-") )
#       if (mes == 12) {
#         mes_seg = 1
#         currentYEARseg = currentYEAR + 1
#       } else {
#         mes_seg = mes + 1
#         currentYEARseg = currentYEAR
#       }
#       data_ini = paste(toString(currentYEAR), formatC(mes, width=2, flag="0"), "01", sep="-")
#       data_ini = paste("'", data_ini, "'")
#       data_fim = paste(toString(currentYEARseg) ,formatC(mes_seg, width=2, flag="0"), "01", sep="-")
#       data_fim = paste("'", data_fim, "'")
#       
#       parameters = data.table(from = "@selectedDateSTART", to = data_ini)
#       parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
#       parameters = data.table(from = "@selectedDateEND", to = data_fim)
#       parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
#       
#       data = as.data.table(extractTeradataDataSEDE(parametrizedQuery))
#       # Save the aggregated data for each month
#       if (dim(data)[1] > 0) {
#         fwrite(x = data, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/",tipo[i],  
#                                        as.character(currentYEAR*100+mes), ".csv"))
#       }
#       mes = mes + 1
#     }
#     currentYEAR = currentYEAR + 1 
#   }  
# } # end of tipo loop   
# 
# tipo <- c("adoption_new") 
# for(i in 1:length(tipo)) {
#   paste0("./queries/", tipo[i], ".sql")
#   standardQuery <- read_file(paste0("./queries/", tipo[i], ".sql"), 
#                              locale = locale(encoding = "latin1"))
#   data = as.data.table(extractTeradataDataSEDE(standardQuery))
#   # Save the aggregated data for each month
#   fwrite(x = data, file = paste0("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/",tipo[i],".csv"))
# } # end of tipo loop   
# 








