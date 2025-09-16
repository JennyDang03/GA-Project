#Download_sqls
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
#library(optiRum)
library(gdata)
library(lubridate)
library("arrow")

setwd("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer")
source("./functions/parametrizeQuery.R")
source("./functions/extractTeradataDataSEDE.R")

path_query <- "//sbcdf176/PIX_Matheus$/R/DataExtraction/"
path_data <- "//sbcdf176/PIX_Matheus$/DadosOriginais/"
path_dta <- "//sbcdf176/PIX_Matheus$/Stata/dta/" 

# Global settings ---------------------------------------------------------

# Hashmap -----------------------------------------------------------------
mapCNPJ8 = fread("./data/hash_CNPJ8_DE.csv")
#mapCNPJ8 = fread("./data/hash_firms8_2022.csv")
#mapCNPJ14 = fread("./data/hash_firms14_2022.csv")

mapIndividuals = fread("./data/hash_persons_DE.csv")


# Functions -------------------------------------------------------------------
stata_week_number <- function(date) {
  year <- year(date)
  month <- month(date)
  days_from_jan1 <- as.numeric(date - as.Date(paste(year, "01-01", sep = "-")))
  week_number <- 1 + (days_from_jan1 %/% 7)
  if (week_number == 53) {
    week_number <- 52
  }
  week_number <- week_number + (year-1960)*52 - 1
  return(week_number)
}
gen_week_list <- function(start_year, end_year) {
  date_list <- list()
  for (y in start_year:end_year){
    start_date <- as.Date(paste(y, "01-01", sep = "-"))
    for (i in 1:52) {
      date_list[[i+(y-start_year)*52]] <- start_date + (i - 1) * 7
    }
  }
  start_date <- as.Date(paste(end_year+1, "01-01", sep = "-"))
  date_list[[1+(end_year+1-start_year)*52]] <- start_date + (1 - 1) * 7
  return(date_list)
}

# Settings -------------------------------------------------------------------
# Load Flood data -> keep only date_flood. 
flood_data <- read_dta(paste0(dta,"flood_weekly_2020_2022.dta"))
#flood_data <- read_dta("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/flood_weekly_2020_2022.dta")
flood_data <- flood_data %>%
  select(muni_cd, week, date_flood)

# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

# Query -------------------------------------------------------------------

################# MUNICIPAL ################


# ------------------------------------------------------------------------------
# Pix_flow_new
# ------------------------------------------------------------------------------
# Variables: flow_code, week, muni_cd, sender_type, receiver_type, senders, receivers, valor, trans, valor_w

tipo <- c("Pix_self_new", "Pix_inflow_new","Pix_outflow_new","Pix_intra_new")
loop_week <- gen_week_list(2020,2022)
week_number_list <- lapply(loop_week, stata_week_number)

#### EXAMPLE FOR JOSE TO TEST:
# pix started on the 46 week, 45 is a test to see if I get zero data and processes fine. 
# 55 is in 2021, so it is nice to test the code for week number.  
loop_week <- loop_week[45:55]
week_number_list <- lapply(loop_week, stata_week_number)
#### 

for(i in 1:length(tipo)) {
  data <- data.table()
  for (j in 1:(length(loop_week) - 1)) {
    
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


data_pix = read_parquet(paste0(path_data, tipo[1], ".parquet"), as_tibble = TRUE)

for(i in 2:length(tipo)) {
  teste <- read_parquet(paste0(path_data, tipo[i], ".parquet"), as_tibble = TRUE)
  data_pix <- rbindlist(list(data_pix, teste))   
}

# Needs to fill the data. Some muni_cd have weeks with no transactions for example. 
data_pix %>%
  complete(
    week,
    nesting(muni_cd, sender_type, receiver_type, flow_code),
    fill = list(senders = 0, receivers = 0, valor = 0, trans=0, valor_w = 0),
    explicit = FALSE)

# Add Flood - keep only matches. 
data_pix <- data_pix %>%
  inner_join(flood_data, by = c("muni_cd", "week"))

# Add Log
data_pix <- data_pix %>%
  mutate(
    lvalor = log1p(valor),
    ltrans = log1p(trans),
    lvalor_w = log1p(valor_w),
    lsenders = log1p(senders),
    lreceivers = log1p(receivers)
  )

# Download data
write_dta(data_pix, paste0(path_dta,base_pix_muni,".dta"))
# Variables: date_flood, flow_code, week, muni_cd, sender_type, receiver_type, senders, receivers, valor, trans, valor_w,
#             lsenders, lreceivers, lvalor, ltrans, lvalor_w

rm(data_pix) # Just to clean the working space

# ------------------------------------------------------------------------------
# bank_flow_new
# ------------------------------------------------------------------------------
# Variables: week, muni_cd, tipo, bank, value_send, trans_send, send_users, value_send_w, value_rec, trans_rec, rec_users, value_rec_w

tipo <- c("bank_flow_new_rec","bank_flow_new_send", "bank_flow_self_new_rec", "bank_flow_self_new_send")
loop_week <- gen_week_list(2020,2022)
week_number_list <- lapply(loop_week, stata_week_number)

#### EXAMPLE FOR JOSE TO TEST:
loop_week <- loop_week[45:55]
week_number_list <- lapply(loop_week, stata_week_number)
#### 

for(i in 1:length(tipo)) {
  data <- data.table()
  for (j in 1:(length(loop_week) - 1)) {
    
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
  }
  ### Now, lets clean it. ### 
  # Variables: week, muni_cd, tipo, bank, value_send, trans_send, send_users, value_send_w, value_rec, trans_rec, rec_users, value_rec_w
  
  # Deletes those with muni_cd < 0
  data <- data[data$muni_cd >= 0, ]
  
  # Needs to fill the data. Some muni_cd have weeks with no transactions for example. 
  data %>%
    complete(
      week,
      nesting(muni_cd, tipo, bank),
      fill = list(value_send = 0, trans_send = 0, send_users = 0, value_send_w = 0, value_rec = 0, trans_rec = 0, rec_users = 0, value_rec_w = 0),
      explicit = FALSE)
  
  # Add Flood - keep only matches. 
  data <- data %>%
    inner_join(flood_data, by = c("muni_cd", "week"))
  
  # Add Log
  data <- data %>%
    mutate(
      lvalue_send = log1p(value_send),
      ltrans_send = log1p(trans_send),
      lsend_users = log1p(send_users),
      lvalue_send_w = log1p(value_send_w),
      lvalue_rec = log1p(value_rec),
      ltrans_rec = log1p(trans_rec),
      lrec_users = log1p(rec_users),
      lvalue_rec_w = log1p(value_rec_w)      
    )
  
  # Download data
  write_dta(data, paste0(path_dta,tipo[i],".dta"))
  # Variables: date_flood, week, muni_cd, tipo, bank, value_send, trans_send, send_users, value_send_w, value_rec, trans_rec, rec_users, value_rec_w
  #             lvalue_send, ltrans_send, lsend_users, lvalue_send_w, lvalue_rec, ltrans_rec, lrec_users, lvalue_rec_w
}

# ------------------------------------------------------------------------------
# CCS_Muni_IF_new
# ------------------------------------------------------------------------------
# Variables: week, muni_cd, tipo, bank, opening, stock, closing
tipo <- c("CCS_opening", "CCS_closing","CCS_stock")
loop_week <- gen_week_list(2018,2022)
week_number_list <- lapply(loop_week, stata_week_number)

#### EXAMPLE FOR JOSE TO TEST:
loop_week <- loop_week[45:55]
week_number_list <- lapply(loop_week, stata_week_number)
#### 

for(i in 1:length(tipo)) {
  data <- data.table()
  for (j in 1:(length(loop_week) - 1)) {
    
    cat("Extracting data for", tipo[i], "...\n")
    standardQuery <- read_file(paste0(path_query, tipo[i], ".sql"), 
                               locale = locale(encoding = "latin1"))
    
    data_ini <- format(loop_week[[j]], "%Y-%m-%d")
    data_fim <- format(loop_week[[j + 1]], "%Y-%m-%d")
    data_ini <- paste("'", data_ini, "'")
    data_fim <- paste("'", data_fim, "'")
    cat("The week starts at", data_ini, "and ends at", data_fim, "\n")
    
    # Note that I am getting the number of accounts by the end of the week
    parameters = data.table(from = "@selectedDateSTART", to = data_ini)
    parametrizedQuery <- parametrizeQuery(standardQuery, parameters)
    parameters = data.table(from = "@selectedDateEND", to = data_fim)
    parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
    parameters = data.table(from = "@WEEK", to = week_number_list[j])
    parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
    
    # ANONIMIZACAO DA IF
    data$bank = as.integer64(data$bank)
    data = merge(x = data, y = mapCNPJ8,
                 by.x = "bank", by.y = "CNPJ8",
                 all.x = T)
    
    # Check numero de nulls
    flog.info(sum(is.na (data$CNPJ8_HASH)))
    
    # Drop identified columns and keep hashed ones ----------------------------
    data$bank = data$CNPJ8_HASH
    data$CNPJ8_HASH = NULL

    # Append the new data to the existing data table
    data <- rbindlist(list(data, as.data.table(dbGetQuery(connection, parametrizedQuery))))
  }
  
  ### Now, lets clean it. ### 
  # Variables: week, muni_cd, tipo, bank, opening, stock, closing
  
  # Deletes those with muni_cd < 0
  data <- data[data$muni_cd >= 0, ]
  
  # Needs to fill the data. Some muni_cd have weeks with no transactions for example. 
  data %>%
    complete(
      week,
      nesting(muni_cd, tipo, bank),
      fill = list(opening = 0, closing = 0, stock = 0),
      explicit = FALSE)
  
  # Add Flood - keep only matches. 
  data <- data %>%
    inner_join(flood_data, by = c("muni_cd", "week"))
  
  # Add Log
  data <- data %>%
    mutate(
      lopening = log1p(opening),
      lclosing = log1p(closing),
      lstock = log1p(stock)    
    )
  
  # Download data
  write_dta(data, paste0(path_dta,tipo[i],".dta"))
  # Variables: date_flood, week, muni_cd, tipo, bank, opening, stock, closing
  #             lopening, lstock, lclosing
}

# ------------------------------------------------------------------------------
# CCS_Muni_new
# ------------------------------------------------------------------------------
# Variables: week, muni_cd, tipo, muni_stock, muni_stock_w, banked_pop
tipo <- c("CCS_Muni_new")
loop_week <- gen_week_list(2018,2022)
week_number_list <- lapply(loop_week, stata_week_number)

#### EXAMPLE FOR JOSE TO TEST:
loop_week <- loop_week[45:55]
week_number_list <- lapply(loop_week, stata_week_number)
#### 

for(i in 1:length(tipo)) {
  data <- data.table()
  for (j in 1:(length(loop_week) - 1)) {
    
    cat("Extracting data for", tipo[i], "...\n")
    standardQuery <- read_file(paste0(path_query, tipo[i], ".sql"), 
                               locale = locale(encoding = "latin1"))
    
    data_ini <- format(loop_week[[j]], "%Y-%m-%d")
    data_fim <- format(loop_week[[j + 1]], "%Y-%m-%d")
    data_ini <- paste("'", data_ini, "'")
    data_fim <- paste("'", data_fim, "'")
    cat("The week starts at", data_ini, "and ends at", data_fim, "\n")
    
    # Note that I am getting the number of accounts by the end of the week
    parameters = data.table(from = "@selectedDateEND", to = data_fim)
    parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
    parameters = data.table(from = "@WEEK", to = week_number_list[j])
    parametrizedQuery <- parametrizeQuery(parametrizedQuery, parameters)
    
    # Append the new data to the existing data table
    data <- rbindlist(list(data, as.data.table(dbGetQuery(connection, parametrizedQuery))))
  }
  
  ### Now, lets clean it. ### 
  # Variables: week, muni_cd, tipo, muni_stock, muni_stock_w, banked_pop
  
  # Deletes those with muni_cd < 0
  data <- data[data$muni_cd >= 0, ]
  
  # Needs to fill the data. Some muni_cd have weeks with no transactions for example. 
  data %>%
    complete(
      week,
      nesting(muni_cd, tipo, bank),
      fill = list(muni_stock = 0, muni_stock_w = 0, banked_pop = 0),
      explicit = FALSE)
  
  # Add Flood - keep only matches. 
  data <- data %>%
    inner_join(flood_data, by = c("muni_cd", "week"))
  
  # Add Log
  data <- data %>%
    mutate(
      lmuni_stock = log1p(muni_stock),
      lmuni_stock_w = log1p(muni_stock_w),
      lbanked_pop = log1p(banked_pop)    
    )
  
  # Download data
  write_dta(data, paste0(path_dta,tipo[i],".dta"))
  # Variables: week, muni_cd, tipo, muni_stock, muni_stock_w, banked_pop
  #             lmuni_stock, lmuni_stock_w, lbanked_pop
}


# ------------------------------------------------------------------------------
# CCS_first_account
# ------------------------------------------------------------------------------
# Variables: day, muni_cd, tipo, first_account
  
tipo <- c("CCS_first_account")
for(i in 1:length(tipo)) {
  data <- data.table()
  cat("Extracting data for", tipo[i], "...\n")
  standardQuery <- read_file(paste0(path_query, tipo[i], ".sql"), 
                             locale = locale(encoding = "latin1"))
  
  # Append the new data to the existing data table
  data <- as.data.table(dbGetQuery(connection, parametrizedQuery))
}

### Now, lets clean it. ### 
# Variables: day, muni_cd, tipo, first_account

loop_week <- gen_week_list(2018,2022)
week_number_list <- lapply(loop_week, stata_week_number)


# Deletes those with muni_cd < 0
data <- data[data$muni_cd >= 0, ]

# Needs to fill the data. Some muni_cd have days with no transactions for example. 
data %>%
  complete(
    day,
    nesting(muni_cd, tipo),
    fill = list(first_account = 0),
    explicit = FALSE)

#Add Week variable
data <- data %>%
  mutate(week = stata_week_number(day))

#Collapse by week
data <- data %>%
  group_by(muni_cd, tipo, week) %>%
  summarize(first_account = sum(first_account, na.rm = TRUE))

# Add Flood - keep only matches. 
data <- data %>%
  inner_join(flood_data, by = c("muni_cd", "week"))

# Add Log
data <- data %>%
  mutate(
    lfirst_account = log1p(first_account)
  )

# Download data
write_dta(data, paste0(path_dta,"CCS_first_account",".dta"))
# Variables: date_flood, week, muni_cd, tipo, first_account
#             lfirst_account




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








