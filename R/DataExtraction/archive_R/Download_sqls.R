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

setwd("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer")
source("./functions/parametrizeQuery.R")
source("./functions/extractTeradataDataSEDE.R")

# Global settings ---------------------------------------------------------
# Hashmap -----------------------------------------------------------------
mapCNPJ8 = fread("./data/hash_CNPJ8_DE.csv")
#mapCNPJ8 = fread("./data/hash_firms8_2022.csv")

#mapCNPJ14 = fread("./data/hash_firms14_2022.csv")

mapIndividuals = fread("./data/hash_persons_DE.csv")
#mapCPF = fread("./data/hash_persons_DE.csv")

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


# DB Settings
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

# Query -------------------------------------------------------------------

################# MUNICIPAL ################

######## Pix_flow_new ######## 
# Variables: flow_code, week, muni_cd, sender_type, receiver_type, senders, receivers, users, value, trans, value_w

tipo <- c("Pix_flow_new")

path <- "//sbcdf176/PIX_Matheus$/R/DataExtraction/"

file <- "Pix_flow_new"
filename <- paste0(path,file, ".dta")
data <- data.table()

loop_week <- gen_week_list(2020,2020)
week_number_list <- lapply(loop_week, stata_week_number)

#### EXAMPLE FOR JOSE TO TEST:
loop_week <- tail(loop_week, 10)
week_number_list <- lapply(loop_week, stata_week_number)
#### 

for(i in 1:length(tipo)) {
  for (j in 1:(length(loop_week) - 1)) {
  
    cat("Extracting data for", tipo[i], "...\n")
    #paste0(path, tipo[i], ".sql")
    standardQuery <- read_file(paste0(path, tipo[i], ".sql"), 
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
}

### Now, lets clean it. ### 
# Variables: flow_code, week, muni_cd, sender_type, receiver_type, senders, receivers, users, value, trans, value_w

# Collapse data


# Checks for duplicate
# It is not possible to have duplicates. This is a sanity check
key_columns <- c("flow_code", "week", "muni_cd", "sender_type", "receiver_type")
duplicates <- data[duplicated(data[key_columns]), ]
data <- data %>% 
  distinct(flow_code, week, muni_cd, sender_type, receiver_type, .keep_all = TRUE)

# Deletes those with muni_cd < 0
data <- data[data$muni_cd >= 0, ]

# Needs to fill the data. Some muni_cd have weeks with no transactions for example. 
data %>%
  complete(
    week,
    nesting(muni_cd, sender_type, receiver_type, flow_code),
    fill = list(senders = 0, receivers = 0, users = 0, value = 0, trans=0, value_w = 0),
    explicit = FALSE)

# Add Flood -> keep only date_flood. keep only matches. 
flood_data <- read_dta("//sbcdf176/PIX_Matheus$/Stata/dta/flood_weekly_2020_2022.dta")
#flood_data <- read_dta("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/flood_weekly_2020_2022.dta")
flood_data <- flood_data %>%
  select(muni_cd, week, date_flood)

data <- data %>%
  inner_join(flood_data, by = c("muni_cd", "week"))

# Add Log
data <- data %>%
  mutate(
    lvalue = log1p(value),
    ltrans = log1p(trans),
    lvalue_w = log1p(value_w),
    lsenders = log1p(senders),
    lreceivers = log1p(receivers),
    lusers = log1p(users)
  )

# Download data

write_dta(data, "//sbcdf176/PIX_Matheus$/Stata/dta/feio.dta")
# Variables: date_flood, flow_code, week, muni_cd, sender_type, receiver_type, senders, receivers, users, value, trans, value_w,
#             lsenders, lreceivers, lusers, lvalue, ltrans, lvalue_w











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








