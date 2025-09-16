

#Download_TED.r
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
source(paste0("./functions/parametrizeQuery.R"))
source(paste0(R_path,"/functions/stata_week_number.R"))
source(paste0(R_path,"/functions/gen_week_list.R"))
source(paste0(R_path,"/functions/gen_ano_mes_list.R"))
source(paste0(R_path,"/functions/stata_month_number.R"))

#source("./functions/extractTeradataDataSEDE.R")
#source("./functions/extractSQLServerData.R")
# Query -------------------------------------------------------------------

run_TED_STR <- 0
run_TED_ind_sample_STR <- 1
run_TED_SITRAF <- 0
run_TED_ind_sample_SITRAF <- 0 # RODOU!

run_TED_adoption_STR <- 0 # Exclude dead people, exclude dead firms
run_TED_adoption_SITRAF <- 0 # Exclude dead people, exclude dead firms


################# MUNICIPAL + INDIVIDUAL ################

# ------------------------------------------------------------------------------
# TED_ind_sample_SITRAF
# ------------------------------------------------------------------------------

# Variables: week, id, id_municipio_receita, tipo, value_send, trans_send, value_rec, trans_rec, value_self, trans_self

if(run_TED_ind_sample_SITRAF == 1){
# DB Settings
#DATA_SOURCE_NAME = "TED"
#connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

tipo <- c("TED_ind_sample_SITRAF_PF", "TED_ind_sample_SITRAF_PJ")
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
    
    data <- rbindlist(list(data, as.data.table(dbGetQuery(connection, parametrizedQuery)))) 
  } 
    ################ ANONIMIZACAO IDs ###############
    ### 1) Firmas
    ### 2) Pessoas fisicas
    #################################################

    ##########################################
    # 1) Des-identificacao firmas
    ##########################################
    if(tipo[i]=="TED_ind_sample_SITRAF_PJ"){
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
    if(tipo[i]=="TED_ind_sample_SITRAF_PF"){
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
  write_parquet(data, sink = paste0(path_data, tipo[i], ".parquet"))
} 
}, error = function(e) {
  print(e)
  print("Error in the SQL: TED_ind_sample_SITRAF")
})
}

# ------------------------------------------------------------------------------
# TED_SITRAF
# ------------------------------------------------------------------------------

# Variables: week, id_municipio_receita, tipo, valor_rec, trans_rec, receivers
# Variables: week, id_municipio_receita, tipo, valor_send, trans_send, senders

if(run_TED_SITRAF == 1){
# DB Settings
#DATA_SOURCE_NAME = "TED"
#connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

tipo <- c("TED_SITRAF_B2B", "TED_SITRAF_B2P", "TED_SITRAF_P2B", "TED_SITRAF_P2P") # fazer esse depois.
loop_week <- gen_week_list(2019,2022)
week_number_list <- lapply(loop_week, stata_week_number)

###
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

    data_week <- as.data.table(dbGetQuery(connection, parametrizedQuery))
    data_week <- data_week[data_week$id_municipio_receita >= 0, ]
    write_parquet(data_week, sink = paste0(path_data, tipo[i], "_teradata_", format(loop_week[[j]], "%Y-%m-%d"), ".parquet"))
    # Append the new data to the existing data table
    #data <- rbindlist(list(data, as.data.table(dbGetQuery(connection, parametrizedQuery)))) 
    #data = as.data.table(extractSQLServerData(parametrizedQuery)) #WRONG

  } #end of Loop j through weeks
  
  # Deletes those with id_municipio_receita < 0
  # data <- data[data$id_municipio_receita >= 0, ]
  
  # # Merge with old data if it exists 
  # if (file.exists(paste0(path_data, tipo[i], ".parquet"))) {
  #   # rbindlist data and old data
  #   old_data <- read_parquet(paste0(path_data, tipo[i], ".parquet"))
  #   data <- rbindlist(list(data, old_data))
  #   # Delete repeated rows
  #   data <- unique(data)
  #   rm(old_data)
  # }
  
  # write_parquet(data, sink = paste0(path_data, tipo[i], ".parquet"))
  
}  #end of Loop i through tipo[i]
#rm(data)
#rm(data_week)
}, error = function(e) {
  print(e)
  print("Error in the SQL: TED_SITRAF")
})

for(i in 1:length(tipo)) {
  data <- data.table()
  for (j in (length(loop_week) - 1):1) {
    data_week <- read_parquet(paste0(path_data, tipo[i], "_teradata_", format(loop_week[[j]], "%Y-%m-%d"), ".parquet"))
    data <- rbindlist(list(data, data_week))
    #rm(data_week)
  }
  write_parquet(data, sink = paste0(path_data, tipo[i], ".parquet"))
  #rm(data)
}
}

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

tipo <- c("TED_ind_sample_STR_PF", "TED_ind_sample_STR_PJ")
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
    write_parquet(data, sink = paste0(path_data, tipo[i], "_teradata_", format(loop_week[[j]], "%Y-%m-%d"), ".parquet"))
  } 
} 
}, error = function(e) {
  print(e)
  print("Error in the SQL: TED_ind_sample_STR")
})

tipo <- c("TED_ind_sample_STR_PF", "TED_ind_sample_STR_PJ")
loop_week <- gen_week_list(2019, 2022)
week_number_list <- lapply(loop_week, stata_week_number)
for(i in 1:length(tipo)) {
  data <- data.table()
  for (j in (length(loop_week) - 1):1) {
    data_week <- read_parquet(paste0(path_data, tipo[i], "_teradata_", format(loop_week[[j]], "%Y-%m-%d"), ".parquet"))
    data <- rbindlist(list(data, data_week))
  }
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
  write_parquet(data, sink = paste0(path_data, tipo[i], ".parquet"))
}
}

# ------------------------------------------------------------------------------
# TED_STR
# ------------------------------------------------------------------------------
# Variables: week, id_municipio_receita, tipo, valor_rec, trans_rec, receivers
# Variables: week, id_municipio_receita, tipo, valor_send, trans_send, senders

if(run_TED_STR == 1){
# DB Settings
#DATA_SOURCE_NAME = "TED"
#connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

tipo <- c("TED_STR_B2B", "TED_STR_B2P", "TED_STR_P2B", "TED_STR_P2P") #  fazer esse depois. 
loop_week <- gen_week_list(2019, 2022)
week_number_list <- lapply(loop_week, stata_week_number)

###
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
    
    
    data_week <- as.data.table(dbGetQuery(connection, parametrizedQuery))
    data_week <- data_week[data_week$id_municipio_receita >= 0, ]
    write_parquet(data_week, sink = paste0(path_data, tipo[i], "_teradata_", format(loop_week[[j]], "%Y-%m-%d"), ".parquet"))
    # Append the new data to the existing data table
    #data <- rbindlist(list(data, as.data.table(dbGetQuery(connection, parametrizedQuery)))) 
    #data = as.data.table(extractSQLServerData(parametrizedQuery)) #WRONG
  } #end of Loop j through weeks

  # # Merge with old data if it exists 
  # if (file.exists(paste0(path_data, tipo[i], ".parquet"))) {
  #   # rbindlist data and old data
  #   old_data <- read_parquet(paste0(path_data, tipo[i], ".parquet"))
  #   data <- rbindlist(list(data, old_data))
  #   # Delete repeated rows
  #   data <- unique(data)
  #   rm(old_data)
  # }
  
  
  #write_parquet(data, sink = paste0(path_data, tipo[i], ".parquet"))
  
}  #end of Loop i through tipo[i]
#rm(data)
#rm(data_week)
}, error = function(e) {
  print(e)
  print("Error in the SQL: TED_STR")
})


tipo <- c("TED_STR_B2B", "TED_STR_B2P", "TED_STR_P2B", "TED_STR_P2P") #  fazer esse depois. 
loop_week <- gen_week_list(2019, 2022)
week_number_list <- lapply(loop_week, stata_week_number)

for(i in 1:length(tipo)) {
  data <- data.table()
  for (j in (length(loop_week) - 1):1) {
    data_week <- read_parquet(paste0(path_data, tipo[i], "_teradata_", format(loop_week[[j]], "%Y-%m-%d"), ".parquet"))
    data <- rbindlist(list(data, data_week))
    #rm(data_week)
  }
  write_parquet(data, sink = paste0(path_data, tipo[i], ".parquet"))
  #rm(data)
}
}

# ------------------------------------------------------------------------------
# TED_adoption_STR
# ------------------------------------------------------------------------------
# Variables: time_id, id_municipio_receita, tipo, adopters

if(run_TED_adoption_STR == 1){
# DB Settings
DATA_SOURCE_NAME = "TED"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')

tipo <- c("TED_adoption_STR_PJ","TED_adoption_STR_PF") 
####
# Test
mun_list <- mun_convert[1:2,]
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
    #data = as.data.table(extractSQLServerData(parametrizedQuery))
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
  print("Error in the SQL: TED_adoption_STR")
})
}

# ------------------------------------------------------------------------------
# TED_adoption_SITRAF
# ------------------------------------------------------------------------------
# Variables: time_id, id_municipio_receita, tipo, adopters

if(run_TED_adoption_SITRAF == 1){
# DB Settings
DATA_SOURCE_NAME = "TED"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')

tipo <- c("TED_adoption_SITRAF") 
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
    #data = as.data.table(extractSQLServerData(parametrizedQuery))
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
  print("Error in the SQL: TED_adoption_SITRAF")
})
}
