# Process data to create dta file

#Processa_sqls.R
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

setwd("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer")
#source(paste0(R_path,"/functions/parametrizeQuery.R"))
source(paste0(R_path,"/functions/stata_week_number.R"))
source(paste0(R_path,"/functions/gen_week_list.R"))
source(paste0(R_path,"/functions/gen_ano_mes_list.R"))
source(paste0(R_path,"/functions/stata_month_number.R"))

# "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\cod_mun.dta" -> id_munic_7 and id_bcb
#  "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\municipios.dta"-> MUN_CD MUN_CD_CADMU 
# "$dta\municipios2.dta"
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
setorder(mun_convert, muni_cd)


# Cadastro_IF
Cadastro_IF <- read_parquet(paste0(path_data, "Cadastro_IF", ".parquet"))
Cadastro_IF <- Cadastro_IF %>%
  select(bank, tipo_inst, bank_type) 
Cadastro_IF <- data.table(Cadastro_IF)

# Query -------------------------------------------------------------------

run_Pix_Muni_flow <- 0 # Code ready.
run_Pix_Muni_Bank <- 0 # Code ready.
run_Pix_Muni_user <- 0 # Code ready.
run_CCS_Muni_stock <- 1 # Code ready.
run_CCS_Muni_IF <- 1 # Code ready.
run_CCS_first_account <- 1 # Code ready.
run_Pix_adoption <- 0 # Code ready. ----> Need to download.
run_Card_rec <- 1 # Code ready.
run_Card_adoption <- 0 # Code ready. ----> Need to download.
run_Boleto <- 1 # Code ready.
run_Boleto_adoption <- 0 # Code ready. ----> Need to download.
run_Credito <- 0 # Code ready. ----> Need to download.
run_Pix_ind_sample <- 0 # Code ready. ----> Need to download.
run_Boleto_ind_sample <- 0 # Code ready. ----> Need to download.
run_Card_ind_sample <- 0 # Code ready. ----> Need to download.
run_TED <- 0 # Code ready. ----> Need to download.
run_TED_adoption <- 0 # Code ready. ----> Need to download.
run_TED_ind_sample <- 0 # Code ready. ----> Need to download.

run_CCS_ind_sample <- 0 #--- Need to think of the goal----> Need to download.
run_Credito_ind_sample <- 0 #--- Need to think of the goal----> Need to download.

# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# Pix_Muni_flow
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------

# Notes: -> senders and sender_type for self is NULL
# -> flow code: 99 = self, 0 = intra, 1 = inflow, -1 = outflow

# Before Variables: flow_code, week, muni_cd, sender_type, receiver_type, senders, receivers, valor, trans, valor_w

# Pix_Muni_flow.dta
# Variables: flow_code, week, muni_cd, sender_type, receiver_type, senders, receivers, valor, trans, valor_w,
#             lsenders, lreceivers, lvalor, ltrans, lvalor_w

#Pix_Muni_flow_aggreg.dta
# Variables: week, muni_cd, sender_type, receiver_type, 
#             senders_rec, receivers_rec, valor_rec, trans_rec, valor_w_rec, 
#             senders_sent, receivers_sent, valor_sent, trans_sent, valor_w_sent
# Plus l variations.

  #Pix_Muni_flow_aggreg_rec.dta
  # Variables: week, muni_cd, tipo, 
  #             trans, valor, valor_w
  #             ltrans, lvalor, lvalor_w
  
  #Pix_Muni_flow_aggreg_send.dta
  # Variables: week, muni_cd, tipo, 
  #             trans, valor, valor_w
  #             ltrans, lvalor, lvalor_w

if(run_Pix_Muni_flow == 1){
filename <- c("Pix_Muni_flow")
for (i in 1:length(filename)) {
  files <- c("Pix_Muni_self", "Pix_Muni_inflow","Pix_Muni_outflow","Pix_Muni_intra")
  data = read_parquet(paste0(path_data, files[1], ".parquet"), as_tibble = TRUE)
  # Make sender_type = receiver_type for self transactions
  data <- data %>%
    mutate(sender_type = ifelse(flow_code == 99, receiver_type, sender_type),
            senders = ifelse(flow_code == 99, receivers, senders))

  if (length(files)>1){
    for(i in 2:length(files)) {
      temp <- read_parquet(paste0(path_data, files[i], ".parquet"), as_tibble = TRUE)
      data <- rbindlist(list(data, temp), fill=TRUE)   
    }
    rm(temp)
  } # We could have merged using all variables like we do in the following codes. 
  
  # Needs to fill the data. Some muni_cd have weeks with no transactions for example. 
  data <- data %>%
    complete(
      week,
      nesting(muni_cd, sender_type, receiver_type, flow_code),
      fill = list(senders = 0, receivers = 0, valor = 0, trans=0, valor_w = 0),
      explicit = TRUE) # explicit = TRUE makes all NA values, even the ones already there, in the choice of fill
  
  # Add Log
  data <- data %>%
    mutate(
      lvalor = log1p(valor),
      ltrans = log1p(trans),
      lvalor_w = log1p(valor_w),
      lsenders = log1p(senders),
      lreceivers = log1p(receivers)
    )
  
  # write data
  write_dta(data, paste0(path_dta,"Pix_Muni_flow",".dta"))
  # Variables: flow_code, week, muni_cd, sender_type, receiver_type, senders, receivers, valor, trans, valor_w,
  #             lsenders, lreceivers, lvalor, ltrans, lvalor_w
  
  
  ### Create aggregated Muni flow
  # Sum intra, inflow to get received
  data_temp1 <- data %>%
    filter(flow_code == 0 | flow_code == 1) %>%
    group_by(week, muni_cd, sender_type, receiver_type) %>%
    summarise(senders_rec = sum(senders, na.rm = TRUE),
              receivers_rec = sum(receivers, na.rm = TRUE), 
              valor_rec = sum(valor, na.rm = TRUE), 
              trans_rec = sum(trans, na.rm = TRUE), 
              valor_w_rec = sum(valor_w, na.rm = TRUE)) %>%
    ungroup()
  # Sum intra, outflow to get sent
  data_temp2 <- data %>%
    filter(flow_code == 0 | flow_code == -1) %>%
    group_by(week, muni_cd, sender_type, receiver_type) %>%
    summarise(senders_sent = sum(senders, na.rm = TRUE),
              receivers_sent = sum(receivers, na.rm = TRUE), 
              valor_sent = sum(valor, na.rm = TRUE), 
              trans_sent = sum(trans, na.rm = TRUE), 
              valor_w_sent = sum(valor_w, na.rm = TRUE)) %>%
    ungroup()
  
  #data_aggreg <- merge(data_temp1,data_temp2, all.x = TRUE, all.y = TRUE) 
  data_aggreg <- merge(data_temp1,data_temp2, by=c("week","muni_cd","sender_type","receiver_type") , all.x = TRUE, all.y = TRUE) 
  
  # Add Log
  data_aggreg <- data_aggreg %>%
    mutate(
      lsenders_rec = log1p(senders_rec), lreceivers_rec = log1p(receivers_rec), lvalor_rec = log1p(valor_rec), ltrans_rec = log1p(trans_rec), lvalor_w_rec = log1p(valor_w_rec),
      lsenders_sent = log1p(senders_sent), lreceivers_sent = log1p(receivers_sent), lvalor_sent = log1p(valor_sent), ltrans_sent = log1p(trans_sent), lvalor_w_sent = log1p(valor_w_sent)
    )
  
  write_dta(data_aggreg, paste0(path_dta,"Pix_Muni_flow_aggreg",".dta"))
  # Variables: week, muni_cd, sender_type, receiver_type, 
  #             senders_rec, receivers_rec, valor_rec, trans_rec, valor_w_rec, 
  #             senders_sent, receivers_sent, valor_sent, trans_sent, valor_w_sent
  # Plus l variations. 
  rm(data_temp1,data_temp2)
  
  #############
  dat_rec <- data_aggreg %>%
    group_by(week, muni_cd, receiver_type) %>%
    summarise(trans = sum(trans_rec, na.rm = TRUE),
              valor = sum(valor_rec, na.rm = TRUE),
              valor_w = sum(valor_w_rec, na.rm = TRUE)) %>%
    mutate(ltrans = log1p(trans),
           lvalor = log1p(valor),
           lvalor_w = log1p(valor_w)) %>%
    rename(tipo = receiver_type)
  write_dta(dat_rec, paste0(path_dta,"Pix_Muni_flow_aggreg_rec",".dta"))
  # Variables: week, muni_cd, tipo, 
  #             trans, valor, valor_w
  #             ltrans, lvalor, lvalor_w
  rm(dat_rec)
  dat_sent <- data_aggreg %>%
    group_by(week, muni_cd, sender_type) %>%
    summarise(trans = sum(trans_sent, na.rm = TRUE),
              valor = sum(valor_sent, na.rm = TRUE),
              valor_w = sum(valor_w_sent, na.rm = TRUE)) %>%
    mutate(ltrans = log1p(trans),
           lvalor = log1p(valor),
           lvalor_w = log1p(valor_w)) %>%
    rename(tipo = sender_type)
  write_dta(dat_sent, paste0(path_dta,"Pix_Muni_flow_aggreg_send",".dta"))
  # Variables: week, muni_cd, tipo, 
  #             trans, valor, valor_w
  #             ltrans, lvalor, lvalor_w
  rm(dat_sent)


  ################
  
  ### SUMMARY STATS
  # For each Transaction Type
  tryCatch({
  transaction_types <- matrix(c(1, 1, 1, 2, 2, 1, 2, 2), ncol = 2, byrow = TRUE)
  for(t in 1:4) {
    filtered_data <- data_aggreg %>%
      filter(sender_type == transaction_types[t, 1], receiver_type == transaction_types[t, 2])
    
    if (transaction_types[t, 1] == 1 && transaction_types[t, 2] == 1) {
      title_table <- "P2P"
    } else if (transaction_types[t, 1] == 1 && transaction_types[t, 2] == 2) {
      title_table <- "P2B"
    } else if (transaction_types[t, 1] == 2 && transaction_types[t, 2] == 1) {
      title_table <- "B2P"
    } else {
      title_table <- "B2B"
    }
    
    #stargazer(filtered_data, type = "text")
    selected_vars <- filtered_data %>% 
                      select(valor_rec, trans_rec, valor_sent, trans_sent) %>%
                      rename("Value Received" = valor_rec, "Transactions Received" = trans_rec, "Value Sent" = valor_sent, "Transactions Sent" = trans_sent)
    stargazer(as.data.frame(selected_vars), summary.stat = c("n","mean","sd","min", "p25", "median", "p75", "max"), type = "latex", title = title_table, out=paste0(path_output,"Pix_Muni_flow_aggreg_summary", t[1], t[2],".tex"))
    stargazer(as.data.frame(selected_vars), summary.stat = c("n","mean","sd","min", "p25", "median", "p75", "max"), type = "text", title = title_table)
    rm(selected_vars)
    
    # how many municipalities have 0 transactions in a week. 
    zero_stats <- filtered_data %>%
      group_by(week) %>%
      summarise(
        "Number of Municipalities" = n_distinct(muni_cd),
        "Transactions = 0" = sum(trans_rec == 0 & trans_sent == 0)) %>%
      ungroup()
    
    stargazer(zero_stats[1:20,],summary=FALSE,rownames=FALSE,
              type = "latex", title=title_table, out=paste0(path_output,"Pix_Muni_flow_aggreg_zero_stats", t[1], t[2],".tex"))
    stargazer(zero_stats[1:20,],summary=FALSE,rownames=FALSE,
              type = "text", title=title_table)
    
    rm(filtered_data,zero_stats)
  }
  rm(transaction_types,data_aggreg)

  # Self
  data_self <- data %>%
    filter(flow_code == 99)
  
  transaction_types <- c(1,2)
  for(t in 1:length(transaction_types)) {
    filtered_data <- data_self %>%
      filter(receiver_type == t)
    if      (t == 1) {title_table <- "Individuals"}
    else             {title_table <- "Firms"}
    
    #stargazer(filtered_data, type = "text")
    selected_vars <- filtered_data %>% 
      select(receivers, valor, trans) %>%
      rename("Senders" = receivers, "Value Sent" = valor, "Transactions Sent" = trans)
    stargazer(as.data.frame(selected_vars), summary.stat = c("n","mean","sd","min", "p25", "median", "p75", "max"), type = "latex", title = title_table, out=paste0(path_output,"Pix_Muni_self_summary", t,".tex"))
    stargazer(as.data.frame(selected_vars), summary.stat = c("n","mean","sd","min", "p25", "median", "p75", "max"), type = "text", title = title_table)
    rm(selected_vars)
    
    # how many municipalities have 0 transactions in a week. 
    zero_stats <- filtered_data %>%
      group_by(week) %>%
      summarise(
        "Number of Municipalities" = n_distinct(muni_cd),
        "Transactions = 0" = sum(trans == 0)) %>%
      ungroup()
    
    stargazer(zero_stats[1:20,],summary=FALSE,rownames=FALSE,
              type = "latex", title=title_table, out=paste0(path_output,"Pix_Muni_self_zero_stats", t,".tex"))
    stargazer(zero_stats[1:20,],summary=FALSE,rownames=FALSE,
              type = "text", title=title_table)
  
    rm(filtered_data,zero_stats)
  }
  rm(data_self, transaction_types, data)
  }, error = function(e) {
    cat("An error occurred at Pix Muni Flow Summary Stats:", conditionMessage(e), "\n")
  })
  

}
}

#take a look at the summary stats.

# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# Pix_Muni_Bank
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------

# Before Variables: week, muni_cd, tipo, bank,
#                   value_send, trans_send, send_users, value_send_w, value_rec, trans_rec, rec_users, value_rec_w

  #Pix_Muni_Bank.dta and Pix_Muni_Bank_self.dta
  # Variables:  week, muni_cd, tipo, bank, tipo_inst, bank_type,
  #             value_send, trans_send, send_users, value_send_w, value_rec, trans_rec, rec_users, value_rec_w
  #             lvalue_send, ltrans_send, lsend_users, lvalue_send_w, lvalue_rec, ltrans_rec, lrec_users, lvalue_rec_w

if(run_Pix_Muni_Bank == 1){
filename <- c("Pix_Muni_Bank","Pix_Muni_Bank_self")
for (i in 1:length(filename)) {
  if (i == 1) {
    files <- c("Pix_Muni_Bank_rec", "Pix_Muni_Bank_send")
  } else {
    files <- c("Pix_Muni_Bank_self_rec","Pix_Muni_Bank_self_send")
  }
  
  data <- read_parquet(paste0(path_data, files[1], ".parquet"), as_tibble = TRUE)
  if (length(files)>1){
    for (j in 2:length(files)) {
      temp <- read_parquet(paste0(path_data, files[j], ".parquet"), as_tibble = TRUE)
      data <- merge(data, temp, by= c("week","muni_cd","tipo","bank"), all.x = TRUE, all.y = TRUE)
    }
    rm(temp)
  }
  
  data <- data %>%
    complete(
      week,
      nesting(muni_cd, tipo, bank),
      fill = list(value_send = 0, trans_send = 0, send_users = 0, value_send_w = 0, value_rec = 0, trans_rec = 0, rec_users = 0, value_rec_w = 0),
      explicit = TRUE) # explicit = TRUE makes all NA values, even the ones already there, in the choice of fill
  
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
  
  #Add bank type
  data <- merge(data, Cadastro_IF, by="bank", all = FALSE)

  # Download data
  write_dta(data, paste0(path_dta,filename[i],".dta"))
  # Variables:  week, muni_cd, tipo, bank, tipo_inst, bank_type,
  #             value_send, trans_send, send_users, value_send_w, value_rec, trans_rec, rec_users, value_rec_w
  #             lvalue_send, ltrans_send, lsend_users, lvalue_send_w, lvalue_rec, ltrans_rec, lrec_users, lvalue_rec_w
  cat("Pix Muni Bank saved!")
  rm(data)
}
}

# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# Pix_Muni_user
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------

# Before Variables: week, muni_cd, tipo, users
#                   week, muni_cd, tipo, senders
#                   week, muni_cd, tipo, receivers

  #Pix_Muni_user.dta
  # Variables: week, muni_cd, tipo, 
  #             users, senders, receivers
  #             lusers, lsenders, lreceivers
if(run_Pix_Muni_user == 1){
filename <- c("Pix_Muni_user")
for (i in 1:length(filename)) {
  files <- c("Pix_Muni_user", "Pix_Muni_user_rec", "Pix_Muni_user_send")
  data <- read_parquet(paste0(path_data, files[1], ".parquet"), as_tibble = TRUE)
  
  if (length(files)>1){
    for (j in 2:length(files)) {
      temp <- read_parquet(paste0(path_data, files[j], ".parquet"), as_tibble = TRUE)
      data <- merge(data, temp, by= c("week","muni_cd","tipo"), all.x = TRUE, all.y = TRUE)
    }
    rm(temp)
  }

  data <- data %>%
    complete(
      week,
      nesting(muni_cd, tipo),
      fill = list(users = 0, senders = 0, receivers = 0),
      explicit = TRUE) # explicit = TRUE makes all NA values, even the ones already there, in the choice of fill
  # Add Log
  data <- data %>%
    mutate(lusers = log1p(users),
           lsenders = log1p(senders),
           lreceivers = log1p(receivers))
  
  # Download data
  write_dta(data, paste0(path_dta,filename[i],".dta"))
  # Variables: week, muni_cd, tipo, users
  #             lusers, lsenders, lreceivers
  cat("Pix Muni users saved!")
  
  
  ### SUMMARY STATS
  # For each Tipo
  tryCatch({
    transaction_types <- c(1,2)
    for(t in 1:length(transaction_types)) {
      filtered_data <- data %>%
        filter(tipo == t)
      if(t == 1) {title_table <- "Individuals"}
      else       {title_table <- "Firms"}
      
      selected_vars <- filtered_data %>% 
        select(users) %>%
        rename("Users" = users)
      stargazer(as.data.frame(selected_vars), summary.stat = c("n","mean","sd","min", "p25", "median", "p75", "max"), type = "latex", title = title_table, out=paste0(path_output,"adoption_ind_summary", t,".tex"))
      stargazer(as.data.frame(selected_vars), summary.stat = c("n","mean","sd","min", "p25", "median", "p75", "max"), type = "text", title = title_table)
      rm(selected_vars,filtered_data)
    }
  }, error = function(e) {
    cat("An error occurred at Pix Muni Users Summary Stats:", conditionMessage(e), "\n")
  })
  
  #### Make line graphs. 
  
  rm(data)
}
}
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# CCS_Muni
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# CCS_Muni_stock
# ------------------------------------------------------------------------------
# Before Variables: week, id_municipio_receita, tipo, muni_stock, muni_stock_w, banked_pop

  #CCS_Muni_stock_v2.dta
  # Variables: week, muni_cd, tipo, muni_stock, muni_stock_w, banked_pop
  #             lmuni_stock, lmuni_stock_w, lbanked_pop

# [1] "Error in CCS_Muni_stock: Error: '//sbcdf176/PIX_Matheus$/Stata/dta//CCS_Muni_stock.dta' does not exist.\n"
# Parece que nunca foi rodado
if(run_CCS_Muni_stock == 1){
filename <- c("CCS_Muni_stock")
for (i in 1:length(filename)) {
  if(i == 1){files <- c("CCS_Muni_stock_PF", "CCS_Muni_stock_PJ")} 

  data <- read_parquet(paste0(path_data, files[1], "_v2.parquet"), as_tibble = TRUE)
  if (length(files)>1){
    for (j in 2:length(files)) {
      temp <- read_parquet(paste0(path_data, files[j], "_v2.parquet"), as_tibble = TRUE)
      data <- rbindlist(list(data, temp), fill=TRUE)
      #data <- merge(data, temp, by= c("week","id_municipio_receita","tipo"), all.x = TRUE, all.y = TRUE)
    }
    rm(temp)
  }

  data <- data %>%
    complete(
      week,
      nesting(id_municipio_receita, tipo),
      fill = list(muni_stock = 0, muni_stock_w = 0, banked_pop = 0),
      explicit = TRUE)

  # Add Log
  data <- data %>%
    mutate(
      lmuni_stock = log1p(muni_stock),
      lmuni_stock_w = log1p(muni_stock_w),
      lbanked_pop = log1p(banked_pop)
    )

  # Add muni_cd
  cat("Converting municipality code for CCS_Muni_stock, number of rows:", nrow(data))
  data <- data %>% mutate(id_municipio_receita = as.integer(id_municipio_receita))
  mun_convert2 <- mun_convert %>% select(muni_cd, id_municipio_receita)
  data <- merge(data, mun_convert2, by="id_municipio_receita", all.x = FALSE, all.y = FALSE)
  data <- data %>% select(-id_municipio_receita)
  rm(mun_convert2)
  cat("Converting municipality DONE for CCS_Muni_stock, number of rows:", nrow(data))


  # Download data
  write_dta(data, paste0(path_dta,filename[i],"_v2.dta"))
  # Variables: week, muni_cd, tipo, muni_stock, muni_stock_w, banked_pop
  #             lmuni_stock, lmuni_stock_w, lbanked_pop
  cat("CCS Muni Stock done!")
  ### SUMMARY STATS
  tryCatch({
    transaction_types <- c(1,2)
    for(t in 1:length(transaction_types)) {
      filtered_data <- data %>%
        filter(receiver_type == t)
      if(t == 1) {title_table <- "Individuals"}
      else       {title_table <- "Firms"}
  
      selected_vars <- filtered_data %>% 
        select(muni_stock, muni_stock_w, banked_pop) %>%
        rename("Stock" = muni_stock, "Stock Winsorized" = muni_stock_w, "Population with Bank Account" = banked_pop)
      stargazer(as.data.frame(selected_vars), summary.stat = c("n","mean","sd","min", "p25", "median", "p75", "max"), type = "latex", title = title_table, out=paste0(path_output,"CCS_Muni_stock_summary", t,".tex"))
      stargazer(as.data.frame(selected_vars), summary.stat = c("n","mean","sd","min", "p25", "median", "p75", "max"), type = "text", title = title_table)
      rm(selected_vars,filtered_data)
    }
  }, error = function(e) {
    cat("An error occurred at CCS_Muni_stock Summary Stats:", conditionMessage(e), "\n")
  })
  #### Make line graphs. 
  
  rm(transaction_types, data)
}
}
# ------------------------------------------------------------------------------
# CCS_Muni_IF
# ------------------------------------------------------------------------------
# Before Variables: week, id_municipio_receita, tipo, bank, stock
  
  # CCS_Muni_IF_PF_v2.dta and CCS_Muni_IF_PJ_v2.dta
  # Variables: week, muni_cd, tipo, bank, tipo_inst, bank_type,
  #             stock,
  #             lstock
  #CCS_Muni_HHI_PF_v2.dta and CCS_Muni_HHI_PJ_v2.dta
  # Variables: week, muni_cd, tipo, HHI_account

if(run_CCS_Muni_IF == 1){
filename <- c("CCS_Muni_IF_PF", "CCS_Muni_IF_PJ")
filename2 <- c("CCS_Muni_HHI_PF", "CCS_Muni_HHI_PJ")
for (i in 1:length(filename)) {
  if (i == 1) {
    files <- c("CCS_Muni_IF_stock_PF") #c("CCS_Muni_IF_opening_PF", "CCS_Muni_IF_closing_PF", "CCS_Muni_IF_stock_PF")
  } else if (i==2) {
    files <- c("CCS_Muni_IF_stock_PJ") #c("CCS_Muni_IF_opening_PJ", "CCS_Muni_IF_closing_PJ", "CCS_Muni_IF_stock_PJ")
  }
  
  data <- read_parquet(paste0(path_data, files[1], "_v2.parquet"), as_tibble = TRUE)

  if (length(files)>1){
    for (j in 2:length(files)) {
      temp <- read_parquet(paste0(path_data, files[j], "_v2.parquet"), as_tibble = TRUE)
      data <- merge(data, temp, by= c("week","id_municipio_receita","tipo","bank"), all.x = TRUE, all.y = TRUE)
    }
    rm(temp)
  }
  
  # Needs to fill the data. Some id_municipio_receita have weeks with no transactions for example. 
  data <- data %>%
    complete(
      week,
      nesting(id_municipio_receita, tipo, bank),
      fill = list(stock = 0), #list(opening = 0, closing = 0, stock = 0),
      explicit = TRUE) # explicit = TRUE makes all NA values, even the ones already there, in the choice of fill
  
  # Add Log
  data <- data %>%
    mutate(
      lstock = log1p(stock)
      #,lopening = log1p(opening)
      #,lclosing = log1p(closing)
    )
  
  # Add muni_cd
  data <- data %>% mutate(id_municipio_receita = as.integer(id_municipio_receita))
  mun_convert2 <- mun_convert %>% select(muni_cd, id_municipio_receita)
  data <- merge(data, mun_convert2, by="id_municipio_receita", all.x = FALSE, all.y = FALSE)
  data <- data %>% select(-id_municipio_receita)
  rm(mun_convert2)

  #Add bank type
  data <- merge(data, Cadastro_IF, by="bank", all = FALSE)

  # Download data
  write_dta(data, paste0(path_dta,filename[i],"_v2.dta"))
  cat("CCS Muni IF done!")
  # Variables: week, muni_cd, tipo, bank, tipo_inst, bank_type,
  #             stock,
  #             lstock
  
  #-----------------------
  # Calculate the HHI for the bank accounts. 
  #-----------------------
  temp <- data %>%
    group_by(week, muni_cd, tipo) %>%
    summarise(sum_stock = sum(stock, na.rm = TRUE)) %>% # Note that summarise can sometimes be written with z and give an error
    ungroup()
  data <- merge(data, temp, by=c("week", "muni_cd", "tipo"), all.x = FALSE, all.y = FALSE)
  data <- data %>%
    mutate(hhi = (stock/sum_stock)^2) %>%
    group_by(week, muni_cd, tipo) %>%
    summarise(HHI_account = sum(hhi, na.rm = TRUE)) %>%
    ungroup()
  
  # Download data
  
  write_dta(data, paste0(path_dta,filename2[i],"_v2.dta"))
  # Variables: week, muni_cd, tipo, HHI_account
  cat("CCS Muni HHI done!")
  rm(temp)
  
  ### SUMMARY STATS
  tryCatch({
    
    data_aggreg <- data %>%
      group_by(week, muni_cd, tipo) %>%
      summarise(opening = sum(opening, na.rm = TRUE),
                stock = sum(stock, na.rm = TRUE),
                closing = sum(closing, na.rm = TRUE)) %>%
      ungroup()
    data_aggreg <- merge(data_aggreg, data_hhi, by=c("week", "muni_cd", "tipo"), all.x = FALSE, all.y = FALSE)
    
    transaction_types <- c(1,2)
    for(t in 1:length(transaction_types)) {
      filtered_data <- data_aggreg %>%
        filter(receiver_type == t)
      if(t == 1) {title_table <- "Individuals"}
      else       {title_table <- "Firms"}
      
      #stargazer(filtered_data, type = "text")
      selected_vars <- filtered_data %>%
        select(stock, opening, closing, HHI_account) %>%
        rename("Stock" = stock, "Opening" = opening, "Closing" = closing, "HHI Stock" = HHI_account)
      stargazer(as.data.frame(selected_vars), summary.stat = c("n","mean","sd","min", "p25", "median", "p75", "max"), type = "latex", title = title_table, out=paste0(path_output,"CCS_Muni_summary", t,".tex"))
      stargazer(as.data.frame(selected_vars), summary.stat = c("n","mean","sd","min", "p25", "median", "p75", "max"), type = "text", title = title_table)
      rm(selected_vars,filtered_data)
    }
    rm(data_aggreg, transaction_types, data)
  }, error = function(e) {
    cat("An error occurred at CCS_Muni_IF Summary Stats:", conditionMessage(e), "\n")
  })
}
}

# ------------------------------------------------------------------------------
# CCS_first_account
# ------------------------------------------------------------------------------
# Before Variables: dia, id_municipio_receita, tipo, first_account

  #CCS_Muni_first_account_v2.dta
  # Variables: week, muni_cd, tipo, first_account
  #             lfirst_account
if(run_CCS_first_account == 1){
filename <- c("CCS_Muni_first_account")
for (i in 1:length(filename)) {
  if (i == 1) {files <- c("CCS_first_account_PF", "CCS_first_account_PJ")} 
  data <- read_parquet(paste0(path_data, files[1], "_v2.parquet"), as_tibble = TRUE)
  # Filter Dates too old or wrong
  data <- data %>% filter(dia >= as.Date("2018-01-01") & dia <= as.Date("2023-12-31"))

  #Add Week variable
  data$week <- stata_week_number(data$dia)
  
  #Collapse by week
  data <- data %>%
    group_by(id_municipio_receita, tipo, week) %>%
    summarise(first_account = sum(first_account, na.rm = TRUE)) %>%
    ungroup()
  
  if (length(files)>1){
    for (j in 2:length(files)) {
      temp <- read_parquet(paste0(path_data, files[j], "_v2.parquet"), as_tibble = TRUE)
      temp <- temp %>% filter(dia >= as.Date("2018-01-01") & dia <= as.Date("2023-12-31"))
      temp$week <- stata_week_number(temp$dia)
      temp <- temp %>%
        group_by(id_municipio_receita, tipo, week) %>%
        summarise(first_account = sum(first_account, na.rm = TRUE)) %>%
        ungroup()
      data <- rbindlist(list(data, temp), fill=TRUE)
      #data <- merge(data, temp, by= c("week","id_municipio_receita","tipo"), all.x = TRUE, all.y = TRUE)
    }
    rm(temp)
  }

  #Complete
  data <- data %>%
    complete(
      week,
      nesting(id_municipio_receita, tipo),
      fill = list(first_account = 0),
      explicit = TRUE)

  # Add Log
  data <- data %>%
    mutate(
      lfirst_account = log1p(first_account)
    )
  
  # Add muni_cd
  cat("Converting municipality code for CCS_Muni_IF, number of rows:", nrow(data))
  data <- data %>% mutate(id_municipio_receita = as.integer(id_municipio_receita))
  mun_convert2 <- mun_convert %>% select(muni_cd, id_municipio_receita)
  data <- merge(data, mun_convert2, by="id_municipio_receita", all.x = FALSE, all.y = FALSE)
  data <- data %>% select(-id_municipio_receita)
  rm(mun_convert2)
  cat("Converting municipality DONE for CCS_Muni_IF, number of rows:", nrow(data))

  # Download data
  write_dta(data, paste0(path_dta,filename[i],"_v2.dta"))
  # Variables: week, muni_cd, tipo, first_account
  #             lfirst_account
  cat("CCS Muni first account done!")
  ### SUMMARY STATS
  
  # Variables: dia, muni_cd, tipo, first_account
  
  tryCatch({
    transaction_types <- c(1,2)
    for(t in 1:length(transaction_types)) {
      filtered_data <- data %>%
        filter(tipo == t)
      if(t == 1) {title_table <- "Individuals"}
      else       {title_table <- "Firms"}
      
      selected_vars <- filtered_data %>% 
        select(first_account) %>%
        rename("First Bank Account" = first_account)
      stargazer(as.data.frame(selected_vars), summary.stat = c("n","mean","sd","min", "p25", "median", "p75", "max"), type = "latex", title = title_table, out=paste0(path_output,"CCS_first_account_summary", t,".tex"))
      stargazer(as.data.frame(selected_vars), summary.stat = c("n","mean","sd","min", "p25", "median", "p75", "max"), type = "text", title = title_table)
      rm(selected_vars,filtered_data)
    }
  }, error = function(e) {
    cat("An error occurred at CCS_first_account Summary Stats:", conditionMessage(e), "\n")
  })
  
  #### Make line graphs. 
  rm(transaction_types, data)
}
}

# ------------------------------------------------------------------------------
# Pix_adoption
# ------------------------------------------------------------------------------

# Before Variables: time_id, muni_cd, tipo, adopters

  # Pix_adoption.dta
  # Variables: time_id, muni_cd, tipo, adopters
  #             ladopters
if(run_Pix_adoption == 1){
filename <- c("Pix_adoption")
for (i in 1:length(filename)) {
  if (i == 1) {files <- c("Pix_adoption")} 
  data <- read_parquet(paste0(path_data, files[1], ".parquet"), as_tibble = TRUE)

  if (length(files)>1){
    for (j in 2:length(files)) {
      temp <- read_parquet(paste0(path_data, files[j], ".parquet"), as_tibble = TRUE)
      data <- rbindlist(list(data, temp), fill=TRUE)
    }
    rm(temp)
  }

  #Complete
  data <- data %>%
    complete(
      time_id,
      nesting(muni_cd, tipo),
      fill = list(adopters = 0),
      explicit = TRUE)

  # Add Log
  data <- data %>%
    mutate(ladopters = log1p(adopters))

  # Download data
  write_dta(data, paste0(path_dta,filename[i],".dta"))
  # Variables: time_id, muni_cd, tipo, adopters
  #             ladopters
  cat("Pix_adoption done!")
  ### SUMMARY STATS
}
}

# ------------------------------------------------------------------------------
# Card_rec
# ------------------------------------------------------------------------------

# Before Variables: week, id_municipio, tipo, receivers, valor

  # Card_rec.dta
  # Variables: week, muni_cd, tipo, receivers, valor, receivers_credit, valor_credit, receivers_debit, valor_debit
  #            lreceivers, lvalor, lreceivers_credit, lvalor_credit, lreceivers_debit, lvalor_debit
if(run_Card_rec == 1){
filename <- c("Card_rec")
for (i in 1:length(filename)) {
  if (i == 1) {files <- c("Credit_card_rec","Debit_card_rec")}

  data <- read_parquet(paste0(path_data, files[1], ".parquet"), as_tibble = TRUE)
  data <- data %>% rename(receivers_credit = receivers, valor_credit = valor)
  if (length(files)>1){
    for (j in 2:length(files)) {
      temp <- read_parquet(paste0(path_data, files[j], ".parquet"), as_tibble = TRUE)
      temp <- temp %>% rename(receivers_debit = receivers, valor_debit = valor)
      data <- merge(data, temp, by=c("week","id_municipio","tipo"), all.x = TRUE, all.y = TRUE)
    }
    rm(temp)
  }

  data <- data %>%
    complete(
      week,
      nesting(id_municipio, tipo),
      fill = list(receivers_credit = 0, valor_credit = 0, receivers_debit = 0, valor_debit = 0),
      explicit = TRUE)
  cat("19")
  # Add credit and debit
  data <- data %>%
    mutate(receivers = receivers_credit + receivers_debit,
           valor = valor_credit + valor_debit)
  # Add Log
  data <- data %>%
    mutate(lreceivers = log1p(receivers),
           lvalor = log1p(valor),
           lreceivers_credit = log1p(receivers_credit),
            lvalor_credit = log1p(valor_credit),
            lreceivers_debit = log1p(receivers_debit),
            lvalor_debit = log1p(valor_debit))
  cat("20")
  # Add muni_cd
  cat("Converting municipality code for Card_rec, number of rows:", nrow(data))
  data <- data %>% mutate(id_municipio = as.integer(id_municipio))
  mun_convert2 <- mun_convert %>% select(muni_cd, id_municipio)
  data <- merge(data, mun_convert2, by="id_municipio", all.x = FALSE, all.y = FALSE)
  data <- data %>% select(-id_municipio)
  rm(mun_convert2)
  cat("Converting municipality DONE for Card_rec, number of rows:", nrow(data))

  # Download data
  write_dta(data, paste0(path_dta,filename[i],".dta"))
  # Variables: week, muni_cd, tipo, receivers, valor, receivers_credit, valor_credit, receivers_debit, valor_debit
  #            lreceivers, lvalor, lreceivers_credit, lvalor_credit, lreceivers_debit, lvalor_debit
  cat("Card rec done!")
  ### SUMMARY STATS

  rm(data)
}
}

# ------------------------------------------------------------------------------
# Card_adoption
# ------------------------------------------------------------------------------
    # Before Variables: time_id, tipo, id_municipio, adopters, adopters_credit, adopters_debit

  # Card_adoption.dta
  # Variables: time_id, muni_cd, tipo, adopters, adopters_credit, adopters_debit
  #             ladopters, ladopters_credit, ladopters_debit
if(run_Card_adoption == 1){
filename <- c("Card_adoption")
for (i in 1:length(filename)) {
  files <- c("Card_adoption")
  data <- read_parquet(paste0(path_data, files[1], ".parquet"), as_tibble = TRUE)

  if (length(files)>1){
    for (j in 2:length(files)) {
      temp <- read_parquet(paste0(path_data, files[j], ".parquet"), as_tibble = TRUE)
      data <- rbindlist(list(data, temp), fill=TRUE)
      }
    rm(temp)
  }

  #Complete
  data <- data %>%
    complete(
      time_id,
      nesting(id_municipio, tipo),
      fill = list(adopters = 0, adopters_credit = 0, adopters_debit = 0),
      explicit = TRUE)

  # Add Log
  data <- data %>%
    mutate(ladopters = log1p(adopters),
           ladopters_credit = log1p(adopters_credit),
           ladopters_debit = log1p(adopters_debit))

  # Add muni_cd
  cat("Converting municipality code for Card_adoption, number of rows:", nrow(data))
  data <- data %>% mutate(id_municipio = as.integer(id_municipio))
  mun_convert2 <- mun_convert %>% select(muni_cd, id_municipio)
  data <- merge(data, mun_convert2, by="id_municipio", all.x = FALSE, all.y = FALSE)
  data <- data %>% select(-id_municipio)
  rm(mun_convert2)
  cat("Converting municipality DONE for Card_adoption, number of rows:", nrow(data))
  
  # Download data
  write_dta(data, paste0(path_dta,filename[i],".dta"))
  # Variables: time_id, muni_cd, tipo, adopters, adopters_credit, adopters_debit
  #             ladopters, ladopters_credit, ladopters_debit
  cat("Card_adoption done!")
  ### SUMMARY STATS

}
}


# ------------------------------------------------------------------------------
# Boleto 
# ------------------------------------------------------------------------------
# week, id_municipio_receita, tipo, receivers, trans_rec, valor_rec
# week, id_municipio_receita, tipo, senders, trans_send, valor_send

  # Boleto.dta
  # Variables: week, muni_cd, tipo, senders, trans_send, valor_send, receivers, trans_rec, valor_rec
  #             lsenders, ltrans_send, lvalor_send, lreceivers, ltrans_rec, lvalor_rec
if(run_Boleto == 1){
filename <- c("Boleto")
for (i in 1:length(filename)) {
  files <- c("Boleto_rec", "Boleto_send")
  data <- read_parquet(paste0(path_data, files[1], ".parquet"), as_tibble = TRUE)
  if (length(files)>1){
    for (j in 2:length(files)) {
      temp <- read_parquet(paste0(path_data, files[j], ".parquet"), as_tibble = TRUE)
      data <- merge(data, temp, by=c("week","id_municipio_receita","tipo"), all.x = TRUE, all.y = TRUE)
    }
    rm(temp)
  }

  data <- data %>%
    complete(
      week,
      nesting(id_municipio_receita, tipo),
      fill = list(senders = 0, trans_send = 0, valor_send = 0, receivers = 0, trans_rec = 0, valor_rec = 0),
      explicit = TRUE)
  cat("19")
  # Add Log
  data <- data %>%
    mutate(lsenders = log1p(senders),
           ltrans_send = log1p(trans_send),
           lvalor_send = log1p(valor_send),
           lreceivers = log1p(receivers),
           ltrans_rec = log1p(trans_rec),
            lvalor_rec = log1p(valor_rec))
  cat("20")
  # Add muni_cd
  cat("Converting municipality code for Boleto, number of rows:", nrow(data))
  data <- data %>% mutate(id_municipio_receita = as.integer(id_municipio_receita))
  mun_convert2 <- mun_convert %>% select(muni_cd, id_municipio_receita)
  data <- merge(data, mun_convert2, by="id_municipio_receita", all.x = FALSE, all.y = FALSE)
  data <- data %>% select(-id_municipio_receita)
  rm(mun_convert2)
  cat("Converting municipality DONE for Boleto, number of rows:", nrow(data))
  
  # Download data
  write_dta(data, paste0(path_dta,filename[i],".dta"))
  # Variables: week, muni_cd, tipo, senders, trans_send, valor_send, receivers, trans_rec, valor_rec
  #             lsenders, ltrans_send, lvalor_send, lreceivers, ltrans_rec, lvalor_rec
  cat("Boleto done!")
  rm(data)
}
}
# ------------------------------------------------------------------------------
# Boleto_adoption
# ------------------------------------------------------------------------------

# Before Variables: time_id, tipo, adopters, id_municipio_receita

  # Boleto_adoption.dta
  # Variables: time_id, muni_cd, tipo, adopters
  #             ladopters
if(run_Boleto_adoption == 1){
filename <- c("Boleto_adoption")
for (i in 1:length(filename)) {
  if (i == 1) {files <- c("Boleto_adoption")} 
  data <- read_parquet(paste0(path_data, files[1], ".parquet"), as_tibble = TRUE)

  if (length(files)>1){
    for (j in 2:length(files)) {
      temp <- read_parquet(paste0(path_data, files[j], ".parquet"), as_tibble = TRUE)
      data <- rbindlist(list(data, temp), fill=TRUE)
    }
    rm(temp)
  }

  #Complete
  data <- data %>%
    complete(
      time_id,
      nesting(id_municipio_receita, tipo),
      fill = list(adopters = 0),
      explicit = TRUE)

  # Add Log
  data <- data %>%
    mutate(ladopters = log1p(adopters))

  # Add muni_cd
  cat("Converting municipality code for Boleto_adoption, number of rows:", nrow(data))
  data <- data %>% mutate(id_municipio_receita = as.integer(id_municipio_receita))
  mun_convert2 <- mun_convert %>% select(muni_cd, id_municipio_receita)
  data <- merge(data, mun_convert2, by="id_municipio_receita", all.x = FALSE, all.y = FALSE)
  data <- data %>% select(-id_municipio_receita)
  rm(mun_convert2)
  cat("Converting municipality DONE for Boleto_adoption, number of rows:", nrow(data))

  # Download data
  write_dta(data, paste0(path_dta,filename[i],".dta"))
  # Variables: time_id, muni_cd, tipo, adopters
  #             ladopters
  cat("Boleto_adoption done!")
  ### SUMMARY STATS
}
}

# ------------------------------------------------------------------------------
# Credito
# ------------------------------------------------------------------------------
# Before PF variables: time_id, id_municipio, new_users, new_users_if, new_users_cg, new_operation, valor, users, qtd, valor_consignado, users_consignado, qtd_consignado, new_users_consignado, valor_emp_pessoal, users_emp_pessoal, qtd_emp_pessoal, new_users_emp_pessoal, valor_veiculo, users_veiculo, qtd_veiculo, new_users_veiculo, valor_imob, users_imob, qtd_imob, new_users_imob, valor_cartao, users_cartao, qtd_cartao, new_users_cartao, valor_rural, users_rural, qtd_rural, new_users_rural, valor_outros, users_outros, qtd_outros, new_users_outros
# Before PJ variables: time_id, id_municipio, new_users, new_users_if, new_users_cg, new_operation, valor, users, qtd, valor_cartao, users_cartao, qtd_cartao, new_users_cartao, valor_wc, users_wc, qtd_wc, new_users_wc, valor_invest, users_invest, qtd_invest, new_users_invest, valor_desc_ovdraft, users_desc_ovdraft, qtd_desc_ovdraft, new_users_desc_ovdraft, valor_desc_reb, users_desc_reb, qtd_desc_reb, new_users_desc_reb, valor_comex, users_comex, qtd_comex, new_users_comex, valor_outros, users_outros, qtd_outros, new_users_outros

# Credito_Muni_PF.dta
# Variables: time_id, id_municipio, new_users, new_users_if, new_users_cg, new_operation, valor, users, qtd, valor_consignado, users_consignado, qtd_consignado, new_users_consignado, valor_emp_pessoal, users_emp_pessoal, qtd_emp_pessoal, new_users_emp_pessoal, valor_veiculo, users_veiculo, qtd_veiculo, new_users_veiculo, valor_imob, users_imob, qtd_imob, new_users_imob, valor_cartao, users_cartao, qtd_cartao, new_users_cartao, valor_rural, users_rural, qtd_rural, new_users_rural, valor_outros, users_outros, qtd_outros, new_users_outros
#                                     lnew_users, lnew_users_if, lnew_users_cg, lnew_operation, lvalor, lusers, lqtd, lvalor_consignado, lusers_consignado, lqtd_consignado, lnew_users_consignado, lvalor_emp_pessoal, lusers_emp_pessoal, lqtd_emp_pessoal, lnew_users_emp_pessoal, lvalor_veiculo, lusers_veiculo, lqtd_veiculo, lnew_users_veiculo, lvalor_imob, lusers_imob, lqtd_imob, lnew_users_imob, lvalor_cartao, lusers_cartao, lqtd_cartao, lnew_users_cartao, lvalor_rural, lusers_rural, lqtd_rural, lnew_users_rural, lvalor_outros, lusers_outros, lqtd_outros, lnew_users_outros

# Credito_Muni_PJ.dta
# Variables: time_id, id_municipio, new_users, new_users_if, new_users_cg, new_operation, valor, users, qtd, valor_cartao, users_cartao, qtd_cartao, new_users_cartao, valor_wc, users_wc, qtd_wc, new_users_wc, valor_invest, users_invest, qtd_invest, new_users_invest, valor_desc_ovdraft, users_desc_ovdraft, qtd_desc_ovdraft, new_users_desc_ovdraft, valor_desc_reb, users_desc_reb, qtd_desc_reb, new_users_desc_reb, valor_comex, users_comex, qtd_comex, new_users_comex, valor_outros, users_outros, qtd_outros, new_users_outros
#                                     lnew_users, lnew_users_if, lnew_users_cg, lnew_operation, lvalor, lusers, lqtd, lvalor_cartao, lusers_cartao, lqtd_cartao, lnew_users_cartao, lvalor_wc, lusers_wc, lqtd_wc, lnew_users_wc, lvalor_invest, lusers_invest, lqtd_invest, lnew_users_invest, lvalor_desc_ovdraft, lusers_desc_ovdraft, lqtd_desc_ovdraft, lnew_users_desc_ovdraft, lvalor_desc_reb, lusers_desc_reb, lqtd_desc_reb, lnew_users_desc_reb, lvalor_comex, lusers_comex, lqtd_comex, lnew_users_comex, lvalor_outros, lusers_outros, lqtd_outros, lnew_users_outros

if(run_Credito == 1){
filename <- c("Credito_Muni_PF")
for (i in 1:length(filename)) {
  if (i == 1) {files <- c("Credito_Muni_PF")}

  data <- read_parquet(paste0(path_data, files[1], ".parquet"), as_tibble = TRUE)
  if (length(files)>1){
    for (j in 2:length(files)) {
      temp <- read_parquet(paste0(path_data, files[j], ".parquet"), as_tibble = TRUE)
      data <- merge(data, temp, by=c("time_id","id_municipio"), all.x = TRUE, all.y = TRUE)
    }
    rm(temp)
  }

  data <- data %>%
    complete(
      time_id,
      nesting(id_municipio),
      fill = list(new_users = 0, new_users_if = 0, new_users_cg = 0, new_operation = 0, valor = 0, users = 0, qtd = 0, valor_consignado = 0, users_consignado = 0, qtd_consignado = 0, new_users_consignado = 0, valor_emp_pessoal = 0, users_emp_pessoal = 0, qtd_emp_pessoal = 0, new_users_emp_pessoal = 0, valor_veiculo = 0, users_veiculo = 0, qtd_veiculo = 0, new_users_veiculo = 0, valor_imob = 0, users_imob = 0, qtd_imob = 0, new_users_imob = 0, valor_cartao = 0, users_cartao = 0, qtd_cartao = 0, new_users_cartao = 0, valor_rural = 0, users_rural = 0, qtd_rural = 0, new_users_rural = 0, valor_outros = 0, users_outros = 0, qtd_outros = 0, new_users_outros = 0),
      explicit = TRUE)
  cat("19")
  # Add Log
  data <- data %>%
    mutate(lnew_users = log1p(new_users),
            lnew_users_if = log1p(new_users_if),
            lnew_users_cg = log1p(new_users_cg),
            lnew_operation = log1p(new_operation),
            lvalor = log1p(valor),
            lusers = log1p(users),
            lqtd = log1p(qtd),
            lvalor_consignado = log1p(valor_consignado),
            lusers_consignado = log1p(users_consignado),
            lqtd_consignado = log1p(qtd_consignado),
            lnew_users_consignado = log1p(new_users_consignado),
            lvalor_emp_pessoal = log1p(valor_emp_pessoal),
            lusers_emp_pessoal = log1p(users_emp_pessoal),
            lqtd_emp_pessoal = log1p(qtd_emp_pessoal),
            lnew_users_emp_pessoal = log1p(new_users_emp_pessoal),
            lvalor_veiculo = log1p(valor_veiculo),
            lusers_veiculo = log1p(users_veiculo),
            lqtd_veiculo = log1p(qtd_veiculo),
            lnew_users_veiculo = log1p(new_users_veiculo),
            lvalor_imob = log1p(valor_imob),
            lusers_imob = log1p(users_imob),
            lqtd_imob = log1p(qtd_imob),
            lnew_users_imob = log1p(new_users_imob),
            lvalor_cartao = log1p(valor_cartao),
            lusers_cartao = log1p(users_cartao),
            lqtd_cartao = log1p(qtd_cartao),
            lnew_users_cartao = log1p(new_users_cartao),
            lvalor_rural = log1p(valor_rural),
            lusers_rural = log1p(users_rural),
            lqtd_rural = log1p(qtd_rural),
            lnew_users_rural = log1p(new_users_rural),
            lvalor_outros = log1p(valor_outros),
            lusers_outros = log1p(users_outros),
            lqtd_outros = log1p(qtd_outros),
            lnew_users_outros = log1p(new_users_outros))
  cat("20")
  # Add muni_cd
  cat("Converting municipality code for Credito, number of rows:", nrow(data))
  data <- data %>% mutate(id_municipio = as.integer(id_municipio))
  mun_convert2 <- mun_convert %>% select(muni_cd, id_municipio)
  data <- merge(data, mun_convert2, by="id_municipio", all.x = FALSE, all.y = FALSE)
  data <- data %>% select(-id_municipio)
  rm(mun_convert2)
  cat("Converting municipality DONE for Credito, number of rows:", nrow(data))

  # Download data
  write_dta(data, paste0(path_dta,filename[i],".dta"))
  # Variables: time_id, id_municipio, new_users, new_users_if, new_users_cg, new_operation, valor, users, qtd, valor_consignado, users_consignado, qtd_consignado, new_users_consignado, valor_emp_pessoal, users_emp_pessoal, qtd_emp_pessoal, new_users_emp_pessoal, valor_veiculo, users_veiculo, qtd_veiculo, new_users_veiculo, valor_imob, users_imob, qtd_imob, new_users_imob, valor_cartao, users_cartao, qtd_cartao, new_users_cartao, valor_rural, users_rural, qtd_rural, new_users_rural, valor_outros, users_outros, qtd_outros, new_users_outros
  #             lnew_users, lnew_users_if, lnew_users_cg, lnew_operation, lvalor, lusers, lqtd, lvalor_consignado, lusers_consignado, lqtd_consignado, lnew_users_consignado, lvalor_emp_pessoal, lusers_emp_pessoal, lqtd_emp_pessoal, lnew_users_emp_pessoal, lvalor_veiculo, lusers_veiculo, lqtd_veiculo, lnew_users_veiculo, lvalor_imob, lusers_imob, lqtd_imob, lnew_users_imob, lvalor_cartao, lusers_cartao, lqtd_cartao, lnew_users_cartao, lvalor_rural, lusers_rural, lqtd_rural, lnew_users_rural, lvalor_outros, lusers_outros, lqtd_outros, lnew_users_outros
  cat("Credito done!")
  rm(data)
}

# PJ
# PJ variables: time_id, id_municipio, new_users, new_users_if, new_users_cg, new_operation, valor, users, qtd, valor_cartao, users_cartao, qtd_cartao, new_users_cartao, valor_wc, users_wc, qtd_wc, new_users_wc, valor_invest, users_invest, qtd_invest, new_users_invest, valor_desc_ovdraft, users_desc_ovdraft, qtd_desc_ovdraft, new_users_desc_ovdraft, valor_desc_reb, users_desc_reb, qtd_desc_reb, new_users_desc_reb, valor_comex, users_comex, qtd_comex, new_users_comex, valor_outros, users_outros, qtd_outros, new_users_outros
filename <- c("Credito_Muni_PJ")
for (i in 1:length(filename)) {
  if (i == 1) {files <- c("Credito_Muni_PJ")}

  data <- read_parquet(paste0(path_data, files[1], ".parquet"), as_tibble = TRUE)
  if (length(files)>1){
    for (j in 2:length(files)) {
      temp <- read_parquet(paste0(path_data, files[j], ".parquet"), as_tibble = TRUE)
      data <- merge(data, temp, by=c("time_id","id_municipio"), all.x = TRUE, all.y = TRUE)
    }
    rm(temp)
  }

  data <- data %>%
    complete(
      time_id,
      nesting(id_municipio),
      fill = list(new_users = 0, new_users_if = 0, new_users_cg = 0, new_operation = 0, valor = 0, users = 0, qtd = 0, valor_cartao = 0, users_cartao = 0, qtd_cartao = 0, new_users_cartao = 0, valor_wc = 0, users_wc = 0, qtd_wc = 0, new_users_wc = 0, valor_invest = 0, users_invest = 0, qtd_invest = 0, new_users_invest = 0, valor_desc_ovdraft = 0, users_desc_ovdraft = 0, qtd_desc_ovdraft = 0, new_users_desc_ovdraft = 0, valor_desc_reb = 0, users_desc_reb = 0, qtd_desc_reb = 0, new_users_desc_reb = 0, valor_comex = 0, users_comex = 0, qtd_comex = 0, new_users_comex = 0, valor_outros = 0, users_outros = 0, qtd_outros = 0, new_users_outros = 0),
      explicit = TRUE)
  cat("19")
  # Add Log
  data <- data %>%
    mutate(lnew_users = log1p(new_users),
            lnew_users_if = log1p(new_users_if),
            lnew_users_cg = log1p(new_users_cg),
            lnew_operation = log1p(new_operation),
            lvalor = log1p(valor),
            lusers = log1p(users),
            lqtd = log1p(qtd),
            lvalor_cartao = log1p(valor_cartao),
            lusers_cartao = log1p(users_cartao),
            lqtd_cartao = log1p(qtd_cartao),
            lnew_users_cartao = log1p(new_users_cartao),
            lvalor_wc = log1p(valor_wc),
            lusers_wc = log1p(users_wc),
            lqtd_wc = log1p(qtd_wc),
            lnew_users_wc = log1p(new_users_wc),
            lvalor_invest = log1p(valor_invest),
            lusers_invest = log1p(users_invest),
            lqtd_invest = log1p(qtd_invest),
            lnew_users_invest = log1p(new_users_invest),
            lvalor_desc_ovdraft = log1p(valor_desc_ovdraft),
            lusers_desc_ovdraft = log1p(users_desc_ovdraft),
            lqtd_desc_ovdraft = log1p(qtd_desc_ovdraft),
            lnew_users_desc_ovdraft = log1p(new_users_desc_ovdraft),
            lvalor_desc_reb = log1p(valor_desc_reb),
            lusers_desc_reb = log1p(users_desc_reb),
            lqtd_desc_reb = log1p(qtd_desc_reb),
            lnew_users_desc_reb = log1p(new_users_desc_reb),
            lvalor_comex = log1p(valor_comex),
            lusers_comex = log1p(users_comex),
            lqtd_comex = log1p(qtd_comex),
            lnew_users_comex = log1p(new_users_comex),
            lvalor_outros = log1p(valor_outros),
            lusers_outros = log1p(users_outros),
            lqtd_outros = log1p(qtd_outros),
            lnew_users_outros = log1p(new_users_outros))
  cat("20")
  # Convert muni_cd
  cat("Converting municipality code for Credito, number of rows:", nrow(data))
  data <- data %>% mutate(id_municipio = as.integer(id_municipio))
  mun_convert2 <- mun_convert %>% select(muni_cd, id_municipio)
  data <- merge(data, mun_convert2, by="id_municipio", all.x = FALSE, all.y = FALSE)
  data <- data %>% select(-id_municipio)
  rm(mun_convert2)
  cat("Converting municipality DONE for Credito, number of rows:", nrow(data))

  # Download data
  write_dta(data, paste0(path_dta,filename[i],".dta"))
  # Variables: time_id, id_municipio, new_users, new_users_if, new_users_cg, new_operation, valor, users, qtd, valor_cartao, users_cartao, qtd_cartao, new_users_cartao, valor_wc, users_wc, qtd_wc, new_users_wc, valor_invest, users_invest, qtd_invest, new_users_invest, valor_desc_ovdraft, users_desc_ovdraft, qtd_desc_ovdraft, new_users_desc_ovdraft, valor_desc_reb, users_desc_reb, qtd_desc_reb, new_users_desc_reb, valor_comex, users_comex, qtd_comex, new_users_comex, valor_outros, users_outros, qtd_outros, new_users_outros
  #             lnew_users, lnew_users_if, lnew_users_cg, lnew_operation, lvalor, lusers, lqtd, lvalor_cartao, lusers_cartao, lqtd_cartao, lnew_users_cartao, lvalor_wc, lusers_wc, lqtd_wc, lnew_users_wc, lvalor_invest, lusers_invest, lqtd_invest, lnew_users_invest, lvalor_desc_ovdraft, lusers_desc_ovdraft, lqtd_desc_ovdraft, lnew_users_desc_ovdraft, lvalor_desc_reb, lusers_desc_reb, lqtd_desc_reb, lnew_users_desc_reb, lvalor_comex, lusers_comex, lqtd_comex, lnew_users_comex, lvalor_outros, lusers_outros, lqtd_outros, lnew_users_outros

  cat("Credito done!")
  rm(data)
}
}


# ------------------------------------------------------------------------------
# Pix_ind_sample
# ------------------------------------------------------------------------------
# Before Variables: week, id, id_municipio_receita, tipo, value_send, trans_send, value_rec, trans_rec, value_self, trans_self

  # Pix_ind_sample.dta
  # Variables: week, id, muni_cd, tipo, value_send, trans_send, value_rec, trans_rec, value_self, trans_self
  #                               lvalue_send, ltrans_send, lvalue_rec, ltrans_rec, lvalue_self, ltrans_self
if(run_Pix_ind_sample == 1){
filename <- c("Pix_ind_sample")
for (i in 1:length(filename)) {
  files <- c("Pix_ind_sample_PF", "Pix_ind_sample_PJ")
  data <- read_parquet(paste0(path_data, files[1], ".parquet"), as_tibble = TRUE)
  if (length(files)>1){
    for (j in 2:length(files)) {
      temp <- read_parquet(paste0(path_data, files[j], ".parquet"), as_tibble = TRUE)
      data <- rbindlist(list(data, temp), fill=TRUE)
    }
    rm(temp)
  }

  data <- data %>%
    complete(
      week,
      nesting(id, id_municipio_receita, tipo),
      fill = list(value_send = 0, trans_send = 0, value_rec = 0, trans_rec = 0, value_self = 0, trans_self = 0),
      explicit = TRUE)
  cat("19")
  # Add Log
  data <- data %>%
    mutate(lvalue_send = log1p(value_send),
           ltrans_send = log1p(trans_send),
            lvalue_rec = log1p(value_rec),
            ltrans_rec = log1p(trans_rec),
            lvalue_self = log1p(value_self),
            ltrans_self = log1p(trans_self))
  cat("20")
  # Add muni_cd
  cat("Converting municipality code for Pix_ind_sample, number of rows:", nrow(data))
  data <- data %>% mutate(id_municipio_receita = as.integer(id_municipio_receita))
  mun_convert2 <- mun_convert %>% select(muni_cd, id_municipio_receita)
  data <- merge(data, mun_convert2, by="id_municipio_receita", all.x = FALSE, all.y = FALSE)
  data <- data %>% select(-id_municipio_receita)
  rm(mun_convert2)
  cat("Converting municipality DONE for Pix_ind_sample, number of rows:", nrow(data))
  
  # Download data
  write_dta(data, paste0(path_dta,filename[i],".dta"))
  # Variables: week, id, muni_cd, tipo, value_send, trans_send, value_rec, trans_rec, value_self, trans_self
  #             lvalue_send, ltrans_send, lvalue_rec, ltrans_rec, lvalue_self, ltrans_self
  cat("Pix_ind_sample done!")
  rm(data)
}
}

# ------------------------------------------------------------------------------
# Boleto_ind_sample
# ------------------------------------------------------------------------------
# Before Variables: week, id, id_municipio_receita, tipo, value_send, trans_send, value_rec, trans_rec, value_self, trans_self

  # Boleto_ind_sample.dta
  # Variables: week, id, muni_cd, tipo, value_send, trans_send, value_rec, trans_rec, value_self, trans_self
  #                               lvalue_send, ltrans_send, lvalue_rec, ltrans_rec, lvalue_self, ltrans_self
if(run_Boleto_ind_sample == 1){
filename <- c("Boleto_ind_sample")
for (i in 1:length(filename)) {
  files <- c("Boleto_ind_sample_PF", "Boleto_ind_sample_PJ")
  data <- read_parquet(paste0(path_data, files[1], ".parquet"), as_tibble = TRUE)
  if (length(files)>1){
    for (j in 2:length(files)) {
      temp <- read_parquet(paste0(path_data, files[j], ".parquet"), as_tibble = TRUE)
      data <- rbindlist(list(data, temp), fill=TRUE)
    }
    rm(temp)
  }

  data <- data %>%
    complete(
      week,
      nesting(id, id_municipio_receita, tipo),
      fill = list(value_send = 0, trans_send = 0, value_rec = 0, trans_rec = 0, value_self = 0, trans_self = 0),
      explicit = TRUE)
  cat("19")
  # Add Log
  data <- data %>%
    mutate(lvalue_send = log1p(value_send),
           ltrans_send = log1p(trans_send),
            lvalue_rec = log1p(value_rec),
            ltrans_rec = log1p(trans_rec),
            lvalue_self = log1p(value_self),
            ltrans_self = log1p(trans_self))
  cat("20")
  # Add muni_cd
  cat("Converting municipality code for Boleto_ind_sample, number of rows:", nrow(data))
  data <- data %>% mutate(id_municipio_receita = as.integer(id_municipio_receita))
  mun_convert2 <- mun_convert %>% select(muni_cd, id_municipio_receita)
  data <- merge(data, mun_convert2, by="id_municipio_receita", all.x = FALSE, all.y = FALSE)
  data <- data %>% select(-id_municipio_receita)
  rm(mun_convert2)
  cat("Converting municipality DONE for Boleto_ind_sample, number of rows:", nrow(data))
  
  # Download data
  write_dta(data, paste0(path_dta,filename[i],".dta"))
  # Variables: week, id, muni_cd, tipo, value_send, trans_send, value_rec, trans_rec, value_self, trans_self
  #             lvalue_send, ltrans_send, lvalue_rec, ltrans_rec, lvalue_self, ltrans_self
  cat("Boleto_ind_sample done!")
  rm(data)
}
}


# ------------------------------------------------------------------------------
# Card_ind_sample
# ------------------------------------------------------------------------------
# Before Variables: week, id, id_municipio_receita, tipo, value_credit, trans_credit, value_debit, trans_debit

  # Card_ind_sample.dta 
  # Variables: week, id, muni_cd, tipo, valor, trans, value_credit, trans_credit, value_debit, trans_debit
  #             lvalor, ltrans, lvalue_credit, ltrans_credit, lvalue_debit, ltrans_debit

if(run_Card_ind_sample == 1){
filename <- c("Card_ind_sample")
for (i in 1:length(filename)) {
  files <- c("Card_ind_sample_PF", "Card_ind_sample_PJ")
  data <- read_parquet(paste0(path_data, files[1], ".parquet"), as_tibble = TRUE)
  if (length(files)>1){
    for (j in 2:length(files)) {
      temp <- read_parquet(paste0(path_data, files[j], ".parquet"), as_tibble = TRUE)
      data <- rbindlist(list(data, temp), fill=TRUE)
    }
    rm(temp)
  }

  data <- data %>%
    complete(
      week,
      nesting(id, id_municipio_receita, tipo),
      fill = list(value_credit = 0, trans_credit = 0, value_debit = 0, trans_debit = 0),
      explicit = TRUE)
  cat("19")
  # Add credit and debit
  data <- data %>%
    mutate(valor = value_credit + value_debit,
          trans = trans_credit + trans_debit)
  # Add Log
  data <- data %>%
    mutate(lvalue_credit = log1p(value_credit),
           ltrans_credit = log1p(trans_credit),
            lvalue_debit = log1p(value_debit),
            ltrans_debit = log1p(trans_debit),
            lvalor = log1p(valor),
            ltrans = log1p(trans))
  cat("20")
  # Add muni_cd
  cat("Converting municipality code for Card_ind_sample, number of rows:", nrow(data))
  data <- data %>% mutate(id_municipio_receita = as.integer(id_municipio_receita))
  mun_convert2 <- mun_convert %>% select(muni_cd, id_municipio_receita)
  data <- merge(data, mun_convert2, by="id_municipio_receita", all.x = FALSE, all.y = FALSE)
  data <- data %>% select(-id_municipio_receita)
  rm(mun_convert2)
  cat("Converting municipality DONE for Card_ind_sample, number of rows:", nrow(data))
  
  # Download data
  write_dta(data, paste0(path_dta,filename[i],".dta"))
  # Variables: week, id, muni_cd, tipo, valor, trans, value_credit, trans_credit, value_debit, trans_debit
  #             lvalor, ltrans, lvalue_credit, ltrans_credit, lvalue_debit, ltrans_debit
  cat("Card_ind_sample done!")
  rm(data)
}
}


# ------------------------------------------------------------------------------
# CCS_ind_sample
# ------------------------------------------------------------------------------
# Before Variables: id, id_municipio_receita, tipo, bank, dia_inicio, dia_fim

  # CCS_ind_sample.dta
  # Variables: id, muni_cd, tipo, bank, tipo_inst, bank_type, dia_inicio, dia_fim

  # Create stock per week, per bank_type as well. Beware of people having multiple accounts in the same bank. --------------------------
if(run_CCS_ind_sample == 1){
filename <- c("CCS_ind_sample")
for (i in 1:length(filename)) {
  files <- c("CCS_ind_sample_PF", "CCS_ind_sample_PJ")
  data <- read_parquet(paste0(path_data, files[1], ".parquet"), as_tibble = TRUE)
  if (length(files)>1){
    for (j in 2:length(files)) {
      temp <- read_parquet(paste0(path_data, files[j], ".parquet"), as_tibble = TRUE)
      data <- rbindlist(list(data, temp), fill=TRUE)
    }
    rm(temp)
  }

  # Add muni_cd
  cat("Converting municipality code for CCS_ind_sample, number of rows:", nrow(data))
  data <- data %>% mutate(id_municipio_receita = as.integer(id_municipio_receita))
  mun_convert2 <- mun_convert %>% select(muni_cd, id_municipio_receita)
  data <- merge(data, mun_convert2, by="id_municipio_receita", all.x = FALSE, all.y = FALSE)
  data <- data %>% select(-id_municipio_receita)
  rm(mun_convert2)
  cat("Converting municipality DONE for CCS_ind_sample, number of rows:", nrow(data))
  
  #Add bank type
  data <- merge(data, Cadastro_IF, by="bank", all = FALSE)

  # Download data
  write_dta(data, paste0(path_dta,filename[i],".dta"))
  # Variables: id, muni_cd, tipo, bank, tipo_inst, bank_type, dia_inicio, dia_fim
  cat("Card_ind_sample done!")
  rm(data)
}
}


# ------------------------------------------------------------------------------
# Credito_ind_sample
# ------------------------------------------------------------------------------
# Before Variables: time_id, id, id_municipio_receita, tipo, bank, operation_id, mod_Desig, valor, new_client, new_operation, new_bank, new_cg

# Need to think better at what I want to accomplish here. 

if(run_Credito_ind_sample == 1){
filename <- c("Credito_ind_sample")
for (i in 1:length(filename)) {
  files <- c("Credito_ind_sample_PF", "Credito_ind_sample_PJ")
  data <- read_parquet(paste0(path_data, files[1], ".parquet"), as_tibble = TRUE)
  if (length(files)>1){
    for (j in 2:length(files)) {
      temp <- read_parquet(paste0(path_data, files[j], ".parquet"), as_tibble = TRUE)
      data <- rbindlist(list(data, temp), fill=TRUE)
    }
    rm(temp)
  }

  data <- data %>%
    complete(
      time_id,
      nesting(id, id_municipio_receita, tipo, bank, operation_id, mod_Desig),
      fill = list(),
      explicit = TRUE)
  cat("19")
  # Add Log
  data <- data %>%
    mutate()
  cat("20")
  # Add muni_cd
  cat("Converting municipality code for Credito_ind_sample, number of rows:", nrow(data))
  data <- data %>% mutate(id_municipio_receita = as.integer(id_municipio_receita))
  mun_convert2 <- mun_convert %>% select(muni_cd, id_municipio_receita)
  data <- merge(data, mun_convert2, by="id_municipio_receita", all.x = FALSE, all.y = FALSE)
  data <- data %>% select(-id_municipio_receita)
  rm(mun_convert2)
  cat("Converting municipality DONE for Credito_ind_sample, number of rows:", nrow(data))
  
  # Download data
  write_dta(data, paste0(path_dta,filename[i],".dta"))
  # Variables: week, id, muni_cd, tipo, value_credit, trans_credit, value_debit, trans_debit
  #             lvalue_credit, ltrans_credit, lvalue_debit, ltrans_debit
  cat("Credito_ind_sample done!")
  rm(data)
}
}



# ------------------------------------------------------------------------------
# TED
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# TED = TED_STR + TED_SITRAF
# ------------------------------------------------------------------------------
# Before Variables: flow_code, week, id_municipio_receita, sender_type, receiver_type, senders, receivers, valor, trans
# -> flow code: 99 = self, 0 = intra, 1 = inflow, -1 = outflow

# TED.dta
  # Variables: flow_code, week, muni_cd, sender_type, receiver_type, senders, receivers, valor, trans,
  #             lsenders, lreceivers, lvalor, ltrans

# TED_aggreg.dta
  # Variables: week, muni_cd, sender_type, receiver_type, 
  #             senders_rec, receivers_rec, valor_rec, trans_rec 
  #             senders_sent, receivers_sent, valor_sent, trans_sent
  # Plus l variations. 

  #TED_aggreg_rec.dta
  # Variables: week, muni_cd, tipo, 
  #             trans, valor
  #             ltrans, lvalor
  
  #TED_aggreg_send.dta
  # Variables: week, muni_cd, tipo, 
  #             trans, valor
  #             ltrans, lvalor

if(run_TED == 1){
filename <- c("TED")
for (i in 1:length(filename)) {
  files <- c("TED_STR_B2B", "TED_STR_B2P", "TED_STR_P2B", "TED_STR_P2P", "TED_SITRAF_B2B", "TED_SITRAF_B2P", "TED_SITRAF_P2B", "TED_SITRAF_P2P")
  data <- read_parquet(paste0(path_data, files[1], ".parquet"), as_tibble = TRUE)
  if (length(files)>1){
    for (j in 2:length(files)) {
      temp <- read_parquet(paste0(path_data, files[j], ".parquet"), as_tibble = TRUE)
      data <- rbindlist(list(data, temp), fill=TRUE)   
    }
    rm(temp)
  }

  data <- data %>%
    group_by(flow_code, week, id_municipio_receita, sender_type, receiver_type) %>%
    summarise(receivers = sum(receivers, na.rm = TRUE),
              senders = sum(senders, na.rm = TRUE),
              valor = sum(valor, na.rm = TRUE),
              trans = sum(trans, na.rm = TRUE)) %>%
    ungroup()

  data <- data %>%
    complete(
      week,
      nesting(id_municipio_receita, sender_type, receiver_type, flow_code),
      fill = list(receivers = 0, senders = 0, valor = 0, trans = 0),
      explicit = TRUE)
  cat("19")
  # Add Log
  data <- data %>%
    mutate(lreceivers = log1p(receivers),
            lsenders = log1p(senders),
            lvalor = log1p(valor),
            ltrans = log1p(trans))
  cat("20")

  # Add muni_cd
  cat("Converting municipality code for TED, number of rows:", nrow(data))
  data <- data %>% mutate(id_municipio_receita = as.integer(id_municipio_receita))
  mun_convert2 <- mun_convert %>% select(muni_cd, id_municipio_receita)
  data <- merge(data, mun_convert2, by="id_municipio_receita", all.x = FALSE, all.y = FALSE)
  data <- data %>% select(-id_municipio_receita)
  rm(mun_convert2)
  cat("Converting municipality DONE for TED, number of rows:", nrow(data))
  
  # Download data
  write_dta(data, paste0(path_dta,filename[i],".dta"))
  # Variables: flow_code, week, muni_cd, sender_type, receiver_type, senders, receivers, valor, trans,
  #             lsenders, lreceivers, lvalor, ltrans
  cat("TED done!")
  
  ### Create aggregated Muni flow
  # Sum intra, inflow to get received
  data_temp1 <- data %>%
    filter(flow_code == 0 | flow_code == 1) %>%
    group_by(week, muni_cd, sender_type, receiver_type) %>%
    summarise(senders_rec = sum(senders, na.rm = TRUE),
              receivers_rec = sum(receivers, na.rm = TRUE), 
              valor_rec = sum(valor, na.rm = TRUE), 
              trans_rec = sum(trans, na.rm = TRUE)) %>%
    ungroup()
  # Sum intra, outflow to get sent
  data_temp2 <- data %>%
    filter(flow_code == 0 | flow_code == -1) %>%
    group_by(week, muni_cd, sender_type, receiver_type) %>%
    summarise(senders_sent = sum(senders, na.rm = TRUE),
              receivers_sent = sum(receivers, na.rm = TRUE), 
              valor_sent = sum(valor, na.rm = TRUE), 
              trans_sent = sum(trans, na.rm = TRUE)) %>%
    ungroup()
  
  #data_aggreg <- merge(data_temp1,data_temp2, all.x = TRUE, all.y = TRUE) 
  data_aggreg <- merge(data_temp1,data_temp2, by=c("week","muni_cd","sender_type","receiver_type") , all.x = TRUE, all.y = TRUE) 
  
  # Add Log
  data_aggreg <- data_aggreg %>%
    mutate(
      lsenders_rec = log1p(senders_rec), lreceivers_rec = log1p(receivers_rec), lvalor_rec = log1p(valor_rec), ltrans_rec = log1p(trans_rec),
      lsenders_sent = log1p(senders_sent), lreceivers_sent = log1p(receivers_sent), lvalor_sent = log1p(valor_sent), ltrans_sent = log1p(trans_sent)
    )
  
  write_dta(data_aggreg, paste0(path_dta,"TED_aggreg",".dta"))
  # Variables: week, muni_cd, sender_type, receiver_type, 
  #             senders_rec, receivers_rec, valor_rec, trans_rec 
  #             senders_sent, receivers_sent, valor_sent, trans_sent
  # Plus l variations. 
  rm(data_temp1,data_temp2)
  
  #############
  dat_rec <- data_aggreg %>%
    group_by(week, muni_cd, receiver_type) %>%
    summarise(trans = sum(trans_rec, na.rm = TRUE),
              valor = sum(valor_rec, na.rm = TRUE)) %>%
    mutate(ltrans = log1p(trans),
           lvalor = log1p(valor)) %>%
    rename(tipo = receiver_type)
  write_dta(dat_rec, paste0(path_dta,"TED_aggreg_rec",".dta"))
  # Variables: week, muni_cd, tipo, 
  #             trans, valor
  #             ltrans, lvalor
  rm(dat_rec)
  dat_sent <- data_aggreg %>%
    group_by(week, muni_cd, sender_type) %>%
    summarise(trans = sum(trans_sent, na.rm = TRUE),
              valor = sum(valor_sent, na.rm = TRUE)) %>%
    mutate(ltrans = log1p(trans),
           lvalor = log1p(valor)) %>%
    rename(tipo = sender_type)
  write_dta(dat_sent, paste0(path_dta,"TED_aggreg_send",".dta"))
  # Variables: week, muni_cd, tipo, 
  #             trans, valor
  #             ltrans, lvalor
  rm(dat_sent)
  cat("TED_aggreg done!")
  ################

}
}

# ------------------------------------------------------------------------------
# TED_adoption = TED_adoption_STR + TED_adoption_SITRAF
# ------------------------------------------------------------------------------
# Before Variables: time_id, id_municipio_receita, tipo, adopters

  # TED_adoption.dta
  # Variables: week, muni_cd, tipo, adopters
  #             ladopters

if(run_TED_adoption == 1){
filename <- c("TED_adoption")
for (i in 1:length(filename)) {
  files <- c("TED_adoption_STR", "TED_adoption_SITRAF")
  data <- read_parquet(paste0(path_data, files[1], ".parquet"), as_tibble = TRUE)
  
  if (length(files)>1){
    for (j in 2:length(files)) {
      temp <- read_parquet(paste0(path_data, files[j], ".parquet"), as_tibble = TRUE)
      data <- rbindlist(list(data, temp), fill=TRUE)
    }
    rm(temp)
  }

  data <- data %>%
    group_by(time_id, id_municipio_receita, tipo) %>%
    summarise(adopters = sum(adopters, na.rm = TRUE)) %>%
    ungroup()

  #Complete
  data <- data %>%
    complete(
      time_id,
      nesting(id_municipio_receita, tipo),
      fill = list(adopters = 0),
      explicit = TRUE)

  # Add Log
  data <- data %>%
    mutate(ladopters = log1p(adopters))

  # Add muni_cd
  cat("Converting municipality code for TED_adoption, number of rows:", nrow(data))
  data <- data %>% mutate(id_municipio_receita = as.integer(id_municipio_receita))
  mun_convert2 <- mun_convert %>% select(muni_cd, id_municipio_receita)
  data <- merge(data, mun_convert2, by="id_municipio_receita", all.x = FALSE, all.y = FALSE)
  data <- data %>% select(-id_municipio_receita)
  rm(mun_convert2)
  cat("Converting municipality DONE for TED_adoption, number of rows:", nrow(data))

  # Download data
  write_dta(data, paste0(path_dta,filename[i],".dta"))
  # Variables: week, muni_cd, tipo, adopters
  #             ladopters
  cat("TED_adoption done!")
  ### SUMMARY STATS
}
}

# ------------------------------------------------------------------------------
# TED_ind_sample = TED_ind_sample_STR + TED_ind_sample_SITRAF
# ------------------------------------------------------------------------------
# Before Variables: week, id, id_municipio_receita, tipo, value_send, trans_send, value_rec, trans_rec, value_self, trans_self

  # TED_ind_sample.dta
  # Variables: week, id, muni_cd, tipo, value_send, trans_send, value_rec, trans_rec, value_self, trans_self
  #             lvalue_send, ltrans_send, lvalue_rec, ltrans_rec, lvalue_self, ltrans_self
if(run_TED_ind_sample == 1){
filename <- c("TED_ind_sample")
for (i in 1:length(filename)) {
  files <- c("TED_ind_sample_STR_PF", "TED_ind_sample_STR_PJ")
  data <- read_parquet(paste0(path_data, files[1], ".parquet"), as_tibble = TRUE)
  if (length(files)>1){
    for (j in 2:length(files)) {
      temp <- read_parquet(paste0(path_data, files[j], ".parquet"), as_tibble = TRUE)
      data <- rbindlist(list(data, temp), fill=TRUE)
    }
    rm(temp)
  }

  files <- c("TED_ind_sample_SITRAF_PF", "TED_ind_sample_SITRAF_PJ")
  data2 <- read_parquet(paste0(path_data, files[1], ".parquet"), as_tibble = TRUE)
  if (length(files)>1){
    for (j in 2:length(files)) {
      temp <- read_parquet(paste0(path_data, files[j], ".parquet"), as_tibble = TRUE)
      data2 <- rbindlist(list(data2, temp), fill=TRUE)
    }
    rm(temp)
  }

  data <- rbindlist(list(data, data2), fill=TRUE)
  rm(data2)
  data <- data %>%
    group_by(week, id, id_municipio_receita, tipo) %>%
    summarise(value_send = sum(value_send, na.rm = TRUE),
              trans_send = sum(trans_send, na.rm = TRUE),
              value_rec = sum(value_rec, na.rm = TRUE),
              trans_rec = sum(trans_rec, na.rm = TRUE),
              value_self = sum(value_self, na.rm = TRUE),
              trans_self = sum(trans_self, na.rm = TRUE)) %>%
    ungroup()

  data <- data %>%
    complete(
      week,
      nesting(id, id_municipio_receita, tipo),
      fill = list(value_send = 0, trans_send = 0, value_rec = 0, trans_rec = 0, value_self = 0, trans_self = 0),
      explicit = TRUE)
  cat("19")
  # Add Log
  data <- data %>%
    mutate(lvalue_send = log1p(value_send),
           ltrans_send = log1p(trans_send),
            lvalue_rec = log1p(value_rec),
            ltrans_rec = log1p(trans_rec),
            lvalue_self = log1p(value_self),
            ltrans_self = log1p(trans_self))
  cat("20")
  # Add muni_cd
  cat("Converting municipality code for TED_ind_sample, number of rows:", nrow(data))
  data <- data %>% mutate(id_municipio_receita = as.integer(id_municipio_receita))
  mun_convert2 <- mun_convert %>% select(muni_cd, id_municipio_receita)
  data <- merge(data, mun_convert2, by="id_municipio_receita", all.x = FALSE, all.y = FALSE)
  data <- data %>% select(-id_municipio_receita)
  rm(mun_convert2)
  cat("Converting municipality DONE for TED_ind_sample, number of rows:", nrow(data))
  
  # Download data
  write_dta(data, paste0(path_dta,filename[i],".dta"))
  # Variables: week, id, muni_cd, tipo, value_send, trans_send, value_rec, trans_rec, value_self, trans_self
  #             lvalue_send, ltrans_send, lvalue_rec, ltrans_rec, lvalue_self, ltrans_self
  cat("TED_ind_sample done!")
  rm(data)
}
}

