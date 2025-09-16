# Flood Event Studies
#https://lost-stats.github.io/Model_Estimation/Research_Design/event_study.html


#flood_event_studies_estimates.R
################################################################################
options(download.file.method = "wininet")
rm(list = ls()) ## Clear workspace

# install.packages("coefplot")
# install.packages("magrittr")
library(readr)
library(stringr)
library(dplyr)
library(tidyr)
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
library(coefplot)
library(magrittr)
library(odbc)
library(RODBC)

setwd("//sbcdf176/Pix_Matheus$")
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

# Constants
xl <- -26
xu <- 52
xl_balanced <- -26
xu_balanced <- 52
xl_balanced_covid <- -13
xu_balanced_covid <- 13

xl_month <- -6
xu_month <- 12
xl_balanced_month <- -6
xu_balanced_month <- 12
xl_balanced_covid_month <- -3
xu_balanced_covid_month <- 3
################################################################################
#-------------------------------------------------------------------------------
# Load auxiliary data
#-------------------------------------------------------------------------------
source(paste0(R_path, "/auxiliary_data.r"))

#Idea: just load the data, no work on it. save it beforehand. 

Cadastro_IF <- read_parquet(paste0(path_data, "Cadastro_IF", ".parquet"))
Cadastro_IF <- Cadastro_IF %>%
  select(bank, tipo_inst, bank_type, macroseg_IF, cong_id) 
Cadastro_IF <- data.table(Cadastro_IF)
Cadastro_IF <- Cadastro_IF %>% rename(macroseg_if_txt = macroseg_IF)

#-------------------------------------------------------------------------------
# Functions
#-------------------------------------------------------------------------------
source(paste0(R_path,"/functions/stata_week_number.R"))
source(paste0(R_path,"/functions/gen_week_list.R"))
source(paste0(R_path,"/functions/time_id_to_month.R"))
source(paste0(R_path,"/functions/time_id_to_year.R"))
source(paste0(R_path,"/functions/week_to_month.R"))
source(paste0(R_path,"/functions/week_to_year.R"))
source(paste0(R_path,"/functions/prepare_data.R"))
source(paste0(R_path,"/functions/twfe.R"))
source(paste0(R_path,"/functions/ylim_function.R"))
source(paste0(R_path,"/functions/print_twfe.R"))
source(paste0(R_path,"/functions/twfe2.R"))
source(paste0(R_path,"/functions/ylim_function2.R"))
source(paste0(R_path,"/functions/print_twfe2.R"))
source(paste0(R_path,"/functions/twfe_ind.R"))
source(paste0(R_path,"/functions/print_twfe_week.R"))
source(paste0(R_path,"/functions/print_twfe_month.R"))
source(paste0(R_path,"/functions/day_to_stata_month.R"))
source(paste0(R_path,"/functions/prepare_data_syn_month.R"))

# Test
#source(paste0(R_path,"/Test_flood_event_studies.R"))
#dat_a <- dat_function("Base_week_muni_fake.dta",flood_week_after,mun_fe,mun_control,xl, xu)
#dat_b <- dat_function("Base_week_muni_fake.dta",flood_week_before,mun_fe,mun_control,xl, xu)
#twfe("fake_TED_","log_valor_TED_intra","constant","constant","constant",list(dat_b,dat_a),c("before","after"))
#print_twfe("fake_TED_", "log_valor_TED_intra", "Log Value TED", list(dat_b,dat_a), c("before","after"), c("Before Pix","After Pix"), xl, xu)
#print_twfe2("fake_TED_", "log_valor_TED_intra", "Log Value TED", list(dat_b,dat_a), c("before","after"), c("Before Pix","After Pix"), xl, xu)

# Query -------------------------------------------------------------------

run_Pix_Muni_flow_syn <- 0 # worked

run_CCS_Muni_stock <- 0 # Worked ---- new stuff -----------------------------------------------------------
run_CCS_Muni_IF <- 0 # Worked ---- new stuff -----------------------------------------------------------
run_CCS_HHI <- 0 # Worked
run_CCS_first_account <- 0  # Worked
run_CCS_first_account_month <- 0 # Worked
run_Pix_Muni_Bank <- 1 # Worked ---- new stuff -----------------------------------------------------------
run_Pix_Muni_flow <- 0 # Worked
run_Pix_Muni_user <- 0 # Worked
run_Card_rec <- 0 # Worked
run_Boletos <- 0 # Worked
run_Estban <- 0 # Worked 
run_Base_old <- 0 # Worked
run_Credito_old <- 0 # Worked
run_Credito <- 0 # worked

run_TED_SITRAF <- 0
run_TED_STR <- 0 # check
run_TED <- 0 # check

run_Pix_individual <- 0
run_adoption_pix <- 0 # We created new file. pix_adoption_sample. 

#run_Pix_adoption <- 0 # Code NOT ready. ----> Need to download.
#run_Card_adoption <- 0 # Code NOT ready. ----> Need to download.
#run_Boleto_adoption <- 0 # Code NOT ready. ----> Need to download.
#run_TED_adoption <- 0 # Code NOT ready. ----> Need to download.

run_Pix_ind_sample <- 0
run_Boleto_ind_sample <- 0
run_Card_ind_sample <- 0
run_CCS_ind_sample <- 0  #---- new stuff -----------------------------------------------------------
run_Credito_ind_sample <- 0 # ---- new stuff -----------------------------------------------------------
run_TED_SITRAF_ind_sample <- 0 # check.
run_TED_STR_ind_sample <- 0 #----------------------------------------------------------------
run_TED_ind_sample <- 0 #--------------------------------------------------------------------


run_Pix_Muni_user_month <- 0 # worked
run_TED_boleto <- 0
run_TED_boleto_card <- 0

# write a report of errors.
# I got some new stuff on processa sqls
# need to run ccs if and pix muni bank
#also, lets work with pix flow B2P and p2b


# ------------------------------------------------------------------------------
# Pix_Muni_user_month - worked
# ------------------------------------------------------------------------------
# Before Variables: time_id, muni_cd, tipo, senders, receivers, users

# Pix_Muni_user_month.dta
# after Variables: time_id, muni_cd, tipo, senders, receivers, users
#                  lusers, lsenders, lreceivers
variables <- c("lusers", "lsenders", "lreceivers")
variables_labels <- c("Log Active Users", "Log Active Senders", "Log Active Receivers")
if(run_Pix_Muni_user_month == 1){
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood_a <- flood_month_after
        xll <- xl_month
        xuu <- xu_month
        ending <- "_"
        legend_name <- c("Pix")
      }
      if(i==2){
        flood_a <- flood_month_after_balanced
        xll <- xl_balanced_month
        xuu <- xu_balanced_month
        ending <- "_balanced_"
        legend_name <- c("Pix")
      }
      if(i==3){
        flood_a <- flood_month_after_balanced_covid
        xll <- xl_balanced_covid_month
        xuu <- xu_balanced_covid_month
        ending <- "_balanced_covid_"
        legend_name <- c("Pix")
      }
      dat_a <- prepare_data("Pix_Muni_user_month.dta",flood_a,mun_fe,mun_control)
      beginning_PF <- paste0("Pix_Muni_user_month_PF",ending)
      beginning_PJ <- paste0("Pix_Muni_user_month_PJ",ending)
      dat_a_PF <- dat_a %>% filter(tipo==1)
      dat_a_PJ <- dat_a %>% filter(tipo==2)
      
      
      for(z in 1:length(variables)){
        twfe2(beginning_PF,variables[[z]],"constant","constant","flood_risk5", list(dat_a_PF), c("Pix"))
        print_twfe_month(beginning_PF, variables[[z]], variables_labels[[z]], c("Pix"), legend_name, xll, xuu)
        
        twfe2(beginning_PJ,variables[[z]],"constant","constant","flood_risk5", list(dat_a_PJ), c("Pix"))
        print_twfe_month(beginning_PJ, variables[[z]], variables_labels[[z]], c("Pix"), legend_name, xll, xuu)
      }
      rm(dat_a, dat_a_PF, dat_a_PJ)
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in Pix_Muni_user_month:", e))
  })
}


# ------------------------------------------------------------------------------
# Credito - worked
# ------------------------------------------------------------------------------

# Credito_Muni_PF.dta
# Variables: time_id, muni_cd, new_users, new_users_if, new_users_cg, valor, valor_ativo, users, qtd, banks, valor_w, users_w, qtd_w, valor_cartao, users_cartao, qtd_cartao
#                                 Plus l variations

variables <- c("lnew_users", "lnew_users_if", "lnew_users_cg", "lvalor","lvalor_ativo","lusers","lqtd","lbanks","lvalor_w","lusers_w","lqtd_w","lvalor_cartao","lusers_cartao","lqtd_cartao")
variables_labels <- c("Log New User", "Log New Bank", "Log New Conglomerate", "Log Volume Loans","Log Active Loan Value","Log Credit Access","Log Quantity of Loans","Log Banks","Log Value","Log Users","Log Quantity","Log Value Card","Log Users Card","Log Quantity Card")
if(run_Credito == 1){
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood_a <- flood_month_after
        flood_b <- flood_month_before2019
        xll <- xl_month
        xuu <- xu_month
        ending <- "_"
        legend_name <- c("2019.01 - 2020.10", "2020.11 - 2022.12")
      }
      if(i==2){
        flood_a <- flood_month_after_balanced
        flood_b <- flood_month_before_balanced2019
        xll <- xl_balanced_month
        xuu <- xu_balanced_month
        ending <- "_balanced_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==3){
        flood_a <- flood_month_after_balanced_covid
        flood_b <- flood_month_before_balanced_covid
        xll <- xl_balanced_covid_month
        xuu <- xu_balanced_covid_month
        ending <- "_balanced_covid_"
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
      }
      dat_a <- prepare_data("Credito_Muni_PF.dta",flood_a,mun_fe,mun_control)
      dat_b <- prepare_data("Credito_Muni_PF.dta",flood_b,mun_fe,mun_control)
      beginning <- paste0("Credito_Muni_PF",ending)
      
      for(z in 1:length(variables)){
        twfe2(beginning,variables[[z]],"constant","constant","flood_risk5", list(dat_b,dat_a), c("before","after"))
        print_twfe_month(beginning, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
      }
      rm(dat_a,dat_b)
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in Credito:", e))
  })
}
# Credito_Muni_PJ.dta
# Variables: time_id, muni_cd, new_users, new_users_if, new_users_cg, valor, valor_ativo, users, qtd, banks,
#                                 Plus l variations
variables <- c("lnew_users", "lnew_users_if", "lnew_users_cg", "lvalor","lvalor_ativo","lusers","lqtd","lbanks")
variables_labels <- c("Log New User", "Log New Bank", "Log New Conglomerate","Log Volume Loans","Log Active Loan Value","Log Credit Access","Log Quantity of Loans","Log Banks")
if(run_Credito == 1){
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood_a <- flood_month_after
        flood_b <- flood_month_before2019
        xll <- xl_month
        xuu <- xu_month
        ending <- "_"
        legend_name <- c("2019.01 - 2020.10", "2020.11 - 2022.12")
      }
      if(i==2){
        flood_a <- flood_month_after_balanced
        flood_b <- flood_month_before_balanced2019
        xll <- xl_balanced_month
        xuu <- xu_balanced_month
        ending <- "_balanced_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==3){
        flood_a <- flood_month_after_balanced_covid
        flood_b <- flood_month_before_balanced_covid
        xll <- xl_balanced_covid_month
        xuu <- xu_balanced_covid_month
        ending <- "_balanced_covid_"
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
      }
      dat_a <- prepare_data("Credito_Muni_PJ.dta",flood_a,mun_fe,mun_control)
      dat_b <- prepare_data("Credito_Muni_PJ.dta",flood_b,mun_fe,mun_control)
      beginning <- paste0("Credito_Muni_PJ",ending)
      
      for(z in 1:length(variables)){
        twfe2(beginning,variables[[z]],"constant","constant","flood_risk5", list(dat_b,dat_a), c("before","after"))
        print_twfe_month(beginning, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
      }
      rm(dat_a,dat_b)
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in Credito2:", e))
  })
}

# ------------------------------------------------------------------------------
# TED - worked, need to create ted users
# ------------------------------------------------------------------------------

# TED_SITRAF.dta
# Variables: flow_code, week, muni_cd, sender_type, receiver_type, senders, receivers, valor, trans,
#             lsenders, lreceivers, lvalor, ltrans

# TED_SITRAF_aggreg.dta
# Variables: week, muni_cd, sender_type, receiver_type, 
#             senders_rec, receivers_rec, valor_rec, trans_rec 
#             senders_sent, receivers_sent, valor_sent, trans_sent
# Plus l variations. 

# TED_SITRAF_aggreg_rec.dta
# Variables: week, muni_cd, tipo, 
#             trans, valor,
#             ltrans, lvalor

# TED_SITRAF_aggreg_send.dta
# Variables: week, muni_cd, tipo, 
#             trans, valor,
#             ltrans, lvalor


# Before and After!
if(run_TED_SITRAF == 1){
    #Send and Rec ---------------- WORKED!
    variables <- c("ltrans","lvalor")
    variables_labels <- c("Log Transactions","Log Value")
    tryCatch({
      for(i in 1:3){
        if(i==1){
          flood_a <- flood_week_after
          flood_b <- flood_week_before2019
          xll <- xl
          xuu <- xu
          ending <- ""
          beginning <- paste0("Ted_sitraf_", ending)
          beginning_PF <- paste0("Ted_sitraf_PF_", ending)
          beginning_PJ <- paste0("Ted_sitraf_PJ_", ending)
          beginning_PF2 <- paste0("Ted_sitraf_PF_2", ending)
          beginning_PJ2 <- paste0("Ted_sitraf_PJ_2", ending)
          legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
        }
        if(i==2){
          flood_a <- flood_week_after_balanced
          flood_b <- flood_week_before_balanced2019
          xll <- xl_balanced
          xuu <- xu_balanced
          ending <- "balanced_"
          beginning <- paste0("Ted_sitraf_", ending)
          beginning_PF <- paste0("Ted_sitraf_PF_", ending)
          beginning_PJ <- paste0("Ted_sitraf_PJ_", ending)
          beginning_PF2 <- paste0("Ted_sitraf_PF_2", ending)
          beginning_PJ2 <- paste0("Ted_sitraf_PJ_2", ending)
          legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
        }
        if(i==3){
          flood_a <- flood_week_after_balanced_covid
          flood_b <- flood_week_before_balanced_covid
          xll <- xl_balanced_covid
          xuu <- xu_balanced_covid
          ending <- "balanced_covid_"
          beginning <- paste0("Ted_sitraf_", ending)
          beginning_PF <- paste0("Ted_sitraf_PF_", ending)
          beginning_PJ <- paste0("Ted_sitraf_PJ_", ending)
          beginning_PF2 <- paste0("Ted_sitraf_PF_2", ending)
          beginning_PJ2 <- paste0("Ted_sitraf_PJ_2", ending)
          legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
        }
        dat_rec <- prepare_data("TED_SITRAF_aggreg_rec.dta",flood_a,mun_fe,mun_control)
        dat_send <- prepare_data("TED_SITRAF_aggreg_send.dta",flood_a,mun_fe,mun_control)
        dat_combined <- bind_rows(dat_send, dat_rec) %>%
          group_by(across(-c(trans, valor, ltrans, lvalor))) %>%
          summarize(trans = sum(trans, na.rm = TRUE),
                    valor = sum(valor, na.rm = TRUE)) %>% ungroup() %>% 
          mutate(ltrans = log1p(trans),
                 lvalor = log1p(valor)) 
        dat_rec1 <- dat_rec %>% filter(tipo == 1)
        dat_send1 <- dat_send %>% filter(tipo == 1)
        dat_combined1 <- dat_combined %>% filter(tipo == 1)
        # Firms
        dat_rec2 <- dat_rec %>% filter(tipo == 2)
        dat_send2 <- dat_send %>% filter(tipo == 2)
        dat_combined2 <- dat_combined %>% filter(tipo == 2)
        # Firms + People
        dat_combined0 <- dat_combined %>% group_by(across(-c(trans, valor, ltrans, lvalor, tipo))) %>%
          summarize(trans = sum(trans, na.rm = TRUE),
                    valor = sum(valor, na.rm = TRUE)) %>% ungroup() %>% 
          mutate(ltrans = log1p(trans),
                 lvalor = log1p(valor)) 

        dat_rec_b <- prepare_data("TED_SITRAF_aggreg_rec.dta",flood_b,mun_fe,mun_control)
        dat_send_b <- prepare_data("TED_SITRAF_aggreg_send.dta",flood_b,mun_fe,mun_control)
        dat_combined_b <- bind_rows(dat_send_b, dat_rec_b) %>%
          group_by(across(-c(trans, valor, ltrans, lvalor))) %>%
          summarize(trans = sum(trans, na.rm = TRUE),
                    valor = sum(valor, na.rm = TRUE)) %>% ungroup() %>% 
          mutate(ltrans = log1p(trans),
                 lvalor = log1p(valor)) 
        dat_rec1_b <- dat_rec_b %>% filter(tipo == 1)
        dat_send1_b <- dat_send_b %>% filter(tipo == 1)
        dat_combined1_b <- dat_combined_b %>% filter(tipo == 1)
        # Firms
        dat_rec2_b <- dat_rec_b %>% filter(tipo == 2)
        dat_send2_b <- dat_send_b %>% filter(tipo == 2)
        dat_combined2_b <- dat_combined_b %>% filter(tipo == 2)
        # Firms + People
          dat_combined0_b <- dat_combined_b %>% group_by(across(-c(trans, valor, ltrans, lvalor, tipo))) %>%
          summarize(trans = sum(trans, na.rm = TRUE),
                    valor = sum(valor, na.rm = TRUE)) %>% ungroup() %>% 
          mutate(ltrans = log1p(trans),
                 lvalor = log1p(valor)) 
        
        for(z in 1:length(variables)){
          twfe2(beginning_PF,variables[[z]],"constant","constant","flood_risk5", list(dat_rec1, dat_send1), c("rec","send"))
          print_twfe_week(beginning_PF, variables[[z]], variables_labels[[z]], c("rec","send"), c("Received","Sent"), xll, xuu)
          twfe2(beginning_PF2,variables[[z]],"constant","constant","flood_risk5", list(dat_combined1_b, dat_combined1), c("before","after"))
          print_twfe_week(beginning_PF2, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
          
          twfe2(beginning_PJ,variables[[z]],"constant","constant","flood_risk5", list(dat_rec2, dat_send2), c("rec","send"))
          print_twfe_week(beginning_PJ, variables[[z]], variables_labels[[z]], c("rec","send"), c("Received","Sent"), xll, xuu)
          twfe2(beginning_PJ2,variables[[z]],"constant","constant","flood_risk5", list(dat_combined2_b, dat_combined2), c("before","after"))
          print_twfe_week(beginning_PJ2, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)

          twfe2(beginning,variables[[z]],"constant","constant","flood_risk5", list(dat_combined0_b, dat_combined0), c("before","after"))
          print_twfe_week(beginning, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
        }
        rm(dat_rec1, dat_send1, dat_combined1, dat_rec2, dat_send2, dat_combined2, dat_rec, dat_send, dat_combined)
        rm(dat_rec1_b, dat_send1_b, dat_combined1_b, dat_rec2_b, dat_send2_b, dat_combined2_b, dat_rec_b, dat_send_b, dat_combined_b)
      }
    }, error = function(e) {
      print(paste("Error in Send Rec:", e))
    })

    ########### USERS HERE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    # Self worked! The rest didnt work.
    
    # Inflow vs Outflow + Self        
    variables <- c("lusers","lusers2","ltrans", "lvalor")
    variables_labels <- c("Log Active Users Inside the Municipality","Log Active Users Outside the Municipality","Log Transactions","Log Value")
    variables_self <- c("senders", "ltrans", "lvalor")
    variables_self_labels <- c("Log Active Users", "Log Transactions", "Log Value")
    tryCatch({
      for(i in 1:3){
        if(i==1){
          flood_a <- flood_week_after
          flood_b <- flood_week_before2019
          xll <- xl
          xuu <- xu
          ending <- ""
          beginning_p2p <- paste0("Ted_sitraf_flow_p2p_", ending)
          beginning_p2p_b <- paste0("Ted_sitraf_flow_p2p_b_", ending)
          beginning_PF_self <- paste0("Ted_sitraf_PF_self_", ending)
          beginning_PJ_self <- paste0("Ted_sitraf_PJ_self_", ending)
          legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
        }
        if(i==2){
          flood_a <- flood_week_after_balanced
          flood_b <- flood_week_before_balanced2019
          xll <- xl_balanced
          xuu <- xu_balanced
          ending <- "balanced_"
          beginning_p2p <- paste0("Ted_sitraf_flow_p2p_", ending)
          beginning_p2p_b <- paste0("Ted_sitraf_flow_p2p_b_", ending)
          beginning_PF_self <- paste0("Ted_sitraf_PF_self_", ending)
          beginning_PJ_self <- paste0("Ted_sitraf_PJ_self_", ending)
          legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
        }
        if(i==3){
          flood_a <- flood_week_after_balanced_covid
          flood_b <- flood_week_before_balanced_covid
          xll <- xl_balanced_covid
          xuu <- xu_balanced_covid
          ending <- "balanced_covid_"
          beginning_p2p <- paste0("Ted_sitraf_flow_p2p_", ending)
          beginning_p2p_b <- paste0("Ted_sitraf_flow_p2p_b_", ending)
          beginning_PF_self <- paste0("Ted_sitraf_PF_self_", ending)
          beginning_PJ_self <- paste0("Ted_sitraf_PJ_self_", ending)
          legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
        }
        dat <- prepare_data("TED_SITRAF.dta",flood_a,mun_fe,mun_control)
        dat_inflow_p2p <- dat %>%
          filter(sender_type == 1, receiver_type == 1, flow_code == 1) %>%
          rename(lusers = lreceivers,
                 lusers2 = lsenders)
        dat_outflow_p2p <- dat %>%
          filter(sender_type == 1, receiver_type == 1, flow_code == -1) %>%
          rename(lusers=lsenders,
                 lusers2=lreceivers)
        dat_self <- dat %>%
          filter(flow_code == 99)
        dat_self_PF <- dat_self %>%
          filter(receiver_type == 1)
        dat_self_PJ <- dat_self %>%
          filter(receiver_type == 2)
        
        dat_b <- prepare_data("TED_SITRAF.dta",flood_b,mun_fe,mun_control)
        dat_inflow_p2p_b <- dat_b %>%
          filter(sender_type == 1, receiver_type == 1, flow_code == 1) %>%
          rename(lusers = lreceivers,
                 lusers2 = lsenders)
        dat_outflow_p2p_b <- dat_b %>%
          filter(sender_type == 1, receiver_type == 1, flow_code == -1) %>%
          rename(lusers=lsenders,
                 lusers2=lreceivers)
        dat_self_b <- dat_b %>%
          filter(flow_code == 99)
        dat_self_PF_b <- dat_self_b %>%
          filter(receiver_type == 1)
        dat_self_PJ_b <- dat_self_b %>%
          filter(receiver_type == 2)
        
        for(z in 1:length(variables)){
          twfe2(beginning_p2p,variables[[z]],"constant","constant","flood_risk5", list(dat_inflow_p2p, dat_outflow_p2p), c("inflow","outflow"))
          print_twfe_week(beginning_p2p, variables[[z]], variables_labels[[z]], c("inflow","outflow"), c("Receiving","Sending"), xll, xuu)
          
          twfe2(beginning_p2p_b,variables[[z]],"constant","constant","flood_risk5", list(dat_inflow_p2p_b, dat_outflow_p2p_b), c("inflow","outflow"))
          print_twfe_week(beginning_p2p_b, variables[[z]], variables_labels[[z]], c("inflow","outflow"), c("Receiving","Sending"), xll, xuu)
        }
        twfe2(beginning_p2p,"lusers2","constant","constant","flood_risk5",list(dat_outflow_p2p, dat_inflow_p2p), c("inflow","outflow"))
        print_twfe_week(beginning_p2p,"lusers","Log Active Users Inside the Municipaltiy", c("inflow"), c("Receiving"), xll, xuu)
        print_twfe_week(beginning_p2p,"lusers2","Log Active Users Outside the Municipaltiy", c("outflow"), c("Sending"), xll, xuu)
        
        twfe2(beginning_p2p_b,"lusers2","constant","constant","flood_risk5",list(dat_outflow_p2p_b, dat_inflow_p2p_b), c("inflow","outflow"))
        print_twfe_week(beginning_p2p_b,"lusers","Log Active Users Inside the Municipaltiy", c("inflow"), c("Receiving"), xll, xuu)
        print_twfe_week(beginning_p2p_b,"lusers2","Log Active Users Outside the Municipaltiy", c("outflow"), c("Sending"), xll, xuu)
        
        for(z in 1:length(variables_self)){
        twfe2(beginning_PF_self,variables_self[[z]],"constant","constant","flood_risk5", list(dat_self_PF_b, dat_self_PF), c("before","after"))
        print_twfe_week(beginning_PF_self, variables_self[[z]], variables_self_labels[[z]], c("before","after"), legend_name, xll, xuu)
        
        twfe2(beginning_PJ_self,variables_self[[z]],"constant","constant","flood_risk5", list(dat_self_PJ_b, dat_self_PJ), c("before","after"))
        print_twfe_week(beginning_PJ_self, variables_self[[z]], variables_self_labels[[z]], c("before","after"), legend_name, xll, xuu)
        }
        
        rm(dat_inflow_p2p, dat_outflow_p2p, dat, dat_self, dat_self_PF, dat_self_PJ)
      }
    }, error = function(e) {
      print(paste("Error in inflow and outflow:", e))
    })
}


if(run_TED_STR == 1){
  #Send and Rec ---------------- WORKED!
  variables <- c("ltrans","lvalor")
  variables_labels <- c("Log Transactions","Log Value")
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood_a <- flood_week_after
        flood_b <- flood_week_before2019
        xll <- xl
        xuu <- xu
        ending <- ""
        beginning <- paste0("Ted_str_", ending)
        beginning_PF <- paste0("Ted_str_PF_", ending)
        beginning_PJ <- paste0("Ted_str_PJ_", ending)
        beginning_PF2 <- paste0("Ted_str_PF_2", ending)
        beginning_PJ2 <- paste0("Ted_str_PJ_2", ending)
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==2){
        flood_a <- flood_week_after_balanced
        flood_b <- flood_week_before_balanced2019
        xll <- xl_balanced
        xuu <- xu_balanced
        ending <- "balanced_"
        beginning <- paste0("Ted_str_", ending)
        beginning_PF <- paste0("Ted_str_PF_", ending)
        beginning_PJ <- paste0("Ted_str_PJ_", ending)
        beginning_PF2 <- paste0("Ted_str_PF_2", ending)
        beginning_PJ2 <- paste0("Ted_str_PJ_2", ending)
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==3){
        flood_a <- flood_week_after_balanced_covid
        flood_b <- flood_week_before_balanced_covid
        xll <- xl_balanced_covid
        xuu <- xu_balanced_covid
        ending <- "balanced_covid_"
        beginning <- paste0("Ted_str_", ending)
        beginning_PF <- paste0("Ted_str_PF_", ending)
        beginning_PJ <- paste0("Ted_str_PJ_", ending)
        beginning_PF2 <- paste0("Ted_str_PF_2", ending)
        beginning_PJ2 <- paste0("Ted_str_PJ_2", ending)
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
      }
      dat_rec <- prepare_data("TED_STR_aggreg_rec.dta",flood_a,mun_fe,mun_control)
      dat_send <- prepare_data("TED_STR_aggreg_send.dta",flood_a,mun_fe,mun_control)
      dat_combined <- bind_rows(dat_send, dat_rec) %>%
        group_by(across(-c(trans, valor, ltrans, lvalor))) %>%
        summarize(trans = sum(trans, na.rm = TRUE),
                  valor = sum(valor, na.rm = TRUE)) %>% ungroup() %>% 
        mutate(ltrans = log1p(trans),
               lvalor = log1p(valor)) 
      dat_rec1 <- dat_rec %>% filter(tipo == 1)
      dat_send1 <- dat_send %>% filter(tipo == 1)
      dat_combined1 <- dat_combined %>% filter(tipo == 1)
      # Firms
      dat_rec2 <- dat_rec %>% filter(tipo == 2)
      dat_send2 <- dat_send %>% filter(tipo == 2)
      dat_combined2 <- dat_combined %>% filter(tipo == 2)
      # Firms + People
      dat_combined0 <- dat_combined %>% group_by(across(-c(trans, valor, ltrans, lvalor, tipo))) %>%
        summarize(trans = sum(trans, na.rm = TRUE),
                  valor = sum(valor, na.rm = TRUE)) %>% ungroup() %>% 
        mutate(ltrans = log1p(trans),
               lvalor = log1p(valor)) 
      
      dat_rec_b <- prepare_data("TED_STR_aggreg_rec.dta",flood_b,mun_fe,mun_control)
      dat_send_b <- prepare_data("TED_STR_aggreg_send.dta",flood_b,mun_fe,mun_control)
      dat_combined_b <- bind_rows(dat_send_b, dat_rec_b) %>%
        group_by(across(-c(trans, valor, ltrans, lvalor))) %>%
        summarize(trans = sum(trans, na.rm = TRUE),
                  valor = sum(valor, na.rm = TRUE)) %>% ungroup() %>% 
        mutate(ltrans = log1p(trans),
               lvalor = log1p(valor)) 
      dat_rec1_b <- dat_rec_b %>% filter(tipo == 1)
      dat_send1_b <- dat_send_b %>% filter(tipo == 1)
      dat_combined1_b <- dat_combined_b %>% filter(tipo == 1)
      # Firms
      dat_rec2_b <- dat_rec_b %>% filter(tipo == 2)
      dat_send2_b <- dat_send_b %>% filter(tipo == 2)
      dat_combined2_b <- dat_combined_b %>% filter(tipo == 2)
      # Firms + People
      dat_combined0_b <- dat_combined_b %>% group_by(across(-c(trans, valor, ltrans, lvalor, tipo))) %>%
        summarize(trans = sum(trans, na.rm = TRUE),
                  valor = sum(valor, na.rm = TRUE)) %>% ungroup() %>% 
        mutate(ltrans = log1p(trans),
               lvalor = log1p(valor)) 
      
      for(z in 1:length(variables)){
        twfe2(beginning_PF,variables[[z]],"constant","constant","flood_risk5", list(dat_rec1, dat_send1), c("rec","send"))
        print_twfe_week(beginning_PF, variables[[z]], variables_labels[[z]], c("rec","send"), c("Received","Sent"), xll, xuu)
        twfe2(beginning_PF2,variables[[z]],"constant","constant","flood_risk5", list(dat_combined1_b, dat_combined1), c("before","after"))
        print_twfe_week(beginning_PF2, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
        
        twfe2(beginning_PJ,variables[[z]],"constant","constant","flood_risk5", list(dat_rec2, dat_send2), c("rec","send"))
        print_twfe_week(beginning_PJ, variables[[z]], variables_labels[[z]], c("rec","send"), c("Received","Sent"), xll, xuu)
        twfe2(beginning_PJ2,variables[[z]],"constant","constant","flood_risk5", list(dat_combined2_b, dat_combined2), c("before","after"))
        print_twfe_week(beginning_PJ2, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
        
        twfe2(beginning,variables[[z]],"constant","constant","flood_risk5", list(dat_combined0_b, dat_combined0), c("before","after"))
        print_twfe_week(beginning, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
      }
      rm(dat_rec1, dat_send1, dat_combined1, dat_rec2, dat_send2, dat_combined2, dat_rec, dat_send, dat_combined)
      rm(dat_rec1_b, dat_send1_b, dat_combined1_b, dat_rec2_b, dat_send2_b, dat_combined2_b, dat_rec_b, dat_send_b, dat_combined_b)
    }
  }, error = function(e) {
    print(paste("Error in Send Rec2:", e))
  })
  
  ########### USERS HERE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  # Self worked! The rest didnt work.
  
  # Inflow vs Outflow + Self        
  variables <- c("lusers","lusers2","ltrans", "lvalor")
  variables_labels <- c("Log Active Users Inside the Municipality","Log Active Users Outside the Municipality","Log Transactions","Log Value")
  variables_self <- c("senders", "ltrans", "lvalor")
  variables_self_labels <- c("Log Active Users", "Log Transactions", "Log Value")
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood_a <- flood_week_after
        flood_b <- flood_week_before2019
        xll <- xl
        xuu <- xu
        ending <- ""
        beginning_p2p <- paste0("Ted_str_flow_p2p_", ending)
        beginning_p2p_b <- paste0("Ted_str_flow_p2p_b_", ending)
        beginning_PF_self <- paste0("Ted_str_PF_self_", ending)
        beginning_PJ_self <- paste0("Ted_str_PJ_self_", ending)
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==2){
        flood_a <- flood_week_after_balanced
        flood_b <- flood_week_before_balanced2019
        xll <- xl_balanced
        xuu <- xu_balanced
        ending <- "balanced_"
        beginning_p2p <- paste0("Ted_str_flow_p2p_", ending)
        beginning_p2p_b <- paste0("Ted_str_flow_p2p_b_", ending)
        beginning_PF_self <- paste0("Ted_str_PF_self_", ending)
        beginning_PJ_self <- paste0("Ted_str_PJ_self_", ending)
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==3){
        flood_a <- flood_week_after_balanced_covid
        flood_b <- flood_week_before_balanced_covid
        xll <- xl_balanced_covid
        xuu <- xu_balanced_covid
        ending <- "balanced_covid_"
        beginning_p2p <- paste0("Ted_str_flow_p2p_", ending)
        beginning_p2p_b <- paste0("Ted_str_flow_p2p_b_", ending)
        beginning_PF_self <- paste0("Ted_str_PF_self_", ending)
        beginning_PJ_self <- paste0("Ted_str_PJ_self_", ending)
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
      }
      dat <- prepare_data("TED_STR.dta",flood_a,mun_fe,mun_control)
      dat_inflow_p2p <- dat %>%
        filter(sender_type == 1, receiver_type == 1, flow_code == 1) %>%
        rename(lusers = lreceivers,
               lusers2 = lsenders)
      dat_outflow_p2p <- dat %>%
        filter(sender_type == 1, receiver_type == 1, flow_code == -1) %>%
        rename(lusers=lsenders,
               lusers2=lreceivers)
      dat_self <- dat %>%
        filter(flow_code == 99)
      dat_self_PF <- dat_self %>%
        filter(receiver_type == 1)
      dat_self_PJ <- dat_self %>%
        filter(receiver_type == 2)
      
      dat_b <- prepare_data("TED_STR.dta",flood_b,mun_fe,mun_control)
      dat_inflow_p2p_b <- dat_b %>%
        filter(sender_type == 1, receiver_type == 1, flow_code == 1) %>%
        rename(lusers = lreceivers,
               lusers2 = lsenders)
      dat_outflow_p2p_b <- dat_b %>%
        filter(sender_type == 1, receiver_type == 1, flow_code == -1) %>%
        rename(lusers=lsenders,
               lusers2=lreceivers)
      dat_self_b <- dat_b %>%
        filter(flow_code == 99)
      dat_self_PF_b <- dat_self_b %>%
        filter(receiver_type == 1)
      dat_self_PJ_b <- dat_self_b %>%
        filter(receiver_type == 2)
      
      for(z in 1:length(variables)){
        twfe2(beginning_p2p,variables[[z]],"constant","constant","flood_risk5", list(dat_inflow_p2p, dat_outflow_p2p), c("inflow","outflow"))
        print_twfe_week(beginning_p2p, variables[[z]], variables_labels[[z]], c("inflow","outflow"), c("Receiving","Sending"), xll, xuu)
        
        twfe2(beginning_p2p_b,variables[[z]],"constant","constant","flood_risk5", list(dat_inflow_p2p_b, dat_outflow_p2p_b), c("inflow","outflow"))
        print_twfe_week(beginning_p2p_b, variables[[z]], variables_labels[[z]], c("inflow","outflow"), c("Receiving","Sending"), xll, xuu)
      }
      twfe2(beginning_p2p,"lusers2","constant","constant","flood_risk5",list(dat_outflow_p2p, dat_inflow_p2p), c("inflow","outflow"))
      print_twfe_week(beginning_p2p,"lusers","Log Active Users Inside the Municipaltiy", c("inflow"), c("Receiving"), xll, xuu)
      print_twfe_week(beginning_p2p,"lusers2","Log Active Users Outside the Municipaltiy", c("outflow"), c("Sending"), xll, xuu)
      
      twfe2(beginning_p2p_b,"lusers2","constant","constant","flood_risk5",list(dat_outflow_p2p_b, dat_inflow_p2p_b), c("inflow","outflow"))
      print_twfe_week(beginning_p2p_b,"lusers","Log Active Users Inside the Municipaltiy", c("inflow"), c("Receiving"), xll, xuu)
      print_twfe_week(beginning_p2p_b,"lusers2","Log Active Users Outside the Municipaltiy", c("outflow"), c("Sending"), xll, xuu)
      
      for(z in 1:length(variables_self)){
        twfe2(beginning_PF_self,variables_self[[z]],"constant","constant","flood_risk5", list(dat_self_PF_b, dat_self_PF), c("before","after"))
        print_twfe_week(beginning_PF_self, variables_self[[z]], variables_self_labels[[z]], c("before","after"), legend_name, xll, xuu)
        
        twfe2(beginning_PJ_self,variables_self[[z]],"constant","constant","flood_risk5", list(dat_self_PJ_b, dat_self_PJ), c("before","after"))
        print_twfe_week(beginning_PJ_self, variables_self[[z]], variables_self_labels[[z]], c("before","after"), legend_name, xll, xuu)
      }
      
      rm(dat_inflow_p2p, dat_outflow_p2p, dat, dat_self, dat_self_PF, dat_self_PJ)
    }
  }, error = function(e) {
    print(paste("Error in inflow and outflow2:", e))
  })
}


if(run_TED == 1){
  #Send and Rec ---------------- WORKED!
  variables <- c("ltrans","lvalor")
  variables_labels <- c("Log Transactions","Log Value")
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood_a <- flood_week_after
        flood_b <- flood_week_before2019
        xll <- xl
        xuu <- xu
        ending <- ""
        beginning <- paste0("Ted_", ending)
        beginning_PF <- paste0("Ted_PF_", ending)
        beginning_PJ <- paste0("Ted_PJ_", ending)
        beginning_PF2 <- paste0("Ted_PF_2", ending)
        beginning_PJ2 <- paste0("Ted_PJ_2", ending)
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==2){
        flood_a <- flood_week_after_balanced
        flood_b <- flood_week_before_balanced2019
        xll <- xl_balanced
        xuu <- xu_balanced
        ending <- "balanced_"
        beginning <- paste0("Ted_", ending)
        beginning_PF <- paste0("Ted_PF_", ending)
        beginning_PJ <- paste0("Ted_PJ_", ending)
        beginning_PF2 <- paste0("Ted_PF_2", ending)
        beginning_PJ2 <- paste0("Ted_PJ_2", ending)
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==3){
        flood_a <- flood_week_after_balanced_covid
        flood_b <- flood_week_before_balanced_covid
        xll <- xl_balanced_covid
        xuu <- xu_balanced_covid
        ending <- "balanced_covid_"
        beginning <- paste0("Ted_", ending)
        beginning_PF <- paste0("Ted_PF_", ending)
        beginning_PJ <- paste0("Ted_PJ_", ending)
        beginning_PF2 <- paste0("Ted_PF_2", ending)
        beginning_PJ2 <- paste0("Ted_PJ_2", ending)
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
      }
      dat_rec <- prepare_data("TED_aggreg_rec.dta",flood_a,mun_fe,mun_control)
      dat_send <- prepare_data("TED_aggreg_send.dta",flood_a,mun_fe,mun_control)
      dat_combined <- bind_rows(dat_send, dat_rec) %>%
        group_by(across(-c(trans, valor, ltrans, lvalor))) %>%
        summarize(trans = sum(trans, na.rm = TRUE),
                  valor = sum(valor, na.rm = TRUE)) %>% ungroup() %>% 
        mutate(ltrans = log1p(trans),
               lvalor = log1p(valor)) 
      dat_rec1 <- dat_rec %>% filter(tipo == 1)
      dat_send1 <- dat_send %>% filter(tipo == 1)
      dat_combined1 <- dat_combined %>% filter(tipo == 1)
      # Firms
      dat_rec2 <- dat_rec %>% filter(tipo == 2)
      dat_send2 <- dat_send %>% filter(tipo == 2)
      dat_combined2 <- dat_combined %>% filter(tipo == 2)
      # Firms + People
      dat_combined0 <- dat_combined %>% group_by(across(-c(trans, valor, ltrans, lvalor, tipo))) %>%
        summarize(trans = sum(trans, na.rm = TRUE),
                  valor = sum(valor, na.rm = TRUE)) %>% ungroup() %>% 
        mutate(ltrans = log1p(trans),
               lvalor = log1p(valor)) 
      
      dat_rec_b <- prepare_data("TED_aggreg_rec.dta",flood_b,mun_fe,mun_control)
      dat_send_b <- prepare_data("TED_aggreg_send.dta",flood_b,mun_fe,mun_control)
      dat_combined_b <- bind_rows(dat_send_b, dat_rec_b) %>%
        group_by(across(-c(trans, valor, ltrans, lvalor))) %>%
        summarize(trans = sum(trans, na.rm = TRUE),
                  valor = sum(valor, na.rm = TRUE)) %>% ungroup() %>% 
        mutate(ltrans = log1p(trans),
               lvalor = log1p(valor)) 
      dat_rec1_b <- dat_rec_b %>% filter(tipo == 1)
      dat_send1_b <- dat_send_b %>% filter(tipo == 1)
      dat_combined1_b <- dat_combined_b %>% filter(tipo == 1)
      # Firms
      dat_rec2_b <- dat_rec_b %>% filter(tipo == 2)
      dat_send2_b <- dat_send_b %>% filter(tipo == 2)
      dat_combined2_b <- dat_combined_b %>% filter(tipo == 2)
      # Firms + People
      dat_combined0_b <- dat_combined_b %>% group_by(across(-c(trans, valor, ltrans, lvalor, tipo))) %>%
        summarize(trans = sum(trans, na.rm = TRUE),
                  valor = sum(valor, na.rm = TRUE)) %>% ungroup() %>% 
        mutate(ltrans = log1p(trans),
               lvalor = log1p(valor)) 
      
      for(z in 1:length(variables)){
        twfe2(beginning_PF,variables[[z]],"constant","constant","flood_risk5", list(dat_rec1, dat_send1), c("rec","send"))
        print_twfe_week(beginning_PF, variables[[z]], variables_labels[[z]], c("rec","send"), c("Received","Sent"), xll, xuu)
        twfe2(beginning_PF2,variables[[z]],"constant","constant","flood_risk5", list(dat_combined1_b, dat_combined1), c("before","after"))
        print_twfe_week(beginning_PF2, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
        
        twfe2(beginning_PJ,variables[[z]],"constant","constant","flood_risk5", list(dat_rec2, dat_send2), c("rec","send"))
        print_twfe_week(beginning_PJ, variables[[z]], variables_labels[[z]], c("rec","send"), c("Received","Sent"), xll, xuu)
        twfe2(beginning_PJ2,variables[[z]],"constant","constant","flood_risk5", list(dat_combined2_b, dat_combined2), c("before","after"))
        print_twfe_week(beginning_PJ2, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
        
        twfe2(beginning,variables[[z]],"constant","constant","flood_risk5", list(dat_combined0_b, dat_combined0), c("before","after"))
        print_twfe_week(beginning, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
      }
      rm(dat_rec1, dat_send1, dat_combined1, dat_rec2, dat_send2, dat_combined2, dat_rec, dat_send, dat_combined)
      rm(dat_rec1_b, dat_send1_b, dat_combined1_b, dat_rec2_b, dat_send2_b, dat_combined2_b, dat_rec_b, dat_send_b, dat_combined_b)
    }
  }, error = function(e) {
    print(paste("Error in Send Rec3:", e))
  })
  
  ########### USERS HERE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  # Self worked! The rest didnt work.
  
  # Inflow vs Outflow + Self        
  variables <- c("lusers","lusers2","ltrans", "lvalor")
  variables_labels <- c("Log Active Users Inside the Municipality","Log Active Users Outside the Municipality","Log Transactions","Log Value")
  variables_self <- c("senders", "ltrans", "lvalor")
  variables_self_labels <- c("Log Active Users", "Log Transactions", "Log Value")
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood_a <- flood_week_after
        flood_b <- flood_week_before2019
        xll <- xl
        xuu <- xu
        ending <- ""
        beginning_p2p <- paste0("Ted_flow_p2p_", ending)
        beginning_p2p_b <- paste0("Ted_flow_p2p_b_", ending)
        beginning_PF_self <- paste0("Ted_PF_self_", ending)
        beginning_PJ_self <- paste0("Ted_PJ_self_", ending)
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==2){
        flood_a <- flood_week_after_balanced
        flood_b <- flood_week_before_balanced2019
        xll <- xl_balanced
        xuu <- xu_balanced
        ending <- "balanced_"
        beginning_p2p <- paste0("Ted_flow_p2p_", ending)
        beginning_p2p_b <- paste0("Ted_flow_p2p_b_", ending)
        beginning_PF_self <- paste0("Ted_PF_self_", ending)
        beginning_PJ_self <- paste0("Ted_PJ_self_", ending)
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==3){
        flood_a <- flood_week_after_balanced_covid
        flood_b <- flood_week_before_balanced_covid
        xll <- xl_balanced_covid
        xuu <- xu_balanced_covid
        ending <- "balanced_covid_"
        beginning_p2p <- paste0("Ted_flow_p2p_", ending)
        beginning_p2p_b <- paste0("Ted_flow_p2p_b_", ending)
        beginning_PF_self <- paste0("Ted_PF_self_", ending)
        beginning_PJ_self <- paste0("Ted_PJ_self_", ending)
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
      }
      dat <- prepare_data("TED.dta",flood_a,mun_fe,mun_control)
      dat_inflow_p2p <- dat %>%
        filter(sender_type == 1, receiver_type == 1, flow_code == 1) %>%
        rename(lusers = lreceivers,
               lusers2 = lsenders)
      dat_outflow_p2p <- dat %>%
        filter(sender_type == 1, receiver_type == 1, flow_code == -1) %>%
        rename(lusers=lsenders,
               lusers2=lreceivers)
      dat_self <- dat %>%
        filter(flow_code == 99)
      dat_self_PF <- dat_self %>%
        filter(receiver_type == 1)
      dat_self_PJ <- dat_self %>%
        filter(receiver_type == 2)
      
      dat_b <- prepare_data("TED.dta",flood_b,mun_fe,mun_control)
      dat_inflow_p2p_b <- dat_b %>%
        filter(sender_type == 1, receiver_type == 1, flow_code == 1) %>%
        rename(lusers = lreceivers,
               lusers2 = lsenders)
      dat_outflow_p2p_b <- dat_b %>%
        filter(sender_type == 1, receiver_type == 1, flow_code == -1) %>%
        rename(lusers=lsenders,
               lusers2=lreceivers)
      dat_self_b <- dat_b %>%
        filter(flow_code == 99)
      dat_self_PF_b <- dat_self_b %>%
        filter(receiver_type == 1)
      dat_self_PJ_b <- dat_self_b %>%
        filter(receiver_type == 2)
      
      for(z in 1:length(variables)){
        twfe2(beginning_p2p,variables[[z]],"constant","constant","flood_risk5", list(dat_inflow_p2p, dat_outflow_p2p), c("inflow","outflow"))
        print_twfe_week(beginning_p2p, variables[[z]], variables_labels[[z]], c("inflow","outflow"), c("Receiving","Sending"), xll, xuu)
        
        twfe2(beginning_p2p_b,variables[[z]],"constant","constant","flood_risk5", list(dat_inflow_p2p_b, dat_outflow_p2p_b), c("inflow","outflow"))
        print_twfe_week(beginning_p2p_b, variables[[z]], variables_labels[[z]], c("inflow","outflow"), c("Receiving","Sending"), xll, xuu)
      }
      twfe2(beginning_p2p,"lusers2","constant","constant","flood_risk5",list(dat_outflow_p2p, dat_inflow_p2p), c("inflow","outflow"))
      print_twfe_week(beginning_p2p,"lusers","Log Active Users Inside the Municipaltiy", c("inflow"), c("Receiving"), xll, xuu)
      print_twfe_week(beginning_p2p,"lusers2","Log Active Users Outside the Municipaltiy", c("outflow"), c("Sending"), xll, xuu)
      
      twfe2(beginning_p2p_b,"lusers2","constant","constant","flood_risk5",list(dat_outflow_p2p_b, dat_inflow_p2p_b), c("inflow","outflow"))
      print_twfe_week(beginning_p2p_b,"lusers","Log Active Users Inside the Municipaltiy", c("inflow"), c("Receiving"), xll, xuu)
      print_twfe_week(beginning_p2p_b,"lusers2","Log Active Users Outside the Municipaltiy", c("outflow"), c("Sending"), xll, xuu)
      
      for(z in 1:length(variables_self)){
        twfe2(beginning_PF_self,variables_self[[z]],"constant","constant","flood_risk5", list(dat_self_PF_b, dat_self_PF), c("before","after"))
        print_twfe_week(beginning_PF_self, variables_self[[z]], variables_self_labels[[z]], c("before","after"), legend_name, xll, xuu)
        
        twfe2(beginning_PJ_self,variables_self[[z]],"constant","constant","flood_risk5", list(dat_self_PJ_b, dat_self_PJ), c("before","after"))
        print_twfe_week(beginning_PJ_self, variables_self[[z]], variables_self_labels[[z]], c("before","after"), legend_name, xll, xuu)
      }
      
      rm(dat_inflow_p2p, dat_outflow_p2p, dat, dat_self, dat_self_PF, dat_self_PJ)
    }
  }, error = function(e) {
    print(paste("Error in inflow and outflow3:", e))
  })
}


# ------------------------------------------------------------------------------
# Pix_ind_sample
# ------------------------------------------------------------------------------

# Pix_ind_sample.dta
# Variables: week, id, muni_cd, tipo, value_send, trans_send, value_rec, trans_rec, value_self, trans_self
#                               lvalue_send, ltrans_send, lvalue_rec, ltrans_rec, lvalue_self, ltrans_self, lvalue, ltrans

# Pix_ind_sample_adoption.dta
# Variables: week, muni_cd, tipo, adoption, adoption_send, adoption_rec, adoption_self
#             ladoption, ladopt_send, ladopt_rec, ladopt_self


variables <- c("lvalue", "ltrans", "lvalue_self","ltrans_self")
variables_labels <- c("Log Value", "Log Transactions", "Log Value", "Log Transactions")
variables_adoption <- c("adoption", "adoption_send", "adoption_rec","adoption_self", "ladoption", "ladopt_send", "ladopt_rec","ladopt_self")
variables_adoption_labels <- c("Adoption", "Adoption Send", "Adoption Receive", "Self Adoption", "Log Adoption", "Log Adoption Send", "Log Adoption Receive", "Log Self Adoption")
if(run_Pix_ind_sample == 1){
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood <- flood_week_after
        xll <- xl
        xuu <- xu
        ending <- "_"
      }
      if(i==2){
        flood <- flood_week_after_balanced
        xll <- xl_balanced
        xuu <- xu_balanced
        ending <- "_balanced_"
      }
      if(i==3){
        flood <- flood_week_after_balanced_covid
        xll <- xl_balanced_covid
        xuu <- xu_balanced_covid
        ending <- "_balanced_covid_"
      }
      
      for(j in 1:2){
        if(j==1){
          beginning <- paste0("Pix_ind_sample_PF",ending)
          dat_a <- prepare_data("Pix_ind_sample_PF.dta",flood,mun_fe,mun_control)
        }
        if(j==2){
          beginning <- paste0("Pix_ind_sample_PJ",ending)
          dat_a <- prepare_data("Pix_ind_sample_PJ.dta",flood,mun_fe,mun_control)
        }
        
        for(z in 1:length(variables)){
          twfe_ind(beginning,variables[[z]],"constant","constant","flood_risk5", list(dat_a), c("pix"))
          print_twfe_week(beginning, variables[[z]], variables_labels[[z]], c("pix"), c("Pix"), xll, xuu)
        }
        rm(dat_a)
      }
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in Pix_ind_sample:", e))
  })
  
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood <- flood_week_after
        xll <- xl
        xuu <- xu
        ending <- "_"
      }
      if(i==2){
        flood <- flood_week_after_balanced
        xll <- xl_balanced
        xuu <- xu_balanced
        ending <- "_balanced_"
      }
      if(i==3){
        flood <- flood_week_after_balanced_covid
        xll <- xl_balanced_covid
        xuu <- xu_balanced_covid
        ending <- "_balanced_covid_"
      }
      
      for(j in 1:2){
        if(j==1){
          beginning_adoption <- paste0("Pix_ind_sample_PF_adoption",ending)
          dat_adoption_a <- prepare_data("Pix_ind_sample_PF_adoption.dta",flood,mun_fe,mun_control)
        }
        if(j==2){
          beginning_adoption <- paste0("Pix_ind_sample_PJ_adoption",ending)
          dat_adoption_a <- prepare_data("Pix_ind_sample_PJ_adoption.dta",flood,mun_fe,mun_control)
        }

        for(z in 1:length(variables_adoption)){
          twfe2(beginning_adoption,variables_adoption[[z]],"constant","constant","flood_risk5", list(dat_adoption_a), c("pix"))
          print_twfe_week(beginning_adoption, variables_adoption[[z]], variables_adoption_labels[[z]], c("pix"), c("Pix"), xll, xuu)
        }
        rm(dat_adoption_a)
      }
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in Pix_ind_sample adoption:", e))
  })
}

# ------------------------------------------------------------------------------
# Boleto_ind_sample
# ------------------------------------------------------------------------------

# Boleto_ind_sample.dta
# Variables: week, id, muni_cd, tipo, value_send, trans_send, value_rec, trans_rec, value_self, trans_self
#                               lvalue_send, ltrans_send, lvalue_rec, ltrans_rec, lvalue_self, ltrans_self, lvalue, ltrans

# Boleto_ind_sample_adoption.dta
# Variables: week, muni_cd, tipo, adoption, adoption_send, adoption_rec, adoption_self
#             ladoption, ladopt_send, ladopt_rec, ladopt_self

variables <- c("lvalue", "ltrans", "lvalue_self","ltrans_self")
variables_labels <- c("Log Value", "Log Transactions", "Log Value", "Log Transactions")
variables_adoption <- c("adoption", "adoption_send", "adoption_rec","adoption_self", "ladoption", "ladopt_send", "ladopt_rec","ladopt_self")
variables_adoption_labels <- c("Adoption", "Adoption Send", "Adoption Receive", "Self Adoption", "Log Adoption", "Log Adoption Send", "Log Adoption Receive", "Log Self Adoption")
if(run_Boleto_ind_sample == 1){
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood_a <- flood_week_after
        flood_b <- flood_week_before2019
        xll <- xl
        xuu <- xu
        ending <- "_"
        legend_name <- c("2019.01 - 2020.10", "2020.11 - 2022.12")
      }
      if(i==2){
        flood_a <- flood_week_after_balanced
        flood_b <- flood_week_before_balanced2019
        xll <- xl_balanced
        xuu <- xu_balanced
        ending <- "_balanced_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==3){
        flood_a <- flood_week_after_balanced_covid
        flood_b <- flood_week_before_balanced_covid
        xll <- xl_balanced_covid
        xuu <- xu_balanced_covid
        ending <- "_balanced_covid_"
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
      }
      for(j in 1:2){
        if(j==1){
          beginning <- paste0("Boleto_ind_sample_PF",ending)
          dat_a <- prepare_data("Boleto_ind_sample_PF.dta",flood_a,mun_fe,mun_control)
          dat_bb <- prepare_data("Boleto_ind_sample_PF.dta",flood_b,mun_fe,mun_control)
        }
        if(j==2){
          beginning <- paste0("Boleto_ind_sample_PJ",ending)
          dat_a <- prepare_data("Boleto_ind_sample_PJ.dta",flood_a,mun_fe,mun_control)
          dat_bb <- prepare_data("Boleto_ind_sample_PJ.dta",flood_b,mun_fe,mun_control)
        }
        
        for(z in 1:length(variables)){
          twfe_ind(beginning,variables[[z]],"constant","constant","flood_risk5", list(dat_bb, dat_a), c("before", "after"))
          print_twfe_week(beginning, variables[[z]], variables_labels[[z]], c("before", "after"), legend_name, xll, xuu)
        }
        rm(dat_a)
        rm(dat_bb)
      }
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in Boleto_ind_sample:", e))
  })
  
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood_a <- flood_week_after
        flood_b <- flood_week_before2019
        xll <- xl
        xuu <- xu
        ending <- "_"
        legend_name <- c("2019.01 - 2020.10", "2020.11 - 2022.12")
      }
      if(i==2){
        flood_a <- flood_week_after_balanced
        flood_b <- flood_week_before_balanced2019
        xll <- xl_balanced
        xuu <- xu_balanced
        ending <- "_balanced_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==3){
        flood_a <- flood_week_after_balanced_covid
        flood_b <- flood_week_before_balanced_covid
        xll <- xl_balanced_covid
        xuu <- xu_balanced_covid
        ending <- "_balanced_covid_"
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
      }
      for(j in 1:2){
        if(j==1){
          beginning_adoption <- paste0("Boleto_ind_sample_PF_adoption",ending)
          dat_adoption_a <- prepare_data("Boleto_ind_sample_PF_adoption.dta",flood_a,mun_fe,mun_control)
          dat_adoption_bb <- prepare_data("Boleto_ind_sample_PF_adoption.dta",flood_b,mun_fe,mun_control)
        }
        if(j==2){
          beginning_adoption <- paste0("Boleto_ind_sample_PJ_adoption",ending)
          dat_adoption_a <- prepare_data("Boleto_ind_sample_PJ_adoption.dta",flood_a,mun_fe,mun_control)
          dat_adoption_bb <- prepare_data("Boleto_ind_sample_PJ_adoption.dta",flood_b,mun_fe,mun_control)
        }
        
        for(z in 1:length(variables_adoption)){
          twfe2(beginning_adoption,variables_adoption[[z]],"constant","constant","flood_risk5", list(dat_adoption_bb, dat_adoption_a), c("before", "after"))
          print_twfe_week(beginning_adoption, variables_adoption[[z]], variables_adoption_labels[[z]], c("before", "after"), legend_name, xll, xuu)
        }
        rm(dat_adoption_a)
        rm(dat_adoption_bb)
      }
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in Boleto_ind_sample adoption:", e))
  })
}

# ------------------------------------------------------------------------------
# Card_ind_sample 
# ------------------------------------------------------------------------------
# Before Variables: week, id, id_municipio_receita, tipo, value_credit, trans_credit, value_debit, trans_debit

# Card_ind_sample.dta 
# Variables: week, id, muni_cd, tipo, valor, trans, value_credit, trans_credit, value_debit, trans_debit
#             lvalor, ltrans, lvalue_credit, ltrans_credit, lvalue_debit, ltrans_debit

# Card_ind_sample_adoption.dta
# Variables: week, muni_cd, tipo, adoption, adoption_credit, adoption_debit
#             ladoption, ladopt_credit, ladopt_debit

variables <- c("lvalor", "ltrans", "lvalue_credit", "ltrans_credit", "lvalue_debit", "ltrans_debit")
variables_labels <- c("Log Value", "Log Transactions", "Log Value Credit", "Log Transactions Credit", "Log Value Debit", "Log Transactions Debit")
variables_adoption <- c("adoption", "adoption_credit", "adoption_debit", "ladoption", "ladopt_credit", "ladopt_debit")
variables_adoption_labels <- c("Adoption", "Adoption Credit", "Adoption Debit", "Log Adoption", "Log Adoption Credit", "Log Adoption Debit")
if(run_Card_ind_sample == 1){
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood_a <- flood_week_after
        flood_b <- flood_week_before2019
        xll <- xl
        xuu <- xu
        ending <- "_"
        legend_name <- c("2019.01 - 2020.10", "2020.11 - 2022.12")
      }
      if(i==2){
        flood_a <- flood_week_after_balanced
        flood_b <- flood_week_before_balanced2019
        xll <- xl_balanced
        xuu <- xu_balanced
        ending <- "_balanced_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==3){
        flood_a <- flood_week_after_balanced_covid
        flood_b <- flood_week_before_balanced_covid
        xll <- xl_balanced_covid
        xuu <- xu_balanced_covid
        ending <- "_balanced_covid_"
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
      }
  
      for(j in 2:2){
        if(j==1){
          beginning <- paste0("Card_ind_sample_PF",ending)
          dat_a <- prepare_data("Card_ind_sample_PF.dta",flood_a,mun_fe,mun_control)
          dat_bb <- prepare_data("Card_ind_sample_PF.dta",flood_b,mun_fe,mun_control)
        }
        if(j==2){
          beginning <- paste0("Card_ind_sample_PJ",ending)
          dat_a <- prepare_data("Card_ind_sample_PJ.dta",flood_a,mun_fe,mun_control)
          dat_bb <- prepare_data("Card_ind_sample_PJ.dta",flood_b,mun_fe,mun_control)
        }
        
        for(z in 1:length(variables)){
          twfe_ind(beginning,variables[[z]],"constant","constant","flood_risk5", list(dat_bb, dat_a), c("before", "after"))
          print_twfe_week(beginning, variables[[z]], variables_labels[[z]], c("before", "after"), legend_name, xll, xuu)
        }
      }
      rm(dat_a)
      rm(dat_bb)
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in Card_ind_sample:", e))
  })
  
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood_a <- flood_week_after
        flood_b <- flood_week_before2019
        xll <- xl
        xuu <- xu
        ending <- "_"
        legend_name <- c("2019.01 - 2020.10", "2020.11 - 2022.12")
      }
      if(i==2){
        flood_a <- flood_week_after_balanced
        flood_b <- flood_week_before_balanced2019
        xll <- xl_balanced
        xuu <- xu_balanced
        ending <- "_balanced_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==3){
        flood_a <- flood_week_after_balanced_covid
        flood_b <- flood_week_before_balanced_covid
        xll <- xl_balanced_covid
        xuu <- xu_balanced_covid
        ending <- "_balanced_covid_"
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
      }
      
      for(j in 2:2){
        if(j==1){
          beginning_adoption <- paste0("Card_ind_sample_PF_adoption",ending)
          dat_adoption_a <- prepare_data("Card_ind_sample_PF_adoption.dta",flood_a,mun_fe,mun_control)
          dat_adoption_bb <- prepare_data("Card_ind_sample_PF_adoption.dta",flood_b,mun_fe,mun_control)
        }
        if(j==2){
          beginning_adoption <- paste0("Card_ind_sample_PJ_adoption",ending)
          dat_adoption_a <- prepare_data("Card_ind_sample_PJ_adoption.dta",flood_a,mun_fe,mun_control)
          dat_adoption_bb <- prepare_data("Card_ind_sample_PJ_adoption.dta",flood_b,mun_fe,mun_control)
        }
        
        for(z in 1:length(variables_adoption)){
          twfe2(beginning_adoption,variables_adoption[[z]],"constant","constant","flood_risk5", list(dat_adoption_bb, dat_adoption_a), c("before", "after"))
          print_twfe_week(beginning_adoption, variables_adoption[[z]], variables_adoption_labels[[z]], c("before", "after"), legend_name, xll, xuu)
        }
      }
      rm(dat_adoption_a)
      rm(dat_adoption_bb)
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in Card_ind_sample adoption:", e))
  })
}

# ------------------------------------------------------------------------------
# CCS_ind_sample - problem on trad_digi #For some reason, there is only bank_type == 1, there is no other bank_type.
# ------------------------------------------------------------------------------

# Before Variables: id, tipo, bank, dia_inicio, dia_fim

# "CCS_ind_sample_PF", "CCS_ind_sample_PJ"
# Variables: id, muni_cd, tipo, bank, tipo_inst, bank_type, dia_inicio, dia_fim

# "CCS_ind_sample_PF_stock.dta", "CCS_ind_sample_PJ_stock.dta"
# Variables: id, muni_cd, tipo, bank_type, stock, week

# "CCS_ind_sample_PF_adoption.dta", "CCS_ind_sample_PJ_adoption.dta"
# Variables: id, muni_cd, tipo, bank_type, adoption_date, week

# "CCS_ind_sample_PF_adoption_collapsed.dta", "CCS_ind_sample_PJ_adoption_collapsed.dta"
# Variables: muni_cd, tipo, bank_type, week, adopters, ladopters


# "CCS_ind_sample_PF_stock2.dta", "CCS_ind_sample_PJ_stock2.dta"
# Variables: id, muni_cd, tipo, Bank, stock, week

# "CCS_ind_sample_PF_adoption2.dta", "CCS_ind_sample_PJ_adoption2.dta"
# Variables: id, muni_cd, tipo, Bank, adoption_date, week

# "CCS_ind_sample_PF_adoption_collapsed2.dta", "CCS_ind_sample_PJ_adoption_collapsed2.dta"
# Variables: muni_cd, tipo, Bank, week, adopters, ladopters



#install.packages("readstata13")
#library("readstata13")
#data <- read.dta13(file.path(dta_path,"CCS_ind_sample_PJ_stock.dta"), select.cols = c("bank_type"))
#table(data$bank_type)

#data <- read_dta(file.path(dta_path,"CCS_ind_sample_PJ_stock.dta"), n_max = 1000)
#table(data$bank_type)

#For some reason, there is only bank_type == 1, there is no other bank_type.


variables <- c("stock")
variables_labels <- c("Bank Accounts Stock")
if(run_CCS_ind_sample == 1){
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood_a <- flood_week_after
        flood_b <- flood_week_before2019
        xll <- xl
        xuu <- xu
        ending <- "_"
        legend_name <- c("2019.01 - 2020.10", "2020.11 - 2022.12")
      }
      if(i==2){
        flood_a <- flood_week_after_balanced
        flood_b <- flood_week_before_balanced2019
        xll <- xl_balanced
        xuu <- xu_balanced
        ending <- "_balanced_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==3){
        flood_a <- flood_week_after_balanced_covid
        flood_b <- flood_week_before_balanced_covid
        xll <- xl_balanced_covid
        xuu <- xu_balanced_covid
        ending <- "_balanced_covid_"
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
      }
      
      for(j in 1:2){
        if(j==1){
          beginning <- paste0("CCS_ind_sample_PF",ending)
          beginning_trad_digi <- paste0("CCS_ind_sample_PF_trad_digi",ending)
          dat_a <- prepare_data("CCS_ind_sample_PF_stock.dta",flood_a,mun_fe,mun_control)
          dat_bb <- prepare_data("CCS_ind_sample_PF_stock.dta",flood_b,mun_fe,mun_control)
          
          beginning_bank_ip <- paste0("CCS_ind_sample_PF_bank_ip",ending)
          dat_a2 <- prepare_data("CCS_ind_sample_PF_stock2.dta",flood_a,mun_fe,mun_control)
        }
        if(j==2){
          beginning <- paste0("CCS_ind_sample_PJ",ending)
          beginning_trad_digi <- paste0("CCS_ind_sample_PJ_trad_digi",ending)
          dat_a <- prepare_data("CCS_ind_sample_PJ_stock.dta",flood_a,mun_fe,mun_control)
          dat_bb <- prepare_data("CCS_ind_sample_PJ_stock.dta",flood_b,mun_fe,mun_control)
          
          beginning_bank_ip <- paste0("CCS_ind_sample_PJ_bank_ip",ending)
          dat_a2 <- prepare_data("CCS_ind_sample_PJ_stock2.dta",flood_a,mun_fe,mun_control)
        }
        # Variables: id, muni_cd, tipo, bank_type, stock, week
        dat_a_collapsed <- dat_a %>% group_by(across(-c(stock, bank_type))) %>% summarise(stock = sum(stock)) %>% ungroup()
        dat_bb_collapsed <- dat_bb %>% group_by(across(-c(stock, bank_type))) %>% summarise(stock = sum(stock)) %>% ungroup()

        digi_a <- dat_a %>% filter(bank_type == 2)
        trad_a <- dat_a %>% filter(bank_type == 1)
        
        bank_a <- dat_a2 %>% filter(Bank == 1)
        ip_a <- dat_a2 %>% filter(Bank == 0)
        for(z in 1:length(variables)){
          twfe_ind(beginning,variables[[z]],"constant","constant","flood_risk5", list(dat_bb_collapsed, dat_a_collapsed), c("before", "after"))
          print_twfe_week(beginning, variables[[z]], variables_labels[[z]], c("before", "after"), legend_name, xll, xuu)
          
          # After, bank types. 
          twfe2(beginning_trad_digi,variables[[z]],"constant","constant","flood_risk5", list(trad_a, digi_a), c("trad_after","digi_after"))
          print_twfe_week(beginning_trad_digi, variables[[z]], variables_labels[[z]], c("trad_after","digi_after"), c("Traditional","Digital"), xll, xuu)
          
          twfe2(beginning_bank_ip,variables[[z]],"constant","constant","flood_risk5", list(bank_a, ip_a), c("bank_after","ip_after"))
          print_twfe_week(beginning_bank_ip, variables[[z]], variables_labels[[z]], c("bank_after","ip_after"), c("Banks","Shadow Banks"), xll, xuu)
        }
        rm(dat_a)
        rm(dat_bb)
        rm(dat_a_collapsed)
        rm(dat_bb_collapsed)
        rm(digi_a, trad_a)
      }
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in CCS_ind_sample:", e))
  })
}



# ------------------------------------------------------------------------------
# Credito_ind_sample - problem on trad digi #For some reason, there is only bank_type == 1, there is no other bank_type.
# ------------------------------------------------------------------------------
# Before Variables: time_id, id, tipo, bank, new_users, new_users_if, new_users_cg, valor, valor_ativo, qtd

#   "Credito_ind_sample_PF.dta", "Credito_ind_sample_PJ.dta"))
# Variables: id, muni_cd, tipo, bank_type, new_users, new_users_if, new_users_cg, valor, valor_ativo, qtd
#             lnew_users, lnew_users_if, lnew_users_cg, lvalor, lvalor_ativo, lqtd

#   "Credito_ind_sample_PF2.dta", "Credito_ind_sample_PJ2.dta"))
# Variables: id, muni_cd, tipo, Bank, new_users, new_users_if, new_users_cg, valor, valor_ativo, qtd
#             lnew_users, lnew_users_if, lnew_users_cg, lvalor, lvalor_ativo, lqtd

variables <- c("lvalor", "lvalor_ativo", "lqtd", "lnew_users", "lnew_users_if", "lnew_users_cg")
variables_labels <- c("Log Value", "Log Active Value", "Log Quantity", "Log New Users", "Log New Users IF", "Log New Users CG")
if(run_Credito_ind_sample == 1){
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood_a <- flood_month_after
        flood_b <- flood_month_before2019
        xll <- xl_month
        xuu <- xu_month
        ending <- "_"
        legend_name <- c("2019.01 - 2020.10", "2020.11 - 2022.12")
      }
      if(i==2){
        flood_a <- flood_month_after_balanced
        flood_b <- flood_month_before_balanced2019
        xll <- xl_balanced_month
        xuu <- xu_balanced_month
        ending <- "_balanced_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==3){
        flood_a <- flood_month_after_balanced_covid
        flood_b <- flood_month_before_balanced_covid
        xll <- xl_balanced_covid_month
        xuu <- xu_balanced_covid_month
        ending <- "_balanced_covid_"
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
      }
      
      for(j in 1:2){
        if(j==1){
          beginning <- paste0("Credito_ind_sample_PF",ending)
          beginning_trad_digi <- paste0("Credito_ind_sample_PF_trad_digi",ending)
          dat_a <- prepare_data("Credito_ind_sample_PF.dta",flood_a,mun_fe,mun_control)
          dat_bb <- prepare_data("Credito_ind_sample_PF.dta",flood_b,mun_fe,mun_control)
          
          beginning_bank_ip <- paste0("Credito_ind_sample_PF_bank_ip",ending)
          dat_a2 <- prepare_data("Credito_ind_sample_PF2.dta",flood_a,mun_fe,mun_control)
          #dat_bb2 <- prepare_data("Credito_ind_sample_PF2.dta",flood_b,mun_fe,mun_control)
        }
        if(j==2){
          beginning <- paste0("Credito_ind_sample_PJ",ending)
          beginning_trad_digi <- paste0("Credito_ind_sample_PJ_trad_digi",ending)
          dat_a <- prepare_data("Credito_ind_sample_PJ.dta",flood_a,mun_fe,mun_control)
          dat_bb <- prepare_data("Credito_ind_sample_PJ.dta",flood_b,mun_fe,mun_control)
          
          beginning_bank_ip <- paste0("Credito_ind_sample_PJ_bank_ip",ending)
          dat_a2 <- prepare_data("Credito_ind_sample_PJ2.dta",flood_a,mun_fe,mun_control)
          #dat_bb2 <- prepare_data("Credito_ind_sample_PJ2.dta",flood_b,mun_fe,mun_control)
        }
        dat_a_collapsed <- dat_a %>% group_by(across(-c(bank_type, new_users, new_users_if, new_users_cg, valor, valor_ativo, qtd))) %>% 
          summarise(new_users = sum(new_users), new_users_if = sum(new_users_if), new_users_cg = sum(new_users_cg), valor = sum(valor), valor_ativo = sum(valor_ativo), qtd = sum(qtd)) %>%
          mutate(lnew_users = log(new_users), lnew_users_if = log(new_users_if), lnew_users_cg = log(new_users_cg), lvalor = log(valor), lvalor_ativo = log(valor_ativo), lqtd = log(qtd))
        dat_bb_collapsed <- dat_bb %>% group_by(across(-c(bank_type, new_users, new_users_if, new_users_cg, valor, valor_ativo, qtd))) %>% 
          summarise(new_users = sum(new_users), new_users_if = sum(new_users_if), new_users_cg = sum(new_users_cg), valor = sum(valor), valor_ativo = sum(valor_ativo), qtd = sum(qtd)) %>%
          mutate(lnew_users = log(new_users), lnew_users_if = log(new_users_if), lnew_users_cg = log(new_users_cg), lvalor = log(valor), lvalor_ativo = log(valor_ativo), lqtd = log(qtd))

        digi_a <- dat_a %>% filter(bank_type == 2)
        trad_a <- dat_a %>% filter(bank_type == 1)
        
        bank_a <- dat_a2 %>% filter(Bank == 1)
        ip_a <- dat_a2 %>% filter(Bank == 0)
        for(z in 1:length(variables)){
          tryCatch({
          twfe_ind(beginning,variables[[z]],"constant","constant","flood_risk5", list(dat_bb_collapsed, dat_a_collapsed), c("before", "after"))
          print_twfe_month(beginning, variables[[z]], variables_labels[[z]], c("before", "after"), legend_name, xll, xuu)
          
          # After, bank types. 
          twfe_ind(beginning_trad_digi,variables[[z]],"constant","constant","flood_risk5", list(trad_a, digi_a), c("trad_after","digi_after"))
          print_twfe_month(beginning_trad_digi, variables[[z]], variables_labels[[z]], c("trad_after","digi_after"), c("Traditional","Digital"), xll, xuu)
          
          twfe_ind(beginning_bank_ip,variables[[z]],"constant","constant","flood_risk5", list(bank_a, ip_a), c("bank_after","ip_after"))
          print_twfe_month(beginning_bank_ip, variables[[z]], variables_labels[[z]], c("bank_after","ip_after"), c("Banks","Shadow Banks"), xll, xuu)
          }, error = function(e) {
            print(paste("Error in Credito_ind_sample for variable:", variables[[z]]))
            print(paste("Error message:", e$message))
          })        
        }
        rm(dat_a)
        rm(dat_bb)
        rm(dat_a_collapsed)
        rm(dat_bb_collapsed)
        rm(digi_a, trad_a)
        rm(bank_a, ip_a)
        rm(dat_a2)
      }
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in Credito_ind_sample:", e))
  })
}





# ------------------------------------------------------------------------------
# TED_ind_sample = TED_ind_sample_STR + TED_ind_sample_SITRAF
# ------------------------------------------------------------------------------
# Before Variables: week, id, tipo, value_send, trans_send, value_rec, trans_rec, value_self, trans_self


# TED_ind_sample.dta
# Variables: week, id, muni_cd, tipo, value_send, trans_send, value_rec, trans_rec, value_self, trans_self
#             lvalue_send, ltrans_send, lvalue_rec, ltrans_rec, lvalue_self, ltrans_self, lvalue, ltrans
# "TED_ind_sample_SITRAF_PF", "TED_ind_sample_SITRAF_PJ"


variables <- c("lvalue", "ltrans", "lvalue_self","ltrans_self")
variables_labels <- c("Log Value", "Log Transactions", "Log Value", "Log Transactions")
variables_adoption <- c("adoption", "adoption_send", "adoption_rec","adoption_self", "ladoption", "ladopt_send", "ladopt_rec","ladopt_self")
variables_adoption_labels <- c("Adoption", "Adoption Send", "Adoption Receive", "Self Adoption", "Log Adoption", "Log Adoption Send", "Log Adoption Receive", "Log Self Adoption")

if(run_TED_SITRAF_ind_sample == 1){
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood_a <- flood_week_after
        flood_b <- flood_week_before2019
        xll <- xl
        xuu <- xu
        ending <- "_"
        legend_name <- c("2019.01 - 2020.10", "2020.11 - 2022.12")
      }
      if(i==2){
        flood_a <- flood_week_after_balanced
        flood_b <- flood_week_before_balanced2019
        xll <- xl_balanced
        xuu <- xu_balanced
        ending <- "_balanced_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==3){
        flood_a <- flood_week_after_balanced_covid
        flood_b <- flood_week_before_balanced_covid
        xll <- xl_balanced_covid
        xuu <- xu_balanced_covid
        ending <- "_balanced_covid_"
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
      }
      for(j in 1:2){
        if(j==1){
          beginning <- paste0("TED_SITRAF_ind_sample_PF",ending)
          dat_a <- prepare_data("TED_ind_sample_SITRAF_PF.dta",flood_a,mun_fe,mun_control)
          dat_bb <- prepare_data("TED_ind_sample_SITRAF_PF.dta",flood_b,mun_fe,mun_control)
        }
        if(j==2){
          beginning <- paste0("TED_SITRAF_ind_sample_PJ",ending)
          dat_a <- prepare_data("TED_ind_sample_SITRAF_PJ.dta",flood_a,mun_fe,mun_control)
          dat_bb <- prepare_data("TED_ind_sample_SITRAF_PJ.dta",flood_b,mun_fe,mun_control)
        }
        
        for(z in 1:length(variables)){
          twfe_ind(beginning,variables[[z]],"constant","constant","flood_risk5", list(dat_bb, dat_a), c("before", "after"))
          print_twfe_week(beginning, variables[[z]], variables_labels[[z]], c("before", "after"), legend_name, xll, xuu)
        }
        rm(dat_a)
        rm(dat_bb)
      }
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in TED_SITRAF_ind_sample:", e))
  })
  
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood_a <- flood_week_after
        flood_b <- flood_week_before2019
        xll <- xl
        xuu <- xu
        ending <- "_"
        legend_name <- c("2019.01 - 2020.10", "2020.11 - 2022.12")
      }
      if(i==2){
        flood_a <- flood_week_after_balanced
        flood_b <- flood_week_before_balanced2019
        xll <- xl_balanced
        xuu <- xu_balanced
        ending <- "_balanced_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==3){
        flood_a <- flood_week_after_balanced_covid
        flood_b <- flood_week_before_balanced_covid
        xll <- xl_balanced_covid
        xuu <- xu_balanced_covid
        ending <- "_balanced_covid_"
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
      }
      for(j in 1:2){
        if(j==1){
          beginning_adoption <- paste0("TED_SITRAF_ind_sample_PF_adoption",ending)
          dat_adoption_a <- prepare_data("TED_ind_sample_SITRAF_PF_adoption.dta",flood_a,mun_fe,mun_control)
          dat_adoption_bb <- prepare_data("TED_ind_sample_SITRAF_PF_adoption.dta",flood_b,mun_fe,mun_control)
        }
        if(j==2){
          beginning_adoption <- paste0("TED_SITRAF_ind_sample_PJ_adoption",ending)
          dat_adoption_a <- prepare_data("TED_ind_sample_SITRAF_PJ_adoption.dta",flood_a,mun_fe,mun_control)
          dat_adoption_bb <- prepare_data("TED_ind_sample_SITRAF_PJ_adoption.dta",flood_b,mun_fe,mun_control)
        }
        
        for(z in 1:length(variables_adoption)){
          twfe2(beginning_adoption,variables_adoption[[z]],"constant","constant","flood_risk5", list(dat_adoption_bb, dat_adoption_a), c("before", "after"))
          print_twfe_week(beginning_adoption, variables_adoption[[z]], variables_adoption_labels[[z]], c("before", "after"), legend_name, xll, xuu)
        }
        rm(dat_adoption_a)
        rm(dat_adoption_bb)
      }
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in TED_SITRAF_ind_sample adoption:", e))
  })
  
  
}

if(run_TED_STR_ind_sample == 1){
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood_a <- flood_week_after
        flood_b <- flood_week_before2019
        xll <- xl
        xuu <- xu
        ending <- "_"
        legend_name <- c("2019.01 - 2020.10", "2020.11 - 2022.12")
      }
      if(i==2){
        flood_a <- flood_week_after_balanced
        flood_b <- flood_week_before_balanced2019
        xll <- xl_balanced
        xuu <- xu_balanced
        ending <- "_balanced_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==3){
        flood_a <- flood_week_after_balanced_covid
        flood_b <- flood_week_before_balanced_covid
        xll <- xl_balanced_covid
        xuu <- xu_balanced_covid
        ending <- "_balanced_covid_"
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
      }
      for(j in 1:2){
        if(j==1){
          beginning <- paste0("TED_STR_ind_sample_PF",ending)
          dat_a <- prepare_data("TED_ind_sample_STR_PF.dta",flood_a,mun_fe,mun_control)
          dat_bb <- prepare_data("TED_ind_sample_STR_PF.dta",flood_b,mun_fe,mun_control)
        }
        if(j==2){
          beginning <- paste0("TED_STR_ind_sample_PJ",ending)
          dat_a <- prepare_data("TED_ind_sample_STR_PJ.dta",flood_a,mun_fe,mun_control)
          dat_bb <- prepare_data("TED_ind_sample_STR_PJ.dta",flood_b,mun_fe,mun_control)
        }
        
        for(z in 1:length(variables)){
          twfe_ind(beginning,variables[[z]],"constant","constant","flood_risk5", list(dat_bb, dat_a), c("before", "after"))
          print_twfe_week(beginning, variables[[z]], variables_labels[[z]], c("before", "after"), legend_name, xll, xuu)
        }
        rm(dat_a)
        rm(dat_bb)
      }
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in TED_STR_ind_sample:", e))
  })
  
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood_a <- flood_week_after
        flood_b <- flood_week_before2019
        xll <- xl
        xuu <- xu
        ending <- "_"
        legend_name <- c("2019.01 - 2020.10", "2020.11 - 2022.12")
      }
      if(i==2){
        flood_a <- flood_week_after_balanced
        flood_b <- flood_week_before_balanced2019
        xll <- xl_balanced
        xuu <- xu_balanced
        ending <- "_balanced_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==3){
        flood_a <- flood_week_after_balanced_covid
        flood_b <- flood_week_before_balanced_covid
        xll <- xl_balanced_covid
        xuu <- xu_balanced_covid
        ending <- "_balanced_covid_"
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
      }
      for(j in 1:2){
        if(j==1){
          beginning_adoption <- paste0("TED_STR_ind_sample_PF_adoption",ending)
          dat_adoption_a <- prepare_data("TED_ind_sample_STR_PF_adoption.dta",flood_a,mun_fe,mun_control)
          dat_adoption_bb <- prepare_data("TED_ind_sample_STR_PF_adoption.dta",flood_b,mun_fe,mun_control)
        }
        if(j==2){
          beginning_adoption <- paste0("TED_STR_ind_sample_PJ_adoption",ending)
          dat_adoption_a <- prepare_data("TED_ind_sample_STR_PJ_adoption.dta",flood_a,mun_fe,mun_control)
          dat_adoption_bb <- prepare_data("TED_ind_sample_STR_PJ_adoption.dta",flood_b,mun_fe,mun_control)
        }
        
        for(z in 1:length(variables_adoption)){
          twfe2(beginning_adoption,variables_adoption[[z]],"constant","constant","flood_risk5", list(dat_adoption_bb, dat_adoption_a), c("before", "after"))
          print_twfe_week(beginning_adoption, variables_adoption[[z]], variables_adoption_labels[[z]], c("before", "after"), legend_name, xll, xuu)
        }
        rm(dat_adoption_a)
        rm(dat_adoption_bb)
      }
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in TED_STR_ind_sample adoption:", e))
  })
}

if(run_TED_ind_sample == 1){
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood_a <- flood_week_after
        flood_b <- flood_week_before2019
        xll <- xl
        xuu <- xu
        ending <- "_"
        legend_name <- c("2019.01 - 2020.10", "2020.11 - 2022.12")
      }
      if(i==2){
        flood_a <- flood_week_after_balanced
        flood_b <- flood_week_before_balanced2019
        xll <- xl_balanced
        xuu <- xu_balanced
        ending <- "_balanced_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==3){
        flood_a <- flood_week_after_balanced_covid
        flood_b <- flood_week_before_balanced_covid
        xll <- xl_balanced_covid
        xuu <- xu_balanced_covid
        ending <- "_balanced_covid_"
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
      }
      for(j in 1:2){
        if(j==1){
          beginning <- paste0("TED_ind_sample_PF",ending)
          dat_a <- prepare_data("TED_ind_sample_PF.dta",flood_a,mun_fe,mun_control)
          dat_bb <- prepare_data("TED_ind_sample_PF.dta",flood_b,mun_fe,mun_control)
        }
        if(j==2){
          beginning <- paste0("TED_ind_sample_PJ",ending)
          dat_a <- prepare_data("TED_ind_sample_PJ.dta",flood_a,mun_fe,mun_control)
          dat_bb <- prepare_data("TED_ind_sample_PJ.dta",flood_b,mun_fe,mun_control)
        }
        
        for(z in 1:length(variables)){
          twfe_ind(beginning,variables[[z]],"constant","constant","flood_risk5", list(dat_bb, dat_a), c("before", "after"))
          print_twfe_week(beginning, variables[[z]], variables_labels[[z]], c("before", "after"), legend_name, xll, xuu)
        }
        rm(dat_a)
        rm(dat_bb)
      }
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in TED_ind_sample:", e))
  })
  
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood_a <- flood_week_after
        flood_b <- flood_week_before2019
        xll <- xl
        xuu <- xu
        ending <- "_"
        legend_name <- c("2019.01 - 2020.10", "2020.11 - 2022.12")
      }
      if(i==2){
        flood_a <- flood_week_after_balanced
        flood_b <- flood_week_before_balanced2019
        xll <- xl_balanced
        xuu <- xu_balanced
        ending <- "_balanced_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==3){
        flood_a <- flood_week_after_balanced_covid
        flood_b <- flood_week_before_balanced_covid
        xll <- xl_balanced_covid
        xuu <- xu_balanced_covid
        ending <- "_balanced_covid_"
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
      }
      for(j in 1:2){
        if(j==1){
          beginning_adoption <- paste0("TED_ind_sample_PF_adoption",ending)
          dat_adoption_a <- prepare_data("TED_ind_sample_PF_adoption.dta",flood_a,mun_fe,mun_control)
          dat_adoption_bb <- prepare_data("TED_ind_sample_PF_adoption.dta",flood_b,mun_fe,mun_control)
        }
        if(j==2){
          beginning_adoption <- paste0("TED_ind_sample_PJ_adoption",ending)
          dat_adoption_a <- prepare_data("TED_ind_sample_PJ_adoption.dta",flood_a,mun_fe,mun_control)
          dat_adoption_bb <- prepare_data("TED_ind_sample_PJ_adoption.dta",flood_b,mun_fe,mun_control)
        }
        
        for(z in 1:length(variables_adoption)){
          twfe2(beginning_adoption,variables_adoption[[z]],"constant","constant","flood_risk5", list(dat_adoption_bb, dat_adoption_a), c("before", "after"))
          print_twfe_week(beginning_adoption, variables_adoption[[z]], variables_adoption_labels[[z]], c("before", "after"), legend_name, xll, xuu)
        }
        rm(dat_adoption_a)
        rm(dat_adoption_bb)
      }
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in TED_ind_sample adoption:", e))
  })
}


# ------------------------------------------------------------------------------
# Card_rec - worked
# ------------------------------------------------------------------------------

# Card_rec.dta
# Variables: week, muni_cd, tipo, receivers, valor, receivers_credit, valor_credit, receivers_debit, valor_debit
#            lreceivers, lvalor, lreceivers_credit, lvalor_credit, lreceivers_debit, lvalor_debit

variables <- c("lvalor", "lreceivers", "lvalor_credit", "lreceivers_credit", "lvalor_debit", "lreceivers_debit")
variables_labels_PF <- c("Log Card Payments", "Log People Accepting Card Payments", "Log Credit Payments", "Log People Accepting Credit Payments", "Log Debit Payments", "Log People Accepting Debit Payments")
variables_labels_PJ <- c("Log Card Payments", "Log Firms Accepting Card Payments", "Log Credit Payments", "Log Firms Accepting Credit Payments", "Log Debit Payments", "Log Firms Accepting Debit Payments")
if(run_Card_rec == 1){
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood_a <- flood_week_after
        flood_b <- flood_week_before2019
        xll <- xl
        xuu <- xu
        ending <- "_"
        legend_name <- c("2019.01 - 2020.10", "2020.11 - 2022.12")
        beginning_PF <- paste0("Card_rec_PF", ending)
        beginning_PJ <- paste0("Card_rec_PJ", ending)
      }
      if(i==2){
        flood_a <- flood_week_after_balanced
        flood_b <- flood_week_before_balanced2019
        xll <- xl_balanced
        xuu <- xu_balanced
        ending <- "_balanced_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
        beginning_PF <- paste0("Card_rec_PF", ending)
        beginning_PJ <- paste0("Card_rec_PJ", ending)
      }
      if(i==3){
        flood_a <- flood_week_after_balanced_covid
        flood_b <- flood_week_before_balanced_covid
        xll <- xl_balanced_covid
        xuu <- xu_balanced_covid
        ending <- "_balanced_covid_"
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
        beginning_PF <- paste0("Card_rec_PF", ending)
        beginning_PJ <- paste0("Card_rec_PJ", ending)
      }
      dat_after <- prepare_data("Card_rec.dta",flood_a,mun_fe,mun_control)
      dat_before <- prepare_data("Card_rec.dta",flood_b,mun_fe,mun_control)
      dat_after_PF <- dat_after %>% filter(tipo==1)
      dat_before_PF <- dat_before %>% filter(tipo==1)
      dat_after_PJ <- dat_after %>% filter(tipo==2)
      dat_before_PJ <- dat_before %>% filter(tipo==2)
      
      for(z in 1:length(variables)){
        twfe2(beginning_PF,variables[[z]],"constant","constant","flood_risk5", list(dat_before_PF, dat_after_PF), c("before","after"))
        print_twfe_week(beginning_PF, variables[[z]], variables_labels_PF[[z]], c("before","after"), legend_name, xll, xuu)
        
        twfe2(beginning_PJ,variables[[z]],"constant","constant","flood_risk5", list(dat_before_PJ, dat_after_PJ), c("before","after"))
        print_twfe_week(beginning_PJ, variables[[z]], variables_labels_PJ[[z]], c("before","after"), legend_name, xll, xuu)
      }
      rm(dat_after,dat_before,dat_after_PF,dat_before_PF,dat_after_PJ,dat_before_PJ)
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in Card:", e))
  })
}


# ------------------------------------------------------------------------------
# Boletos - worked
# ------------------------------------------------------------------------------

# Boleto.dta
# Variables: week, muni_cd, tipo, senders, trans_send, valor_send, receivers, trans_rec, valor_rec
#             lsenders, ltrans_send, lvalor_send, lreceivers, ltrans_rec, lvalor_rec


# Do PF + PJ?
variables <- c("lvalor","ltrans","lusers", "lvalor_send","ltrans_send","lsenders", "lvalor_rec","ltrans_rec","lreceivers")
variables_labels_PF <- c("Log Value Payment Slips", "Log Transactions Payment Slips", "Log People using Payment Slips", "Log Value Payment Slips", "Log Transactions Payment Slips", "Log People using Payment Slips", "Log Value Payment Slips", "Log Transactions Payment Slips", "Log People using Payment Slips")
variables_labels_PJ <- c("Log Value Payment Slips", "Log Transactions Payment Slips", "Log Firms using Payment Slips", "Log Value Payment Slips", "Log Transactions Payment Slips", "Log Firms using Payment Slips", "Log Value Payment Slips", "Log Transactions Payment Slips", "Log Firms using Payment Slips")
if(run_Boletos == 1){
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood_a <- flood_week_after
        flood_b <- flood_week_before2019
        xll <- xl
        xuu <- xu
        ending <- "_"
        legend_name <- c("2019.01 - 2020.10", "2020.11 - 2022.12")
        beginning_PF <- paste0("Boletos_PF", ending)
        beginning_PJ <- paste0("Boletos_PJ", ending)
      }
      if(i==2){
        flood_a <- flood_week_after_balanced
        flood_b <- flood_week_before_balanced2019
        xll <- xl_balanced
        xuu <- xu_balanced
        ending <- "_balanced_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
        beginning_PF <- paste0("Boletos_PF", ending)
        beginning_PJ <- paste0("Boletos_PJ", ending)
      }
      if(i==3){
        flood_a <- flood_week_after_balanced_covid
        flood_b <- flood_week_before_balanced_covid
        xll <- xl_balanced_covid
        xuu <- xu_balanced_covid
        ending <- "_balanced_covid_"
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
        beginning_PF <- paste0("Boletos_PF", ending)
        beginning_PJ <- paste0("Boletos_PJ", ending)
      }
      dat_after <- prepare_data("Boleto.dta",flood_a,mun_fe,mun_control)
      dat_before <- prepare_data("Boleto.dta",flood_b,mun_fe,mun_control)
      dat_after_PF <- dat_after %>% filter(tipo==1) %>%
        mutate(valor = valor_send + valor_rec,
               trans = trans_send + trans_rec,
               users = senders + receivers) %>%
        mutate(lvalor = log1p(valor),
               ltrans = log1p(trans),
               lusers = log1p(users))
      dat_before_PF <- dat_before %>% filter(tipo==1) %>%
        mutate(valor = valor_send + valor_rec,
               trans = trans_send + trans_rec,
               users = senders + receivers) %>%
        mutate(lvalor = log1p(valor),
               ltrans = log1p(trans),
               lusers = log1p(users))
      dat_after_PJ <- dat_after %>% filter(tipo==2) %>%
        mutate(valor = valor_send + valor_rec,
               trans = trans_send + trans_rec,
               users = senders + receivers) %>%
        mutate(lvalor = log1p(valor),
               ltrans = log1p(trans),
               lusers = log1p(users))
      dat_before_PJ <- dat_before %>% filter(tipo==2) %>%
        mutate(valor = valor_send + valor_rec,
               trans = trans_send + trans_rec,
               users = senders + receivers) %>%
        mutate(lvalor = log1p(valor),
               ltrans = log1p(trans),
               lusers = log1p(users))
      
      for(z in 1:length(variables)){
        twfe2(beginning_PF,variables[[z]],"constant","constant","flood_risk5", list(dat_before_PF, dat_after_PF), c("before","after"))
        print_twfe_week(beginning_PF, variables[[z]], variables_labels_PF[[z]], c("before","after"), legend_name, xll, xuu)
        
        twfe2(beginning_PJ,variables[[z]],"constant","constant","flood_risk5", list(dat_before_PJ, dat_after_PJ), c("before","after"))
        print_twfe_week(beginning_PJ, variables[[z]], variables_labels_PJ[[z]], c("before","after"), legend_name, xll, xuu)
      }
      rm(dat_after,dat_before,dat_after_PF,dat_before_PF,dat_after_PJ,dat_before_PJ)
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in Boletos:", e))
  })
}


# ------------------------------------------------------------------------------
# CCS_Muni_stock - Worked! --- Change lbanked_pop to banked_pop/pop  ------------------------------------
# ------------------------------------------------------------------------------
# It was not downloaded yet.
# We can improve the way we deal with banked_pop or stock. Maybe divide by population. talk to sean and Jacopo.

#CCS_Muni_stock_v2.dta
# Variables: week, muni_cd, tipo, muni_stock, muni_stock_w, banked_pop
#             lmuni_stock, lmuni_stock_w, lbanked_pop

variables <- c("lmuni_stock_w","lmuni_stock","lbanked_pop", "banked_pop_2")
variables_labels_PF <- c("Log Number of Bank Accounts","Log Number of Bank Accounts","Log Banked Population", "Proportion of Banked Population")
variables_labels_PJ <- c("Log Number of Bank Accounts","Log Number of Bank Accounts","Log Banked Population", "Proportion of Banked Population")
if(run_CCS_Muni_stock == 1){
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood_a <- flood_week_after
        flood_b <- flood_week_before2019
        xll <- xl
        xuu <- xu
        ending <- "_"
        legend_name <- c("2019.01 - 2020.10", "2020.11 - 2022.12")
        beginning_PF <- paste0("CCS_Muni_PF", ending)
        beginning_PJ <- paste0("CCS_Muni_PJ", ending)
      }
      if(i==2){
        flood_a <- flood_week_after_balanced
        flood_b <- flood_week_before_balanced2019
        xll <- xl_balanced
        xuu <- xu_balanced
        ending <- "_balanced_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
        beginning_PF <- paste0("CCS_Muni_PF", ending)
        beginning_PJ <- paste0("CCS_Muni_PJ", ending)
      }
      if(i==3){
        flood_a <- flood_week_after_balanced_covid
        flood_b <- flood_week_before_balanced_covid
        xll <- xl_balanced_covid
        xuu <- xu_balanced_covid
        ending <- "_balanced_covid_"
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
        beginning_PF <- paste0("CCS_Muni_PF", ending)
        beginning_PJ <- paste0("CCS_Muni_PJ", ending)
      }
      dat_after <- prepare_data("CCS_Muni_stock_v2.dta",flood_a,mun_fe,mun_control)
      dat_before <- prepare_data("CCS_Muni_stock_v2.dta",flood_b,mun_fe,mun_control)
      
      
      dat_after <- dat_after %>%
        mutate(banked_pop_2 = ifelse(is.na(pop2022) | pop2022 == 0 | is.na(banked_pop), NA, banked_pop/pop2022))
      dat_before <- dat_before %>%
        mutate(banked_pop_2 = ifelse(is.na(pop2022) | pop2022 == 0 | is.na(banked_pop), NA, banked_pop/pop2022))
      
      
      dat_after_PF <- dat_after %>% filter(tipo==1)
      dat_before_PF <- dat_before %>% filter(tipo==1)
      dat_after_PJ <- dat_after %>% filter(tipo==2)
      dat_before_PJ <- dat_before %>% filter(tipo==2)
      
      for(z in 1:length(variables)){
        twfe2(beginning_PF,variables[[z]],"constant","constant","flood_risk5", list(dat_before_PF, dat_after_PF), c("before","after"))
        print_twfe_week(beginning_PF, variables[[z]], variables_labels_PF[[z]], c("before","after"), legend_name, xll, xuu)
        
        twfe2(beginning_PJ,variables[[z]],"constant","constant","flood_risk5", list(dat_before_PJ, dat_after_PJ), c("before","after"))
        print_twfe_week(beginning_PJ, variables[[z]], variables_labels_PJ[[z]], c("before","after"), legend_name, xll, xuu)
      }
      rm(dat_after,dat_before,dat_after_PF,dat_before_PF,dat_after_PJ,dat_before_PJ)
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in CCS_Muni_stock:", e))
  })
}

# ------------------------------------------------------------------------------
# CCS_Muni_IF - Worked! ---- Merge bank variable with cadastro IF and make new bank_type variable- why these results are so bad??? growing SDerrors.

# make sure we count a conglomerate only once. group by cong and max accounts. cong_id
# ------------------------------------------------------------------------------

# CCS_Muni_IF_PF_v2.dta and CCS_Muni_IF_PJ_v2.dta
# Variables: week, muni_cd, tipo, bank, tipo_inst, bank_type,
#             stock,
#             lstock

#filename <- c("CCS_Muni_IF_PF", "CCS_Muni_IF_PJ")
# Variables: week, muni_cd, tipo, bank, opening, stock, closing
#             lopening, lstock, lclosing

# Graphs: lstock after Pix, for digital banks and traditional banks. 
variables <- c("lstock")
variables_labels <- c("Log Number of Bank Accounts")
if(run_CCS_Muni_IF == 1){
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood_a <- flood_week_after
        flood_b <- flood_week_before2019
        xll <- xl
        xuu <- xu
        ending <- "_"
        legend_name <- c("Traditional","Digital")
        beginning_PF <- paste0("CCS_Muni_IF_PF", ending)
        beginning_PJ <- paste0("CCS_Muni_IF_PJ", ending)        
        legend_name2 <- c("Banks","NBFIs")
        beginning_PF2 <- paste0("CCS_Muni_IF_PF2", ending)
        beginning_PJ2 <- paste0("CCS_Muni_IF_PJ2", ending)        
        legend_name3 <- c("2019.01 - 2020.10", "2020.11 - 2022.12")
        beginning_PF3 <- paste0("CCS_Muni_IF_PF3", ending)
        beginning_PJ3 <- paste0("CCS_Muni_IF_PJ3", ending)
        beginning_3 <- paste0("CCS_Muni_IF_3", ending)
        beginning_PF4 <- paste0("CCS_Muni_IF_PF4", ending)
        beginning_PJ4 <- paste0("CCS_Muni_IF_PJ4", ending)
        beginning_4 <- paste0("CCS_Muni_IF_4", ending)
      }
      if(i==2){
        flood_a <- flood_week_after_balanced
        flood_b <- flood_week_before_balanced2019
        xll <- xl_balanced
        xuu <- xu_balanced
        ending <- "_balanced_"
        legend_name <- c("Traditional","Digital")
        beginning_PF <- paste0("CCS_Muni_IF_PF", ending)
        beginning_PJ <- paste0("CCS_Muni_IF_PJ", ending)
        legend_name2 <- c("Banks","NBFIs")
        beginning_PF2 <- paste0("CCS_Muni_IF_PF2", ending)
        beginning_PJ2 <- paste0("CCS_Muni_IF_PJ2", ending)        
        legend_name3 <- c("2019.01 - 2020.10", "2020.11 - 2022.12")
        beginning_PF3 <- paste0("CCS_Muni_IF_PF3", ending)
        beginning_PJ3 <- paste0("CCS_Muni_IF_PJ3", ending)
        beginning_3 <- paste0("CCS_Muni_IF_3", ending)
        beginning_PF4 <- paste0("CCS_Muni_IF_PF4", ending)
        beginning_PJ4 <- paste0("CCS_Muni_IF_PJ4", ending)
        beginning_4 <- paste0("CCS_Muni_IF_4", ending)
      }
      if(i==3){
        flood_a <- flood_week_after_balanced_covid
        flood_b <- flood_week_before_balanced_covid
        xll <- xl_balanced_covid
        xuu <- xu_balanced_covid
        ending <- "_balanced_covid_"
        legend_name <- c("Traditional","Digital")
        beginning_PF <- paste0("CCS_Muni_IF_PF", ending)
        beginning_PJ <- paste0("CCS_Muni_IF_PJ", ending)
        legend_name2 <- c("Banks","NBFIs")
        beginning_PF2 <- paste0("CCS_Muni_IF_PF2", ending)
        beginning_PJ2 <- paste0("CCS_Muni_IF_PJ2", ending)
        legend_name3 <- c("2020.03 - 2020.10","2020.11 - 2021.06")
        beginning_PF3 <- paste0("CCS_Muni_IF_PF3", ending)
        beginning_PJ3 <- paste0("CCS_Muni_IF_PJ3", ending)
        beginning_3 <- paste0("CCS_Muni_IF_3", ending)
        beginning_PF4 <- paste0("CCS_Muni_IF_PF4", ending)
        beginning_PJ4 <- paste0("CCS_Muni_IF_PJ4", ending)
        beginning_4 <- paste0("CCS_Muni_IF_4", ending)
      }
      
      dat_after_PF <- prepare_data("CCS_Muni_IF_PF_v2.dta",flood_a,mun_fe,mun_control)
      
      #-------------------------------------------------------------------------
      dat_after_PF2 <- dat_after_PF %>% select(-tipo_inst, -bank_type)
      dat_after_PF2 <- merge(dat_after_PF2, Cadastro_IF, by = "bank", all.x = TRUE)
      dat_after_PF2 <- dat_after_PF2 %>%
        mutate(Bank = ifelse(macroseg_if_txt == "b1", 1, 0),
               IP = ifelse(macroseg_if_txt == "n4", 1, 0))

      dat_after_PF2 <- dat_after_PF2 %>% 
        group_by_at(vars(-c(bank, tipo_inst, bank_type, macroseg_if_txt, stock, lstock, IP, Bank))) %>% # Group by cong_id, week, muni_cd, tipo
        summarise(stock = max(stock, na.rm = TRUE),
                  IP = max(IP, na.rm = TRUE),
                  Bank = max(Bank, na.rm = TRUE), .groups = "drop") %>% # taking the max so I do not double count. 
        mutate(lstock = log1p(stock))
      
      # Banks vs IPs
      dat_after_PF2 <- dat_after_PF2 %>% 
        filter((Bank == 1) | (Bank == 0 & IP == 1)) %>%
        mutate(IP = ifelse(Bank == 1, 0, 1)) # this would make sure a conglomerate that is a bank and has an IP is counted just as bank. 
      # Need to group by Bank and IP --------------------------------------------------------------------------------------------------------------------------------------------
      dat_after_PF2 <- dat_after_PF2 %>% 
        group_by_at(vars(-c(cong_id, stock, lstock))) %>% # Group by week, muni_cd, tipo, Bank, IP
        summarise(stock = sum(stock, na.rm = TRUE), .groups = "drop") %>%
        mutate(lstock = log1p(stock))
      
      ip_after_PF <- dat_after_PF2 %>%
        filter(IP == 1)
      bank_after_PF <- dat_after_PF2 %>%
        filter(Bank == 1)
      
      # stock of the entire municipality - Note that I am counting only Bank + IP (if you want to know all types -> CCS_Muni_PF_balanced_lmuni_stock)
      dat_after_PF2_total <- dat_after_PF2 %>% 
        group_by_at(vars(-c(IP, Bank, stock, lstock))) %>% # Group by week, muni_cd, tipo
        summarise(stock = sum(stock, na.rm = TRUE), .groups = "drop") %>% # taking the max so I do not double count. 
        mutate(lstock = log1p(stock))
      
      #-------------------------------------------------------------------------      

      dat_after_PF <- dat_after_PF %>%
        select(-tipo_inst, -bank, -lstock) %>%
        group_by_at(vars(-stock)) %>%
        summarise(stock = sum(stock, na.rm = TRUE)) %>%
        mutate(lstock = log1p(stock)) %>%
        ungroup()
      
      dat_after_PF <- dat_after_PF %>% filter(bank_type == 1 | bank_type == 2)
      dat_after_PF_total <- dat_after_PF %>% 
        group_by_at(vars(-c(bank_type, stock, lstock))) %>% # Group by week, muni_cd
        summarise(stock = sum(stock, na.rm = TRUE), .groups = "drop") %>%  
        mutate(lstock = log1p(stock))
      
      digi_after_PF <- dat_after_PF %>%
        filter(bank_type == 2)
      trad_after_PF <- dat_after_PF %>%
        filter(bank_type == 1)
      
      dat_after_PJ <- prepare_data("CCS_Muni_IF_PJ_v2.dta",flood_a,mun_fe,mun_control)
      #-------------------------------------------------------------------------
      dat_after_PJ2 <- dat_after_PJ %>% select(-tipo_inst, -bank_type)
      dat_after_PJ2 <- merge(dat_after_PJ2, Cadastro_IF, by = "bank", all.x = TRUE)
      dat_after_PJ2 <- dat_after_PJ2 %>%
        mutate(Bank = ifelse(macroseg_if_txt == "b1", 1, 0),
               IP = ifelse(macroseg_if_txt == "n4", 1, 0))
      
      dat_after_PJ2 <- dat_after_PJ2 %>%
        group_by_at(vars(-c(bank, tipo_inst, bank_type, macroseg_if_txt, stock, lstock, IP, Bank))) %>% # Group by cong_id, week, muni_cd, tipo
        summarise(stock = max(stock, na.rm = TRUE),
                  IP = max(IP, na.rm = TRUE),
                  Bank = max(Bank, na.rm = TRUE), .groups = "drop") %>% # taking the max so I do not double count. 
        mutate(lstock = log1p(stock))
      
      # Banks vs IPs
      dat_after_PJ2 <- dat_after_PJ2 %>% 
        filter((Bank == 1) | (Bank == 0 & IP == 1)) %>%
        mutate(IP = ifelse(Bank == 1, 0, 1)) # this would make sure a conglomerate that is a bank and has an IP is counted just as bank.
      # Need to group by Bank and IP --------------------------------------------------------------------------------------------------------------------------------------------
      dat_after_PJ2 <- dat_after_PJ2 %>% 
        group_by_at(vars(-c(cong_id, stock, lstock))) %>% # Group by week, muni_cd, tipo, Bank, IP
        summarise(stock = sum(stock, na.rm = TRUE), .groups = "drop") %>%
        mutate(lstock = log1p(stock))
      
      ip_after_PJ <- dat_after_PJ2 %>%
        filter(IP == 1)
      bank_after_PJ <- dat_after_PJ2 %>%
        filter(Bank == 1)
      
      # stock of the entire municipality - Note that I am counting only Bank + IP (if you want to know all types -> CCS_Muni_PJ_balanced_lmuni_stock)
      dat_after_PJ2_total <- dat_after_PJ2 %>% 
        group_by_at(vars(-c(IP, Bank, stock, lstock))) %>% # Group by week, muni_cd, tipo
        summarise(stock = sum(stock, na.rm = TRUE), .groups = "drop") %>% # taking the max so I do not double count. 
        mutate(lstock = log1p(stock))
      
      #-------------------------------------------------------------------------
      dat_after_PJ <- dat_after_PJ %>%
        select(-tipo_inst, -bank, -lstock) %>%
        group_by_at(vars(-stock)) %>%
        summarise(stock = sum(stock, na.rm = TRUE)) %>%
        mutate(lstock = log1p(stock)) %>%
        ungroup()
      
      dat_after_PJ <- dat_after_PJ %>% filter(bank_type == 1 | bank_type == 2)
      dat_after_PJ_total <- dat_after_PJ %>% 
        group_by_at(vars(-c(bank_type, stock, lstock))) %>% # Group by week, muni_cd
        summarise(stock = sum(stock, na.rm = TRUE), .groups = "drop") %>%  
        mutate(lstock = log1p(stock))
      
      digi_after_PJ <- dat_after_PJ %>%
        filter(bank_type == 2)
      trad_after_PJ <- dat_after_PJ %>%
        filter(bank_type == 1)
      rm(dat_after_PJ)      
      rm(dat_after_PF)
      
      #-------------------------------------------------------------------------
      # Total accounts PF+PJ
      # dat_after_PF2_total dat_after_PJ2_total
      dat_after2_total <- rbind(dat_after_PF2_total, dat_after_PJ2_total)
      dat_after2_total <- dat_after2_total %>%
        group_by_at(vars(-c(tipo, stock, lstock))) %>% # Group by week, muni_cd
        summarise(stock = sum(stock, na.rm = TRUE), .groups = "drop") %>%
        mutate(lstock = log1p(stock))
      
      dat_after_total <- rbind(dat_after_PF_total, dat_after_PJ_total)
      dat_after_total <- dat_after_total %>%
        group_by_at(vars(-c(tipo, stock, lstock))) %>% # Group by week, muni_cd
        summarise(stock = sum(stock, na.rm = TRUE), .groups = "drop") %>%  
        mutate(lstock = log1p(stock))
      
      #-------------------------------------------------------------------------
      
      # Get the before
      dat_before_PF <- prepare_data("CCS_Muni_IF_PF_v2.dta",flood_b,mun_fe,mun_control)

      dat_before_PF2 <- dat_before_PF %>% select(-tipo_inst, -bank_type)
      dat_before_PF2 <- merge(dat_before_PF2, Cadastro_IF, by = "bank", all.x = TRUE)
      dat_before_PF2 <- dat_before_PF2 %>%
        mutate(Bank = ifelse(macroseg_if_txt == "b1", 1, 0),
               IP = ifelse(macroseg_if_txt == "n4", 1, 0))
      
      dat_before_PF2 <- dat_before_PF2 %>% 
        group_by_at(vars(-c(bank, tipo_inst, bank_type, macroseg_if_txt, stock, lstock))) %>% # Group by cong_id, week, muni_cd, tipo
        summarise(stock = max(stock, na.rm = TRUE),
                  IP = max(IP, na.rm = TRUE),
                  Bank = max(Bank, na.rm = TRUE), .groups = "drop") %>% # taking the max so I do not double count. 
        mutate(lstock = log1p(stock))
      
      # Banks vs IPs
      dat_before_PF2 <- dat_before_PF2 %>% 
        filter((Bank == 1) | (Bank == 0 & IP == 1)) %>%
        mutate(IP = ifelse(Bank == 1, 0, 1)) # this would make sure a conglomerate that is a bank and has an IP is counted just as bank.
      
      # Need to group by Bank and IP --------------------------------------------------------------------------------------------------------------------------------------------
      dat_before_PF2 <- dat_before_PF2 %>% 
        group_by_at(vars(-c(cong_id, stock, lstock))) %>% # Group by week, muni_cd, tipo, Bank, IP
        summarise(stock = sum(stock, na.rm = TRUE), .groups = "drop") %>%
        mutate(lstock = log1p(stock))
      
      # stock of the entire municipality
      dat_before_PF2_total <- dat_before_PF2 %>% 
        group_by_at(vars(-c(IP, Bank, stock, lstock))) %>% # Group by week, muni_cd, tipo
        summarise(stock = sum(stock, na.rm = TRUE), .groups = "drop") %>% # taking the max so I do not double count. 
        mutate(lstock = log1p(stock))
      
      dat_before_PF <- dat_before_PF %>%
        select(-tipo_inst, -bank, -lstock) %>%
        group_by_at(vars(-stock)) %>%
        summarise(stock = sum(stock, na.rm = TRUE)) %>%
        mutate(lstock = log1p(stock)) %>%
        ungroup()
      
      dat_before_PF <- dat_before_PF %>% filter(bank_type == 1 | bank_type == 2)
      dat_before_PF_total <- dat_before_PF %>% 
        group_by_at(vars(-c(bank_type, stock, lstock))) %>% # Group by week, muni_cd
        summarise(stock = sum(stock, na.rm = TRUE), .groups = "drop") %>%  
        mutate(lstock = log1p(stock))
      
      dat_before_PJ <- prepare_data("CCS_Muni_IF_PJ_v2.dta",flood_b,mun_fe,mun_control)
      
      dat_before_PJ2 <- dat_before_PJ %>% select(-tipo_inst, -bank_type)
      dat_before_PJ2 <- merge(dat_before_PJ2, Cadastro_IF, by = "bank", all.x = TRUE)
      dat_before_PJ2 <- dat_before_PJ2 %>%
        mutate(Bank = ifelse(macroseg_if_txt == "b1", 1, 0),
               IP = ifelse(macroseg_if_txt == "n4", 1, 0))
      
      dat_before_PJ2 <- dat_before_PJ2 %>%
        group_by_at(vars(-c(bank, tipo_inst, bank_type, macroseg_if_txt, stock, lstock))) %>% # Group by cong_id, week, muni_cd, tipo
        summarise(stock = max(stock, na.rm = TRUE),
                  IP = max(IP, na.rm = TRUE),
                  Bank = max(Bank, na.rm = TRUE), .groups = "drop") %>% # taking the max so I do not double count. 
        mutate(lstock = log1p(stock))
      
      # Banks vs IPs
      dat_before_PJ2 <- dat_before_PJ2 %>% 
        filter((Bank == 1) | (Bank == 0 & IP == 1)) %>%
        mutate(IP = ifelse(Bank == 1, 0, 1)) # this would make sure a conglomerate that is a bank and has an IP is counted just as bank.
      
      # Need to group by Bank and IP --------------------------------------------------------------------------------------------------------------------------------------------
      dat_before_PJ2 <- dat_before_PJ2 %>% 
        group_by_at(vars(-c(cong_id, stock, lstock))) %>% # Group by week, muni_cd, tipo, Bank, IP
        summarise(stock = sum(stock, na.rm = TRUE), .groups = "drop") %>%
        mutate(lstock = log1p(stock))
      
      # stock of the entire municipality
      dat_before_PJ2_total <- dat_before_PJ2 %>% 
        group_by_at(vars(-c(IP, Bank, stock, lstock))) %>% # Group by week, muni_cd, tipo
        summarise(stock = sum(stock, na.rm = TRUE), .groups = "drop") %>% # taking the max so I do not double count. 
        mutate(lstock = log1p(stock))
      
      dat_before_PJ <- dat_before_PJ %>%
        select(-tipo_inst, -bank, -lstock) %>%
        group_by_at(vars(-stock)) %>%
        summarise(stock = sum(stock, na.rm = TRUE)) %>%
        mutate(lstock = log1p(stock)) %>%
        ungroup()
      
      dat_before_PJ <- dat_before_PJ %>% filter(bank_type == 1 | bank_type == 2)
      dat_before_PJ_total <- dat_before_PJ %>% 
        group_by_at(vars(-c(bank_type, stock, lstock))) %>% # Group by week, muni_cd
        summarise(stock = sum(stock, na.rm = TRUE), .groups = "drop") %>%  
        mutate(lstock = log1p(stock))
      
      
      #-------------------------------------------------------------------------
      # Total accounts PF+PJ
      # dat_before_PF2_total dat_before_PJ2_total
      dat_before2_total <- rbind(dat_before_PF2_total, dat_before_PJ2_total)
      dat_before2_total <- dat_before2_total %>%
        group_by_at(vars(-c(tipo, stock, lstock))) %>% # Group by week, muni_cd
        summarise(stock = sum(stock, na.rm = TRUE), .groups = "drop") %>% # taking the max so I do not double count. 
        mutate(lstock = log1p(stock))
      
      dat_before_total <- rbind(dat_before_PF_total, dat_before_PJ_total)
      dat_before_total <- dat_before_total %>%
        group_by_at(vars(-c(tipo, stock, lstock))) %>% # Group by week, muni_cd
        summarise(stock = sum(stock, na.rm = TRUE), .groups = "drop") %>%  
        mutate(lstock = log1p(stock))
      
      for(z in 1:length(variables)){
        twfe2(beginning_PF,variables[[z]],"constant","constant","flood_risk5", list(trad_after_PF, digi_after_PF), c("trad_after","digi_after"))
        print_twfe_week(beginning_PF, variables[[z]], variables_labels[[z]], c("trad_after","digi_after"), legend_name, xll, xuu)
        twfe2(beginning_PJ,variables[[z]],"constant","constant","flood_risk5", list(trad_after_PJ, digi_after_PJ), c("trad_after","digi_after"))
        print_twfe_week(beginning_PJ, variables[[z]], variables_labels[[z]], c("trad_after","digi_after"), legend_name, xll, xuu)
        
        twfe2(beginning_PF2,variables[[z]],"constant","constant","flood_risk5", list(bank_after_PF, ip_after_PF), c("bank_after", "ip_after"))
        print_twfe_week(beginning_PF2, variables[[z]], variables_labels[[z]], c("bank_after", "ip_after"), legend_name2, xll, xuu)
        twfe2(beginning_PJ2,variables[[z]],"constant","constant","flood_risk5", list(bank_after_PJ, ip_after_PJ), c("bank_after", "ip_after"))
        print_twfe_week(beginning_PJ2, variables[[z]], variables_labels[[z]], c("bank_after", "ip_after"), legend_name2, xll, xuu)
        
        #total
        twfe2(beginning_PF3,variables[[z]],"constant","constant","flood_risk5", list(dat_before_PF2_total, dat_after_PF2_total), c("before", "after"))
        print_twfe_week(beginning_PF3, variables[[z]], variables_labels[[z]], c("before", "after"), legend_name3, xll, xuu)
        twfe2(beginning_PJ3,variables[[z]],"constant","constant","flood_risk5", list(dat_before_PJ2_total, dat_after_PJ2_total), c("before", "after"))
        print_twfe_week(beginning_PJ3, variables[[z]], variables_labels[[z]], c("before", "after"), legend_name3, xll, xuu)
        
        twfe2(beginning_3,variables[[z]],"constant","constant","flood_risk5", list(dat_before2_total, dat_after2_total), c("before", "after"))
        print_twfe_week(beginning_3, variables[[z]], variables_labels[[z]], c("before", "after"), legend_name3, xll, xuu)
        
        twfe2(beginning_PF4,variables[[z]],"constant","constant","flood_risk5", list(dat_before_PF_total, dat_after_PF_total), c("before", "after"))
        print_twfe_week(beginning_PF4, variables[[z]], variables_labels[[z]], c("before", "after"), legend_name3, xll, xuu)
        twfe2(beginning_PJ4,variables[[z]],"constant","constant","flood_risk5", list(dat_before_PJ_total, dat_after_PJ_total), c("before", "after"))
        print_twfe_week(beginning_PJ4, variables[[z]], variables_labels[[z]], c("before", "after"), legend_name3, xll, xuu)
        
        twfe2(beginning_4,variables[[z]],"constant","constant","flood_risk5", list(dat_before_total, dat_after_total), c("before", "after"))
        print_twfe_week(beginning_4, variables[[z]], variables_labels[[z]], c("before", "after"), legend_name3, xll, xuu)
      }
      rm(digi_after_PF, trad_after_PF, digi_after_PJ, trad_after_PJ, dat_after_PJ, dat_after_PF)
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in CCS_Muni_IF:", e))
  })
}

# ------------------------------------------------------------------------------
# CCS_HHI - Worked!
# ------------------------------------------------------------------------------
#CCS_Muni_HHI_PF_v2.dta and CCS_Muni_HHI_PJ_v2.dta
# Variables: week, muni_cd, tipo, HHI_account

# Graphs: HHI before vs after for people and firms
variables <- c("HHI_account")
variables_labels <- c("HHI Bank Accounts")
if(run_CCS_HHI == 1){
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood_a <- flood_week_after
        flood_b <- flood_week_before2019
        xll <- xl
        xuu <- xu
        ending <- "_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
        beginning_PF <- paste0("CCS_Muni_PF", ending)
        beginning_PJ <- paste0("CCS_Muni_PJ", ending)
      }
      if(i==2){
        flood_a <- flood_week_after_balanced
        flood_b <- flood_week_before_balanced2019
        xll <- xl_balanced
        xuu <- xu_balanced
        ending <- "_balanced_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
        beginning_PF <- paste0("CCS_Muni_PF", ending)
        beginning_PJ <- paste0("CCS_Muni_PJ", ending)
      }
      if(i==3){
        flood_a <- flood_week_after_balanced_covid
        flood_b <- flood_week_before_balanced_covid
        xll <- xl_balanced_covid
        xuu <- xu_balanced_covid
        ending <- "_balanced_covid_"
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
        beginning_PF <- paste0("CCS_Muni_PF", ending)
        beginning_PJ <- paste0("CCS_Muni_PJ", ending)
      }
      dat_HHI_after_PF <- prepare_data("CCS_Muni_HHI_PF_v2.dta",flood_a,mun_fe,mun_control)
      dat_HHI_before_PF <- prepare_data("CCS_Muni_HHI_PF_v2.dta",flood_b,mun_fe,mun_control)
      dat_HHI_after_PJ <- prepare_data("CCS_Muni_HHI_PJ_v2.dta",flood_a,mun_fe,mun_control)
      dat_HHI_before_PJ <- prepare_data("CCS_Muni_HHI_PJ_v2.dta",flood_b,mun_fe,mun_control)      
      
      for(z in 1:length(variables)){
        twfe2(beginning_PF,variables[[z]],"constant","constant","flood_risk5", list(dat_HHI_before_PF, dat_HHI_after_PF), c("before","after"))
        print_twfe_week(beginning_PF, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
        
        twfe2(beginning_PJ,variables[[z]],"constant","constant","flood_risk5", list(dat_HHI_before_PJ, dat_HHI_after_PJ), c("before","after"))
        print_twfe_week(beginning_PJ, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
      }
      rm(dat_HHI_after_PF,dat_HHI_before_PF,dat_HHI_after_PJ,dat_HHI_before_PJ)
    }
  }, error = function(e) {
    print(paste("Error in CCS_HHI:", e))
  })
}
# ------------------------------------------------------------------------------
# CCS_first_account - Worked!
# ------------------------------------------------------------------------------

#CCS_Muni_first_account_v2.dta
# Variables: week, muni_cd, tipo, first_account
#             lfirst_account

# Graphs: first_account, lfirst_account, and first account / population before vs after for people and firms
variables <- c("lfirst_account","first_account","f_account_pop")
variables_labels <- c("Log Adoption of Bank Accounts","Adoption of Bank Accounts","Adoption of Bank Accounts over Population")
if(run_CCS_first_account == 1){
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood_a <- flood_week_after
        flood_b <- flood_week_before2019
        xll <- xl
        xuu <- xu
        ending <- "_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
        beginning_PF <- paste0("CCS_Muni_PF", ending)
        beginning_PJ <- paste0("CCS_Muni_PJ", ending)
      }
      if(i==2){
        flood_a <- flood_week_after_balanced
        flood_b <- flood_week_before_balanced2019
        xll <- xl_balanced
        xuu <- xu_balanced
        ending <- "_balanced_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
        beginning_PF <- paste0("CCS_Muni_PF", ending)
        beginning_PJ <- paste0("CCS_Muni_PJ", ending)
      }
      if(i==3){
        flood_a <- flood_week_after_balanced_covid
        flood_b <- flood_week_before_balanced_covid
        xll <- xl_balanced_covid
        xuu <- xu_balanced_covid
        ending <- "_balanced_covid_"
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
        beginning_PF <- paste0("CCS_Muni_PF", ending)
        beginning_PJ <- paste0("CCS_Muni_PJ", ending)
      }
      dat_after <- prepare_data("CCS_Muni_first_account_v2.dta",flood_a,mun_fe,mun_control)
      dat_before <- prepare_data("CCS_Muni_first_account_v2.dta",flood_b,mun_fe,mun_control)
      dat_after <- dat_after %>%
        mutate(f_account_pop = ifelse(is.na(pop2022) | pop2022 == 0 | is.na(first_account), NA, first_account/pop2022))
      dat_before <- dat_before %>%
        mutate(f_account_pop = ifelse(is.na(pop2022) | pop2022 == 0 | is.na(first_account), NA, first_account/pop2022))
      dat_after_PF <- dat_after %>% filter(tipo==1)
      dat_before_PF <- dat_before %>% filter(tipo==1)
      dat_after_PJ <- dat_after %>% filter(tipo==2)
      dat_before_PJ <- dat_before %>% filter(tipo==2)
      
      for(z in 1:length(variables)){
        twfe2(beginning_PF,variables[[z]],"constant","constant","flood_risk5", list(dat_before_PF, dat_after_PF), c("before","after"))
        print_twfe_week(beginning_PF, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
        
        twfe2(beginning_PJ,variables[[z]],"constant","constant","flood_risk5", list(dat_before_PJ, dat_after_PJ), c("before","after"))
        print_twfe_week(beginning_PJ, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
      }
      rm(dat_after,dat_before,dat_after_PF,dat_before_PF,dat_after_PJ,dat_before_PJ)
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in CCS_first_account:", e))
  })
}

#CCS_Muni_first_account_month_v2.dta
# Variables: time_id, muni_cd, tipo, first_account
#             lfirst_account

if(run_CCS_first_account_month == 1){
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood_a <- flood_month_after
        flood_b <- flood_month_before2019
        xll <- xl_month
        xuu <- xu_month
        ending <- "_month_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
        beginning_PF <- paste0("CCS_Muni_PF", ending)
        beginning_PJ <- paste0("CCS_Muni_PJ", ending)
      }
      if(i==2){
        flood_a <- flood_month_after_balanced
        flood_b <- flood_month_before_balanced2019
        xll <- xl_balanced_month
        xuu <- xu_balanced_month
        ending <- "_month_balanced_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
        beginning_PF <- paste0("CCS_Muni_PF", ending)
        beginning_PJ <- paste0("CCS_Muni_PJ", ending)
      }
      if(i==3){
        flood_a <- flood_month_after_balanced_covid
        flood_b <- flood_month_before_balanced_covid
        xll <- xl_balanced_covid_month
        xuu <- xu_balanced_covid_month
        ending <- "_month_balanced_covid_"
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
        beginning_PF <- paste0("CCS_Muni_PF", ending)
        beginning_PJ <- paste0("CCS_Muni_PJ", ending)
      }
      dat_after <- prepare_data("CCS_Muni_first_account_month_v2.dta",flood_a,mun_fe,mun_control)
      dat_before <- prepare_data("CCS_Muni_first_account_month_v2.dta",flood_b,mun_fe,mun_control)
      dat_after <- dat_after %>%
        mutate(f_account_pop = ifelse(is.na(pop2022) | pop2022 == 0 | is.na(first_account), NA, first_account/pop2022))
      dat_before <- dat_before %>%
        mutate(f_account_pop = ifelse(is.na(pop2022) | pop2022 == 0 | is.na(first_account), NA, first_account/pop2022))
      dat_after_PF <- dat_after %>% filter(tipo==1)
      dat_before_PF <- dat_before %>% filter(tipo==1)
      dat_after_PJ <- dat_after %>% filter(tipo==2)
      dat_before_PJ <- dat_before %>% filter(tipo==2)
      
      for(z in 1:length(variables)){
        twfe2(beginning_PF,variables[[z]],"constant","constant","flood_risk5", list(dat_before_PF, dat_after_PF), c("before","after"))
        print_twfe_month(beginning_PF, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
        
        twfe2(beginning_PJ,variables[[z]],"constant","constant","flood_risk5", list(dat_before_PJ, dat_after_PJ), c("before","after"))
        print_twfe_month(beginning_PJ, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
      }
      rm(dat_after,dat_before,dat_after_PF,dat_before_PF,dat_after_PJ,dat_before_PJ)
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in CCS_first_account:", e))
  })
}

# ------------------------------------------------------------------------------
# Pix_Muni_Bank - Worked ____________> Need to run again, the coeficients are not there.
# ------------------------------------------------------------------------------

# SELF?

#Pix_Muni_Bank.dta and Pix_Muni_Bank_self.dta
# Variables:  week, muni_cd, tipo, bank, tipo_inst, bank_type,
#             value_send, trans_send, send_users, value_send_w, value_rec, trans_rec, rec_users, value_rec_w
#             lvalue_send, ltrans_send, lsend_users, lvalue_send_w, lvalue_rec, ltrans_rec, lrec_users, lvalue_rec_w

# Graphs: ltrans, lvalue_w, lusers Digital vs Traditional for people and firms + users of all accounts. 
variables <- c("ltrans","lvalue_w","lvalue","lusers")
variables_labels <- c("Log Transactions","Log Value","Log Value","Log Users")
if(run_Pix_Muni_Bank == 1){
  tryCatch({
    for(i in 2:3){
      if(i==1){
        flood_a <- flood_week_after
        xll <- xl
        xuu <- xu
        ending <- ""
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
        beginning_PF <- paste0("Pix_Muni_Bank_PF_", ending)
        beginning_PJ <- paste0("Pix_Muni_Bank_PJ_", ending)
        beginning_PF2 <- paste0("Pix_Muni_Bank_PF_2", ending) # Problem here!???
        beginning_PJ2 <- paste0("Pix_Muni_Bank_PJ_2", ending)
        beginning_PF3 <- paste0("Pix_Muni_Bank_PF_3", ending)
        beginning_PJ3 <- paste0("Pix_Muni_Bank_PJ_3", ending)
      }
      if(i==2){
        flood_a <- flood_week_after_balanced
        xll <- xl_balanced
        xuu <- xu_balanced
        ending <- "balanced_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
        beginning_PF <- paste0("Pix_Muni_Bank_PF_", ending)
        beginning_PJ <- paste0("Pix_Muni_Bank_PJ_", ending)
        beginning_PF2 <- paste0("Pix_Muni_Bank_PF_2", ending)
        beginning_PJ2 <- paste0("Pix_Muni_Bank_PJ_2", ending)
        beginning_PF3 <- paste0("Pix_Muni_Bank_PF_3", ending)
        beginning_PJ3 <- paste0("Pix_Muni_Bank_PJ_3", ending)
      }
      if(i==3){
        flood_a <- flood_week_after_balanced_covid
        xll <- xl_balanced_covid
        xuu <- xu_balanced_covid
        ending <- "balanced_covid_"
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
        beginning_PF <- paste0("Pix_Muni_Bank_PF_", ending)
        beginning_PJ <- paste0("Pix_Muni_Bank_PJ_", ending)
        beginning_PF2 <- paste0("Pix_Muni_Bank_PF_2", ending)
        beginning_PJ2 <- paste0("Pix_Muni_Bank_PJ_2", ending)
        beginning_PF3 <- paste0("Pix_Muni_Bank_PF_3", ending)
        beginning_PJ3 <- paste0("Pix_Muni_Bank_PJ_3", ending)
      }
      print(i)
      print("dat")
      dat <- prepare_data("Pix_Muni_Bank.dta",flood_a,mun_fe,mun_control)
      # Variables: time, month, year, muni_cd
      # flood_a: date_flood
      # mun_fe: "flood_risk5" "flood_risk4" "flood_risk2" "pop2010" "pop2022" "pop2010_quart" "pop2022_quart" "rural_urban" "pib_2017" "flood_risk" "muni_cd" "capital_uf" "id_regiao_imediata" "id_regiao_intermediaria" "id_uf" "nome_regiao" "nome_regiao_code" "pib_2017_quart" 
      # mun_control: "month" "year" "muni_cd" "pre" "mobile_access" "internet_access" "mobile_internet" "constant" "Na"    
      # treat, time_to_treat, time_id_treated, after_flood
      # Event Study Variables

      #------------------------------------------------------------------------
      print("dat_bank_ip")
      dat_bank_ip <- dat %>% select(-tipo_inst, -bank_type)
      print("merging with Cadastro_IF")
      dat_bank_ip <- merge(dat_bank_ip, Cadastro_IF, by = "bank", all.x = TRUE)
      print("Creating Bank and IP variables")
      dat_bank_ip <- dat_bank_ip %>%
        mutate(Bank = ifelse(macroseg_if_txt == "b1", 1, 0),
               IP = ifelse(macroseg_if_txt == "n4", 1, 0))
      print("grouping")
      colnames(dat_bank_ip)
      
      tryCatch({
      dat_bank_ip <- dat_bank_ip %>%
        select(-bank, -tipo_inst, -bank_type, -macroseg_if_txt, -lvalue_send, -ltrans_send, -lsend_users, -lvalue_send_w, -lvalue_rec, -ltrans_rec, -lrec_users, -lvalue_rec_w) %>%
        group_by(across(-c(value_send, trans_send, send_users, value_send_w, value_rec, trans_rec, rec_users, value_rec_w, IP, Bank))) %>% 
        summarise(value_send_w = sum(value_send_w, na.rm = TRUE),
                  trans_send = sum(trans_send, na.rm = TRUE),
                  send_users = sum(send_users, na.rm = TRUE),
                  value_rec_w = sum(value_rec_w, na.rm = TRUE),
                  trans_rec = sum(trans_rec, na.rm = TRUE),
                  rec_users = sum(rec_users, na.rm = TRUE),
                  value_send = sum(value_send, na.rm = TRUE),
                  value_rec = sum(value_rec, na.rm = TRUE),
                  IP = max(IP, na.rm = TRUE),
                  Bank = max(Bank, na.rm = TRUE)) %>%
        mutate(lvalue_send_w = log1p(value_send_w),
               ltrans_send = log1p(trans_send),
               lsend_users = log1p(send_users),
               lvalue_rec_w = log1p(value_rec_w),
               ltrans_rec = log1p(trans_rec),
               lrec_users = log1p(rec_users),
               lvalue_send = log1p(value_send),
               lvalue_rec = log1p(value_rec)) %>%
        ungroup()
      print("group_by(across worked")
      }, error = function(e) {
        dat_bank_ip <- dat_bank_ip %>%
          select(cong_id, week, muni_cd, tipo, time, month, year, date_flood, flood_risk5, constant, Na,
                 value_send, trans_send, send_users, value_send_w, value_rec, trans_rec, rec_users, value_rec_w,
                 Bank, IP,
                 treat, time_to_treat, time_id_treated, after_flood) %>% 
          group_by(cong_id, week, muni_cd, tipo, time, month, year, date_flood, flood_risk5, constant, Na,
                   treat, time_to_treat, time_id_treated, after_flood) %>% 
          summarise(value_send_w = sum(value_send_w, na.rm = TRUE),
                    trans_send = sum(trans_send, na.rm = TRUE),
                    send_users = sum(send_users, na.rm = TRUE),
                    value_rec_w = sum(value_rec_w, na.rm = TRUE),
                    trans_rec = sum(trans_rec, na.rm = TRUE),
                    rec_users = sum(rec_users, na.rm = TRUE),
                    value_send = sum(value_send, na.rm = TRUE),
                    value_rec = sum(value_rec, na.rm = TRUE),
                    IP = max(IP, na.rm = TRUE),
                    Bank = max(Bank, na.rm = TRUE)) %>%
          mutate(lvalue_send_w = log1p(value_send_w),
                 ltrans_send = log1p(trans_send),
                 lsend_users = log1p(send_users),
                 lvalue_rec_w = log1p(value_rec_w),
                 ltrans_rec = log1p(trans_rec),
                 lrec_users = log1p(rec_users),
                 lvalue_send = log1p(value_send),
                 lvalue_rec = log1p(value_rec)) %>%
          ungroup()
        print("group_by worked")
      })
      
      print("filtering")
      dat_bank_ip <- dat_bank_ip %>% 
        filter((Bank == 1) | (Bank == 0 & IP == 1)) %>%
        mutate(IP = ifelse(Bank == 1, 0, 1)) # this would make sure a conglomerate that is a bank and has an IP is counted just as bank.
      
      # Need to group by Bank and IP --------------------------------------------------------------------------------------------------------------------------------------------
      dat_bank_ip <- dat_bank_ip %>%
        group_by(week, muni_cd, tipo, time, month, year, date_flood, flood_risk5, constant, Na, Bank, IP,
                 treat, time_to_treat, time_id_treated, after_flood) %>%
        summarise(value_send_w = sum(value_send_w, na.rm = TRUE),
                  trans_send = sum(trans_send, na.rm = TRUE),
                  send_users = sum(send_users, na.rm = TRUE),
                  value_rec_w = sum(value_rec_w, na.rm = TRUE),
                  trans_rec = sum(trans_rec, na.rm = TRUE),
                  rec_users = sum(rec_users, na.rm = TRUE),
                  value_send = sum(value_send, na.rm = TRUE),
                  value_rec = sum(value_rec, na.rm = TRUE)) %>%
        mutate(lvalue_send_w = log1p(value_send_w),
               ltrans_send = log1p(trans_send),
               lsend_users = log1p(send_users),
               lvalue_rec_w = log1p(value_rec_w),
               ltrans_rec = log1p(trans_rec),
               lrec_users = log1p(rec_users),
               lvalue_send = log1p(value_send),
               lvalue_rec = log1p(value_rec)) %>%
        ungroup()
      
      # PEOPLE
      print("people")
      bank_PF <- dat_bank_ip %>%
        filter(Bank == 1, tipo == 1) %>%
        mutate(value_w = value_rec_w + value_send_w,
               value = value_rec + value_send,
               trans = trans_rec + trans_send,
               users = rec_users + send_users) %>%
        mutate(lvalue_w = log1p(value_w),
               lvalue = log1p(value),
               ltrans = log1p(trans),
               lusers = log1p(users))
      ip_PF <- dat_bank_ip %>%
        filter(Bank == 0, tipo == 1) %>%
        mutate(value_w = value_rec_w + value_send_w,
               value = value_rec + value_send,
               trans = trans_rec + trans_send,
               users = rec_users + send_users) %>%
        mutate(lvalue_w = log1p(value_w),
               lvalue = log1p(value),
               ltrans = log1p(trans),
               lusers = log1p(users))
  
      #Firms
      print("firms")
      bank_PJ <- dat_bank_ip %>%
        filter(Bank == 1, tipo == 2) %>%
        mutate(value_w = value_rec_w + value_send_w,
               value = value_rec + value_send,
               trans = trans_rec + trans_send,
               users = rec_users + send_users) %>%
        mutate(lvalue_w = log1p(value_w),
               lvalue = log1p(value),
               ltrans = log1p(trans),
               lusers = log1p(users))
      ip_PJ <- dat_bank_ip %>%
        filter(Bank == 0, tipo == 2) %>%
        mutate(value_w = value_rec_w + value_send_w,
               value = value_rec + value_send,
               trans = trans_rec + trans_send,
               users = rec_users + send_users) %>%
        mutate(lvalue_w = log1p(value_w),
               lvalue = log1p(value),
               ltrans = log1p(trans),
               lusers = log1p(users))
      
      
      #------------------------------------------------------------------------
      print("Now trad vs digi")
      dat <- dat %>%
        select(-tipo_inst, -bank, -lvalue_send, -ltrans_send, -lsend_users, -lvalue_send_w, -lvalue_rec, -ltrans_rec, -lrec_users, -lvalue_rec_w) %>%
        group_by_at(vars(-value_send, -trans_send, -send_users, -value_send_w, -value_rec, -trans_rec, -rec_users, -value_rec_w)) %>%
        summarise(value_send_w = sum(value_send_w, na.rm = TRUE),
                  trans_send = sum(trans_send, na.rm = TRUE),
                  send_users = sum(send_users, na.rm = TRUE),
                  value_rec_w = sum(value_rec_w, na.rm = TRUE),
                  trans_rec = sum(trans_rec, na.rm = TRUE),
                  rec_users = sum(rec_users, na.rm = TRUE),
                  value_send = sum(value_send, na.rm = TRUE),
                  value_rec = sum(value_rec, na.rm = TRUE)) %>%
        mutate(lvalue_send_w = log1p(value_send_w),
               ltrans_send = log1p(trans_send),
               lsend_users = log1p(send_users),
               lvalue_rec_w = log1p(value_rec_w),
               ltrans_rec = log1p(trans_rec),
               lrec_users = log1p(rec_users),
               lvalue_send = log1p(value_send),
               lvalue_rec = log1p(value_rec)) %>%
        ungroup()
      print("grouping trad vs digi")
      # PEOPLE
      trad_PF <- dat %>%
        filter(bank_type == 1, tipo == 1) %>%
        mutate(value_w = value_rec_w + value_send_w,
               value = value_rec + value_send,
               trans = trans_rec + trans_send,
               users = rec_users + send_users) %>%
        mutate(lvalue_w = log1p(value_w),
               lvalue = log1p(value),
               ltrans = log1p(trans),
               lusers = log1p(users))
      digi_PF <- dat %>%
        filter(bank_type == 2, tipo == 1) %>%
        mutate(value_w = value_rec_w + value_send_w,
               value = value_rec + value_send,
               trans = trans_rec + trans_send,
               users = rec_users + send_users) %>%
        mutate(lvalue_w = log1p(value_w),
               lvalue = log1p(value),
               ltrans = log1p(trans),
               lusers = log1p(users))
      all_banks_PF <- dat %>%
        filter(tipo == 1) %>%
        mutate(value_w = value_rec_w + value_send_w,
               value = value_rec + value_send,
               trans = trans_rec + trans_send,
               users = rec_users + send_users) %>%
        mutate(lvalue_w = log1p(value_w),
               lvalue = log1p(value),
               ltrans = log1p(trans),
               lusers = log1p(users))
      #Firms
      trad_PJ <- dat %>%
        filter(bank_type == 1, tipo == 2) %>%
        mutate(value_w = value_rec_w + value_send_w,
               value = value_rec + value_send,
               trans = trans_rec + trans_send,
               users = rec_users + send_users) %>%
        mutate(lvalue_w = log1p(value_w),
               lvalue = log1p(value),
               ltrans = log1p(trans),
               lusers = log1p(users))
      digi_PJ <- dat %>%
        filter(bank_type == 2, tipo == 2) %>%
        mutate(value_w = value_rec_w + value_send_w,
               value = value_rec + value_send,
               trans = trans_rec + trans_send,
               users = rec_users + send_users) %>%
        mutate(lvalue_w = log1p(value_w),
               lvalue = log1p(value),
               ltrans = log1p(trans),
               lusers = log1p(users))
      all_banks_PJ <- dat %>%
        filter(tipo == 2) %>%
        mutate(value_w = value_rec_w + value_send_w,
               value = value_rec + value_send,
               trans = trans_rec + trans_send,
               users = rec_users + send_users) %>%
        mutate(lvalue_w = log1p(value_w),
               lvalue = log1p(value),
               ltrans = log1p(trans),
               lusers = log1p(users)) 
      
      for(z in 1:length(variables)){
        twfe2(beginning_PF,variables[[z]],"constant","constant","flood_risk5", list(trad_PF,digi_PF), c("trad", "digi"))
        print_twfe_week(beginning_PF, variables[[z]], variables_labels[[z]], c("trad", "digi"), c("Traditional", "Digital"), xll, xuu)
        twfe2(beginning_PF2,variables[[z]],"constant","constant","flood_risk5", list(all_banks_PF), c("all"))
        print_twfe_week(beginning_PF2, variables[[z]], variables_labels[[z]], c("all"), c("All Banks"), xll, xuu)
        
        twfe2(beginning_PJ,variables[[z]],"constant","constant","flood_risk5", list(trad_PJ,digi_PJ), c("trad", "digi"))
        print_twfe_week(beginning_PJ, variables[[z]], variables_labels[[z]], c("trad", "digi"), c("Traditional", "Digital"), xll, xuu)
        twfe2(beginning_PJ2,variables[[z]],"constant","constant","flood_risk5", list(all_banks_PJ), c("all"))
        print_twfe_week(beginning_PJ2, variables[[z]], variables_labels[[z]], c("all"), c("All Banks"), xll, xuu)
        
        twfe2(beginning_PF3,variables[[z]],"constant","constant","flood_risk5", list(bank_PF,ip_PF), c("bank", "ip"))
        print_twfe_week(beginning_PF3, variables[[z]], variables_labels[[z]], c("bank", "ip"), c("Banks", "NBFIs"), xll, xuu)
        twfe2(beginning_PJ3,variables[[z]],"constant","constant","flood_risk5", list(bank_PJ,ip_PJ), c("bank", "ip"))
        print_twfe_week(beginning_PJ3, variables[[z]], variables_labels[[z]], c("bank", "ip"), c("Banks", "NBFIs"), xll, xuu)

      }
      
      
      # Value sent and received by Banks
      
      #value sent and received by NBFIs
      
      # unite pf and pj. 
      bank_0 <- dat_bank_ip %>%
        filter(Bank == 1) %>%
        select(week, muni_cd, time, month, year, date_flood, flood_risk5, constant, Na, value_send, trans_send, value_rec, trans_rec, value_send_w, value_rec_w,
               treat, time_to_treat, time_id_treated, after_flood) %>%
        group_by(week, muni_cd, time, month, year, date_flood, flood_risk5, constant, Na,
                 treat, time_to_treat, time_id_treated, after_flood) %>%
        summarise(value_send_w = sum(value_send_w, na.rm = TRUE),
                  trans_send = sum(trans_send, na.rm = TRUE),
                  value_rec_w = sum(value_rec_w, na.rm = TRUE),
                  trans_rec = sum(trans_rec, na.rm = TRUE),
                  value_send = sum(value_send, na.rm = TRUE),
                  value_rec = sum(value_rec, na.rm = TRUE)) %>%
        mutate(lvalue_send_w = log1p(value_send_w),
               ltrans_send = log1p(trans_send),
               lvalue_rec_w = log1p(value_rec_w),
               ltrans_rec = log1p(trans_rec),
               lvalue_send = log1p(value_send),
               lvalue_rec = log1p(value_rec)) %>%
        ungroup()
      
      bank_1 <- bank_0 %>% select(week, muni_cd, time, month, year, date_flood, flood_risk5, constant, Na, lvalue_send, ltrans_send, treat, time_to_treat, time_id_treated, after_flood) %>%
        rename(lvalue = lvalue_send,
               ltrans = ltrans_send)
      bank_2 <- bank_0 %>% select(week, muni_cd, time, month, year, date_flood, flood_risk5, constant, Na, lvalue_rec, ltrans_rec, treat, time_to_treat, time_id_treated, after_flood) %>%
        rename(lvalue = lvalue_rec,
               ltrans = ltrans_rec)
      
      ip_0 <- dat_bank_ip %>%
        filter(Bank == 0) %>%
        select(week, muni_cd, time, month, year, date_flood, flood_risk5, constant, Na, value_send, trans_send, value_rec, trans_rec, value_send_w, value_rec_w,
               treat, time_to_treat, time_id_treated, after_flood) %>%
        group_by(week, muni_cd, time, month, year, date_flood, flood_risk5, constant, Na,
                 treat, time_to_treat, time_id_treated, after_flood) %>%
        summarise(value_send_w = sum(value_send_w, na.rm = TRUE),
                  trans_send = sum(trans_send, na.rm = TRUE),
                  value_rec_w = sum(value_rec_w, na.rm = TRUE),
                  trans_rec = sum(trans_rec, na.rm = TRUE),
                  value_send = sum(value_send, na.rm = TRUE),
                  value_rec = sum(value_rec, na.rm = TRUE)) %>%
        mutate(lvalue_send_w = log1p(value_send_w),
               ltrans_send = log1p(trans_send),
               lvalue_rec_w = log1p(value_rec_w),
               ltrans_rec = log1p(trans_rec),
               lvalue_send = log1p(value_send),
               lvalue_rec = log1p(value_rec)) %>%
        ungroup()
        
      ip_1 <- ip_0 %>% select(week, muni_cd, time, month, year, date_flood, flood_risk5, constant, Na, lvalue_send, ltrans_send, treat, time_to_treat, time_id_treated, after_flood) %>%
        rename(lvalue = lvalue_send,
               ltrans = ltrans_send)
      ip_2 <- ip_0 %>% select(week, muni_cd, time, month, year, date_flood, flood_risk5, constant, Na, lvalue_rec, ltrans_rec, treat, time_to_treat, time_id_treated, after_flood) %>%
        rename(lvalue = lvalue_rec,
               ltrans = ltrans_rec)
      
      twfe2(paste0("Pix_Muni_Bank_4", ending),"lvalue","constant","constant","flood_risk5", list(bank_2,bank_1), c("bank_rec", "bank_sent"))
      print_twfe_week(paste0("Pix_Muni_Bank_4", ending), "lvalue", "Log Value", c("bank_rec", "bank_sent"), c("Received by Banks", "Sent by Banks"), xll, xuu)
      
      twfe2(paste0("Pix_Muni_Bank_42", ending),"lvalue","constant","constant","flood_risk5", list(ip_2,ip_1), c("ip_rec", "ip_sent"))
      print_twfe_week(paste0("Pix_Muni_Bank_42", ending), "lvalue", "Log Value", c("ip_rec", "ip_sent"), c("Received by NBFIs", "Sent by NBFIs"), xll, xuu)
      
      rm(trad_PF, digi_PF, all_banks_PF, trad_PJ, digi_PJ, all_banks_PJ, dat)
      rm(bank_0, ip_0, bank_1, bank_2, ip_1, ip_2)
    }
  }, error = function(e) {
    print(paste("Error in Pix_Muni_Bank:", e))
  })
}

# ------------------------------------------------------------------------------
# Pix_Muni_flow - Worked!
# ------------------------------------------------------------------------------

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


# WORK ON SEAN SUGESTIONS. - P2p, p2b, b2p, b2b. missing
if(run_Pix_Muni_flow == 1){
  tryCatch({
    #Send and Rec
    variables <- c("ltrans","lvalor_w","lvalor")
    variables_labels <- c("Log Transactions","Log Value","Log Value")
    tryCatch({
      for(i in 1:3){
        if(i==1){
          flood_a <- flood_week_after
          xll <- xl
          xuu <- xu
          ending <- ""
          beginning_PF <- paste0("Pix_Muni_PF_", ending)
          beginning_PJ <- paste0("Pix_Muni_PJ_", ending)
          beginning_PF2 <- paste0("Pix_Muni_PF_2", ending)
          beginning_PJ2 <- paste0("Pix_Muni_PJ_2", ending)
        }
        if(i==2){
          flood_a <- flood_week_after_balanced
          xll <- xl_balanced
          xuu <- xu_balanced
          ending <- "balanced_"
          beginning_PF <- paste0("Pix_Muni_PF_", ending)
          beginning_PJ <- paste0("Pix_Muni_PJ_", ending)
          beginning_PF2 <- paste0("Pix_Muni_PF_2", ending)
          beginning_PJ2 <- paste0("Pix_Muni_PJ_2", ending)
        }
        if(i==3){
          flood_a <- flood_week_after_balanced_covid
          xll <- xl_balanced_covid
          xuu <- xu_balanced_covid
          ending <- "balanced_covid_"
          beginning_PF <- paste0("Pix_Muni_PF_", ending)
          beginning_PJ <- paste0("Pix_Muni_PJ_", ending)
          beginning_PF2 <- paste0("Pix_Muni_PF_2", ending)
          beginning_PJ2 <- paste0("Pix_Muni_PJ_2", ending)
        }
        dat_rec <- prepare_data("Pix_Muni_flow_aggreg_rec.dta",flood_a,mun_fe,mun_control)
        dat_send <- prepare_data("Pix_Muni_flow_aggreg_send.dta",flood_a,mun_fe,mun_control)
        dat_combined <- bind_rows(dat_send, dat_rec) %>%
          group_by(across(-c(trans, valor, valor_w,ltrans, lvalor, lvalor_w))) %>%
          summarize(trans = sum(trans, na.rm = TRUE),
                    valor = sum(valor, na.rm = TRUE),
                    valor_w = sum(valor_w, na.rm = TRUE)) %>% ungroup() %>% 
          mutate(ltrans = log1p(trans),
                 lvalor = log1p(valor),
                 lvalor_w = log1p(valor_w)) 
        dat_rec1 <- dat_rec %>% filter(tipo == 1)
        dat_send1 <- dat_send %>% filter(tipo == 1)
        dat_combined1 <- dat_combined %>% filter(tipo == 1)
        # Firms
        dat_rec2 <- dat_rec %>% filter(tipo == 2)
        dat_send2 <- dat_send %>% filter(tipo == 2)
        dat_combined2 <- dat_combined %>% filter(tipo == 2)
        
        for(z in 1:length(variables)){
          twfe2(beginning_PF,variables[[z]],"constant","constant","flood_risk5", list(dat_rec1, dat_send1), c("rec","send"))
          print_twfe_week(beginning_PF, variables[[z]], variables_labels[[z]], c("rec","send"), c("Received","Sent"), xll, xuu)
          twfe2(beginning_PF2,variables[[z]],"constant","constant","flood_risk5", list(dat_combined1), c("pix"))
          print_twfe_week(beginning_PF2, variables[[z]], variables_labels[[z]], c("pix"), c("Pix"), xll, xuu)
          
          twfe2(beginning_PJ,variables[[z]],"constant","constant","flood_risk5", list(dat_rec2, dat_send2), c("rec","send"))
          print_twfe_week(beginning_PJ, variables[[z]], variables_labels[[z]], c("rec","send"), c("Received","Sent"), xll, xuu)
          twfe2(beginning_PJ2,variables[[z]],"constant","constant","flood_risk5", list(dat_combined2), c("pix"))
          print_twfe_week(beginning_PJ2, variables[[z]], variables_labels[[z]], c("pix"), c("Pix"), xll, xuu)
        }
        rm(dat_rec1, dat_send1, dat_combined1, dat_rec2, dat_send2, dat_combined2, dat_rec, dat_send, dat_combined)
      }
    }, error = function(e) {
      print(paste("Error in Send Rec:", e))
    })
    
    # Inflow vs Outflow + Self        
    variables <- c("lusers","lusers2","ltrans","lvalor_w", "lvalor")
    variables_labels <- c("Log Active Users Inside the Municipality","Log Active Users Outside the Municipality","Log Transactions","Log Value","Log Value")
    tryCatch({
      for(i in 1:3){
        if(i==1){
          flood_a <- flood_week_after
          xll <- xl
          xuu <- xu
          ending <- ""
          beginning_p2p <- paste0("Pix_Muni_flow_p2p_", ending)
          beginning_PF_self <- paste0("Pix_Muni_PF_self_", ending)
          beginning_PJ_self <- paste0("Pix_Muni_PJ_self_", ending)
        }
        if(i==2){
          flood_a <- flood_week_after_balanced
          xll <- xl_balanced
          xuu <- xu_balanced
          ending <- "balanced_"
          beginning_p2p <- paste0("Pix_Muni_flow_p2p_", ending)
          beginning_PF_self <- paste0("Pix_Muni_PF_self_", ending)
          beginning_PJ_self <- paste0("Pix_Muni_PJ_self_", ending)
        }
        if(i==3){
          flood_a <- flood_week_after_balanced_covid
          xll <- xl_balanced_covid
          xuu <- xu_balanced_covid
          ending <- "balanced_covid_"
          beginning_p2p <- paste0("Pix_Muni_flow_p2p_", ending)
          beginning_PF_self <- paste0("Pix_Muni_PF_self_", ending)
          beginning_PJ_self <- paste0("Pix_Muni_PJ_self_", ending)
        }
        dat <- prepare_data("Pix_Muni_flow.dta",flood_a,mun_fe,mun_control)
        dat_inflow_p2p <- dat %>%
          filter(sender_type == 1, receiver_type == 1, flow_code == 1) %>%
          rename(lusers = lreceivers,
                 lusers2 = lsenders)
        dat_outflow_p2p <- dat %>%
          filter(sender_type == 1, receiver_type == 1, flow_code == -1) %>%
          rename(lusers=lsenders,
                 lusers2=lreceivers)
        dat_self <- dat %>%
          filter(flow_code == 99)
        dat_self_PF <- dat_self %>%
          filter(receiver_type == 1)
        dat_self_PJ <- dat_self %>%
          filter(receiver_type == 2)

        for(z in 1:length(variables)){
          twfe2(beginning_p2p,variables[[z]],"constant","constant","flood_risk5", list(dat_inflow_p2p, dat_outflow_p2p), c("inflow","outflow"))
          print_twfe_week(beginning_p2p, variables[[z]], variables_labels[[z]], c("inflow","outflow"), c("Receiving","Sending"), xll, xuu)
        }
        twfe2(beginning_p2p,"lusers2","constant","constant","flood_risk5",list(dat_outflow_p2p, dat_inflow_p2p), c("inflow","outflow"))
        print_twfe_week(beginning_p2p,"lusers","Log Active Users Inside the Municipaltiy", c("inflow"), c("Receiving"), xll, xuu)
        print_twfe_week(beginning_p2p,"lusers2","Log Active Users Outside the Municipaltiy", c("outflow"), c("Sending"), xll, xuu)
        
        for(z in 1:(length(variables)-2)){
          twfe2(beginning_PF_self,variables[[z+2]],"constant","constant","flood_risk5", list(dat_self_PF), c("self"))
          print_twfe_week(beginning_PF_self, variables[[z+2]], variables_labels[[z+2]], c("self"), c("Self"), xll, xuu)
          
          twfe2(beginning_PJ_self,variables[[z+2]],"constant","constant","flood_risk5", list(dat_self_PJ), c("self"))
          print_twfe_week(beginning_PJ_self, variables[[z+2]], variables_labels[[z+2]], c("self"), c("Self"), xll, xuu)
        }
        twfe2(beginning_PF_self,"lreceivers","constant","constant","flood_risk5", list(dat_self_PF), c("self"))
        print_twfe_week(beginning_PF_self, "lreceivers", "Log Active Users", c("self"), c("Self"), xll, xuu)
        twfe2(beginning_PJ_self,"lreceivers","constant","constant","flood_risk5", list(dat_self_PJ), c("self"))
        print_twfe_week(beginning_PJ_self, "lreceivers", "Log Active Users", c("self"), c("Self"), xll, xuu)

        rm(dat_inflow_p2p, dat_outflow_p2p, dat, dat_self, dat_self_PF, dat_self_PJ)
      }
    }, error = function(e) {
      print(paste("Error in inflow and outflow:", e))
    })
    
    
    
    # B2P and P2B
    
    #Pix_Muni_flow.dta
    # Variables: week, muni_cd, sender_type, receiver_type, 
    # #             senders_rec, receivers_rec, valor_rec, trans_rec, valor_w_rec, 
    # #             senders_sent, receivers_sent, valor_sent, trans_sent, valor_w_sent
    
    
    variables <- c("ltrans_sent", "lvalor_w_sent", "lvalor_sent")
    variables_labels <- c("Log Transactions","Log Value","Log Value")
    tryCatch({
      for(i in 1:3){
        if(i==1){
          flood_a <- flood_week_after
          xll <- xl
          xuu <- xu
          ending <- ""
          beginning_p2p <- paste0("Pix_Muni_flow_b2p_", ending)
        }
        if(i==2){
          flood_a <- flood_week_after_balanced
          xll <- xl_balanced
          xuu <- xu_balanced
          ending <- "balanced_"
          beginning_p2p <- paste0("Pix_Muni_flow_b2p_", ending)
        }
        if(i==3){
          flood_a <- flood_week_after_balanced_covid
          xll <- xl_balanced_covid
          xuu <- xu_balanced_covid
          ending <- "balanced_covid_"
          beginning_p2p <- paste0("Pix_Muni_flow_b2p_", ending)
        }
        dat <- prepare_data("Pix_Muni_flow_aggreg.dta",flood_a,mun_fe,mun_control)
        
        dat_b2p <- dat %>%
          filter(sender_type == 2, receiver_type == 1)
        dat_p2b <- dat %>%
          filter(sender_type == 1, receiver_type == 2)
        
        for(z in 1:length(variables)){
          twfe2(beginning_p2p,variables[[z]],"constant","constant","flood_risk5", list(dat_b2p, dat_p2b), c("B2P","P2B"))
          print_twfe_week(beginning_p2p, variables[[z]], variables_labels[[z]], c("B2P","P2B"), c("B2P","P2B"), xll, xuu)
        }

        rm(dat_b2p, dat_p2b, dat)
      }
    }, error = function(e) {
      print(paste("Error in b2p p2b:", e))
    })
    
    
    
    # Total: P2P vs P2B vs B2P vs B2B - focus on sent variables since received variables are obvious (p2b sent = p2b rec)
    tryCatch({
      # dat_aggreg <- prepare_data("Pix_Muni_flow_aggreg.dta",flood_week_after,mun_fe,mun_control,xl, xu)
      # # Variables: week, muni_cd, sender_type, receiver_type, 
      # #             senders_rec, receivers_rec, valor_rec, trans_rec, valor_w_rec, 
      # #             senders_sent, receivers_sent, valor_sent, trans_sent, valor_w_sent
      # # Plus l variations. 
      # dat_p2p <- dat_aggreg %>%
      #   filter(sender_type == 1, receiver_type == 1) %>%
      #   rename(ltrans = ltrans_sent, lvalor_w = lvalor_w_sent, lsenders = lsenders_sent, lreceivers = lreceivers_rec)
      # dat_p2b <- dat_aggreg %>%
      #   filter(sender_type == 1, receiver_type == 2) %>%
      #   rename(ltrans = ltrans_sent, lvalor_w = lvalor_w_sent, lsenders = lsenders_sent, lreceivers = lreceivers_rec)
      # dat_b2p <- dat_aggreg %>%
      #   filter(sender_type == 2, receiver_type == 1) %>%
      #   rename(ltrans = ltrans_sent, lvalor_w = lvalor_w_sent, lsenders = lsenders_sent, lreceivers = lreceivers_rec)
      # dat_b2b <- dat_aggreg %>%
      #   filter(sender_type == 2, receiver_type == 2) %>%
      #   rename(ltrans = ltrans_sent, lvalor_w = lvalor_w_sent, lsenders = lsenders_sent, lreceivers = lreceivers_rec)
      # 
      # print_twfe("Pix_Muni_type_","ltrans","pre","internet_access","constant","Log Transactions",list(dat_p2p,dat_p2b,dat_b2p,dat_b2b), c("P2P","P2B", "B2P", "B2B"), xl, xu)
      # print_twfe("Pix_Muni_type_","lvalor_w","pre","internet_access","constant","Log Value",list(dat_p2p,dat_p2b,dat_b2p,dat_b2b), c("P2P","P2B", "B2P", "B2B"), xl, xu)
      # 
      # rm(dat_p2p,dat_p2b,dat_b2p,dat_b2b)
      # rm(dat_aggreg)
      # 
      # # Do balanced now! _balanced
      # 
      # dat_aggreg <- prepare_data("Pix_Muni_flow_aggreg.dta",flood_week_after_balanced,mun_fe,mun_control,-26, 26)
      # dat_p2p <- dat_aggreg %>%
      #   filter(sender_type == 1, receiver_type == 1) %>%
      #   rename(ltrans = ltrans_sent, lvalor_w = lvalor_w_sent, lsenders = lsenders_sent, lreceivers = lreceivers_rec)
      # dat_p2b <- dat_aggreg %>%
      #   filter(sender_type == 1, receiver_type == 2) %>%
      #   rename(ltrans = ltrans_sent, lvalor_w = lvalor_w_sent, lsenders = lsenders_sent, lreceivers = lreceivers_rec)
      # dat_b2p <- dat_aggreg %>%
      #   filter(sender_type == 2, receiver_type == 1) %>%
      #   rename(ltrans = ltrans_sent, lvalor_w = lvalor_w_sent, lsenders = lsenders_sent, lreceivers = lreceivers_rec)
      # dat_b2b <- dat_aggreg %>%
      #   filter(sender_type == 2, receiver_type == 2) %>%
      #   rename(ltrans = ltrans_sent, lvalor_w = lvalor_w_sent, lsenders = lsenders_sent, lreceivers = lreceivers_rec)
      # 
      # print_twfe("Pix_Muni_type_balanced_","ltrans","pre","internet_access","constant","Log Transactions",list(dat_p2p,dat_p2b,dat_b2p,dat_b2b), c("P2P","P2B", "B2P", "B2B"), -26, 26)
      # print_twfe("Pix_Muni_type_balanced_","lvalor_w","pre","internet_access","constant","Log Value",list(dat_p2p,dat_p2b,dat_b2p,dat_b2b), c("P2P","P2B", "B2P", "B2B"), -26, 26)
      # 
      # rm(dat_p2p,dat_p2b,dat_b2p,dat_b2b)
      # rm(dat_aggreg)
    }, error = function(e) {
      print(paste("Error in p2p, p2b, ...:", e))
    })
    
  }, error = function(e) {
    print(paste("Error in Pix_Muni_flow:", e))
  })
}
# ------------------------------------------------------------------------------
# Adoption_Pix
# ------------------------------------------------------------------------------
#filename <- c("adoption_ind")
#write_dta(data, paste0(dta_path,"adoption_ind",".dta"))
# Variables: dia, muni_cd, tipo, rec_adopters, send_adopters, self_adopters
#             lrec_adopters, lsend_adopters, lself_adopters

#Worked!

# It is an old file from processa. we created a new file! Need to create graphs for this new value. 
if(run_adoption_pix == 1) {
  tryCatch({
    dat_after_PF <- prepare_data("adoption_ind.dta",flood_week_after,mun_fe,mun_control)
    rec_adopt <- dat_after_PF %>%
      rename(ladopt = lrec_adopters,
             adopt = rec_adopters) %>%
      mutate(adopt_pop = ifelse(is.na(pop2022) | pop2022 == 0 | is.na(adopt), NA, adopt/pop2022))
    send_adopt <- dat_after_PF %>%
      rename(ladopt = lsend_adopters,
             adopt = send_adopters) %>%
      mutate(adopt_pop = ifelse(is.na(pop2022) | pop2022 == 0 | is.na(adopt), NA, adopt/pop2022))
    self_adopt <- dat_after_PF %>%
      rename(ladopt = lself_adopters,
             adopt = self_adopters) %>%
      mutate(adopt_pop = ifelse(is.na(pop2022) | pop2022 == 0 | is.na(adopt), NA, adopt/pop2022))
    
    # PF
    twfe2("Pix_adoption_PF_","ladopt","constant","constant","flood_risk5",list(rec_adopt, send_adopt), c("rec","send"))
    print_twfe_week("Pix_adoption_PF_","ladopt","Log Pix Adoption", c("rec","send"), c("Received","Sent"), xl, xu)
    twfe2("Pix_adoption_PF_","adopt","constant","constant","flood_risk5",list(rec_adopt, send_adopt), c("rec","send"))
    print_twfe_week("Pix_adoption_PF_","adopt","Pix Adoption", c("rec","send"), c("Received","Sent"), xl, xu)
    twfe2("Pix_adoption_PF_","adopt_pop","constant","constant","flood_risk5",list(rec_adopt, send_adopt), c("rec","send"))
    print_twfe_week("Pix_adoption_PF_","adopt_pop","Pix Adoption over Population", c("rec","send"), c("Received","Sent"), xl, xu)
    
    twfe2("Pix_adoption_PF_self_","ladopt","constant","constant","flood_risk5",list(self_adopt), c("self"))
    print_twfe_week("Pix_adoption_PF_self_","ladopt","Log Pix Adoption", c("self"), c("Self"), xl, xu)
    twfe2("Pix_adoption_PF_self_","adopt","constant","constant","flood_risk5",list(self_adopt), c("self"))
    print_twfe_week("Pix_adoption_PF_self_","adopt","Pix Adoption", c("self"), c("Self"), xl, xu)
    twfe2("Pix_adoption_PF_self_","adopt_pop","constant","constant","flood_risk5",list(self_adopt), c("self"))
    print_twfe_week("Pix_adoption_PF_self_","adopt_pop","Pix Adoption over Population", c("self"), c("Self"), xl, xu)
    
    rm(dat_after_PF,rec_adopt,send_adopt,self_adopt)
    
    # now balanced
    
    dat_after_PF <- prepare_data("adoption_ind.dta",flood_week_after_balanced,mun_fe,mun_control)
    rec_adopt <- dat_after_PF %>%
      rename(ladopt = lrec_adopters,
             adopt = rec_adopters) %>%
      mutate(adopt_pop = ifelse(is.na(pop2022) | pop2022 == 0 | is.na(adopt), NA, adopt/pop2022))
    send_adopt <- dat_after_PF %>%
      rename(ladopt = lsend_adopters,
             adopt = send_adopters) %>%
      mutate(adopt_pop = ifelse(is.na(pop2022) | pop2022 == 0 | is.na(adopt), NA, adopt/pop2022))
    self_adopt <- dat_after_PF %>%
      rename(ladopt = lself_adopters,
             adopt = self_adopters) %>%
      mutate(adopt_pop = ifelse(is.na(pop2022) | pop2022 == 0 | is.na(adopt), NA, adopt/pop2022))
    # PF
    twfe2("Pix_adoption_PF_balanced_","ladopt","constant","constant","flood_risk5",list(rec_adopt, send_adopt), c("rec","send"))
    print_twfe_week("Pix_adoption_PF_balanced_","ladopt","Log Pix Adoption", c("rec","send"), c("Received","Sent"), xl_balanced, xu_balanced)
    twfe2("Pix_adoption_PF_balanced_","adopt","constant","constant","flood_risk5",list(rec_adopt, send_adopt), c("rec","send"))
    print_twfe_week("Pix_adoption_PF_balanced_","adopt","Pix Adoption", c("rec","send"), c("Received","Sent"), xl_balanced, xu_balanced)
    twfe2("Pix_adoption_PF_balanced_","adopt_pop","constant","constant","flood_risk5",list(rec_adopt, send_adopt), c("rec","send"))
    print_twfe_week("Pix_adoption_PF_balanced_","adopt_pop","Pix Adoption over Population", c("rec","send"), c("Received","Sent"), xl_balanced, xu_balanced)
    
    twfe2("Pix_adoption_PF_self_balanced_","ladopt","constant","constant","flood_risk5",list(self_adopt), c("self"))
    print_twfe_week("Pix_adoption_PF_self_balanced_","ladopt","Log Pix Adoption", c("self"), c("Self"), xl_balanced, xu_balanced)
    twfe2("Pix_adoption_PF_self_balanced_","adopt","constant","constant","flood_risk5",list(self_adopt), c("self"))
    print_twfe_week("Pix_adoption_PF_self_balanced_","adopt","Pix Adoption", c("self"), c("Self"), xl_balanced, xu_balanced)
    twfe2("Pix_adoption_PF_self_balanced_","adopt_pop","constant","constant","flood_risk5",list(self_adopt), c("self"))
    print_twfe_week("Pix_adoption_PF_self_balanced_","adopt_pop","Pix Adoption over Population", c("self"), c("Self"), xl_balanced, xu_balanced)
    
    rm(dat_after_PF,rec_adopt,send_adopt,self_adopt)
    
    # now balanced_covid
    
    dat_after_PF <- prepare_data("adoption_ind.dta",flood_week_after_balanced_covid,mun_fe,mun_control)
    rec_adopt <- dat_after_PF %>%
      rename(ladopt = lrec_adopters,
             adopt = rec_adopters) %>%
      mutate(adopt_pop = ifelse(is.na(pop2022) | pop2022 == 0 | is.na(adopt), NA, adopt/pop2022))
    send_adopt <- dat_after_PF %>%
      rename(ladopt = lsend_adopters,
             adopt = send_adopters) %>%
      mutate(adopt_pop = ifelse(is.na(pop2022) | pop2022 == 0 | is.na(adopt), NA, adopt/pop2022))
    self_adopt <- dat_after_PF %>%
      rename(ladopt = lself_adopters,
             adopt = self_adopters) %>%
      mutate(adopt_pop = ifelse(is.na(pop2022) | pop2022 == 0 | is.na(adopt), NA, adopt/pop2022))
    # PF
    twfe2("Pix_adoption_PF_balanced_covid_","ladopt","constant","constant","flood_risk5",list(rec_adopt, send_adopt), c("rec","send"))
    print_twfe_week("Pix_adoption_PF_balanced_covid_","ladopt","Log Pix Adoption", c("rec","send"), c("Received","Sent"), xl_balanced_covid, xu_balanced_covid)
    twfe2("Pix_adoption_PF_balanced_covid_","adopt","constant","constant","flood_risk5",list(rec_adopt, send_adopt), c("rec","send"))
    print_twfe_week("Pix_adoption_PF_balanced_covid_","adopt","Pix Adoption", c("rec","send"), c("Received","Sent"), xl_balanced_covid, xu_balanced_covid)
    twfe2("Pix_adoption_PF_balanced_covid_","adopt_pop","constant","constant","flood_risk5",list(rec_adopt, send_adopt), c("rec","send"))
    print_twfe_week("Pix_adoption_PF_balanced_covid_","adopt_pop","Pix Adoption over Population", c("rec","send"), c("Received","Sent"), xl_balanced_covid, xu_balanced_covid)
    
    
    twfe2("Pix_adoption_PF_self_balanced_covid_","ladopt","constant","constant","flood_risk5",list(self_adopt), c("self"))
    print_twfe_week("Pix_adoption_PF_self_balanced_covid_","ladopt","Log Pix Adoption", c("self"), c("Self"), xl_balanced_covid, xu_balanced_covid)
    twfe2("Pix_adoption_PF_self_balanced_covid_","adopt","constant","constant","flood_risk5",list(self_adopt), c("self"))
    print_twfe_week("Pix_adoption_PF_self_balanced_covid_","adopt","Pix Adoption", c("self"), c("Self"), xl_balanced_covid, xu_balanced_covid)
    twfe2("Pix_adoption_PF_self_balanced_covid_","adopt_pop","constant","constant","flood_risk5",list(self_adopt), c("self"))
    print_twfe_week("Pix_adoption_PF_self_balanced_covid_","adopt_pop","Pix Adoption over Population", c("self"), c("Self"), xl_balanced_covid, xu_balanced_covid)
    
    rm(dat_after_PF,rec_adopt,send_adopt,self_adopt)
    
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in Adoption_Pix:", e))
  })
}


# ------------------------------------------------------------------------------
# Pix_Muni_user - Worked! 
# ------------------------------------------------------------------------------


#Pix_Muni_user.dta
# Variables: week, muni_cd, tipo, 
#             users, senders, receivers
#             lusers, lsenders, lreceivers

#expand later to send users, rec users. self users (already done)
variables <- c("lusers","lusers2")
variables_labels <- c("Log Active Users","Log Active Users")
if(run_Pix_Muni_user == 1) {
  tryCatch({
    for(i in 1:3){
      if(i==1){
        flood_a <- flood_week_after
        xll <- xl
        xuu <- xu
        ending <- ""
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
        beginning_PF <- paste0("Pix_Muni_user_PF_", ending)
        beginning_PJ <- paste0("Pix_Muni_user_PJ_", ending)
      }
      if(i==2){
        flood_a <- flood_week_after_balanced
        xll <- xl_balanced
        xuu <- xu_balanced
        ending <- "balanced_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
        beginning_PF <- paste0("Pix_Muni_user_PF_", ending)
        beginning_PJ <- paste0("Pix_Muni_user_PJ_", ending)
      }
      if(i==3){
        flood_a <- flood_week_after_balanced_covid
        xll <- xl_balanced_covid
        xuu <- xu_balanced_covid
        ending <- "balanced_covid_"
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
        beginning_PF <- paste0("Pix_Muni_user_PF_", ending)
        beginning_PJ <- paste0("Pix_Muni_user_PJ_", ending)
      }
      dat <- prepare_data("Pix_Muni_user.dta",flood_a,mun_fe,mun_control)
      dat_PF <- dat %>%
        filter(tipo==1) %>%
        rename(lusers2 = lsenders)
      dat_PF2 <- dat %>%
        filter(tipo==1) %>%
        rename(lusers2 = lreceivers)
      dat_PJ <- dat %>%
        filter(tipo==2) %>%
        rename(lusers2 = lsenders)
      dat_PJ2 <- dat %>%
        filter(tipo==2) %>%
        rename(lusers2 = lreceivers)
      
      twfe2(beginning_PF,"lusers","constant","constant","flood_risk5",list(dat_PF), c("Pix"))
      print_twfe_week(beginning_PF,"lusers","Log Active Users", c("Pix"), c("Pix"), xll, xuu)
      twfe2(beginning_PF,"lusers2","constant","constant","flood_risk5",list(dat_PF, dat_PF2), c("Senders","Receivers"))
      print_twfe_week(beginning_PF,"lusers2","Log Active Users", c("Senders","Receivers"), c("Senders","Receivers"), xll, xuu)
      
      twfe2(beginning_PJ,"lusers","constant","constant","flood_risk5",list(dat_PJ), c("Pix"))
      print_twfe_week(beginning_PJ,"lusers","Log Active Users", c("Pix"), c("Pix"), xll, xuu)
      twfe2(beginning_PJ,"lusers2","constant","constant","flood_risk5",list(dat_PJ, dat_PJ2), c("Senders","Receivers"))
      print_twfe_week(beginning_PJ,"lusers2","Log Active Users", c("Senders","Receivers"), c("Senders","Receivers"), xll, xuu)
  
      rm(dat, dat_PF, dat_PJ, dat_PF2, dat_PJ2)
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in Pix_Muni_user:", e))
  })
}


#-------------------------------------------------------------------------------
# Old Graphs redone. 
#-------------------------------------------------------------------------------


#Base_week_muni.dta - Worked
if(run_Base_old == 1) {
  tryCatch({
    dat_a <- prepare_data("Base_week_muni.dta",flood_week_after,mun_fe,mun_control)
    dat_b <- prepare_data("Base_week_muni.dta",flood_week_before2019,mun_fe,mun_control)
    
    dat_a <- dat_a %>%
      mutate(log_valor_TED_intra = log1p(valor_TED_intra),
             log_qtd_TED_intra = log1p(qtd_TED_intra),
             log_qtd_cli_TED_rec_PJ = log1p(qtd_cli_TED_rec_PJ),
             log_qtd_cli_TED_pag_PJ = log1p(qtd_cli_TED_pag_PJ),
             log_valor_boleto = log1p(valor_boleto),
             log_qtd_boleto = log1p(qtd_boleto),
             log_qtd_cli_pag_pf_boleto = log1p(qtd_cli_pag_pf_boleto),
             log_qtd_cli_pag_pj_boleto = log1p(qtd_cli_pag_pj_boleto),
             log_qtd_cli_rec_pj_boleto = log1p(qtd_cli_rec_pj_boleto),
             log_valor_cartao_credito = log1p(valor_cartao_credito),
             log_valor_cartao_debito = log1p(valor_cartao_debito),
             log_qtd_cli_cartao_debito = log1p(qtd_cli_cartao_debito),
             log_qtd_cli_cartao_credito = log1p(qtd_cli_cartao_credito),
             log_qtd_cli_cartao = log1p(qtd_cli_cartao_debito+qtd_cli_cartao_credito),
             log_valor_cartao = log1p(valor_cartao_debito+valor_cartao_credito))
    dat_b <- dat_b %>%
      mutate(log_valor_TED_intra = log1p(valor_TED_intra),
             log_qtd_TED_intra = log1p(qtd_TED_intra),
             log_qtd_cli_TED_rec_PJ = log1p(qtd_cli_TED_rec_PJ),
             log_qtd_cli_TED_pag_PJ = log1p(qtd_cli_TED_pag_PJ),
             log_valor_boleto = log1p(valor_boleto),
             log_qtd_boleto = log1p(qtd_boleto),
             log_qtd_cli_pag_pf_boleto = log1p(qtd_cli_pag_pf_boleto),
             log_qtd_cli_pag_pj_boleto = log1p(qtd_cli_pag_pj_boleto),
             log_qtd_cli_rec_pj_boleto = log1p(qtd_cli_rec_pj_boleto),
             log_valor_cartao_credito = log1p(valor_cartao_credito),
             log_valor_cartao_debito = log1p(valor_cartao_debito),
             log_qtd_cli_cartao_debito = log1p(qtd_cli_cartao_debito),
             log_qtd_cli_cartao_credito = log1p(qtd_cli_cartao_credito),
             log_qtd_cli_cartao = log1p(qtd_cli_cartao_debito+qtd_cli_cartao_credito),
             log_valor_cartao = log1p(valor_cartao_debito+valor_cartao_credito))
    # var_list <- c("valor_TED_intra","qtd_TED_intra","qtd_cli_TED_rec_PJ","qtd_cli_TED_pag_PJ","valor_boleto","qtd_boleto","qtd_cli_pag_pf_boleto","qtd_cli_pag_pj_boleto","qtd_cli_rec_pj_boleto","valor_cartao_credito","valor_cartao_debito","qtd_cli_cartao_debito","qtd_cli_cartao_credito")
    # dat_b <- dat_b %>%
    #   mutate(across(all_of(var_list), ~ log1p(.), .names ="log_{.col}"))
    
    #TED
    twfe2("TED_","log_valor_TED_intra","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("TED_","log_valor_TED_intra","Log Value TED", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    twfe2("TED_","log_qtd_TED_intra","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("TED_","log_qtd_TED_intra","Log Transactions TED", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    twfe2("TED_","log_qtd_cli_TED_rec_PJ","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("TED_","log_qtd_cli_TED_rec_PJ","Log Quantity of Firms Receiving TED", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    twfe2("TED_","log_qtd_cli_TED_pag_PJ","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("TED_","log_qtd_cli_TED_pag_PJ","Log Quantity of Firms Sending TED", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    # + Uso da conta + "adocao"
    
    #Boleto
    twfe2("Boleto_","log_valor_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_","log_valor_boleto","Log Value Boleto", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    twfe2("Boleto_","log_qtd_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_","log_qtd_boleto","Log Transactions Boleto", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    twfe2("Boleto_","log_qtd_cli_pag_pf_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_","log_qtd_cli_pag_pf_boleto","Log Quantity of People Sending Boleto", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    twfe2("Boleto_","log_qtd_cli_pag_pj_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_","log_qtd_cli_pag_pj_boleto","Log Quantity of Firms Sending Boleto", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    twfe2("Boleto_","log_qtd_cli_rec_pj_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_","log_qtd_cli_rec_pj_boleto","Log Quantity of Firms Receiving Boleto", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    # dat_a <- dat_a %>%
    #   mutate(log_valor_boleto_eletronico = log1p(valor_boleto_eletronico),
    #          log_valor_boleto_presencial = log1p(valor_boleto-valor_boleto_eletronico),
    #          log_valor_boleto_dinheiro = log1p(valor_boleto_dinheiro),
    #          log_qtd_boleto_eletronico = log1p(qtd_boleto_eletronico),
    #          log_qtd_boleto_presencial = log1p(qtd_boleto-qtd_boleto_eletronico),
    #          log_qtd_boleto_dinheiro = log1p(qtd_boleto_dinheiro))
    # dat_b <- dat_b %>%
    #   mutate(log_valor_boleto_eletronico = log1p(valor_boleto_eletronico),
    #          log_valor_boleto_presencial = log1p(valor_boleto-valor_boleto_eletronico),
    #          log_valor_boleto_dinheiro = log1p(valor_boleto_dinheiro),
    #          log_qtd_boleto_eletronico = log1p(qtd_boleto_eletronico),
    #          log_qtd_boleto_presencial = log1p(qtd_boleto-qtd_boleto_eletronico),
    #          log_qtd_boleto_dinheiro = log1p(qtd_boleto_dinheiro))
    # 
    # print_twfe("Boleto_","log_qtd_boleto_eletronico","pre","internet_access","constant","Log Transactions Boleto - Eletronic",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
    # print_twfe("Boleto_","log_qtd_boleto_presencial","pre","internet_access","constant","Log Transactions Boleto - In Person",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
    # print_twfe("Boleto_","log_qtd_boleto_dinheiro","pre","internet_access","constant","Log Transactions Boleto - Money",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
    # print_twfe("Boleto_","log_valor_boleto_eletronico","pre","internet_access","constant","Log Value Boleto - Eletronic",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
    # print_twfe("Boleto_","log_valor_boleto_presencial","pre","internet_access","constant","Log Value Boleto - In Person",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
    # print_twfe("Boleto_","log_valor_boleto_dinheiro","pre","internet_access","constant","Log Value Boleto - Money",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
    # # + Uso da conta + "adocao"
    
    
    #Cartao 
    # * valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito 
    twfe2("Cartao_","log_valor_cartao_debito","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_","log_valor_cartao_debito","Log Value Debit Card", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    twfe2("Cartao_","log_valor_cartao_credito","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_","log_valor_cartao_credito","Log Value Credit Card", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    twfe2("Cartao_","log_qtd_cli_cartao_debito","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_","log_qtd_cli_cartao_debito","Log Quantity of Firms Accepting Debit Card", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    twfe2("Cartao_","log_qtd_cli_cartao_credito","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_","log_qtd_cli_cartao_credito","Log Quantity of Firms Accepting Credit Card", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    twfe2("Cartao_","log_valor_cartao","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_","log_valor_cartao","Log Value Cards", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    twfe2("Cartao_","log_qtd_cli_cartao","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_","log_qtd_cli_cartao","Log Quantity of Firms Accepting Cards", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    
    # + Adocao 
    
    rm(dat_a, dat_b)
    
    # _balanced
    
    dat_a <- prepare_data("Base_week_muni.dta",flood_week_after_balanced,mun_fe,mun_control)
    dat_b <- prepare_data("Base_week_muni.dta",flood_week_before_balanced2019,mun_fe,mun_control)
    dat_a <- dat_a %>%
      mutate(log_valor_TED_intra = log1p(valor_TED_intra),
             log_qtd_TED_intra = log1p(qtd_TED_intra),
             log_qtd_cli_TED_rec_PJ = log1p(qtd_cli_TED_rec_PJ),
             log_qtd_cli_TED_pag_PJ = log1p(qtd_cli_TED_pag_PJ),
             log_valor_boleto = log1p(valor_boleto),
             log_qtd_boleto = log1p(qtd_boleto),
             log_qtd_cli_pag_pf_boleto = log1p(qtd_cli_pag_pf_boleto),
             log_qtd_cli_pag_pj_boleto = log1p(qtd_cli_pag_pj_boleto),
             log_qtd_cli_rec_pj_boleto = log1p(qtd_cli_rec_pj_boleto),
             log_valor_cartao_credito = log1p(valor_cartao_credito),
             log_valor_cartao_debito = log1p(valor_cartao_debito),
             log_qtd_cli_cartao_debito = log1p(qtd_cli_cartao_debito),
             log_qtd_cli_cartao_credito = log1p(qtd_cli_cartao_credito),
             log_qtd_cli_cartao = log1p(qtd_cli_cartao_debito+qtd_cli_cartao_credito),
             log_valor_cartao = log1p(valor_cartao_debito+valor_cartao_credito))
    dat_b <- dat_b %>%
      mutate(log_valor_TED_intra = log1p(valor_TED_intra),
             log_qtd_TED_intra = log1p(qtd_TED_intra),
             log_qtd_cli_TED_rec_PJ = log1p(qtd_cli_TED_rec_PJ),
             log_qtd_cli_TED_pag_PJ = log1p(qtd_cli_TED_pag_PJ),
             log_valor_boleto = log1p(valor_boleto),
             log_qtd_boleto = log1p(qtd_boleto),
             log_qtd_cli_pag_pf_boleto = log1p(qtd_cli_pag_pf_boleto),
             log_qtd_cli_pag_pj_boleto = log1p(qtd_cli_pag_pj_boleto),
             log_qtd_cli_rec_pj_boleto = log1p(qtd_cli_rec_pj_boleto),
             log_valor_cartao_credito = log1p(valor_cartao_credito),
             log_valor_cartao_debito = log1p(valor_cartao_debito),
             log_qtd_cli_cartao_debito = log1p(qtd_cli_cartao_debito),
             log_qtd_cli_cartao_credito = log1p(qtd_cli_cartao_credito),
             log_qtd_cli_cartao = log1p(qtd_cli_cartao_debito+qtd_cli_cartao_credito),
             log_valor_cartao = log1p(valor_cartao_debito+valor_cartao_credito))
    # var_list <- c("valor_TED_intra","qtd_TED_intra","qtd_cli_TED_rec_PJ","qtd_cli_TED_pag_PJ","valor_boleto","qtd_boleto","qtd_cli_pag_pf_boleto","qtd_cli_pag_pj_boleto","qtd_cli_rec_pj_boleto","valor_cartao_credito","valor_cartao_debito","qtd_cli_cartao_debito","qtd_cli_cartao_credito")
    # dat_b <- dat_b %>%
    #   mutate(across(all_of(var_list), ~ log1p(.), .names ="log_{.col}"))
    
    #TED
    twfe2("TED_balanced_","log_valor_TED_intra","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("TED_balanced_","log_valor_TED_intra","Log Value TED", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    
    twfe2("TED_balanced_","log_qtd_TED_intra","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("TED_balanced_","log_qtd_TED_intra","Log Transactions TED", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    
    twfe2("TED_balanced_","log_qtd_cli_TED_rec_PJ","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("TED_balanced_","log_qtd_cli_TED_rec_PJ","Log Quantity of Firms Receiving TED", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    
    twfe2("TED_balanced_","log_qtd_cli_TED_pag_PJ","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("TED_balanced_","log_qtd_cli_TED_pag_PJ","Log Quantity of Firms Sending TED", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    
    # + Uso da conta + "adocao"
    
    #Boleto
    twfe2("Boleto_balanced_","log_valor_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_balanced_","log_valor_boleto","Log Value Boleto", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    
    twfe2("Boleto_balanced_","log_qtd_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_balanced_","log_qtd_boleto","Log Transactions Boleto", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    
    twfe2("Boleto_balanced_","log_qtd_cli_pag_pf_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_balanced_","log_qtd_cli_pag_pf_boleto","Log Quantity of People Sending Boleto", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    
    twfe2("Boleto_balanced_","log_qtd_cli_pag_pj_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_balanced_","log_qtd_cli_pag_pj_boleto","Log Quantity of Firms Sending Boleto", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    
    twfe2("Boleto_balanced_","log_qtd_cli_rec_pj_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_balanced_","log_qtd_cli_rec_pj_boleto","Log Quantity of Firms Receiving Boleto", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    
    # dat_a <- dat_a %>%
    #   mutate(log_valor_boleto_eletronico = log1p(valor_boleto_eletronico),
    #          log_valor_boleto_presencial = log1p(valor_boleto-valor_boleto_eletronico),
    #          log_valor_boleto_dinheiro = log1p(valor_boleto_dinheiro),
    #          log_qtd_boleto_eletronico = log1p(qtd_boleto_eletronico),
    #          log_qtd_boleto_presencial = log1p(qtd_boleto-qtd_boleto_eletronico),
    #          log_qtd_boleto_dinheiro = log1p(qtd_boleto_dinheiro))
    # dat_b <- dat_b %>%
    #   mutate(log_valor_boleto_eletronico = log1p(valor_boleto_eletronico),
    #          log_valor_boleto_presencial = log1p(valor_boleto-valor_boleto_eletronico),
    #          log_valor_boleto_dinheiro = log1p(valor_boleto_dinheiro),
    #          log_qtd_boleto_eletronico = log1p(qtd_boleto_eletronico),
    #          log_qtd_boleto_presencial = log1p(qtd_boleto-qtd_boleto_eletronico),
    #          log_qtd_boleto_dinheiro = log1p(qtd_boleto_dinheiro))
    # 
    # print_twfe("Boleto_balanced_","log_qtd_boleto_eletronico","constant","constant","flood_risk5","Log Transactions Boleto - Eletronic",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    # print_twfe("Boleto_balanced_","log_qtd_boleto_presencial","constant","constant","flood_risk5","Log Transactions Boleto - In Person",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    # print_twfe("Boleto_balanced_","log_qtd_boleto_dinheiro","constant","constant","flood_risk5","Log Transactions Boleto - Money",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    # print_twfe("Boleto_balanced_","log_valor_boleto_eletronico","constant","constant","flood_risk5","Log Value Boleto - Eletronic",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    # print_twfe("Boleto_balanced_","log_valor_boleto_presencial","constant","constant","flood_risk5","Log Value Boleto - In Person",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    # print_twfe("Boleto_balanced_","log_valor_boleto_dinheiro","constant","constant","flood_risk5","Log Value Boleto - Money",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    # # + Uso da conta + "adocao"
    
    #Cartao 
    # * valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito 
    twfe2("Cartao_balanced_","log_valor_cartao_debito","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_balanced_","log_valor_cartao_debito","Log Value Debit Card", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    
    twfe2("Cartao_balanced_","log_valor_cartao_credito","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_balanced_","log_valor_cartao_credito","Log Value Credit Card", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    
    twfe2("Cartao_balanced_","log_qtd_cli_cartao_debito","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_balanced_","log_qtd_cli_cartao_debito","Log Quantity of Firms Accepting Debit Card", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    
    twfe2("Cartao_balanced_","log_qtd_cli_cartao_credito","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_balanced_","log_qtd_cli_cartao_credito","Log Quantity of Firms Accepting Credit Card", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    
    twfe2("Cartao_balanced_","log_valor_cartao","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_balanced_","log_valor_cartao","Log Value Cards", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    twfe2("Cartao_balanced_","log_qtd_cli_cartao","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_balanced_","log_qtd_cli_cartao","Log Quantity of Firms Accepting Cards", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    # + Adocao 
    rm(dat_a, dat_b)
    
    # _balanced_covid
    
    dat_a <- prepare_data("Base_week_muni.dta",flood_week_after_balanced_covid,mun_fe,mun_control)
    dat_b <- prepare_data("Base_week_muni.dta",flood_week_before_balanced_covid,mun_fe,mun_control)
    dat_a <- dat_a %>%
      mutate(log_valor_TED_intra = log1p(valor_TED_intra),
             log_qtd_TED_intra = log1p(qtd_TED_intra),
             log_qtd_cli_TED_rec_PJ = log1p(qtd_cli_TED_rec_PJ),
             log_qtd_cli_TED_pag_PJ = log1p(qtd_cli_TED_pag_PJ),
             log_valor_boleto = log1p(valor_boleto),
             log_qtd_boleto = log1p(qtd_boleto),
             log_qtd_cli_pag_pf_boleto = log1p(qtd_cli_pag_pf_boleto),
             log_qtd_cli_pag_pj_boleto = log1p(qtd_cli_pag_pj_boleto),
             log_qtd_cli_rec_pj_boleto = log1p(qtd_cli_rec_pj_boleto),
             log_valor_cartao_credito = log1p(valor_cartao_credito),
             log_valor_cartao_debito = log1p(valor_cartao_debito),
             log_qtd_cli_cartao_debito = log1p(qtd_cli_cartao_debito),
             log_qtd_cli_cartao_credito = log1p(qtd_cli_cartao_credito),
             log_qtd_cli_cartao = log1p(qtd_cli_cartao_debito+qtd_cli_cartao_credito),
             log_valor_cartao = log1p(valor_cartao_debito+valor_cartao_credito))
    dat_b <- dat_b %>%
      mutate(log_valor_TED_intra = log1p(valor_TED_intra),
             log_qtd_TED_intra = log1p(qtd_TED_intra),
             log_qtd_cli_TED_rec_PJ = log1p(qtd_cli_TED_rec_PJ),
             log_qtd_cli_TED_pag_PJ = log1p(qtd_cli_TED_pag_PJ),
             log_valor_boleto = log1p(valor_boleto),
             log_qtd_boleto = log1p(qtd_boleto),
             log_qtd_cli_pag_pf_boleto = log1p(qtd_cli_pag_pf_boleto),
             log_qtd_cli_pag_pj_boleto = log1p(qtd_cli_pag_pj_boleto),
             log_qtd_cli_rec_pj_boleto = log1p(qtd_cli_rec_pj_boleto),
             log_valor_cartao_credito = log1p(valor_cartao_credito),
             log_valor_cartao_debito = log1p(valor_cartao_debito),
             log_qtd_cli_cartao_debito = log1p(qtd_cli_cartao_debito),
             log_qtd_cli_cartao_credito = log1p(qtd_cli_cartao_credito),
             log_qtd_cli_cartao = log1p(qtd_cli_cartao_debito+qtd_cli_cartao_credito),
             log_valor_cartao = log1p(valor_cartao_debito+valor_cartao_credito))
    
    #TED
    twfe2("TED_balanced_covid_","log_valor_TED_intra","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("TED_balanced_covid_","log_valor_TED_intra","Log Value TED", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    twfe2("TED_balanced_covid_","log_qtd_TED_intra","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("TED_balanced_covid_","log_qtd_TED_intra","Log Transactions TED", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    twfe2("TED_balanced_covid_","log_qtd_cli_TED_rec_PJ","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("TED_balanced_covid_","log_qtd_cli_TED_rec_PJ","Log Quantity of Firms Receiving TED", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    twfe2("TED_balanced_covid_","log_qtd_cli_TED_pag_PJ","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("TED_balanced_covid_","log_qtd_cli_TED_pag_PJ","Log Quantity of Firms Sending TED", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    # + Uso da conta + "adocao"
    
    #Boleto
    twfe2("Boleto_balanced_covid_","log_valor_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_balanced_covid_","log_valor_boleto","Log Value Boleto", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    twfe2("Boleto_balanced_covid_","log_qtd_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_balanced_covid_","log_qtd_boleto","Log Transactions Boleto", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    twfe2("Boleto_balanced_covid_","log_qtd_cli_pag_pf_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_balanced_covid_","log_qtd_cli_pag_pf_boleto","Log Quantity of People Sending Boleto", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    twfe2("Boleto_balanced_covid_","log_qtd_cli_pag_pj_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_balanced_covid_","log_qtd_cli_pag_pj_boleto","Log Quantity of Firms Sending Boleto", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    twfe2("Boleto_balanced_covid_","log_qtd_cli_rec_pj_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_balanced_covid_","log_qtd_cli_rec_pj_boleto","Log Quantity of Firms Receiving Boleto", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    #Cartao
    twfe2("Cartao_balanced_covid_","log_valor_cartao_debito","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_balanced_covid_","log_valor_cartao_debito","Log Value Debit Card", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    twfe2("Cartao_balanced_covid_","log_valor_cartao_credito","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_balanced_covid_","log_valor_cartao_credito","Log Value Credit Card", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    twfe2("Cartao_balanced_covid_","log_qtd_cli_cartao_debito","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_balanced_covid_","log_qtd_cli_cartao_debito","Log Quantity of Firms Accepting Debit Card", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    twfe2("Cartao_balanced_covid_","log_qtd_cli_cartao_credito","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_balanced_covid_","log_qtd_cli_cartao_credito","Log Quantity of Firms Accepting Credit Card", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    twfe2("Cartao_balanced_covid_","log_valor_cartao","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_balanced_covid_","log_valor_cartao","Log Value Cards", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    twfe2("Cartao_balanced_covid_","log_qtd_cli_cartao","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_balanced_covid_","log_qtd_cli_cartao","Log Quantity of Firms Accepting Cards", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    # + Adocao
    rm(dat_a, dat_b)
    
  }, error = function(e) {
    print(paste("Error in Base_week_muni:", e))
  })
}
# Credito - Worked
if(run_Credito_old == 1){
  tryCatch({
    dat_a <- read_dta(file.path(dta_path,"Base_credito_muni_flood.dta"))
    setDT(dat_a)
    dat_a$time <- dat_a$time_id
    dat_a$month <- time_id_to_month(dat_a$time_id)
    dat_a$year <- time_id_to_year(dat_a$time_id)
    dat_a <- merge(dat_a, mun_fe, by="muni_cd", all.x = TRUE)
    dat_a <- merge(dat_a, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
    dat_a[, treat := ifelse(is.na(date_flood), 0, 1)]
    dat_a[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
    dat_a[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
    #xlimits <- seq(ceiling(-6*1.333),ceiling(12*1.333),by=1)
    #dat_a <- subset(dat_a,time_to_treat %in% xlimits)
    
    dat_b <- read_dta(file.path(dta_path,"Base_credito_muni_flood_beforePIX.dta"))
    setDT(dat_b)
    dat_b$time <- dat_b$time_id
    dat_b$month <- time_id_to_month(dat_b$time_id)
    dat_b$year <- time_id_to_year(dat_b$time_id)
    dat_b <- merge(dat_b, mun_fe, by="muni_cd", all.x = TRUE)
    dat_b <- merge(dat_b, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
    dat_b[, treat := ifelse(is.na(date_flood), 0, 1)]
    dat_b[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
    dat_b[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
    #xlimits <- seq(ceiling(-6*1.333),ceiling(12*1.333),by=1)
    #dat_b <- subset(dat_b,time_to_treat %in% xlimits)
    
    twfe2("Credito_","log_vol_cartao","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_","log_vol_cartao","Log Credit Card Balance", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Credito_","log_qtd_cli_cartao","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_","log_qtd_cli_cartao","Log Quantity of Credit Cards Users", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Credito_","log_vol_emprestimo_pessoal","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_","log_vol_emprestimo_pessoal","Log Volume Personal Loan", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Credito_","log_qtd_cli_emp_pessoal","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_","log_qtd_cli_emp_pessoal","Log Quantity Personal Loan", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Credito_","log_vol_credito_total","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_","log_vol_credito_total","Log Volume Loan", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Credito_","log_qtd_cli_total","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_","log_qtd_cli_total","Log Quantity Loan", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Credito_","log_vol_credito_total_PF","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_","log_vol_credito_total_PF","Log Volume Loan", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Credito_","log_qtd_cli_total_PF","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_","log_qtd_cli_total_PF","Log Quantity Loan", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Credito_","log_vol_credito_total_PJ","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_","log_vol_credito_total_PJ","Log Volume Loan", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Credito_","log_qtd_cli_total_PJ","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_","log_qtd_cli_total_PJ","Log Quantity Loan", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    rm(dat_a, dat_b)
    # variables: log_vol_cartao, log_qtd_cli_cartao, log_vol_emprestimo_pessoal, log_qtd_cli_emp_pessoal, log_vol_credito_total, log_qtd_cli_total, log_vol_credito_total_PF, log_qtd_cli_total_PF, log_vol_credito_total_PJ, log_qtd_cli_total_PJ
    # _balanced
    
    dat_a <- read_dta(file.path(dta_path,"Base_credito_muni_flood.dta"))
    setDT(dat_a)
    dat_a$time <- dat_a$time_id
    dat_a$month <- time_id_to_month(dat_a$time_id)
    dat_a$year <- time_id_to_year(dat_a$time_id)
    dat_a <- merge(dat_a, mun_fe, by="muni_cd", all.x = TRUE)
    dat_a <- merge(dat_a, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
    # I need to delete the flood info (date_flood) and upload the new flood balanced. 
    # flood_month_after_balanced
    dat_a <- dat_a %>% select(-date_flood)
    dat_a <- merge(dat_a, flood_month_after_balanced, by=c("muni_cd","time"), all=FALSE) 
    #
    dat_a[, treat := ifelse(is.na(date_flood), 0, 1)]
    dat_a[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
    dat_a[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
    #xlimits <- seq(ceiling(-6*1.333),ceiling(12*1.333),by=1)
    #dat_a <- subset(dat_a,time_to_treat %in% xlimits)
    
    dat_b <- read_dta(file.path(dta_path,"Base_credito_muni_flood_beforePIX.dta"))
    setDT(dat_b)
    dat_b$time <- dat_b$time_id
    dat_b$month <- time_id_to_month(dat_b$time_id)
    dat_b$year <- time_id_to_year(dat_b$time_id)
    dat_b <- merge(dat_b, mun_fe, by="muni_cd", all.x = TRUE)
    dat_b <- merge(dat_b, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
    dat_b <- dat_b %>% select(-date_flood)
    dat_b <- merge(dat_b, flood_month_before_balanced2019, by=c("muni_cd","time"), all=FALSE) 
    dat_b[, treat := ifelse(is.na(date_flood), 0, 1)]
    dat_b[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
    dat_b[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
    #xlimits <- seq(ceiling(-6*1.333),ceiling(12*1.333),by=1)
    #dat_b <- subset(dat_b,time_to_treat %in% xlimits)
    
    twfe2("Credito_balanced_","log_vol_cartao","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_balanced_","log_vol_cartao","Log Credit Card Balance", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Credito_balanced_","log_qtd_cli_cartao","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_balanced_","log_qtd_cli_cartao","Log Quantity of Credit Cards Users", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Credito_balanced_","log_vol_emprestimo_pessoal","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_balanced_","log_vol_emprestimo_pessoal","Log Volume Personal Loan", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Credito_balanced_","log_qtd_cli_emp_pessoal","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_balanced_","log_qtd_cli_emp_pessoal","Log Quantity Personal Loan", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Credito_balanced_","log_vol_credito_total","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_balanced_","log_vol_credito_total","Log Volume Loan", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Credito_balanced_","log_qtd_cli_total","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_balanced_","log_qtd_cli_total","Log Quantity Loan", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Credito_balanced_","log_vol_credito_total_PF","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_balanced_","log_vol_credito_total_PF","Log Volume Loan", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Credito_balanced_","log_qtd_cli_total_PF","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_balanced_","log_qtd_cli_total_PF","Log Quantity Loan", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Credito_balanced_","log_vol_credito_total_PJ","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_balanced_","log_vol_credito_total_PJ","Log Volume Loan", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Credito_balanced_","log_qtd_cli_total_PJ","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_balanced_","log_qtd_cli_total_PJ","Log Quantity Loan", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    rm(dat_a, dat_b)
    
    # _balanced_covid
    
    dat_a <- read_dta(file.path(dta_path,"Base_credito_muni_flood.dta"))
    setDT(dat_a)
    dat_a$time <- dat_a$time_id
    dat_a$month <- time_id_to_month(dat_a$time_id)
    dat_a$year <- time_id_to_year(dat_a$time_id)
    dat_a <- merge(dat_a, mun_fe, by="muni_cd", all.x = TRUE)
    dat_a <- merge(dat_a, mun_control, by=c("muni_cd","month","year"), all.x = TRUE)
    dat_a <- dat_a %>% select(-date_flood)
    dat_a <- merge(dat_a, flood_month_after_balanced_covid, by=c("muni_cd","time"), all=FALSE)
    dat_a[, treat := ifelse(is.na(date_flood), 0, 1)]
    dat_a[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
    dat_a[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
    #xlimits <- seq(ceiling(-6*1.333),ceiling(12*1.333),by=1)
    #dat_a <- subset(dat_a,time_to_treat %in% xlimits)
    
    dat_b <- read_dta(file.path(dta_path,"Base_credito_muni_flood_beforePIX.dta"))
    setDT(dat_b)
    dat_b$time <- dat_b$time_id
    dat_b$month <- time_id_to_month(dat_b$time_id)
    dat_b$year <- time_id_to_year(dat_b$time_id)
    dat_b <- merge(dat_b, mun_fe, by="muni_cd", all.x = TRUE)
    dat_b <- merge(dat_b, mun_control, by=c("muni_cd","month","year"), all.x = TRUE)
    dat_b <- dat_b %>% select(-date_flood)
    dat_b <- merge(dat_b, flood_month_before_balanced_covid, by=c("muni_cd","time"), all=FALSE)
    dat_b[, treat := ifelse(is.na(date_flood), 0, 1)]
    dat_b[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
    dat_b[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
    #xlimits <- seq(ceiling(-6*1.333),ceiling(12*1.333),by=1)
    #dat_b <- subset(dat_b,time_to_treat %in% xlimits)
    
    twfe2("Credito_balanced_covid_","log_vol_cartao","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_balanced_covid_","log_vol_cartao","Log Credit Card Balance", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), -3, 3)
    
    twfe2("Credito_balanced_covid_","log_qtd_cli_cartao","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_balanced_covid_","log_qtd_cli_cartao","Log Quantity of Credit Cards Users", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), -3, 3)
    
    twfe2("Credito_balanced_covid_","log_vol_emprestimo_pessoal","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_balanced_covid_","log_vol_emprestimo_pessoal","Log Volume Personal Loan", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), -3, 3)
    
    twfe2("Credito_balanced_covid_","log_qtd_cli_emp_pessoal","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_balanced_covid_","log_qtd_cli_emp_pessoal","Log Quantity Personal Loan", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), -3, 3)
    
    twfe2("Credito_balanced_covid_","log_vol_credito_total","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_balanced_covid_","log_vol_credito_total","Log Volume Loan", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), -3, 3)
    
    twfe2("Credito_balanced_covid_","log_qtd_cli_total","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_balanced_covid_","log_qtd_cli_total","Log Quantity Loan", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), -3, 3)
    
    twfe2("Credito_balanced_covid_","log_vol_credito_total_PF","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_balanced_covid_","log_vol_credito_total_PF","Log Volume Loan", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), -3, 3)
    
    twfe2("Credito_balanced_covid_","log_qtd_cli_total_PF","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_balanced_covid_","log_qtd_cli_total_PF","Log Quantity Loan", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), -3, 3)
    
    twfe2("Credito_balanced_covid_","log_vol_credito_total_PJ","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_balanced_covid_","log_vol_credito_total_PJ","Log Volume Loan", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), -3, 3)
    
    twfe2("Credito_balanced_covid_","log_qtd_cli_total_PJ","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_month("Credito_balanced_covid_","log_qtd_cli_total_PJ","Log Quantity Loan", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), -3, 3)
    
    rm(dat_a, dat_b)
    
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in Credito:", e))
  })
}

# Pix_individual
if(run_Pix_individual == 1){
  tryCatch({
    dat_a <- read_dta(file.path(dta_path,"Pix_individuo_sample_flood.dta"))
    setDT(dat_a)
    dat_a$time <- dat_a$time_id
    dat_a$month <- time_id_to_month(dat_a$time_id)
    dat_a$year <- time_id_to_year(dat_a$time_id)
    dat_a <- merge(dat_a, mun_fe, by="muni_cd", all.x = TRUE)
    dat_a <- merge(dat_a, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
    dat_a[, treat := ifelse(is.na(date_flood), 0, 1)]
    dat_a[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
    dat_a[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
    xlimits <- seq(ceiling(-6*1.333),ceiling(12*1.333),by=1)
    dat_a <- subset(dat,time_to_treat %in% xlimits)
    
    dat_rec <- dat_a %>%
      rename(adoption=after_first_pix_rec,
             use = receiver,
             log_trans = log_trans_rec,
             log_valor = log_value_rec)
    dat_sent <- dat_a %>%
      rename(adoption=after_first_pix_sent,
             use = sender,
             log_trans = log_trans_sent,
             log_valor = log_value_sent)
    dat_self<- dat_a %>%
      rename(use = user,
             log_trans = log_trans_self,
             log_valor = log_value_self)
    
    
    twfe2("Pix_PF_","adoption","constant","constant","flood_risk5",list(dat_rec,dat_sent), c("rec","send"))
    print_twfe_month("Pix_PF_","adoption","Adoption", c("rec","send"), c("Receivers","Senders"), -6, 12)
    
    twfe2("Pix_PF_","use","constant","constant","flood_risk5",list(dat_rec,dat_sent), c("rec","send"))
    print_twfe_month("Pix_PF_","use","Active Use", c("rec","send"), c("Receivers","Senders"), -6, 12)
    
    twfe2("Pix_PF_", "log_trans","constant","constant","flood_risk5",list(dat_rec, dat_sent), c("rec","send"))
    print_twfe_month("Pix_PF_","log_trans","Log Transactions", c("rec","send"), c("Received","Sent"), -6, 12)
    
    twfe2("Pix_PF_", "log_valor","constant","constant","flood_risk5",list(dat_rec, dat_sent), c("rec","send"))
    print_twfe_month("Pix_PF_","log_valor","Log Value", c("rec","send"), c("Received","Sent"), -6, 12)
    
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in Pix_individual:", e))
  })
}

# ESTBAN - Worked
if(run_Estban == 1){
  tryCatch({
    dat_a <- read_dta(file.path(dta_path,"Estban_detalhado_flood_collapsed.dta"))
    setDT(dat_a)
    dat_a$time <- dat_a$time_id
    dat_a$month <- time_id_to_month(dat_a$time_id)
    dat_a$year <- time_id_to_year(dat_a$time_id)
    dat_a <- merge(dat_a, mun_fe, by="muni_cd", all.x = TRUE)
    dat_a <- merge(dat_a, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
    dat_a[, treat := ifelse(is.na(date_flood), 0, 1)]
    dat_a[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
    dat_a[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
    #xlimits <- seq(ceiling(-9*1.333),ceiling(12*1.333),by=1)
    #dat_a <- subset(dat,time_to_treat %in% xlimits)
    dat_a_large <- subset(dat_a, large_bank %in% c(1))
    dat_a_small <- dat_a[large_bank %in% c(0)]
    
    dat_a2 <- read_dta(file.path(dta_path,"Estban_detalhado_flood_collapsed2.dta")) 
    setDT(dat_a2)
    dat_a2$time <- dat_a2$time_id
    dat_a2$month <- time_id_to_month(dat_a2$time_id)
    dat_a2$year <- time_id_to_year(dat_a2$time_id)
    dat_a2 <- merge(dat_a2, mun_fe, by="muni_cd", all.x = TRUE)
    dat_a2 <- merge(dat_a2, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
    dat_a2[, treat := ifelse(is.na(date_flood), 0, 1)]
    dat_a2[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
    dat_a2[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
    #xlimits <- seq(ceiling(-9*1.333),ceiling(12*1.333),by=1)
    #dat_a2 <- subset(dat,time_to_treat %in% xlimits)
    
    dat_b <- read_dta(file.path(dta_path,"Estban_detalhado_flood_beforePIX_collapsed.dta"))
    setDT(dat_b)
    dat_b$time <- dat_b$time_id
    dat_b$month <- time_id_to_month(dat_b$time_id)
    dat_b$year <- time_id_to_year(dat_b$time_id)
    dat_b <- merge(dat_b, mun_fe, by="muni_cd", all.x = TRUE)
    dat_b <- merge(dat_b, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
    dat_b[, treat := ifelse(is.na(date_flood), 0, 1)]
    dat_b[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
    dat_b[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
    #xlimits <- seq(ceiling(-6*1.333),ceiling(12*1.333),by=1)
    #dat_b <- subset(dat,time_to_treat %in% xlimits)
    dat_b_large <- subset(dat_b, large_bank %in% c(1))
    dat_b_small <- dat_b[large_bank %in% c(0)]
    
    dat_b2 <- read_dta(file.path(dta_path,"Estban_detalhado_flood_beforePIX_collapsed.dta"))
    setDT(dat_b2)
    dat_b2$time <- dat_b2$time_id
    dat_b2$month <- time_id_to_month(dat_b2$time_id)
    dat_b2$year <- time_id_to_year(dat_b2$time_id)
    dat_b2 <- merge(dat_b2, mun_fe, by="muni_cd", all.x = TRUE)
    dat_b2 <- merge(dat_b2, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
    dat_b2[, treat := ifelse(is.na(date_flood), 0, 1)]
    dat_b2[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
    dat_b2[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
    #xlimits <- seq(ceiling(-6*1.333),ceiling(12*1.333),by=1)
    #dat_b2 <- subset(dat,time_to_treat %in% xlimits)
    
    #log_caixa, log_total_deposits, log_poupanca, log_dep_prazo, log_dep_vista_PF, log_dep_vista_PJ
    # Large vs Small
    twfe2("Estban_","log_caixa","constant","constant","flood_risk5",list(dat_a_large,dat_a_small), c("large","small"))
    print_twfe_month("Estban_","log_caixa","Log Monetary Inventory", c("large","small"), c("Top 5 Bank","Others"), -6, 12)
    
    twfe2("Estban_","log_total_deposits","constant","constant","flood_risk5",list(dat_a_large,dat_a_small), c("large","small"))
    print_twfe_month("Estban_","log_total_deposits","Log Total Deposits", c("large","small"), c("Top 5 Bank","Others"), -6, 12)
    
    twfe2("Estban_","log_poupanca","constant","constant","flood_risk5",list(dat_a_large,dat_a_small), c("large","small"))
    print_twfe_month("Estban_","log_poupanca","Log Savings Account", c("large","small"), c("Top 5 Bank","Others"), -6, 12)
    
    twfe2("Estban_","log_dep_prazo","constant","constant","flood_risk5",list(dat_a_large,dat_a_small), c("large","small"))
    print_twfe_month("Estban_","log_dep_prazo","Log Time Deposit", c("large","small"), c("Top 5 Bank","Others"), -6, 12)
    
    twfe2("Estban_","log_dep_vista_PF","constant","constant","flood_risk5",list(dat_a_large,dat_a_small), c("large","small"))
    print_twfe_month("Estban_","log_dep_vista_PF","Log Deposits", c("large","small"), c("Top 5 Bank","Others"), -6, 12)
    
    twfe2("Estban_","log_dep_vista_PJ","constant","constant","flood_risk5",list(dat_a_large,dat_a_small), c("large","small"))
    print_twfe_month("Estban_","log_dep_vista_PJ","Log Deposits", c("large","small"), c("Top 5 Bank","Others"), -6, 12)
    
    # Before vs After
    twfe2("Estban2_","log_caixa","constant","constant","flood_risk5",list(dat_b2,dat_a2), c("before","after"))
    print_twfe_month("Estban2_","log_caixa","Log Monetary Inventory", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Estban2_","log_total_deposits","constant","constant","flood_risk5",list(dat_b2,dat_a2), c("before","after"))
    print_twfe_month("Estban2_","log_total_deposits","Log Total Deposits", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Estban2_","log_poupanca","constant","constant","flood_risk5",list(dat_b2,dat_a2), c("before","after"))
    print_twfe_month("Estban2_","log_poupanca","Log Savings Account", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Estban2_","log_dep_prazo","constant","constant","flood_risk5",list(dat_b2,dat_a2), c("before","after"))
    print_twfe_month("Estban2_","log_dep_prazo","Log Time Deposit", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Estban2_","log_dep_vista_PF","constant","constant","flood_risk5",list(dat_b2,dat_a2), c("before","after"))
    print_twfe_month("Estban2_","log_dep_vista_PF","Log Deposits", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Estban2_","log_dep_vista_PJ","constant","constant","flood_risk5",list(dat_b2,dat_a2), c("before","after"))
    print_twfe_month("Estban2_","log_dep_vista_PJ","Log Deposits", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    rm(dat_a,dat_a2,dat_b,dat_b2)
    
    # _balanced
    
    # _balanced
    dat_a <- read_dta(file.path(dta_path,"Estban_detalhado_flood_collapsed.dta"))
    setDT(dat_a)
    dat_a$time <- dat_a$time_id
    dat_a$month <- time_id_to_month(dat_a$time_id)
    dat_a$year <- time_id_to_year(dat_a$time_id)
    dat_a <- merge(dat_a, mun_fe, by="muni_cd", all.x = TRUE)
    dat_a <- merge(dat_a, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
    # I need to delete the flood info (date_flood) and upload the new flood balanced. 
    # flood_month_after_balanced
    dat_a <- dat_a %>% select(-date_flood)
    dat_a <- merge(dat_a, flood_month_after_balanced, by=c("muni_cd","time"), all=FALSE) 
    
    dat_a[, treat := ifelse(is.na(date_flood), 0, 1)]
    dat_a[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
    dat_a[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
    #xlimits <- seq(ceiling(-9*1.333),ceiling(12*1.333),by=1)
    #dat_a <- subset(dat,time_to_treat %in% xlimits)
    dat_a_large <- subset(dat_a, large_bank %in% c(1))
    dat_a_small <- dat_a[large_bank %in% c(0)]
    
    dat_a2 <- read_dta(file.path(dta_path,"Estban_detalhado_flood_collapsed2.dta")) 
    setDT(dat_a2)
    dat_a2$time <- dat_a2$time_id
    dat_a2$month <- time_id_to_month(dat_a2$time_id)
    dat_a2$year <- time_id_to_year(dat_a2$time_id)
    dat_a2 <- merge(dat_a2, mun_fe, by="muni_cd", all.x = TRUE)
    dat_a2 <- merge(dat_a2, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
    # I need to delete the flood info (date_flood) and upload the new flood balanced. 
    # flood_month_after_balanced
    dat_a2 <- dat_a2 %>% select(-date_flood)
    dat_a2 <- merge(dat_a2, flood_month_after_balanced, by=c("muni_cd","time"), all=FALSE)
    
    dat_a2[, treat := ifelse(is.na(date_flood), 0, 1)]
    dat_a2[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
    dat_a2[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
    
    dat_b <- read_dta(file.path(dta_path,"Estban_detalhado_flood_beforePIX_collapsed.dta"))
    setDT(dat_b)
    dat_b$time <- dat_b$time_id
    dat_b$month <- time_id_to_month(dat_b$time_id)
    dat_b$year <- time_id_to_year(dat_b$time_id)
    dat_b <- merge(dat_b, mun_fe, by="muni_cd", all.x = TRUE)
    dat_b <- merge(dat_b, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
    dat_b <- dat_b %>% select(-date_flood)
    dat_b <- merge(dat_b, flood_month_before_balanced2019, by=c("muni_cd","time"), all=FALSE)
    dat_b[, treat := ifelse(is.na(date_flood), 0, 1)]
    dat_b[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
    dat_b[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
    #xlimits <- seq(ceiling(-6*1.333),ceiling(12*1.333),by=1)
    #dat_b <- subset(dat,time_to_treat %in% xlimits)
    dat_b_large <- subset(dat_b, large_bank %in% c(1))
    dat_b_small <- dat_b[large_bank %in% c(0)]
    
    dat_b2 <- read_dta(file.path(dta_path,"Estban_detalhado_flood_beforePIX_collapsed2.dta"))
    setDT(dat_b2)
    dat_b2$time <- dat_b2$time_id
    dat_b2$month <- time_id_to_month(dat_b2$time_id)
    dat_b2$year <- time_id_to_year(dat_b2$time_id)
    dat_b2 <- merge(dat_b2, mun_fe, by="muni_cd", all.x = TRUE)
    dat_b2 <- merge(dat_b2, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
    dat_b2 <- dat_b2 %>% select(-date_flood)
    dat_b2 <- merge(dat_b2, flood_month_before_balanced2019, by=c("muni_cd","time"), all=FALSE)
    dat_b2[, treat := ifelse(is.na(date_flood), 0, 1)]
    dat_b2[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
    dat_b2[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
    #xlimits <- seq(ceiling(-6*1.333),ceiling(12*1.333),by=1)
    #dat_b2 <- subset(dat,time_to_treat %in% xlimits)
    
    #log_caixa, log_total_deposits, log_poupanca, log_dep_prazo, log_dep_vista_PF, log_dep_vista_PJ
    # Large vs Small
    twfe2("Estban_balanced_","log_caixa","constant","constant","flood_risk5",list(dat_a_large,dat_a_small), c("large","small"))
    print_twfe_month("Estban_balanced_","log_caixa","Log Monetary Inventory", c("large","small"), c("Top 5 Bank","Others"), -6, 12)
    
    twfe2("Estban_balanced_","log_total_deposits","constant","constant","flood_risk5",list(dat_a_large,dat_a_small), c("large","small"))
    print_twfe_month("Estban_balanced_","log_total_deposits","Log Total Deposits", c("large","small"), c("Top 5 Bank","Others"), -6, 12)
    
    twfe2("Estban_balanced_","log_poupanca","constant","constant","flood_risk5",list(dat_a_large,dat_a_small), c("large","small"))
    print_twfe_month("Estban_balanced_","log_poupanca","Log Savings Account", c("large","small"), c("Top 5 Bank","Others"), -6, 12)
    
    twfe2("Estban_balanced_","log_dep_prazo","constant","constant","flood_risk5",list(dat_a_large,dat_a_small), c("large","small"))
    print_twfe_month("Estban_balanced_","log_dep_prazo","Log Time Deposit", c("large","small"), c("Top 5 Bank","Others"), -6, 12)
    
    twfe2("Estban_balanced_","log_dep_vista_PF","constant","constant","flood_risk5",list(dat_a_large,dat_a_small), c("large","small"))
    print_twfe_month("Estban_balanced_","log_dep_vista_PF","Log Deposits", c("large","small"), c("Top 5 Bank","Others"), -6, 12)
    
    twfe2("Estban_balanced_","log_dep_vista_PJ","constant","constant","flood_risk5",list(dat_a_large,dat_a_small), c("large","small"))
    print_twfe_month("Estban_balanced_","log_dep_vista_PJ","Log Deposits", c("large","small"), c("Top 5 Bank","Others"), -6, 12)
    
    # Before vs After
    twfe2("Estban2_balanced_","log_caixa","constant","constant","flood_risk5",list(dat_b2,dat_a2), c("before","after"))
    print_twfe_month("Estban2_balanced_","log_caixa","Log Monetary Inventory", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Estban2_balanced_","log_total_deposits","constant","constant","flood_risk5",list(dat_b2,dat_a2), c("before","after"))
    print_twfe_month("Estban2_balanced_","log_total_deposits","Log Total Deposits", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Estban2_balanced_","log_poupanca","constant","constant","flood_risk5",list(dat_b2,dat_a2), c("before","after"))
    print_twfe_month("Estban2_balanced_","log_poupanca","Log Savings Account", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Estban2_balanced_","log_dep_prazo","constant","constant","flood_risk5",list(dat_b2,dat_a2), c("before","after"))
    print_twfe_month("Estban2_balanced_","log_dep_prazo","Log Time Deposit", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Estban2_balanced_","log_dep_vista_PF","constant","constant","flood_risk5",list(dat_b2,dat_a2), c("before","after"))
    print_twfe_month("Estban2_balanced_","log_dep_vista_PF","Log Deposits", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    twfe2("Estban2_balanced_","log_dep_vista_PJ","constant","constant","flood_risk5",list(dat_b2,dat_a2), c("before","after"))
    print_twfe_month("Estban2_balanced_","log_dep_vista_PJ","Log Deposits", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
    
    rm(dat_a,dat_a2,dat_b,dat_b2)
    
    # _balanced_covid
    dat_a <- read_dta(file.path(dta_path,"Estban_detalhado_flood_collapsed.dta"))
    setDT(dat_a)
    dat_a$time <- dat_a$time_id
    dat_a$month <- time_id_to_month(dat_a$time_id)
    dat_a$year <- time_id_to_year(dat_a$time_id)
    dat_a <- merge(dat_a, mun_fe, by="muni_cd", all.x = TRUE)
    dat_a <- merge(dat_a, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
    # I need to delete the flood info (date_flood) and upload the new flood balanced. 
    # flood_month_after_balanced
    dat_a <- dat_a %>% select(-date_flood)
    dat_a <- merge(dat_a, flood_month_after_balanced_covid, by=c("muni_cd","time"), all=FALSE)
    dat_a[, treat := ifelse(is.na(date_flood), 0, 1)]
    dat_a[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
    dat_a[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
    #xlimits <- seq(ceiling(-9*1.333),ceiling(12*1.333),by=1)
    #dat_a <- subset(dat,time_to_treat %in% xlimits)
    dat_a_large <- subset(dat_a, large_bank %in% c(1))
    dat_a_small <- dat_a[large_bank %in% c(0)]
    
    
    dat_a2 <- read_dta(file.path(dta_path,"Estban_detalhado_flood_collapsed2.dta"))
    setDT(dat_a2)
    dat_a2$time <- dat_a2$time_id
    dat_a2$month <- time_id_to_month(dat_a2$time_id)
    dat_a2$year <- time_id_to_year(dat_a2$time_id)
    dat_a2 <- merge(dat_a2, mun_fe, by="muni_cd", all.x = TRUE)
    dat_a2 <- merge(dat_a2, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
    # I need to delete the flood info (date_flood) and upload the new flood balanced. 
    # flood_month_after_balanced
    dat_a2 <- dat_a2 %>% select(-date_flood)
    dat_a2 <- merge(dat_a2, flood_month_after_balanced_covid, by=c("muni_cd","time"), all=FALSE)
    dat_a2[, treat := ifelse(is.na(date_flood), 0, 1)]
    dat_a2[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
    dat_a2[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
    
    dat_b <- read_dta(file.path(dta_path,"Estban_detalhado_flood_beforePIX_collapsed.dta"))
    setDT(dat_b)
    dat_b$time <- dat_b$time_id
    dat_b$month <- time_id_to_month(dat_b$time_id)
    dat_b$year <- time_id_to_year(dat_b$time_id)
    dat_b <- merge(dat_b, mun_fe, by="muni_cd", all.x = TRUE)
    dat_b <- merge(dat_b, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
    dat_b <- dat_b %>% select(-date_flood)
    dat_b <- merge(dat_b, flood_month_before_balanced_covid, by=c("muni_cd","time"), all=FALSE)
    dat_b[, treat := ifelse(is.na(date_flood), 0, 1)]
    dat_b[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
    dat_b[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
    #xlimits <- seq(ceiling(-6*1.333),ceiling(12*1.333),by=1)
    #dat_b <- subset(dat,time_to_treat %in% xlimits)
    dat_b_large <- subset(dat_b, large_bank %in% c(1))
    dat_b_small <- dat_b[large_bank %in% c(0)]
    
    dat_b2 <- read_dta(file.path(dta_path,"Estban_detalhado_flood_beforePIX_collapsed2.dta"))
    setDT(dat_b2)
    dat_b2$time <- dat_b2$time_id
    dat_b2$month <- time_id_to_month(dat_b2$time_id)
    dat_b2$year <- time_id_to_year(dat_b2$time_id)
    dat_b2 <- merge(dat_b2, mun_fe, by="muni_cd", all.x = TRUE)
    dat_b2 <- merge(dat_b2, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
    dat_b2 <- dat_b2 %>% select(-date_flood)
    dat_b2 <- merge(dat_b2, flood_month_before_balanced_covid, by=c("muni_cd","time"), all=FALSE)
    dat_b2[, treat := ifelse(is.na(date_flood), 0, 1)]
    dat_b2[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
    dat_b2[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
    #xlimits <- seq(ceiling(-6*1.333),ceiling(12*1.333),by=1)
    #dat_b2 <- subset(dat,time_to_treat %in% xlimits)
    
    #log_caixa, log_total_deposits, log_poupanca, log_dep_prazo, log_dep_vista_PF, log_dep_vista_PJ
    # Large vs Small
    twfe2("Estban_balanced_covid_","log_caixa","constant","constant","flood_risk5",list(dat_a_large,dat_a_small), c("large","small"))
    print_twfe_month("Estban_balanced_covid_","log_caixa","Log Monetary Inventory", c("large","small"), c("Top 5 Bank","Others"), -3, 3)
    
    twfe2("Estban_balanced_covid_","log_total_deposits","constant","constant","flood_risk5",list(dat_a_large,dat_a_small), c("large","small"))
    print_twfe_month("Estban_balanced_covid_","log_total_deposits","Log Total Deposits", c("large","small"), c("Top 5 Bank","Others"), -3, 3)
    
    twfe2("Estban_balanced_covid_","log_poupanca","constant","constant","flood_risk5",list(dat_a_large,dat_a_small), c("large","small"))
    print_twfe_month("Estban_balanced_covid_","log_poupanca","Log Savings Account", c("large","small"), c("Top 5 Bank","Others"), -3, 3)
    
    twfe2("Estban_balanced_covid_","log_dep_prazo","constant","constant","flood_risk5",list(dat_a_large,dat_a_small), c("large","small"))
    print_twfe_month("Estban_balanced_covid_","log_dep_prazo","Log Time Deposit", c("large","small"), c("Top 5 Bank","Others"), -3, 3)
    
    twfe2("Estban_balanced_covid_","log_dep_vista_PF","constant","constant","flood_risk5",list(dat_a_large,dat_a_small), c("large","small"))
    print_twfe_month("Estban_balanced_covid_","log_dep_vista_PF","Log Deposits", c("large","small"), c("Top 5 Bank","Others"), -3, 3)
    
    twfe2("Estban_balanced_covid_","log_dep_vista_PJ","constant","constant","flood_risk5",list(dat_a_large,dat_a_small), c("large","small"))
    print_twfe_month("Estban_balanced_covid_","log_dep_vista_PJ","Log Deposits", c("large","small"), c("Top 5 Bank","Others"), -3, 3)
    
    # Before vs After
    twfe2("Estban2_balanced_covid_","log_caixa","constant","constant","flood_risk5",list(dat_b2,dat_a2), c("before","after"))
    print_twfe_month("Estban2_balanced_covid_","log_caixa","Log Monetary Inventory", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), -3, 3)
    
    twfe2("Estban2_balanced_covid_","log_total_deposits","constant","constant","flood_risk5",list(dat_b2,dat_a2), c("before","after"))
    print_twfe_month("Estban2_balanced_covid_","log_total_deposits","Log Total Deposits", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), -3, 3)
    
    twfe2("Estban2_balanced_covid_","log_poupanca","constant","constant","flood_risk5",list(dat_b2,dat_a2), c("before","after"))
    print_twfe_month("Estban2_balanced_covid_","log_poupanca","Log Savings Account", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), -3, 3)
    
    twfe2("Estban2_balanced_covid_","log_dep_prazo","constant","constant","flood_risk5",list(dat_b2,dat_a2), c("before","after"))
    print_twfe_month("Estban2_balanced_covid_","log_dep_prazo","Log Time Deposit", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), -3, 3)
    
    twfe2("Estban2_balanced_covid_","log_dep_vista_PF","constant","constant","flood_risk5",list(dat_b2,dat_a2), c("before","after"))
    print_twfe_month("Estban2_balanced_covid_","log_dep_vista_PF","Log Deposits", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), -3, 3)
    
    twfe2("Estban2_balanced_covid_","log_dep_vista_PJ","constant","constant","flood_risk5",list(dat_b2,dat_a2), c("before","after"))
    print_twfe_month("Estban2_balanced_covid_","log_dep_vista_PJ","Log Deposits", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), -3, 3)
    
    rm(dat_a,dat_a2,dat_b,dat_b2)
    
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in ESTBAN:", e))
  })
}

# RAIS


#-------------------------------------------------------------------------------
# TED + Boleto
#-------------------------------------------------------------------------------
## Should have users!
# ted novo + boleto novo
# Before and After!
# divide tipo 1 and tipo 2?
if(run_TED_boleto == 1){
    #Send and Rec
    variables <- c("ltrans","lvalor")
    variables_labels <- c("Log Transactions","Log Value")
    tryCatch({
      for(i in 1:3){
        if(i==1){
          flood_a <- flood_week_after
          flood_b <- flood_week_before2019
          xll <- xl
          xuu <- xu
          ending <- ""
          beginning <- paste0("Ted_boleto_", ending)
          legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
        }
        if(i==2){
          flood_a <- flood_week_after_balanced
          flood_b <- flood_week_before_balanced2019
          xll <- xl_balanced
          xuu <- xu_balanced
          ending <- "balanced_"
          beginning <- paste0("Ted_boleto_", ending)
          legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
        }
        if(i==3){
          flood_a <- flood_week_after_balanced_covid
          flood_b <- flood_week_before_balanced_covid
          xll <- xl_balanced_covid
          xuu <- xu_balanced_covid
          ending <- "balanced_covid_"
          beginning <- paste0("Ted_boleto_", ending)
          legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
        }
        
        dat_after <- prepare_data("Boleto.dta",flood_a,mun_fe,mun_control)
        dat_before <- prepare_data("Boleto.dta",flood_b,mun_fe,mun_control)
        dat_after0 <- dat_after %>% 
          group_by(across(-c(tipo,
                          valor_send, lvalor_send, trans_send, ltrans_send, senders, lsenders,
                          valor_rec, lvalor_rec, trans_rec, ltrans_rec, receivers, lreceivers))) %>%
          summarize(valor = sum(valor_send, na.rm = TRUE) + sum(valor_rec, na.rm = TRUE),
                    trans = sum(trans_send, na.rm = TRUE) + sum(trans_rec, na.rm = TRUE)) %>% ungroup()
        dat_before0 <- dat_before %>% 
          group_by(across(-c(tipo, 
                          valor_send, lvalor_send, trans_send, ltrans_send, senders, lsenders,
                          valor_rec, lvalor_rec, trans_rec, ltrans_rec, receivers, lreceivers))) %>%
          summarize(valor = sum(valor_send, na.rm = TRUE) + sum(valor_rec, na.rm = TRUE),
                    trans = sum(trans_send, na.rm = TRUE) + sum(trans_rec, na.rm = TRUE)) %>% ungroup()
        
        dat_rec <- prepare_data("TED_SITRAF_aggreg_rec.dta",flood_a,mun_fe,mun_control)
        dat_send <- prepare_data("TED_SITRAF_aggreg_send.dta",flood_a,mun_fe,mun_control)
        dat_combined0 <- bind_rows(dat_send, dat_rec) %>% group_by(across(-c(trans, valor, ltrans, lvalor, tipo))) %>%
          summarize(trans = sum(trans, na.rm = TRUE),
                    valor = sum(valor, na.rm = TRUE)) %>% ungroup()
        dat_rec_b <- prepare_data("TED_SITRAF_aggreg_rec.dta",flood_b,mun_fe,mun_control)
        dat_send_b <- prepare_data("TED_SITRAF_aggreg_send.dta",flood_b,mun_fe,mun_control)
        dat_combined0_b <- bind_rows(dat_send_b, dat_rec_b) %>% group_by(across(-c(trans, valor, ltrans, lvalor, tipo))) %>%
          summarize(trans = sum(trans, na.rm = TRUE),
                    valor = sum(valor, na.rm = TRUE)) %>% ungroup()

        dat_together <- bind_rows(dat_after0, dat_combined0) %>%
          group_by(across(-c(trans, valor))) %>%
          summarize(trans = sum(trans, na.rm = TRUE),
                    valor = sum(valor, na.rm = TRUE)) %>% ungroup() %>%
          mutate(ltrans = log1p(trans),
                 lvalor = log1p(valor))

        dat_together_b <- bind_rows(dat_before0, dat_combined0_b) %>%
          group_by(across(-c(trans, valor))) %>%
          summarize(trans = sum(trans, na.rm = TRUE),
                    valor = sum(valor, na.rm = TRUE)) %>% ungroup() %>%
          mutate(ltrans = log1p(trans),
                 lvalor = log1p(valor))
        
        for(z in 1:length(variables)){
          twfe2(beginning,variables[[z]],"constant","constant","flood_risk5", list(dat_together_b, dat_together), c("before","after"))
          print_twfe_week(beginning, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
        }
      }
    }, error = function(e) {
      print(paste("Error in Ted boleto:", e))
    })   
}

#-------------------------------------------------------------------------------
# TED + Boleto + Credit card + Debit card
#-------------------------------------------------------------------------------
## Should have users!
# Before and After!
if(run_TED_boleto_card == 1){
    #Send and Rec
    variables <- c("lvalor")
    variables_labels <- c("Log Value")
    tryCatch({
      for(i in 1:3){
        if(i==1){
          flood_a <- flood_week_after
          flood_b <- flood_week_before2019
          xll <- xl
          xuu <- xu
          ending <- ""
          beginning <- paste0("Ted_boleto_card_", ending)
          legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
        }
        if(i==2){
          flood_a <- flood_week_after_balanced
          flood_b <- flood_week_before_balanced2019
          xll <- xl_balanced
          xuu <- xu_balanced
          ending <- "balanced_"
          beginning <- paste0("Ted_boleto_card_", ending)
          legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
        }
        if(i==3){
          flood_a <- flood_week_after_balanced_covid
          flood_b <- flood_week_before_balanced_covid
          xll <- xl_balanced_covid
          xuu <- xu_balanced_covid
          ending <- "balanced_covid_"
          beginning <- paste0("Ted_boleto_card_", ending)
          legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
        }
        
        dat_after <- prepare_data("Boleto.dta",flood_a,mun_fe,mun_control)
        dat_before <- prepare_data("Boleto.dta",flood_b,mun_fe,mun_control)
        dat_after0 <- dat_after %>% 
          group_by(across(-c(tipo,
                             valor_send, lvalor_send, trans_send, ltrans_send, senders, lsenders,
                             valor_rec, lvalor_rec, trans_rec, ltrans_rec, receivers, lreceivers))) %>%
          summarize(valor = sum(valor_send, na.rm = TRUE) + sum(valor_rec, na.rm = TRUE)) %>% ungroup()
        dat_before0 <- dat_before %>% 
          group_by(across(-c(tipo, 
                             valor_send, lvalor_send, trans_send, ltrans_send, senders, lsenders,
                             valor_rec, lvalor_rec, trans_rec, ltrans_rec, receivers, lreceivers))) %>%
          summarize(valor = sum(valor_send, na.rm = TRUE) + sum(valor_rec, na.rm = TRUE)) %>% ungroup()
        
        dat_rec <- prepare_data("TED_SITRAF_aggreg_rec.dta",flood_a,mun_fe,mun_control)
        dat_send <- prepare_data("TED_SITRAF_aggreg_send.dta",flood_a,mun_fe,mun_control)
        dat_combined0 <- bind_rows(dat_send, dat_rec) %>% group_by(across(-c(trans, valor, ltrans, lvalor, tipo))) %>%
          summarize(valor = sum(valor, na.rm = TRUE)) %>% ungroup()
        dat_rec_b <- prepare_data("TED_SITRAF_aggreg_rec.dta",flood_b,mun_fe,mun_control)
        dat_send_b <- prepare_data("TED_SITRAF_aggreg_send.dta",flood_b,mun_fe,mun_control)
        dat_combined0_b <- bind_rows(dat_send_b, dat_rec_b) %>% group_by(across(-c(trans, valor, ltrans, lvalor, tipo))) %>%
          summarize(valor = sum(valor, na.rm = TRUE)) %>% ungroup()

        dat_after_card <- prepare_data("Card_rec.dta",flood_a,mun_fe,mun_control)
        dat_before_card <- prepare_data("Card_rec.dta",flood_b,mun_fe,mun_control)
        dat_after_card0 <- dat_after_card %>% 
          group_by(across(-c(tipo, receivers, valor, receivers_credit, valor_credit, receivers_debit, valor_debit,
                              lreceivers, lvalor, lreceivers_credit, lvalor_credit, lreceivers_debit, lvalor_debit))) %>%
          summarize(valor = sum(valor, na.rm = TRUE)) %>% ungroup()
        dat_before_card0 <- dat_before_card %>%
          group_by(across(-c(tipo, receivers, valor, receivers_credit, valor_credit, receivers_debit, valor_debit,
                              lreceivers, lvalor, lreceivers_credit, lvalor_credit, lreceivers_debit, lvalor_debit))) %>%
          summarize(valor = sum(valor, na.rm = TRUE)) %>% ungroup()

        dat_together <- bind_rows(list(dat_after0, dat_combined0, dat_after_card0)) %>%
          group_by(across(-c(valor))) %>%
          summarize(valor = sum(valor, na.rm = TRUE)) %>% ungroup() %>%
          mutate(lvalor = log1p(valor))

        dat_together_b <- bind_rows(list(dat_before0, dat_combined0_b, dat_before_card0)) %>%
          group_by(across(-c(valor))) %>%
          summarize(valor = sum(valor, na.rm = TRUE)) %>% ungroup() %>%
          mutate(lvalor = log1p(valor))
        
        for(z in 1:length(variables)){
          twfe2(beginning,variables[[z]],"constant","constant","flood_risk5", list(dat_together_b, dat_together), c("before","after"))
          print_twfe_week(beginning, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
        }
      }
    }, error = function(e) {
      print(paste("Error in Ted boleto card:", e))
    })   
}


#-------------------------------------------------------------------------------
# Synthetic Month of 4 weeks 
#-------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# Pix_Muni_flow - Worked!
# ------------------------------------------------------------------------------

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


# WORK ON SEAN SUGESTIONS. - P2p, p2b, b2p, b2b. missing
if(run_Pix_Muni_flow_syn == 1){
  tryCatch({
    #Send and Rec
    variables <- c("ltrans","lvalor_w","lvalor")
    variables_labels <- c("Log Transactions","Log Value","Log Value")
    value_list <- c("trans","valor_w","valor")
    identifiers <- c("tipo")
    tryCatch({
      for(i in 1:3){
        if(i==1){
          flood_a <- flood_week_after
          xll <- xl_month
          xuu <- xu_month
          ending <- "syn_"
          beginning_PF <- paste0("Pix_Muni_PF_", ending)
          beginning_PJ <- paste0("Pix_Muni_PJ_", ending)
          beginning_PF2 <- paste0("Pix_Muni_PF_2", ending)
          beginning_PJ2 <- paste0("Pix_Muni_PJ_2", ending)
        }
        if(i==2){
          flood_a <- flood_week_after_balanced
          xll <- xl_balanced_month
          xuu <- xu_balanced_month
          ending <- "syn_balanced_"
          beginning_PF <- paste0("Pix_Muni_PF_", ending)
          beginning_PJ <- paste0("Pix_Muni_PJ_", ending)
          beginning_PF2 <- paste0("Pix_Muni_PF_2", ending)
          beginning_PJ2 <- paste0("Pix_Muni_PJ_2", ending)
        }
        if(i==3){
          flood_a <- flood_week_after_balanced_covid
          xll <- xl_balanced_covid_month
          xuu <- xu_balanced_covid_month
          ending <- "syn_balanced_covid_"
          beginning_PF <- paste0("Pix_Muni_PF_", ending)
          beginning_PJ <- paste0("Pix_Muni_PJ_", ending)
          beginning_PF2 <- paste0("Pix_Muni_PF_2", ending)
          beginning_PJ2 <- paste0("Pix_Muni_PJ_2", ending)
        }
        dat_rec <- prepare_data_syn_month("Pix_Muni_flow_aggreg_rec.dta",flood_a,mun_fe,identifiers,value_list)
        dat_send <- prepare_data_syn_month("Pix_Muni_flow_aggreg_send.dta",flood_a,mun_fe,identifiers,value_list)
        dat_combined <- bind_rows(dat_send, dat_rec) %>%
          group_by(across(-c(trans, valor, valor_w,ltrans, lvalor, lvalor_w))) %>%
          summarize(trans = sum(trans, na.rm = TRUE),
                    valor = sum(valor, na.rm = TRUE),
                    valor_w = sum(valor_w, na.rm = TRUE)) %>% ungroup() %>% 
          mutate(ltrans = log1p(trans),
                 lvalor = log1p(valor),
                 lvalor_w = log1p(valor_w)) 
        dat_rec1 <- dat_rec %>% filter(tipo == 1)
        dat_send1 <- dat_send %>% filter(tipo == 1)
        dat_combined1 <- dat_combined %>% filter(tipo == 1)
        # Firms
        dat_rec2 <- dat_rec %>% filter(tipo == 2)
        dat_send2 <- dat_send %>% filter(tipo == 2)
        dat_combined2 <- dat_combined %>% filter(tipo == 2)
        
        for(z in 1:length(variables)){
          twfe2(beginning_PF,variables[[z]],"constant","constant","flood_risk5", list(dat_rec1, dat_send1), c("rec","send"))
          print_twfe_month(beginning_PF, variables[[z]], variables_labels[[z]], c("rec","send"), c("Received","Sent"), xll, xuu)
          twfe2(beginning_PF2,variables[[z]],"constant","constant","flood_risk5", list(dat_combined1), c("pix"))
          print_twfe_month(beginning_PF2, variables[[z]], variables_labels[[z]], c("pix"), c("Pix"), xll, xuu)
          
          twfe2(beginning_PJ,variables[[z]],"constant","constant","flood_risk5", list(dat_rec2, dat_send2), c("rec","send"))
          print_twfe_month(beginning_PJ, variables[[z]], variables_labels[[z]], c("rec","send"), c("Received","Sent"), xll, xuu)
          twfe2(beginning_PJ2,variables[[z]],"constant","constant","flood_risk5", list(dat_combined2), c("pix"))
          print_twfe_month(beginning_PJ2, variables[[z]], variables_labels[[z]], c("pix"), c("Pix"), xll, xuu)
        }
        rm(dat_rec1, dat_send1, dat_combined1, dat_rec2, dat_send2, dat_combined2, dat_rec, dat_send, dat_combined)
      }
    }, error = function(e) {
      print(paste("Error in Send Rec:", e))
    })
    
    # Inflow vs Outflow + Self        
    variables <- c("lusers","lusers2","ltrans","lvalor_w", "lvalor")
    variables_labels <- c("Log Active Users Inside the Municipality","Log Active Users Outside the Municipality","Log Transactions","Log Value","Log Value")
    value_list <- c("senders", "receivers","trans","valor_w","valor")
    identifiers <- c("sender_type", "receiver_type", "flow_code")
    tryCatch({
      for(i in 1:3){
        if(i==1){
          flood_a <- flood_week_after
          xll <- xl
          xuu <- xu
          ending <- "syn_"
          beginning_p2p <- paste0("Pix_Muni_flow_p2p_", ending)
          beginning_PF_self <- paste0("Pix_Muni_PF_self_", ending)
          beginning_PJ_self <- paste0("Pix_Muni_PJ_self_", ending)
        }
        if(i==2){
          flood_a <- flood_week_after_balanced
          xll <- xl_balanced
          xuu <- xu_balanced
          ending <- "syn_balanced_"
          beginning_p2p <- paste0("Pix_Muni_flow_p2p_", ending)
          beginning_PF_self <- paste0("Pix_Muni_PF_self_", ending)
          beginning_PJ_self <- paste0("Pix_Muni_PJ_self_", ending)
        }
        if(i==3){
          flood_a <- flood_week_after_balanced_covid
          xll <- xl_balanced_covid
          xuu <- xu_balanced_covid
          ending <- "syn_balanced_covid_"
          beginning_p2p <- paste0("Pix_Muni_flow_p2p_", ending)
          beginning_PF_self <- paste0("Pix_Muni_PF_self_", ending)
          beginning_PJ_self <- paste0("Pix_Muni_PJ_self_", ending)
        }
        dat <- prepare_data("Pix_Muni_flow.dta",flood_a,mun_fe,identifiers,value_list)
        dat_inflow_p2p <- dat %>%
          filter(sender_type == 1, receiver_type == 1, flow_code == 1) %>%
          rename(lusers = lreceivers,
                 lusers2 = lsenders)
        dat_outflow_p2p <- dat %>%
          filter(sender_type == 1, receiver_type == 1, flow_code == -1) %>%
          rename(lusers=lsenders,
                 lusers2=lreceivers)
        dat_self <- dat %>%
          filter(flow_code == 99)
        dat_self_PF <- dat_self %>%
          filter(receiver_type == 1)
        dat_self_PJ <- dat_self %>%
          filter(receiver_type == 2)
        
        for(z in 1:length(variables)){
          twfe2(beginning_p2p,variables[[z]],"constant","constant","flood_risk5", list(dat_inflow_p2p, dat_outflow_p2p), c("inflow","outflow"))
          print_twfe_month(beginning_p2p, variables[[z]], variables_labels[[z]], c("inflow","outflow"), c("Receiving","Sending"), xll, xuu)
          
          twfe2(beginning_PF_self,variables[[z]],"constant","constant","flood_risk5", list(dat_self_PF), c("self"))
          print_twfe_month(beginning_PF_self, variables[[z]], variables_labels[[z]], c("self"), c("Self"), xll, xuu)
          
          twfe2(beginning_PJ_self,variables[[z]],"constant","constant","flood_risk5", list(dat_self_PJ), c("self"))
          print_twfe_month(beginning_PJ_self, variables[[z]], variables_labels[[z]], c("self"), c("Self"), xll, xuu)
        }
        twfe2(beginning_p2p,"lusers2","constant","constant","flood_risk5",list(dat_outflow_p2p, dat_inflow_p2p), c("inflow","outflow"))
        print_twfe_month(beginning_p2p,"lusers","Log Active Users Inside the Municipaltiy", c("inflow"), c("Receiving"), xll, xuu)
        print_twfe_month(beginning_p2p,"lusers2","Log Active Users Outside the Municipaltiy", c("outflow"), c("Sending"), xll, xuu)
        
        rm(dat_inflow_p2p, dat_outflow_p2p, dat, dat_self, dat_self_PF, dat_self_PJ)
      }
    }, error = function(e) {
      print(paste("Error in inflow and outflow:", e))
    })

  }, error = function(e) {
    print(paste("Error in Pix_Muni_flow:", e))
  })
}






################################################################################
# Create TED + Boleto, Credito + Debito, Ted + Boleto + Credito + Debito
