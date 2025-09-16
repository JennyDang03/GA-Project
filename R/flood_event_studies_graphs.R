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

run_Pix_Muni_flow_syn <- 1 # worked

run_CCS_Muni_stock <- 1 # Worked
run_CCS_Muni_IF <- 1 # Worked
run_CCS_HHI <- 1 # Worked
run_CCS_first_account <- 1  # Worked
run_CCS_first_account_month <- 1 # Worked
run_Pix_Muni_Bank <- 1 # Worked
run_Pix_Muni_flow <- 1 # Worked
run_Pix_Muni_user <- 1 # Worked
run_Card_rec <- 1 # Worked
run_Boletos <- 1 # Worked
run_Estban <- 1 # Worked 
run_Base_old <- 1 # Worked
run_Credito_old <- 1 # Worked

run_Pix_ind_sample <- 1
run_Credito <- 1

run_TED <- 1 # Code NOT ready. ----> Need to download.
run_TED_SITRAF <- 1

run_Pix_individual <- 1
run_adoption_pix <- 1 # We created new file. pix_adoption_sample. 

#run_Pix_adoption <- 0 # Code NOT ready. ----> Need to download.
#run_Card_adoption <- 0 # Code NOT ready. ----> Need to download.
#run_Boleto_adoption <- 0 # Code NOT ready. ----> Need to download.
#run_TED_adoption <- 0 # Code NOT ready. ----> Need to download.

run_Pix_ind_sample <- 1 
run_Boleto_ind_sample <- 1 
run_Card_ind_sample <- 1 
run_TED_ind_sample <- 1
run_CCS_ind_sample <- 1
run_Credito_ind_sample <- 1
run_TED_SITRAF_ind_sample <- 1
# ------------------------------------------------------------------------------
# Credito
# ------------------------------------------------------------------------------

# Credito_Muni_PF.dta
# Variables: time_id, muni_cd, new_users, new_users_if, new_users_cg, valor, valor_ativo, users, qtd, banks, valor_w, users_w, qtd_w, valor_cartao, users_cartao, qtd_cartao
#                                 Plus l variations

variables <- c("lnew_users", "lnew_users_if", "lnew_users_cg", "lvalor","lvalor_ativo","lusers","lqtd","lbanks","lvalor_w","lusers_w","lqtd_w","lvalor_cartao","lusers_cartao","lqtd_cartao")
variables_labels <- c("Log New User", "Log New Bank", "Log New Conglomerate", "Log Volume Loans","Log Active Loan Value","Log Credit Access","Log Quantity of Loans","Log Banks","Log Value","Log Users","Log Quantity","Log Value Card","Log Users Card","Log Quantity Card")


variables <- c("lvalor","lvalor_ativo")
variables_labels <- c("Log New Debt","Log New Debt")
if(run_Credito == 1){
  tryCatch({
    for(i in 2:2){
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

      beginning <- paste0("Credito_Muni_PF",ending)
      for(z in 1:length(variables)){
        print_twfe_month(beginning, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
      }
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
      beginning <- paste0("Credito_Muni_PJ",ending)
      
      for(z in 1:length(variables)){
        #twfe2(beginning,variables[[z]],"constant","constant","flood_risk5", list(dat_b,dat_a), c("before","after"))
        print_twfe_month(beginning, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
      }
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in Credito2:", e))
  })
}

# ------------------------------------------------------------------------------
# TED
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
        beginning_PF <- paste0("Ted_sitraf_PF_", ending)
        beginning_PJ <- paste0("Ted_sitraf_PJ_", ending)
        beginning_PF2 <- paste0("Ted_sitraf_PF_2", ending)
        beginning_PJ2 <- paste0("Ted_sitraf_PJ_2", ending)
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
      }

      for(z in 1:length(variables)){
        #twfe2(beginning_PF,variables[[z]],"constant","constant","flood_risk5", list(dat_rec1, dat_send1), c("rec","send"))
        print_twfe_week(beginning_PF, variables[[z]], variables_labels[[z]], c("rec","send"), c("Received","Sent"), xll, xuu)
        #twfe2(beginning_PF2,variables[[z]],"constant","constant","flood_risk5", list(dat_combined1_b, dat_combined1), c("before","after"))
        print_twfe_week(beginning_PF2, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
        
        #twfe2(beginning_PJ,variables[[z]],"constant","constant","flood_risk5", list(dat_rec2, dat_send2), c("rec","send"))
        print_twfe_week(beginning_PJ, variables[[z]], variables_labels[[z]], c("rec","send"), c("Received","Sent"), xll, xuu)
        #twfe2(beginning_PJ2,variables[[z]],"constant","constant","flood_risk5", list(dat_combined2_b, dat_combined2), c("before","after"))
        print_twfe_week(beginning_PJ2, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
      }
    }
  }, error = function(e) {
    print(paste("Error in Send Rec:", e))
  })
  
  # Inflow vs Outflow + Self        
  variables <- c("lusers")
  variables_labels <- c("Log Active Users Inside the Municipality")
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

      for(z in 1:length(variables)){
        print_twfe_week(beginning_p2p, variables[[z]], variables_labels[[z]], c("inflow","outflow"), c("Receiving","Sending"), xll, xuu)
        print_twfe_week(beginning_p2p_b, variables[[z]], variables_labels[[z]], c("inflow","outflow"), c("Receiving","Sending"), xll, xuu)
      }
      print_twfe_week(beginning_p2p,"lusers","Log Active Users Inside the Municipaltiy", c("inflow"), c("Receiving"), xll, xuu)
      #print_twfe_week(beginning_p2p,"lusers2","Log Active Users Outside the Municipaltiy", c("outflow"), c("Sending"), xll, xuu)
      
      print_twfe_week(beginning_p2p_b,"lusers","Log Active Users Inside the Municipaltiy", c("inflow"), c("Receiving"), xll, xuu)
      #print_twfe_week(beginning_p2p_b,"lusers2","Log Active Users Outside the Municipaltiy", c("outflow"), c("Sending"), xll, xuu)
      
      for(z in 1:length(variables_self)){
        print_twfe_week(beginning_PF_self, variables_self[[z]], variables_self_labels[[z]], c("before","after"), legend_name, xll, xuu)
        print_twfe_week(beginning_PJ_self, variables_self[[z]], variables_self_labels[[z]], c("before","after"), legend_name, xll, xuu)
      }
    }
  }, error = function(e) {
    print(paste("Error in inflow and outflow:", e))
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
      legend_name <- c("Before Pix","After Pix")
      for(z in 1:length(variables)){
        print_twfe_week(beginning_PF, variables[[z]], variables_labels[[z]], c("rec","send"), c("Received","Sent"), xll, xuu)
        print_twfe_week(beginning_PF2, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
        
        print_twfe_week(beginning_PJ, variables[[z]], variables_labels[[z]], c("rec","send"), c("Received","Sent"), xll, xuu)
        print_twfe_week(beginning_PJ2, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
        
        print_twfe_week(beginning, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
      }
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
      legend_name <- c("Before Pix","After Pix")
      for(z in 1:length(variables)){
        print_twfe_week(beginning_p2p, variables[[z]], variables_labels[[z]], c("inflow","outflow"), c("Receiving","Sending"), xll, xuu)
        
        print_twfe_week(beginning_p2p_b, variables[[z]], variables_labels[[z]], c("inflow","outflow"), c("Receiving","Sending"), xll, xuu)
      }
      print_twfe_week(beginning_p2p,"lusers","Log Active Users Inside the Municipaltiy", c("inflow"), c("Receiving"), xll, xuu)
      print_twfe_week(beginning_p2p,"lusers2","Log Active Users Outside the Municipaltiy", c("outflow"), c("Sending"), xll, xuu)
      
      print_twfe_week(beginning_p2p_b,"lusers","Log Active Users Inside the Municipaltiy", c("inflow"), c("Receiving"), xll, xuu)
      print_twfe_week(beginning_p2p_b,"lusers2","Log Active Users Outside the Municipaltiy", c("outflow"), c("Sending"), xll, xuu)
      
      for(z in 1:length(variables_self)){
        print_twfe_week(beginning_PF_self, variables_self[[z]], variables_self_labels[[z]], c("before","after"), legend_name, xll, xuu)
        
        print_twfe_week(beginning_PJ_self, variables_self[[z]], variables_self_labels[[z]], c("before","after"), legend_name, xll, xuu)
      }
      
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
variables_adoption <- c("ladoption", "ladopt_send", "ladopt_rec","ladopt_self")
variables_adoption_labels <- c("Log Adoption", "Log Adoption Send", "Log Adoption Receive", "Log Self Adoption")
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
          beginning_adoption <- paste0("Pix_ind_sample_PF_adoption",ending)
        }
        if(j==2){
          beginning <- paste0("Pix_ind_sample_PJ",ending)
          beginning_adoption <- paste0("Pix_ind_sample_PJ_adoption",ending)
        }
        
        for(z in 1:length(variables)){
          print_twfe_week(beginning, variables[[z]], variables_labels[[z]], c("pix"), c("Pix"), xll, xuu)
        }
        for(z in 1:length(variables_adoption)){
          print_twfe_week(beginning_adoption, variables_adoption[[z]], variables_adoption_labels[[z]], c("pix"), c("Pix"), xll, xuu)
        }
        #rm(dat_a, dat_adoption_a)
      }
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in Pix_ind_sample:", e))
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
variables_adoption <- c("ladoption", "ladopt_send", "ladopt_rec","ladopt_self")
variables_adoption_labels <- c("Log Adoption", "Log Adoption Send", "Log Adoption Receive", "Log Self Adoption")
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
          beginning_adoption <- paste0("Boleto_ind_sample_PF_adoption",ending)
        }
        if(j==2){
          beginning <- paste0("Boleto_ind_sample_PJ",ending)
          beginning_adoption <- paste0("Boleto_ind_sample_PJ_adoption",ending)
        }
        
        for(z in 1:length(variables)){
          #twfe_ind(beginning,variables[[z]],"constant","constant","flood_risk5", list(dat_bb, dat_a), c("before", "after"))
          print_twfe_week(beginning, variables[[z]], variables_labels[[z]], c("before", "after"), legend_name, xll, xuu)
        }
        for(z in 1:length(variables_adoption)){
          #twfe_ind(beginning_adoption,variables_adoption[[z]],"constant","constant","flood_risk5", list(dat_adoption_bb, dat_adoption_a), c("before", "after"))
          print_twfe_week(beginning_adoption, variables_adoption[[z]], variables_adoption_labels[[z]], c("before", "after"), legend_name, xll, xuu)
        }
        #rm(dat_a, dat_adoption_a)
        #rm(dat_bb, dat_adoption_bb)
      }
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in Boleto_ind_sample:", e))
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
variables_adoption <- c("ladoption", "ladopt_credit", "ladopt_debit")
variables_adoption_labels <- c("Log Adoption", "Log Adoption Credit", "Log Adoption Debit")
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
      
      for(j in 1:2){
        if(j==1){
          beginning <- paste0("Card_ind_sample_PF",ending)
          beginning_adoption <- paste0("Card_ind_sample_PF_adoption",ending)
        }
        if(j==2){
          beginning <- paste0("Card_ind_sample_PJ",ending)
          beginning_adoption <- paste0("Card_ind_sample_PJ_adoption",ending)
        }
        
        for(z in 1:length(variables)){
          #twfe_ind(beginning,variables[[z]],"constant","constant","flood_risk5", list(dat_bb, dat_a), c("before", "after"))
          print_twfe_week(beginning, variables[[z]], variables_labels[[z]], c("before", "after"), legend_name, xll, xuu)
        }
        for(z in 1:length(variables_adoption)){
          #twfe_ind(beginning_adoption,variables_adoption[[z]],"constant","constant","flood_risk5", list(dat_adoption_bb, dat_adoption_a), c("before", "after"))
          print_twfe_week(beginning_adoption, variables_adoption[[z]], variables_adoption_labels[[z]], c("before", "after"), legend_name, xll, xuu)
        }
        #rm(dat_a, dat_adoption_a)
        #rm(dat_bb, dat_adoption_bb)
      }
      #rm(dat, dat_adoption)
      #rm(dat_b, dat_adoption_b)
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in Card_ind_sample:", e))
  })
}

# ------------------------------------------------------------------------------
# CCS_ind_sample
# ------------------------------------------------------------------------------

# Before Variables: id, tipo, bank, dia_inicio, dia_fim

# "CCS_ind_sample_PF", "CCS_ind_sample_PJ"
# Variables: id, muni_cd, tipo, bank, tipo_inst, bank_type, dia_inicio, dia_fim

# "CCS_ind_sample_PF_stock.dta", "CCS_ind_sample_PJ_stock.dta"
# Variables: id, muni_cd, tipo, bank_type, stock, week

# "CCS_ind_sample_PF_adoption.dta", "CCS_ind_sample_PJ_adoption.dta"
# Variables: id, muni_cd, tipo, bank_type, adoption_date, week

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
          #dat_a <- prepare_data("CCS_ind_sample_PF_stock.dta",flood_a,mun_fe,mun_control)
          #dat_bb <- prepare_data("CCS_ind_sample_PF_stock.dta",flood_b,mun_fe,mun_control)
        }
        if(j==2){
          beginning <- paste0("CCS_ind_sample_PJ",ending)
          beginning_trad_digi <- paste0("CCS_ind_sample_PJ_trad_digi",ending)
          #dat_a <- prepare_data("CCS_ind_sample_PJ_stock.dta",flood_a,mun_fe,mun_control)
          #dat_bb <- prepare_data("CCS_ind_sample_PJ_stock.dta",flood_b,mun_fe,mun_control)
        }
        
        for(z in 1:length(variables)){
          #twfe_ind(beginning,variables[[z]],"constant","constant","flood_risk5", list(dat_bb_collapsed, dat_a_collapsed), c("before", "after"))
          print_twfe_week(beginning, variables[[z]], variables_labels[[z]], c("before", "after"), legend_name, xll, xuu)
          
          # After, bank types. 
          #twfe_ind(beginning_trad_digi,variables[[z]],"constant","constant","flood_risk5", list(trad_a, digi_a), c("trad_after","digi_after"))
          print_twfe_week(beginning_trad_digi, variables[[z]], variables_labels[[z]], c("trad_after","digi_after"), c("Traditional","Digital"), xll, xuu)
        }
        #rm(dat_a)
        #rm(dat_bb)
        #rm(dat_a_collapsed)
        #rm(dat_bb_collapsed)
        #rm(digi_a, trad_a)
      }
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in CCS_ind_sample:", e))
  })
}



# ------------------------------------------------------------------------------
# Credito_ind_sample
# ------------------------------------------------------------------------------
# Before Variables: time_id, id, tipo, bank, new_users, new_users_if, new_users_cg, valor, valor_ativo, qtd

#   "Credito_ind_sample_PF.dta", "Credito_ind_sample_PJ.dta"))
# Variables: id, muni_cd, tipo, bank_type, new_users, new_users_if, new_users_cg, valor, valor_ativo, qtd
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
        }
        if(j==2){
          beginning <- paste0("Credito_ind_sample_PJ",ending)
          beginning_trad_digi <- paste0("Credito_ind_sample_PJ_trad_digi",ending)
        }
        
        for(z in 1:length(variables)){
          tryCatch({
            print_twfe_month(beginning, variables[[z]], variables_labels[[z]], c("before", "after"), legend_name, xll, xuu)
            
            # After, bank types. 
            print_twfe_month(beginning_trad_digi, variables[[z]], variables_labels[[z]], c("trad_after","digi_after"), c("Traditional","Digital"), xll, xuu)
          }, error = function(e) {
            print(paste("Error in Credito_ind_sample for variable:", variables[[z]]))
            print(paste("Error message:", e$message))
          })        
        }
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
variables_adoption <- c("ladoption", "ladopt_send", "ladopt_rec","ladopt_self")
variables_adoption_labels <- c("Log Adoption", "Log Adoption Send", "Log Adoption Receive", "Log Self Adoption")
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
          beginning_adoption <- paste0("TED_SITRAF_ind_sample_PF_adoption",ending)
        }
        if(j==2){
          beginning <- paste0("TED_SITRAF_ind_sample_PJ",ending)
          beginning_adoption <- paste0("TED_SITRAF_ind_sample_PJ_adoption",ending)
        }
        
        for(z in 1:length(variables)){
          #twfe_ind(beginning,variables[[z]],"constant","constant","flood_risk5", list(dat_bb, dat_a), c("before", "after"))
          print_twfe_week(beginning, variables[[z]], variables_labels[[z]], c("before", "after"), legend_name, xll, xuu)
        }
        for(z in 1:length(variables_adoption)){
          #twfe_ind(beginning_adoption,variables_adoption[[z]],"constant","constant","flood_risk5", list(dat_adoption_bb, dat_adoption_a), c("before", "after"))
          print_twfe_week(beginning_adoption, variables_adoption[[z]], variables_adoption_labels[[z]], c("before", "after"), legend_name, xll, xuu)
        }
        #rm(dat_a, dat_adoption_a)
        #rm(dat_bb, dat_adoption_bb)
      }
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in TED_SITRAF_ind_sample:", e))
  })
}






# ------------------------------------------------------------------------------
# Card_rec
# ------------------------------------------------------------------------------

# Card_rec.dta
# Variables: week, muni_cd, tipo, receivers, valor, receivers_credit, valor_credit, receivers_debit, valor_debit
#            lreceivers, lvalor, lreceivers_credit, lvalor_credit, lreceivers_debit, lvalor_debit

variables <- c("lvalor", "lreceivers", "lvalor_credit", "lreceivers_credit", "lvalor_debit", "lreceivers_debit")
variables_labels_PF <- c("Log Card Payments", "Log People Accepting Card Payments", "Log Credit Payments", "Log People Accepting Credit Payments", "Log Debit Payments", "Log People Accepting Debit Payments")
variables_labels_PJ <- c("Log Card Payments", "Log Firms Accepting Card Payments", "Log Credit Payments", "Log Firms Accepting Credit Payments", "Log Debit Payments", "Log Firms Accepting Debit Payments")
if(run_Card_rec == 1){
  tryCatch({
    for(i in 2:3){
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
      legend_name <- c("Before Pix", "After Pix")
      for(z in 1:length(variables)){
        print_twfe_week(beginning_PF, variables[[z]], variables_labels_PF[[z]], c("before","after"), legend_name, xll, xuu)
        
        print_twfe_week(beginning_PJ, variables[[z]], variables_labels_PJ[[z]], c("before","after"), legend_name, xll, xuu)
      }
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in Card:", e))
  })
}


# ------------------------------------------------------------------------------
# Boletos
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
    for(i in 2:3){
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
      legend_name <- c("Before Pix", "After Pix")
      for(z in 1:length(variables)){
        print_twfe_week(beginning_PF, variables[[z]], variables_labels_PF[[z]], c("before","after"), legend_name, xll, xuu)
        
        print_twfe_week(beginning_PJ, variables[[z]], variables_labels_PJ[[z]], c("before","after"), legend_name, xll, xuu)
      }
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in Boletos:", e))
  })
}


# ------------------------------------------------------------------------------
# CCS_Muni_stock - Worked!
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
    for(i in 2:2){
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
      legend_name <- c("Before Pix", "After Pix")
      for(z in 1:length(variables)){
        #twfe2(beginning_PF,variables[[z]],"constant","constant","flood_risk5", list(dat_before_PF, dat_after_PF), c("before","after"))
        print_twfe_week(beginning_PF, variables[[z]], variables_labels_PF[[z]], c("before","after"), legend_name, xll, xuu)
        
        #twfe2(beginning_PJ,variables[[z]],"constant","constant","flood_risk5", list(dat_before_PJ, dat_after_PJ), c("before","after"))
        print_twfe_week(beginning_PJ, variables[[z]], variables_labels_PJ[[z]], c("before","after"), legend_name, xll, xuu)
      }
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in CCS_Muni_stock:", e))
  })
}

# ------------------------------------------------------------------------------
# CCS_Muni_IF - Worked!
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
      }
      legend_name <- c("Banks", "NBFIs")
      for(z in 1:length(variables)){
        #twfe2(beginning_PF,variables[[z]],"constant","constant","flood_risk5", list(trad_after_PF, digi_after_PF), c("trad_after","digi_after"))
        print_twfe_week(beginning_PF, variables[[z]], variables_labels[[z]], c("trad_after","digi_after"), legend_name, xll, xuu)
        
        #twfe2(beginning_PJ,variables[[z]],"constant","constant","flood_risk5", list(trad_after_PJ, digi_after_PJ), c("trad_after","digi_after"))
        print_twfe_week(beginning_PJ, variables[[z]], variables_labels[[z]], c("trad_after","digi_after"), legend_name, xll, xuu)
      }
      #rm(digi_after_PF, trad_after_PF, digi_after_PJ, trad_after_PJ, dat_after_PJ, dat_after_PF)
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

      for(z in 1:length(variables)){
        #twfe2(beginning_PF,variables[[z]],"constant","constant","flood_risk5", list(dat_HHI_before_PF, dat_HHI_after_PF), c("before","after"))
        print_twfe_week(beginning_PF, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
        
        #twfe2(beginning_PJ,variables[[z]],"constant","constant","flood_risk5", list(dat_HHI_before_PJ, dat_HHI_after_PJ), c("before","after"))
        print_twfe_week(beginning_PJ, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
      }
      #rm(dat_HHI_after_PF,dat_HHI_before_PF,dat_HHI_after_PJ,dat_HHI_before_PJ)
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
    for(i in 2:2){
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

      legend_name <- c("Before Pix", "After Pix")
      for(z in 1:length(variables)){
        #twfe2(beginning_PF,variables[[z]],"constant","constant","flood_risk5", list(dat_before_PF, dat_after_PF), c("before","after"))
        print_twfe_week(beginning_PF, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
        
        #twfe2(beginning_PJ,variables[[z]],"constant","constant","flood_risk5", list(dat_before_PJ, dat_after_PJ), c("before","after"))
        print_twfe_week(beginning_PJ, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
      }
      #rm(dat_after,dat_before,dat_after_PF,dat_before_PF,dat_after_PJ,dat_before_PJ)
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
      legend_name <- c("Before Pix", "After Pix")
      for(z in 1:length(variables)){
        #twfe2(beginning_PF,variables[[z]],"constant","constant","flood_risk5", list(dat_before_PF, dat_after_PF), c("before","after"))
        print_twfe_month(beginning_PF, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
        
        #twfe2(beginning_PJ,variables[[z]],"constant","constant","flood_risk5", list(dat_before_PJ, dat_after_PJ), c("before","after"))
        print_twfe_month(beginning_PJ, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
      }
      #rm(dat_after,dat_before,dat_after_PF,dat_before_PF,dat_after_PJ,dat_before_PJ)
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in CCS_first_account:", e))
  })
}

# ------------------------------------------------------------------------------
# Pix_Muni_Bank - Worked - "all" is not working
# ------------------------------------------------------------------------------


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
   
      for(z in 1:length(variables)){
        #twfe2(beginning_PF,variables[[z]],"constant","constant","flood_risk5", list(trad_PF,digi_PF), c("trad", "digi"))
        print_twfe_week(beginning_PF, variables[[z]], variables_labels[[z]], c("trad", "digi"), c("Banks", "NBFIs"), xll, xuu)
        #twfe2(beginning_PF2,variables[[z]],"constant","constant","flood_risk5", list(all_banks_PF), c("all"))
        print_twfe_week(beginning_PF2, variables[[z]], variables_labels[[z]], c("all"), c("All Banks"), xll, xuu)
        
        #twfe2(beginning_PJ,variables[[z]],"constant","constant","flood_risk5", list(trad_PJ,digi_PJ), c("trad", "digi"))
        print_twfe_week(beginning_PJ, variables[[z]], variables_labels[[z]], c("trad", "digi"), c("Banks", "NBFIs"), xll, xuu)
        #twfe2(beginning_PJ2,variables[[z]],"constant","constant","flood_risk5", list(all_banks_PJ), c("all"))
        print_twfe_week(beginning_PJ2, variables[[z]], variables_labels[[z]], c("all"), c("All Banks"), xll, xuu)
        
        #twfe2(beginning_PF3,variables[[z]],"constant","constant","flood_risk5", list(bank_PF,ip_PF), c("bank", "ip"))
        print_twfe_week(beginning_PF3, variables[[z]], variables_labels[[z]], c("bank", "ip"), c("Banks", "NBFIs"), xll, xuu)
        #twfe2(beginning_PJ3,variables[[z]],"constant","constant","flood_risk5", list(bank_PJ,ip_PJ), c("bank", "ip"))
        print_twfe_week(beginning_PJ3, variables[[z]], variables_labels[[z]], c("bank", "ip"), c("Banks", "NBFIs"), xll, xuu)
        
      }

      
     
      #twfe2(paste0("Pix_Muni_Bank_4", ending),"lvalue","constant","constant","flood_risk5", list(bank_2,bank_1), c("bank_rec", "bank_sent"))
      print_twfe_week(paste0("Pix_Muni_Bank_4", ending), "lvalue", "Log Value", c("bank_rec", "bank_sent"), c("Received by Banks", "Sent by Banks"), xll, xuu)
      
      #twfe2(paste0("Pix_Muni_Bank_42", ending),"lvalue","constant","constant","flood_risk5", list(ip_2,ip_1), c("ip_rec", "ip_sent"))
      print_twfe_week(paste0("Pix_Muni_Bank_42", ending), "lvalue", "Log Value", c("ip_rec", "ip_sent"), c("Received by NBFIs", "Sent by NBFIs"), xll, xuu)

    }
  }, error = function(e) {
    print(paste("Error in Pix_Muni_Bank:", e))
  })
}
# ------------------------------------------------------------------------------
# Pix_Muni_flow - Worked! - "all" is not working
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

        for(z in 1:length(variables)){
          print_twfe_week(beginning_PF, variables[[z]], variables_labels[[z]], c("rec","send"), c("Received","Sent"), xll, xuu)
          print_twfe_week(beginning_PF2, variables[[z]], variables_labels[[z]], c("pix"), c("Pix"), xll, xuu)
          
          print_twfe_week(beginning_PJ, variables[[z]], variables_labels[[z]], c("rec","send"), c("Received","Sent"), xll, xuu)
          print_twfe_week(beginning_PJ2, variables[[z]], variables_labels[[z]], c("pix"), c("Pix"), xll, xuu)
        }
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

        for(z in 1:length(variables)){
          print_twfe_week(beginning_p2p, variables[[z]], variables_labels[[z]], c("inflow","outflow"), c("Receiving","Sending"), xll, xuu)
        }
        print_twfe_week(beginning_p2p,"lusers","Log Active Users Inside the Municipaltiy", c("inflow"), c("Receiving"), xll, xuu)
        print_twfe_week(beginning_p2p,"lusers2","Log Active Users Outside the Municipaltiy", c("outflow"), c("Sending"), xll, xuu)
        
        for(z in 1:(length(variables)-2)){
          print_twfe_week(beginning_PF_self, variables[[z+2]], variables_labels[[z+2]], c("self"), c("Self"), xll, xuu)
          
          print_twfe_week(beginning_PJ_self, variables[[z+2]], variables_labels[[z+2]], c("self"), c("Self"), xll, xuu)
        }
        print_twfe_week(beginning_PF_self, "lreceivers", "Log Active Users", c("self"), c("Self"), xll, xuu)
        print_twfe_week(beginning_PJ_self, "lreceivers", "Log Active Users", c("self"), c("Self"), xll, xuu)
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
        for(z in 1:length(variables)){
          print_twfe_week(beginning_p2p, variables[[z]], variables_labels[[z]], c("B2P","P2B"), c("B2P","P2B"), xll, xuu)
        }
        
      }
    }, error = function(e) {
      print(paste("Error in b2p p2b:", e))
    })
    
    
  }, error = function(e) {
    print(paste("Error in Pix_Muni_flow:", e))
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
    for(i in 2:2){
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
      
      #twfe2(beginning_PF,"lusers","constant","constant","flood_risk5",list(dat_PF), c("Pix"))
      print_twfe_week(beginning_PF,"lusers","Log Active Users", c("Pix"), c("Pix"), xll, xuu)
      #twfe2(beginning_PF,"lusers2","constant","constant","flood_risk5",list(dat_PF, dat_PF2), c("Senders","Receivers"))
      print_twfe_week(beginning_PF,"lusers2","Log Active Users", c("Senders","Receivers"), c("Senders","Receivers"), xll, xuu)
      
      #twfe2(beginning_PJ,"lusers","constant","constant","flood_risk5",list(dat_PJ), c("Pix"))
      print_twfe_week(beginning_PJ,"lusers","Log Active Users", c("Pix"), c("Pix"), xll, xuu)
      #twfe2(beginning_PJ,"lusers2","constant","constant","flood_risk5",list(dat_PJ, dat_PJ2), c("Senders","Receivers"))
      print_twfe_week(beginning_PJ,"lusers2","Log Active Users", c("Senders","Receivers"), c("Senders","Receivers"), xll, xuu)
      
      #rm(dat, dat_PF, dat_PJ, dat_PF2, dat_PJ2)
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

    #TED
    #twfe2("TED_","log_valor_TED_intra","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("TED_","log_valor_TED_intra","Log Value TED", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    #twfe2("TED_","log_qtd_TED_intra","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("TED_","log_qtd_TED_intra","Log Transactions TED", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    #twfe2("TED_","log_qtd_cli_TED_rec_PJ","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("TED_","log_qtd_cli_TED_rec_PJ","Log Quantity of Firms Receiving TED", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    #twfe2("TED_","log_qtd_cli_TED_pag_PJ","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("TED_","log_qtd_cli_TED_pag_PJ","Log Quantity of Firms Sending TED", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    # + Uso da conta + "adocao"
    
    #Boleto
    #twfe2("Boleto_","log_valor_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_","log_valor_boleto","Log Value Boleto", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    #twfe2("Boleto_","log_qtd_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_","log_qtd_boleto","Log Transactions Boleto", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    #twfe2("Boleto_","log_qtd_cli_pag_pf_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_","log_qtd_cli_pag_pf_boleto","Log Quantity of People Sending Boleto", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    #twfe2("Boleto_","log_qtd_cli_pag_pj_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_","log_qtd_cli_pag_pj_boleto","Log Quantity of Firms Sending Boleto", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    #twfe2("Boleto_","log_qtd_cli_rec_pj_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_","log_qtd_cli_rec_pj_boleto","Log Quantity of Firms Receiving Boleto", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    #Cartao 
    # * valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito 
    #twfe2("Cartao_","log_valor_cartao_debito","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_","log_valor_cartao_debito","Log Value Debit Card", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    #twfe2("Cartao_","log_valor_cartao_credito","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_","log_valor_cartao_credito","Log Value Credit Card", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    #twfe2("Cartao_","log_qtd_cli_cartao_debito","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_","log_qtd_cli_cartao_debito","Log Quantity of Firms Accepting Debit Card", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    #twfe2("Cartao_","log_qtd_cli_cartao_credito","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_","log_qtd_cli_cartao_credito","Log Quantity of Firms Accepting Credit Card", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    #twfe2("Cartao_","log_valor_cartao","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_","log_valor_cartao","Log Value Cards", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    #twfe2("Cartao_","log_qtd_cli_cartao","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_","log_qtd_cli_cartao","Log Quantity of Firms Accepting Cards", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
    
    
    #TED
    #twfe2("TED_balanced_","log_valor_TED_intra","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("TED_balanced_","log_valor_TED_intra","Log Value TED", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    
    #twfe2("TED_balanced_","log_qtd_TED_intra","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("TED_balanced_","log_qtd_TED_intra","Log Transactions TED", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    
    #twfe2("TED_balanced_","log_qtd_cli_TED_rec_PJ","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("TED_balanced_","log_qtd_cli_TED_rec_PJ","Log Quantity of Firms Receiving TED", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    
    #twfe2("TED_balanced_","log_qtd_cli_TED_pag_PJ","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("TED_balanced_","log_qtd_cli_TED_pag_PJ","Log Quantity of Firms Sending TED", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    
    # + Uso da conta + "adocao"
    
    #Boleto
    #twfe2("Boleto_balanced_","log_valor_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_balanced_","log_valor_boleto","Log Value Boleto", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    
    #twfe2("Boleto_balanced_","log_qtd_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_balanced_","log_qtd_boleto","Log Transactions Boleto", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    
    #twfe2("Boleto_balanced_","log_qtd_cli_pag_pf_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_balanced_","log_qtd_cli_pag_pf_boleto","Log Quantity of People Sending Boleto", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    
    #twfe2("Boleto_balanced_","log_qtd_cli_pag_pj_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_balanced_","log_qtd_cli_pag_pj_boleto","Log Quantity of Firms Sending Boleto", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    
    #twfe2("Boleto_balanced_","log_qtd_cli_rec_pj_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_balanced_","log_qtd_cli_rec_pj_boleto","Log Quantity of Firms Receiving Boleto", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    
    #Cartao 
    # * valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito 
    #twfe2("Cartao_balanced_","log_valor_cartao_debito","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_balanced_","log_valor_cartao_debito","Log Value Debit Card", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    
    #twfe2("Cartao_balanced_","log_valor_cartao_credito","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_balanced_","log_valor_cartao_credito","Log Value Credit Card", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    
    #twfe2("Cartao_balanced_","log_qtd_cli_cartao_debito","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_balanced_","log_qtd_cli_cartao_debito","Log Quantity of Firms Accepting Debit Card", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    
    #twfe2("Cartao_balanced_","log_qtd_cli_cartao_credito","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_balanced_","log_qtd_cli_cartao_credito","Log Quantity of Firms Accepting Credit Card", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    
    #twfe2("Cartao_balanced_","log_valor_cartao","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_balanced_","log_valor_cartao","Log Value Cards", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    #twfe2("Cartao_balanced_","log_qtd_cli_cartao","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_balanced_","log_qtd_cli_cartao","Log Quantity of Firms Accepting Cards", c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
    # + Adocao 
    rm(dat_a, dat_b)
    
    #TED
    #twfe2("TED_balanced_covid_","log_valor_TED_intra","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("TED_balanced_covid_","log_valor_TED_intra","Log Value TED", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    #twfe2("TED_balanced_covid_","log_qtd_TED_intra","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("TED_balanced_covid_","log_qtd_TED_intra","Log Transactions TED", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    #twfe2("TED_balanced_covid_","log_qtd_cli_TED_rec_PJ","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("TED_balanced_covid_","log_qtd_cli_TED_rec_PJ","Log Quantity of Firms Receiving TED", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    #twfe2("TED_balanced_covid_","log_qtd_cli_TED_pag_PJ","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("TED_balanced_covid_","log_qtd_cli_TED_pag_PJ","Log Quantity of Firms Sending TED", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    # + Uso da conta + "adocao"
    
    #Boleto
    #twfe2("Boleto_balanced_covid_","log_valor_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_balanced_covid_","log_valor_boleto","Log Value Boleto", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    #twfe2("Boleto_balanced_covid_","log_qtd_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_balanced_covid_","log_qtd_boleto","Log Transactions Boleto", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    #twfe2("Boleto_balanced_covid_","log_qtd_cli_pag_pf_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_balanced_covid_","log_qtd_cli_pag_pf_boleto","Log Quantity of People Sending Boleto", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    #twfe2("Boleto_balanced_covid_","log_qtd_cli_pag_pj_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_balanced_covid_","log_qtd_cli_pag_pj_boleto","Log Quantity of Firms Sending Boleto", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    #twfe2("Boleto_balanced_covid_","log_qtd_cli_rec_pj_boleto","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Boleto_balanced_covid_","log_qtd_cli_rec_pj_boleto","Log Quantity of Firms Receiving Boleto", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    #Cartao
    #twfe2("Cartao_balanced_covid_","log_valor_cartao_debito","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_balanced_covid_","log_valor_cartao_debito","Log Value Debit Card", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    #twfe2("Cartao_balanced_covid_","log_valor_cartao_credito","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_balanced_covid_","log_valor_cartao_credito","Log Value Credit Card", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    #twfe2("Cartao_balanced_covid_","log_qtd_cli_cartao_debito","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_balanced_covid_","log_qtd_cli_cartao_debito","Log Quantity of Firms Accepting Debit Card", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    #twfe2("Cartao_balanced_covid_","log_qtd_cli_cartao_credito","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_balanced_covid_","log_qtd_cli_cartao_credito","Log Quantity of Firms Accepting Credit Card", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    #twfe2("Cartao_balanced_covid_","log_valor_cartao","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_balanced_covid_","log_valor_cartao","Log Value Cards", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    #twfe2("Cartao_balanced_covid_","log_qtd_cli_cartao","constant","constant","flood_risk5",list(dat_b,dat_a), c("before","after"))
    print_twfe_week("Cartao_balanced_covid_","log_qtd_cli_cartao","Log Quantity of Firms Accepting Cards", c("before","after"), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
    
    # + Adocao
    rm(dat_a, dat_b)
    
  }, error = function(e) {
    print(paste("Error in Base_week_muni:", e))
  })
}

# RAIS

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

        for(z in 1:length(variables)){
          #twfe2(beginning_PF,variables[[z]],"constant","constant","flood_risk5", list(dat_rec1, dat_send1), c("rec","send"))
          print_twfe_month(beginning_PF, variables[[z]], variables_labels[[z]], c("rec","send"), c("Received","Sent"), xll, xuu)
          #twfe2(beginning_PF2,variables[[z]],"constant","constant","flood_risk5", list(dat_combined1), c("pix"))
          print_twfe_month(beginning_PF2, variables[[z]], variables_labels[[z]], c("pix"), c("Pix"), xll, xuu)
          
          #twfe2(beginning_PJ,variables[[z]],"constant","constant","flood_risk5", list(dat_rec2, dat_send2), c("rec","send"))
          print_twfe_month(beginning_PJ, variables[[z]], variables_labels[[z]], c("rec","send"), c("Received","Sent"), xll, xuu)
          #twfe2(beginning_PJ2,variables[[z]],"constant","constant","flood_risk5", list(dat_combined2), c("pix"))
          print_twfe_month(beginning_PJ2, variables[[z]], variables_labels[[z]], c("pix"), c("Pix"), xll, xuu)
        }
        #rm(dat_rec1, dat_send1, dat_combined1, dat_rec2, dat_send2, dat_combined2, dat_rec, dat_send, dat_combined)
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
        
        for(z in 1:length(variables)){
          #twfe2(beginning_p2p,variables[[z]],"constant","constant","flood_risk5", list(dat_inflow_p2p, dat_outflow_p2p), c("inflow","outflow"))
          print_twfe_month(beginning_p2p, variables[[z]], variables_labels[[z]], c("inflow","outflow"), c("Receiving","Sending"), xll, xuu)
          
          #twfe2(beginning_PF_self,variables[[z]],"constant","constant","flood_risk5", list(dat_self_PF), c("self"))
          print_twfe_month(beginning_PF_self, variables[[z]], variables_labels[[z]], c("self"), c("Self"), xll, xuu)
          
          #twfe2(beginning_PJ_self,variables[[z]],"constant","constant","flood_risk5", list(dat_self_PJ), c("self"))
          print_twfe_month(beginning_PJ_self, variables[[z]], variables_labels[[z]], c("self"), c("Self"), xll, xuu)
        }
        #twfe2(beginning_p2p,"lusers2","constant","constant","flood_risk5",list(dat_outflow_p2p, dat_inflow_p2p), c("inflow","outflow"))
        print_twfe_month(beginning_p2p,"lusers","Log Active Users Inside the Municipaltiy", c("inflow"), c("Receiving"), xll, xuu)
        print_twfe_month(beginning_p2p,"lusers2","Log Active Users Outside the Municipaltiy", c("outflow"), c("Sending"), xll, xuu)
        
        #rm(dat_inflow_p2p, dat_outflow_p2p, dat, dat_self, dat_self_PF, dat_self_PJ)
      }
    }, error = function(e) {
      print(paste("Error in inflow and outflow:", e))
    })
    
  }, error = function(e) {
    print(paste("Error in Pix_Muni_flow:", e))
  })
}







