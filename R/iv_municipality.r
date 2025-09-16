#iv_municipality
################################################################################
# iv_municipality.R
# Input: 
#         
# Output: 
#         
# y: 

# The goal: 

# To do:  

################################################################################

# https://lost-stats.github.io/Model_Estimation/Research_Design/instrumental_variables.html
# install.packages(c("data.table","fixest","haven","ggplot2","xtable","stargazer",
#                    "lfe","AER","readr","stringr","odbc","dplyr","tidyr","RODBC",
#                    "futile.logger","bit64","gdata","arrow","stargazer","lubridate",
#                    "xtable","ivreg","plm"))
options(download.file.method = "wininet")
rm(list = ls())
install.packages("ppcor")
library(ppcor)

library(plm)
library(pglm)
library("readstata13")
#library(mfx)
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
library(xtable)
library(lfe)
library(AER)
library("ivreg")
library("plm")
library(coefplot)
library(magrittr)

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

# Global settings ---------------------------------------------------------
setwd("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer")

################################################################################
# 0. Functions
################################################################################
source(paste0(R_path,"/functions/stata_week_number.R"))
source(paste0(R_path,"/functions/gen_week_list.R"))
source(paste0(R_path,"/functions/time_id_to_month.R"))
source(paste0(R_path,"/functions/time_id_to_year.R"))
source(paste0(R_path,"/functions/week_to_month.R"))
source(paste0(R_path,"/functions/week_to_year.R"))
source(paste0(R_path,"/functions/prepare_data.R"))
source(paste0(R_path,"/functions/week_to_startDate.R"))
source(paste0(R_path,"/functions/day_to_stata_month.R"))
source(paste0(R_path,"/functions/prepare_data2.R"))
source(paste0(R_path,"/functions/prepare_data3.R"))

source(paste0(R_path,"/functions/twfe.R"))
source(paste0(R_path,"/functions/ylim_function.R"))
source(paste0(R_path,"/functions/print_twfe.R"))
source(paste0(R_path,"/functions/twfe2.R"))
source(paste0(R_path,"/functions/ylim_function2.R"))
source(paste0(R_path,"/functions/print_twfe2.R"))
source(paste0(R_path,"/functions/twfe_ind.R"))
source(paste0(R_path,"/functions/print_twfe_week.R"))
source(paste0(R_path,"/functions/print_twfe_month.R"))
source(paste0(R_path,"/functions/prepare_data_syn_month.R"))

#-------------------------------------------------------------------------------
# Load auxiliary data
#-------------------------------------------------------------------------------
source(paste0(R_path, "/auxiliary_data.r"))
#xl, xu, mun_fe, mun_control, flood_week_after, flood_week_before2019, flood_month_after, flood_month_before2019, flood_week_after2023, flood_week_before2019, flood_month_after2023, flood_month_before2019, flood_week_after_balanced, flood_week_before_balanced, flood_month_after_balanced, flood_month_before_balanced, flood_week_after_balanced2023, flood_week_before2019, flood_month_after_balanced2023, flood_month_before2019, flood_week_after_balanced_covid, flood_week_before_balanced_covid, flood_month_after_balanced_covid, flood_month_before_balanced_covid

################################################################################
# 1. Data Preparation
################################################################################

# Idea, do the data prep and download it. then never repeat it again. 


diff_function <- function(vars, labels, dep_var_label, dat, dat_name) {
  models <- lapply(seq_along(vars), function(i) {
    felm_model <- felm(as.formula(paste(vars[i], "~ after_flood | muni_cd + time:flood_risk5 | 0 | muni_cd")),
                       data = dat)
    return(felm_model)
  })
  names(models) <- labels
  latex_table <- stargazer(models, title = "Differences in Differences", 
                           type = "latex", float = FALSE, align = TRUE, 
                           covariate.labels = c("Flood"),
                           dep.var.labels = c(dep_var_label),
                           column.labels = labels,
                           #omit.stat=c("ser", "adj.rsq", "LL", "f"),
                           no.space=TRUE,
                           single.row = FALSE,
                           header = FALSE, font.size = "small",
                           add.lines = list(c("Mun. FE", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
                                            c("Time x Region FE", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")))
  text_table <- stargazer(models, title = "Differences in Differences", 
                          type = "text", float = FALSE, align = TRUE, 
                          covariate.labels = c("Flood"),
                          dep.var.labels = c(dep_var_label),
                          column.labels = labels,
                          #omit.stat=c("ser", "adj.rsq", "LL", "f"),
                          no.space=TRUE,
                          single.row = FALSE,
                          header = FALSE, font.size = "small",
                          add.lines = list(c("Mun. FE", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
                                           c("Flood Risk x Time FE", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")))
  
  latex_file <- file.path(output_path, paste0("tables/diff_",dep_var_label,"_",dat_name,".tex"))
  cat(latex_table, file = latex_file)
  text_file <- file.path(output_path, paste0("tables/diff_",dep_var_label,"_",dat_name,".txt"))
  cat(text_table, file = text_file)
  return()
}

iv_function <- function(y,y_label,x,x_label,instrument,fe,dat, dat_name){
  dat$Y <- dat[[y]]
  dat$X <- dat[[x]]
  dat$Ins <- dat[[instrument]]
  dat$FE <- dat[[fe]]
  
  dat <- dat[complete.cases(dat$Y), ]
  dat <- dat[complete.cases(dat$X), ]
  dat <- dat[complete.cases(dat$Ins), ]
  dat <- dat[complete.cases(dat$FE), ]
  
  olsmodel <- felm(Y ~ X | muni_cd + time:FE | 0 | muni_cd, data = dat)
  olsmodel2 <- felm(X ~ factor(Ins) | muni_cd + time:FE | 0 | muni_cd, data = dat)
  olsmodel3 <- felm(Y ~ factor(Ins) |  muni_cd + time:FE | 0 | muni_cd, data = dat)
  #dat$X_pred <- dat$X - olsmodel2$residuals
  #olsmodel_IV <- felm(Y ~ X_pred | muni_cd + time:FE, data = dat)
  ivmodel_FE <- felm(Y ~ 0| muni_cd + time:FE | (X ~ factor(Ins)) | muni_cd, data = dat)
  #ivmodel <- felm(Y ~ 0 | 0 | (X ~ Ins), data = dat)
  #ivmodel_munFE <- felm(Y ~ 0 | muni_cd | (X ~ Ins) | muni_cd, data = dat)
  #ivmodel_timeriskFE <- felm(Y ~ 0 | FE:time | (X ~ Ins) | muni_cd, data = dat)
  #ivmodel_FE <- felm(Y ~ 0 | muni_cd + FE:time | (X ~ Ins) | muni_cd, data = dat)
  
  # Combine models into a list
  models_list <- list(olsmodel2, olsmodel3, olsmodel, ivmodel_FE)
  
  # Save models - iv_coefficients
  #save(models_list, file = file.path(output_path, paste0("iv_coefficients/iv_",y,"_",x, dat_name, ".RData")))
  
  results <- lapply(models_list, function(model){
    summary_model <- summary(model)
    
    df <- data.frame(Estimate = coef(summary_model)[,1],
                     `Cluster SE` = coef(summary_model)[,2],
                     `t value` = coef(summary_model)[,3],
                     `Pr(>|t|)` = coef(summary_model)[,4])
    
    list(df = df,
         nobs = length(residuals(model)),
         r.squared = summary_model$r.squared,
         adj.r.squared = summary_model$adj.r.squared)
  })
  
  save(results, file = file.path(output_path, paste0("iv_coefficients/iv_",y,"_",x, dat_name, ".RData")))
  load(file.path(output_path, paste0("iv_coefficients/iv_",y,"_",x, dat_name, ".RData")))
  #rm(olsmodel2, olsmodel3, olsmodel, ivmodel_FE)
  #rm(models_list)
  #create Model list again
  # models_list <- lapply(results, function(result){
  #   model <- list(coefficients = result$coefficients,
  #                 std.error = result$std.error,
  #                 t.value = result$t.value,
  #                 p.value = result$p.value,
  #                 nobs = result$nobs)})
  #r.squared = result$r.squared,
  #adj.r.squared = result$adj.r.squared,
  
  # Create LaTeX table using stargazer
  latex_table <- stargazer(models_list, title = "IV Models",
                           float = FALSE,
                           align = TRUE, 
                           dep.var.labels = c(x_label, y_label, y_label, y_label),
                           covariate.labels = c("Flood", "", x_label),
                           #omit.stat = c("ser", "adj.rsq", "LL", "f"), 
                           no.space = TRUE,
                           single.row = FALSE,
                           header = FALSE, 
                           font.size = "small",
                           add.lines = list(c("Mun. FE", "Yes", "Yes", "Yes", "Yes", "Yes"),
                                            c("Time x Region FE", "Yes", "Yes", "Yes", "Yes", "Yes")),
                           column.labels = c("OLS", "OLS", "OLS", "IV"))
  
  latex_file <- file.path(output_path, paste0("tables/iv_",y,"_",x, dat_name, ".tex"))
  cat(latex_table, file = latex_file)
  text_table <- stargazer(models_list, title = "IV Models", type = "text",
                          float = FALSE,
                          align = TRUE, 
                          dep.var.labels = c(x_label, y_label, y_label, y_label),
                          covariate.labels = c("Flood", "", x_label),
                          #omit.stat = c("ser", "adj.rsq", "LL", "f"), 
                          no.space = TRUE,
                          single.row = FALSE,
                          header = FALSE, 
                          font.size = "small",
                          add.lines = list(c("Mun. FE", "Yes", "Yes", "Yes", "Yes", "Yes"),
                                           c("Time x Region FE", "Yes", "Yes", "Yes", "Yes", "Yes")),
                          column.labels = c("OLS", "OLS", "OLS", "IV"))
  
  text_file <- file.path(output_path, paste0("tables/iv_",y,"_",x, dat_name, ".txt"))
  cat(text_table, file = text_file)
  return()
}

#Function downloads all data
dat_function_week <- function(){
  #Pix_Muni_user.dta
  # Variables: week, muni_cd, tipo, 
  #             users, senders, receivers
  #             lusers, lsenders, lreceivers
  pix_use <- prepare_data2("Pix_Muni_user.dta")
  pix_use_PF <- pix_use %>%
    dplyr::filter(tipo==1) %>%
    dplyr::select(lusers, time, muni_cd) %>%
    dplyr::rename(lusers_PF = lusers)
  pix_use_PJ <- pix_use %>%
    dplyr::filter(tipo==2) %>%
    dplyr::select(lusers, time, muni_cd) %>%
    dplyr::rename(lusers_PJ = lusers)
  pix_use <- merge(pix_use_PF, pix_use_PJ, by=c("muni_cd","time"), all=FALSE)
  # time, muni_cd, lusers_PJ, lusers_PF
  rm(pix_use_PF, pix_use_PJ)
  
  # CCS
  #CCS_Muni_stock_v2.dta
  # Variables: week, muni_cd, tipo, muni_stock, muni_stock_w, banked_pop
  #             lmuni_stock, lmuni_stock_w, lbanked_pop
  ccs_muni_stock <- prepare_data2("CCS_Muni_stock_v2.dta")
  ccs_muni_stock_PF <- ccs_muni_stock %>% dplyr::filter(tipo==1) %>%
    dplyr::select(lmuni_stock, lmuni_stock_w, lbanked_pop, time, muni_cd) %>%
    dplyr::rename(lmuni_stock_w_PF = lmuni_stock_w,
           lbanked_pop_PF = lbanked_pop,
           lmuni_stock_PF = lmuni_stock)
  ccs_muni_stock_PJ <- ccs_muni_stock %>% dplyr::filter(tipo==2) %>%
    dplyr::select(lmuni_stock, lmuni_stock_w, lbanked_pop, time, muni_cd) %>%
    dplyr::rename(lmuni_stock_w_PJ = lmuni_stock_w,
           lbanked_pop_PJ = lbanked_pop,
           lmuni_stock_PJ = lmuni_stock)
  ccs_muni_stock <- merge(ccs_muni_stock_PF, ccs_muni_stock_PJ, by=c("muni_cd","time"), all=FALSE)
  # Sum PJ and PF
  ccs_muni_stock <- ccs_muni_stock %>%
    dplyr::mutate(lmuni_stock = log1p(expm1(lmuni_stock_PF) + expm1(lmuni_stock_PJ)),
           lmuni_stock_w = log1p(expm1(lmuni_stock_w_PF) + expm1(lmuni_stock_w_PJ)),
           lbanked_pop = log1p(expm1(lbanked_pop_PF) + expm1(lbanked_pop_PJ)))
  # time, muni_cd, lmuni_stock_PF, lmuni_stock_w_PF, lbanked_pop_PF, lmuni_stock_PJ, lmuni_stock_w_PJ, lbanked_pop_PJ, lmuni_stock, lmuni_stock_w, lbanked_pop
  rm(ccs_muni_stock_PF, ccs_muni_stock_PJ)
  
  #CCS_Muni_first_account_v2.dta
  # Variables: week, muni_cd, tipo, first_account
  #             lfirst_account
  ccs_first_account <- prepare_data2("CCS_Muni_first_account_v2.dta")
  ccs_first_account <- merge(ccs_first_account, mun_fe, by="muni_cd", all.x = TRUE) 
  ccs_first_account <- ccs_first_account %>%
    dplyr::mutate(f_account_pop = ifelse(is.na(pop2022) | pop2022 == 0 | is.na(first_account), NA, first_account/pop2022))
  ccs_first_account_PF <- ccs_first_account %>% dplyr::filter(tipo==1) %>%
    dplyr::select(time, muni_cd, lfirst_account, f_account_pop, first_account) %>%
    dplyr::rename(lfirst_account_PF = lfirst_account,
           f_account_pop_PF = f_account_pop,
           first_account_PF = first_account)
  ccs_first_account_PJ <- ccs_first_account %>% dplyr::filter(tipo==2) %>%
    dplyr::select(time, muni_cd, lfirst_account, f_account_pop, first_account) %>%
    dplyr::rename(lfirst_account_PJ = lfirst_account,
           f_account_pop_PJ = f_account_pop,
           first_account_PJ = first_account)
  ccs_first_account <- merge(ccs_first_account_PF, ccs_first_account_PJ, by=c("muni_cd","time"), all=FALSE)
  #sum PF and PJ
  ccs_first_account <- ccs_first_account %>%
    dplyr::mutate(lfirst_account = log1p(expm1(lfirst_account_PF) + expm1(lfirst_account_PJ)),
           f_account_pop = ifelse(is.na(f_account_pop_PF) | is.na(f_account_pop_PJ), NA, f_account_pop_PF + f_account_pop_PJ),
           first_account = ifelse(is.na(first_account_PF) | is.na(first_account_PJ), NA, first_account_PF + first_account_PJ))
  # time, muni_cd, lfirst_account_PJ, f_account_pop_PJ, first_account_PJ, lfirst_account_PF, f_account_pop_PF, first_account_PF, lfirst_account, f_account_pop, first_account
  rm(ccs_first_account_PF,ccs_first_account_PJ)
  
  # Card
  # Card_rec.dta
  # Variables: week, muni_cd, tipo, receivers, valor, receivers_credit, valor_credit, receivers_debit, valor_debit
  #            lreceivers, lvalor, lreceivers_credit, lvalor_credit, lreceivers_debit, lvalor_debit  
  card <- prepare_data2("Card_rec.dta")
  card_PF <- card %>% dplyr::filter(tipo==1) %>%
    dplyr::select(time, muni_cd, lreceivers, lvalor, lreceivers_credit, lvalor_credit, lreceivers_debit, lvalor_debit) %>%
    dplyr::rename(lreceivers_card_PF = lreceivers,
           lvalor_card_PF = lvalor,
           lreceivers_credit_PF = lreceivers_credit, 
           lvalor_credit_PF = lvalor_credit, 
           lreceivers_debit_PF = lreceivers_debit, 
           lvalor_debit_PF = lvalor_debit)
  card_PJ <- card %>% dplyr::filter(tipo==2) %>%
    dplyr::select(time, muni_cd, lreceivers, lvalor, lreceivers_credit, lvalor_credit, lreceivers_debit, lvalor_debit) %>%
    dplyr::rename(lreceivers_card_PJ = lreceivers,
           lvalor_card_PJ = lvalor,
           lreceivers_credit_PJ = lreceivers_credit, 
           lvalor_credit_PJ = lvalor_credit, 
           lreceivers_debit_PJ = lreceivers_debit, 
           lvalor_debit_PJ = lvalor_debit)
  card <- merge(card_PF, card_PJ, by=c("muni_cd","time"), all=FALSE)
  # sum PF and PJ
  card <- card %>%
    dplyr::mutate(lreceivers_card = log1p(expm1(lreceivers_card_PF) + expm1(lreceivers_card_PJ)),
           lvalor_card = log1p(expm1(lvalor_card_PF) + expm1(lvalor_card_PJ)),
           lreceivers_credit = log1p(expm1(lreceivers_credit_PF) + expm1(lreceivers_credit_PJ)),
           lvalor_credit = log1p(expm1(lvalor_credit_PF) + expm1(lvalor_credit_PJ)),
           lreceivers_debit = log1p(expm1(lreceivers_debit_PF) + expm1(lreceivers_debit_PJ)),
           lvalor_debit = log1p(expm1(lvalor_debit_PF) + expm1(lvalor_debit_PJ)))
  # time, muni_cd, lreceivers_card_PF, lvalor_card_PF, lreceivers_credit_PF, lvalor_credit_PF, lreceivers_debit_PF, lvalor_debit_PF,
  #                lreceivers_card_PJ, lvalor_card_PJ, lreceivers_credit_PJ, lvalor_credit_PJ, lreceivers_debit_PJ, lvalor_debit_PJ
  #                lreceivers_card, lvalor_card, lreceivers_credit, lvalor_credit, lreceivers_debit, lvalor_debit
  rm(card_PF, card_PJ)
  
  # Boleto
  # Boleto.dta
  # Variables: week, muni_cd, tipo, senders, trans_send, valor_send, receivers, trans_rec, valor_rec
  #             lsenders, ltrans_send, lvalor_send, lreceivers, ltrans_rec, lvalor_rec
  boleto <- prepare_data2("Boleto.dta")
  boleto_PF <- boleto %>% dplyr::filter(tipo==1) %>%
    dplyr::mutate(ltrans_boleto_PF = log1p(trans_send+trans_rec),
           lvalor_boleto_PF = log1p(valor_send+valor_rec)) %>%
    dplyr::select(time, muni_cd, ltrans_boleto_PF, lvalor_boleto_PF, lsenders, ltrans_send, lvalor_send, lreceivers, ltrans_rec, lvalor_rec) %>%
    dplyr::rename(lsenders_boleto_PF = lsenders, 
           ltrans_send_boleto_PF = ltrans_send, 
           lvalor_send_boleto_PF = lvalor_send, 
           lreceivers_boleto_PF =  lreceivers, 
           ltrans_rec_boleto_PF = ltrans_rec, 
           lvalor_rec_boleto_PF = lvalor_rec)
  boleto_PJ <- boleto %>% dplyr::filter(tipo==2) %>%
    dplyr::mutate(ltrans_boleto_PJ = log1p(trans_send+trans_rec),
           lvalor_boleto_PJ = log1p(valor_send+valor_rec)) %>%
    dplyr::select(time, muni_cd, ltrans_boleto_PJ, lvalor_boleto_PJ, lsenders, ltrans_send, lvalor_send, lreceivers, ltrans_rec, lvalor_rec) %>%
    dplyr::rename(lsenders_boleto_PJ = lsenders, 
           ltrans_send_boleto_PJ = ltrans_send, 
           lvalor_send_boleto_PJ = lvalor_send, 
           lreceivers_boleto_PJ =  lreceivers, 
           ltrans_rec_boleto_PJ = ltrans_rec, 
           lvalor_rec_boleto_PJ = lvalor_rec)
  boleto <- merge(boleto_PF, boleto_PJ, by=c("muni_cd","time"), all=FALSE)
  # sum PF and PJ
  boleto <- boleto %>%
    dplyr::mutate(ltrans_boleto = log1p(expm1(ltrans_boleto_PF) + expm1(ltrans_boleto_PJ)),
           lvalor_boleto = log1p(expm1(lvalor_boleto_PF) + expm1(lvalor_boleto_PJ)),
           lsenders_boleto = log1p(expm1(lsenders_boleto_PF) + expm1(lsenders_boleto_PJ)),
           ltrans_send_boleto = log1p(expm1(ltrans_send_boleto_PF) + expm1(ltrans_send_boleto_PJ)),
           lvalor_send_boleto = log1p(expm1(lvalor_send_boleto_PF) + expm1(lvalor_send_boleto_PJ)),
           lreceivers_boleto = log1p(expm1(lreceivers_boleto_PF) + expm1(lreceivers_boleto_PJ)),
           ltrans_rec_boleto = log1p(expm1(ltrans_rec_boleto_PF) + expm1(ltrans_rec_boleto_PJ)),
           lvalor_rec_boleto = log1p(expm1(lvalor_rec_boleto_PF) + expm1(lvalor_rec_boleto_PJ))
    )
  # time, muni_cd, ltrans_boleto_PF, lvalor_boleto_PF, lsenders_boleto_PF, ltrans_send_boleto_PF, lvalor_send_boleto_PF, lreceivers_boleto_PF, ltrans_rec_boleto_PF, lvalor_rec_boleto_PF
  #                ltrans_boleto_PJ, lvalor_boleto_PJ, lsenders_boleto_PJ, ltrans_send_boleto_PJ, lvalor_send_boleto_PJ, lreceivers_boleto_PJ, ltrans_rec_boleto_PJ, lvalor_rec_boleto_PJ
  #                ltrans_boleto, lvalor_boleto, lsenders_boleto, ltrans_send_boleto, lvalor_send_boleto, lreceivers_boleto, ltrans_rec_boleto, lvalor_rec_boleto
  
  # TED
  
  
  # TED, Boleto, Card
  ted_boleto_card <- prepare_data2("Base_week_muni.dta")
  ted_boleto_card <- ted_boleto_card %>%
    dplyr::mutate(log_valor_TED_intra = log1p(valor_TED_intra),
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
           log_valor_cartao = log1p(valor_cartao_debito+valor_cartao_credito)) %>%
    dplyr::select(muni_cd, time, log_valor_TED_intra, log_qtd_TED_intra, log_qtd_cli_TED_rec_PJ, log_qtd_cli_TED_pag_PJ, log_valor_boleto, log_qtd_boleto, log_qtd_cli_pag_pf_boleto, log_qtd_cli_rec_pj_boleto, log_valor_cartao_credito, log_valor_cartao_debito, log_qtd_cli_cartao_debito, log_qtd_cli_cartao_credito, log_qtd_cli_cartao, log_valor_cartao)
  # muni_cd, time, log_valor_TED_intra, log_qtd_TED_intra, log_qtd_cli_TED_rec_PJ, log_qtd_cli_TED_pag_PJ, log_valor_boleto, log_qtd_boleto, log_qtd_cli_pag_pf_boleto, log_qtd_cli_rec_pj_boleto, log_valor_cartao_credito, log_valor_cartao_debito, log_qtd_cli_cartao_debito, log_qtd_cli_cartao_credito, log_qtd_cli_cartao, log_valor_cartao
  
  # MERGE ALL
  dat_main <- merge(pix_use, ccs_muni_stock, by=c("muni_cd","time"), all=TRUE)
  dat_main <- merge(dat_main, ccs_first_account, by=c("muni_cd","time"), all=TRUE)
  dat_main <- merge(dat_main, card, by=c("muni_cd","time"), all=TRUE)
  dat_main <- merge(dat_main, boleto, by=c("muni_cd","time"), all=TRUE)
  dat_main <- merge(dat_main, ted_boleto_card, by=c("muni_cd","time"), all=TRUE)
  
  rm(pix_use, ccs_muni_stock, ccs_first_account, card, boleto, ted_boleto_card)
  
  dat_main$month <- week_to_month(dat_main$time)
  dat_main$year <- week_to_year(dat_main$time)
  
  return(dat_main)   
  # time, muni_cd, month, year
  # lusers_PJ, lusers_PF,
  # lmuni_stock_PF, lmuni_stock_w_PF, lbanked_pop_PF, lmuni_stock_PJ, lmuni_stock_w_PJ, lbanked_pop_PJ, lmuni_stock, lmuni_stock_w, lbanked_pop,
  # lfirst_account_PJ, f_account_pop_PJ, first_account_PJ, lfirst_account_PF, f_account_pop_PF, first_account_PF, lfirst_account, f_account_pop, first_account,
  # lreceivers_card_PF, lvalor_card_PF, lreceivers_credit_PF, lvalor_credit_PF, lreceivers_debit_PF, lvalor_debit_PF,
  # lreceivers_card_PJ, lvalor_card_PJ, lreceivers_credit_PJ, lvalor_credit_PJ, lreceivers_debit_PJ, lvalor_debit_PJ,
  # lreceivers_card, lvalor_card, lreceivers_credit, lvalor_credit, lreceivers_debit, lvalor_debit,
  # ltrans_boleto_PF, lvalor_boleto_PF, lsenders_boleto_PF, ltrans_send_boleto_PF, lvalor_send_boleto_PF, lreceivers_boleto_PF, ltrans_rec_boleto_PF, lvalor_rec_boleto_PF,
  # ltrans_boleto_PJ, lvalor_boleto_PJ, lsenders_boleto_PJ, ltrans_send_boleto_PJ, lvalor_send_boleto_PJ, lreceivers_boleto_PJ, ltrans_rec_boleto_PJ, lvalor_rec_boleto_PJ,
  # ltrans_boleto, lvalor_boleto, lsenders_boleto, ltrans_send_boleto, lvalor_send_boleto, lreceivers_boleto, ltrans_rec_boleto, lvalor_rec_boleto,
  # log_valor_TED_intra, log_qtd_TED_intra, log_qtd_cli_TED_rec_PJ, log_qtd_cli_TED_pag_PJ, log_valor_boleto, log_qtd_boleto, log_qtd_cli_pag_pf_boleto, log_qtd_cli_rec_pj_boleto, log_valor_cartao_credito, log_valor_cartao_debito, log_qtd_cli_cartao_debito, log_qtd_cli_cartao_credito, log_qtd_cli_cartao, log_valor_cartao
}

#Adds controls
dat_flood_function <- function(dat, flood_data, fe, controls){
  setDT(dat)
  # Flood, FE, and Control Variables
  dat <- merge(dat, flood_data, by=c("muni_cd","time"), all=FALSE) # it deletes if no match.
  dat <- merge(dat, fe, by="muni_cd", all.x = TRUE) 
  dat <- merge(dat, controls, by=c("muni_cd","month","year"), all.x = TRUE) 
  
  # Event Study Variables
  dat[, treat := ifelse(is.na(date_flood), 0, 1)]
  dat[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
  dat[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
  dat[, after_flood := ifelse(is.na(date_flood), 0, ifelse(time >= date_flood, 1, 0))]
  
  dat$muni_cd <- factor(dat$muni_cd)
  dat$time <- factor(dat$time)
  dat$flood_risk5 <- factor(dat$flood_risk5)
  
  return(dat)
}

# Transforms weekly to monthly 
dat_week_to_month <- function(dat, value_list){
  setDT(dat)
  # Select variables from the value_list
  dat <- dplyr::select(dat, "time", "muni_cd", one_of(value_list))
  dat <- dat %>%
    dplyr::mutate(startDat = unlist(lapply(time, week_to_startDate)),
           endDat = unlist(lapply(time + 1, week_to_startDate)) - 1,
           diff_days = as.numeric(endDat - startDat), 
           Dat0 = 1 / diff_days,
           Dat1 = 1 / diff_days,
           Dat2 = 1 / diff_days,
           Dat3 = 1 / diff_days,
           Dat4 = 1 / diff_days,
           Dat5 = 1 / diff_days,
           Dat6 = 1 / diff_days,
           Dat7 = ifelse(diff_days > 7, 1 / diff_days, 0),
           Dat8 = ifelse(diff_days > 8, 1 / diff_days, 0)) %>%
    dplyr::select(-diff_days, -endDat)  # Remove the temporary column,  # Convert difftime to numeric
  
  dat <- dat %>%
    pivot_longer(cols = starts_with("Dat"),  # Select columns to pivot
                 names_to = "plus_day",  # New column name for start count
                 values_to = "Weights") %>%  # New column name for start value
    dplyr::mutate(plus_day = as.numeric(sub(".*([0-9])$", "\\1", plus_day)),
           day = as.Date(startDat + plus_day, origin ="1970-01-01"))
  dat <- dat %>%
    dplyr::mutate(time_id = unlist(lapply(day, day_to_stata_month))) %>%
    dplyr::select(-plus_day, -startDat) 
  
  # Multiply values by weights
  dat <- dat %>%
    rowwise() %>%
    dplyr::mutate(across(all_of(value_list), ~ . * Weights))
  
  # Summarize by grouping
  dat <- dat %>%
    dplyr::group_by(time_id, muni_cd) %>%
    dplyr::summarise(across(all_of(value_list), sum, na.rm = TRUE)) %>%
    dplyr::ungroup()
  
  #Get ready to add controls and flood
  dat$time <- dat$time_id
  dat$month <- time_id_to_month(dat$time_id)
  dat$year <- time_id_to_year(dat$time_id)
  
  # Variables: time, muni_cd, month, year, 
  #            value_list
  return(dat)
}

#Add Estban, add credito, also, we have ccs first account on the monthly level. RAIS. 
dat_function_month <- function(){
  
  # Credito_Muni_PF.dta
  # Variables: time_id, muni_cd, new_users, new_users_if, new_users_cg, valor, valor_ativo, users, qtd, banks, valor_w, users_w, qtd_w, valor_cartao, users_cartao, qtd_cartao
  #                                 Plus l variations
  credito_PF <- prepare_data2("Credito_Muni_PF.dta")
  credito_PF <- credito_PF %>%
    dplyr::select(time, muni_cd, lnew_users, lnew_users_if, lnew_users_cg, lvalor, lvalor_ativo, lusers, lqtd, lbanks, lvalor_w, lusers_w, lqtd_w, lvalor_cartao, lusers_cartao, lqtd_cartao) %>%
    dplyr::rename(lnew_users_PF = lnew_users,
           lnew_users_if_PF = lnew_users_if,
           lnew_users_cg_PF = lnew_users_cg,
           lvalor_PF = lvalor,
           lvalor_ativo_PF = lvalor_ativo,
           lusers_c_PF = lusers,
           lqtd_PF = lqtd,
           lbanks_PF = lbanks,
           lvalor_w_PF = lvalor_w,
           lusers_w_PF = lusers_w,
           lqtd_w_PF = lqtd_w,
           lvalor_cartao_PF = lvalor_cartao,
           lusers_cartao_PF = lusers_cartao,
           lqtd_cartao_PF = lqtd_cartao)
  
  # Credito_Muni_PJ.dta
  # Variables: time_id, muni_cd, new_users, new_users_if, new_users_cg, valor, valor_ativo, users, qtd, banks,
  #                                 Plus l variations
  credito_PJ <- prepare_data2("Credito_Muni_PJ.dta")
  credito_PJ <- credito_PJ %>%
    dplyr::select(time, muni_cd, lnew_users, lnew_users_if, lnew_users_cg, lvalor, lvalor_ativo, lusers, lqtd, lbanks) %>%
    dplyr::rename(lnew_users_PJ = lnew_users,
           lnew_users_if_PJ = lnew_users_if,
           lnew_users_cg_PJ = lnew_users_cg,
           lvalor_PJ = lvalor,
           lvalor_ativo_PJ = lvalor_ativo,
           lusers_c_PJ = lusers,
           lqtd_PJ = lqtd,
           lbanks_PJ = lbanks)
  
  credito <- merge(credito_PF, credito_PJ, by=c("muni_cd","time"), all=TRUE)
  # Variables: time, muni_cd, lnew_users_PF, lnew_users_if_PF, lnew_users_cg_PF, lvalor_PF, lvalor_ativo_PF, lusers_c_PF, lqtd_PF, lbanks_PF, lvalor_w_PF, lusers_w_PF, lqtd_w_PF, lvalor_cartao_PF, lusers_cartao_PF, lqtd_cartao_PF,
  #            lnew_users_PJ, lnew_users_if_PJ, lnew_users_cg_PJ, lvalor_PJ, lvalor_ativo_PJ, lusers_c_PJ, lqtd_PJ, lbanks_PJ
  rm(credito_PF, credito_PJ)
  
  # ESTBAN
  #log_caixa, log_total_deposits, log_poupanca, log_dep_prazo, log_dep_vista_PF, log_dep_vista_PJ
  estban <- prepare_data2("Estban_detalhado_flood_collapsed2.dta")
  estban <- estban %>%
    dplyr::select(time, muni_cd, log_caixa, log_total_deposits, log_poupanca, log_dep_prazo, log_dep_vista_PF, log_dep_vista_PJ)
  # Variables: time, muni_cd, log_caixa, log_total_deposits, log_poupanca, log_dep_prazo, log_dep_vista_PF, log_dep_vista_PJ
  
  # Credito old
  credito_old <- prepare_data2("Base_credito_muni_flood.dta")
  credito_old <- credito_old %>%
    dplyr::select(time, muni_cd, log_vol_cartao, log_qtd_cli_cartao, log_vol_emprestimo_pessoal, log_qtd_cli_emp_pessoal, log_vol_credito_total, log_qtd_cli_total, log_vol_credito_total_PF, log_qtd_cli_total_PF, log_vol_credito_total_PJ, log_qtd_cli_total_PJ)
  # Variables: time, muni_cd, log_vol_cartao, log_qtd_cli_cartao, log_vol_emprestimo_pessoal, log_qtd_cli_emp_pessoal, log_vol_credito_total, log_qtd_cli_total, log_vol_credito_total_PF, log_qtd_cli_total_PF, log_vol_credito_total_PJ, log_qtd_cli_total_PJ
  
  
  # MERGE ALL
  dat_main <- merge(credito, estban, by=c("muni_cd","time"), all=TRUE)
  dat_main <- merge(dat_main, credito_old, by=c("muni_cd","time"), all=TRUE)
  rm(credito, estban, credito_old)
  
  dat_main$month <- time_id_to_month(dat_main$time)
  dat_main$year <- time_id_to_year(dat_main$time)
  
  # Variables: time, muni_cd, month, year, 
  #            lnew_users_PF, lnew_users_if_PF, lnew_users_cg_PF, lvalor_PF, lvalor_ativo_PF, lusers_c_PF, lqtd_PF, lbanks_PF, lvalor_w_PF, lusers_w_PF, lqtd_w_PF, lvalor_cartao_PF, lusers_cartao_PF, lqtd_cartao_PF,
  #            lnew_users_PJ, lnew_users_if_PJ, lnew_users_cg_PJ, lvalor_PJ, lvalor_ativo_PJ, lusers_c_PJ, lqtd_PJ, lbanks_PJ,
  #            log_caixa, log_total_deposits, log_poupanca, log_dep_prazo, log_dep_vista_PF, log_dep_vista_PJ
  #            log_vol_cartao, log_qtd_cli_cartao, log_vol_emprestimo_pessoal, log_qtd_cli_emp_pessoal, log_vol_credito_total, log_qtd_cli_total, log_vol_credito_total_PF, log_qtd_cli_total_PF, log_vol_credito_total_PJ, log_qtd_cli_total_PJ
  return(dat_main)
}

# Function that creates nice tables
iv_function_new <- function(y_list,y_label_list,x,x_label,instrument,fe,dat, dat_name, file_name){
  dat$X <- dat[[x]]
  dat$Ins <- dat[[instrument]]
  dat$FE <- dat[[fe]]
  
  dat <- dat[complete.cases(dat$X), ]
  dat <- dat[complete.cases(dat$Ins), ]
  dat <- dat[complete.cases(dat$FE), ]
  olsmodel2 <- felm(X ~ factor(Ins) | muni_cd + time:FE | 0 | muni_cd, data = dat)
  models_list <- list(olsmodel2)
  
  ols_summary <- summary(olsmodel2)
  first_stage_fstat <- round(ols_summary$fstat[1],3)
  print(paste("F-statistic of the model:", first_stage_fstat))
  f_statistics <- c("F-stat", first_stage_fstat)
  
  dep_var_labels <- c("OLS")
  add_lines1 <- c("Mun. FE", "Yes")
  add_lines2 <- c("Time x Region FE", "Yes")
  column_labels <- c(x_label)
  for(i in 1:length(y_list)){
    dat$Y <- dat[[y_list[i]]]
    dat_temp <- dat[complete.cases(dat$Y), ]
    ivmodel_FE <- felm(Y ~ 0| muni_cd + time:FE | (X ~ factor(Ins)) | muni_cd, data = dat_temp)
    
    # Extract the first-stage F-statistic
    first_stage_summary <- summary(ivmodel_FE, stage = 1)  # Get first-stage results
    first_stage_fstat <- round(first_stage_summary$fstat[1], 3)  # Extract and round the F-statistic to 3 decimals
    print(paste("First-stage F-statistic for IV model", i, ":", first_stage_fstat))  # Print the F-statistic
    f_statistics <- c(f_statistics, first_stage_fstat)
    
    # append model to model_list
    models_list[[i+1]] <- ivmodel_FE
    dep_var_labels <- c(dep_var_labels, "IV")
    add_lines1 <- c(add_lines1, "Yes")
    add_lines2 <- c(add_lines2, "Yes")
    column_labels <- c(column_labels, y_label_list[[i]])
  }
  olsmodel2 <- felm(X ~ factor(Ins) | muni_cd + time:FE | 0 | muni_cd, data = dat)
  models_list[[1]] <- list(olsmodel2)
  
  # Create LaTeX table using stargazer
  latex_table <- stargazer(models_list, title = "IV Models",
                           float = FALSE,
                           align = TRUE, 
                           dep.var.labels = dep_var_labels,
                           covariate.labels = c("Flood", x_label),
                           #omit.stat = c("ser", "adj.rsq", "LL", "f"), 
                           no.space = TRUE,
                           single.row = FALSE,
                           header = FALSE, 
                           font.size = "small",
                           add.lines = list(add_lines1,
                                            add_lines2,
                                            f_statistics),
                           column.labels = column_labels)
  
  latex_file <- file.path(output_path, paste0("tables/iv_",file_name, dat_name, ".tex")) 
  cat(latex_table, file = latex_file)
  text_table <- stargazer(models_list, title = "IV Models", type = "text",
                          float = FALSE,
                          align = TRUE, 
                          dep.var.labels = dep_var_labels,
                          covariate.labels = c("Flood", x_label),
                          #omit.stat = c("ser", "adj.rsq", "LL", "f"), 
                          no.space = TRUE,
                          single.row = FALSE,
                          header = FALSE, 
                          font.size = "small",
                          add.lines = list(add_lines1,
                                           add_lines2,
                                           f_statistics),
                          column.labels = column_labels)
  
  text_file <- file.path(output_path, paste0("tables/iv_", file_name, dat_name, ".txt")) 
  cat(text_table, file = text_file)
  return()
}

# Function that creates nice tables

iv_function_ind_new <- function(y_list,y_label_list,x,x_label,instrument,fe,dat, dat_name, file_name){
  print(paste("Initial number of rows in the dataset:", nrow(dat)))  # Check number of rows after each merge or transformation
  dat$X <- dat[[x]]
  dat$Ins <- dat[[instrument]]
  dat$FE <- dat[[fe]]
  print(paste("Number of missing values in", x, "(X):", sum(is.na(dat$X))))  # Check for missing values in 'X'
  print(paste("Number of missing values in", instrument, "(Ins):", sum(is.na(dat$Ins))))  # Check for missing values in 'Ins'
  print(paste("Number of missing values in", fe, "(FE):", sum(is.na(dat$FE))))  # Check for missing values in 'FE'
  dat <- dat[complete.cases(dat$X), ]
  dat <- dat[complete.cases(dat$Ins), ]
  dat <- dat[complete.cases(dat$FE), ]
  print(paste("Number of rows after removing missing values:", nrow(dat)))  # Check number of rows after filtering
  print("Data types and structure of the dataset:")  # Check the types of all relevant variables
  str(dat)
  olsmodel2 <- felm(X ~ factor(Ins) | id + time:FE | 0 | id, data = dat)
  
  ols_summary <- summary(olsmodel2)
  first_stage_fstat <- round(ols_summary$fstat[1],3)
  print(paste("F-statistic of the model:", first_stage_fstat))
  f_statistics <- c("F-stat", first_stage_fstat)
  
  print("OLS model estimation (first model) is done")
  models_list <- list(olsmodel2)
  dep_var_labels <- c("OLS")
  add_lines1 <- c("Ind. FE", "Yes")
  add_lines2 <- c("Time x Region FE", "Yes")
  column_labels <- c(x_label)
  for(i in 1:length(y_list)){
    dat$Y <- dat[[y_list[i]]]
    dat_temp <- dat[complete.cases(dat$Y), ]
    print(paste("Running IV model", i, "for dependent variable:", y_list[i]))  # Notify model progress
    ivmodel_FE <- felm(Y ~ 0| id + time:FE | (X ~ factor(Ins)) | id, data = dat_temp)
    # Extract the first-stage F-statistic
    first_stage_summary <- summary(ivmodel_FE, stage = 1)  # Get first-stage results
    first_stage_fstat <- round(first_stage_summary$fstat[1], 3)  # Extract and round the F-statistic to 3 decimals
    print(paste("First-stage F-statistic for IV model", i, ":", first_stage_fstat))  # Print the F-statistic
    f_statistics <- c(f_statistics, first_stage_fstat)
    
    # append model to model_list
    models_list[[i+1]] <- ivmodel_FE
    dep_var_labels <- c(dep_var_labels, "IV")
    add_lines1 <- c(add_lines1, "Yes")
    add_lines2 <- c(add_lines2, "Yes")
    column_labels <- c(column_labels, y_label_list[[i]])
    print(paste("IV model", i, "for", y_list[i], "is done"))  # Notify completion of model
  }
  olsmodel2 <- felm(X ~ factor(Ins) | id + time:FE | 0 | id, data = dat)
  models_list[[1]] <- list(olsmodel2)
  print("Final model re-estimation completed")
  # Create LaTeX table using stargazer
  latex_table <- stargazer(models_list, title = "IV Models",
                           float = FALSE,
                           align = TRUE, 
                           dep.var.labels = dep_var_labels,
                           covariate.labels = c("Flood", x_label),
                           #omit.stat = c("ser", "adj.rsq", "LL", "f"), 
                           no.space = TRUE,
                           single.row = FALSE,
                           header = FALSE, 
                           font.size = "small",
                           add.lines = list(add_lines1,
                                            add_lines2,
                                            f_statistics),
                           column.labels = column_labels)
  print("LaTeX table created and saved")
  latex_file <- file.path(output_path, paste0("tables/iv_",file_name, dat_name, ".tex")) 
  cat(latex_table, file = latex_file)
  text_table <- stargazer(models_list, title = "IV Models", type = "text",
                          float = FALSE,
                          align = TRUE, 
                          dep.var.labels = dep_var_labels,
                          covariate.labels = c("Flood", x_label),
                          #omit.stat = c("ser", "adj.rsq", "LL", "f"), 
                          no.space = TRUE,
                          single.row = FALSE,
                          header = FALSE, 
                          font.size = "small",
                          add.lines = list(add_lines1,
                                           add_lines2,
                                           f_statistics),
                          column.labels = column_labels)
  
  text_file <- file.path(output_path, paste0("tables/iv_", file_name, dat_name, ".txt")) 
  print("Text table created and saved")
  cat(text_table, file = text_file)
  return()
}


iv_function_ind_new2 <- function(y_list,y_label_list,x,x_label,instrument,fe,dat, dat_name, file_name){
  print(paste("Initial number of rows in the dataset:", nrow(dat)))  # Check number of rows after each merge or transformation
  dat$X <- dat[[x]]
  dat$Ins <- dat[[instrument]]
  dat$FE <- dat[[fe]]
  str(dat)
  #print(paste("Number of missing values in", x, "(X):", sum(is.na(dat$X))))  # Check for missing values in 'X'
  #print(paste("Number of missing values in", instrument, "(Ins):", sum(is.na(dat$Ins))))  # Check for missing values in 'Ins'
  #print(paste("Number of missing values in", fe, "(FE):", sum(is.na(dat$FE))))  # Check for missing values in 'FE'
  if ("X" %in% names(dat)) {
    print(paste("Number of missing values in", x, "(X):", sum(is.na(dat$X))))  # Check for missing values in 'X'
  } else {
    print(paste("Variable", x, "(X) does not exist in the dataset."))
  }
  if ("Ins" %in% names(dat)) {
    print(paste("Number of missing values in", instrument, "(Ins):", sum(is.na(dat$Ins))))  # Check for missing values in 'X'
  } else {
    print(paste("Variable", instrument, "(Ins) does not exist in the dataset."))
  }
  if ("FE" %in% names(dat)) {
    print(paste("Number of missing values in", fe, "(FE):", sum(is.na(dat$FE))))  # Check for missing values in 'X'
  } else {
    print(paste("Variable", fe, "(FE) does not exist in the dataset."))
  }
  dat <- dat[complete.cases(dat$X), ]
  dat <- dat[complete.cases(dat$Ins), ]
  dat <- dat[complete.cases(dat$FE), ]
  print(paste("Number of rows after removing missing values:", nrow(dat)))  # Check number of rows after filtering
  print("Data types and structure of the dataset:")  # Check the types of all relevant variables
  str(dat)
  olsmodel2 <- felm(X ~ factor(Ins) | muni_cd + time:FE | 0 | muni_cd, data = dat)
  
  ols_summary <- summary(olsmodel2)
  first_stage_fstat <- round(ols_summary$fstat[1],3)
  print(paste("F-statistic of the model:", first_stage_fstat))
  f_statistics <- c("F-stat", first_stage_fstat)
  
  print("OLS model estimation (first model) is done")
  models_list <- list(olsmodel2)
  dep_var_labels <- c("OLS")
  add_lines1 <- c("Mun. FE", "Yes")
  add_lines2 <- c("Time x Flood Risk FE", "Yes")
  column_labels <- c(x_label)
  
  for(i in 1:length(y_list)){
    dat$Y <- dat[[y_list[i]]]
    dat_temp <- dat[complete.cases(dat$Y), ]
    print(paste("Running IV model", i, "for dependent variable:", y_list[i]))  # Notify model progress
    ivmodel_FE <- felm(Y ~ 0| muni_cd + time:FE | (X ~ factor(Ins)) | muni_cd, data = dat_temp)
    
    # Extract the first-stage F-statistic
    first_stage_summary <- summary(ivmodel_FE, stage = 1)  # Get first-stage results
    first_stage_fstat <- round(first_stage_summary$fstat[1], 3)  # Extract and round the F-statistic to 3 decimals
    print(paste("First-stage F-statistic for IV model", i, ":", first_stage_fstat))  # Print the F-statistic
    f_statistics <- c(f_statistics, first_stage_fstat)
    
    # append model to model_list
    models_list[[i+1]] <- ivmodel_FE
    dep_var_labels <- c(dep_var_labels, "IV")
    add_lines1 <- c(add_lines1, "Yes")
    add_lines2 <- c(add_lines2, "Yes")
    column_labels <- c(column_labels, y_label_list[[i]])
    print(paste("IV model", i, "for", y_list[i], "is done"))  # Notify completion of model
  }
  olsmodel2 <- felm(X ~ factor(Ins) | muni_cd + time:FE | 0 | muni_cd, data = dat)
  models_list[[1]] <- list(olsmodel2)
  print("Final model re-estimation completed")
  # Create LaTeX table using stargazer
  latex_table <- stargazer(models_list, title = "IV Models",
                           float = FALSE,
                           align = TRUE, 
                           dep.var.labels = dep_var_labels,
                           covariate.labels = c("Flood", x_label),
                           #omit.stat = c("ser", "adj.rsq", "LL"), # , "f"
                           no.space = TRUE,
                           single.row = FALSE,
                           header = FALSE, 
                           font.size = "small",
                           add.lines = list(add_lines1,
                                            add_lines2,
                                            f_statistics),
                           column.labels = column_labels)
  print("LaTeX table created and saved")
  latex_file <- file.path(output_path, paste0("tables/iv2_",file_name, dat_name, ".tex")) 
  cat(latex_table, file = latex_file)
  text_table <- stargazer(models_list, title = "IV Models", type = "text",
                          float = FALSE,
                          align = TRUE, 
                          dep.var.labels = dep_var_labels,
                          covariate.labels = c("Flood", x_label),
                          #omit.stat = c("ser", "adj.rsq", "LL"), #, "f"
                          no.space = TRUE,
                          single.row = FALSE,
                          header = FALSE, 
                          font.size = "small",
                          add.lines = list(add_lines1,
                                           add_lines2,
                                           f_statistics),
                          column.labels = column_labels)
  
  text_file <- file.path(output_path, paste0("tables/iv2_", file_name, dat_name, ".txt")) 
  print("Text table created and saved")
  cat(text_table, file = text_file)
  return()
}

# Create new stuff to download data from the new dta, like card_rec, boleto, ted, ccs, credito, also, get ind_sample, also adoption
# 6 months?



################################################################################
# 3. IV + Diff-in-Diff
################################################################################



if(!file.exists(file.path(dta_path, "weekly_database.parquet"))){
weekly_database <- dat_function_week()
write_parquet(weekly_database, sink = paste0(dta_path, "weekly_database.parquet"))
} else {
weekly_database <- read_parquet(file = file.path(dta_path, "weekly_database.parquet"))
}

if(!file.exists(file.path(dta_path, "monthly_database.parquet"))){
  tryCatch({
    # value_list <- c("lusers_PJ", "lusers_PF",
    #                 "lmuni_stock_PF", "lmuni_stock_w_PF", "lbanked_pop_PF", "lmuni_stock_PJ", "lmuni_stock_w_PJ", "lbanked_pop_PJ", "lmuni_stock", "lmuni_stock_w", "lbanked_pop",
    #                 "lfirst_account_PJ", "f_account_pop_PJ", "first_account_PJ", "lfirst_account_PF", "f_account_pop_PF", "first_account_PF", "lfirst_account", "f_account_pop", "first_account",
    #                 "lreceivers_card_PF", "lvalor_card_PF", "lreceivers_credit_PF", "lvalor_credit_PF", "lreceivers_debit_PF", "lvalor_debit_PF",
    #                 "lreceivers_card_PJ", "lvalor_card_PJ", "lreceivers_credit_PJ", "lvalor_credit_PJ", "lreceivers_debit_PJ", "lvalor_debit_PJ",
    #                 "lreceivers_card", "lvalor_card", "lreceivers_credit", "lvalor_credit", "lreceivers_debit", "lvalor_debit",
    #                 "ltrans_boleto_PF", "lvalor_boleto_PF", "lsenders_boleto_PF", "ltrans_send_boleto_PF", "lvalor_send_boleto_PF", "lreceivers_boleto_PF", "ltrans_rec_boleto_PF", "lvalor_rec_boleto_PF",
    #                 "ltrans_boleto_PJ", "lvalor_boleto_PJ", "lsenders_boleto_PJ", "ltrans_send_boleto_PJ", "lvalor_send_boleto_PJ", "lreceivers_boleto_PJ", "ltrans_rec_boleto_PJ", "lvalor_rec_boleto_PJ",
    #                 "ltrans_boleto", "lvalor_boleto", "lsenders_boleto", "ltrans_send_boleto", "lvalor_send_boleto", "lreceivers_boleto", "ltrans_rec_boleto", "lvalor_rec_boleto",
    #                 "log_valor_TED_intra", "log_qtd_TED_intra", "log_qtd_cli_TED_rec_PJ", "log_qtd_cli_TED_pag_PJ", "log_valor_boleto", "log_qtd_boleto", "log_qtd_cli_pag_pf_boleto", "log_qtd_cli_rec_pj_boleto", "log_valor_cartao_credito", "log_valor_cartao_debito", "log_qtd_cli_cartao_debito", "log_qtd_cli_cartao_credito", "log_qtd_cli_cartao", "log_valor_cartao")
    value_list <- c("lusers_PF")
    monthly_database2 <- dat_week_to_month(weekly_database, value_list)
    # Variables: time, muni_cd, month, year,
    #            value_list
    monthly_database <- dat_function_month()
    # Variables: time, muni_cd, month, year,
    #            lnew_users_PF, lnew_users_if_PF, lnew_users_cg_PF, lvalor_PF, lvalor_ativo_PF, lusers_c_PF, lqtd_PF, lbanks_PF, lvalor_w_PF, lusers_w_PF, lqtd_w_PF, lvalor_cartao_PF, lusers_cartao_PF, lqtd_cartao_PF,
    #            lnew_users_PJ, lnew_users_if_PJ, lnew_users_cg_PJ, lvalor_PJ, lvalor_ativo_PJ, lusers_c_PJ, lqtd_PJ, lbanks_PJ,
    #            log_caixa, log_total_deposits, log_poupanca, log_dep_prazo, log_dep_vista_PF, log_dep_vista_PJ
    #            log_vol_cartao, log_qtd_cli_cartao, log_vol_emprestimo_pessoal, log_qtd_cli_emp_pessoal, log_vol_credito_total, log_qtd_cli_total, log_vol_credito_total_PF, log_qtd_cli_total_PF, log_vol_credito_total_PJ, log_qtd_cli_total_PJ
  
    # Merge
    monthly_database <- merge(monthly_database, monthly_database2, by=c("muni_cd","time","month","year"), all=FALSE)

    dat_pix_month <- prepare_data2("Pix_Muni_user_month.dta")
    dat_pix_month <- dat_pix_month %>% dplyr::filter(tipo==1) %>% 
      dplyr::select(muni_cd, time, lusers) %>%
      dplyr::rename(lusers_pix_pf = lusers)
    
    monthly_database <- merge(dat_pix_month, monthly_database, by=c("muni_cd","time"), all=TRUE)
    
    # save in parquet
    write_parquet(monthly_database, sink = paste0(dta_path, "monthly_database.parquet"))
  }, error = function(e) {
    print(paste("Error in monthly_database:", e))
  })
} else {
  monthly_database <- read_parquet(file = file.path(dta_path, "monthly_database.parquet"))
}
#Pix_Muni_Bank.dta and Pix_Muni_Bank_self.dta
# Variables:  week, muni_cd, tipo, bank, tipo_inst, bank_type,
#             value_send, trans_send, send_users, value_send_w, value_rec, trans_rec, rec_users, value_rec_w
#             lvalue_send, ltrans_send, lsend_users, lvalue_send_w, lvalue_rec, ltrans_rec, lrec_users, lvalue_rec_w

pix_accounts <- prepare_data2("Pix_Muni_Bank.dta")
pix_accounts <- pix_accounts %>%
  dplyr::select(time, muni_cd, tipo, rec_users, send_users) %>%
  dplyr::group_by(time, muni_cd, tipo) %>%
  dplyr::summarise(send_users = sum(send_users, na.rm = TRUE),
            rec_users = sum(rec_users, na.rm = TRUE)) %>%
  dplyr::ungroup()

pix_accounts_PF <- pix_accounts %>%
  dplyr::filter(tipo == 1) %>%
  dplyr::mutate(users = rec_users + send_users) %>%
  dplyr::mutate(laccounts_pf = log1p(users)) %>%
  dplyr::select(time, muni_cd, laccounts_pf)
pix_accounts_PJ <- pix_accounts %>%
  dplyr::filter(tipo == 2) %>%
  dplyr::mutate(users = rec_users + send_users) %>%
  dplyr::mutate(laccounts_pj = log1p(users)) %>%
  dplyr::select(time, muni_cd, laccounts_pj)
weekly_database <- merge(weekly_database, pix_accounts_PF, by=c("muni_cd","time"), all=TRUE)
weekly_database <- merge(weekly_database, pix_accounts_PJ, by=c("muni_cd","time"), all=TRUE)
rm(pix_accounts, pix_accounts_PF, pix_accounts_PJ)

# Pix_Muni_flow.dta
# Variables: flow_code, week, muni_cd, sender_type, receiver_type, senders, receivers, valor, trans, valor_w,
#             lsenders, lreceivers, lvalor, ltrans, lvalor_w
pix_self <- prepare_data2("Pix_Muni_flow.dta")
pix_self <- pix_self %>%
  dplyr::filter(flow_code == 99) %>% dplyr::select(time, muni_cd, receiver_type, receivers, valor, trans, valor_w) %>%
  dplyr::mutate(lreceivers = log1p(receivers),
         lvalor = log1p(valor),
         ltrans = log1p(trans),
         lvalor_w = log1p(valor_w))
pix_self_PF <- pix_self %>%
  dplyr::filter(receiver_type == 1) %>%
  dplyr::rename(lpix_self_pf_users = lreceivers, lpix_self_pf_value = lvalor, lpix_self_pf_trans = ltrans, lpix_self_pf_value_w = lvalor_w) %>%
  dplyr::select(time, muni_cd, lpix_self_pf_users, lpix_self_pf_value, lpix_self_pf_trans, lpix_self_pf_value_w)
pix_self_PJ <- pix_self %>%
  dplyr::filter(receiver_type == 2) %>%
  dplyr::rename(lpix_self_pj_users = lreceivers, lpix_self_pj_value = lvalor, lpix_self_pj_trans = ltrans, lpix_self_pj_value_w = lvalor_w) %>%
  dplyr::select(time, muni_cd, lpix_self_pj_users, lpix_self_pj_value, lpix_self_pj_trans, lpix_self_pj_value_w)
weekly_database <- merge(weekly_database, pix_self_PF, by=c("muni_cd","time"), all=TRUE)
weekly_database <- merge(weekly_database, pix_self_PJ, by=c("muni_cd","time"), all=TRUE)
rm(pix_self, pix_self_PF, pix_self_PJ)


# ADD NEW TED!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

# TED.dta
# Variables: flow_code, week, muni_cd, sender_type, receiver_type, senders, receivers, valor, trans,
#             lsenders, lreceivers, lvalor, ltrans

# TED_aggreg.dta
# Variables: week, muni_cd, sender_type, receiver_type, 
#             senders_rec, receivers_rec, valor_rec, trans_rec 
#             senders_sent, receivers_sent, valor_sent, trans_sent
# Plus l variations. 

# TED_aggreg_rec.dta
# Variables: week, muni_cd, tipo, 
#             trans, valor,
#             ltrans, lvalor

# TED_aggreg_send.dta
# Variables: week, muni_cd, tipo, 
#             trans, valor,
#             ltrans, lvalor

ted_new_rec <- prepare_data2("TED_aggreg_rec.dta")
ted_new_rec <- ted_new_rec %>%
  dplyr::select(time, muni_cd, tipo, trans, valor)
ted_new_send <- prepare_data2("TED_aggreg_send.dta")
ted_new_send <- ted_new_send %>%
  dplyr::select(time, muni_cd, tipo, trans, valor)
ted_new <- rbind(ted_new_rec, ted_new_send)
rm(ted_new_rec, ted_new_send)
ted_new_pf <- ted_new %>%
  dplyr::filter(tipo == 1) %>%
  dplyr::group_by(time, muni_cd) %>%
  dplyr::summarise(lvalor_ted_new_pf = log1p(sum(valor, na.rm = TRUE)),
            ltrans_ted_new_pf = log1p(sum(trans, na.rm = TRUE)))
ted_new_pj <- ted_new %>%
  dplyr::filter(tipo == 2) %>%
  dplyr::group_by(time, muni_cd) %>%
  dplyr::summarise(lvalor_ted_new_pj = log1p(sum(valor, na.rm = TRUE)),
            ltrans_ted_new_pj = log1p(sum(trans, na.rm = TRUE)))
ted_new <- ted_new %>%
  dplyr::group_by(time, muni_cd) %>%
  dplyr::summarise(lvalor_ted_new = log1p(sum(valor, na.rm = TRUE)),
            ltrans_ted_new = log1p(sum(trans, na.rm = TRUE)))
weekly_database <- merge(weekly_database, ted_new_pf, by=c("muni_cd","time"), all=TRUE)
weekly_database <- merge(weekly_database, ted_new_pj, by=c("muni_cd","time"), all=TRUE)
weekly_database <- merge(weekly_database, ted_new, by=c("muni_cd","time"), all=TRUE)
rm(ted_new, ted_new_pf, ted_new_pj)
# Variables: time, muni_cd, lvalor_ted_new_pf, ltrans_ted_new_pf, lvalor_ted_new_pj, ltrans_ted_new_pj, lvalor_ted_new, ltrans_ted_new

# Add TED + Boleto
# lvalor_ted_new_pf, ltrans_ted_new_pf, lvalor_ted_new_pj, ltrans_ted_new_pj, lvalor_ted_new, ltrans_ted_new

#                     "ltrans_boleto_PF", "lvalor_boleto_PF", "lsenders_boleto_PF", "ltrans_send_boleto_PF", "lvalor_send_boleto_PF", "lreceivers_boleto_PF", "ltrans_rec_boleto_PF", "lvalor_rec_boleto_PF",
# "ltrans_boleto_PJ", "lvalor_boleto_PJ", "lsenders_boleto_PJ", "ltrans_send_boleto_PJ", "lvalor_send_boleto_PJ", "lreceivers_boleto_PJ", "ltrans_rec_boleto_PJ", "lvalor_rec_boleto_PJ",
# "ltrans_boleto", "lvalor_boleto", "lsenders_boleto", "ltrans_send_boleto", "lvalor_send_boleto", "lreceivers_boleto", "ltrans_rec_boleto", "lvalor_rec_boleto",

# Two main variables:
# ltrans_ted_boleto_PJ_rec, lvalor_ted_boleto_PJ_rec
# ltrans_ted_boleto, lvalor_ted_boleto

weekly_database <- weekly_database %>%
  dplyr::mutate(ltrans_ted_boleto_PJ_rec = log1p(exp(ltrans_ted_new_pj) - 1 + exp(ltrans_rec_boleto_PJ) - 1),
         lvalor_ted_boleto_PJ_rec = log1p(exp(lvalor_ted_new_pj) - 1 + exp(lvalor_rec_boleto_PJ) - 1),
         ltrans_ted_boleto = log1p(exp(ltrans_ted_new) - 1 + exp(ltrans_boleto) - 1),
         lvalor_ted_boleto = log1p(exp(lvalor_ted_new) - 1 + exp(lvalor_boleto) - 1))

run_week <- 0
run_month <- 0

if(run_week == 1){
  tryCatch({
    for(i in 2:2){
      if(i==1){
        flood_a <- flood_week_after
        flood_b <- flood_week_before2019
        ending <- ""
      }
      if(i==2){
        flood_a <- flood_week_after_balanced
        flood_b <- flood_week_before2019
        ending <- "_balanced"
      }
      if(i==3){
        flood_a <- flood_week_after_balanced_covid
        flood_b <- flood_week_before_balanced_covid
        ending <- "_balanced_covid"
      }
      
      dat_a <- dat_flood_function(weekly_database, flood_a, mun_fe, mun_control)
      dat_b <- dat_flood_function(weekly_database, flood_b, mun_fe, mun_control)

      variables <- c("ltrans_ted_boleto_PJ_rec", "lvalor_ted_boleto_PJ_rec", "ltrans_ted_boleto", "lvalor_ted_boleto",
                     "lvalor_ted_new_pf", "ltrans_ted_new_pf", "lvalor_ted_new_pj", "ltrans_ted_new_pj", "lvalor_ted_new", "ltrans_ted_new",
                     "laccounts_pf", "laccounts_pj", 
                     "lpix_self_pf_users", "lpix_self_pf_value", "lpix_self_pf_trans", "lpix_self_pf_value_w", 
                     "lpix_self_pj_users", "lpix_self_pj_value", "lpix_self_pj_trans", "lpix_self_pj_value_w",
                     "lusers_PJ",
                     "log_valor_boleto", "log_qtd_boleto", "log_qtd_cli_pag_pf_boleto", "log_qtd_cli_rec_pj_boleto",
                     "ltrans_boleto_PF", "lvalor_boleto_PF", "lsenders_boleto_PF", "ltrans_send_boleto_PF", "lvalor_send_boleto_PF", "lreceivers_boleto_PF", "ltrans_rec_boleto_PF", "lvalor_rec_boleto_PF",
                     "ltrans_boleto_PJ", "lvalor_boleto_PJ", "lsenders_boleto_PJ", "ltrans_send_boleto_PJ", "lvalor_send_boleto_PJ", "lreceivers_boleto_PJ", "ltrans_rec_boleto_PJ", "lvalor_rec_boleto_PJ",
                     "ltrans_boleto", "lvalor_boleto", "lsenders_boleto", "ltrans_send_boleto", "lvalor_send_boleto", "lreceivers_boleto", "ltrans_rec_boleto", "lvalor_rec_boleto",
                     "log_valor_TED_intra", "log_qtd_TED_intra", "log_qtd_cli_TED_rec_PJ", "log_qtd_cli_TED_pag_PJ",
                     "log_valor_cartao_credito", "log_valor_cartao_debito", "log_qtd_cli_cartao_debito", "log_qtd_cli_cartao_credito",
                     "lreceivers_card_PF", "lvalor_card_PF", "lreceivers_credit_PF", "lvalor_credit_PF", "lreceivers_debit_PF", "lvalor_debit_PF",
                     "lreceivers_card_PJ", "lvalor_card_PJ", "lreceivers_credit_PJ", "lvalor_credit_PJ", "lreceivers_debit_PJ", "lvalor_debit_PJ",
                     "lreceivers_card", "lvalor_card", "lreceivers_credit", "lvalor_credit", "lreceivers_debit", "lvalor_debit",
                     "lmuni_stock_w_PF", "lmuni_stock_w_PJ", "lbanked_pop_PF", "lbanked_pop_PJ",
                     "lmuni_stock_PF", "lmuni_stock_w_PF", "lbanked_pop_PF", "lmuni_stock_PJ", "lmuni_stock_w_PJ", "lbanked_pop_PJ", "lmuni_stock", "lmuni_stock_w", "lbanked_pop",
                     "lfirst_account_PJ", "f_account_pop_PJ", "first_account_PJ", "lfirst_account_PF", "f_account_pop_PF", "first_account_PF", "lfirst_account", "f_account_pop", "first_account")
      
      variable_labels <- c("Log Bank Transactions", "Log Bank Value", "Log Bank Transactions", "Log Bank Value",
                           "Log Value Wire", "Log Transactions Wire", "Log Value Wire", "Log Transactions Wire", "Log Value Wire", "Log Transactions Wire",
                           "Log Bank Accounts", "Log Bank Accounts", 
                           "Log Self Users", "Log Self Value", "Log Self Transactions", "Log Self Value", 
                           "Log Self Users", "Log Self Value", "Log Self Transactions", "Log Self Value",
                           "Log Pix Users - Firms",
                           "Log Value", "Log Transactions", "Log People Sending Boleto", "Log Firms Receiving Boleto",
                           "Log Transactions Boleto", "Log Value Boleto", "Log Active People Sending Boleto", "Log Transactions Sending Boleto", "Log Value Sending Boleto", "Log Active People Receiving Boleto", "Log Transactions Receiving Boleto", "Log Value Receiving Boleto",
                           "Log Transactions Boleto", "Log Value Boleto", "Log Active Firms Sending Boleto", "Log Transactions Sending Boleto", "Log Value Sending Boleto", "Log Active Firms Receiving Boleto", "Log Transactions Receiving Boleto", "Log Value Receiving Boleto",
                           "Log Transactions Boleto", "Log Value Boleto", "Log Active Senders of Boleto", "Log Transactions Sending Boleto", "Log Value Sending Boleto", "Log Active Receivers of Boleto", "Log Transactions Receiving Boleto", "Log Value Receiving Boleto",
                           "Log Value", "Log Transactions", "Log Firms Receiving TED", "Log Firms Sending TED",
                           "Log Value Credit Card", "Log Value Debit Card", "Log Firms accepting Debit Card", "Log Firms accepting Credit Card",
                           "Log Active People Receiving Card", "Log Value Card", "Log Active People Receiving Credit", "Log Value Credit", "Log Active People Receiving Debit", "Log Value Debit",
                           "Log Active Firms Receiving Card", "Log Value Card", "Log Active Firms Receiving Credit", "Log Value Credit", "Log Active Firms Receiving Debit", "Log Value Debit",
                           "Log Active Receiving Card", "Log Value Card", "Log Active Receiving Credit", "Log Value Credit", "Log Active Receiving Debit", "Log Value Debit",
                           "Log Bank Accounts - People", "Log Bank Accounts - Firms", "Log Banked Population - People", "Log Banked Population - Firms",
                           "Log Number of Bank Accounts", "Log Number of Bank Accounts", "Log Banked Population", "Log Number of Bank Accounts", "Log Number of Bank Accounts", "Log Banked Population","Log Number of Bank Accounts", "Log Number of Bank Accounts", "Log Banked Population",
                           "Log First Account", "First Account", "First Account", "Log First Account", "First Account", "First Account", "Log First Account", "First Account", "First Account")
      
      for(z in 1:length(variables)){
        iv_function(variables[[z]],variable_labels[[z]],"lusers_PF", "Log Pix Users","after_flood", "flood_risk5",dat_a, ending)
      }
      
      
      # Pix
      variables <- c("lusers_PF", "lusers_PJ")
      variable_labels <- c("Log Pix Users - People", "Log Pix Users - Firms")
      diff_function(variables, variable_labels, "Pix", dat_a, paste0("after",ending))
      
      iv_function("lusers_PJ","Log Pix Users - Firms","lusers_PF", "Log Pix Users - People","after_flood", "flood_risk5",dat_a, ending)
      
      # Boleto
      variables <- c("log_valor_boleto", "log_qtd_boleto", "log_qtd_cli_pag_pf_boleto", "log_qtd_cli_rec_pj_boleto")
      variable_labels <- c("Log Value", "Log Transactions", "Log People Sending Boleto", "Log Firms Receiving Boleto")
      diff_function(variables, variable_labels, "Boleto", dat_a, paste0("after",ending))
      diff_function(variables, variable_labels, "Boleto", dat_b, paste0("before",ending))
      
      # ltrans_boleto_PF, lvalor_boleto_PF, lsenders_boleto_PF, ltrans_send_boleto_PF, lvalor_send_boleto_PF, lreceivers_boleto_PF, ltrans_rec_boleto_PF, lvalor_rec_boleto_PF,
      # ltrans_boleto_PJ, lvalor_boleto_PJ, lsenders_boleto_PJ, ltrans_send_boleto_PJ, lvalor_send_boleto_PJ, lreceivers_boleto_PJ, ltrans_rec_boleto_PJ, lvalor_rec_boleto_PJ,
      # ltrans_boleto, lvalor_boleto, lsenders_boleto, ltrans_send_boleto, lvalor_send_boleto, lreceivers_boleto, ltrans_rec_boleto, lvalor_rec_boleto
      

      # TED log_valor_TED_intra, log_qtd_TED_intra, log_qtd_cli_TED_rec_PJ, log_qtd_cli_TED_pag_PJ
      variables <- c("log_valor_TED_intra", "log_qtd_TED_intra", "log_qtd_cli_TED_rec_PJ", "log_qtd_cli_TED_pag_PJ")
      variable_labels <- c("Log Value", "Log Transactions", "Log Firms Receiving TED", "Log Firms Sending TED")
      diff_function(variables, variable_labels, "TED", dat_a, paste0("after",ending))
      diff_function(variables, variable_labels, "TED", dat_b, paste0("before",ending))
      
      # Card
      variables <- c("log_valor_cartao_credito", "log_valor_cartao_debito", "log_qtd_cli_cartao_debito", "log_qtd_cli_cartao_credito")
      variable_labels <- c("Log Value Credit Card", "Log Value Debit Card", "Log Firms accepting Debit Card", "Log Firms accepting Credit Card")
      diff_function(variables, variable_labels, "Card", dat_a, paste0("after",ending))
      diff_function(variables, variable_labels, "Card", dat_b, paste0("before",ending))
      
      #                lreceivers_card_PF, lvalor_card_PF, lreceivers_credit_PF, lvalor_credit_PF, lreceivers_debit_PF, lvalor_debit_PF,
      #                lreceivers_card_PJ, lvalor_card_PJ, lreceivers_credit_PJ, lvalor_credit_PJ, lreceivers_debit_PJ, lvalor_debit_PJ
      #                lreceivers_card, lvalor_card, lreceivers_credit, lvalor_credit, lreceivers_debit, lvalor_debit
      

      # CCS
      variables <- c("lmuni_stock_w_PF", "lmuni_stock_w_PJ", "lbanked_pop_PF", "lbanked_pop_PJ")
      variable_labels <- c("Log Bank Accounts - People", "Log Bank Accounts - Firms", "Log Banked Population - People", "Log Banked Population - Firms")
      diff_function(variables, variable_labels, "CCS", dat_a, paste0("after",ending))
      diff_function(variables, variable_labels, "CCS", dat_b, paste0("before",ending))
      
      # lmuni_stock_PF, lmuni_stock_w_PF, lbanked_pop_PF, lmuni_stock_PJ, lmuni_stock_w_PJ, lbanked_pop_PJ, lmuni_stock, lmuni_stock_w, lbanked_pop,
      # lfirst_account_PJ, f_account_pop_PJ, first_account_PJ, lfirst_account_PF, f_account_pop_PF, first_account_PF, lfirst_account, f_account_pop, first_account,
      

      
      rm(dat_a, dat_b)
    }
  }, error = function(e) {
    print(paste("Error in run_week:", e))
  })
}

if(run_month == 1){
  tryCatch({
    for(i in 5:5){
      if(i==4){
        flood_a <- flood_month_after
        flood_b <- flood_month_before2019
        ending <- "_month"
      }
      if(i==5){
        flood_a <- flood_month_after_balanced
        flood_b <- flood_month_before_balanced2019
        ending <- "_balanced_month"
      }
      if(i==6){
        flood_a <- flood_month_after_balanced_covid
        flood_b <- flood_month_before_balanced_covid
        ending <- "_balanced_covid_month"
      }
      
      dat_a <- dat_flood_function(monthly_database, flood_a, mun_fe, mun_control)
      dat_b <- dat_flood_function(monthly_database, flood_b, mun_fe, mun_control)
      
      variables <- c("log_vol_cartao", "log_qtd_cli_cartao", "log_vol_emprestimo_pessoal", "log_qtd_cli_emp_pessoal", "log_vol_credito_total", "log_qtd_cli_total", "log_vol_credito_total_PF", "log_qtd_cli_total_PF", "log_vol_credito_total_PJ", "log_qtd_cli_total_PJ",
                     "log_caixa", "log_total_deposits", "log_poupanca", "log_dep_prazo", "log_dep_vista_PF", "log_dep_vista_PJ",
                     "lnew_users_PF", "lnew_users_if_PF", "lnew_users_cg_PF", "lvalor_PF", "lvalor_ativo_PF", "lusers_c_PF", "lqtd_PF", "lbanks_PF", "lvalor_w_PF", "lusers_w_PF", "lqtd_w_PF", "lvalor_cartao_PF", "lusers_cartao_PF", "lqtd_cartao_PF",
                     "lnew_users_PJ", "lnew_users_if_PJ", "lnew_users_cg_PJ", "lvalor_PJ", "lvalor_ativo_PJ", "lusers_c_PJ", "lqtd_PJ", "lbanks_PJ")
      variable_labels <- c("Log Volume Credit Card", "Log Quantity of Credit Cards Users", "Log Volume Personal Loan", "Log Quantity of Personal Loans", "Log Volume Loan", "Log Quantity Loan", "Log Volume Loan - People", "Log Quantity Loan - People", "Log Volume Loan - Firms", "Log Quantity Loan - Firms",
                           "Log Money Inventory", "Log Deposits", "Log Savings", "Log Time Deposits", "Log Checking - People", "Log Checking - Firms",
                           "Log New Users - People", "Log New Users - People - IF", "Log New Users - People - CG", "Log Value - People", "Log Active Value - People", "Log Users - People", "Log Quantity - People", "Log Banks - People", "Log Value - People - W", "Log Users - People - W", "Log Quantity - People - W", "Log Value - People - Card", "Log Users - People - Card", "Log Quantity - People - Card",
                           "Log New Users - Firms", "Log New Users - Firms - IF", "Log New Users - Firms - CG", "Log Value - Firms", "Log Active Value - Firms", "Log Users - Firms", "Log Quantity - Firms", "Log Banks - Firms")

      for(z in 1:length(variables)){
        iv_function(variables[[z]],variable_labels[[z]],"lusers_pix_pf", "Log Pix Users","after_flood", "flood_risk5",dat_a, ending)
      }
      
      rm(dat_a, dat_b)
    }
  }, error = function(e) {
    print(paste("Error in run_month:", e))
  })
}

# Create nice tables ###########################################################

run_week_tables <- 1
run_month_tables <- 1

if(run_week_tables == 1){
  tryCatch({
  for(i in 2:2){
    if(i==1){
      flood_a <- flood_week_after
      ending <- ""
      dat_a <- dat_flood_function(weekly_database, flood_a, mun_fe, mun_control)
    }
    if(i==2){
      flood_a <- flood_week_after_balanced
      ending <- "_balanced"
      dat_a <- dat_flood_function(weekly_database, flood_a, mun_fe, mun_control)
    }
    if(i==3){
      flood_a <- flood_week_after_balanced_covid
      ending <- "_balanced_covid"
      dat_a <- dat_flood_function(weekly_database, flood_a, mun_fe, mun_control)
    }
    
    # 1. Pix Regression - First Stage
    olsmodel2 <- felm(lusers_PF ~ factor(after_flood) | muni_cd + time:flood_risk5 | 0 | muni_cd, data = dat_a)
    
    ols_summary <- summary(olsmodel2)
    first_stage_fstat <- round(ols_summary$fstat[1],3)
    print(paste("F-statistic of the model:", first_stage_fstat))
    f_statistics <- c("F-stat", first_stage_fstat)
    
    column_labels <- c("OLS")
    add_lines1 <- c("Mun. FE", "Yes")
    add_lines2 <- c("Time x Region FE", "Yes")
    latex_table <- stargazer(olsmodel2,
                             float = FALSE,
                             align = TRUE, 
                             dep.var.labels = c("Log Pix Users"),
                             covariate.labels = c("Flood"),
                             #omit.stat = c("ser", "adj.rsq", "LL", "f"), 
                             no.space = TRUE,
                             single.row = FALSE,
                             header = FALSE, 
                             font.size = "small",
                             add.lines = list(add_lines1,
                                              add_lines2,
                                              f_statistics),
                             column.labels = column_labels)
    latex_file <- file.path(output_path, paste0("tables/iv_","first_stage", ending, ".tex")) 
    cat(latex_table, file = latex_file)
    
    # 2. Payment methods
    # log_qtd_TED_intra, ltrans_rec_boleto_PJ, lreceivers_credit_PJ, lreceivers_debit_PJ,
    y_list <- c("log_qtd_TED_intra", "ltrans_rec_boleto_PJ", "lreceivers_credit_PJ", "lreceivers_debit_PJ")
    y_label_list <- c("Log Trans. Wire", "Log Trans. Slip", "Log Credit Acceptance", "Log Debit Acceptance")
    
    iv_function_new(y_list, y_label_list, "lusers_PF", "Log Pix Users","after_flood", "flood_risk5", dat_a, ending, "payment_methods")
    
    # 3. Boleto
    # lsenders_boleto_PF, lreceivers_boleto_PJ, ltrans_rec_boleto_PJ, lvalor_rec_boleto_PJ
    y_list <- c("lsenders_boleto_PF", "lreceivers_boleto_PJ", "ltrans_rec_boleto_PJ", "lvalor_rec_boleto_PJ")
    y_label_list <- c("Log Senders Slip", "Log Receivers Slip", "Log Trans. Slip", "Log Value Slip")
    
    iv_function_new(y_list, y_label_list, "lusers_PF", "Log Pix Users","after_flood", "flood_risk5", dat_a, ending, "boleto")
    
    # 4. TED
    # log_qtd_cli_TED_pag_PJ, log_qtd_cli_TED_rec_PJ, log_valor_TED_intra, log_qtd_TED_intra
    y_list <- c("log_qtd_cli_TED_pag_PJ", "log_qtd_cli_TED_rec_PJ", "log_valor_TED_intra", "log_qtd_TED_intra")
    y_label_list <- c("Log Senders Wire", "Log Receivers Wire", "Log Value Wire", "Log Trans. Wire")
    
    iv_function_new(y_list, y_label_list, "lusers_PF", "Log Pix Users","after_flood", "flood_risk5", dat_a, ending, "ted")
    
    # 5. Card
    # lreceivers_credit_PJ, lvalor_credit_PJ, lreceivers_debit_PJ, lvalor_debit_PJ, lreceivers_card, lvalor_card
    
    y_list <- c("lreceivers_credit_PJ", "lvalor_credit_PJ", "lreceivers_debit_PJ", "lvalor_debit_PJ", "lreceivers_card", "lvalor_card")
    y_label_list <- c("Log Credit Acceptance", "Log Value Credit", "Log Debit Acceptance", "Log Value Debit", "Log Card Acceptance", "Log Value Card") 
    
    iv_function_new(y_list, y_label_list, "lusers_PF", "Log Pix Users","after_flood", "flood_risk5", dat_a, ending, "card") #---------------------------------
    
    # 6. CCS
    # lmuni_stock_PF, lbanked_pop_PF, lfirst_account_PF, lmuni_stock_PJ
    
    y_list <- c("lmuni_stock_PF", "lbanked_pop_PF", "lfirst_account_PF", "lmuni_stock_PJ")
    y_label_list <- c("Log Accounts", "Log Banked Population", "Log First Account", "Log Accounts - Firms")
    
    iv_function_new(y_list, y_label_list, "lusers_PF", "Log Pix Users","after_flood", "flood_risk5", dat_a, ending, "ccs2")
    
    # 7. **** Accounts, new accounts, use of accounts, self use,
    
    y_list <- c("lmuni_stock_PF", "lfirst_account_PF", "laccounts_pf")
    y_label_list <- c("Log Accounts", "Log First Account", "Log Accounts Used")
    
    iv_function_new(y_list, y_label_list, "lusers_PF", "Log Pix Users","after_flood", "flood_risk5", dat_a, ending, "ccs")
    
    # Make credit + debit ----------------------------------------
    # Make Ted + Boleto -----------------------------------------
    # TED New ---------------------------------------------------
    
    # 8. TED New
    y_list <- c("ltrans_ted_new", "lvalor_ted_new", "ltrans_ted_new_pf", "lvalor_ted_new_pf", "ltrans_ted_new_pj", "lvalor_ted_new_pj")
    y_label_list <- c("Log Wire Trans.", "Log Wire Value", "Log Wire Trans. - Ind.", "Log Wire Value - Ind.", "Log Wire Trans. - Firms", "Log Wire Value - Firms")
    
    iv_function_new(y_list, y_label_list, "lusers_PF", "Log Pix Users","after_flood", "flood_risk5", dat_a, ending, "ted_new")
    
    # 9. TED + boleto
    y_list <- c("ltrans_ted_boleto", "lvalor_ted_boleto", "ltrans_ted_boleto_PJ_rec", "lvalor_ted_boleto_PJ_rec")
    y_label_list <- c("Log Bank Trans.", "Log Bank Trans. Value", "Log Bank Trans. - Firms", "Log Bank Trans. Value - Firms")
    
    iv_function_new(y_list, y_label_list, "lusers_PF", "Log Pix Users","after_flood", "flood_risk5", dat_a, ending, "ted_boleto")
    
    # 10. Payment methods
    y_list <- c("ltrans_ted_boleto", "lvalor_ted_boleto", "lreceivers_card", "lvalor_card")
    y_label_list <- c("Log Bank Trans.", "Log Bank Trans. Value", "Log Card Acceptance", "Log Value Card")
    
    iv_function_new(y_list, y_label_list, "lusers_PF", "Log Pix Users","after_flood", "flood_risk5", dat_a, ending, "payment_methods2")

    # 11. Payment methods
    y_list <- c("ltrans_ted_new", "ltrans_rec_boleto_PJ", "lreceivers_card", "lvalor_card")
    y_label_list <- c("Log Trans. Wire", "Log Trans. Slip", "Log Card Acceptance", "Log Value Card")
    
    iv_function_new(y_list, y_label_list, "lusers_PF", "Log Pix Users","after_flood", "flood_risk5", dat_a, ending, "payment_methods3")

    # 12. Payments methods
    y_list <- c("ltrans_ted_boleto", "lreceivers_card", "lvalor_card")
    y_label_list <- c("Log Bank Trans.", "Log Card Acceptance", "Log Value Card")
    
    iv_function_new(y_list, y_label_list, "lusers_PF", "Log Pix Users","after_flood", "flood_risk5", dat_a, ending, "payment_methods4")
    
    # 13. Payments methods
    y_list <- c("ltrans_ted_new_pj", "ltrans_rec_boleto_PJ", "lreceivers_credit_PJ", "lreceivers_debit_PJ")
    y_label_list <- c("Log Trans. Wire", "Log Trans. Slip", "Log Credit Acceptance", "Log Debit Acceptance")
    
    iv_function_new(y_list, y_label_list, "lusers_PF", "Log Pix Users","after_flood", "flood_risk5", dat_a, ending, "payment_methods5") #---------------------------------
    
    # Ted and Boleto
    
    y_list <- c("ltrans_ted_new_pj", "lvalor_ted_new_pj", "ltrans_rec_boleto_PJ", "lvalor_rec_boleto_PJ")
    y_label_list <- c("Log Wire Trans.", "Log Wire Value", "Log Trans. Slip", "Log Value Slip" )
    
    iv_function_new(y_list, y_label_list, "lusers_PF", "Log Pix Users","after_flood", "flood_risk5", dat_a, ending, "ted_and_boleto") #---------------------------------
    
  }
  }, error = function(e) {
    print(paste("Error in nice_tables:", e))
  })
}

if(run_month_tables == 1){
  tryCatch({
  for(i in 2:2){
    if(i==1){
      flood_a <- flood_month_after
      ending <- "_month"
      dat_a <- dat_flood_function(monthly_database, flood_a, mun_fe, mun_control)
    }
    if(i==2){
      flood_a <- flood_month_after_balanced
      ending <- "_balanced_month"
      dat_a <- dat_flood_function(monthly_database, flood_a, mun_fe, mun_control)
    }
    if(i==3){
      flood_a <- flood_month_after_balanced_covid
      ending <- "_balanced_covid_month"
      dat_a <- dat_flood_function(monthly_database, flood_a, mun_fe, mun_control)
    }
    
    # 1. Pix Regression - First Stage
    olsmodel2 <- felm(lusers_pix_pf ~ factor(after_flood) | muni_cd + time:flood_risk5 | 0 | muni_cd, data = dat_a)
    ols_summary <- summary(olsmodel2)
    first_stage_fstat <- round(ols_summary$fstat[1],3)
    print(paste("F-statistic of the model:", first_stage_fstat))
    f_statistics <- c("F-stat", first_stage_fstat)
    
    column_labels <- c("OLS")
    add_lines1 <- c("Mun. FE", "Yes")
    add_lines2 <- c("Time x Region FE", "Yes")
    latex_table <- stargazer(olsmodel2,
                             float = FALSE,
                             align = TRUE, 
                             dep.var.labels = c("Log Pix Users"),
                             covariate.labels = c("Flood"),
                             #omit.stat = c("ser", "adj.rsq", "LL", "f"), 
                             no.space = TRUE,
                             single.row = FALSE,
                             header = FALSE, 
                             font.size = "small",
                             add.lines = list(add_lines1,
                                              add_lines2,
                                              f_statistics),
                             column.labels = column_labels)
    latex_file <- file.path(output_path, paste0("tables/iv_","first_stage", ending, ".tex")) 
    cat(latex_table, file = latex_file)
    
    # 7. Credito
    # lnew_users_PF, lnew_users_cg, lvalor_ativo_PF
    
    y_list <- c("lnew_users_PF", "lnew_users_cg_PF", "lvalor_ativo_PF")
    y_label_list <- c("Log Credit Adoption - People", "Log Bank Adoption - People", "Log Debt - People")
    
    iv_function_new(y_list, y_label_list, "lusers_pix_pf", "Log Pix Users","after_flood", "flood_risk5", dat_a, ending, "credito_PF")
    
    y_list <- c("lnew_users_PJ", "lnew_users_cg_PJ", "lvalor_ativo_PJ")
    y_label_list <- c("Log Credit Adoption - Firms", "Log Bank Adoption - Firms", "Log Debt - Firms")
    
    iv_function_new(y_list, y_label_list, "lusers_pix_pf", "Log Pix Users","after_flood", "flood_risk5", dat_a, ending, "credito_PJ")
    
    
    
    # 8. Estban
    
    
  }
  }, error = function(e) {
    print(paste("Error in nice_tables2:", e))
  })
}


#ADD RAIS! -------------------------------------------------------------------
rais <- read_dta(file.path(dta_path,"rais_jobs_salaries.dta"))
# variables: time_id, muni_cd, year, month, n_jobs, total_salary, ln_jobs, ltotal_salary
setDT(rais)
rais$time <- rais$time_id
rais <- rais %>%
  dplyr::select(time, muni_cd, ln_jobs, ltotal_salary)
monthly_database <- merge(monthly_database, rais, by=c("muni_cd","time"), all=TRUE)


#ADD ESTBAN! -------------------------------------------------------------------

# Add estban with PJ and PF. --------------------
# also, should we do flood event study on them?
# log_dep_vista_PF, log_dep_vista_PJ

dat_a2 <- read_dta(file.path(dta_path,"Estban_detalhado_flood_collapsed2.dta")) 
setDT(dat_a2)
dat_a2$time <- dat_a2$time_id
dat_a2 <- dat_a2 %>%
  dplyr::select(time, muni_cd, log_dep_vista_PF, log_dep_vista_PJ)
dat_b2 <- read_dta(file.path(dta_path,"Estban_detalhado_flood_beforePIX_collapsed2.dta"))
setDT(dat_b2)
dat_b2$time <- dat_b2$time_id
dat_b2 <- dat_b2 %>%
  dplyr::select(time, muni_cd, log_dep_vista_PF, log_dep_vista_PJ)
# bind them
dat_estban_private <- rbind(dat_a2, dat_b2)
#merge to monthly database
monthly_database <- merge(monthly_database, dat_estban_private, by=c("muni_cd","time"), all=TRUE)

# -----------------------------------

# ESTBAN
# ldeposit, lassets, lliab, lcaixa
estban_public <- prepare_data2("estban_2018_2023.dta")

estban_public <- estban_public %>%
  dplyr::select(time, muni_cd, lloans, ldeposit, ldeposit2, lassets, lassets2, lliab, lliab2, lcaixa, lcaixa2, lrevenue, lexpenses, ltotal_do_ativo, l401_419, l420)
# Variables: time, muni_cd, ldeposit, lassets, lliab, lcaixa, lrevenue, lexpenses, ltotal_do_ativo

estban_public5 <- prepare_data2("estban_2018_2023_top5.dta")
estban_public5 <- estban_public5 %>% dplyr::filter(top5 == 1)

estban_public5 <- estban_public5 %>%
  dplyr::select(time, muni_cd, lloans, ldeposit, ldeposit2, lassets, lassets2, lliab, lliab2, lcaixa, lcaixa2, lrevenue, lexpenses, ltotal_do_ativo, l401_419, l420) %>%
  dplyr::rename(lloans5 = lloans, ldeposit5 = ldeposit, ldeposit25 = ldeposit2, lassets5 = lassets, lassets25 = lassets2, lliab5 = lliab, lliab25 = lliab2, lcaixa5 = lcaixa, lcaixa25 = lcaixa2, lrevenue5 = lrevenue, lexpenses5 = lexpenses, ltotal_do_ativo5 = ltotal_do_ativo, l401_4195 = l401_419, l4205 = l420)

monthly_database <- merge(monthly_database, estban_public, by=c("muni_cd","time"), all=TRUE)
monthly_database <- merge(monthly_database, estban_public5, by=c("muni_cd","time"), all=TRUE)

# add l401_419 to l420
monthly_database$ldeposit3 <- log1p(exp(monthly_database$l420) - 1 + exp(monthly_database$l401_419) - 1)
monthly_database$ldeposit35 <- log1p(exp(monthly_database$l4205) - 1 + exp(monthly_database$l401_4195) - 1)


variables <- c("ln_jobs", "ltotal_salary", "log_dep_vista_PF", "log_dep_vista_PJ", "ldeposit3", "l401_419", "l420", "ldeposit35", "l401_4195", "l4205", "lloans", "ldeposit", "ldeposit2", "lassets", "lassets2", "lliab", "lliab2", "lcaixa", "lcaixa2", "lrevenue", "lexpenses", "ltotal_do_ativo", "lloans5", "ldeposit5", "ldeposit25", "lassets5", "lassets25", "lliab5", "lliab25", "lcaixa5", "lcaixa25", "lrevenue5", "lexpenses5", "ltotal_do_ativo5")
variables_labels <- c("Log Number of Jobs", "Log Salary", "Log Checking - Ind.", "Log Checking - Firms", "Log Deposits", "Log Checking", "Log Savings", "Log Deposits", "Log Checking", "Log Savings", "Log Loans", "Log Deposits", "Log Deposits", "Log Assets", "Log Assets", "Log Liabilities", "Log Liabilities", "Log Money Inventory", "Log Money Inventory", "Log Revenue", "Log Expenses", "Log Total Assets", "Log Loans", "Log Deposits", "Log Deposits", "Log Assets", "Log Assets", "Log Liabilities", "Log Liabilities", "Log Money Inventory", "Log Money Inventory", "Log Revenue", "Log Expenses", "Log Total Assets")
run_estban_public <- 1
if(run_estban_public == 1){
  tryCatch({
    for(i in 5:5){
      if(i==4){
        flood_a <- flood_month_after
        flood_b <- flood_month_before2019
        ending <- "_month"
      }
      if(i==5){
        flood_a <- flood_month_after_balanced
        flood_b <- flood_month_before_balanced2019
        ending <- "_balanced_month"
      }
      if(i==6){
        flood_a <- flood_month_after_balanced_covid
        flood_b <- flood_month_before_balanced_covid
        ending <- "_balanced_covid_month"
      }
      dat_a <- dat_flood_function(monthly_database, flood_a, mun_fe, mun_control)
      #dat_b <- dat_flood_function(monthly_database, flood_b, mun_fe, mun_control)
      # ESTBAN
       for(z in 1:length(variables)){
        iv_function(variables[[z]],variables_labels[[z]],"lusers_pix_pf", "Log Pix Users","after_flood", "flood_risk5",dat_a, ending)
      }
      rm(dat_a)
      #rm(dat_b)
    }
  }, error = function(e) {
    print(paste("Error in run_month:", e))
  })
}


#Nice graphs

run_month_tables_estban <- 1
if(run_month_tables_estban == 1){
  tryCatch({
    for(i in 2:2){
      if(i==1){
        flood_a <- flood_month_after
        ending <- "_month"
        dat_a <- dat_flood_function(monthly_database, flood_a, mun_fe, mun_control)
      }
      if(i==2){
        flood_a <- flood_month_after_balanced
        ending <- "_balanced_month"
        dat_a <- dat_flood_function(monthly_database, flood_a, mun_fe, mun_control)
      }
      if(i==3){
        flood_a <- flood_month_after_balanced_covid
        ending <- "_balanced_covid_month"
        dat_a <- dat_flood_function(monthly_database, flood_a, mun_fe, mun_control)
      }
      
      # 1. Pix Regression - First Stage
      olsmodel2 <- felm(lusers_pix_pf ~ factor(after_flood) | muni_cd + time:flood_risk5 | 0 | muni_cd, data = dat_a)
      ols_summary <- summary(olsmodel2)
      first_stage_fstat <- round(ols_summary$fstat[1],3)
      print(paste("F-statistic of the model:", first_stage_fstat))
      f_statistics <- c("F-stat", first_stage_fstat)
      
      column_labels <- c("OLS")
      add_lines1 <- c("Mun. FE", "Yes")
      add_lines2 <- c("Time x Region FE", "Yes")
      latex_table <- stargazer(olsmodel2,
                               float = FALSE,
                               align = TRUE, 
                               dep.var.labels = c("Log Pix Users"),
                               covariate.labels = c("Flood"),
                               #omit.stat = c("ser", "adj.rsq", "LL", "f"), 
                               no.space = TRUE,
                               single.row = FALSE,
                               header = FALSE, 
                               font.size = "small",
                               add.lines = list(add_lines1,
                                                add_lines2,
                                                f_statistics),
                               column.labels = column_labels)
      latex_file <- file.path(output_path, paste0("tables/iv_","first_stage", ending, ".tex")) 
      cat(latex_table, file = latex_file)
      
      # 8. Estban
      y_list <- c("lrevenue5", "lexpenses5","ldeposit25", "lloans5", "ltotal_do_ativo5")
      y_label_list <- c("Log Revenue", "Log Expenses", "Log Deposits", "Log Loans", "Log Total Assets")
      iv_function_new(y_list, y_label_list, "lusers_pix_pf", "Log Pix Users","after_flood", "flood_risk5", dat_a, ending, "estban5")
      
      y_list <- c("lassets5", "lassets25","ldeposit5", "lliab5", "lliab25")
      y_label_list <- c("Log Assets", "Log Assets", "Log Deposits", "Log Liabilities", "Log Liabilities")
      iv_function_new(y_list, y_label_list, "lusers_pix_pf", "Log Pix Users","after_flood", "flood_risk5", dat_a, ending, "estban52")

      
      y_list <- c("lrevenue", "lexpenses","ldeposit2", "lloans", "ltotal_do_ativo")
      y_label_list <- c("Log Revenue", "Log Expenses", "Log Deposits", "Log Loans", "Log Total Assets")
      iv_function_new(y_list, y_label_list, "lusers_pix_pf", "Log Pix Users","after_flood", "flood_risk5", dat_a, ending, "estban")
      
      y_list <- c("lassets", "lassets2","ldeposit", "lliab", "lliab2")
      y_label_list <- c("Log Assets", "Log Assets", "Log Deposits", "Log Liabilities", "Log Liabilities")
      iv_function_new(y_list, y_label_list, "lusers_pix_pf", "Log Pix Users","after_flood", "flood_risk5", dat_a, ending, "estban2")
      
 
      # Loans, first time loan, first time bank, assets (estban)
      
      #y_list <- c("lnew_users_PF", "lnew_users_cg_PF", "lvalor_ativo_PF")
      #y_label_list <- c("Log Credit Adoption - People", "Log Bank Adoption - People", "Log Debt - People")
      
      y_list <- c("lnew_users_PF", "lnew_users_cg_PF", "lvalor_ativo_PF")
      y_label_list <- c("Log First Loan", "Log First Loan with a Bank", "Log New Debt")
      iv_function_new(y_list, y_label_list, "lusers_pix_pf", "Log Pix Users","after_flood", "flood_risk5", dat_a, ending, "credito_PF")
      
      
      
      #"l401_419", "l420"
      y_list <- c("lrevenue", "lexpenses", "l401_419", "l420", "lloans", "ltotal_do_ativo")
      y_label_list <- c("Log Revenue", "Log Expenses", "Log Checking", "Log Savings", "Log Loans", "Log Assets")
      iv_function_new(y_list, y_label_list, "lusers_pix_pf", "Log Pix Users","after_flood", "flood_risk5", dat_a, ending, "estban3")
      
      y_list <- c("lrevenue", "lexpenses","ldeposit3", "lloans", "ltotal_do_ativo")
      y_label_list <- c("Log Revenue", "Log Expenses", "Log Deposits", "Log Loans", "Log Assets")
      iv_function_new(y_list, y_label_list, "lusers_pix_pf", "Log Pix Users","after_flood", "flood_risk5", dat_a, ending, "estban4")
      
      
      # Add estban with PJ and PF. --------------------
      
      # "log_dep_vista_PF", "log_dep_vista_PJ"
      # "Log Checking - Ind.", "Log Checking - Firms"
      
      y_list <- c("lrevenue", "lexpenses", "log_dep_vista_PF", "log_dep_vista_PJ", "l420", "lloans", "ltotal_do_ativo")
      y_label_list <- c("Log Revenue", "Log Expenses", "Log Checking - Ind.", "Log Checking - Firms", "Log Savings", "Log Loans", "Log Assets")
      iv_function_new(y_list, y_label_list, "lusers_pix_pf", "Log Pix Users","after_flood", "flood_risk5", dat_a, ending, "estban33")
      
      #"ln_jobs", "ltotal_salary",
      #"Log Number of Jobs", "Log Salary"
      y_list <- c("ln_jobs", "ltotal_salary")
      y_label_list <- c("Log Number of Jobs", "Log Salary")
      iv_function_new(y_list, y_label_list, "lusers_pix_pf", "Log Pix Users","after_flood", "flood_risk5", dat_a, ending, "rais")
      
    }
  }, error = function(e) {
    print(paste("Error in nice_tables3:", e))
  })
}

 
# How to do top5 vs others?

# Also, IV for digital vs traditional banks

#iv function new 2 - without the ols. 

# rethink all tables presented


#do Ind Sample. --------------------------------------------------------------

# do ind sample monthly!!!
# Ind_sample_summary_

# Save function
save_data_function <- function(data, file_path_base) {
  parquet_path <- paste0(file_path_base, ".parquet")
  dta_path <- paste0(file_path_base, ".dta")
  
  tryCatch({
    # Try to save as .parquet
    write_parquet(data, parquet_path)
    message("Data successfully saved as .parquet")
  }, error = function(e) {
    # If saving as .parquet fails, save as .dta
    write_dta(data, dta_path)
    message("Saving as .parquet failed. Data saved as .dta instead.")
  })
}
# Example usage:
# save_data_function(my_data_frame, "path/to/your/datafile")

# Load function
load_data_function <- function(file_path_base) {
  parquet_path <- paste0(file_path_base, ".parquet")
  dta_path <- paste0(file_path_base, ".dta")
  
  tryCatch({
    # Try to read .parquet file
    data <- read_parquet(parquet_path)
    message("Data successfully loaded from .parquet")
    return(data)
  }, error = function(e) {
    # If reading .parquet fails, try reading .dta
    data <- read_dta(dta_path)
    message("Loading from .parquet failed. Data loaded from .dta instead.")
    return(data)
  })
}

# Example usage:
# my_data_frame <- load_data_function("path/to/your/datafile")

clean_data <- 0
if(clean_data == 1){
for(i in 1:2){
  if((i==1 & !file.exists(file.path(dta_path, "Ind_sample_summary_PF.dta"))) | (i==2 & !file.exists(file.path(dta_path, "Ind_sample_summary_PJ.dta")))){
    
    if(i==1){
      tipo = "PF"
      pix <- read.dta13(paste0(dta_path,"Pix_ind_sample_PF.dta"), select.cols = c("week", "id", "muni_cd", "tipo", "ltrans", "birthdate", "gender"), convert.factors = FALSE)
      #pix <- read_dta(paste0(dta_path,"Pix_ind_sample_PF.dta"), n_max = 10000000)
      pix <- pix %>% dplyr::mutate(pix = ifelse(ltrans > 0, 1, 0),
                                  birth_year = year(as.Date(birthdate))) %>%
        dplyr::select(c("week", "id", "muni_cd", "tipo", "birth_year", "gender", "pix"))
      
      pix <- pix %>% dplyr::mutate(across(where(is.factor), as.character))
      pix <- zap_label(pix)
      save_data_function(pix, paste0(dta_path,"Pix_ind_sample_summary_", tipo))
    } else if(i==2){
      tipo = "PJ"
      pix <- read.dta13(paste0(dta_path,"Pix_ind_sample_PJ.dta"), select.cols = c("week", "id", "muni_cd", "tipo","ltrans", "open_date", "situation"), convert.factors = FALSE)
      #pix <- read_dta(paste0(dta_path,"Pix_ind_sample_PJ.dta"), n_max = 10000000)
      pix <- pix %>% dplyr::mutate(pix = ifelse(ltrans > 0, 1, 0),
                                  birth_year = year(as.Date(open_date))) %>%
        dplyr::select(c("week", "id", "muni_cd", "tipo", "birth_year", "situation", "pix"))
      pix <- pix %>% dplyr::mutate(across(where(is.factor), as.character))
      pix <- zap_label(pix)
      save_data_function(pix, paste0(dta_path,"Pix_ind_sample_summary_", tipo))
    }
    
    ted <- read.dta13(paste0(dta_path,"TED_ind_sample_SITRAF_",tipo,".dta"), select.cols = c("week", "id","ltrans"), convert.factors = FALSE)
    #ted <- read_dta(paste0(dta_path,"TED_ind_sample_SITRAF_",tipo,".dta"), n_max = 10000000) ------------------------------------------- Sitraf is ready, str is not. -----------------------------
    ted <- ted %>% dplyr::mutate(ted = ifelse(ltrans > 0, 1, 0)) %>%
      dplyr::select(c("week", "id", "ted"))
    ted <- ted %>% dplyr::mutate(across(where(is.factor), as.character))
    ted <- zap_label(ted)
    save_data_function(ted, paste0(dta_path,"TED_ind_sample_SITRAF_summary_", tipo))

    boleto <- read.dta13(paste0(dta_path,"Boleto_ind_sample_",tipo,".dta"), select.cols = c("week", "id","ltrans"), convert.factors = FALSE)
    #boleto <- read_dta(paste0(dta_path,"Boleto_ind_sample_",tipo,".dta"), n_max = 10000000)
    boleto <- boleto %>% dplyr::mutate(boleto = ifelse(ltrans > 0, 1, 0)) %>%
      dplyr::select(c("week", "id", "boleto"))
    boleto <- boleto %>% dplyr::mutate(across(where(is.factor), as.character))
    boleto <- zap_label(boleto)
    save_data_function(boleto, paste0(dta_path,"Boleto_ind_sample_summary_", tipo))

    card <- read.dta13(paste0(dta_path,"Card_ind_sample_",tipo,".dta"), select.cols = c("week", "id","ltrans"), convert.factors = FALSE)
    #card <- read_dta(paste0(dta_path,"Card_ind_sample_",tipo,".dta"), n_max = 10000000)
    card <- card %>% dplyr::mutate(card = ifelse(ltrans > 0, 1, 0)) %>%
      dplyr::select(c("week", "id", "card"))
    card <- card %>% dplyr::mutate(across(where(is.factor), as.character))
    card <- zap_label(card)
    save_data_function(card, paste0(dta_path,"Card_ind_sample_summary_", tipo))

    ccs <- read.dta13(paste0(dta_path,"CCS_ind_sample_",tipo,"_stock.dta"), select.cols = c("week", "id", "bank_type", "stock"), convert.factors = FALSE)
    #ccs <- read_dta(paste0(dta_path,"CCS_ind_sample_",tipo,"_stock.dta"), n_max = 10000000)
    ccs <- ccs %>% 
      dplyr::group_by(across(-c("bank_type", "stock"))) %>% 
      summarize(stock = sum(stock, na.rm = TRUE)) %>% dplyr::ungroup()
    ccs <- ccs %>% dplyr::group_by(id) %>% arrange(id, week) %>%
      dplyr::mutate(diff_stock = c(0,diff(stock))) %>%
      dplyr::mutate(ccs = ifelse(diff_stock > 0, 1, 0)) %>% 
      dplyr::select(c("week", "id", "ccs")) %>% dplyr::ungroup()
    ccs <- ccs %>% dplyr::mutate(across(where(is.factor), as.character))
    ccs <- zap_label(ccs)
    save_data_function(ccs, paste0(dta_path,"CCS_ind_sample_summary_", tipo))

    credito <- read.dta13(paste0(dta_path,"Credito_ind_sample_",tipo,".dta"), select.cols = c("time_id", "id", "bank_type", "new_users", "new_users_if", "new_users_cg", "qtd"), convert.factors = FALSE)
    #credito <- read_dta(paste0(dta_path,"Credito_ind_sample_",tipo,".dta"), n_max = 10000000)
    credito <- credito %>% 
      dplyr::group_by(across(-c("bank_type", "new_users", "new_users_if", "new_users_cg", "qtd"))) %>% 
      summarize(new_users = sum(new_users, na.rm = TRUE),
                new_users_if = sum(new_users_if, na.rm = TRUE),
                new_users_cg = sum(new_users_cg, na.rm = TRUE),
                qtd = sum(qtd, na.rm = TRUE)) %>% dplyr::ungroup()
    credito <- credito %>% 
      dplyr::mutate(cred_new_users = ifelse(new_users > 0, 1,0),
             cred_new_users_if = ifelse(new_users_if > 0, 1,0),
             cred_new_users_cg = ifelse(new_users_cg > 0, 1,0),
             cred_qtd = ifelse(qtd > 0, 1,0)) %>%
      dplyr::select(c("time_id", "id", "cred_new_users", "cred_new_users_if", "cred_new_users_cg", "cred_qtd"))
    credito <- credito %>% dplyr::mutate(across(where(is.factor), as.character))
    credito <- zap_label(credito)
    save_data_function(credito, paste0(dta_path,"Credito_ind_sample_summary_", tipo))
    
    dat_all <- merge(pix, ted, by=c("week","id"), all=TRUE)
    dat_all <- merge(dat_all, boleto, by=c("week","id"), all=TRUE)
    dat_all <- merge(dat_all, card, by=c("week","id"), all=TRUE)
    dat_all <- merge(dat_all, ccs, by=c("week","id"), all=TRUE)
    ###dat_all <- merge(dat_all, credito, by=c("week","id"), all=TRUE) # This is monthly 
    
    # Save dta along the way so I dont need to load again.
    dat_all <- dat_all %>% dplyr::mutate(across(where(is.factor), as.character))
    dat_all <- zap_label(dat_all)
    save_data_function(dat_all, paste0(dta_path,"Ind_sample_summary_", tipo))
  }
}
}


# New changes to credit and ccs, also, we have pix ind month now. 
run_ind_tables <- 0
if(run_ind_tables == 1){
  for(i in 1:2){
    if(i==1){
      tipo = "PF"
      y_list <- c("ted", "boleto", "ccs")
      y_label_list <- c("Wire", "Slip", "New Account")
    } else if(i==2){
      tipo = "PJ"
      y_list <- c("ted", "boleto", "card", "ccs")
      y_label_list <- c("Wire", "Slip", "Card", "New Account")
    }
    dat_all <- load_data_function(paste0(dta_path,"Ind_sample_summary_", tipo))
    dat_all <- dat_all %>% dplyr::mutate(time = week)
    #Add "month", and "year" using week_to_month and week_to_year
    dat_all <- dat_all %>% dplyr::mutate(month = week_to_month(week), year = week_to_year(week))
    
    dat_all <- dat_all %>%
      dplyr::mutate(across(c(pix, ted, boleto, card, ccs), ~ replace_na(., 0)))
    for(j in 2:2){
      if(j==1){
        flood_a <- flood_week_after
        ending <- ""
      }
      if(j==2){
        flood_a <- flood_week_after_balanced
        ending <- "_balanced"
      }
      if(j==3){
        flood_a <- flood_week_after_balanced_covid
        ending <- "_balanced_covid"
      }
      
      dat_a <- dat_flood_function(dat_all, flood_a, mun_fe, mun_control)
      print(nrow(dat_a))  # Check number of rows after each merge or transformation
      head(dat_a)
      dat_a$flood_risk5 <- factor(dat_a$flood_risk5)
      dat_a$after_flood <- factor(dat_a$after_flood)
      dat_a$id <- as.numeric(dat_a$id)
      dat_a$week <- as.numeric(dat_a$week)
      dat_a$time <- as.numeric(dat_a$time)
      str(dat_a)  # Check the types of all relevant variables
      
      #iv_function_ind_new(y_list, y_label_list, "pix", "Pix", "after_flood", "flood_risk5", dat_a, ending, paste0("ind_table_", tipo))
      iv_function_ind_new2(y_list, y_label_list, "pix", "Pix", "after_flood", "flood_risk5", dat_a, ending, paste0("ind_table_", tipo))
      print("First one done!")
      
      
      tryCatch({
        variables <- c("pix", y_list)
        data_subset_raw <- dat_a %>% dplyr::select(all_of(variables))
        raw_corr <- function(data, x, y) {
          correlation <- cor(data[[x]], data[[y]], method = "pearson")
          test <- cor.test(data[[x]], data[[y]])
          list(correlation = correlation, p_value = test$p.value)
        }
        raw_results <- lapply(y_list, function(y) {
          raw_corr(data_subset_raw, "pix", y)
        })
        raw_results_df <- data.frame(
          Variable = y_list,
          Raw_Correlation = sapply(raw_results, function(x) x$correlation),
          Raw_P_Value = sapply(raw_results, function(x) x$p_value)
        )
        # Partial Correlation (with controls)
        controls <- c("time", "id")
        variables <- c("pix", y_list, controls)
        data_subset_partial <- dat_a %>% dplyr::select(all_of(variables))
        partial_corr_lm <- function(data, x, y, controls) {
          control_formula <- paste(controls, collapse = " + ")
          x_resid <- lm(as.formula(paste(x, "~", control_formula)), data = data)$residuals
          y_resid <- lm(as.formula(paste(y, "~", control_formula)), data = data)$residuals
          correlation <- cor(x_resid, y_resid, method = "pearson")
          test <- cor.test(x_resid, y_resid)
          list(correlation = correlation, p_value = test$p.value)
        }
        partial_results <- lapply(y_list, function(y) {
          partial_corr_lm(data_subset_partial, "pix", y, controls)
        })
        partial_results_df <- data.frame(
          Variable = y_list,
          Partial_Correlation = sapply(partial_results, function(x) x$correlation),
          Partial_P_Value = sapply(partial_results, function(x) x$p_value)
        )
        combined_results_df <- merge(
          raw_results_df,
          partial_results_df,
          by = "Variable"
        )
        print(combined_results_df)
        write.csv(
          combined_results_df,
          paste0(output_path, "tables/combined_correlation_table_ind_", tipo, ".csv"),
          row.names = FALSE
        )
        
      }, error = function(e) {
        message("An error occurred: ", e$message)
      })
    }
    #another thing, credito is monthly. We can do a synthetic Pix ind month, or download again.
    #can we do ccs adoption as well? ted adoption, pix adoption,... once pix adopted, others adopt.
  }
}
gc()
# Clean data for 4 and 13 weeks. 
clean_data_4 <- 0
if(clean_data_4 == 1){
  tryCatch({
  
  for(i in 1:2){
    if(i==1){
      tipo = "PF"
      y_list <- c("id", "muni_cd", "tipo", "birth_year", "gender", "pix", "ted", "boleto", "card", "ccs")
    } else if(i==2){
      tipo = "PJ"
      y_list <- c("id", "muni_cd", "tipo", "birth_year", "situation", "pix", "ted", "boleto", "card", "ccs")
    }
    dat_all <- load_data_function(paste0(dta_path,"Ind_sample_summary_", tipo))
    setDT(dat_all)
    dat_all <- dat_all %>%
      dplyr::mutate(across(everything(), ~ replace_na(., 0))) 
    
    dat_all <- dat_all %>% dplyr::mutate(time = week)
    dat_all <- dat_all %>% dplyr::mutate(month = week_to_month(week), year = week_to_year(week))
    dat_all <- dat_flood_function(dat_all, flood_week_after_balanced, mun_fe, mun_control)
    dat_all <- dat_all %>%
      mutate_if(is.factor, as.numeric)
    
    dat_all4 <- dat_all %>% 
      dplyr::mutate(time = floor(week/4),
             date_flood = floor(date_flood/4))
    dat_all4 <- dat_all4 %>% dplyr::select(-week) %>%
      dplyr::mutate_if(is.factor, as.numeric) %>%
      dplyr::group_by(id, time) %>%
      dplyr::summarize_all(max, na.rm=FALSE) %>%
      dplyr::ungroup()
    
    dat_all4 <- dat_all4 %>% dplyr::mutate(time_to_treat = ifelse(treat==1, time - date_flood, 0),
                                           time_id_treated = ifelse(treat==0, 10000, date_flood), # For Sun and Abraham
                                           after_flood = ifelse(is.na(date_flood), 0, ifelse(time >= date_flood, 1, 0)))
    # Save dta along the way so I dont need to load again.
    dat_all4 <- dat_all4 %>% dplyr::mutate(across(where(is.factor), as.character))
    dat_all4 <- zap_label(dat_all4)
    save_data_function(dat_all4, paste0(dta_path,"Ind_sample_summary4_", tipo))
    rm(dat_all4)
    gc()
    dat_all13 <- dat_all %>% 
      dplyr::mutate(time = floor(week/13),
                    date_flood = floor(date_flood/13))
    dat_all13 <- dat_all13 %>% dplyr::select(-week) %>%
      dplyr::mutate_if(is.factor, as.numeric) %>%
      dplyr::group_by(id, time) %>%
      dplyr::summarize_all(max, na.rm=FALSE) %>%
      dplyr::ungroup()
    dat_all13 <- dat_all13 %>% dplyr::mutate(time_to_treat = ifelse(treat==1, time - date_flood, 0),
                                           time_id_treated = ifelse(treat==0, 10000, date_flood), # For Sun and Abraham
                                           after_flood = ifelse(is.na(date_flood), 0, ifelse(time >= date_flood, 1, 0)))

    # Save dta along the way so I dont need to load again.
    dat_all13 <- dat_all13 %>% dplyr::mutate(across(where(is.factor), as.character))
    dat_all13 <- zap_label(dat_all13)
    save_data_function(dat_all13, paste0(dta_path,"Ind_sample_summary13_", tipo))
    rm(dat_all13)
    gc()
  }
  }, error = function(e) {
    message("An error occurred on clean_data_4: ", e$message)
  })
}
gc()
run_ind_tables4 <- 1
if(run_ind_tables4 == 1) {
  for (i in 1:2) {
    if (i == 1) {
      tipo <- "PF"
      y_list <- c("ted", "boleto", "ccs")
      y_label_list <- c("Wire", "Slip", "New Account")
      ending <- "_balanced"
    } else if (i == 2) {
      tipo <- "PJ"
      y_list <- c("ted", "boleto", "card", "ccs")
      y_label_list <- c("Wire", "Slip", "Card", "New Account")
      ending <- "_balanced"
    }

    # Define a function to process datasets
    process_data <- function(dat_a, data_type) {
      
      tryCatch({
      print(nrow(dat_a))  # Check the number of rows
      head(dat_a)

      dat_a$flood_risk5 <- factor(dat_a$flood_risk5)
      dat_a$after_flood <- factor(dat_a$after_flood)
      dat_a$id <- as.numeric(dat_a$id)
      dat_a$time <- as.numeric(dat_a$time)
      str(dat_a)
      
      # Run regression or other analyses
      iv_function_ind_new2(y_list, y_label_list, "pix", "Pix", "after_flood", "flood_risk5", dat_a, ending, paste0("ind_table_", tipo, "_", data_type))
      print(paste("Analysis completed for", data_type, "!"))
      }, error = function(e) {
        message("An error occurred during the IV analysis: ", e$message)
      })
      
      # Correlation Analysis
      tryCatch({
        variables <- c("pix", y_list)
        data_subset_raw <- dat_a %>% dplyr::select(all_of(variables))
        raw_corr <- function(data, x, y) {
          correlation <- cor(data[[x]], data[[y]], method = "pearson")
          test <- cor.test(data[[x]], data[[y]])
          list(correlation = correlation, p_value = test$p.value)
        }
        raw_results <- lapply(y_list, function(y) {
          raw_corr(data_subset_raw, "pix", y)
        })
        raw_results_df <- data.frame(
          Variable = y_list,
          Raw_Correlation = sapply(raw_results, function(x) x$correlation),
          Raw_P_Value = sapply(raw_results, function(x) x$p_value)
        )
        # Partial Correlation (with controls)
        controls <- c("time", "id")
        variables <- c("pix", y_list, controls)
        data_subset_partial <- dat_a %>% dplyr::select(all_of(variables))
        partial_corr_lm <- function(data, x, y, controls) {
          control_formula <- paste(controls, collapse = " + ")
          x_resid <- lm(as.formula(paste(x, "~", control_formula)), data = data)$residuals
          y_resid <- lm(as.formula(paste(y, "~", control_formula)), data = data)$residuals
          correlation <- cor(x_resid, y_resid, method = "pearson")
          test <- cor.test(x_resid, y_resid)
          list(correlation = correlation, p_value = test$p.value)
        }
        partial_results <- lapply(y_list, function(y) {
          partial_corr_lm(data_subset_partial, "pix", y, controls)
        })
        partial_results_df <- data.frame(
          Variable = y_list,
          Partial_Correlation = sapply(partial_results, function(x) x$correlation),
          Partial_P_Value = sapply(partial_results, function(x) x$p_value)
        )
        combined_results_df <- merge(
          raw_results_df,
          partial_results_df,
          by = "Variable"
        )
        print(combined_results_df)
        write.csv(
          combined_results_df,
          paste0(output_path, "tables/combined_correlation_table_ind_", tipo, "_", data_type, ".csv"),
          row.names = FALSE
        )
      }, error = function(e) {
        message("An error occurred during correlation analysis: ", e$message)
      })
    }
    
    # Process `dat_all4` and `dat_all13`
    
    
    # Load `dat_all4` and `dat_all13` datasets
    dat_all4 <- load_data_function(paste0(dta_path, "Ind_sample_summary4_", tipo))
    process_data(dat_all4, "4weeks")
    rm(dat_all4)
    gc()
    dat_all13 <- load_data_function(paste0(dta_path, "Ind_sample_summary13_", tipo))
    process_data(dat_all13, "13weeks")
    rm(dat_all13)
    gc()
  }
}
gc()

# Banked vs non-banked

for(i in 1:2){
  if(i==1){
    tipo = "PF"
    y_list <- c("id", "muni_cd", "tipo", "birth_year", "gender", "pix", "ted", "boleto", "card", "ccs")
  } else if(i==2){
    tipo = "PJ"
    y_list <- c("id", "muni_cd", "tipo", "birth_year", "situation", "pix", "ted", "boleto", "card", "ccs")
  }
  print(paste("Banked vs non-banked.For tipo:", tipo))
  dat_all <- load_data_function(paste0(dta_path,"Ind_sample_summary_", tipo))
  setDT(dat_all)
  dat_all <- dat_all %>%
    dplyr::mutate(across(everything(), ~ replace_na(., 0))) 
  
  dat_all <- dat_all %>% dplyr::mutate(time = week)
  dat_all <- dat_all %>% dplyr::mutate(month = week_to_month(week), year = week_to_year(week))
  
  # LETS DETERMINE WHO IS AN ACTIVE USER BEFORE PIX
  print("LETS DETERMINE WHO IS AN ACTIVE USER BEFORE PIX")
  active_user_data <- dat_all %>%
    dplyr::filter(year == 2020, month %in% c(8, 9, 10)) %>%
    dplyr::filter(card > 0 | ted > 0 | boleto > 0) %>%
    dplyr::distinct(id) %>% # Get unique IDs
    dplyr::mutate(active_user = 1)
  
  # Merge back with the original dataset
  print("Merge back with the original dataset")
  dat_all <- dat_all %>%
    dplyr::left_join(active_user_data, by = "id") %>%
    dplyr::mutate(active_user = replace_na(active_user, 0))
  
  dat_all <- dat_all %>%
    mutate_if(is.factor, as.numeric)
  
  # Calculate the proportion of active users
  print("Calculate the proportion of active users")
  total_ids <- dat_all %>% dplyr::distinct(id) %>% nrow()
  active_ids <- active_user_data %>% dplyr::distinct(id) %>% nrow()
  proportion_active <- active_ids / total_ids
  
  # Print the result
  print(paste("For tipo:", tipo))
  print(paste("Total IDs:", total_ids))
  print(paste("Active IDs:", active_ids))
  print(paste("Proportion of Active Users:", round(proportion_active * 100, 2), "%"))
  gc()
  for(j in 2:2){
    if(j==1){
      flood_a <- flood_week_after
      ending <- ""
    }
    if(j==2){
      flood_a <- flood_week_after_balanced
      ending <- "_balanced"
    }
    if(j==3){
      flood_a <- flood_week_after_balanced_covid
      ending <- "_balanced_covid"
    }
    
    dat_a <- dat_flood_function(dat_all, flood_a, mun_fe, mun_control)
    print(nrow(dat_a))  # Check number of rows after each merge or transformation
    head(dat_a)
    dat_a$flood_risk5 <- factor(dat_a$flood_risk5)
    dat_a$after_flood <- factor(dat_a$after_flood)
    dat_a$id <- as.numeric(dat_a$id)
    dat_a$week <- as.numeric(dat_a$week)
    dat_a$time <- as.numeric(dat_a$time)
    str(dat_a)  # Check the types of all relevant variables
    
    # Split the dataset into active and non-active users
    active_users <- dat_all %>% dplyr::filter(active_user == 1)
    non_active_users <- dat_all %>% dplyr::filter(active_user == 0)
    
    #iv_function_ind_new(y_list, y_label_list, "pix", "Pix", "after_flood", "flood_risk5", dat_a, ending, paste0("ind_table_", tipo))
    tryCatch({
    iv_function_ind_new2(y_list, y_label_list, "pix", "Pix", "after_flood", "flood_risk5", active_users, ending, paste0("ind_table_active_", tipo))
    }, error = function(e) {
      message("An error occurred on iv active users: ", e$message)
    })
    tryCatch({
    iv_function_ind_new2(y_list, y_label_list, "pix", "Pix", "after_flood", "flood_risk5", non_active_users, ending, paste0("ind_table_nonactive_", tipo))
    
    }, error = function(e) {
      message("An error occurred on iv non active users: ", e$message)
    })
    #Botar active*flood como instrumento
    print("First one done!")
    
    
    tryCatch({
      variables <- c("pix", y_list)
      data_subset_raw <- active_users %>% dplyr::select(all_of(variables))
      raw_corr <- function(data, x, y) {
        correlation <- cor(data[[x]], data[[y]], method = "pearson")
        test <- cor.test(data[[x]], data[[y]])
        list(correlation = correlation, p_value = test$p.value)
      }
      raw_results <- lapply(y_list, function(y) {
        raw_corr(data_subset_raw, "pix", y)
      })
      raw_results_df <- data.frame(
        Variable = y_list,
        Raw_Correlation = sapply(raw_results, function(x) x$correlation),
        Raw_P_Value = sapply(raw_results, function(x) x$p_value)
      )
      # Partial Correlation (with controls)
      controls <- c("time", "id")
      variables <- c("pix", y_list, controls)
      data_subset_partial <- active_users %>% dplyr::select(all_of(variables))
      partial_corr_lm <- function(data, x, y, controls) {
        control_formula <- paste(controls, collapse = " + ")
        x_resid <- lm(as.formula(paste(x, "~", control_formula)), data = data)$residuals
        y_resid <- lm(as.formula(paste(y, "~", control_formula)), data = data)$residuals
        correlation <- cor(x_resid, y_resid, method = "pearson")
        test <- cor.test(x_resid, y_resid)
        list(correlation = correlation, p_value = test$p.value)
      }
      partial_results <- lapply(y_list, function(y) {
        partial_corr_lm(data_subset_partial, "pix", y, controls)
      })
      partial_results_df <- data.frame(
        Variable = y_list,
        Partial_Correlation = sapply(partial_results, function(x) x$correlation),
        Partial_P_Value = sapply(partial_results, function(x) x$p_value)
      )
      combined_results_df <- merge(
        raw_results_df,
        partial_results_df,
        by = "Variable"
      )
      print(combined_results_df)
      write.csv(
        combined_results_df,
        paste0(output_path, "tables/combined_correlation_table_ind_active_", tipo, ".csv"),
        row.names = FALSE
      )
      
      
      
      variables <- c("pix", y_list)
      data_subset_raw <- non_active_users %>% dplyr::select(all_of(variables))
      raw_corr <- function(data, x, y) {
        correlation <- cor(data[[x]], data[[y]], method = "pearson")
        test <- cor.test(data[[x]], data[[y]])
        list(correlation = correlation, p_value = test$p.value)
      }
      raw_results <- lapply(y_list, function(y) {
        raw_corr(data_subset_raw, "pix", y)
      })
      raw_results_df <- data.frame(
        Variable = y_list,
        Raw_Correlation = sapply(raw_results, function(x) x$correlation),
        Raw_P_Value = sapply(raw_results, function(x) x$p_value)
      )
      # Partial Correlation (with controls)
      controls <- c("time", "id")
      variables <- c("pix", y_list, controls)
      data_subset_partial <- non_active_users %>% dplyr::select(all_of(variables))
      partial_corr_lm <- function(data, x, y, controls) {
        control_formula <- paste(controls, collapse = " + ")
        x_resid <- lm(as.formula(paste(x, "~", control_formula)), data = data)$residuals
        y_resid <- lm(as.formula(paste(y, "~", control_formula)), data = data)$residuals
        correlation <- cor(x_resid, y_resid, method = "pearson")
        test <- cor.test(x_resid, y_resid)
        list(correlation = correlation, p_value = test$p.value)
      }
      partial_results <- lapply(y_list, function(y) {
        partial_corr_lm(data_subset_partial, "pix", y, controls)
      })
      partial_results_df <- data.frame(
        Variable = y_list,
        Partial_Correlation = sapply(partial_results, function(x) x$correlation),
        Partial_P_Value = sapply(partial_results, function(x) x$p_value)
      )
      combined_results_df <- merge(
        raw_results_df,
        partial_results_df,
        by = "Variable"
      )
      print(combined_results_df)
      write.csv(
        combined_results_df,
        paste0(output_path, "tables/combined_correlation_table_ind_nonactive_", tipo, ".csv"),
        row.names = FALSE
      )
      
      
      
      
      
    }, error = function(e) {
      message("An error occurred: ", e$message)
    })
  }
  #another thing, credito is monthly. We can do a synthetic Pix ind month, or download again.
  #can we do ccs adoption as well? ted adoption, pix adoption,... once pix adopted, others adopt.
  rm(dat_a)
  rm(dat_all)
  gc()
}
gc()

##############################################
#TEST
sample_data <- function(n_individuals, n_weeks, ind_per_muni, n_seed){
  set.seed(n_seed)
  id <- rep(1:n_individuals, each = n_weeks) # 11... week times
  week <- rep(1:n_weeks, times = n_individuals) #123... ind times
  muni_cd <- rep(1:(n_individuals/ind_per_muni), each = n_weeks*ind_per_muni) # 11... (ind_per_muni*week) times 
  flood_risk5 <- rep(sample(1:5, n_individuals/ind_per_muni, replace=TRUE), each = n_weeks*ind_per_muni)
  after_flood <- unlist(lapply(1:(n_individuals/ind_per_muni), function(i){
    start_after_flood <- sample(0:(1.5*n_weeks),1) # when the flood happened
    if(start_after_flood > n_weeks){start_after_flood=n_weeks}
    after_flood_muni <- c(rep(0, start_after_flood), rep(1, n_weeks - start_after_flood))
    rep(after_flood_muni, times = ind_per_muni)
  }))
  pix <- rbinom(n_individuals * n_weeks, 1, 0.5)
  ted <- rbinom(n_individuals * n_weeks, 1, 0.5)
  boleto <- rbinom(n_individuals * n_weeks, 1, 0.5)
  card <- rbinom(n_individuals * n_weeks, 1, 0.5)
  ccs <- rbinom(n_individuals * n_weeks, 1, 0.5)
  for(i in 1:(n_individuals*n_weeks)){
    pix[i] <- ifelse(after_flood[i]==1, rbinom(1, 1, 0.3), rbinom(1, 1, 0.1))
    ted[i] <- ifelse(pix[i]==1, rbinom(1, 1, 0.2), rbinom(1, 1, 0.1))
    boleto[i] <- ifelse(pix[i]==1, rbinom(1, 1, 0.1), rbinom(1, 1, 0.2))
    card[i] <- ifelse(pix[i]==1, rbinom(1, 1, 0.3), rbinom(1, 1, 0.3))
    ccs[i] <- ifelse(pix[i]==1, rbinom(1, 1, 0.1), rbinom(1, 1, 0.1))
  }
  dat_all_test <- data.frame(muni_cd, id, week, pix, ted, boleto, card, ccs, flood_risk5, after_flood)
  
  dat_all_test$flood_risk5 <- factor(dat_all_test$flood_risk5)
  dat_all_test$after_flood <- factor(dat_all_test$after_flood)
  dat_all_test$id <- as.numeric(dat_all_test$id)
  dat_all_test$week <- as.numeric(dat_all_test$week)
  dat_all_test$time <- as.numeric(dat_all_test$week)
  dat_all_test$muni_cd <- as.numeric(dat_all_test$muni_cd)
  
  # Calculate date_flood
  date_flood <- aggregate(time ~ muni_cd, data = dat_all_test[dat_all_test$after_flood == 1, ], FUN = min)
  dat_all_test <- merge(dat_all_test, date_flood, by = "muni_cd", all.x = TRUE)
  names(dat_all_test)[names(dat_all_test) == "time.y"] <- "date_flood"
  names(dat_all_test)[names(dat_all_test) == "time.x"] <- "time"
  
  #Calculate treat
  dat_all_test <- dat_all_test %>% dplyr::mutate(treat = ifelse(is.na(date_flood), 0, 1))
  
  dat_all_test$date_flood <- as.numeric(dat_all_test$date_flood)
  head(dat_all_test)
  return(dat_all_test)
}
#dat_all_test0 <- sample_data(100000, 100, 20, 123)
#dat_all_test1 <- sample_data(10000, 50, 10, 123)
#dat_all_test2 <- sample_data(1000, 50, 10, 123)
dat_all_test3 <- sample_data(100, 50, 10, 123)


#data_list <- list(dat_all_test3, dat_all_test2, dat_all_test1)
data_list <- list(dat_all_test3,dat_all4,dat_all13)
test_time <- function(data_list){
  for(i in 1:length(data_list)){
    dat_all_test <- data_list[[i]]
    start_time <- Sys.time()
    y_list <- c("ted", "boleto", "card", "ccs")
    y_label_list <- c("ted", "boleto", "card", "ccs")
    ending <- "_balanced"
    iv_function_ind_new2(y_list, y_label_list, "pix", "Pix", "after_flood", "flood_risk5", dat_all_test, ending, "test")
    iv_function_ind_new(y_list, y_label_list, "pix", "Pix", "after_flood", "flood_risk5", dat_all_test, ending, "test")
    
    tryCatch({
      variables <- c("pix", y_list)
      data_subset_raw <- dat_all_test %>% dplyr::select(all_of(variables))
      raw_corr <- function(data, x, y) {
        correlation <- cor(data[[x]], data[[y]], method = "pearson")
        test <- cor.test(data[[x]], data[[y]])
        list(correlation = correlation, p_value = test$p.value)
      }
      raw_results <- lapply(y_list, function(y) {
        raw_corr(data_subset_raw, "pix", y)
      })
      raw_results_df <- data.frame(
        Variable = y_list,
        Raw_Correlation = sapply(raw_results, function(x) x$correlation),
        Raw_P_Value = sapply(raw_results, function(x) x$p_value)
      )
      # Partial Correlation (with controls)
      controls <- c("time", "id")
      variables <- c("pix", y_list, controls)
      data_subset_partial <- dat_all_test %>% dplyr::select(all_of(variables))
      partial_corr_lm <- function(data, x, y, controls) {
        control_formula <- paste(controls, collapse = " + ")
        x_resid <- lm(as.formula(paste(x, "~", control_formula)), data = data)$residuals
        y_resid <- lm(as.formula(paste(y, "~", control_formula)), data = data)$residuals
        correlation <- cor(x_resid, y_resid, method = "pearson")
        test <- cor.test(x_resid, y_resid)
        list(correlation = correlation, p_value = test$p.value)
      }
      partial_results <- lapply(y_list, function(y) {
        partial_corr_lm(data_subset_partial, "pix", y, controls)
      })
      partial_results_df <- data.frame(
        Variable = y_list,
        Partial_Correlation = sapply(partial_results, function(x) x$correlation),
        Partial_P_Value = sapply(partial_results, function(x) x$p_value)
      )
      combined_results_df <- merge(
        raw_results_df,
        partial_results_df,
        by = "Variable"
      )
      print(combined_results_df)
      write.csv(
        combined_results_df,
        paste0(output_path, "tables/test_combined_correlation_table_ind_", tipo, ".csv"),
        row.names = FALSE
      )
      
    }, error = function(e) {
      message("An error occurred: ", e$message)
    })
    
    # dat_all_test_syn <- dat_all_test
    # setDT(dat_all_test_syn)
    # dat_all_test_syn[, time := floor(week/4)]
    # 
    # dat_all_test_syn <- dat_all_test_syn %>% dplyr::select(-week) %>%
    #   mutate_if(is.factor, as.numeric) %>%
    #   dplyr::group_by(id, time) %>%
    #   summarize_all(max, na.rm=TRUE) %>%
    #   dplyr::ungroup() %>% dplyr::mutate(across(c("flood_risk5", "after_flood"), as.factor))
    # 
    # y_list <- c("ted", "boleto", "card", "ccs")
    # y_label_list <- c("ted", "boleto", "card", "ccs")
    # ending <- "_balanced"
    # iv_function_ind_new2(y_list, y_label_list, "pix", "Pix", "after_flood", "flood_risk5", dat_all_test_syn, ending, "test")
    # iv_function_ind_new(y_list, y_label_list, "pix", "Pix", "after_flood", "flood_risk5", dat_all_test, ending, "test")
    # 
    end_time<- Sys.time()
    execution <- as.numeric(difftime(end_time, start_time, units = "secs"))
    exe_minutes <- floor(execution/60)
    exe_seconds <- execution%%60
    #print(paste("With", n_individuals, "individuals and", n_weeks, "weeks, it took", exe_minutes, "minutes and ", exe_seconds, "seconds",i))
  }
  return()
}
test_time(data_list)


clean_data_4 <- 0
if(clean_data_4 == 1){
  tryCatch({
    dat_all <- dat_all_test3
    setDT(dat_all)
    dat_all <- dat_all %>%
      dplyr::mutate(across(!c(date_flood), ~ replace_na(., 0)))
    
    dat_all <- dat_all %>% dplyr::mutate(time = week)
    dat_all <- dat_all %>% dplyr::mutate(month = week_to_month(week), year = week_to_year(week))
    dat_all <- dat_all %>%
      mutate_if(is.factor, as.numeric)
    
    dat_all4 <- dat_all %>% 
      dplyr::mutate(time = floor(week/4),
                    date_flood = floor(date_flood/4))
    dat_all4 <- dat_all4 %>% dplyr::select(-week) %>%
      dplyr::mutate_if(is.factor, as.numeric) %>%
      dplyr::group_by(id, time) %>%
      dplyr::summarize_all(max, na.rm=FALSE) %>%
      dplyr::ungroup()
    
    dat_all4 <- dat_all4 %>% dplyr::mutate(time_to_treat = ifelse(treat==1, time - date_flood, 0),
                                           time_id_treated = ifelse(treat==0, 10000, date_flood), # For Sun and Abraham
                                           after_flood = ifelse(is.na(date_flood), 0, ifelse(time >= date_flood, 1, 0)))
    # Save dta along the way so I dont need to load again.
    dat_all4 <- dat_all4 %>% dplyr::mutate(across(where(is.factor), as.character))
    dat_all4 <- zap_label(dat_all4)
    #save_data_function(dat_all4, paste0(dta_path,"Ind_sample_summary4_", tipo))
    #rm(dat_all4)
    #gc()
    dat_all13 <- dat_all %>% 
      dplyr::mutate(time = floor(week/13),
                    date_flood = floor(date_flood/13))
    dat_all13 <- dat_all13 %>% dplyr::select(-week) %>%
      dplyr::mutate_if(is.factor, as.numeric) %>%
      dplyr::group_by(id, time) %>%
      dplyr::summarize_all(max, na.rm=FALSE) %>%
      dplyr::ungroup()
    dat_all13 <- dat_all13 %>% dplyr::mutate(time_to_treat = ifelse(treat==1, time - date_flood, 0),
                                             time_id_treated = ifelse(treat==0, 10000, date_flood), # For Sun and Abraham
                                             after_flood = ifelse(is.na(date_flood), 0, ifelse(time >= date_flood, 1, 0)))
    
    # Save dta along the way so I dont need to load again.
    dat_all13 <- dat_all13 %>% dplyr::mutate(across(where(is.factor), as.character))
    dat_all13 <- zap_label(dat_all13)
    #save_data_function(dat_all13, paste0(dta_path,"Ind_sample_summary13_", tipo))
    #rm(dat_all13)
    #gc()
    
    dat_all4$flood_risk5 <- factor(dat_all4$flood_risk5)
    dat_all4$after_flood <- factor(dat_all4$after_flood)
    dat_all4$id <- as.numeric(dat_all4$id)
    dat_all4$time <- as.numeric(dat_all4$time)
    str(dat_all4)
    
    dat_all13$flood_risk5 <- factor(dat_all13$flood_risk5)
    dat_all13$after_flood <- factor(dat_all13$after_flood)
    dat_all13$id <- as.numeric(dat_all13$id)
    dat_all13$time <- as.numeric(dat_all13$time)
    str(dat_all13)
    }, error = function(e) {
    message("An error occurred on clean_data_4: ", e$message)
  })
}

y_list <- c("ted", "boleto", "ccs")
y_label_list <- c("Wire", "Slip", "New Account")
iv_function_ind_new2(y_list, y_label_list, "pix", "Pix", "after_flood", "flood_risk5", dat_all_test3, ending, paste0("ind_table_TEST", tipo, "_", data_type))
iv_function_ind_new2(y_list, y_label_list, "pix", "Pix", "after_flood", "flood_risk5", dat_all4, ending, paste0("ind_table_TEST", tipo, "_", "4weeks"))
iv_function_ind_new2(y_list, y_label_list, "pix", "Pix", "after_flood", "flood_risk5", dat_all_test3, ending, paste0("ind_table_TEST", tipo, "_", "13weeks"))







# #TEST
# weekly_database <- read_dta(paste0(dta_path,"Base_week_muni_fake.dta"))
# weekly_database$time <- weekly_database$week
# value_list <- c("qtd_PIX_inflow")
# monthly_database2 <- dat_week_to_month(weekly_database, value_list)
# dat <- weekly_database
# 
# 
# 
# 
# 
# 
# y_list <- c("valor_boleto", "qtd_boleto", "valor_cartao_credito")
# y_label_list <- c("Log Valor", "Log Transactions", "Credito")
# ending <- "_balanced"
# iv_function_new(y_list, y_label_list, "n_cli_rec_pf_intra", "Log Pix Users","after_flood", "flood_risk5", weekly_database, ending, "payment_methods_Test")
# 
# 
# 
# 
# # Open flood_pix_weekly_fake.dta
# weekly_database <- read_dta(paste0(dta_path,"Base_week_muni_fake.dta"))
# weekly_database$time <- weekly_database$week
# weekly_database$month <- week_to_month(weekly_database$time)
# weekly_database$year <- week_to_year(weekly_database$time)
# weekly_database <- dat_flood_function(weekly_database, flood_week_after_balanced, mun_fe, mun_control)
# 
# olsmodel2 <- felm(n_cli_rec_pf_intra ~ factor(after_flood) | muni_cd + time:flood_risk5 | 0 | muni_cd, data = weekly_database)
# text_table <- stargazer(olsmodel2, type = "text")
# 
# 
# weekly_database <- weekly_database[complete.cases(weekly_database$n_cli_rec_pf_intra), ]
# weekly_database <- weekly_database[complete.cases(weekly_database$after_flood), ]
# weekly_database <- weekly_database[complete.cases(weekly_database$flood_risk5), ]
# olsmodel2 <- felm(n_cli_rec_pf_intra ~ factor(after_flood) | muni_cd + time:flood_risk5 | 0 | muni_cd, data = weekly_database)
# text_table <- stargazer(olsmodel2, type = "text")
# 
# 
# column_labels <- c("OLS")
# add_lines1 <- c("Mun. FE", "Yes")
# add_lines2 <- c("Time x Region FE", "Yes")
# text_table <- stargazer(olsmodel2, type = "text",
#                         float = FALSE,
#                         align = TRUE,
#                         dep.var.labels = c("Log Pix Users"),
#                         covariate.labels = c("Flood"),
#                         #omit.stat = c("ser", "adj.rsq", "LL", "f"),
#                         no.space = TRUE,
#                         single.row = FALSE,
#                         header = FALSE,
#                         font.size = "small",
#                         add.lines = list(add_lines1,
#                                          add_lines2),
#                         column.labels = column_labels)
# 
# 
# 
# 
# 
# 
# 
# 
# 
# # x,x_label,instrument,fe,dat, dat_name = "lusers_PF", "Log Pix Users","after_flood", "flood_risk5",dat_a, ending
# flood_a <- flood_week_after_balanced
# ending <- "_balanced"
# dat_a <- dat_flood_function(weekly_database, flood_a, mun_fe, mun_control)
