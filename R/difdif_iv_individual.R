# difdif_iv_individual.

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

# Ind_sample_summary_PF.dta
# Variables: "week", "id", "muni_cd", "tipo", "birth_year", "gender", "pix", "ted", "boleto", "ccs" <-- tem card mas a gente nao deve usar
# Ind_sample_summary_PJ.dta
# Variables: "week", "id", "muni_cd", "tipo", "birth_year", "situation", "pix", "ted", "boleto", "ccs", "card"



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
          Raw_P_Value = sapply(raw_results, function(x) x$p_value),
          Raw_SE = sapply(raw_results, function(x) x$se)
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
          Partial_P_Value = sapply(partial_results, function(x) x$p_value),
          Partial_SE = sapply(partial_results, function(x) x$se)
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

run_ind_tables4 <- 0
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
          Raw_P_Value = sapply(raw_results, function(x) x$p_value),
          Raw_SE = sapply(raw_results, function(x) x$se)
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
          Partial_P_Value = sapply(partial_results, function(x) x$p_value),
          Partial_SE = sapply(partial_results, function(x) x$se)
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


# Banked vs non-banked - save them and do everything.  
run_active <- 1
if(run_active == 1) {
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
    # Save
    save_data_function(dat_all, paste0(dta_path, "Ind_sample_summary_", tipo, "_activedummy"))
    
    gc()
    # for(j in 2:2){
    #   if(j==1){
    #     flood_a <- flood_week_after
    #     ending <- ""
    #   }
    #   if(j==2){
    #     flood_a <- flood_week_after_balanced
    #     ending <- "_balanced"
    #   }
    #   if(j==3){
    #     flood_a <- flood_week_after_balanced_covid
    #     ending <- "_balanced_covid"
    #   }
    #   
    #   dat_a <- dat_flood_function(dat_all, flood_a, mun_fe, mun_control)
    #   print(nrow(dat_a))  # Check number of rows after each merge or transformation
    #   head(dat_a)
    #   dat_a$flood_risk5 <- factor(dat_a$flood_risk5)
    #   dat_a$after_flood <- factor(dat_a$after_flood)
    #   dat_a$id <- as.numeric(dat_a$id)
    #   dat_a$week <- as.numeric(dat_a$week)
    #   dat_a$time <- as.numeric(dat_a$time)
    #   str(dat_a)  # Check the types of all relevant variables
    #   
    #   # Split the dataset into active and non-active users
    #   active_users <- dat_all %>% dplyr::filter(active_user == 1)
    #   non_active_users <- dat_all %>% dplyr::filter(active_user == 0)
    #   
    #   #save
    #   save_data_function(active_users, paste0(dta_path, "Ind_sample_summary_", tipo, "_active", ending))
    #   save_data_function(non_active_users, paste0(dta_path, "Ind_sample_summary_", tipo, "_nonactive", ending))
    #   
    #   
    #   #iv_function_ind_new(y_list, y_label_list, "pix", "Pix", "after_flood", "flood_risk5", dat_a, ending, paste0("ind_table_", tipo))
    #   tryCatch({
    #     iv_function_ind_new2(y_list, y_label_list, "pix", "Pix", "after_flood", "flood_risk5", active_users, ending, paste0("ind_table_active_", tipo))
    #   }, error = function(e) {
    #     message("An error occurred on iv active users: ", e$message)
    #   })
    #   tryCatch({
    #     iv_function_ind_new2(y_list, y_label_list, "pix", "Pix", "after_flood", "flood_risk5", non_active_users, ending, paste0("ind_table_nonactive_", tipo))
    #   }, error = function(e) {
    #     message("An error occurred on iv non active users: ", e$message)
    #   })
    #   #Botar active*flood como instrumento
    #   print("First one done!")
    #   
    #   
    #   tryCatch({
    #     variables <- c("pix", y_list)
    #     data_subset_raw <- active_users %>% dplyr::select(all_of(variables))
    #     raw_corr <- function(data, x, y) {
    #       correlation <- cor(data[[x]], data[[y]], method = "pearson")
    #       test <- cor.test(data[[x]], data[[y]])
    #       list(correlation = correlation, p_value = test$p.value)
    #     }
    #     raw_results <- lapply(y_list, function(y) {
    #       raw_corr(data_subset_raw, "pix", y)
    #     })
    #     raw_results_df <- data.frame(
    #       Variable = y_list,
    #       Raw_Correlation = sapply(raw_results, function(x) x$correlation),
    #       Raw_P_Value = sapply(raw_results, function(x) x$p_value),
    #       Raw_SE = sapply(raw_results, function(x) x$se)
    #     )
    #     # Partial Correlation (with controls)
    #     controls <- c("time", "id")
    #     variables <- c("pix", y_list, controls)
    #     data_subset_partial <- active_users %>% dplyr::select(all_of(variables))
    #     partial_corr_lm <- function(data, x, y, controls) {
    #       control_formula <- paste(controls, collapse = " + ")
    #       x_resid <- lm(as.formula(paste(x, "~", control_formula)), data = data)$residuals
    #       y_resid <- lm(as.formula(paste(y, "~", control_formula)), data = data)$residuals
    #       correlation <- cor(x_resid, y_resid, method = "pearson")
    #       test <- cor.test(x_resid, y_resid)
    #       list(correlation = correlation, p_value = test$p.value)
    #     }
    #     partial_results <- lapply(y_list, function(y) {
    #       partial_corr_lm(data_subset_partial, "pix", y, controls)
    #     })
    #     partial_results_df <- data.frame(
    #       Variable = y_list,
    #       Partial_Correlation = sapply(partial_results, function(x) x$correlation),
    #       Partial_P_Value = sapply(partial_results, function(x) x$p_value),
    #       Partial_SE = sapply(partial_results, function(x) x$se)
    #     )
    #     combined_results_df <- merge(
    #       raw_results_df,
    #       partial_results_df,
    #       by = "Variable"
    #     )
    #     print(combined_results_df)
    #     write.csv(
    #       combined_results_df,
    #       paste0(output_path, "tables/combined_correlation_table_ind_active_", tipo, ".csv"),
    #       row.names = FALSE
    #     )
    #     
    #     
    #     
    #     variables <- c("pix", y_list)
    #     data_subset_raw <- non_active_users %>% dplyr::select(all_of(variables))
    #     raw_corr <- function(data, x, y) {
    #       correlation <- cor(data[[x]], data[[y]], method = "pearson")
    #       test <- cor.test(data[[x]], data[[y]])
    #       list(correlation = correlation, p_value = test$p.value)
    #     }
    #     raw_results <- lapply(y_list, function(y) {
    #       raw_corr(data_subset_raw, "pix", y)
    #     })
    #     raw_results_df <- data.frame(
    #       Variable = y_list,
    #       Raw_Correlation = sapply(raw_results, function(x) x$correlation),
    #       Raw_P_Value = sapply(raw_results, function(x) x$p_value),
    #       Raw_SE = sapply(raw_results, function(x) x$se)
    #     )
    #     # Partial Correlation (with controls)
    #     controls <- c("time", "id")
    #     variables <- c("pix", y_list, controls)
    #     data_subset_partial <- non_active_users %>% dplyr::select(all_of(variables))
    #     partial_corr_lm <- function(data, x, y, controls) {
    #       control_formula <- paste(controls, collapse = " + ")
    #       x_resid <- lm(as.formula(paste(x, "~", control_formula)), data = data)$residuals
    #       y_resid <- lm(as.formula(paste(y, "~", control_formula)), data = data)$residuals
    #       correlation <- cor(x_resid, y_resid, method = "pearson")
    #       test <- cor.test(x_resid, y_resid)
    #       list(correlation = correlation, p_value = test$p.value)
    #     }
    #     partial_results <- lapply(y_list, function(y) {
    #       partial_corr_lm(data_subset_partial, "pix", y, controls)
    #     })
    #     partial_results_df <- data.frame(
    #       Variable = y_list,
    #       Partial_Correlation = sapply(partial_results, function(x) x$correlation),
    #       Partial_P_Value = sapply(partial_results, function(x) x$p_value),
    #       Partial_SE = sapply(partial_results, function(x) x$se)
    #     )
    #     combined_results_df <- merge(
    #       raw_results_df,
    #       partial_results_df,
    #       by = "Variable"
    #     )
    #     print(combined_results_df)
    #     write.csv(
    #       combined_results_df,
    #       paste0(output_path, "tables/combined_correlation_table_ind_nonactive_", tipo, ".csv"),
    #       row.names = FALSE
    #     )
    #     
    #     
    #     
    #     
    #     
    #   }, error = function(e) {
    #     message("An error occurred: ", e$message)
    #   })
    # }
    #another thing, credito is monthly. We can do a synthetic Pix ind month, or download again.
    #can we do ccs adoption as well? ted adoption, pix adoption,... once pix adopted, others adopt.
    #rm(dat_a)
    rm(dat_all)
    gc()
  }
  gc()
}

# Aggregate everything. Do adoption graphs, do active adoption/use, non active adoption/use. 

run_aggreg <- 1
if(run_aggreg == 1) {
  for(i in 1:2){
    if(i==1){
      tipo = "PF"
    } else if(i==2){
      tipo = "PJ"
    }
    # Load the data
    dat_activedummy <- load_data_function(paste0(dta_path, "Ind_sample_summary_", tipo, "_activedummy"))
    # "week", "id", "muni_cd", "tipo", "birth_year", "gender", "pix", "ted", "boleto", "card", "ccs"
    # "week", "id", "muni_cd", "tipo", "birth_year", "situation", "pix", "ted", "boleto", "card", "ccs", active_user, time, month, year
    
    # select the variables I need: "week", "muni_cd", "tipo", "active_user", "time", "month", "year", "pix", "ted", "boleto", "card", "ccs"
    # Aggregate it at the "week", "muni_cd", "tipo", "active_user", "time", "month", "year",
    dat_activedummy <- dat_activedummy %>%
      dplyr::group_by(week, muni_cd, tipo, active_user, time, month, year) %>%
      dplyr::summarise(
        pix = sum(pix),
        ted = sum(ted),
        boleto = sum(boleto),
        card = sum(card),
        ccs = sum(ccs)
      ) %>%
      dplyr::ungroup()
    # get the log of each variable:
    dat_activedummy <- dat_activedummy %>%
      dplyr::mutate(
        lpix = log(pix + 1),
        lted = log(ted + 1),
        lboleto = log(boleto + 1),
        lcard = log(card + 1),
        lccs = log(ccs + 1)
      )
    
    #save the data
    setDT(dat_activedummy)
    save_data_function(dat_activedummy, paste0(dta_path, "Ind_sample_summary_", tipo, "_activedummy_agg"))
    
    for(i in 2:2){
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
      # Flood, FE, and Control Variables
      dat_a <- merge(dat_activedummy, flood, by=c("muni_cd","time"), all=FALSE) # it deletes if no match.
      dat_a <- merge(dat_a, mun_fe, by="muni_cd", all.x = TRUE) 
      dat_a <- merge(dat_a, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
      
      # Event Study Variables
      dat_a[, treat := ifelse(is.na(date_flood), 0, 1)]
      dat_a[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
      dat_a[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
      dat_a[, after_flood := ifelse(is.na(date_flood), 0, ifelse(time >= date_flood, 1, 0))]
      
      #save
      save_data_function(dat_a, paste0(dta_path, "Ind_sample_summary_", tipo, "_activedummy_agg", ending))
    }
    rm(dat_activedummy)
    rm(dat_a)
    gc()
  }
}
gc()

run_analysis <- 1
if(run_analysis == 1) {
  variables <- c("lpix", "lted", "lboleto","lcard", "lccs")
  variables_labels <- c("Log Pix users", "Log Wire users", "Log Slip users", "Log Card acceptance", "Log New Accounts")        
  tryCatch({
    for(i in 2:2){
      if(i==1){
        xll <- xl
        xuu <- xu
        ending <- "_"
      }
      if(i==2){
        xll <- xl_balanced
        xuu <- xu_balanced
        ending <- "_balanced_"
      }
      if(i==3){
        xll <- xl_balanced_covid
        xuu <- xu_balanced_covid
        ending <- "_balanced_covid_"
      }
      
      for(j in 1:2){
        if(j==1){
          beginning <- paste0("Ind_sample_aggreg_PF",ending)
          dat_a <- load_data_function(paste0(dta_path, "Ind_sample_summary_", "PF", "_activedummy_agg", ending))
        }
        if(j==2){
          beginning <- paste0("Ind_sample_aggreg_PJ",ending)
          dat_a <- load_data_function(paste0(dta_path, "Ind_sample_summary_", "PJ", "_activedummy_agg", ending))
        }
        
        for(z in 1:length(variables)){
          twfe2(beginning,variables[[z]],"constant","constant","flood_risk5", list(dat_a), c(variables[[z]]))
          print_twfe_week(beginning, variables[[z]], variables_labels[[z]], c(variables[[z]]), c(variables_labels[[z]]), xll, xuu)
        }
        rm(dat_a)
      }
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in run_analysis:", e))
  })
}




















######### NEW CODE ##########
run_create_adoption <- 1
if(run_create_adoption == 1) {
  for(i in 1:2){
    if(i==1){
      tipo = "PF"
    } else if(i==2){
      tipo = "PJ"
    }
    # Load the data
    dat_all <- load_data_function(paste0(dta_path, "Ind_sample_summary_", tipo, "_activedummy"))
    # select the variables I need: "id", "muni_cd", "active_user", "month", "year", "pix"
    dat_all <- dat_all %>%
      dplyr::select(id, muni_cd, active_user, month, year, pix)
    # Create time_id = (year-1960)*12+month-1
    dat_all[, time_id := (year - 1960) * 12 + month - 1]
    
    dat_all[, `:=`(
      first_pix = as.integer(time_id == min(time_id[pix > 0]))
    ), by = .(id, muni_cd, active_user, month, year)]
    
    data2 <- dat_all[, .(
      adoption = sum(first_pix)
    ), by = .(time_id, muni_cd, active_user, month, year)]
    
    data2[, `:=`(
      ladoption = log1p(adoption)
    )]
    
    #Variables: time_id, muni_cd, active_user, month, year, adoption, ladoption
    #save
    save_data_function(data2, paste0(dta_path, "Ind_sample_summary_", tipo, "_activedummy_adoption"))
    
  }
}

# Run both, active, and non active.

run_adoption <- 1
if(run_adoption == 1) {
  variables_adoption <- c("ladoption")
  variables_adoption_labels <- c("Log Pix adoption")
  for(i in 1:2){
    if(i==1){
      tipo = "PF"
      flood <- flood_week_after_balanced
      xll <- xl_balanced
      xuu <- xu_balanced
      ending <- "_balanced_"
    } else if(i==2){
      tipo = "PJ"
      flood <- flood_week_after_balanced
      xll <- xl_balanced
      xuu <- xu_balanced
      ending <- "_balanced_"
    }
    
    # Load the data
    dat_active_nonactive <- load_data_function(paste0(dta_path, "Ind_sample_summary_", tipo, "_activedummy_adoption"))
    #Variables: time_id, muni_cd, active_user, month, year, adoption, ladoption
    setDT(dat_active_nonactive)
    dat_active <- dat_active_nonactive %>%
      dplyr::filter(active_user == 1) %>%
      dplyr::select(time_id, muni_cd, month, year, ladoption)
    dat_active$time <- dat_active$time_id

    # Flood, FE, and Control Variables
    dat_active <- merge(dat_active, flood, by=c("muni_cd","time"), all=FALSE) # it deletes if no match.
    dat_active <- merge(dat_active, mun_fe, by="muni_cd", all.x = TRUE) 
    dat_active <- merge(dat_active, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
    
    # Event Study Variables
    dat_active[, treat := ifelse(is.na(date_flood), 0, 1)]
    dat_active[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
    dat_active[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
    dat_active[, after_flood := ifelse(is.na(date_flood), 0, ifelse(time >= date_flood, 1, 0))]
    
    dat_nonactive <- dat_active_nonactive %>%
      dplyr::filter(active_user == 0) %>%
      dplyr::select(time_id, muni_cd, month, year, ladoption)
    dat_nonactive$time <- dat_nonactive$time_id
    
    # Flood, FE, and Control Variables
    dat_nonactive <- merge(dat_nonactive, flood, by=c("muni_cd","time"), all=FALSE) # it deletes if no match.
    dat_nonactive <- merge(dat_nonactive, mun_fe, by="muni_cd", all.x = TRUE)
    dat_nonactive <- merge(dat_nonactive, mun_control, by=c("muni_cd","month","year"), all.x = TRUE)
    
    # Event Study Variables
    dat_nonactive[, treat := ifelse(is.na(date_flood), 0, 1)]
    dat_nonactive[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
    dat_nonactive[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
    dat_nonactive[, after_flood := ifelse(is.na(date_flood), 0, ifelse(time >= date_flood, 1, 0))]
    
    # Pix_ind_sample_month_PF_adoption.dta
    # Variables: time_id, muni_cd, tipo, adoption, adoption_send, adoption_rec, adoption_self
    #             ladoption, ladopt_send, ladopt_rec, ladopt_self
    dat_all <- prepare_data("Pix_ind_sample_month_", tipo, "_adoption.dta",flood,mun_fe,mun_control)
    
    
    beginning_adoption <- paste0("Pix_ind_sample_",tipo,"_adoption_month",ending)
    beginning_adoption_active <- paste0("Pix_ind_sample_",tipo,"_adoption_month_active",ending)
    for(z in 1:length(variables_adoption)){
      twfe2(beginning_adoption,variables_adoption[[z]],"constant","constant","flood_risk5", list(dat_active, dat_nonactive), c("Active","Non Active"))
      print_twfe_week(beginning_adoption, variables_adoption[[z]], variables_adoption_labels[[z]], c("Active","Non Active"), c("Active","Non Active"), xll, xuu)
      
      twfe2(beginning_adoption_active,variables_adoption[[z]],"constant","constant","flood_risk5", list(dat_all), c("pix"))
      print_twfe_week(beginning_adoption_active, variables_adoption[[z]], variables_adoption_labels[[z]], c("pix"), c("Pix"), xll, xuu)
    } 
  }
}






