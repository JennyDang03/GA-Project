

################################################################################

#https://lost-stats.github.io/Model_Estimation/Research_Design/event_study.html
options(download.file.method = "wininet")
rm(list = ls()) ## Clear workspace

# install.packages("coefplot")
# install.packages("magrittr")
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

#Idea: just load the data, no work on it. save it beforehand. (if exists, just load)

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
mun_convert3 <- mun_convert %>% select(muni_cd, id_municipio)

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
source(paste0(R_path,"/functions/day_to_stata_month.R"))
source(paste0(R_path,"/functions/print_twfe_month.R"))
source(paste0(R_path,"/functions/print_twfe_week.R"))

source(paste0(R_path,"/functions/week_to_startDate.R"))
source(paste0(R_path,"/functions/prepare_data2.R"))
source(paste0(R_path,"/functions/prepare_data3.R"))

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
                           omit.stat=c("ser", "adj.rsq", "LL", "f"),
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
                          omit.stat=c("ser", "adj.rsq", "LL", "f"),
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
                           omit.stat = c("ser", "adj.rsq", "LL", "f"), 
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
                          omit.stat = c("ser", "adj.rsq", "LL", "f"), 
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
    filter(tipo==1) %>%
    select(lusers, time, muni_cd) %>%
    rename(lusers_PF = lusers)
  pix_use_PJ <- pix_use %>%
    filter(tipo==2) %>%
    select(lusers, time, muni_cd) %>%
    rename(lusers_PJ = lusers)
  pix_use <- merge(pix_use_PF, pix_use_PJ, by=c("muni_cd","time"), all=FALSE)
  # time, muni_cd, lusers_PJ, lusers_PF
  rm(pix_use_PF, pix_use_PJ)
  
  # CCS
  #CCS_Muni_stock_v2.dta
  # Variables: week, muni_cd, tipo, muni_stock, muni_stock_w, banked_pop
  #             lmuni_stock, lmuni_stock_w, lbanked_pop
  ccs_muni_stock <- prepare_data2("CCS_Muni_stock_v2.dta")
  ccs_muni_stock_PF <- ccs_muni_stock %>% filter(tipo==1) %>%
    select(lmuni_stock, lmuni_stock_w, lbanked_pop, time, muni_cd) %>%
    rename(lmuni_stock_w_PF = lmuni_stock_w,
           lbanked_pop_PF = lbanked_pop,
           lmuni_stock_PF = lmuni_stock)
  ccs_muni_stock_PJ <- ccs_muni_stock %>% filter(tipo==2) %>%
    select(lmuni_stock, lmuni_stock_w, lbanked_pop, time, muni_cd) %>%
    rename(lmuni_stock_w_PJ = lmuni_stock_w,
           lbanked_pop_PJ = lbanked_pop,
           lmuni_stock_PJ = lmuni_stock)
  ccs_muni_stock <- merge(ccs_muni_stock_PF, ccs_muni_stock_PJ, by=c("muni_cd","time"), all=FALSE)
  # Sum PJ and PF
  ccs_muni_stock <- ccs_muni_stock %>%
    mutate(lmuni_stock = log1p(expm1(lmuni_stock_PF) + expm1(lmuni_stock_PJ)),
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
    mutate(f_account_pop = ifelse(is.na(pop2022) | pop2022 == 0 | is.na(first_account), NA, first_account/pop2022))
  ccs_first_account_PF <- ccs_first_account %>% filter(tipo==1) %>%
    select(time, muni_cd, lfirst_account, f_account_pop, first_account) %>%
    rename(lfirst_account_PF = lfirst_account,
           f_account_pop_PF = f_account_pop,
           first_account_PF = first_account)
  ccs_first_account_PJ <- ccs_first_account %>% filter(tipo==2) %>%
    select(time, muni_cd, lfirst_account, f_account_pop, first_account) %>%
    rename(lfirst_account_PJ = lfirst_account,
           f_account_pop_PJ = f_account_pop,
           first_account_PJ = first_account)
  ccs_first_account <- merge(ccs_first_account_PF, ccs_first_account_PJ, by=c("muni_cd","time"), all=FALSE)
  #sum PF and PJ
  ccs_first_account <- ccs_first_account %>%
    mutate(lfirst_account = log1p(expm1(lfirst_account_PF) + expm1(lfirst_account_PJ)),
           f_account_pop = ifelse(is.na(f_account_pop_PF) | is.na(f_account_pop_PJ), NA, f_account_pop_PF + f_account_pop_PJ),
           first_account = ifelse(is.na(first_account_PF) | is.na(first_account_PJ), NA, first_account_PF + first_account_PJ))
  # time, muni_cd, lfirst_account_PJ, f_account_pop_PJ, first_account_PJ, lfirst_account_PF, f_account_pop_PF, first_account_PF, lfirst_account, f_account_pop, first_account
  rm(ccs_first_account_PF,ccs_first_account_PJ)
  
  # Card
  # Card_rec.dta
  # Variables: week, muni_cd, tipo, receivers, valor, receivers_credit, valor_credit, receivers_debit, valor_debit
  #            lreceivers, lvalor, lreceivers_credit, lvalor_credit, lreceivers_debit, lvalor_debit  
  card <- prepare_data2("Card_rec.dta")
  card_PF <- card %>% filter(tipo==1) %>%
    select(time, muni_cd, lreceivers, lvalor, lreceivers_credit, lvalor_credit, lreceivers_debit, lvalor_debit) %>%
    rename(lreceivers_card_PF = lreceivers,
           lvalor_card_PF = lvalor,
           lreceivers_credit_PF = lreceivers_credit, 
           lvalor_credit_PF = lvalor_credit, 
           lreceivers_debit_PF = lreceivers_debit, 
           lvalor_debit_PF = lvalor_debit)
  card_PJ <- card %>% filter(tipo==2) %>%
    select(time, muni_cd, lreceivers, lvalor, lreceivers_credit, lvalor_credit, lreceivers_debit, lvalor_debit) %>%
    rename(lreceivers_card_PJ = lreceivers,
           lvalor_card_PJ = lvalor,
           lreceivers_credit_PJ = lreceivers_credit, 
           lvalor_credit_PJ = lvalor_credit, 
           lreceivers_debit_PJ = lreceivers_debit, 
           lvalor_debit_PJ = lvalor_debit)
  card <- merge(card_PF, card_PJ, by=c("muni_cd","time"), all=FALSE)
  # sum PF and PJ
  card <- card %>%
    mutate(lreceivers_card = log1p(expm1(lreceivers_card_PF) + expm1(lreceivers_card_PJ)),
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
  boleto_PF <- boleto %>% filter(tipo==1) %>%
    mutate(ltrans_boleto_PF = log1p(trans_send+trans_rec),
           lvalor_boleto_PF = log1p(valor_send+valor_rec)) %>%
    select(time, muni_cd, ltrans_boleto_PF, lvalor_boleto_PF, lsenders, ltrans_send, lvalor_send, lreceivers, ltrans_rec, lvalor_rec) %>%
    rename(lsenders_boleto_PF = lsenders, 
           ltrans_send_boleto_PF = ltrans_send, 
           lvalor_send_boleto_PF = lvalor_send, 
           lreceivers_boleto_PF =  lreceivers, 
           ltrans_rec_boleto_PF = ltrans_rec, 
           lvalor_rec_boleto_PF = lvalor_rec)
  boleto_PJ <- boleto %>% filter(tipo==2) %>%
    mutate(ltrans_boleto_PJ = log1p(trans_send+trans_rec),
           lvalor_boleto_PJ = log1p(valor_send+valor_rec)) %>%
    select(time, muni_cd, ltrans_boleto_PJ, lvalor_boleto_PJ, lsenders, ltrans_send, lvalor_send, lreceivers, ltrans_rec, lvalor_rec) %>%
    rename(lsenders_boleto_PJ = lsenders, 
           ltrans_send_boleto_PJ = ltrans_send, 
           lvalor_send_boleto_PJ = lvalor_send, 
           lreceivers_boleto_PJ =  lreceivers, 
           ltrans_rec_boleto_PJ = ltrans_rec, 
           lvalor_rec_boleto_PJ = lvalor_rec)
  boleto <- merge(boleto_PF, boleto_PJ, by=c("muni_cd","time"), all=FALSE)
  # sum PF and PJ
  boleto <- boleto %>%
    mutate(ltrans_boleto = log1p(expm1(ltrans_boleto_PF) + expm1(ltrans_boleto_PJ)),
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
           log_valor_cartao = log1p(valor_cartao_debito+valor_cartao_credito)) %>%
    select(muni_cd, time, log_valor_TED_intra, log_qtd_TED_intra, log_qtd_cli_TED_rec_PJ, log_qtd_cli_TED_pag_PJ, log_valor_boleto, log_qtd_boleto, log_qtd_cli_pag_pf_boleto, log_qtd_cli_rec_pj_boleto, log_valor_cartao_credito, log_valor_cartao_debito, log_qtd_cli_cartao_debito, log_qtd_cli_cartao_credito, log_qtd_cli_cartao, log_valor_cartao)
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
  dat <- select(dat, "time", "muni_cd", one_of(value_list))
  dat <- dat %>%
    mutate(startDat = unlist(lapply(time, week_to_startDate)),
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
    select(-diff_days, -endDat)  # Remove the temporary column,  # Convert difftime to numeric
  
  dat <- dat %>%
    pivot_longer(cols = starts_with("Dat"),  # Select columns to pivot
                 names_to = "plus_day",  # New column name for start count
                 values_to = "Weights") %>%  # New column name for start value
    mutate(plus_day = as.numeric(sub(".*([0-9])$", "\\1", plus_day)),
           day = as.Date(startDat + plus_day, origin ="1970-01-01"))
  dat <- dat %>%
    mutate(time_id = unlist(lapply(day, day_to_stata_month))) %>%
    select(-plus_day, -startDat) 
  
  # Multiply values by weights
  dat <- dat %>%
    rowwise() %>%
    mutate(across(all_of(value_list), ~ . * Weights))
  
  # Summarize by grouping
  dat <- dat %>%
    group_by(time_id, muni_cd) %>%
    summarise(across(all_of(value_list), sum, na.rm = TRUE)) %>%
    ungroup()
  
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
    select(time, muni_cd, lnew_users, lnew_users_if, lnew_users_cg, lvalor, lvalor_ativo, lusers, lqtd, lbanks, lvalor_w, lusers_w, lqtd_w, lvalor_cartao, lusers_cartao, lqtd_cartao) %>%
    rename(lnew_users_PF = lnew_users,
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
    select(time, muni_cd, lnew_users, lnew_users_if, lnew_users_cg, lvalor, lvalor_ativo, lusers, lqtd, lbanks) %>%
    rename(lnew_users_PJ = lnew_users,
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
    select(time, muni_cd, log_caixa, log_total_deposits, log_poupanca, log_dep_prazo, log_dep_vista_PF, log_dep_vista_PJ)
  # Variables: time, muni_cd, log_caixa, log_total_deposits, log_poupanca, log_dep_prazo, log_dep_vista_PF, log_dep_vista_PJ
  
  # Credito old
  credito_old <- prepare_data2("Base_credito_muni_flood.dta")
  credito_old <- credito_old %>%
    select(time, muni_cd, log_vol_cartao, log_qtd_cli_cartao, log_vol_emprestimo_pessoal, log_qtd_cli_emp_pessoal, log_vol_credito_total, log_qtd_cli_total, log_vol_credito_total_PF, log_qtd_cli_total_PF, log_vol_credito_total_PJ, log_qtd_cli_total_PJ)
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
  dep_var_labels <- c("OLS")
  add_lines1 <- c("Mun. FE", "Yes")
  add_lines2 <- c("Time x Region FE", "Yes")
  column_labels <- c(x_label)
  for(i in 1:length(y_list)){
    dat$Y <- dat[[y_list[i]]]
    dat <- dat[complete.cases(dat$Y), ]
    ivmodel_FE <- felm(Y ~ 0| muni_cd + time:FE | (X ~ factor(Ins)) | muni_cd, data = dat)
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
                           omit.stat = c("ser", "adj.rsq", "LL", "f"), 
                           no.space = TRUE,
                           single.row = FALSE,
                           header = FALSE, 
                           font.size = "small",
                           add.lines = list(add_lines1,
                                            add_lines2),
                           column.labels = column_labels)
  
  latex_file <- file.path(output_path, paste0("tables/iv_",file_name, dat_name, ".tex")) 
  cat(latex_table, file = latex_file)
  text_table <- stargazer(models_list, title = "IV Models", type = "text",
                          float = FALSE,
                          align = TRUE, 
                          dep.var.labels = dep_var_labels,
                          covariate.labels = c("Flood", x_label),
                          omit.stat = c("ser", "adj.rsq", "LL", "f"), 
                          no.space = TRUE,
                          single.row = FALSE,
                          header = FALSE, 
                          font.size = "small",
                          add.lines = list(add_lines1,
                                           add_lines2),
                          column.labels = column_labels)
  
  text_file <- file.path(output_path, paste0("tables/iv_", file_name, dat_name, ".txt")) 
  cat(text_table, file = text_file)
  return()
}


# Create new stuff to download data from the new dta, like card_rec, boleto, ted, ccs, credito, also, get ind_sample, also adoption
# 6 months?





# Query -------------------------------------------------------------------
run_Estban <- 1
run_Estban5 <- 1









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
    
    # save in parquet
    write_parquet(monthly_database, sink = paste0(dta_path, "monthly_database.parquet"))
  }, error = function(e) {
    print(paste("Error in monthly_database:", e))
  })
} else {
  monthly_database <- read_parquet(file = file.path(dta_path, "monthly_database.parquet"))
}







if(!file.exists(paste0(dta_path, "estban_2018_2023.dta"))){
  #ESTBAN
  estban <- read_dta(paste0(dta_path, "estban_mun_basedosdados.dta"))
  #filter year, id_verbete, collapse by id_municipio, ano, mes, and id_verbete
  estban <- estban %>% filter(ano >= 2018)
  estban <- estban %>% filter(id_verbete %in% c("110","120","130","160","180","184","190","200","420","432","430","440","460","480","490500","610","710","711","712","111"))
  estban <- estban %>% group_by(ano, mes, id_municipio, id_verbete) %>% summarise(valor = sum(valor, na.rm = TRUE)) %>% ungroup()
  
  #Pivot wide id_verbete
  estban <- estban %>% pivot_wider(names_from = id_verbete, values_from = valor)
  
  #Define deposit
  estban$deposit <- estban$`420` + estban$`432`
  #Define assets and liabilities
  estban$assets <- estban$`110` + estban$`120` + estban$`130` + estban$`160` + estban$`180` + estban$`184` + estban$`190` + estban$`200`
  estban$liab <- estban$`420` + estban$`432` + estban$`430` + estban$`440` + estban$`460` + estban$`480` + estban$`490500` + estban$`610` + estban$`710` + estban$`711` + estban$`712`
  #Define caixa
  estban$caixa <- estban$`111`
  #keep only variables: ano, mes, id_municipio, deposit, assets, liab, caixa
  estban <- estban %>% select(ano, mes, id_municipio, deposit, assets, liab, caixa)
  
  # Create time_id, also, change id_municipio to muni_cd, delete ano, mes
  estban <- estban %>% mutate(time_id = (ano-1960)*12 + mes-1)
  estban <- estban %>% mutate(id_municipio = as.integer(id_municipio))
  estban <- merge(estban, mun_convert3, by="id_municipio", all.x = TRUE)
  
  #Create log variables
  estban <- estban %>% mutate(ldeposit = log1p(deposit),
                              lassets = log1p(assets),
                              lliab = log1p(liab),
                              lcaixa = log1p(caixa))
  estban <- estban %>% select(-ano, -mes, -id_municipio)
  estban <- estban %>% arrange(muni_cd, time_id)
  #Save
  write_dta(estban, paste0(dta_path, "estban_2018_2023.dta"))
}

# Estban, top 5 vs others. (after Pix)

if(!file.exists(paste0(dta_path, "estban_2018_2023_top5.dta"))){
  #ESTBAN
  estban5 <- read_dta(paste0(dta_path, "estban_mun_basedosdados.dta"))
  #filter year, id_verbete, collapse by id_municipio, ano, mes, and id_verbete
  estban5 <- estban5 %>% filter(ano >= 2018)
  estban5 <- estban5 %>% filter(id_verbete %in% c("110","120","130","160","180","184","190","200","420","432","430","440","460","480","490500","610","710","711","712","111"))
  
  count_cnpj <- estban5 %>% group_by(cnpj_basico,instituicao) %>% summarise(n = n()) %>% ungroup()
  count_cnpj <- count_cnpj %>% arrange(desc(n))
  topp5 <- count_cnpj %>% slice(1:5) %>% select(cnpj_basico) %>% mutate(top5 = 1)
  estban5 <- estban5 %>% left_join(topp5, by = "cnpj_basico") %>% mutate(top5 = ifelse(is.na(top5), 0, top5))
  estban5 <- estban5 %>% group_by(ano, mes, id_municipio, id_verbete, top5) %>% summarise(valor = sum(valor, na.rm = TRUE)) %>% ungroup()
  
  #Clean
  estban5 <- estban5 %>% pivot_wider(names_from = id_verbete, values_from = valor)
  estban5$deposit <- estban5$`420` + estban5$`432`
  estban5$assets <- estban5$`110` + estban5$`120` + estban5$`130` + estban5$`160` + estban5$`180` + estban5$`184` + estban5$`190` + estban5$`200`
  estban5$liab <- estban5$`420` + estban5$`432` + estban5$`430` + estban5$`440` + estban5$`460` + estban5$`480` + estban5$`490500` + estban5$`610` + estban5$`710` + estban5$`711` + estban5$`712`
  estban5$caixa <- estban5$`111`
  estban5 <- estban5 %>% select(top5, ano, mes, id_municipio, deposit, assets, liab, caixa)
  
  estban5 <- estban5 %>% mutate(time_id = (ano-1960)*12 + mes-1)
  estban5 <- estban5 %>% mutate(id_municipio = as.integer(id_municipio))
  estban5 <- merge(estban5, mun_convert3, by="id_municipio", all.x = TRUE)
  
  #Create log variables
  estban5 <- estban5 %>% mutate(ldeposit = log1p(deposit),
                                lassets = log1p(assets),
                                lliab = log1p(liab),
                                lcaixa = log1p(caixa))
  estban5 <- estban5 %>% select(-ano, -mes, -id_municipio)
  #sort by muni_cd, time_id, top5
  estban5 <- estban5 %>% arrange(muni_cd, time_id, top5)
  #Save
  write_dta(estban5, paste0(dta_path, "estban_2018_2023_top5.dta"))
}


estban_public5 <- prepare_data2("estban_2018_2023_top5.dta")
# ESTBAN
# ldeposit, lassets, lliab, lcaixa
estban_public <- prepare_data2("estban_2018_2023.dta")
estban_public <- estban_public %>%
  select(time, muni_cd, ldeposit, lassets, lliab, lcaixa)
# Variables: time, muni_cd, ldeposit, lassets, lliab, lcaixa

monthly_database <- merge(monthly_database, estban_public, by=c("muni_cd","time"), all=TRUE)

run_estban_public <- 1
if(run_estban_public == 1){
  tryCatch({
    for(i in 4:6){
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
      variables <- c("ldeposit", "lassets", "lliab", "lcaixa")
      variables_labels <- c("Log Deposits", "Log Assets", "Log Liabilities", "Log Money Inventory")
      for(z in 1:length(variables)){
        iv_function(variables[[z]],variables_labels[[z]],"lusers_PF", "Log Pix Users","after_flood", "flood_risk5",dat_a, ending)
      }
      rm(dat_a)
      #rm(dat_b)
    }
  }, error = function(e) {
    print(paste("Error in run_month:", e))
  })
}


# How to do top5 vs others?

# Also, IV for digital vs traditional banks




















# #TEST
# weekly_database <- read_dta(paste0(dta_path,"Base_week_muni_fake.dta"))
# weekly_database$time <- weekly_database$week
# value_list <- c("qtd_PIX_inflow")
# monthly_database2 <- dat_week_to_month(weekly_database, value_list)
# dat <- weekly_database
# 
