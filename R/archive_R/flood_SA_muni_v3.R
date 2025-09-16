################################################################################

# flood_SA_muni_v3.R
# Input:  "Base_week_muni_flood.dta"
#         "Base_week_muni_flood_beforePIX.dta"
# Output: "muni_pix_flood_",y,".png"
#         "before_pix_flood_",y,".png"
# y:  log_valor_PIX_inflow, log_valor_PIX_outflow, log_valor_PIX_intra, 
#     log_qtd_PIX_inflow, log_qtd_PIX_outflow, log_qtd_PIX_intra,
#     log_n_cli_pag_pf_intra, log_n_cli_rec_pf_intra, log_n_cli_pag_pj_intra, log_n_cli_rec_pj_intra
#     log_valor_TED_intra, log_qtd_TED_intra, log_qtd_cli_TED_rec_PJ, log_qtd_cli_TED_pag_PJ
#     log_valor_boleto, log_qtd_boleto
#     log_valor_cartao_debito, log_valor_cartao_credito, log_qtd_cli_cartao_debito, log_qtd_cli_cartao_credito

# The goal: see the effect of flood on bank transactions before and after Pix.

# To do: I did the ylimits by hand. Thats not the ideal, do the deletion of some variables in the future. 
#         Other cool graphs could be created merging graphs together (delete TWFE). 


################################################################################

#https://lost-stats.github.io/Model_Estimation/Research_Design/event_study.html

options(download.file.method = "wininet")

#install.packages(c("data.table","fixest","haven","ggplot2"))

library(data.table) ## For some minor data wrangling
library(fixest)     ## NB: Requires version >=0.9.0
library(haven)
library(ggplot2)

rm(list = ls()) ## Clear workspace

setwd("//sbcdf176/Pix_Matheus$")

# Set file paths
log_path <- "//sbcdf176/PIX_Matheus$/Stata/log"
dta_path <- "//sbcdf176/PIX_Matheus$/Stata/dta"
output_path <- "//sbcdf176/PIX_Matheus$/Output"
origdata_path <- "//sbcdf176/PIX_Matheus$/DadosOriginais"
R_path <- "//sbcdf176/PIX_Matheus$/R"
#dta_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta"
#output_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output"
#log_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/log"

log_file <- file.path(log_path, "flood_SA_muni_v3.log")
#sink(log_file) ## redirect R output to log file

################################################################################
# Add extra Controls: (id_regiao_imediata x Time) or id_regiao_intermediaria x Time

# TWFE and SA, no extra controls
graph_function3 <- function(y,dat){
  dat$Y <- dat[[y]]
  
  mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                     muni_cd + week,            ## FEs
                   cluster = ~muni_cd,                          ## Clustered SEs
                   data = dat)
  # See `?sunab`.
  mod_sa = feols(Y ~ sunab(time_id_treated, week) | ## The only thing that's changed
                   muni_cd + week,       ## FEs
                 cluster = ~muni_cd,
                 data = dat)
  return(list(mod_twfe, mod_sa))
}

graph_function4 <- function(y,dat,xlimit_l,xlimit_u, main_title){
  png(file.path(output_path,paste0("muni_pix_flood_",y,".png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function3(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Week',
        main = main_title,
        ci_level = 0.90, xlim = c(xlimit_l,xlimit_u))
  
  legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
         legend = c("TWFE", "Sun & Abraham (2020)"), cex = 0.8)
  dev.off()
}

graph_function5 <- function(y,dat,xlimit_l,xlimit_u, main_title){
  png(file.path(output_path,paste0("before_pix_flood_",y,".png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function3(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Week',
        main = main_title,
        ci_level = 0.90, xlim = c(xlimit_l,xlimit_u))
  
  legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
         legend = c("TWFE", "Sun & Abraham (2020)"), cex = 0.8)
  dev.off()
}

################################################################################
# TWFE, No extra controls
graph_function6 <- function(y,dat){
  dat$Y <- dat[[y]]
  
  mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                     muni_cd + week,            ## FEs
                   cluster = ~muni_cd,                          ## Clustered SEs
                   data = dat)
  return(mod_twfe)
}

# TWFE, with extra controls
graph_function7 <- function(y,dat){
  dat$Y <- dat[[y]]
  
  mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                     muni_cd + week  + flood_risk:week + rural_urban:week + nome_regiao_code:week + pop2010_quart:week + capital_uf:week, ## FEs
                   cluster = ~muni_cd,                          ## Clustered SEs
                   data = dat)
  return(mod_twfe)
}
# pib_2017_quart has a few hundred missing values
# pop2022_quart is after the fact
graph_function8 <- function(y,dat){
  dat$Y <- dat[[y]]
  
  mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                     muni_cd + week  + flood_risk:week, ## FEs
                   cluster = ~muni_cd,                          ## Clustered SEs
                   data = dat)
  return(mod_twfe)
}


# TWFE, with and without extra controls: id_regiao_intermediaria:Time
graph_function18 <- function(y,dat,xlimit_l,xlimit_u, main_title){

  png(file.path(output_path,paste0("muni_pix_flood_",y,"nocontrol.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function6(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Week',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5))
  
  legend("bottomleft", col = c(1), pch = c(20), 
         legend = c("TWFE"), cex = 0.8)
  dev.off()  
  
  # png(file.path(output_path,paste0("muni_pix_flood_",y,"control.png")), width = 640*4, height = 480*4, res = 200)
  # iplot(graph_function7(y,dat), sep = 0.5, ref.line = -1,
  #       xlab = 'Week',
  #       main = main_title,
  #       ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5))
  # 
  # legend("bottomleft", col = c(1), pch = c(20), 
  #        legend = c("TWFE"), cex = 0.8)
  # dev.off()
  
  png(file.path(output_path,paste0("muni_pix_flood_",y,"control1.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function8(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Week',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5))
  
  legend("bottomleft", col = c(1), pch = c(20), 
         legend = c("TWFE"), cex = 0.8)
  dev.off()
}
graph_function19 <- function(y,dat,xlimit_l,xlimit_u, main_title){

  png(file.path(output_path,paste0("before_pix_flood_",y,"nocontrol.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function6(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Week',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5))
  
  legend("bottomleft", col = c(1), pch = c(20), 
         legend = c("TWFE"), cex = 0.8)
  dev.off()  

  # png(file.path(output_path,paste0("before_pix_flood_",y,"control.png")), width = 640*4, height = 480*4, res = 200)
  # iplot(graph_function7(y,dat), sep = 0.5, ref.line = -1,
  #       xlab = 'Week',
  #       main = main_title,
  #       ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5))
  # 
  # legend("bottomleft", col = c(1), pch = c(20),
  #        legend = c("TWFE"), cex = 0.8)
  # dev.off()
  
  png(file.path(output_path,paste0("before_pix_flood_",y,"control1.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function8(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Week',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5))
  
  legend("bottomleft", col = c(1), pch = c(20), 
         legend = c("TWFE"), cex = 0.8)
  dev.off()
}

################################################################################
# Flood at the municipality level - Weekly Level
################################################################################

################################
# After Pix
################################

# Load every municipality and every Pix

# Load already prepared data!
dat_flood <- read_dta(file.path(dta_path,"Base_week_muni_flood.dta"))
#dat_flood <- read_dta(file.path(dta_path,"flood_pix_weekly_fake.dta"))
mun_fe <- read_dta(file.path(dta_path, "mun_fe.dta"))
dat_flood <- merge(dat_flood, mun_fe)

# converts to data.table
setDT(dat_flood)

dat_flood[, treat := ifelse(is.na(date_flood), 0, 1)]
table(dat_flood$treat)
dat_flood[, time_to_treat := ifelse(treat==1, week - date_flood, 0)]
table(dat_flood$time_to_treat)
# Following Sun and Abraham, we give our never-treated units a fake "treatment" date far outside the relevant study period.
dat_flood[, time_id_treated := ifelse(treat==0, 10000, date_flood)]
table(dat_flood$time_id_treated)

# Set Limits
xlimit_low <- -39 
xlimit_up <- 52 
xlimits <- seq(ceiling(xlimit_low*1.1),ceiling(xlimit_up*1.1),by=1)
dat_flood <- subset(dat_flood,time_to_treat %in% xlimits)
#

#Pix

dat_flood[, log_qtd_PIX_inflow := log(qtd_PIX_inflow + 1)]
dat_flood[, log_qtd_PIX_outflow := log(qtd_PIX_outflow + 1)]
dat_flood[, log_qtd_PIX_intra := log(qtd_PIX_intra + 1)]
graph_function18("log_valor_PIX_inflow",dat_flood, xlimit_low, xlimit_up, "Log Value Pix Inflow")
graph_function18("log_valor_PIX_outflow",dat_flood, xlimit_low, xlimit_up, "Log Value Pix Outflow")
graph_function18("log_valor_PIX_intra",dat_flood, xlimit_low, xlimit_up, "Log Value Pix Intra")

dat_flood[, log_valor_rec := log(valor_PIX_inflow + valor_PIX_intra + 1)]
graph_function18("log_valor_rec",dat_flood, xlimit_low, xlimit_up, "Log Value Pix Received")
dat_flood[, log_valor_sent := log(valor_PIX_outflow + valor_PIX_intra + 1)]
graph_function18("log_valor_sent",dat_flood, xlimit_low, xlimit_up, "Log Value Pix Sent")

dat_flood[, log_qtd_PIX_inflow := log(qtd_PIX_inflow + 1)]
dat_flood[, log_qtd_PIX_outflow := log(qtd_PIX_outflow + 1)]
dat_flood[, log_qtd_PIX_intra := log(qtd_PIX_intra + 1)]
graph_function18("log_qtd_PIX_inflow",dat_flood, xlimit_low, xlimit_up, "Log Transactions Pix Inflow")
graph_function18("log_qtd_PIX_outflow",dat_flood, xlimit_low, xlimit_up, "Log Transactions Pix Outflow")
graph_function18("log_qtd_PIX_intra",dat_flood, xlimit_low, xlimit_up, "Log Transactions Pix Intra")

dat_flood[, log_qtd_rec := log(qtd_PIX_inflow + qtd_PIX_intra + 1)]
graph_function18("log_qtd_rec",dat_flood, xlimit_low, xlimit_up, "Log Transactions Pix Received")
dat_flood[, log_qtd_sent := log(qtd_PIX_outflow + qtd_PIX_intra + 1)]
graph_function18("log_qtd_sent",dat_flood, xlimit_low, xlimit_up, "Log Transactions Pix Sent")

# n_cli_pag_pf_intra n_cli_rec_pf_intra n_cli_pag_pj_intra n_cli_rec_pj_intra
# n_cli_rec_pf_inflow n_cli_rec_pj_inflow 
# n_cli_pag_pf_outflow n_cli_pag_pj_outflow 

dat_flood[, log_n_cli_pag_pf_intra := log(n_cli_pag_pf_intra + 1)]
graph_function18("log_n_cli_pag_pf_intra",dat_flood, xlimit_low, xlimit_up, "Log Quantity of People Sending Pix - Intra")
dat_flood[, log_n_cli_rec_pf_intra := log(n_cli_rec_pf_intra + 1)]
graph_function18("log_n_cli_rec_pf_intra",dat_flood, xlimit_low, xlimit_up, "Log Quantity of People Receiving Pix - Intra")

dat_flood[, log_n_cli_pag_pj_intra := log(n_cli_pag_pj_intra + 1)]
graph_function18("log_n_cli_pag_pj_intra",dat_flood, xlimit_low, xlimit_up, "Log Quantity of Firms Sending Pix - Intra")
dat_flood[, log_n_cli_rec_pj_intra := log(n_cli_rec_pj_intra + 1)]
graph_function18("log_n_cli_rec_pj_intra",dat_flood, xlimit_low, xlimit_up, "Log Quantity of Firms Receiving Pix - Intra")

dat_flood[, log_n_cli_rec_pf_inflow := log(n_cli_rec_pf_inflow + 1)]
graph_function18("log_n_cli_rec_pf_inflow",dat_flood, xlimit_low, xlimit_up, "Log Quantity of People Receiving Pix - Inflow")
dat_flood[, log_n_cli_rec_pj_inflow := log(n_cli_rec_pj_inflow + 1)]
graph_function18("log_n_cli_rec_pj_inflow",dat_flood, xlimit_low, xlimit_up, "Log Quantity of Firms Receiving Pix - Inflow")

dat_flood[, n_cli_pag_pf_outflow := log(n_cli_pag_pf_outflow + 1)]
graph_function18("log_n_cli_pag_pf_outflow",dat_flood, xlimit_low, xlimit_up, "Log Quantity of People Sending Pix - Outflow")
dat_flood[, n_cli_pag_pj_outflow := log(n_cli_pag_pj_outflow + 1)]
graph_function18("log_n_cli_pag_pj_outflow",dat_flood, xlimit_low, xlimit_up, "Log Quantity of Firms Sending Pix - Outflow")



dat_flood[, log_n_cli_pag_pf := log((n_cli_pag_pf_outflow + n_cli_pag_pf_intra) + 1)]
graph_function18("log_n_cli_pag_pf",dat_flood, xlimit_low, xlimit_up, "Log Quantity of People Sending Pix")

dat_flood[, log_n_cli_rec_pf := log((n_cli_rec_pf_inflow + n_cli_rec_pf_intra) + 1)]
graph_function18("log_n_cli_rec_pf",dat_flood, xlimit_low, xlimit_up, "Log Quantity of People Receiving Pix")

dat_flood[, log_n_cli_pag_pj := log((n_cli_pag_pj_outflow + n_cli_pag_pj_intra) + 1)]
graph_function18("log_n_cli_pag_pj",dat_flood, xlimit_low, xlimit_up, "Log Quantity of Firms Sending Pix")

dat_flood[, log_n_cli_rec_pj := log((n_cli_rec_pj_inflow + n_cli_rec_pj_intra) + 1)]
graph_function18("log_n_cli_rec_pj",dat_flood, xlimit_low, xlimit_up, "Log Quantity of Firms Receiving Pix")

#TED
# * valor_TED_intra qtd_TED_intra qtd_cli_TED_rec_PJ qtd_cli_TED_rec_PF qtd_cli_TED_pag_PJ qtd_cli_TED_pag_PF 

graph_function18("log_valor_TED_intra",dat_flood, xlimit_low, xlimit_up, "Log Value TED Intra") #
graph_function18("log_qtd_TED_intra",dat_flood, xlimit_low, xlimit_up, "Log Transactions TED Intra") #

graph_function18("log_qtd_cli_TED_rec_PJ",dat_flood, xlimit_low, xlimit_up, "Log Quantity of Firms Receiving TED")
graph_function18("log_qtd_cli_TED_pag_PJ",dat_flood, xlimit_low, xlimit_up, "Log Quantity of Firms Sending TED")

#graph_function18("log_qtd_cli_TED_rec_PF",dat_flood, xlimit_low, xlimit_up, "Log Quantity of People Receiving TED")
#graph_function18("log_qtd_cli_TED_pag_PF",dat_flood, xlimit_low, xlimit_up, "Log Quantity of People Sending TED")


#Boleto
# * valor_boleto qtd_boleto qtd_cli_pag_pf_boleto qtd_cli_pag_pj_boleto qtd_cli_rec_pj_boleto 
#qtd_cli_pag_pf_boleto qtd_cli_pag_pj_boleto qtd_cli_rec_pj_boleto

graph_function18("log_valor_boleto",dat_flood, xlimit_low, xlimit_up, "Log Value Boleto")
graph_function18("log_qtd_boleto",dat_flood, xlimit_low, xlimit_up, "Log Transactions Boleto")

graph_function18("log_qtd_cli_pag_pf_boleto",dat_flood, xlimit_low, xlimit_up, "Log Quantity of People Sending Boleto")
graph_function18("log_qtd_cli_pag_pj_boleto",dat_flood, xlimit_low, xlimit_up, "Log Quantity of Firms Sending Boleto")
graph_function18("log_qtd_cli_rec_pj_boleto",dat_flood, xlimit_low, xlimit_up, "Log Quantity of Firms Receiving Boleto")

#Cartao 
# * valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito 

graph_function18("log_valor_cartao_debito",dat_flood, xlimit_low, xlimit_up, "Log Value Debit Card")
graph_function18("log_valor_cartao_credito",dat_flood, xlimit_low, xlimit_up, "Log Value Credit Card")

graph_function18("log_qtd_cli_cartao_debito",dat_flood, xlimit_low, xlimit_up, "Log Quantity of Firms accepting Debit Card")
graph_function18("log_qtd_cli_cartao_credito",dat_flood, xlimit_low, xlimit_up, "Log Quantity of Firms accepting Credit Card")
#???? is it people or firms accepting credit card?

################################
# Before Pix
################################

# Change Base_week_muni_flood_beforePIX.dta to include 2018

# Load already prepared data!
dat_flood <- read_dta(file.path(dta_path,"Base_week_muni_flood_beforePIX.dta"))
mun_fe <- read_dta(file.path(dta_path, "mun_fe.dta"))
dat_flood <- merge(dat_flood, mun_fe)
# converts to data.table
setDT(dat_flood)

dat_flood[, treat := ifelse(is.na(date_flood), 0, 1)]
table(dat_flood$treat)
dat_flood[, time_to_treat := ifelse(treat==1, week - date_flood, 0)]
table(dat_flood$time_to_treat)
# Following Sun and Abraham, we give our never-treated units a fake "treatment" date far outside the relevant study period.
dat_flood[, time_id_treated := ifelse(treat==0, 10000, date_flood)]
table(dat_flood$time_id_treated)

# Set Limits
xlimit_low <- -39
xlimit_up <- 52
xlimits <- seq(ceiling(xlimit_low*1.1),ceiling(xlimit_up*1.1),by=1)
dat_flood <- subset(dat_flood,time_to_treat %in% xlimits)
#

#TED
# * valor_TED_intra qtd_TED_intra qtd_cli_TED_rec_PJ qtd_cli_TED_rec_PF qtd_cli_TED_pag_PJ qtd_cli_TED_pag_PF 

graph_function19("log_valor_TED_intra",dat_flood, xlimit_low, xlimit_up, "Log Value TED Intra") #
graph_function19("log_qtd_TED_intra",dat_flood, xlimit_low, xlimit_up, "Log Transactions TED Intra") #

graph_function19("log_qtd_cli_TED_rec_PJ",dat_flood, xlimit_low, xlimit_up, "Log Quantity of Firms Receiving TED")
graph_function19("log_qtd_cli_TED_pag_PJ",dat_flood, xlimit_low, xlimit_up, "Log Quantity of Firms Sending TED")

#graph_function19("log_qtd_cli_TED_rec_PF",dat_flood, xlimit_low, xlimit_up, "Log Quantity of People Receiving TED")
#graph_function19("log_qtd_cli_TED_pag_PF",dat_flood, xlimit_low, xlimit_up, "Log Quantity of People Sending TED")


#Boleto
# * valor_boleto qtd_boleto qtd_cli_pag_pf_boleto qtd_cli_pag_pj_boleto qtd_cli_rec_pj_boleto 
#qtd_cli_pag_pf_boleto qtd_cli_pag_pj_boleto qtd_cli_rec_pj_boleto

graph_function19("log_valor_boleto",dat_flood, xlimit_low, xlimit_up, "Log Value Boleto")
graph_function19("log_qtd_boleto",dat_flood, xlimit_low, xlimit_up, "Log Transactions Boleto")

graph_function19("log_qtd_cli_pag_pf_boleto",dat_flood, xlimit_low, xlimit_up, "Log Quantity of People Sending Boleto")
graph_function19("log_qtd_cli_pag_pj_boleto",dat_flood, xlimit_low, xlimit_up, "Log Quantity of Firms Sending Boleto")
graph_function19("log_qtd_cli_rec_pj_boleto",dat_flood, xlimit_low, xlimit_up, "Log Quantity of Firms Receiving Boleto")

#Cartao 
# * valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito 

graph_function19("log_valor_cartao_debito",dat_flood, xlimit_low, xlimit_up, "Log Value Debit Card")
graph_function19("log_valor_cartao_credito",dat_flood, xlimit_low, xlimit_up, "Log Value Credit Card")

graph_function19("log_qtd_cli_cartao_debito",dat_flood, xlimit_low, xlimit_up, "Log Quantity of Firms using Debit Card")
graph_function19("log_qtd_cli_cartao_credito",dat_flood, xlimit_low, xlimit_up, "Log Quantity of Firms using Credit Card")

#sink()