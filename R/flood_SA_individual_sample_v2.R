################################################################################
# flood_SA_individual_sample_v2.R
# Input: Pix_individuo_sample_flood.dta
#         
# Output: "pix_flood_sample",y,".png"
#         
# y: log_value_sent log_trans_sent log_value_rec log_trans_rec log_value_self log_trans_self 
#     after_first_pix_rec after_first_pix_sent sender receiver user

# The goal: See effects of flood on Individuals transactions to other and to themselves

# To do:  It is only PF. 

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
#output_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output/tables"
#log_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/log"

log_file <- file.path(log_path, "flood_SA_individual.log")
#sink(log_file) ## redirect R output to log file

# Graph TWFE and SA
################################################################################
graph_function <- function(y,dat){
  dat$Y <- dat[[y]]
  
  mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                     id + time_id,            ## FEs
                   cluster = ~muni_cd,                          ## Clustered SEs
                   data = dat)
  # See `?sunab`.
  mod_sa = feols(Y ~ sunab(time_id_treated, time_id) | ## The only thing that's changed
                   id + time_id,            ## FEs
                 cluster = ~muni_cd,
                 data = dat)
  return(list(mod_twfe, mod_sa))
}

graph_function2 <- function(y,dat,xlimit_l, xlimit_u, main_title){
  png(file.path(output_path,paste0("pix_flood_sample",y,".png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Month',
        main = main_title,
        ci_level = 0.90, xlim = c(xlimit_l,xlimit_u))

  legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
         legend = c("TWFE", "Sun & Abraham (2020)"), cex = 0.8)
  dev.off()
}

# TWFE, with and without extra controls: id_regiao_intermediaria:Time

graph_function_nocontrol <- function(y,dat){
  dat$Y <- dat[[y]]
  
  mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                     id + time_id,            ## FEs
                   cluster = ~muni_cd,                          ## Clustered SEs
                   data = dat)
  return(mod_twfe)
}
graph_function_control <- function(y,dat){
  dat$Y <- dat[[y]]
  
  mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                     id + time_id + flood_risk:time_id + rural_urban:time_id + nome_regiao_code:time_id + pop2010_quart:time_id + capital_uf:time_id,            ## FEs
                   cluster = ~muni_cd,                          ## Clustered SEs
                   data = dat)
  return(mod_twfe)
}
graph_function_control1 <- function(y,dat){
  dat$Y <- dat[[y]]
  
  mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                     id + time_id + flood_risk:time_id,            ## FEs
                   cluster = ~muni_cd,                          ## Clustered SEs
                   data = dat)
  return(mod_twfe)
}
graph_function_pf <- function(y,dat,xlimit_l, xlimit_u, main_title){
  
  png(file.path(output_path,paste0("pix_flood_sample",y,"_PFnocontrol.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function_nocontrol(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Month',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5))
  legend("bottomleft", col = c(1), pch = c(20), 
         legend = c("TWFE"), cex = 0.8)
  dev.off()
  
  # png(file.path(output_path,paste0("pix_flood_sample",y,"_PFcontrol.png")), width = 640*4, height = 480*4, res = 200)
  # iplot(graph_function_control(y,dat), sep = 0.5, ref.line = -1,
  #       xlab = 'Month',
  #       main = main_title,
  #       ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5))
  # legend("bottomleft", col = c(1), pch = c(20), 
  #        legend = c("TWFE"), cex = 0.8)
  # dev.off()
  
  png(file.path(output_path,paste0("pix_flood_sample",y,"_PFcontrol1.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function_control1(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Month',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5))
  legend("bottomleft", col = c(1), pch = c(20), 
         legend = c("TWFE"), cex = 0.8)
  dev.off()
}

################################################################################



################################################################################
# Flood at the individual level - Monthly Level
################################################################################

# Load already prepared data!
dat_flood <- read_dta(file.path(dta_path,"Pix_individuo_sample_flood.dta"))
mun_fe <- read_dta(file.path(dta_path, "mun_fe.dta"))
dat_flood <- merge(dat_flood, mun_fe)
# converts to data.table
setDT(dat_flood)

dat_flood[, treat := ifelse(is.na(date_flood), 0, 1)]
table(dat_flood$treat)
dat_flood[, time_to_treat := ifelse(treat==1, time_id - date_flood, 0)]
table(dat_flood$time_to_treat)
# Following Sun and Abraham, we give our never-treated units a fake "treatment" date far outside the relevant study period.
dat_flood[, time_id_treated := ifelse(treat==0, 1000000, date_flood)]
table(dat_flood$time_id_treated)

# Set Limits
xlimit_low <- -9
xlimit_up <- 12
xlimits <- seq(ceiling(xlimit_low*1.1-2),ceiling(xlimit_up*1.1+2),by=1)
dat_flood <- subset(dat_flood,time_to_treat %in% xlimits)

# PF

graph_function_pf("after_first_pix_rec",dat_flood, xlimit_low, xlimit_up, "Receiver Adoption")
graph_function_pf("after_first_pix_sent",dat_flood, xlimit_low, xlimit_up, "Sender Adoption")

graph_function_pf("receiver",dat_flood, xlimit_low, xlimit_up, "Receiver")
graph_function_pf("sender",dat_flood, xlimit_low, xlimit_up, "Senders")
graph_function_pf("user",dat_flood, xlimit_low, xlimit_up, "Users")

graph_function_pf("log_trans_rec",dat_flood, xlimit_low, xlimit_up, "Log Transactions Received")
graph_function_pf("log_trans_sent",dat_flood, xlimit_low, xlimit_up, "Log Transactions Sent")
graph_function_pf("log_value_rec",dat_flood, xlimit_low, xlimit_up, "Log Value Received")
graph_function_pf("log_value_sent",dat_flood, xlimit_low, xlimit_up, "Log Value Sent")

graph_function_pf("log_trans_self",dat_flood, xlimit_low, xlimit_up, "Log Transactions")
graph_function_pf("log_value_self",dat_flood, xlimit_low, xlimit_up, "Log Value")

##########

#sink()