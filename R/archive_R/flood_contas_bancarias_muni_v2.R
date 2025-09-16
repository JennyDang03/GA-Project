################################################################################
#flood_contas_bancarias_muni_v2.R

# Input: CCS_muni_banco_PF_flood_collapsed.dta 
#       CCS_muni_banco_PF_flood_collapsed_beforePIX.dta
# Output: "accounts_muni_flood_",y,"_PF.png"
#         "accounts_muni_flood_",y,"_PF_beforePIX.png"
# y: log_qtd

# The goal: Create a graph with 3 lines: traditional, digital, others. 
# Then we calculate changes after a flood on log quantity of bank accounts
# for PJ and PF, for Before and After Pix. 

# To do: we can separate btw low deposit rate, high deposit rate
#         we need to create another dta collapsed to all banks to see the reaction of flood on bank accounts. Maybe put before and after in the same graph.

################################################################################

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

log_file <- file.path(log_path, "flood_contas_bancarias_muni.log")
#sink(log_file) ## redirect R output to log file

################################################################################
graph_function2 <- function(y,dat){
  dat$Y <- dat[[y]]
  
  #mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
  #                   muni_cd + time_id,            ## FEs
  #                 cluster = ~muni_cd,                          ## Clustered SEs
  #                 data = dat)
  
  dat_subset <- subset(dat,bank_type %in% c(1))
  
  # See `?sunab`.
  mod_sa_traditional = feols(Y ~ sunab(time_id_treated, time_id) | ## The only thing that's changed
                               muni_cd + time_id,            ## FEs
                             cluster = ~muni_cd,
                             data = dat_subset)
  
  dat_subset <- dat[bank_type %in% c(2)]
  
  # See `?sunab`.
  mod_sa_digital = feols(Y ~ sunab(time_id_treated, time_id) | ## The only thing that's changed
                           muni_cd + time_id,            ## FEs
                         cluster = ~muni_cd,
                         data = dat_subset)
  #dat_subset <- dat[bank_type %in% c(3)]
  
  # See `?sunab`.
  #mod_sa_others = feols(Y ~ sunab(time_id_treated, time_id) | ## The only thing that's changed
  #                        muni_cd + time_id,            ## FEs
  #                      cluster = ~muni_cd,
  #                      data = dat_subset)
  
  return(list(mod_sa_traditional, mod_sa_digital))
}

graph_function4 <- function(y,dat,xlimit_l,xlimit_u, main_title){
  png(file.path(output_path,paste0("accounts_muni_flood_",y,"_PF.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function2(y,dat), sep = 0.333, ref.line = -1,
        xlab = 'Month',
        main = main_title,
        ci_level = 0.90, xlim = c(xlimit_l,xlimit_u))
  legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
         legend = c("Traditional", "Digital"), cex = 0.8)
  dev.off()
}

graph_function5 <- function(y,dat,xlimit_l,xlimit_u, main_title){
  png(file.path(output_path,paste0("accounts_muni_flood_",y,"_PF_beforePIX.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function2(y,dat), sep = 0.333, ref.line = -1,
        xlab = 'Month',
        main = main_title,
        ci_level = 0.90, xlim = c(xlimit_l,xlimit_u))
  legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
         legend = c("Traditional", "Digital"), cex = 0.8)
  dev.off()
}

# TWFE, with and without extra controls

graph_function_pf_nocontrol <- function(y,dat){
  dat$Y <- dat[[y]]

  dat_subset <- subset(dat,bank_type %in% c(1))
  mod_twfe_traditional = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                     muni_cd + time_id,            ## FEs
                   cluster = ~muni_cd,                          ## Clustered SEs
                   data = dat_subset)

  dat_subset <- dat[bank_type %in% c(2)]
  mod_twfe_digital = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                     muni_cd + time_id,            ## FEs
                   cluster = ~muni_cd,                          ## Clustered SEs
                   data = dat_subset)

  #dat_subset <- dat[bank_type %in% c(3)]
  #mod_sa_others = feols(Y ~ sunab(time_id_treated, time_id) | ## The only thing that's changed
  #                        muni_cd + time_id,            ## FEs
  #                      cluster = ~muni_cd,
  #                      data = dat_subset)
  
  return(list(mod_twfe_traditional, mod_twfe_digital))
}

graph_function_pf_control <- function(y,dat){
  dat$Y <- dat[[y]]
  
  dat_subset <- subset(dat,bank_type %in% c(1))
  mod_twfe_traditional = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                                 muni_cd + time_id + flood_risk:time_id + rural_urban:time_id + nome_regiao_code:time_id + pop2010_quart:time_id + capital_uf:time_id,            ## FEs
                               cluster = ~muni_cd,                          ## Clustered SEs
                               data = dat_subset)
  
  dat_subset <- dat[bank_type %in% c(2)]
  mod_twfe_digital = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                             muni_cd + time_id + flood_risk:time_id + rural_urban:time_id + nome_regiao_code:time_id + pop2010_quart:time_id + capital_uf:time_id,            ## FEs
                           cluster = ~muni_cd,                          ## Clustered SEs
                           data = dat_subset)
  
  #dat_subset <- dat[bank_type %in% c(3)]
  #mod_sa_others = feols(Y ~ sunab(time_id_treated, time_id) | ## The only thing that's changed
  #                        muni_cd + time_id,            ## FEs
  #                      cluster = ~muni_cd,
  #                      data = dat_subset)
  
  return(list(mod_twfe_traditional, mod_twfe_digital))
}

graph_function_pf_control1 <- function(y,dat){
  dat$Y <- dat[[y]]
  
  dat_subset <- subset(dat,bank_type %in% c(1))
  mod_twfe_traditional = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                                 muni_cd + time_id + flood_risk:time_id,            ## FEs
                               cluster = ~muni_cd,                          ## Clustered SEs
                               data = dat_subset)
  
  dat_subset <- dat[bank_type %in% c(2)]
  mod_twfe_digital = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                             muni_cd + time_id + flood_risk:time_id,            ## FEs
                           cluster = ~muni_cd,                          ## Clustered SEs
                           data = dat_subset)
  
  #dat_subset <- dat[bank_type %in% c(3)]
  #mod_sa_others = feols(Y ~ sunab(time_id_treated, time_id) | ## The only thing that's changed
  #                        muni_cd + time_id,            ## FEs
  #                      cluster = ~muni_cd,
  #                      data = dat_subset)
  
  return(list(mod_twfe_traditional, mod_twfe_digital))
}


graph_function_pf <- function(y,dat,xlimit_l,xlimit_u, main_title){
  png(file.path(output_path,paste0("accounts_muni_flood_",y,"_PFnocontrol.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function_pf_nocontrol(y,dat), sep = 0.333, ref.line = -1,
        xlab = 'Month',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5),
        col = c("green4", "blue4"), pch = c(20,17), cex = 0.9)
  
  legend("bottomleft", col = c("green4", "blue4"), pch = c(20,17), 
         legend = c("Traditional", "Digital"), cex = 0.8)
  dev.off()
  
  # png(file.path(output_path,paste0("accounts_muni_flood_",y,"_PFcontrol.png")), width = 640*4, height = 480*4, res = 200)
  # iplot(graph_function_pf_control(y,dat), sep = 0.333, ref.line = -1,
  #       xlab = 'Month',
  #       main = main_title,
  #       ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5),
  #       col = c("green4", "blue4"), pch = c(20,17), cex = 0.9)
  # 
  # legend("bottomleft", col = c("green4", "blue4"), pch = c(20,17), 
  #        legend = c("Traditional", "Digital"), cex = 0.8)
  # dev.off()
  
  
  png(file.path(output_path,paste0("accounts_muni_flood_",y,"_PFcontrol1.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function_pf_control1(y,dat), sep = 0.333, ref.line = -1,
        xlab = 'Month',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5),
        col = c("green4", "blue4"), pch = c(20,17), cex = 0.9)
  
  legend("bottomleft", col = c("green4", "blue4"), pch = c(20,17), 
         legend = c("Traditional", "Digital"), cex = 0.8)
  dev.off()
}

graph_function_pf_before <- function(y,dat,xlimit_l,xlimit_u, main_title){
  png(file.path(output_path,paste0("accounts_muni_flood_",y,"_PFnocontrol_beforePIX.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function_pf_nocontrol(y,dat), sep = 0.333, ref.line = -1,
        xlab = 'Month',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5),
        col = c("green4", "blue4"), pch = c(20,17), cex = 0.9)
  
  legend("bottomleft", col = c("green4", "blue4"), pch = c(20,17), 
         legend = c("Traditional", "Digital"), cex = 0.8)
  dev.off()
  
  # png(file.path(output_path,paste0("accounts_muni_flood_",y,"_PFcontrol_beforePIX.png")), width = 640*4, height = 480*4, res = 200)
  # iplot(graph_function_pf_control(y,dat), sep = 0.333, ref.line = -1,
  #       xlab = 'Month',
  #       main = main_title,
  #       ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5),
  #       col = c("green4", "blue4"), pch = c(20,17), cex = 0.9)
  # 
  # legend("bottomleft", col = c("green4", "blue4"), pch = c(20,17), 
  #        legend = c("Traditional", "Digital"), cex = 0.8)
  # dev.off()
  
  png(file.path(output_path,paste0("accounts_muni_flood_",y,"_PFcontrol1_beforePIX.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function_pf_control1(y,dat), sep = 0.333, ref.line = -1,
        xlab = 'Month',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5),
        col = c("green4", "blue4"), pch = c(20,17), cex = 0.9)
  
  legend("bottomleft", col = c("green4", "blue4"), pch = c(20,17), 
         legend = c("Traditional", "Digital"), cex = 0.8)
  dev.off()
}

################################################################################
# Flood at the municipality level - Monthly Level
################################################################################

################################
# After Pix - PF
################################


# Load every municipality and every Pix

# Load already prepared data!
dat_flood <- read_dta(file.path(dta_path,"CCS_muni_banco_PF_flood_collapsed.dta"))
mun_fe <- read_dta(file.path(dta_path, "mun_fe.dta"))
dat_flood <- merge(dat_flood, mun_fe)
# converts to data.table
setDT(dat_flood)

dat_flood[, treat := ifelse(is.na(date_flood), 0, 1)]
table(dat_flood$treat)
dat_flood[, time_to_treat := ifelse(treat==1, time_id - date_flood, 0)]
table(dat_flood$time_to_treat)
# Following Sun and Abraham, we give our never-treated units a fake "treatment" date far outside the relevant study period.
dat_flood[, time_id_treated := ifelse(treat==0, 10000, date_flood)]
table(dat_flood$time_id_treated)

# Set Limits
xlimit_low <- -9
xlimit_up <- 12
xlimits <- seq(ceiling(xlimit_low*1.1-2),ceiling(xlimit_up*1.1+2),by=1)
dat_flood <- subset(dat_flood,time_to_treat %in% xlimits)
#

#Contas Bancarias
# log_qtd 

# PF
graph_function_pf("log_qtd",dat_flood, xlimit_low, xlimit_up, "Log Bank Accounts")



################################
# Before Pix - PF
################################

# Load every municipality and every Pix

# Load already prepared data!
dat_flood <- read_dta(file.path(dta_path,"CCS_muni_banco_PF_flood_collapsed_beforePIX.dta"))
mun_fe <- read_dta(file.path(dta_path, "mun_fe.dta"))
dat_flood <- merge(dat_flood, mun_fe)
# converts to data.table
setDT(dat_flood)

dat_flood[, treat := ifelse(is.na(date_flood), 0, 1)]
table(dat_flood$treat)
dat_flood[, time_to_treat := ifelse(treat==1, time_id - date_flood, 0)]
table(dat_flood$time_to_treat)
# Following Sun and Abraham, we give our never-treated units a fake "treatment" date far outside the relevant study period.
dat_flood[, time_id_treated := ifelse(treat==0, 10000, date_flood)]
table(dat_flood$time_id_treated)

# Set Limits
xlimit_low <- -9
xlimit_up <- 12
xlimits <- seq(ceiling(xlimit_low*1.1-2),ceiling(xlimit_up*1.1+2),by=1)
dat_flood <- subset(dat_flood,time_to_treat %in% xlimits)
#

#Contas Bancarias
# log_qtd 

# PF
graph_function_pf_before("log_qtd",dat_flood, xlimit_low, xlimit_up, "Log Bank Accounts")

