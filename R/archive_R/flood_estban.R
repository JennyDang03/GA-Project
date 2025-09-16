#flood_estban.R


################################################################################
#flood_estban.R
# Input: "$dta\Estban_detalhado_HHI_flood.dta"
#         Estban_detalhado_HHI_flood_beforePIX
#         Estban_detalhado_flood_collapsed
#        "$dta\Estban_detalhado_flood_beforePIX_collapsed.dta"
# Output: 
#         
# y: 


# The goal: 


# To do: 

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
#output_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output"
#log_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/log"

log_file <- file.path(log_path, "flood_estban.log")
sink(log_file) ## redirect R output to log file

################################################################################
prepare_data <- function(dta_path,file,xlimit_low, xlimit_up){
  dat_flood <- read_dta(file.path(dta_path,file))
  mun_fe <- read_dta(file.path(dta_path, "mun_fe.dta"))
  dat_flood <- merge(dat_flood, mun_fe)
  # converts to data.table
  setDT(dat_flood)
  
  dat_flood[, treat := ifelse(is.na(date_flood), 0, 1)]
  #table(dat_flood$treat)
  dat_flood[, time_to_treat := ifelse(treat==1, time_id - date_flood, 0)]
  #table(dat_flood$time_to_treat)
  # Following Sun and Abraham, we give our never-treated units a fake "treatment" date far outside the relevant study period.
  dat_flood[, time_id_treated := ifelse(treat==0, 10000, date_flood)]
  #table(dat_flood$time_id_treated)
  
  # Set Limits
  xlimits <- seq(ceiling(xlimit_low*1.333-2),ceiling(xlimit_up*1.333+2),by=1)
  dat_flood <- subset(dat_flood,time_to_treat %in% xlimits)
  #
  return(dat_flood)
}

graph_function1 <- function(y,dat){
  dat$Y <- dat[[y]]
  
  #mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
  #                   muni_cd + time_id,            ## FEs
  #                 cluster = ~muni_cd,                          ## Clustered SEs
  #                 data = dat)
  
  dat_subset <- subset(dat, large_bank %in% c(1))
  
  # See `?sunab`.
  mod_sa_top5 = feols(Y ~ sunab(time_id_treated, time_id) | ## The only thing that's changed
                               muni_cd + time_id,            ## FEs
                             cluster = ~muni_cd,
                             data = dat_subset)
  
  dat_subset <- dat[large_bank %in% c(0)]
  
  # See `?sunab`.
  mod_sa_others = feols(Y ~ sunab(time_id_treated, time_id) | ## The only thing that's changed
                           muni_cd + time_id,            ## FEs
                         cluster = ~muni_cd,
                         data = dat_subset)

  return(list(mod_sa_top5, mod_sa_others))
}
graph_function2 <- function(y,dat){
  dat$Y <- dat[[y]]
  
  mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                     muni_cd + time_id,            ## FEs
                   cluster = ~muni_cd,                          ## Clustered SEs
                   data = dat)
  
  # See `?sunab`.
  mod_sa = feols(Y ~ sunab(time_id_treated, time_id) | ## The only thing that's changed
                        muni_cd + time_id,            ## FEs
                      cluster = ~muni_cd,
                      data = dat)
  
  
  return(list(mod_twfe, mod_sa))
}
graph_function11 <- function(y,dat,xlimit_l,xlimit_u, main_title){
  png(file.path(output_path,paste0("estban_flood_",y,".png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function1(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Month',
        main = main_title,
        ci_level = 0.90, xlim = c(xlimit_l,xlimit_u))
  
  legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
         legend = c("Top 5 Banks", "Others"), cex = 0.8)
  dev.off()
}

graph_function21 <- function(y,dat,xlimit_l,xlimit_u, main_title){
  png(file.path(output_path,paste0("estban_flood_",y,"_beforePIX.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function1(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Month',
        main = main_title,
        ci_level = 0.90, xlim = c(xlimit_l,xlimit_u))
  
  legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
         legend = c("Top 5 Banks", "Others"), cex = 0.8)
  dev.off()
}

graph_function32 <- function(y,dat,xlimit_l,xlimit_u, main_title){
  png(file.path(output_path,paste0("estban_flood_",y,".png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function2(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Month',
        main = main_title,
        ci_level = 0.90, xlim = c(xlimit_l,xlimit_u))
  
  legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
         legend = c("TWFE", "Sun & Abraham (2020)"), cex = 0.8)
  dev.off()
}

graph_function42 <- function(y,dat,xlimit_l,xlimit_u, main_title){
  png(file.path(output_path,paste0("estban_flood_",y,"_beforePIX.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function2(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Month',
        main = main_title,
        ci_level = 0.90, xlim = c(xlimit_l,xlimit_u))
  
  legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
         legend = c("TWFE", "Sun & Abraham (2020)"), cex = 0.8)
  dev.off()
}
####################################
graph_function_nocontrol <- function(y,dat){
  dat$Y <- dat[[y]]

  dat_subset <- subset(dat, large_bank %in% c(1))
  mod_twfe_top5 = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                                 muni_cd + time_id,            ## FEs
                               cluster = ~muni_cd,                          ## Clustered SEs
                               data = dat_subset)
  
  dat_subset <- dat[large_bank %in% c(0)]
  mod_twfe_others = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                          muni_cd + time_id,            ## FEs
                        cluster = ~muni_cd,                          ## Clustered SEs
                        data = dat_subset)
  return(list(mod_twfe_top5, mod_twfe_others))
}
graph_function_control <- function(y,dat){
  dat$Y <- dat[[y]]
  
  dat_subset <- subset(dat, large_bank %in% c(1))
  mod_twfe_top5 = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                          muni_cd + time_id+ flood_risk:time_id + rural_urban:time_id + nome_regiao_code:time_id + pop2010_quart:time_id + capital_uf:time_id,            ## FEs
                        cluster = ~muni_cd,                          ## Clustered SEs
                        data = dat_subset)
  
  dat_subset <- dat[large_bank %in% c(0)]
  mod_twfe_others = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                            muni_cd + time_id+ flood_risk:time_id + rural_urban:time_id + nome_regiao_code:time_id + pop2010_quart:time_id + capital_uf:time_id,            ## FEs
                          cluster = ~muni_cd,                          ## Clustered SEs
                          data = dat_subset)
  return(list(mod_twfe_top5, mod_twfe_others))
}
graph_function_control1 <- function(y,dat){
  dat$Y <- dat[[y]]
  
  dat_subset <- subset(dat, large_bank %in% c(1))
  mod_twfe_top5 = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                          muni_cd + time_id+ flood_risk:time_id,            ## FEs
                        cluster = ~muni_cd,                          ## Clustered SEs
                        data = dat_subset)
  
  dat_subset <- dat[large_bank %in% c(0)]
  mod_twfe_others = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                            muni_cd + time_id+ flood_risk:time_id,            ## FEs
                          cluster = ~muni_cd,                          ## Clustered SEs
                          data = dat_subset)
  return(list(mod_twfe_top5, mod_twfe_others))
}

graph_function111 <- function(y,dat,xlimit_l,xlimit_u, main_title){
  png(file.path(output_path,paste0("estban_flood_",y,"nocontrol.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function_nocontrol(y,dat), sep = 0.333, ref.line = -1,
        xlab = 'Month',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5),
        col = c("green4", "blue4"), pch = c(20,17), cex = 0.9)
  
  legend("bottomleft", col = c("green4", "blue4"), pch = c(20,17), 
         legend = c("Traditional", "Digital"), cex = 0.8)
  dev.off()
  
  # png(file.path(output_path,paste0("estban_flood_",y,"control.png")), width = 640*4, height = 480*4, res = 200)
  # iplot(graph_function_control(y,dat), sep = 0.333, ref.line = -1,
  #       xlab = 'Month',
  #       main = main_title,
  #       ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5),
  #       col = c("green4", "blue4"), pch = c(20,17), cex = 0.9)
  # 
  # legend("bottomleft", col = c("green4", "blue4"), pch = c(20,17), 
  #        legend = c("Traditional", "Digital"), cex = 0.8)
  # dev.off()
  
  png(file.path(output_path,paste0("estban_flood_",y,"control1.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function_control1(y,dat), sep = 0.333, ref.line = -1,
        xlab = 'Month',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5),
        col = c("green4", "blue4"), pch = c(20,17), cex = 0.9)
  
  legend("bottomleft", col = c("green4", "blue4"), pch = c(20,17), 
         legend = c("Traditional", "Digital"), cex = 0.8)
  dev.off()
}
graph_function211 <- function(y,dat,xlimit_l,xlimit_u, main_title){
  png(file.path(output_path,paste0("estban_flood_",y,"_beforePIXnocontrol.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function_nocontrol(y,dat), sep = 0.333, ref.line = -1,
        xlab = 'Month',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5),
        col = c("green4", "blue4"), pch = c(20,17), cex = 0.9)
  
  legend("bottomleft", col = c("green4", "blue4"), pch = c(20,17), 
         legend = c("Traditional", "Digital"), cex = 0.8)
  dev.off()
  
  # png(file.path(output_path,paste0("estban_flood_",y,"_beforePIXcontrol.png")), width = 640*4, height = 480*4, res = 200)
  # iplot(graph_function_control(y,dat), sep = 0.333, ref.line = -1,
  #       xlab = 'Month',
  #       main = main_title,
  #       ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5),
  #       col = c("green4", "blue4"), pch = c(20,17), cex = 0.9)
  # 
  # legend("bottomleft", col = c("green4", "blue4"), pch = c(20,17), 
  #        legend = c("Traditional", "Digital"), cex = 0.8)
  # dev.off()
  
  png(file.path(output_path,paste0("estban_flood_",y,"_beforePIXcontrol1.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function_control1(y,dat), sep = 0.333, ref.line = -1,
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
xlimit_low <- -9
xlimit_up <- 12

# Do log!

# After Pix - Large vs Small
dat_flood <- prepare_data(dta_path,"Estban_detalhado_flood_collapsed.dta",xlimit_low, xlimit_up)
graph_function111("log_total_deposits",dat_flood, xlimit_low, xlimit_up, "Log Deposits")
graph_function111("log_poupanca",dat_flood, xlimit_low, xlimit_up, "Log Savings")
graph_function111("log_dep_prazo",dat_flood, xlimit_low, xlimit_up, "Log Time Deposits")
graph_function111("log_dep_vista_PF",dat_flood, xlimit_low, xlimit_up, "Log Checking Account of People")
graph_function111("log_dep_vista_PJ",dat_flood, xlimit_low, xlimit_up, "Log Checking Account of Firms")
graph_function111("log_caixa",dat_flood, xlimit_low, xlimit_up, "Log Money Inventory")


# Before Pix - Large vs Small
dat_flood <- prepare_data(dta_path,"Estban_detalhado_flood_beforePIX_collapsed.dta",xlimit_low, xlimit_up)
graph_function211("log_total_deposits",dat_flood, xlimit_low, xlimit_up, "Log Deposits")
graph_function211("log_poupanca",dat_flood, xlimit_low, xlimit_up, "Log Savings")
graph_function211("log_dep_prazo",dat_flood, xlimit_low, xlimit_up, "Log Time Deposits")
graph_function211("log_dep_vista_PF",dat_flood, xlimit_low, xlimit_up, "Log Checking Account of People")
graph_function211("log_dep_vista_PJ",dat_flood, xlimit_low, xlimit_up, "Log Checking Account of Firms")
graph_function211("log_caixa",dat_flood, xlimit_low, xlimit_up, "Log Money Inventory")

# if(FALSE) {
# # After Pix - HHI
# dat_flood <- prepare_data(dta_path,"Estban_detalhado_HHI_flood.dta",xlimit_low, xlimit_up)
# graph_function32("hhi_total_deposits",dat_flood, xlimit_low, xlimit_up, "HHI Deposits")
# graph_function32("hhi_poupanca",dat_flood, xlimit_low, xlimit_up, "HHI Savings")
# graph_function32("hhi_dep_prazo",dat_flood, xlimit_low, xlimit_up, "HHI Time Deposits")
# graph_function32("hhi_dep_vista_PF",dat_flood, xlimit_low, xlimit_up, "HHI Checking Account of People")
# graph_function32("hhi_dep_vista_PJ",dat_flood, xlimit_low, xlimit_up, "HHI Checking Account of Firms")
# graph_function32("hhi_caixa",dat_flood, xlimit_low, xlimit_up, "HHI Money Inventory")
# 
# # Before Pix - HHI
# dat_flood <- prepare_data(dta_path,"Estban_detalhado_HHI_flood_beforePIX.dta",xlimit_low, xlimit_up)
# graph_function42("hhi_total_deposits",dat_flood, xlimit_low, xlimit_up, "HHI Deposits")
# graph_function42("hhi_poupanca",dat_flood, xlimit_low, xlimit_up, "HHI Savings")
# graph_function42("hhi_dep_prazo",dat_flood, xlimit_low, xlimit_up, "HHI Time Deposits")
# graph_function42("hhi_dep_vista_PF",dat_flood, xlimit_low, xlimit_up, "HHI Checking Account of People")
# graph_function42("hhi_dep_vista_PJ",dat_flood, xlimit_low, xlimit_up, "HHI Checking Account of Firms")
# graph_function42 ("hhi_caixa",dat_flood, xlimit_low, xlimit_up, "HHI Money Inventory")
# }

