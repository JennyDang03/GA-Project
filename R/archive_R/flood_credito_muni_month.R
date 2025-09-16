#flood_credito_muni_month_v1

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

log_file <- file.path(log_path, "flood_credito_muni_month_v1.log")
sink(log_file) ## redirect R output to log file

# Graph TWFE and SA
################################################################################
graph_function <- function(y,dat){
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

graph_function2 <- function(y,dat,xlimit_l, xlimit_u, main_title){
  png(file.path(output_path,paste0("credito_flood_",y,".png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Month',
        main = main_title,
        ci_level = 0.90, xlim = c(xlimit_l,xlimit_u))

  legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
         legend = c("TWFE", "Sun & Abraham (2020)"), cex = 0.8)
  dev.off()
}
graph_function3 <- function(y,dat,xlimit_l, xlimit_u, main_title){
  png(file.path(output_path,paste0("before_pix_credito_flood_",y,".png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Month',
        main = main_title,
        ci_level = 0.90, xlim = c(xlimit_l,xlimit_u))

  legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
         legend = c("TWFE", "Sun & Abraham (2020)"), cex = 0.8)
  dev.off()
}


################################################################################

################################################################################
# Flood at the Municipal level - Monthly Level
################################################################################

#####################
# After Pix
#####################

# Load already prepared data!
dat_flood <- read_dta(file.path(dta_path,"Base_credito_muni_flood.dta"))

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
xlimit_low <- -6
xlimit_up <- 6
xlimits <- seq(ceiling(xlimit_low*1.333-2),ceiling(xlimit_up*1.333+2),by=1)
dat_flood <- subset(dat_flood,time_to_treat %in% xlimits)
#


# qtd_op qtd_cli_total_PF vol_credito_total_PF vol_consignado qtd_cli_consignado vol_emprestimo_pessoal qtd_cli_emp_pessoal vol_veiculo qtd_cli_veiculo vol_imob qtd_cli_imob vol_cartao qtd_cli_cartao vol_rural qtd_cli_rural vol_outros_cred qtd_cli_outros_cred date_ym qtd_cli_total_PJ vol_credito_total_PJ volume_wc qtd_cli_wc qtd_op_wc volume_invest qtd_cli_invest qtd_op_invest volume_desc_ovdraft qtd_cli_desc_ovdraft qtd_op_desc_ovdraft volume_desc_reb qtd_cli_desc_reb qtd_op_desc_reb volume_comex qtd_cli_comex qtd_op_comex volume_outros qtd_cli_outros qtd_op_outros qtd_cli_total vol_credito_total
# Principais: qtd_cli_total  qtd_cli_total_PF qtd_cli_total_PJ
# vol_credito_total vol_credito_total_PF vol_credito_total_PJ
# vol_emprestimo_pessoal qtd_cli_emp_pessoal
# vol_cartao qtd_cli_cartao

graph_function2("log_vol_cartao",dat_flood, xlimit_low, xlimit_up, "Log Volume Credit Card")
graph_function2("log_qtd_cli_cartao",dat_flood, xlimit_low, xlimit_up, "Log Quantity Credit Card")
graph_function2("log_vol_emprestimo_pessoal",dat_flood, xlimit_low, xlimit_up, "Log Volume Personal Loan")
graph_function2("log_qtd_cli_emp_pessoal",dat_flood, xlimit_low, xlimit_up, "Log Quantity Personal Loan")
graph_function2("log_vol_credito_total",dat_flood, xlimit_low, xlimit_up, "Log Volume Loan")
graph_function2("log_qtd_cli_total",dat_flood, xlimit_low, xlimit_up, "Log Quantity Loan")
graph_function2("log_vol_credito_total_PF",dat_flood, xlimit_low, xlimit_up, "Log Volume Loan")
graph_function2("log_qtd_cli_total_PF",dat_flood, xlimit_low, xlimit_up, "Log Quantity Loan")
graph_function2("log_vol_credito_total_PJ",dat_flood, xlimit_low, xlimit_up, "Log Volume Loan")
graph_function2("log_qtd_cli_total_PJ",dat_flood, xlimit_low, xlimit_up, "Log Quantity Loan")

#####################
# Before Pix
#####################


# Load already prepared data!
dat_flood <- read_dta(file.path(dta_path,"Base_credito_muni_flood_beforePIX.dta"))

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
xlimit_low <- -12
xlimit_up <- 12
xlimits <- seq(ceiling(xlimit_low*1.1-2),ceiling(xlimit_up*1.1+2),by=1)
dat_flood <- subset(dat_flood,time_to_treat %in% xlimits)
#


graph_function3("log_vol_cartao",dat_flood, xlimit_low, xlimit_up, "Log Volume Credit Card")
graph_function3("log_qtd_cli_cartao",dat_flood, xlimit_low, xlimit_up, "Log Quantity Credit Card")
graph_function3("log_vol_emprestimo_pessoal",dat_flood, xlimit_low, xlimit_up, "Log Volume Personal Loan")
graph_function3("log_qtd_cli_emp_pessoal",dat_flood, xlimit_low, xlimit_up, "Log Quantity Personal Loan")
graph_function3("log_vol_credito_total",dat_flood, xlimit_low, xlimit_up, "Log Volume Loan")
graph_function3("log_qtd_cli_total",dat_flood, xlimit_low, xlimit_up, "Log Quantity Loan")
graph_function3("log_vol_credito_total_PF",dat_flood, xlimit_low, xlimit_up, "Log Volume Loan")
graph_function3("log_qtd_cli_total_PF",dat_flood, xlimit_low, xlimit_up, "Log Quantity Loan")
graph_function3("log_vol_credito_total_PJ",dat_flood, xlimit_low, xlimit_up, "Log Volume Loan")
graph_function3("log_qtd_cli_total_PJ",dat_flood, xlimit_low, xlimit_up, "Log Quantity Loan")

sink()