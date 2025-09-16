################################################################################

# flood_SA_muni_self_v2.R
# Input:  PIX_week_muni_self_flood_sample10.dta
#         
# Output: "self_muni_pix_flood_",y,".png"
#         
# y:  valor_self_pj qtd_self_pj n_cli_self_pj log_valor_self_pj log_qtd_self_pj log_n_cli_self_pj
#     valor_self_pf qtd_self_pf n_cli_self_pf log_valor_self_pf log_qtd_self_pf log_n_cli_self_pf 

# The goal: see the effect of flood on bank transactions to themselves before and after Pix.

# To do: 
#         Also, there is only Pix transactions here. Ideally, it would have TED and boleto so we can do before Pix analysis. 
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
#output_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output/tables"
#log_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/log"

log_file <- file.path(log_path, "flood_SA_muni_self_v1.log")
sink(log_file) ## redirect R output to log file

graph_function3 <- function(y,dat){
  dat$Y <- dat[[y]]
  
  mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                     muni_cd + week,            ## FEs
                   cluster = ~muni_cd,                          ## Clustered SEs
                   data = dat)
  # See `?sunab`.
  mod_sa = feols(Y ~ sunab(time_id_treated, week) | ## The only thing that's changed
                   muni_cd + week,            ## FEs
                 cluster = ~muni_cd,
                 data = dat)
  return(list(mod_twfe, mod_sa))
}

graph_function4 <- function(y,dat,xlimit_l,xlimit_u, main_title){
  png(file.path(output_path,paste0("self_muni_pix_flood_",y,".png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function3(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Week',
        main = main_title,
        ci_level = 0.90, xlim = c(xlimit_l,xlimit_u))

  legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
         legend = c("TWFE", "Sun & Abraham (2020)"), cex = 0.8)
  dev.off()
}

graph_function5 <- function(y,dat,xlimit_l,xlimit_u, main_title){
  png(file.path(output_path,paste0("self_before_pix_flood_",y,".png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function3(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Week',
        main = main_title,
        ci_level = 0.90, xlim = c(xlimit_l,xlimit_u))

  legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
         legend = c("TWFE", "Sun & Abraham (2020)"), cex = 0.8)
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
dat_flood <- read_dta(file.path(dta_path,"PIX_week_muni_self_flood.dta"))
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
xlimit_low <- -52
xlimit_up <- 52
xlimits <- seq(ceiling(xlimit_low*1.1),ceiling(xlimit_up*1.1),by=1)
dat_flood <- subset(dat_flood,time_to_treat %in% xlimits)
#

#Pix

# PJ
# valor_self_pj qtd_self_pj n_cli_self_pj log_valor_self_pj log_qtd_self_pj log_n_cli_self_pj

#graph_function4("valor_self_pj",dat_flood, -12, 24, -2500000, 4000000, "Value")
#graph_function4("qtd_self_pj",dat_flood, -12, 24, -175, 275, "Transactions")
#graph_function4("n_cli_self_pj",dat_flood, -12, 24, -10, 15, "Quantity of Firms")
graph_function4("log_valor_self_pj",dat_flood, xlimit_low, xlimit_up, "Log Value")
graph_function4("log_qtd_self_pj",dat_flood, xlimit_low, xlimit_up, "Log Transactions")
graph_function4("log_n_cli_self_pj",dat_flood, xlimit_low, xlimit_up, "Log Quantity of Firms")

# PF 
# valor_self_pf qtd_self_pf n_cli_self_pf log_valor_self_pf log_qtd_self_pf log_n_cli_self_pf 

#graph_function4("valor_self_pf",dat_flood, -12, 24, -1100000, 5100000, "Value")
#graph_function4("qtd_self_pf",dat_flood, -12, 24, -4000, 8000, "Transactions")
#graph_function4("n_cli_self_pf",dat_flood, -12, 24, -250, 750, "Quantity of People")
graph_function4("log_valor_self_pf",dat_flood, xlimit_low, xlimit_up, "Log Value")
graph_function4("log_qtd_self_pf",dat_flood, xlimit_low, xlimit_up, "Log Transactions")
graph_function4("log_n_cli_self_pf",dat_flood, xlimit_low, xlimit_up, "Log Quantity of People")


################################
# Before Pix
################################




sink()