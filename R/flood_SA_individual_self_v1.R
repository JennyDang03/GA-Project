################################################################################

# flood_SA_individual_self_v1.R
# Input:  Pix_individuo_PJ_self_cleaned_sample1.dta
#         Pix_individuo_PF_self_cleaned.dta
# Output: "self_pix_flood_",y,".png"
#         "self_pix_flood_",y,"_PJ.png"
# y: after_first_pix_self, user, trans_self, value_self, log_trans_self, log_value_self


# The goal:  To see the effect of flood on flow between accounts of themselves. 

# To do: Missing ted and boleto, before and after pix for them. 
#         Should I do cross sectional regressions? Like, being hit with flood (treat == 1)
#         would impact the likelyhood of me being a user at exactly time t (t== -1,-2,..., 0,1,2,...)

source(file.path(R_path, "flood_SA_individual_self_v1.R"))
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
#dta_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta"
#output_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output/tables"
#log_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/log"

log_file <- file.path(log_path, "flood_SA_individual_self_v1.log")
sink(log_file) ## redirect R output to log file

# Graph TWFE and SA
################################################################################
graph_function <- function(y,dat){
  dat$Y <- dat[[y]]
  
  mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time × treatment status
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

graph_function2 <- function(y,dat,xlimit_l, xlimit_u, ylimit_l,ylimit_u, main_title){
  png(file.path(output_path,paste("self_pix_flood_",y,".png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Month',
        main = paste("Event study: Floods on", main_title),
        ci_level = 0.90, xlim = c(xlimit_l,xlimit_u), ylim = c(ylimit_l,ylimit_u))
  # y_range_function(-6,6,mod_twfe,mod_sa)
  legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
         legend = c("TWFE", "Sun & Abraham (2020)"), cex = 0.8)
  dev.off()
}

graph_function3 <- function(y,dat,xlimit_l, xlimit_u, ylimit_l,ylimit_u, main_title){
  png(file.path(output_path,paste("self_pix_flood_",y,"_PJ.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Month',
        main = paste("Event study: Floods on", main_title),
        ci_level = 0.90, xlim = c(xlimit_l,xlimit_u), ylim = c(ylimit_l,ylimit_u))
  # y_range_function(-6,6,mod_twfe,mod_sa)
  legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
         legend = c("TWFE", "Sun & Abraham (2020)"), cex = 0.8)
  dev.off()
}

################################################################################



################################################################################
# Flood at the individual level - Monthly Level - SELF
################################################################################



##########
# PJ
##########

# Load every person and every Pix
# Monthly data on number of transactions and volume transacted per person only 
# limited amount of information on who is sending and who is receiving. 
# maybe get sample and download a more detailed data (transactions to outside the municipality for example)

# Load already prepared data!
dat_flood <- read_dta(file.path(dta_path,"Pix_individuo_PJ_self_cleaned_sample1.dta"))

# converts to data.table
setDT(dat_flood)

dat_flood[, treat := ifelse(is.na(date_flood), 0, 1)]
table(dat_flood$treat)
dat_flood[, time_to_treat := ifelse(treat==1, time_id - date_flood, 0)]
table(dat_flood$time_to_treat)
# Following Sun and Abraham, we give our never-treated units a fake "treatment" date far outside the relevant study period.
dat_flood[, time_id_treated := ifelse(treat==0, 1000000, date_flood)]
table(dat_flood$time_id_treated)

graph_function3("after_first_pix_self",dat_flood, -6, 6, -0.12, 0.18, "Adoption")

graph_function3("user",dat_flood, -6, 6, -0.10, 0.2, "Users")

graph_function3("trans_self",dat_flood, -6, 6, -2, 6, "Transactions")
graph_function3("value_self",dat_flood, -6, 6, -3000, 3000, "Value")

dat_flood[, log_trans_self := log(trans_self + 1)]
dat_flood[, log_value_self := log(value_self + 1)]

graph_function3("log_trans_self",dat_flood, -6, 6, -0.30, 0.50, "Log Transactions")
graph_function3("log_value_self",dat_flood, -6, 6, -0.6, 1.6, "Log Value")


##########
# PF
##########

# Load every person and every Pix
# Monthly data on number of transactions and volume transacted per person only 
# limited amount of information on who is sending and who is receiving. 
# maybe get sample and download a more detailed data (transactions to outside the municipality for example)

# Load already prepared data!
dat_flood <- read_dta(file.path(dta_path,"Pix_individuo_PF_self_cleaned.dta"))

# converts to data.table
setDT(dat_flood)

dat_flood[, treat := ifelse(is.na(date_flood), 0, 1)]
table(dat_flood$treat)
dat_flood[, time_to_treat := ifelse(treat==1, time_id - date_flood, 0)]
table(dat_flood$time_to_treat)
# Following Sun and Abraham, we give our never-treated units a fake "treatment" date far outside the relevant study period.
dat_flood[, time_id_treated := ifelse(treat==0, 1000000, date_flood)]
table(dat_flood$time_id_treated)

graph_function2("after_first_pix_self",dat_flood, -6, 6, -0.006, 0.009, "Adoption")

graph_function2("user",dat_flood, -6, 6, -0.005, 0.01, "Users")

graph_function2("trans_self",dat_flood, -6, 6, -0.1, 0.3, "Transactions")
graph_function2("value_self",dat_flood, -6, 6, -150, 150, "Value")

dat_flood[, log_trans_self := log(trans_self + 1)]
dat_flood[, log_value_self := log(value_self + 1)]

graph_function2("log_trans_self",dat_flood, -6, 6, -0.015, 0.025, "Log Transactions")
graph_function2("log_value_self",dat_flood, -6, 6, -0.03, 0.08, "Log Value")


sink()