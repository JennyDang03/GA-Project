################################################################################

# flood_SA_individual_v1_PJ.R
# Input:  "Pix_individuo_cleaned2_sample1.dta"
#         
# Output: "pix_flood_",y,"_PJ.png"
#         
# y: after_first_pix_rec, after_first_pix_sent
#     receiver, sender, user
#     trans_rec, trans_sent, value_rec, value_sent
#     log_trans_rec, log_trans_sent, log_value_rec, log_value_sent 


# The goal:  The goal is to see the effect of flood on Firms use of Pix.

# To do: This is very badly made. We only downloaded the 5000 smallest municipalities. 
#         Then, we need to take a 1% sample. 
#         Also, there are only Pix users in this dataset. 
#         Jose tried to solve this by downloading 1% sample of CPFs in the country. 

#       ASK JOSE HOW HE DID THIS, HE ALTERED THE WAY WE DO THIS.

source(file.path(R_path, "flood_SA_individual_v1_PJ.R"))
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

log_file <- file.path(log_path, "flood_SA_individual_v1_PJ.log")
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
  png(file.path(output_path,paste("pix_flood_",y,"_PJ.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Month',
        main = paste("Event study: Floods on ", main_title),
        ci_level = 0.90, xlim = c(xlimit_l,xlimit_u), ylim = c(ylimit_l,ylimit_u))
  # y_range_function(-6,6,mod_twfe,mod_sa)
  legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
         legend = c("TWFE", "Sun & Abraham (2020)"), cex = 0.8)
  dev.off()
  
}


################################################################################



################################################################################
# Flood at the individual level - Monthly Level
################################################################################
 

# Load every person and every Pix
# Monthly data on number of transactions and volume transacted per person only 
# limited amount of information on who is sending and who is receiving. 
# maybe get sample and download a more detailed data (transactions to outside the municipality for example)

# Load already prepared data!
dat_flood <- read_dta(file.path(dta_path,"Pix_individuo_cleaned2_sample1.dta"))

# converts to data.table
setDT(dat_flood)

dat_flood[, treat := ifelse(is.na(date_flood), 0, 1)]
table(dat_flood$treat)
dat_flood[, time_to_treat := ifelse(treat==1, time_id - date_flood, 0)]
table(dat_flood$time_to_treat)
# Following Sun and Abraham, we give our never-treated units a fake "treatment" date far outside the relevant study period.
dat_flood[, time_id_treated := ifelse(treat==0, 1000000, date_flood)]
table(dat_flood$time_id_treated)


graph_function2("after_first_pix_rec",dat_flood, -6, 6, -0.012, 0.027, "Receiver Adoption")
dat_flood[, after_first_pix_rec := NULL]
graph_function2("after_first_pix_sent",dat_flood, -6, 6, -0.012, 0.027, "Sender Adoption")
dat_flood[, after_first_pix_sent := NULL]

graph_function2("receiver",dat_flood, -6, 6, -0.02, 0.033, "Receiver")
dat_flood[, receiver := NULL]
graph_function2("sender",dat_flood, -6, 6, -0.02, 0.033, "Senders")
dat_flood[, sender := NULL]
graph_function2("user",dat_flood, -6, 6, -0.02, 0.033, "Users")
dat_flood[, user := NULL]

graph_function2("trans_rec",dat_flood, -6, 6, -2.5, 2.3, "Transactions Received")
dat_flood[, log_trans_rec := log(trans_rec + 1)]
dat_flood[, trans_rec := NULL]
graph_function2("trans_sent",dat_flood, -6, 6, -2.5, 2.3, "Transactions Sent")
dat_flood[, log_trans_sent := log(trans_sent + 1)]
dat_flood[, trans_sent := NULL]
graph_function2("value_rec",dat_flood, -6, 6, -8000, 10000, "Value Received")
dat_flood[, log_value_rec := log(value_rec + 1)]
dat_flood[, value_rec := NULL]
graph_function2("value_sent",dat_flood, -6, 6, -8000, 10000, "Value Sent")
dat_flood[, log_value_sent := log(value_sent + 1)]
dat_flood[, value_sent := NULL]

graph_function2("log_trans_rec",dat_flood, -6, 6, -0.023, 0.070, "Log Transactions Received")
dat_flood[, log_trans_rec := NULL]
graph_function2("log_trans_sent",dat_flood, -6, 6, -0.023, 0.070, "Log Transactions Sent")
dat_flood[, log_trans_sent := NULL]
graph_function2("log_value_rec",dat_flood, -6, 6, -0.15, 0.25, "Log Value Received")
dat_flood[, log_value_rec := NULL]
graph_function2("log_value_sent",dat_flood, -6, 6, -0.15, 0.25, "Log Value Sent")
dat_flood[, log_value_sent := NULL]

sink()