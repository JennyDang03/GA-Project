#flood_SA_v2.r

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

log_file <- file.path(log_path, "flood_SA_v2_R.log")
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

graph_function2 <- function(y,dat,xlimit_l,xlimit_u,ylimit_l,ylimit_u, main_title){
  png(file.path(output_path,paste("pix_flood_",y,".png")), width = 640*4, height = 480*4, res = 200)
    iplot(graph_function(y,dat), sep = 0.5, ref.line = -1,
          xlab = 'Month',
          main = paste("Event study: Floods on ", main_title),
          ci_level = 0.90, xlim = c(xlimit_l,xlimit_u), ylim = c(ylimit_l,ylimit_u))
    # y_range_function(-6,6,mod_twfe,mod_sa)
    legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
           legend = c("TWFE", "Sun & Abraham (2020)"), cex = 0.8)
  dev.off()
}


graph_function3 <- function(y,dat){
  dat$Y <- dat[[y]]
  
  mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time × treatment status
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

graph_function4 <- function(y,dat,xlimit_l,xlimit_u,ylimit_l,ylimit_u, main_title){
  png(file.path(output_path,paste("muni_pix_flood_",y,".png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function3(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Week',
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
dat_flood <- read_dta(file.path(dta_path,"Pix_individuo_cleaned1_sample10.dta"))
# converts to data.table
setDT(dat_flood)

dat_flood[, treat := ifelse(is.na(date_flood), 0, 1)]
table(dat_flood$treat)
dat_flood[, time_to_treat := ifelse(treat==1, time_id - date_flood, 0)]
table(dat_flood$time_to_treat)
# Following Sun and Abraham, we give our never-treated units a fake "treatment" date far outside the relevant study period.
dat_flood[, time_id_treated := ifelse(treat==0, 1000000, date_flood)]
table(dat_flood$time_id_treated)


graph_function2("after_first_pix_rec",dat_flood, -6, 6, -0.025, 0.025, "Receiver Adoption")
graph_function2("after_first_pix_sent",dat_flood, -6, 6, -0.025, 0.025, "Sender Adoption")

graph_function2("receiver",dat_flood, -6, 6, -0.025, 0.025, "Receiver")
graph_function2("sender",dat_flood, -6, 6, -0.025, 0.025, "Senders")
graph_function2("user",dat_flood, -6, 6, -0.025, 0.025, "Users")

graph_function2("trans_rec",dat_flood, -6, 6, -1.5, 1.5, "Transactions Received")
graph_function2("trans_sent",dat_flood, -6, 6, -1.5, 1.5, "Transactions Sent")
graph_function2("value_rec",dat_flood, -6, 6, -100, 750, "Value Received")
graph_function2("value_sent",dat_flood, -6, 6, -100, 750, "Value Sent")

graph_function2("log_trans_rec",dat_flood, -6, 6, -0.1, 1, "Log Transactions Received")
graph_function2("log_trans_sent",dat_flood, -6, 6, -0.1, 1, "Log Transactions Sent")
graph_function2("log_value_rec",dat_flood, -6, 6, -0.1, 1, "Log Value Received")
graph_function2("log_value_sent",dat_flood, -6, 6, -0.1, 1, "Log Value Sent")



################################################################################
# Flood at the municipality level - Weekly Level
################################################################################

# Load every municipality and every Pix

# Load already prepared data!
dat_flood <- read_dta(file.path(dta_path,"flood_pix_monthly_fake.dta"))
# converts to data.table
setDT(dat_flood)

dat_flood[, treat := ifelse(is.na(date_flood), 0, 1)]
table(dat_flood$treat)
dat_flood[, time_to_treat := ifelse(treat==1, week - date_flood, 0)]
table(dat_flood$time_to_treat)
# Following Sun and Abraham, we give our never-treated units a fake "treatment" date far outside the relevant study period.
dat_flood[, time_id_treated := ifelse(treat==0, 100000000, date_flood)]
table(dat_flood$time_id_treated)

#Pix
#graph_function4("valor_PIX_inflow",dat_flood, -8, 16, -150000, 500000, "Value Pix Inflow")
#graph_function4("qtd_PIX_inflow",dat_flood, -8, 16, 500, 1000, "Transactions Pix Inflow")
#graph_function4("valor_PIX_outflow",dat_flood, -8, 16, -150000, 500000, "Value Pix Outflow")
#graph_function4("qtd_PIX_outflow",dat_flood, -8, 16, 500, 1000, "Transactions Pix Outflow")
#graph_function4("valor_PIX_intra",dat_flood, -8, 16, -150000, 500000, "Value Pix Intra")
#graph_function4("qtd_PIX_intra",dat_flood, -8, 16, 500, 1000, "Transactions Pix Intra")

# n_cli_pag_pf_intra n_cli_rec_pf_intra n_cli_pag_pj_intra n_cli_rec_pj_intra



#TED
# * valor_TED_intra qtd_TED_intra qtd_cli_TED_rec_PJ qtd_cli_TED_rec_PF qtd_cli_TED_pag_PJ qtd_cli_TED_pag_PF 



#Boleto
# * valor_boleto qtd_boleto qtd_cli_pag_pf_boleto qtd_cli_pag_pj_boleto qtd_cli_rec_pj_boleto 

#Cartao 
# * valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito 


sink()