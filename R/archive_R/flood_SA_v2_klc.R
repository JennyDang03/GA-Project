#flood_SA_v2.r

#https://lost-stats.github.io/Model_Estimation/Research_Design/event_study.html

#options(download.file.method = "wininet")

#install.packages(c("data.table","fixest","haven","ggplot2"))

library(data.table) ## For some minor data wrangling
library(fixest)     ## NB: Requires version >=0.9.0
library(haven)
library(ggplot2)

rm(list = ls()) ## Clear workspace

#setwd("//sbcdf176/Pix_Matheus$")

# Set file paths
#log_path <- "//sbcdf176/PIX_Matheus$/Stata/log"
#dta_path <- "//sbcdf176/PIX_Matheus$/Stata/dta"
#output_path <- "//sbcdf176/PIX_Matheus$/Output"
#origdata_path <- "//sbcdf176/PIX_Matheus$/DadosOriginais"
#dta_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta"
#output_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output/tables"
#log_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/log"
dta_path <- "/home/mcs038/Documents/Pix_regressions/Stata/dta"
output_path <- "/home/mcs038/Documents/Pix_regressions/Output/tables"
log_path <- "/home/mcs038/Documents/Pix_regressions/Stata/log"



log_file <- file.path(log_path, "flood_SA_v2_R.log")
sink(log_file) ## redirect R output to log file

# Graph TWFE and SA
################################################################################
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

graph_function4 <- function(y,dat,xlimit_l,xlimit_u, ylimit_l,ylimit_u, main_title){
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
# Flood at the municipality level - Weekly Level
################################################################################

# Load every municipality and every Pix

# Load already prepared data!
dat_flood <- read_dta(file.path(dta_path,"flood_pix_weekly_fake.dta"))
# converts to data.table
setDT(dat_flood)

dat_flood[, treat := ifelse(is.na(date_flood), 0, 1)]
table(dat_flood$treat)
dat_flood[, time_to_treat := ifelse(treat==1, week - date_flood, 0)]
table(dat_flood$time_to_treat)
# Following Sun and Abraham, we give our never-treated units a fake "treatment" date far outside the relevant study period.
dat_flood[, time_id_treated := ifelse(treat==0, 10000, date_flood)]
table(dat_flood$time_id_treated)

#Pix

graph_function4("log_valor_PIX_inflow",dat_flood, -12, 24, -0.04, 0.08, "Log Value Pix Inflow")
graph_function4("log_valor_PIX_outflow",dat_flood, -12, 24, -0.04, 0.08, "Log Value Pix Outflow")
graph_function4("log_valor_PIX_intra",dat_flood, -12, 24, -0.04, 0.08, "Log Value Pix Intra")

graph_function4("log_qtd_PIX_inflow",dat_flood, -12, 24, -0.04, 0.08, "Log Transactions Pix Inflow")
graph_function4("log_qtd_PIX_outflow",dat_flood, -12, 24, -0.04, 0.08, "Log Transactions Pix Outflow")
graph_function4("log_qtd_PIX_intra",dat_flood, -12, 24, -0.04, 0.08, "Log Transactions Pix Intra")

graph_function4("valor_PIX_inflow",dat_flood, -8, 16, -150000, 700000, "Value Pix Inflow")
graph_function4("valor_PIX_outflow",dat_flood, -8, 16, -150000, 700000, "Value Pix Outflow")
graph_function4("valor_PIX_intra",dat_flood, -8, 16, -150000, 700000, "Value Pix Intra")

graph_function4("qtd_PIX_inflow",dat_flood, -8, 16, -400, 700, "Transactions Pix Inflow")
graph_function4("qtd_PIX_outflow",dat_flood, -8, 16, -400, 700, "Transactions Pix Outflow")
graph_function4("qtd_PIX_intra",dat_flood, -8, 16, -400, 700, "Transactions Pix Intra")

# n_cli_pag_pf_intra n_cli_rec_pf_intra n_cli_pag_pj_intra n_cli_rec_pj_intra

graph_function4("n_cli_pag_pf_intra",dat_flood, -12, 24, -50, 60, "Quantity of People Sending Pix")
graph_function4("n_cli_rec_pf_intra",dat_flood, -12, 24, -50, 60, "Quantity of People Receiving Pix")

graph_function4("n_cli_pag_pj_intra",dat_flood, -12, 24, -3, 8, "Quantity of Firms Sending Pix")
graph_function4("n_cli_rec_pj_intra",dat_flood, -12, 24, -3, 8, "Quantity of Firms Receiving Pix")

graph_function4("log_n_cli_pag_pf_intra",dat_flood, -12, 24, -0.05, 0.08, "Log Quantity of People Sending Pix")
graph_function4("log_n_cli_rec_pf_intra",dat_flood, -12, 24, -0.05, 0.08, "Log Quantity of People Receiving Pix") #

graph_function4("log_n_cli_pag_pj_intra",dat_flood, -12, 24, -0.10, 0.20, "Log Quantity of Firms Sending Pix")
graph_function4("log_n_cli_rec_pj_intra",dat_flood, -12, 24, -0.10, 0.20, "Log Quantity of Firms Receiving Pix")

#TED
# * valor_TED_intra qtd_TED_intra qtd_cli_TED_rec_PJ qtd_cli_TED_rec_PF qtd_cli_TED_pag_PJ qtd_cli_TED_pag_PF 

graph_function4("valor_TED_intra",dat_flood, -12, 24, -100000, 220000, "Value TED Intra") # 
graph_function4("qtd_TED_intra",dat_flood, -12, 24, -4, 3, "Transactions TED Intra") #

graph_function4("log_valor_TED_intra",dat_flood, -12, 24, -0.40, 0.18, "Log Value TED Intra") #
graph_function4("log_qtd_TED_intra",dat_flood, -12, 24, -0.20, 0.15, "Log Transactions TED Intra") #
 
graph_function4("qtd_cli_TED_rec_PJ",dat_flood, -12, 24, -0.15, 0.6, "Quantity of Firms Receiving TED") #
graph_function4("qtd_cli_TED_pag_PJ",dat_flood, -12, 24, -0.15, 0.6, "Quantity of Firms Sending TED") # 

graph_function4("log_qtd_cli_TED_rec_PJ",dat_flood, -12, 24, -0.04, 0.1, "Log Quantity of Firms Receiving TED")
graph_function4("log_qtd_cli_TED_pag_PJ",dat_flood, -12, 24, -0.04, 0.1, "Log Quantity of Firms Sending TED")

#graph_function4("qtd_cli_TED_rec_PF",dat_flood, -12, 24, "Quantity of People Receiving TED")
#graph_function4("qtd_cli_TED_pag_PF",dat_flood, -12, 24, "Quantity of People Sending TED")
#graph_function4("log_qtd_cli_TED_rec_PF",dat_flood, -12, 24, -0.05, 0.1, "Log Quantity of People Receiving TED") #
#graph_function4("log_qtd_cli_TED_pag_PF",dat_flood, -12, 24, -0.05, 0.1, "Log Quantity of People Sending TED") #


#Boleto
# * valor_boleto qtd_boleto qtd_cli_pag_pf_boleto qtd_cli_pag_pj_boleto qtd_cli_rec_pj_boleto 
graph_function4("valor_boleto",dat_flood, -12, 24, -6500000, 6500000, "Value Boleto")
graph_function4("qtd_boleto",dat_flood, -12, 24, -200, 300, "Transactions Boleto")

graph_function4("log_valor_boleto",dat_flood, -12, 24, -0.05, 0.15, "Log Value Boleto")
graph_function4("log_qtd_boleto",dat_flood, -12, 24, -0.03, 0.06, "Log Transactions Boleto")


#Cartao 
# * valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito 
graph_function4("valor_cartao_credito",dat_flood, -12, 24, -40000, 50000, "Value Credit Card")
graph_function4("valor_cartao_debito",dat_flood, -12, 24, -50000, 100000, "Value Debit Card")

graph_function4("log_valor_cartao_debito",dat_flood, -12, 24, -0.04, 0.08, "Log Value Debit Card")
graph_function4("log_valor_cartao_credito",dat_flood, -12, 24, -0.04, 0.08, "Log Value Credit Card")

graph_function4("qtd_cli_cartao_credito",dat_flood, -12, 24, -2, 2.5, "Quantity of People using Credit Card")
graph_function4("qtd_cli_cartao_debito",dat_flood, -12, 24, -4, 6, "Quantity of People using Debit Card")

graph_function4("log_qtd_cli_cartao_debito",dat_flood, -12, 24, -0.03, 0.04, "Log Quantity of People using Debit Card")
graph_function4("log_qtd_cli_cartao_credito",dat_flood, -12, 24, -0.03, 0.04, "Log Quantity of People using Credit Card")

sink()