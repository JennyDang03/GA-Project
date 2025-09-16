################################################################################
# iv_v2.R
# Input: 
#         
# Output: 
#         
# y: 

# The goal: 

# To do:  

################################################################################

options(download.file.method = "wininet")

install.packages(c("data.table","fixest","haven","ggplot2","xtable","stargazer","lfe","AER"))

library(xtable)
library(stargazer)
library(data.table) 
library(fixest)     
library(haven)
library(ggplot2)
library(lfe)
library(AER)

rm(list = ls()) ## Clear workspace

system_info <- Sys.info()
if (system_info["user"] == "mathe") {
  base <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study"
  print("Working in Matheus' computer")
} else {
  data_folder <- "//sbcdf176/Pix_Matheus$"
  base <- "//sbcdf176/Pix_Matheus$"
  setwd("//sbcdf176/Pix_Matheus$")
  print("Working in Central Bank' computer")
}

# Set file paths
log_path <- file.path(base,"Stata/log")
dta_path <- file.path(base,"Stata/dta")
output_path <- file.path(base,"Output")
origdata_path <- file.path(base,"DadosOriginais")
R_path <- file.path(base,"R")

log_file <- file.path(log_path, "iv_v2.log")
sink(log_file) ## redirect R output to log file

################################################################################
# 0. Make IV function
################################################################################
iv_function_month <- function(y,y_label,x,x_label,instrument,dat){
  dat$Y <- dat[[y]]
  dat$X <- dat[[x]]
  dat$Ins <- dat[[instrument]]
  
  ivmodel <- felm(Y ~ 0 | 0 | (X ~ Ins), data = dat)
  ivmodel_munFE <- felm(Y ~ 0 | muni_cd | (X ~ Ins), data = dat)
  ivmodel_timeFE <- felm(Y ~ 0 | time_id | (X ~ Ins), data = dat)
  ivmodel_FE <- felm(Y ~ 0 | muni_cd + time_id | (X ~ Ins), data = dat)
  
  # Combine models into a list
  models_list <- list(ivmodel, ivmodel_munFE, ivmodel_timeFE, ivmodel_FE)
  
  # Create LaTeX table using stargazer
  latex_table <- stargazer(models_list, title = "IV Models",
                           float = FALSE,
                           align = TRUE, 
                           dep.var.labels=c(y_label),
                           covariate.labels=c(x_label),
                           omit.stat=c("LL","ser","f"), 
                           no.space=TRUE,
                           single.row = FALSE,
                           header = FALSE, font.size = "small",
                           add.lines = list(c("Mun. FE", "No", "Yes", "No", "Yes"),
                                            c("Time FE", "No", "No", "Yes", "Yes")))
  
  latex_file <- file.path(output_path, paste0("iv_",y,"_",x,".tex"))
  cat(latex_table, file = latex_file)
  return(latex_table)
}

iv_function_week <- function(y,y_label,x,x_label,instrument,dat){
  dat$Y <- dat[[y]]
  dat$X <- dat[[x]]
  dat$Ins <- dat[[instrument]]
  
  ivmodel <- felm(Y ~ 0 | 0 | (X ~ Ins), data = dat)
  ivmodel_munFE <- felm(Y ~ 0 | muni_cd | (X ~ Ins), data = dat)
  ivmodel_timeFE <- felm(Y ~ 0 | week | (X ~ Ins), data = dat)
  ivmodel_FE <- felm(Y ~ 0 | muni_cd + week | (X ~ Ins), data = dat)
  
  # Combine models into a list
  models_list <- list(ivmodel, ivmodel_munFE, ivmodel_timeFE, ivmodel_FE)
  
  # Create LaTeX table using stargazer
  latex_table <- stargazer(models_list, title = "IV Models",
                           float = FALSE,
                           align = TRUE, 
                           dep.var.labels=c(y_label),
                           covariate.labels=c(x_label),
                           omit.stat=c("LL","ser","f"), 
                           no.space=TRUE,
                           single.row = FALSE,
                           header = FALSE, font.size = "small",
                           add.lines = list(c("Mun. FE", "No", "Yes", "No", "Yes"),
                                            c("Time FE", "No", "No", "Yes", "Yes")))
  
  latex_file <- file.path(output_path, paste0("iv_",y,"_",x,".tex"))
  cat(latex_table, file = latex_file)
  return(latex_table)
}
################################################################################
# 0. Load Bases
################################################################################

# Base Month

dat_flood_month <- read_dta(file.path(dta_path,"Base_month_muni_flood.dta"))
#dat_flood_month <- read_dta(file.path(dta_path,"flood_pix_monthly_fake.dta"))
setDT(dat_flood_month)

dat_flood_month[, treat := ifelse(is.na(date_flood), 0, 1)]
dat_flood_month[, time_to_treat := ifelse(treat==1, time_id - date_flood, 0)]
# Following Sun and Abraham, we give our never-treated units a fake "treatment" date far outside the relevant study period.
dat_flood_month[, time_id_treated := ifelse(treat==0, 10000, dat_flood_month)]

# Create Variables

# create log_n_cli_pag_pf 
dat_flood_month[, log_n_cli_pag_pf := log((n_cli_pag_pf_outflow + n_cli_pag_pf_intra) + 1)]
dat_flood_month[, log_n_cli_rec_pf := log((n_cli_rec_pf_inflow + n_cli_rec_pf_intra) + 1)]
dat_flood_month[, log_n_cli_pag_pj := log((n_cli_pag_pj_outflow + n_cli_pag_pj_intra) + 1)]
dat_flood_month[, log_n_cli_rec_pj := log((n_cli_rec_pj_inflow + n_cli_rec_pj_intra) + 1)]

# create after_flood
dat_flood_month[, after_flood := ifelse(is.na(date_flood), 0, ifelse(time_id >= date_flood, 1, 0))]

################################
dat_flood_month[, time_to_treat2 := ifelse(treat == 1, time_id - date_flood, NA)]
# Create the histogram
plot <- ggplot(dat_flood_month, aes(x = time_to_treat2)) +
  geom_histogram(binwidth = 1, fill = "blue", color = "black") +
  geom_histogram(data = dat_flood_month[time_to_treat2 == 0, ], binwidth = 1, fill = "red", color = "black", alpha = 0.5) + # Highlight the bin with 0
  labs(x = "Months Since the Flood", y = "Frequency", title = "Histogram") +
  theme_minimal()

# Display the plot
print(plot)

# Save the plot as a PNG file (adjust the filename as needed)
ggsave(file.path(output_path,"histogram_time_to_treat_month.png"), plot, width = 6, height = 4, units = "in")

# Create also a historgram with the collapsed number of floods by mun (I create in stata but lost the code)


################################

# Base Week

dat_flood_week <- read_dta(file.path(dta_path,"Base_week_muni_flood.dta"))
#dat_flood_week <- read_dta(file.path(dta_path,"flood_pix_weekly_fake.dta"))
setDT(dat_flood_week)

dat_flood_week[, treat := ifelse(is.na(date_flood), 0, 1)]
dat_flood_week[, time_to_treat := ifelse(treat==1, week - date_flood, 0)]
# Following Sun and Abraham, we give our never-treated units a fake "treatment" date far outside the relevant study period.
dat_flood_week[, time_id_treated := ifelse(treat==0, 10000, date_flood)]

# Create Variables

# create log_n_cli_pag_pf 
dat_flood_week[, log_n_cli_pag_pf := log((n_cli_pag_pf_outflow + n_cli_pag_pf_intra) + 1)]
dat_flood_week[, log_n_cli_rec_pf := log((n_cli_rec_pf_inflow + n_cli_rec_pf_intra) + 1)]
dat_flood_week[, log_n_cli_pag_pj := log((n_cli_pag_pj_outflow + n_cli_pag_pj_intra) + 1)]
dat_flood_week[, log_n_cli_rec_pj := log((n_cli_rec_pj_inflow + n_cli_rec_pj_intra) + 1)]

# create after_flood
dat_flood_week[, after_flood := ifelse(is.na(date_flood), 0, ifelse(week >= date_flood, 1, 0))]

################################
dat_flood_week[, time_to_treat2 := ifelse(treat == 1, week - date_flood, NA)]
# Create the histogram
plot <- ggplot(dat_flood_week, aes(x = time_to_treat2)) +
  geom_histogram(binwidth = 4, fill = "blue", color = "black") +
  geom_histogram(data = dat_flood_week[time_to_treat2 == 0, ], binwidth = 1, fill = "red", color = "black", alpha = 0.5) + # Highlight the bin with 0
  labs(x = "Weeks Since the Flood", y = "Frequency", title = "Histogram") +
  theme_minimal()

# Display the plot
print(plot)

# Save the plot as a PNG file (adjust the filename as needed)
ggsave(file.path(output_path,"histogram_time_to_treat_week.png"), plot, width = 6, height = 4, units = "in")

# Create also a historgram with the collapsed number of floods by mun (I create in stata but lost the code)

################################



################################################################################

# 1.  Merge with CCS_muni_banco_PF_flood_collapsed.dta to get n_account_stock
#     Do IV for "Log Number of Bank Accounts"

################################################################################

# merge to get n_account_stock
dat_accounts <- read_dta(file.path(dta_path,"CCS_muni_banco_PF_flood_collapsed.dta"))
setDT(dat_accounts)

# Merge the data tables by month and muni
merged_data <- merge(dat_accounts, dat_flood_month, by = c("muni_cd", "time_id"))

iv_function_month("log_qtd","Log Number of Bank Accounts","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data)

################################################################################

# 2. Load Base_week_muni_flood.dta
#     Do IV with its variables

################################################################################

# PF


#Boleto
iv_function_week("log_valor_boleto","Log Value Boleto","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",dat_flood_week)
iv_function_week("log_qtd_boleto","Log Transactions Boleto","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",dat_flood_week)
iv_function_week("log_qtd_cli_pag_pf_boleto","Log Quantity of People Sending Boleto","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",dat_flood_week)

#TED
iv_function_week("log_valor_TED_intra","Log Value TED Intra","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",dat_flood_week)
iv_function_week("log_qtd_TED_intra","Log Transactions TED Intra","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",dat_flood_week)

#PJ
#Credit and Debit
iv_function_week("log_valor_cartao_credito","Log Value Credit Card","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",dat_flood_week)
iv_function_week("log_valor_cartao_debito","Log Value Debit Card","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",dat_flood_week)
iv_function_week("log_qtd_cli_cartao_debito","Log Quantity of Firms using Debit Card","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",dat_flood_week)
iv_function_week("log_qtd_cli_cartao_credito","Log Quantity of Firms using Credit Card","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",dat_flood_week)

#TED
iv_function_week("log_qtd_cli_TED_rec_PJ","Log Quantity of Firms Receiving TED","log_n_cli_pag_pj", "Log Quantity of Firms Sending Pix","after_flood",dat_flood_week)
iv_function_week("log_qtd_cli_TED_pag_PJ","Log Quantity of Firms Sending TED","log_n_cli_pag_pj", "Log Quantity of Firms Sending Pix","after_flood",dat_flood_week)

#Boleto
iv_function_week("log_qtd_cli_pag_pj_boleto","Log Quantity of Firms Sending Boleto","log_n_cli_pag_pj", "Log Quantity of Firms Sending Pix","after_flood",dat_flood_week)
iv_function_week("log_qtd_cli_rec_pj_boleto","Log Quantity of Firms Receiving Boleto","log_n_cli_pag_pj", "Log Quantity of Firms Sending Pix","after_flood",dat_flood_week)


################################################################################

# 3. Load PIX_week_muni_self_flood_sample10.dta
#     Merge with Base_week_muni_flood.dta
#     Do IV with its variables

################################################################################


# merge 
dat_self <- read_dta(file.path(dta_path,"PIX_week_muni_self_flood.dta"))
setDT(dat_self)

# Merge the data tables by month and muni
merged_data <- merge(dat_self, dat_flood_week, by = c("muni_cd", "week"))

iv_function_week("log_valor_self_pf","Log Value","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data)
iv_function_week("log_qtd_self_pf","Log Transactions","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data)
iv_function_week("log_n_cli_self_pf","Log Quantity of People","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data)

#THEN DO PJ
iv_function_week("log_valor_self_pj","Log Value","log_n_cli_pag_pj", "Log Quantity of Firms Sending Pix","after_flood",merged_data)
iv_function_week("log_qtd_self_pj","Log Transactions","log_n_cli_pag_pj", "Log Quantity of Firms Sending Pix","after_flood",merged_data)
iv_function_week("log_n_cli_self_pj","Log Quantity of People","log_n_cli_pag_pj", "Log Quantity of Firms Sending Pix","after_flood",merged_data)



################################################################################

# 4. Base_credito_muni_flood.dta
#     Do IV with its variables

################################################################################

# merge 
dat_credito <- read_dta(file.path(dta_path,"Base_credito_muni_flood.dta"))
setDT(dat_credito)

# Merge the data tables by month and muni
merged_data <- merge(dat_credito, dat_flood_month, by = c("muni_cd", "time_id"))

iv_function_month("log_vol_cartao","Log Volume Credit Card","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data)
iv_function_month("log_qtd_cli_cartao","Log Quantity Credit Card","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data)
iv_function_month("log_vol_emprestimo_pessoal","Log Volume Personal Loan","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data)
iv_function_month("log_qtd_cli_emp_pessoal","Log Quantity Personal Loan","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data)
iv_function_month("log_vol_credito_total","Log Volume Loan","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data)
iv_function_month("log_qtd_cli_total","Log Quantity Loan","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data)
iv_function_month("log_vol_credito_total_PF","Log Volume Loan","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data)
iv_function_month("log_qtd_cli_total_PF","Log Quantity Loan","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data)

# PJ

iv_function_week("log_vol_credito_total_PJ","Log Volume Loan","log_n_cli_pag_pj", "Log Quantity of Firms Sending Pix","after_flood",dat_flood_week)
iv_function_week("log_qtd_cli_total_PJ","Log Quantity Loan","log_n_cli_pag_pj", "Log Quantity of Firms Sending Pix","after_flood",dat_flood_week)

################################################################################

# 5. Base_muni_banco_flood_collapsed.dta
#     Do IV with its variables

################################################################################

# merge 
dat_flow <- read_dta(file.path(dta_path,"Base_muni_banco_flood_collapsed2.dta"))
setDT(dat_flow)

# Merge the data tables by month and muni
merged_data <- merge(dat_flow, dat_flood_week, by = c("muni_cd", "week"))

merged_data_1 <- subset(merged_data,tipo %in% c(1))
merged_data_2 <- subset(merged_data,tipo %in% c(2))
#bank_type %in% c(1) &    Means traditional banks
#bank_type %in% c(2) &    Means digital banks - but I collapsed already
#tipo %in% c(1) means People and 2 means firm

#PF
iv_function_week("log_valor_totalflow","Log Flow Value","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data_1)
iv_function_week("log_qtd_totalflow","Log Flow Transaction","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data_1)
iv_function_week("valor_netflow","NetFlow Value","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data_1)
iv_function_week("qtd_netflow","NetFlow Transaction","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data_1)

#THEN DO PJ
iv_function_week("log_valor_totalflow","Log Flow Value","log_n_cli_pag_pj", "Log Quantity of Firms Sending Pix","after_flood",merged_data_2)
iv_function_week("log_qtd_totalflow","Log Flow Transaction","log_n_cli_pag_pj", "Log Quantity of Firms Sending Pix","after_flood",merged_data_2)
iv_function_week("valor_netflow","NetFlow Value","log_n_cli_pag_pj", "Log Quantity of Firms Sending Pix","after_flood",merged_data_2)
iv_function_week("qtd_netflow","NetFlow Transaction","log_n_cli_pag_pj", "Log Quantity of Firms Sending Pix","after_flood",merged_data_2)


################################################################################

# 6. self flow - Base_muni_banco_self_flood_collapsed.dta - 
#     Do IV with its variables

################################################################################

# merge 
dat_self_flow <- read_dta(file.path(dta_path,"Base_muni_banco_self_flood_collapsed2.dta"))
setDT(dat_self_flow)

# Merge the data tables by month and muni
merged_data <- merge(dat_self_flow, dat_flood_week, by = c("muni_cd", "week"))

merged_data_1 <- subset(merged_data,tipo %in% c(1))
merged_data_2 <- subset(merged_data,tipo %in% c(2))
#bank_type %in% c(1) &    Means traditional banks 
#bank_type %in% c(2) &    Means digital banks - but I collapsed already
#tipo %in% c(1) means People and 2 means firm

#PF
iv_function_week("log_valor_self_totalflow","Log Flow Value","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data_1)
iv_function_week("log_qtd_self_totalflow","Log Flow Transaction","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data_1)
iv_function_week("valor_self_netflow","NetFlow Value","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data_1)
iv_function_week("qtd_self_netflow","NetFlow Transaction","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data_1)

#THEN DO PJ
iv_function_week("log_valor_self_totalflow","Log Flow Value","log_n_cli_pag_pj", "Log Quantity of Firms Sending Pix","after_flood",merged_data_2)
iv_function_week("log_qtd_self_totalflow","Log Flow Transaction","log_n_cli_pag_pj", "Log Quantity of Firms Sending Pix","after_flood",merged_data_2)
iv_function_week("valor_self_netflow","NetFlow Value","log_n_cli_pag_pj", "Log Quantity of Firms Sending Pix","after_flood",merged_data_2)
iv_function_week("qtd_self_netflow","NetFlow Transaction","log_n_cli_pag_pj", "Log Quantity of Firms Sending Pix","after_flood",merged_data_2)

################################################################################

# 7. estban and HHI 
#     Do IV with its variables

################################################################################

# HHI

# merge 
dat_hhi <- read_dta(file.path(dta_path,"Estban_detalhado_HHI_flood.dta"))
setDT(dat_hhi)

# Merge the data tables by month and muni
merged_data <- merge(dat_hhi, dat_flood_month, by = c("muni_cd", "time_id"))

merged_data_1 <- subset(merged_data,tipo %in% c(1))
merged_data_2 <- subset(merged_data,tipo %in% c(2))

#PF
iv_function_month("hhi_total_deposits","HHI Deposits","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data_1)
iv_function_month("hhi_poupanca","HHI Savings","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data_1)
iv_function_month("hhi_dep_prazo","HHI Time Deposits","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data_1)
iv_function_month("hhi_dep_vista_PF","HHI Checking Account of People","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data_1)

#THEN DO PJ
iv_function_month("hhi_dep_vista_PJ","HHI Checking Account of Firms","log_n_cli_pag_pj", "Log Quantity of Firms Sending Pix","after_flood",merged_data_2)


# Deposito

# merge 
dat_deposit <- read_dta(file.path(dta_path,"Estban_detalhado_flood_collapsed.dta"))
setDT(dat_deposit)

# Merge the data tables by month and muni
merged_data <- merge(dat_deposit, dat_flood_month, by = c("muni_cd", "time_id"))

merged_data_1 <- subset(merged_data,tipo %in% c(1))
merged_data_2 <- subset(merged_data,tipo %in% c(2))

#PF
iv_function_month("log_total_deposits","Log Deposits","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data_1)
iv_function_month("log_poupanca","Log Savings","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data_1)
iv_function_month("log_dep_prazo","Log Time Deposits","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data_1)
iv_function_month("log_dep_vista_PF","Log Checking Account of People","log_n_cli_pag_pf", "Log Quantity of People Sending Pix","after_flood",merged_data_1)

#THEN DO PJ
iv_function_month("log_dep_vista_PJ","Log Checking Account of Firms","log_n_cli_pag_pj", "Log Quantity of Firms Sending Pix","after_flood",merged_data_2)

sink()