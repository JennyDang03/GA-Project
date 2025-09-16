#temp_flood_event_studies.R
# Flood Event Studies

################################################################################

#https://lost-stats.github.io/Model_Estimation/Research_Design/event_study.html

options(download.file.method = "wininet")

#install.packages(c("data.table","fixest","haven","ggplot2"))

library(data.table) ## For some minor data wrangling
library(fixest)     ## NB: Requires version >=0.9.0
library(haven)
library(ggplot2)
library(dplyr)

rm(list = ls()) ## Clear workspace

setwd("//sbcdf176/Pix_Matheus$")

# Set file paths
log_path <- "//sbcdf176/PIX_Matheus$/Stata/log/"
dta_path <- "//sbcdf176/PIX_Matheus$/Stata/dta/"
path_dta <- dta_path
output_path <- "//sbcdf176/PIX_Matheus$/Output/"
origdata_path <- "//sbcdf176/PIX_Matheus$/DadosOriginais/"
R_path <- "//sbcdf176/PIX_Matheus$/R/"
#dta_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/"
#path_dta <- dta_path
#output_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output/Matheus_test/"
#log_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/log/"
xl <- -39
xu <- 52
################################################################################
#-------------------------------------------------------------------------------
# Load auxiliary data
#-------------------------------------------------------------------------------

# Load estatic Fixed Effects
mun_fe <- read_dta(file.path(dta_path, "mun_fe.dta"))
mun_fe <- mun_fe %>%
  select(-id_municipio)
# Load time varying Fixed Effects
mun_control <- read_dta(paste0(dta_path,"mun_control.dta"))
mun_control <- mun_control %>%
  select(-id_municipio)
# Load Weekly Flood data - After Pix
flood_week_after <- read_dta(paste0(dta_path,"flood_weekly_2020_2022.dta"))
flood_week_after <- flood_week_after %>%
  select(muni_cd, week, date_flood) %>%
  rename(time = week)
# Load Weekly Flood data - Before Pix
flood_week_before <- read_dta(paste0(dta_path,"flood_weekly_2018_2020.dta"))
flood_week_before <- flood_week_before %>%
  select(muni_cd, week, date_flood) %>%
  rename(time = week)
# Load Monthly Flood data - After Pix
flood_month_after <- read_dta(paste0(dta_path,"flood_monthly_2020_2022.dta"))
flood_month_after <- flood_month_after %>%
  select(muni_cd, time_id, date_flood) %>%
  rename(time = time_id)
# Load Monthly Flood data - Before Pix
flood_month_before <- read_dta(paste0(dta_path,"flood_monthly_2018_2020.dta"))
flood_month_before <- flood_month_before %>%
  select(muni_cd, time_id, date_flood) %>%
  rename(time = time_id)
#Balanced Flood for 6 months.
flood_week_after_balanced <- read_dta(paste0(dta_path,"natural_disasters_weekly_filled_flood.dta"))
municipios2 <- read_dta(paste0(dta_path,"municipios2.dta"))
flood_week_after_balanced <- merge(flood_week_after_balanced, municipios2, by="id_municipio", all= FALSE)
flood_week_after_balanced <- flood_week_after_balanced %>%
  rename(muni_cd = id_municipio_bcb) %>%
  filter(week >= 3165+26 & week <= 3275-26) %>%
  mutate(flood = ifelse(number_disasters > 0, 1, 0)) %>%
  group_by(muni_cd) %>%
  mutate(temp = ifelse(any(flood > 0, na.rm = TRUE), min(week[flood > 0], na.rm = TRUE), NA),
         date_flood = ifelse(any(flood > 0, na.rm = TRUE), max(temp, na.rm = TRUE), NA),
         after_flood = ifelse(week >= date_flood, 1, 0)) %>%
  select(muni_cd, week, date_flood) %>%
  arrange(muni_cd, week) %>%
  rename(time = week) %>% 
  ungroup()
temp <- flood_week_after %>%
  select(muni_cd, time)
flood_week_after_balanced <- merge(flood_week_after_balanced, temp, by=c("muni_cd","time"), all=TRUE)
flood_week_after_balanced <- flood_week_after_balanced %>% 
  group_by(muni_cd) %>% fill(date_flood, .direction = "downup") %>% ungroup()

flood_week_before_balanced <- read_dta(paste0(dta_path,"natural_disasters_weekly_filled_flood.dta"))
municipios2 <- read_dta(paste0(dta_path,"municipios2.dta"))
flood_week_before_balanced <- merge(flood_week_before_balanced, municipios2, by="id_municipio", all= FALSE)
flood_week_before_balanced <- flood_week_before_balanced %>%
  rename(muni_cd = id_municipio_bcb) %>%
  filter(week >= 3016+26 & week < 3165-26) %>%
  mutate(flood = ifelse(number_disasters > 0, 1, 0)) %>%
  group_by(muni_cd) %>%
  mutate(temp = ifelse(any(flood > 0, na.rm = TRUE), min(week[flood > 0], na.rm = TRUE), NA),
         date_flood = ifelse(any(flood > 0, na.rm = TRUE), max(temp, na.rm = TRUE), NA),
         after_flood = ifelse(week >= date_flood, 1, 0)) %>%
  select(muni_cd, week, date_flood) %>%
  arrange(muni_cd, week) %>%
  rename(time = week) %>% 
  ungroup()
temp <- flood_week_before %>%
  select(muni_cd, time)
flood_week_before_balanced <- merge(flood_week_before_balanced, temp, by=c("muni_cd","time"), all=TRUE)
flood_week_before_balanced <- flood_week_before_balanced %>% 
  group_by(muni_cd) %>% fill(date_flood, .direction = "downup") %>% ungroup()

setDT(mun_fe) 
setDT(mun_control) 
setDT(flood_week_after) 
setDT(flood_week_before) 
setDT(flood_month_after) 
setDT(flood_month_before) 
setDT(flood_week_after_balanced) 
setDT(flood_week_before_balanced) 
#-------------------------------------------------------------------------------
# Functions
#-------------------------------------------------------------------------------
stata_week_number <- function(date) {
  year <- year(date)
  month <- month(date)
  days_from_jan1 <- as.numeric(date - as.Date(paste(year, "01-01", sep = "-")))
  week_number <- 1 + (days_from_jan1 %/% 7)
  week_number <- ifelse(week_number == 53, 52, week_number)
  week_number <- week_number + (year-1960)*52 - 1
  return(week_number)
}
gen_week_list <- function(start_year, end_year) {
  date_list <- list()
  for (y in start_year:end_year){
    start_date <- as.Date(paste(y, "01-01", sep = "-"))
    for (i in 1:52) {
      date_list[[i+(y-start_year)*52]] <- start_date + (i - 1) * 7
    }
  }
  start_date <- as.Date(paste(end_year+1, "01-01", sep = "-"))
  date_list[[1+(end_year+1-start_year)*52]] <- start_date + (1 - 1) * 7
  return(date_list)
}
time_id_to_month <- function(month_number){
  month <- month_number %% 12 + 1
  return(month)
}
time_id_to_year <- function(month_number){
  year <- month_number %/% 12 + 1960
  return(year)
}
week_to_month <- function(week_number) {
  year <- week_number %/% 52 + 1960
  week_n <- week_number %% 52 + 1
  first_day <- as.Date((week_n - 1) * 7, origin = paste0(year, "-01-01"))
  month <- month(first_day)
  return(month)
}
week_to_year <- function(week_number) {
  year <- week_number %/% 52 + 1960
  return(year)
}

prepare_data <- function(file,flood_data,fe,controls,xlimit_low, xlimit_up){
  dat <- read_dta(file.path(dta_path,file))
  setDT(dat)
  if ("time_id" %in% names(dat)) {
    dat$time <- dat$time_id
    dat$month <- time_id_to_month(dat$time_id)
    dat$year <- time_id_to_year(dat$time_id)
  } else if ("week" %in% names(dat)) {
    dat$time <- dat$week
    dat$month <- week_to_month(dat$week)
    dat$year <- week_to_year(dat$week)
  }
  
  # Flood, FE, and Control Variables
  dat <- merge(dat, flood_data, by=c("muni_cd","time"), all=FALSE) # maybe I should change week and time_id to time in the previous. then add by=c("muni_cd","time")
  dat <- merge(dat, fe, by="muni_cd", all.x = TRUE) #  by="muni_cd",
  dat <- merge(dat, controls, by=c("muni_cd","month","year"), all.x = TRUE) 
  
  # Event Study Variables
  dat[, treat := ifelse(is.na(date_flood), 0, 1)]
  dat[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
  dat[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
  
  # Set Limits <- Temporary measure - The right way is to run for everything and choose the limits by hand (which takes time and effort)
  xlimits <- seq(ceiling(xlimit_low*1.333),ceiling(xlimit_up*1.333),by=1)
  dat <- subset(dat,time_to_treat %in% xlimits)
  #
  return(dat)
}

twfe <- function(y,control1,control2,fe,dat_list){
  mod_list <- list()
  for (i in 1:length(dat_list)) {
    dat <- dat_list[[i]]
    dat$Y <- dat[[y]]
    dat$C1 <- dat[[control1]]
    dat$C2 <- dat[[control2]]
    dat$FE <- dat[[fe]]
    
    mod_twfe <- feols(Y ~ i(time_to_treat, treat, ref = -1) + ## Our key interaction: time ? treatment status
                        C1 + C2 |                          ## Control variables
                        muni_cd + time + FE:time,          ## FEs
                      cluster = ~muni_cd,                  ## Clustered SEs
                      data = dat)
    
    mod_list[[i]] <- mod_twfe
  }
  return(mod_list)
}
print_twfe <- function(graphname,y,control1,control2,fe,main_title,dat_list,legend_list,xlimit_l,xlimit_u){
  pch_list = c(16,17,15,1,2,0,5,3) # 18 didnt work
  col_list = c("black", "red", "green", "blue", "purple", "orange", "cyan", "magenta", "yellow")
  mod_list <- twfe(y,control1,control2,fe,dat_list)
  if(length(dat_list) == 1){mod_list <- mod_list[[1]]}
  png(file.path(output_path,paste0(graphname,y,".png")), width = 640*4, height = 480*4, res = 200)
  par(cex.main = 1.75, cex.lab = 1.25, cex.axis = 1.75)
  iplot(mod_list, sep = 0.5, ref.line = -1,
        xlab = '',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.1,xlimit_u+0.1),
        col = col_list, pch = pch_list, cex=0.7) 
  legend("bottomleft", col = col_list, pch = pch_list, 
         legend = legend_list, cex = 1)
  dev.off()
}


# Test
#dat_flood <- prepare_data("flood_pix_weekly_fake.dta",flood_week_after,mun_fe,mun_control,xl, xu)
#print_twfe("Pix_fake_pre+int_test1","log_valor_PIX_inflow","pre","internet_access","constant","Log Value Pix Inflow",list(dat_flood),c("TWFE"), xl, xu) 
#dat2 <- dat_flood %>% filter(muni_cd < 3000)
#print_twfe("Pix_fake_pre+int_test2","log_valor_PIX_inflow","pre","internet_access","constant","Log Value Pix Inflow",list(dat_flood,dat2),c("test1","test2"), xl, xu) 
#dat3 <- dat_flood %>% filter(muni_cd < 4000)
#print_twfe("Pix_fake_pre+int_test3","log_valor_PIX_inflow","pre","internet_access","constant","Log Value Pix Inflow",list(dat_flood,dat2,dat3),c("test1","test2","test3"), xl, xu) 
#print_twfe("Pix_fake_pre+int_test4","log_valor_PIX_inflow","pre","internet_access","constant","Log Value Pix Inflow",list(dat_flood,dat2,dat3,dat_flood),c("test1","test2","test3","test4"), xl, xu) 


#-------------------------------------------------------------------------------
# Old Graphs redone. 
#-------------------------------------------------------------------------------
#Base_week_muni.dta
tryCatch({
  dat_a <- prepare_data("Base_week_muni.dta",flood_week_after,mun_fe,mun_control,xl, xu)
  dat_b <- prepare_data("Base_week_muni.dta",flood_week_before,mun_fe,mun_control,xl, xu)
  dat_a <- dat_a %>%
    mutate(log_valor_TED_intra = log1p(valor_TED_intra),
           log_qtd_TED_intra = log1p(qtd_TED_intra),
           log_qtd_cli_TED_rec_PJ = log1p(qtd_cli_TED_rec_PJ),
           log_qtd_cli_TED_pag_PJ = log1p(qtd_cli_TED_pag_PJ),
           log_valor_boleto = log1p(valor_boleto),
           log_qtd_boleto = log1p(qtd_boleto),
           log_qtd_cli_pag_pf_boleto = log1p(qtd_cli_pag_pf_boleto),
           log_qtd_cli_pag_pj_boleto = log1p(qtd_cli_pag_pj_boleto),
           log_qtd_cli_rec_pj_boleto = log1p(qtd_cli_rec_pj_boleto),
           log_valor_cartao_credito = log1p(valor_cartao_credito),
           log_valor_cartao_debito = log1p(valor_cartao_debito),
           log_qtd_cli_cartao_debito = log1p(qtd_cli_cartao_debito),
           log_qtd_cli_cartao_credito = log1p(qtd_cli_cartao_credito))
  dat_b <- dat_b %>%
    mutate(log_valor_TED_intra = log1p(valor_TED_intra),
           log_qtd_TED_intra = log1p(qtd_TED_intra),
           log_qtd_cli_TED_rec_PJ = log1p(qtd_cli_TED_rec_PJ),
           log_qtd_cli_TED_pag_PJ = log1p(qtd_cli_TED_pag_PJ),
           log_valor_boleto = log1p(valor_boleto),
           log_qtd_boleto = log1p(qtd_boleto),
           log_qtd_cli_pag_pf_boleto = log1p(qtd_cli_pag_pf_boleto),
           log_qtd_cli_pag_pj_boleto = log1p(qtd_cli_pag_pj_boleto),
           log_qtd_cli_rec_pj_boleto = log1p(qtd_cli_rec_pj_boleto),
           log_valor_cartao_credito = log1p(valor_cartao_credito),
           log_valor_cartao_debito = log1p(valor_cartao_debito),
           log_qtd_cli_cartao_debito = log1p(qtd_cli_cartao_debito),
           log_qtd_cli_cartao_credito = log1p(qtd_cli_cartao_credito))
  
  #TED
  print_twfe("TED_","log_valor_TED_intra","pre","internet_access","constant","Log Value TED",list(dat_b,dat_a), c("Before","After"), xl, xu)
  print_twfe("TED_","log_qtd_TED_intra","pre","internet_access","constant","Log Transactions TED",list(dat_b,dat_a), c("Before","After"), xl, xu)
  print_twfe("TED_","log_qtd_cli_TED_rec_PJ","pre","internet_access","constant","Log Quantity of Firms Receiving TED",list(dat_b,dat_a), c("Before","After"), xl, xu)
  print_twfe("TED_","log_qtd_cli_TED_pag_PJ","pre","internet_access","constant","Log Quantity of Firms Sending TED",list(dat_b,dat_a), c("Before","After"), xl, xu)
  
  #Boleto
  print_twfe("Boleto_","log_valor_boleto","pre","internet_access","constant","Log Value Boleto",list(dat_b,dat_a), c("Before","After"), xl, xu)
  print_twfe("Boleto_","log_qtd_boleto","pre","internet_access","constant","Log Transactions Boleto",list(dat_b,dat_a), c("Before","After"), xl, xu)
  print_twfe("Boleto_","log_qtd_cli_pag_pf_boleto","pre","internet_access","constant","Log Quantity of People Sending Boleto",list(dat_b,dat_a), c("Before","After"), xl, xu)
  print_twfe("Boleto_","log_qtd_cli_pag_pj_boleto","pre","internet_access","constant","Log Quantity of Firms Sending Boleto",list(dat_b,dat_a), c("Before","After"), xl, xu)
  print_twfe("Boleto_","log_qtd_cli_rec_pj_boleto","pre","internet_access","constant","Log Quantity of Firms Receiving Boleto",list(dat_b,dat_a), c("Before","After"), xl, xu)
  
  #Cartao 
  # * valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito 
  print_twfe("Cartao_","log_valor_cartao_debito","pre","internet_access","constant","Log Value Debit Card",list(dat_b,dat_a), c("Before","After"), xl, xu)
  print_twfe("Cartao_","log_valor_cartao_credito","pre","internet_access","constant","Log Value Credit Card",list(dat_b,dat_a), c("Before","After"), xl, xu)
  print_twfe("Cartao_","log_qtd_cli_cartao_debito","pre","internet_access","constant","Log Quantity of Firms accepting Debit Card",list(dat_b,dat_a), c("Before","After"), xl, xu)
  print_twfe("Cartao_","log_qtd_cli_cartao_credito","pre","internet_access","constant","Log Quantity of Firms accepting Credit Card",list(dat_b,dat_a), c("Before","After"), xl, xu)
  
  #PIX
  dat_a <- dat_a %>%
    mutate(log_valor_PIX_inflow = log1p(valor_PIX_inflow),
           log_valor_PIX_outflow = log1p(valor_PIX_outflow),
           log_valor_PIX_intra = log1p(valor_PIX_intra),
           log_qtd_PIX_inflow = log1p(qtd_PIX_inflow),
           log_qtd_PIX_outflow = log1p(qtd_PIX_outflow),
           log_qtd_PIX_intra = log1p(qtd_PIX_intra),
           log_valor_rec = log1p(valor_PIX_inflow + valor_PIX_intra),
           log_valor_sent = log1p(valor_PIX_outflow + valor_PIX_intra),
           log_qtd_rec = log1p(qtd_PIX_inflow + qtd_PIX_intra),
           log_qtd_sent = log1p(qtd_PIX_outflow + qtd_PIX_intra),
           log_n_cli_pag_pf_intra = log1p(n_cli_pag_pf_intra),
           log_n_cli_rec_pf_intra = log1p(n_cli_rec_pf_intra),
           log_n_cli_pag_pj_intra = log1p(n_cli_pag_pj_intra),
           log_n_cli_rec_pj_intra = log1p(n_cli_rec_pj_intra),
           log_n_cli_rec_pf_inflow = log1p(n_cli_rec_pf_inflow),
           log_n_cli_rec_pj_inflow = log1p(n_cli_rec_pj_inflow),
           log_n_cli_pag_pf_outflow = log1p(n_cli_pag_pf_outflow),
           log_n_cli_pag_pj_outflow = log1p(n_cli_pag_pj_outflow),
           log_n_cli_pag_pf = log1p(n_cli_pag_pf_outflow + n_cli_pag_pf_intra),
           log_n_cli_rec_pf = log1p(n_cli_rec_pf_inflow + n_cli_rec_pf_intra),
           log_n_cli_pag_pj = log1p(n_cli_pag_pj_outflow + n_cli_pag_pj_intra),
           log_n_cli_rec_pj = log1p(n_cli_rec_pj_inflow + n_cli_rec_pj_intra))
  dat_inflow <- dat_a %>%
    rename(log_valor_flow = log_valor_PIX_inflow,
           log_qtd_flow = log_qtd_PIX_inflow)
  dat_outflow <- dat_a %>%
    rename(log_valor_flow = log_valor_PIX_outflow,
           log_qtd_flow = log_qtd_PIX_outflow)
  dat_intra <- dat_a %>%
    rename(log_valor_flow = log_valor_PIX_intra,
           log_qtd_flow = log_qtd_PIX_intra)
  dat_rec <- dat_a %>%
    rename(log_valor = log_valor_rec,
           log_qtd = log_qtd_rec,
           log_n_cli_pf = log_n_cli_rec_pf,
           log_n_cli_pj = log_n_cli_rec_pj)
  dat_sent <- dat_a %>%
    rename(log_valor = log_valor_sent,
           log_qtd = log_qtd_sent,
           log_n_cli_pf = log_n_cli_pag_pf,
           log_n_cli_pj = log_n_cli_pag_pj)
  
  print_twfe("Pix_old_","log_valor_flow","pre","internet_access","constant","Log Value Pix",list(dat_inflow,dat_outflow, dat_intra),c("Inflow","Outflow", "Intra"), xl, xu)
  print_twfe("Pix_old_","log_qtd_flow","pre","internet_access","constant","Log Transactions Pix",list(dat_inflow,dat_outflow, dat_intra),c("Inflow","Outflow", "Intra"), xl, xu)
  
  print_twfe("Pix_old_","log_valor","pre","internet_access","constant","Log Value Pix",list(dat_rec, dat_sent), c("Received","Sent"), xl, xu)
  print_twfe("Pix_old_","log_qtd","pre","internet_access","constant","Log Transactions Pix",list(dat_rec, dat_sent), c("Received","Sent"), xl, xu)
  print_twfe("Pix_old_","log_n_cli_pf","pre","internet_access","constant","Log Active Users - People",list(dat_rec, dat_sent), c("Received","Sent"), xl, xu)
  print_twfe("Pix_old_","log_n_cli_pj","pre","internet_access","constant","Log Active Users - Firms",list(dat_rec, dat_sent), c("Received","Sent"), xl, xu)
  
}, error = function(e) {
  # Handle errors or print a message
  print(paste("Error in Base_week_muni:", e))
})

# ESTBAN
tryCatch({
  dat_a <- read_dta(file.path(dta_path,"Estban_detalhado_flood_collapsed.dta"))
  setDT(dat_a)
  dat_a$time <- dat_a$time_id
  dat_a$month <- time_id_to_month(dat_a$time_id)
  dat_a$year <- time_id_to_year(dat_a$time_id)
  dat_a <- merge(dat_a, mun_fe, by="muni_cd", all.x = TRUE)
  dat_a <- merge(dat_a, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
  dat_a[, treat := ifelse(is.na(date_flood), 0, 1)]
  dat_a[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
  dat_a[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
  xlimits <- seq(ceiling(-9*1.333),ceiling(12*1.333),by=1)
  dat_a <- subset(dat,time_to_treat %in% xlimits)
  dat_a_large <- subset(dat_a, large_bank %in% c(1))
  dat_a_small <- dat_a[large_bank %in% c(0)]
  
  dat_a2 <- read_dta(file.path(dta_path,"Estban_detalhado_flood_collapsed2.dta"))
  setDT(dat_a2)
  dat_a2$time <- dat_a2$time_id
  dat_a2$month <- time_id_to_month(dat_a2$time_id)
  dat_a2$year <- time_id_to_year(dat_a2$time_id)
  dat_a2 <- merge(dat_a2, mun_fe, by="muni_cd", all.x = TRUE)
  dat_a2 <- merge(dat_a2, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
  dat_a2[, treat := ifelse(is.na(date_flood), 0, 1)]
  dat_a2[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
  dat_a2[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
  xlimits <- seq(ceiling(-9*1.333),ceiling(12*1.333),by=1)
  dat_a2 <- subset(dat_a2,time_to_treat %in% xlimits)
  
  dat_b <- read_dta(file.path(dta_path,"Estban_detalhado_flood_beforePIX_collapsed.dta"))
  setDT(dat_b)
  dat_b$time <- dat_b$time_id
  dat_b$month <- time_id_to_month(dat_b$time_id)
  dat_b$year <- time_id_to_year(dat_b$time_id)
  dat_b <- merge(dat_b, mun_fe, by="muni_cd", all.x = TRUE)
  dat_b <- merge(dat_b, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
  dat_b[, treat := ifelse(is.na(date_flood), 0, 1)]
  dat_b[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
  dat_b[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
  xlimits <- seq(ceiling(-9*1.333),ceiling(12*1.333),by=1)
  dat_b <- subset(dat,time_to_treat %in% xlimits)
  dat_b_large <- subset(dat_b, large_bank %in% c(1))
  dat_b_small <- dat_b[large_bank %in% c(0)]
  
  dat_b2 <- read_dta(file.path(dta_path,"Estban_detalhado_flood_beforePIX_collapsed.dta"))
  setDT(dat_b2)
  dat_b2$time <- dat_b2$time_id
  dat_b2$month <- time_id_to_month(dat_b2$time_id)
  dat_b2$year <- time_id_to_year(dat_b2$time_id)
  dat_b2 <- merge(dat_b2, mun_fe, by="muni_cd", all.x = TRUE)
  dat_b2 <- merge(dat_b2, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
  dat_b2[, treat := ifelse(is.na(date_flood), 0, 1)]
  dat_b2[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
  dat_b2[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
  xlimits <- seq(ceiling(-9*1.333),ceiling(12*1.333),by=1)
  dat_b2 <- subset(dat_b2,time_to_treat %in% xlimits)
  
  #log_caixa, log_total_deposits, log_poupanca, log_dep_prazo, log_dep_vista_PF, log_dep_vista_PJ
  # Large vs Small
  print_twfe("Estban_ls_","log_caixa","pre","internet_access","constant","Log Monetary Inventory",list(dat_a_large,dat_a_small), c("Top 5 Bank","Others"), -9, 12)
  
  # Before vs After
  print_twfe("Estban_ba_","log_caixa","pre","internet_access","constant","Log Monetary Inventory",list(dat_b2,dat_a2), c("Before","After"), -9, 12)
  
  rm(dat_a,dat_a2,dat_b,dat_b2)
}, error = function(e) {
  # Handle errors or print a message
  print(paste("Error in ESTBAN:", e))
})

# RAIS



