# Flood Event Studies
#https://lost-stats.github.io/Model_Estimation/Research_Design/event_study.html

################################################################################
options(download.file.method = "wininet")
rm(list = ls()) ## Clear workspace

library(readr)
library(stringr)
library(odbc)
library(dplyr)
library(tidyr)
library(RODBC)
library(futile.logger)
library(data.table)
library(bit64)
library(haven)
library(gdata)
library("arrow")
library(stargazer)
library(lubridate)
library(fixest)     
library(ggplot2)

setwd("//sbcdf176/Pix_Matheus$")
path_main <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/"
path_main <- "//sbcdf176/PIX_Matheus$/"

path_query <- paste0(path_main, "R/DataExtraction/")
path_data <- paste0(path_main, "DadosOriginais/")
path_dta <- paste0(path_main, "Stata/dta/")
path_output <- paste0(path_main, "Output/")
log_path <- paste0(path_main, "Stata/log/")
dta_path <- paste0(path_main, "Stata/dta/")
output_path <- paste0(path_main, "Output/")
origdata_path <- paste0(path_main, "DadosOriginais/")
R_path <- paste0(path_main, "R/")

# Constants
xl <- -26
xu <- 52
xl_balanced <- -26
xu_balanced <- 52
xl_balanced_covid <- -13
xu_balanced_covid <- 13
################################################################################
#-------------------------------------------------------------------------------
# Load auxiliary data
#-------------------------------------------------------------------------------
source(paste0(R_path, "/auxiliary_data.r"))

#-------------------------------------------------------------------------------
# Functions
#-------------------------------------------------------------------------------
source(paste0(R_path,"/functions/stata_week_number.R"))
source(paste0(R_path,"/functions/gen_week_list.R"))
source(paste0(R_path,"/functions/time_id_to_month.R"))
source(paste0(R_path,"/functions/time_id_to_year.R"))
source(paste0(R_path,"/functions/week_to_month.R"))
source(paste0(R_path,"/functions/week_to_year.R"))

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
  dat <- merge(dat, flood_data, by=c("muni_cd","time"), all=FALSE) # it deletes if no match.
  dat <- merge(dat, fe, by="muni_cd", all.x = TRUE) 
  dat <- merge(dat, controls, by=c("muni_cd","month","year"), all.x = TRUE) 
  
  # Event Study Variables
  dat[, treat := ifelse(is.na(date_flood), 0, 1)]
  dat[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
  dat[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
  dat[, after_flood := ifelse(is.na(date_flood), 0, ifelse(time >= date_flood, 1, 0))]

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
  par(cex.main = 1.75, cex.lab = 1.5, cex.axis = 1.75)
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
#source(paste0(R_path,"/Test_flood_event_studies.R"))

# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# CCS
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# CCS_Muni_stock
# ------------------------------------------------------------------------------
# It was not downloaded yet.
# We can improve the way we deal with banked_pop or stock. Maybe divide by population. talk to sean and Jacopo.

tryCatch({
# write_dta(data, paste0(dta_path,"CCS_Muni_stock",".dta"))
# Variables: week, muni_cd, tipo, muni_stock, muni_stock_w, banked_pop
#             lmuni_stock, lmuni_stock_w, lbanked_pop

dat_after <- prepare_data("CCS_Muni_stock.dta",flood_week_after,mun_fe,mun_control, xl, xu)
dat_before <- prepare_data("CCS_Muni_stock.dta",flood_week_before2019,mun_fe,mun_control, xl, xu)
dat_after_PF <- dat_after %>% filter(tipo==1)
dat_before_PF <- dat_before %>% filter(tipo==1)
dat_after_PJ <- dat_after %>% filter(tipo==2)
dat_before_PJ <- dat_before %>% filter(tipo==2)
# PF - Before vs After
print_twfe("CCS_Muni_PF_","lmuni_stock_w","constant","constant","flood_risk5","Log Number of Bank Accounts", list(dat_before_PF, dat_after_PF), c("2019.01 - 2020.10","2020.11 - 2022.12"),xl, xu)
print_twfe("CCS_Muni_PF_","lbanked_pop","constant","constant","flood_risk5","Log Banked Population",list(dat_before_PF, dat_after_PF), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
# PJ - Before vs After
print_twfe("CCS_Muni_PJ_","lmuni_stock_w","constant","constant","flood_risk5","Log Number of Bank Accounts",list(dat_before_PJ, dat_after_PJ), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
print_twfe("CCS_Muni_PJ_","lbanked_pop","constant","constant","flood_risk5","Log Banked Population",list(dat_before_PJ, dat_after_PJ), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
rm(dat_after,dat_before,dat_after_PF,dat_before_PF,dat_after_PJ,dat_before_PJ)

# Generalized difference in difference
model1 <- feols(lmuni_stock_w ~ after_flood | 0 | 
                muni_cd + time + time:flood_risk5, 
                cluster = ~muni_cd, 
                data = dat_after_PF)
model2 <- feols(lmuni_stock_w ~ after_flood | 0 | 
                muni_cd + time + time:flood_risk5, 
                cluster = ~muni_cd, 
                data = dat_before_PF)
model3 <- feols(lmuni_stock_w ~ after_flood | 0 |
                muni_cd + time + time:flood_risk5, 
                cluster = ~muni_cd, 
                data = dat_after_PJ)
model4 <- feols(lmuni_stock_w ~ after_flood | 0 |
                muni_cd + time + time:flood_risk5, 
                cluster = ~muni_cd, 
                data = dat_before_PJ)
stargazer(model1, model2, model3, model4, type = "text")

model5 <- feols(lbanked_pop ~ after_flood | 0 | 
                muni_cd + time + time:flood_risk5, 
                cluster = ~muni_cd, 
                data = dat_after_PF)
model6 <- feols(lbanked_pop ~ after_flood | 0 |
                muni_cd + time + time:flood_risk5, 
                cluster = ~muni_cd, 
                data = dat_before_PF)
model7 <- feols(lbanked_pop ~ after_flood | 0 |
                muni_cd + time + time:flood_risk5, 
                cluster = ~muni_cd, 
                data = dat_after_PJ)
model8 <- feols(lbanked_pop ~ after_flood | 0 |
                muni_cd + time + time:flood_risk5, 
                cluster = ~muni_cd, 
                data = dat_before_PJ)
stargazer(model5, model6, model7, model8, type = "text")



# Now balanced

dat_after <- prepare_data("CCS_Muni_stock.dta",flood_week_after_balanced,mun_fe,mun_control, xl_balanced, xu_balanced)
dat_before <- prepare_data("CCS_Muni_stock.dta",flood_week_before_balanced2019,mun_fe,mun_control, xl_balanced, xu_balanced)
dat_after_PF <- dat_after %>% filter(tipo==1)
dat_before_PF <- dat_before %>% filter(tipo==1)
dat_after_PJ <- dat_after %>% filter(tipo==2)
dat_before_PJ <- dat_before %>% filter(tipo==2)
# PF - Before vs After
print_twfe("CCS_Muni_PF_balanced_","lmuni_stock_w","constant","constant","flood_risk5","Log Number of Bank Accounts", list(dat_before_PF, dat_after_PF), c("2019.01 - 2020.10","2020.11 - 2022.12"),xl_balanced, xu_balanced)
print_twfe("CCS_Muni_PF_balanced_","lbanked_pop","constant","constant","flood_risk5","Log Banked Population",list(dat_before_PF, dat_after_PF), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
# PJ - Before vs After
print_twfe("CCS_Muni_PJ_balanced_","lmuni_stock_w","constant","constant","flood_risk5","Log Number of Bank Accounts",list(dat_before_PJ, dat_after_PJ), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
print_twfe("CCS_Muni_PJ_balanced_","lbanked_pop","constant","constant","flood_risk5","Log Banked Population",list(dat_before_PJ, dat_after_PJ), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
rm(dat_after,dat_before,dat_after_PF,dat_before_PF,dat_after_PJ,dat_before_PJ)

# Now covid_balanced

dat_after <- prepare_data("CCS_Muni_stock.dta",flood_week_after_balanced_covid,mun_fe,mun_control, xl_balanced_covid, xu_balanced_covid)
dat_before <- prepare_data("CCS_Muni_stock.dta",flood_week_before_balanced_covid,mun_fe,mun_control, xl_balanced_covid, xu_balanced_covid)
dat_after_PF <- dat_after %>% filter(tipo==1)
dat_before_PF <- dat_before %>% filter(tipo==1)
dat_after_PJ <- dat_after %>% filter(tipo==2)
dat_before_PJ <- dat_before %>% filter(tipo==2)
# PF - Before vs After
print_twfe("CCS_Muni_PF_balanced_covid_","lmuni_stock_w","constant","constant","flood_risk5","Log Number of Bank Accounts", list(dat_before_PF, dat_after_PF), c("2020.03 - 2020.10","2020.11 - 2021.06"),xl_balanced_covid, xu_balanced_covid)
print_twfe("CCS_Muni_PF_balanced_covid_","lbanked_pop","constant","constant","flood_risk5","Log Banked Population",list(dat_before_PF, dat_after_PF), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
# PJ - Before vs After
print_twfe("CCS_Muni_PJ_balanced_covid_","lmuni_stock_w","constant","constant","flood_risk5","Log Number of Bank Accounts",list(dat_before_PJ, dat_after_PJ), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
print_twfe("CCS_Muni_PJ_balanced_covid_","lbanked_pop","constant","constant","flood_risk5","Log Banked Population",list(dat_before_PJ, dat_after_PJ), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
rm(dat_after,dat_before,dat_after_PF,dat_before_PF,dat_after_PJ,dat_before_PJ)

}, error = function(e) {
  # Handle errors or print a message
  print(paste("Error in CCS_Muni_stock:", e))
})


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# CCS_Muni_IF
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# Worked
# Only small errors by the end.K
# print_twfe("CCS_Muni_IF_PJ_b_balanced_","lclosing","pre","internet_access","constant","Log Closing of Bank Accounts",list(trad_before, digi_before), c("Traditional","Digital"), -26,26)
# Error: the dependend variable is a constant. the estimation with fixest cannot be done. 

# All the graphs are horrible, focus on lstock. And put traditional and digital together. Thats the best we can hope for.
# Actually, lstock is just like the graphs above, so it doesnt matter. 
# maybe it would be useful to show stock of tech vs trad so it would settle that both remain stable. 


tryCatch({

#filename <- c("CCS_Muni_IF_PF", "CCS_Muni_IF_PJ")
# Variables: week, muni_cd, tipo, bank, opening, stock, closing
#             lopening, lstock, lclosing
# PEOPLE
dat_after <- prepare_data("CCS_Muni_IF_PF.dta",flood_week_after,mun_fe,mun_control,xl, xu)
cat("number of rows for CCS_Muni_IF_PF.dta:", nrow(dat_after))
dat_after <- merge(dat_after, Cadastro_IF, by="bank", all = FALSE)
cat("number of rows for CCS_Muni_IF_PF.dta after merge:", nrow(dat_after))
dat_after <- dat_after %>%
  select(-tipo_inst, -bank, -lopening, -lstock, -lclosing) %>%
  group_by_at(vars(-opening, -stock, -closing)) %>%
  summarise(opening = sum(opening, na.rm = TRUE),
    stock = sum(stock, na.rm = TRUE),
    closing = sum(closing, na.rm = TRUE)) %>%
  mutate(lopening = log1p(opening),
    lclosing = log1p(closing),
    lstock = log1p(stock)) %>%
  ungroup()

digi_after <- dat_after %>%
  filter(bank_type == 2)
trad_after <- dat_after %>%
  filter(bank_type == 1)
rm(dat_after)

# dat_before <- prepare_data("CCS_Muni_IF_PF.dta",flood_week_before,mun_fe,mun_control,xl, xu)
# dat_before <- merge(dat_before, Cadastro_IF, by="bank", all = FALSE)
# dat_before <- dat_before %>%
#   select(-tipo_inst, -bank, -lopening, -lstock, -lclosing) %>%
#   group_by_at(vars(-opening, -stock, -closing)) %>%
#   summarise(opening = sum(opening, na.rm = TRUE),
#             stock = sum(stock, na.rm = TRUE),
#             closing = sum(closing, na.rm = TRUE)) %>%
#   mutate(lopening = log1p(opening),
#          lclosing = log1p(closing),
#          lstock = log1p(stock)) %>%
#   ungroup()
# digi_before <- dat_before %>%
#   filter(bank_type == 2)
# trad_before <- dat_before %>%
#   filter(bank_type == 1)
# rm(dat_before)

print_twfe("CCS_Muni_IF_PF_","lstock","constant","constant","flood_risk5","Log Number of Bank Accounts",list(trad_after, digi_after), c("Traditional","Digital"), xl, xu)
#print_twfe("CCS_Muni_IF_PF_b_","lstock","pre","internet_access","constant","Log Number of Bank Accounts",list(trad_before, digi_before), c("Traditional","Digital"), xl, xu)
#print_twfe("CCS_Muni_IF_PF_trad_","lstock","pre","internet_access","constant","Log Number of Bank Accounts",list(trad_before, trad_after), c("Before Pix","After Pix"), xl, xu)
#print_twfe("CCS_Muni_IF_PF_digi_","lstock","pre","internet_access","constant","Log Number of Bank Accounts",list(digi_before, digi_after), c("Before Pix","After Pix"), xl, xu)

rm(digi_after, trad_after)
#rm(digi_before, trad_before)

# FIRMS
dat_after <- prepare_data("CCS_Muni_IF_PJ.dta",flood_week_after,mun_fe,mun_control,xl, xu)
dat_after <- merge(dat_after, Cadastro_IF, by="bank", all = FALSE)
dat_after <- dat_after %>%
  select(-tipo_inst, -bank, -lopening, -lstock, -lclosing) %>%
  group_by_at(vars(-opening, -stock, -closing)) %>%
  summarise(opening = sum(opening, na.rm = TRUE),
            stock = sum(stock, na.rm = TRUE),
            closing = sum(closing, na.rm = TRUE)) %>%
  mutate(lopening = log1p(opening),
         lclosing = log1p(closing),
         lstock = log1p(stock)) %>%
  ungroup()

digi_after <- dat_after %>%
  filter(bank_type == 2)
trad_after <- dat_after %>%
  filter(bank_type == 1)
rm(dat_after)

# dat_before <- prepare_data("CCS_Muni_IF_PJ.dta",flood_week_before,mun_fe,mun_control,xl, xu)
# dat_before <- merge(dat_before, Cadastro_IF, by="bank", all = FALSE)
# dat_before <- dat_before %>%
#   select(-tipo_inst, -bank, -lopening, -lstock, -lclosing) %>%
#   group_by_at(vars(-opening, -stock, -closing)) %>%
#   summarise(opening = sum(opening, na.rm = TRUE),
#             stock = sum(stock, na.rm = TRUE),
#             closing = sum(closing, na.rm = TRUE)) %>%
#   mutate(lopening = log1p(opening),
#          lclosing = log1p(closing),
#          lstock = log1p(stock)) %>%
#   ungroup()
# digi_before <- dat_before %>%
#   filter(bank_type == 2)
# trad_before <- dat_before %>%
#   filter(bank_type == 1)
# rm(dat_before)

print_twfe("CCS_Muni_IF_PJ_","lstock","constant","constant","flood_risk5","Log Number of Bank Accounts",list(trad_after, digi_after), c("Traditional","Digital"), xl, xu)
#print_twfe("CCS_Muni_IF_PJ_b_","lstock","pre","internet_access","constant","Log Number of Bank Accounts",list(trad_before, digi_before), c("Traditional","Digital"), xl, xu)
#print_twfe("CCS_Muni_IF_PJ_trad_","lstock","pre","internet_access","constant","Log Number of Bank Accounts",list(trad_before, trad_after), c("Before Pix","After Pix"), xl, xu)
#print_twfe("CCS_Muni_IF_PJ_digi_","lstock","pre","internet_access","constant","Log Number of Bank Accounts",list(digi_before, digi_after), c("Before Pix","After Pix"), xl, xu)

rm(digi_after, trad_after)
#rm(digi_before, trad_before)

# _balanced #####################################

# PEOPLE
dat_after <- prepare_data("CCS_Muni_IF_PF.dta",flood_week_after_balanced,mun_fe,mun_control, xl_balanced, xu_balanced)
dat_after <- merge(dat_after, Cadastro_IF, by="bank", all = FALSE)
dat_after <- dat_after %>%
  select(-tipo_inst, -bank, -lopening, -lstock, -lclosing) %>%
  group_by_at(vars(-opening, -stock, -closing)) %>%
  summarise(opening = sum(opening, na.rm = TRUE),
            stock = sum(stock, na.rm = TRUE),
            closing = sum(closing, na.rm = TRUE)) %>%
  mutate(lopening = log1p(opening),
         lclosing = log1p(closing),
         lstock = log1p(stock)) %>%
  ungroup()

digi_after <- dat_after %>%
  filter(bank_type == 2)
trad_after <- dat_after %>%
  filter(bank_type == 1)
rm(dat_after)

# dat_before <- prepare_data("CCS_Muni_IF_PF.dta",flood_week_before_balanced,mun_fe,mun_control, xl_balanced, xu_balanced)
# dat_before <- merge(dat_before, Cadastro_IF, by="bank", all = FALSE)
# dat_before <- dat_before %>%
#   select(-tipo_inst, -bank, -lopening, -lstock, -lclosing) %>%
#   group_by_at(vars(-opening, -stock, -closing)) %>%
#   summarise(opening = sum(opening, na.rm = TRUE),
#             stock = sum(stock, na.rm = TRUE),
#             closing = sum(closing, na.rm = TRUE)) %>%
#   mutate(lopening = log1p(opening),
#          lclosing = log1p(closing),
#          lstock = log1p(stock)) %>%
#   ungroup()
# digi_before <- dat_before %>%
#   filter(bank_type == 2)
# trad_before <- dat_before %>%
#   filter(bank_type == 1)
# rm(dat_before)

print_twfe("CCS_Muni_IF_PF_balanced_","lstock","constant","constant","flood_risk5","Log Number of Bank Accounts",list(trad_after, digi_after), c("Traditional","Digital"),  xl_balanced, xu_balanced)
#print_twfe("CCS_Muni_IF_PF_b_balanced_","lstock","pre","internet_access","constant","Log Number of Bank Accounts",list(trad_before, digi_before), c("Traditional","Digital"), -26,26)
#print_twfe("CCS_Muni_IF_PF_trad_balanced_","lstock","pre","internet_access","constant","Log Number of Bank Accounts",list(trad_before, trad_after), c("Before Pix","After Pix"), -26,26)
#print_twfe("CCS_Muni_IF_PF_digi_balanced_","lstock","pre","internet_access","constant","Log Number of Bank Accounts",list(digi_before, digi_after), c("Before Pix","After Pix"), -26,26)

rm(digi_after, trad_after)
#rm(digi_before, trad_before)
# FIRMS
dat_after <- prepare_data("CCS_Muni_IF_PJ.dta",flood_week_after_balanced,mun_fe,mun_control, xl_balanced, xu_balanced)
dat_after <- merge(dat_after, Cadastro_IF, by="bank", all = FALSE)
dat_after <- dat_after %>%
  select(-tipo_inst, -bank, -lopening, -lstock, -lclosing) %>%
  group_by_at(vars(-opening, -stock, -closing)) %>%
  summarise(opening = sum(opening, na.rm = TRUE),
            stock = sum(stock, na.rm = TRUE),
            closing = sum(closing, na.rm = TRUE)) %>%
  mutate(lopening = log1p(opening),
         lclosing = log1p(closing),
         lstock = log1p(stock)) %>%
  ungroup()

digi_after <- dat_after %>%
  filter(bank_type == 2)
trad_after <- dat_after %>%
  filter(bank_type == 1)
rm(dat_after)

# dat_before <- prepare_data("CCS_Muni_IF_PJ.dta",flood_week_before_balanced,mun_fe,mun_control, xl_balanced, xu_balanced)
# dat_before <- merge(dat_before, Cadastro_IF, by="bank", all = FALSE)
# dat_before <- dat_before %>%
#   select(-tipo_inst, -bank, -lopening, -lstock, -lclosing) %>%
#   group_by_at(vars(-opening, -stock, -closing)) %>%
#   summarise(opening = sum(opening, na.rm = TRUE),
#             stock = sum(stock, na.rm = TRUE),
#             closing = sum(closing, na.rm = TRUE)) %>%
#   mutate(lopening = log1p(opening),
#          lclosing = log1p(closing),
#          lstock = log1p(stock)) %>%
#   ungroup()
# digi_before <- dat_before %>%
#   filter(bank_type == 2)
# trad_before <- dat_before %>%
#   filter(bank_type == 1)
# rm(dat_before)

print_twfe("CCS_Muni_IF_PJ_balanced_","lstock","constant","constant","flood_risk5","Log Number of Bank Accounts",list(trad_after, digi_after), c("Traditional","Digital"),  xl_balanced, xu_balanced)
#print_twfe("CCS_Muni_IF_PJ_b_balanced_","lstock","pre","internet_access","constant","Log Number of Bank Accounts",list(trad_before, digi_before), c("Traditional","Digital"), -26,26)
#print_twfe("CCS_Muni_IF_PJ_trad_balanced_","lstock","pre","internet_access","constant","Log Number of Bank Accounts",list(trad_before, trad_after), c("Before Pix","After Pix"), -26,26)
#print_twfe("CCS_Muni_IF_PJ_digi_balanced_","lstock","pre","internet_access","constant","Log Number of Bank Accounts",list(digi_before, digi_after), c("Before Pix","After Pix"), -26,26)

rm(digi_after, trad_after)
#rm(digi_before, trad_before)

# Now, lets do covid_balanced - NAO FAZ MUITO SENTIDO, MAS VAMOS LA

# PEOPLE
dat_after <- prepare_data("CCS_Muni_IF_PF.dta",flood_week_after_balanced_covid,mun_fe,mun_control, xl_balanced_covid, xu_balanced_covid)
dat_after <- merge(dat_after, Cadastro_IF, by="bank", all = FALSE)
dat_after <- dat_after %>%
  select(-tipo_inst, -bank, -lopening, -lstock, -lclosing) %>%
  group_by_at(vars(-opening, -stock, -closing)) %>%
  summarise(opening = sum(opening, na.rm = TRUE),
            stock = sum(stock, na.rm = TRUE),
            closing = sum(closing, na.rm = TRUE)) %>%
  mutate(lopening = log1p(opening),
         lclosing = log1p(closing),
         lstock = log1p(stock)) %>%
  ungroup()

digi_after <- dat_after %>%
  filter(bank_type == 2)
trad_after <- dat_after %>%
  filter(bank_type == 1)
rm(dat_after)

print_twfe("CCS_Muni_IF_PF_balanced_covid_","lstock","constant","constant","flood_risk5","Log Number of Bank Accounts",list(trad_after, digi_after), c("Traditional","Digital"),  xl_balanced_covid, xu_balanced_covid)
rm(digi_after, trad_after)

# FIRMS
dat_after <- prepare_data("CCS_Muni_IF_PJ.dta",flood_week_after_balanced_covid,mun_fe,mun_control, xl_balanced, xu_balanced)
dat_after <- merge(dat_after, Cadastro_IF, by="bank", all = FALSE)
dat_after <- dat_after %>%
  select(-tipo_inst, -bank, -lopening, -lstock, -lclosing) %>%
  group_by_at(vars(-opening, -stock, -closing)) %>%
  summarise(opening = sum(opening, na.rm = TRUE),
            stock = sum(stock, na.rm = TRUE),
            closing = sum(closing, na.rm = TRUE)) %>%
  mutate(lopening = log1p(opening),
         lclosing = log1p(closing),
         lstock = log1p(stock)) %>%
  ungroup()

digi_after <- dat_after %>%
  filter(bank_type == 2)
trad_after <- dat_after %>%
  filter(bank_type == 1)
rm(dat_after)

print_twfe("CCS_Muni_IF_PJ_balanced_covid_","lstock","constant","constant","flood_risk5","Log Number of Bank Accounts",list(trad_after, digi_after), c("Traditional","Digital"),  xl_balanced_covid, xu_balanced_covid)
rm(digi_after, trad_after)


}, error = function(e) {
  # Handle errors or print a message
  print(paste("Error in CCS_Muni_IF:", e))
})

# ------------------------------------------------------------------------------
# CCS_HHI
# ------------------------------------------------------------------------------

# worked 
tryCatch({
  #filename2 <- c("CCS_Muni_HHI_PF", "CCS_Muni_HHI_PJ")
  # Variables: week, muni_cd, tipo, HHI_account
  dat_HHI_after_PF <- prepare_data("CCS_Muni_HHI_PF.dta",flood_week_after,mun_fe,mun_control,xl, xu)
  dat_HHI_before_PF <- prepare_data("CCS_Muni_HHI_PF.dta",flood_week_before2019,mun_fe,mun_control,xl, xu)
  dat_HHI_after_PJ <- prepare_data("CCS_Muni_HHI_PJ.dta",flood_week_after,mun_fe,mun_control,xl, xu)
  dat_HHI_before_PJ <- prepare_data("CCS_Muni_HHI_PJ.dta",flood_week_before2019,mun_fe,mun_control,xl, xu)
  
  print_twfe("CCS_Muni_PF_","HHI_account","constant","constant","flood_risk5","HHI Bank Accounts",list(dat_HHI_before_PF, dat_HHI_after_PF), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
  print_twfe("CCS_Muni_PJ_","HHI_account","constant","constant","flood_risk5","HHI Bank Accounts",list(dat_HHI_before_PJ, dat_HHI_after_PJ), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
  rm(dat_HHI_after_PF,dat_HHI_before_PF,dat_HHI_after_PJ,dat_HHI_before_PJ)
  
  # Now with balanced data:
  dat_HHI_after_PF <- prepare_data("CCS_Muni_HHI_PF.dta",flood_week_after_balanced,mun_fe,mun_control,xl_balanced, xu_balanced)
  dat_HHI_before_PF <- prepare_data("CCS_Muni_HHI_PF.dta",flood_week_before_balanced2019,mun_fe,mun_control,xl_balanced, xu_balanced)
  dat_HHI_after_PJ <- prepare_data("CCS_Muni_HHI_PJ.dta",flood_week_after_balanced,mun_fe,mun_control,xl_balanced, xu_balanced)
  dat_HHI_before_PJ <- prepare_data("CCS_Muni_HHI_PJ.dta",flood_week_before_balanced2019,mun_fe,mun_control,xl_balanced, xu_balanced)
  
  print_twfe("CCS_Muni_PF_balanced_","HHI_account","constant","constant","flood_risk5","HHI Bank Accounts",list(dat_HHI_before_PF, dat_HHI_after_PF), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
  print_twfe("CCS_Muni_PJ_balanced_","HHI_account","constant","constant","flood_risk5","HHI Bank Accounts",list(dat_HHI_before_PJ, dat_HHI_after_PJ), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
  rm(dat_HHI_after_PF,dat_HHI_before_PF,dat_HHI_after_PJ,dat_HHI_before_PJ)
  
  # Now with covid_balanced data:
  dat_HHI_after_PF <- prepare_data("CCS_Muni_HHI_PF.dta",flood_week_after_balanced_covid,mun_fe,mun_control,xl_balanced_covid, xu_balanced_covid)
  dat_HHI_before_PF <- prepare_data("CCS_Muni_HHI_PF.dta",flood_week_before_balanced_covid,mun_fe,mun_control,xl_balanced_covid, xu_balanced_covid)
  dat_HHI_after_PJ <- prepare_data("CCS_Muni_HHI_PJ.dta",flood_week_after_balanced_covid,mun_fe,mun_control,xl_balanced_covid, xu_balanced_covid)
  dat_HHI_before_PJ <- prepare_data("CCS_Muni_HHI_PJ.dta",flood_week_before_balanced_covid,mun_fe,mun_control,xl_balanced_covid, xu_balanced_covid)
  
  print_twfe("CCS_Muni_PF_balanced_covid_","HHI_account","constant","constant","flood_risk5","HHI Bank Accounts",list(dat_HHI_before_PF, dat_HHI_after_PF), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
  print_twfe("CCS_Muni_PJ_balanced_covid_","HHI_account","constant","constant","flood_risk5","HHI Bank Accounts",list(dat_HHI_before_PJ, dat_HHI_after_PJ), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
  rm(dat_HHI_after_PF,dat_HHI_before_PF,dat_HHI_after_PJ,dat_HHI_before_PJ)
}, error = function(e) {
  print(paste("Error in CCS_HHI:", e))
})

# ------------------------------------------------------------------------------
# CCS_first_account
# ------------------------------------------------------------------------------

# Worked!
# Done!

tryCatch({
  #write_dta(data, paste0(dta_path,"CCS_Muni_first_account",".dta"))
  # Variables: week, muni_cd, tipo, first_account
  #             lfirst_account
  
  dat_after <- prepare_data("CCS_Muni_first_account.dta",flood_week_after,mun_fe,mun_control,xl, xu)
  dat_before <- prepare_data("CCS_Muni_first_account.dta",flood_week_before2019,mun_fe,mun_control,xl, xu)
  dat_after_PF <- dat_after %>% filter(tipo==1)
  dat_before_PF <- dat_before %>% filter(tipo==1)
  dat_after_PJ <- dat_after %>% filter(tipo==2)
  dat_before_PJ <- dat_before %>% filter(tipo==2)
  # Before vs After
  print_twfe("CCS_Muni_PF_","lfirst_account","constant","constant","flood_risk5","Log Adoption of Bank Accounts",list(dat_before_PF, dat_after_PF), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
  print_twfe("CCS_Muni_PJ_","lfirst_account","constant","constant","flood_risk5","Log Adoption of Bank Accounts",list(dat_before_PJ, dat_after_PJ), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
  print_twfe("CCS_Muni_PF_","first_account","constant","constant","flood_risk5","Adoption of Bank Accounts",list(dat_before_PF, dat_after_PF), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
  print_twfe("CCS_Muni_PJ_","first_account","constant","constant","flood_risk5","Adoption of Bank Accounts",list(dat_before_PJ, dat_after_PJ), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
  
  rm(dat_after,dat_before,dat_after_PF,dat_before_PF,dat_after_PJ,dat_before_PJ)
  
  # Now with balanced data:
  dat_after <- prepare_data("CCS_Muni_first_account.dta",flood_week_after_balanced,mun_fe,mun_control,xl_balanced, xu_balanced)
  dat_before <- prepare_data("CCS_Muni_first_account.dta",flood_week_before_balanced2019,mun_fe,mun_control,xl_balanced, xu_balanced)
  dat_after_PF <- dat_after %>% filter(tipo==1)
  dat_before_PF <- dat_before %>% filter(tipo==1)
  dat_after_PJ <- dat_after %>% filter(tipo==2)
  dat_before_PJ <- dat_before %>% filter(tipo==2)
  # Before vs After
  print_twfe("CCS_Muni_PF_balanced_","lfirst_account","constant","constant","flood_risk5","Log Adoption of Bank Accounts",list(dat_before_PF, dat_after_PF), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
  print_twfe("CCS_Muni_PJ_balanced_","lfirst_account","constant","constant","flood_risk5","Log Adoption of Bank Accounts",list(dat_before_PJ, dat_after_PJ), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
  print_twfe("CCS_Muni_PF_balanced_","first_account","constant","constant","flood_risk5","Adoption of Bank Accounts",list(dat_before_PF, dat_after_PF), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
  print_twfe("CCS_Muni_PJ_balanced_","first_account","constant","constant","flood_risk5","Adoption of Bank Accounts",list(dat_before_PJ, dat_after_PJ), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
  rm(dat_after,dat_before,dat_after_PF,dat_before_PF,dat_after_PJ,dat_before_PJ)
  
  # Now with covid_balanced data:
  dat_after <- prepare_data("CCS_Muni_first_account.dta",flood_week_after_balanced_covid,mun_fe,mun_control,xl_balanced_covid, xu_balanced_covid)
  dat_before <- prepare_data("CCS_Muni_first_account.dta",flood_week_before_balanced_covid,mun_fe,mun_control,xl_balanced_covid, xu_balanced_covid)
  dat_after_PF <- dat_after %>% filter(tipo==1)
  dat_before_PF <- dat_before %>% filter(tipo==1)
  dat_after_PJ <- dat_after %>% filter(tipo==2)
  dat_before_PJ <- dat_before %>% filter(tipo==2)
  # Before vs After
  print_twfe("CCS_Muni_PF_balanced_covid_","lfirst_account","constant","constant","flood_risk5","Log Adoption of Bank Accounts",list(dat_before_PF, dat_after_PF), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
  print_twfe("CCS_Muni_PJ_balanced_covid_","lfirst_account","constant","constant","flood_risk5","Log Adoption of Bank Accounts",list(dat_before_PJ, dat_after_PJ), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
  print_twfe("CCS_Muni_PF_balanced_covid_","first_account","constant","constant","flood_risk5","Adoption of Bank Accounts",list(dat_before_PF, dat_after_PF), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
  print_twfe("CCS_Muni_PJ_balanced_covid_","first_account","constant","constant","flood_risk5","Adoption of Bank Accounts",list(dat_before_PJ, dat_after_PJ), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
  rm(dat_after,dat_before,dat_after_PF,dat_before_PF,dat_after_PJ,dat_before_PJ)
  
}, error = function(e) {
  # Handle errors or print a message
  print(paste("Error in CCS_first_account:", e))
})



# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# Pix_Muni_Bank
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------

tryCatch({
# Variables: tipo_inst, week, muni_cd, tipo, bank, 
#             value_send, trans_send, send_users, value_send_w, value_rec, trans_rec, rec_users, value_rec_w
#             lvalue_send, ltrans_send, lsend_users, lvalue_send_w, lvalue_rec, ltrans_rec, lrec_users, lvalue_rec_w

dat <- prepare_data("Pix_Muni_Bank.dta",flood_week_after,mun_fe,mun_control,xl, xu)
cat("number of rows for Pix_Muni_Bank.dta:", nrow(dat))
dat <- merge(dat, Cadastro_IF, by="bank", all = FALSE)
cat("number of rows for Pix_Muni_Bank.dta after merge:", nrow(dat))

dat <- dat %>%
  select(-tipo_inst, -bank, -lvalue_send, -ltrans_send, -lsend_users, -lvalue_send_w, -lvalue_rec, -ltrans_rec, -lrec_users, -lvalue_rec_w) %>%
  group_by_at(vars(-value_send, -trans_send, -send_users, -value_send_w, -value_rec, -trans_rec, -rec_users, -value_rec_w)) %>%
  summarise(value_send_w = sum(value_send_w),
            trans_send = sum(trans_send),
            send_users = sum(send_users),
            value_rec_w = sum(value_rec_w),
            trans_rec = sum(trans_rec),
            rec_users = sum(rec_users),
            value_send = sum(value_send),
            value_rec = sum(value_rec)) %>%
  mutate(lvalue_send_w = log1p(value_send_w),
         ltrans_send = log1p(trans_send),
         lsend_users = log1p(send_users),
         lvalue_rec_w = log1p(value_rec_w),
         ltrans_rec = log1p(trans_rec),
         lrec_users = log1p(rec_users),
         lvalue_send = log1p(value_send),
         lvalue_rec = log1p(value_rec)) %>%
  ungroup()
# PEOPLE
trad <- dat %>%
  filter(bank_type == 1, tipo == 1) %>%
  mutate(value_w = value_rec_w + value_send_w,
         trans = trans_rec + trans_send,
         users = rec_users + send_users) %>%
  mutate(lvalue_w = log1p(value_w),
         ltrans = log1p(trans),
         lusers = log1p(users))
digi <- dat %>%
  filter(bank_type == 2, tipo == 1) %>%
  mutate(value_w = value_rec_w + value_send_w,
         trans = trans_rec + trans_send,
         users = rec_users + send_users) %>%
  mutate(lvalue_w = log1p(value_w),
         ltrans = log1p(trans),
         lusers = log1p(users))

print_twfe("Pix_Muni_Bank_PF_","ltrans","constant","constant","flood_risk5","Log Transactions",list(trad,digi), c("Traditional", "Digital"), xl, xu)
print_twfe("Pix_Muni_Bank_PF_","lvalue_w","constant","constant","flood_risk5","Log Value",list(trad,digi), c("Traditional", "Digital"), xl, xu)
print_twfe("Pix_Muni_Bank_PF_","lusers","constant","constant","flood_risk5","Log Active Users",list(trad,digi), c("Traditional", "Digital"), xl, xu)
print_twfe("Pix_Muni_Bank_PF_","lrec_users","constant","constant","flood_risk5","Log Active Receivers",list(trad,digi), c("Traditional", "Digital"), xl, xu)
print_twfe("Pix_Muni_Bank_PF_","lsend_users","constant","constant","flood_risk5","Log Active Receivers",list(trad,digi), c("Traditional", "Digital"), xl, xu)
rm(trad, digi)
# FIRMS
trad <- dat %>%
  filter(bank_type == 1, tipo == 2) %>%
  mutate(value_w = value_rec_w + value_send_w,
         trans = trans_rec + trans_send,
         users = rec_users + send_users) %>%
  mutate(lvalue_w = log1p(value_w),
         ltrans = log1p(trans),
         lusers = log1p(users))
digi <- dat %>%
  filter(bank_type == 2, tipo == 2) %>%
  mutate(value_w = value_rec_w + value_send_w,
         trans = trans_rec + trans_send,
         users = rec_users + send_users) %>%
  mutate(lvalue_w = log1p(value_w),
         ltrans = log1p(trans),
         lusers = log1p(users))

print_twfe("Pix_Muni_Bank_PJ_","ltrans","constant","constant","flood_risk5","Log Transactions",list(trad,digi), c("Traditional", "Digital"), xl, xu)
print_twfe("Pix_Muni_Bank_PJ_","lvalue_w","constant","constant","flood_risk5","Log Value",list(trad,digi), c("Traditional", "Digital"), xl, xu)
print_twfe("Pix_Muni_Bank_PJ_","lusers","constant","constant","flood_risk5","Log Active Users",list(trad,digi), c("Traditional", "Digital"), xl, xu)
print_twfe("Pix_Muni_Bank_PJ_","lrec_users","constant","constant","flood_risk5","Log Active Receivers",list(trad,digi), c("Traditional", "Digital"), xl, xu)
print_twfe("Pix_Muni_Bank_PJ_","lsend_users","constant","constant","flood_risk5","Log Active Receivers",list(trad,digi), c("Traditional", "Digital"), xl, xu)
rm(trad, digi)

# SELF

# dat <- prepare_data("Pix_Muni_Bank_self.dta",flood_week_after,mun_fe,mun_control,xl, xu)
# cat("number of rows for Pix_Muni_Bank_self.dta:", nrow(dat))
# dat <- merge(dat, Cadastro_IF, by="bank", all = FALSE)
# cat("number of rows for Pix_Muni_Bank_self.dta after merge:", nrow(dat))
# 
# dat <- dat %>%
#   select(-tipo_inst, -bank, -lvalue_send, -ltrans_send, -lsend_users, -lvalue_send_w, -lvalue_rec, -ltrans_rec, -lrec_users, -lvalue_rec_w) %>%
#   group_by_at(vars(-value_send, -trans_send, -send_users, -value_send_w, -value_rec, -trans_rec, -rec_users, -value_rec_w)) %>%
#   summarise(value_send_w = sum(value_send_w),
#             trans_send = sum(trans_send),
#             send_users = sum(send_users),
#             value_rec_w = sum(value_rec_w),
#             trans_rec = sum(trans_rec),
#             rec_users = sum(rec_users),
#             value_send = sum(value_send),
#             value_rec = sum(value_rec)) %>%
#   mutate(lvalue_send_w = log1p(value_send_w),
#          ltrans_send = log1p(trans_send),
#          lsend_users = log1p(send_users),
#          lvalue_rec_w = log1p(value_rec_w),
#          ltrans_rec = log1p(trans_rec),
#          lrec_users = log1p(rec_users),
#          lvalue_send = log1p(value_send),
#          lvalue_rec = log1p(value_rec)) %>%
#   ungroup()
# 
# trad_rec <- dat %>%
#   filter(bank_type == 1, tipo == 1) %>%
#   rename(lvalue_w = lvalue_rec_w,
#          ltrans = ltrans_rec,
#          lusers = lrec_users)
# digi_rec <- dat %>%
#   filter(bank_type == 2, tipo == 1) %>%
#   rename(lvalue_w = lvalue_rec_w,
#          ltrans = ltrans_rec,
#          lusers = lrec_users)
# trad_sent <- dat %>%
#   filter(bank_type == 1, tipo == 1) %>%
#   rename(lvalue_w = lvalue_send_w,
#          ltrans = ltrans_send,
#          lusers = lsend_users)
# digi_sent <- dat %>%
#   filter(bank_type == 2, tipo == 1) %>%
#   rename(lvalue_w = lvalue_send_w,
#          ltrans = ltrans_send,
#          lusers = lsend_users)
# print_twfe("Pix_Muni_self_Bank_PF_","lusers","pre","internet_access","constant","Log Active Users",list(trad_rec,digi_rec,trad_sent,digi_sent), c("Traditional Receivers","Digital Receivers", "Traditional Senders", "Digital Senders"), xl, xu)
# print_twfe("Pix_Muni_self_Bank_PF_","ltrans","pre","internet_access","constant","Log Transactions",list(trad_rec,digi_rec,trad_sent,digi_sent), c("Traditional Receivers","Digital Receivers", "Traditional Senders", "Digital Senders"), xl, xu)
# print_twfe("Pix_Muni_self_Bank_PF_","lvalue_w","pre","internet_access","constant","Log Value",list(trad_rec,digi_rec,trad_sent,digi_sent), c("Traditional Receivers","Digital Receivers", "Traditional Senders", "Digital Senders"), xl, xu)
# rm(trad_rec, digi_rec, trad_sent, digi_sent)
# trad <- dat %>%
#   filter(bank_type == 1, tipo == 1) %>%
#   mutate(value_w = value_rec_w + value_send_w,
#          trans = trans_rec + trans_send) %>%
#   mutate(lvalue_w = log1p(value_w),
#          ltrans = log1p(trans))
# digi <- dat %>%
#   filter(bank_type == 2, tipo == 1) %>%
#   mutate(value_w = value_rec_w + value_send_w,
#          trans = trans_rec + trans_send) %>%
#   mutate(lvalue_w = log1p(value_w),
#          ltrans = log1p(trans))
# #print_twfe("Pix_Muni_self_Bank_PF_","ltrans","pre","internet_access","constant","Log Transactions",list(trad,digi), c("Traditional", "Digital"), xl, xu)
# #print_twfe("Pix_Muni_self_Bank_PF_","lvalue_w","pre","internet_access","constant","Log Value",list(trad,digi), c("Traditional", "Digital"), xl, xu)
# print_twfe("Pix_Muni_self_Bank_PF_","lrec_users","pre","internet_access","constant","Log Active Receivers",list(trad,digi), c("Traditional", "Digital"), xl, xu)
# print_twfe("Pix_Muni_self_Bank_PF_","lsend_users","pre","internet_access","constant","Log Active Receivers",list(trad,digi), c("Traditional", "Digital"), xl, xu)
# rm(trad, digi)
# # FIRMS
# trad_rec <- dat %>%
#   filter(bank_type == 1, tipo == 2) %>%
#   rename(lvalue_w = lvalue_rec_w,
#          ltrans = ltrans_rec,
#          lusers = lrec_users)
# digi_rec <- dat %>%
#   filter(bank_type == 2, tipo == 2) %>%
#   rename(lvalue_w = lvalue_rec_w,
#          ltrans = ltrans_rec,
#          lusers = lrec_users)
# trad_sent <- dat %>%
#   filter(bank_type == 1, tipo == 2) %>%
#   rename(lvalue_w = lvalue_send_w,
#          ltrans = ltrans_send,
#          lusers = lsend_users)
# digi_sent <- dat %>%
#   filter(bank_type == 2, tipo == 2) %>%
#   rename(lvalue_w = lvalue_send_w,
#          ltrans = ltrans_send,
#          lusers = lsend_users)
# print_twfe("Pix_Muni_Bank_self_PJ_","lusers","pre","internet_access","constant","Log Active Users",list(trad_rec,digi_rec,trad_sent,digi_sent), c("Traditional Receivers","Digital Receivers", "Traditional Senders", "Digital Senders"), xl, xu)
# print_twfe("Pix_Muni_Bank_self_PJ_","ltrans","pre","internet_access","constant","Log Transactions",list(trad_rec,digi_rec,trad_sent,digi_sent), c("Traditional Receivers","Digital Receivers", "Traditional Senders", "Digital Senders"), xl, xu)
# print_twfe("Pix_Muni_Bank_self_PJ_","lvalue_w","pre","internet_access","constant","Log Value",list(trad_rec,digi_rec,trad_sent,digi_sent), c("Traditional Receivers","Digital Receivers", "Traditional Senders", "Digital Senders"), xl, xu)
# rm(trad_rec, digi_rec, trad_sent, digi_sent)
# trad <- dat %>%
#   filter(bank_type == 1, tipo == 2) %>%
#   mutate(value_w = value_rec_w + value_send_w,
#          trans = trans_rec + trans_send) %>%
#   mutate(lvalue_w = log1p(value_w),
#          ltrans = log1p(trans))
# digi <- dat %>%
#   filter(bank_type == 2, tipo == 2) %>%
#   mutate(value_w = value_rec_w + value_send_w,
#          trans = trans_rec + trans_send) %>%
#   mutate(lvalue_w = log1p(value_w),
#          ltrans = log1p(trans))
# #print_twfe("Pix_Muni_Bank_self_PJ_","ltrans","pre","internet_access","constant","Log Transactions",list(trad,digi), c("Traditional", "Digital"), xl, xu)
# #print_twfe("Pix_Muni_Bank_self_PJ_","lvalue_w","pre","internet_access","constant","Log Value",list(trad,digi), c("Traditional", "Digital"), xl, xu)
# print_twfe("Pix_Muni_Bank_self_PJ_","lrec_users","pre","internet_access","constant","Log Active Receivers",list(trad,digi), c("Traditional", "Digital"), xl, xu)
# print_twfe("Pix_Muni_Bank_self_PJ_","lsend_users","pre","internet_access","constant","Log Active Receivers",list(trad,digi), c("Traditional", "Digital"), xl, xu)
# rm(trad, digi)
# rm(dat)

# _balanced

dat <- prepare_data("Pix_Muni_Bank.dta",flood_week_after_balanced,mun_fe,mun_control,xl_balanced, xu_balanced)
dat <- merge(dat, Cadastro_IF, by="bank", all = FALSE)
dat <- dat %>%
  select(-tipo_inst, -bank, -lvalue_send, -ltrans_send, -lsend_users, -lvalue_send_w, -lvalue_rec, -ltrans_rec, -lrec_users, -lvalue_rec_w) %>%
  group_by_at(vars(-value_send, -trans_send, -send_users, -value_send_w, -value_rec, -trans_rec, -rec_users, -value_rec_w)) %>%
  summarise(value_send_w = sum(value_send_w),
            trans_send = sum(trans_send),
            send_users = sum(send_users),
            value_rec_w = sum(value_rec_w),
            trans_rec = sum(trans_rec),
            rec_users = sum(rec_users),
            value_send = sum(value_send),
            value_rec = sum(value_rec)) %>%
  mutate(lvalue_send_w = log1p(value_send_w),
         ltrans_send = log1p(trans_send),
         lsend_users = log1p(send_users),
         lvalue_rec_w = log1p(value_rec_w),
         ltrans_rec = log1p(trans_rec),
         lrec_users = log1p(rec_users),
         lvalue_send = log1p(value_send),
         lvalue_rec = log1p(value_rec)) %>%
  ungroup()
trad <- dat %>%
  filter(bank_type == 1, tipo == 1) %>%
  mutate(value_w = value_rec_w + value_send_w,
         trans = trans_rec + trans_send,
         users = rec_users + send_users) %>%
  mutate(lvalue_w = log1p(value_w),
         ltrans = log1p(trans),
         lusers = log1p(users))
digi <- dat %>%
  filter(bank_type == 2, tipo == 1) %>%
  mutate(value_w = value_rec_w + value_send_w,
         trans = trans_rec + trans_send,
         users = rec_users + send_users) %>%
  mutate(lvalue_w = log1p(value_w),
         ltrans = log1p(trans),
         lusers = log1p(users))
print_twfe("Pix_Muni_Bank_PF_balanced_","ltrans","constant","constant","flood_risk5","Log Transactions",list(trad,digi), c("Traditional", "Digital"), xl_balanced, xu_balanced)
print_twfe("Pix_Muni_Bank_PF_balanced_","lvalue_w","constant","constant","flood_risk5","Log Value",list(trad,digi), c("Traditional", "Digital"), xl_balanced, xu_balanced)
print_twfe("Pix_Muni_Bank_PF_balanced_","lusers","constant","constant","flood_risk5","Log Active Users",list(trad,digi), c("Traditional", "Digital"), xl_balanced, xu_balanced)
print_twfe("Pix_Muni_Bank_PF_balanced_","lrec_users","constant","constant","flood_risk5","Log Active Receivers",list(trad,digi), c("Traditional", "Digital"), xl_balanced, xu_balanced)
print_twfe("Pix_Muni_Bank_PF_balanced_","lsend_users","constant","constant","flood_risk5","Log Active Receivers",list(trad,digi), c("Traditional", "Digital"), xl_balanced, xu_balanced)
rm(trad, digi)
trad <- dat %>%
  filter(bank_type == 1, tipo == 2) %>%
  mutate(value_w = value_rec_w + value_send_w,
         trans = trans_rec + trans_send,
         users = rec_users + send_users) %>%
  mutate(lvalue_w = log1p(value_w),
         ltrans = log1p(trans),
         lusers = log1p(users))
digi <- dat %>%
  filter(bank_type == 2, tipo == 2) %>%
  mutate(value_w = value_rec_w + value_send_w,
         trans = trans_rec + trans_send,
         users = rec_users + send_users) %>%
  mutate(lvalue_w = log1p(value_w),
         ltrans = log1p(trans),
         lusers = log1p(users))
print_twfe("Pix_Muni_Bank_PJ_balanced_","ltrans","constant","constant","flood_risk5","Log Transactions",list(trad,digi), c("Traditional", "Digital"), xl_balanced, xu_balanced)
print_twfe("Pix_Muni_Bank_PJ_balanced_","lvalue_w","constant","constant","flood_risk5","Log Value",list(trad,digi), c("Traditional", "Digital"), xl_balanced, xu_balanced)
print_twfe("Pix_Muni_Bank_PJ_balanced_","lusers","constant","constant","flood_risk5","Log Active Users",list(trad,digi), c("Traditional", "Digital"), xl_balanced, xu_balanced)
print_twfe("Pix_Muni_Bank_PJ_balanced_","lrec_users","constant","constant","flood_risk5","Log Active Receivers",list(trad,digi), c("Traditional", "Digital"), xl_balanced, xu_balanced)
print_twfe("Pix_Muni_Bank_PJ_balanced_","lsend_users","constant","constant","flood_risk5","Log Active Receivers",list(trad,digi), c("Traditional", "Digital"), xl_balanced, xu_balanced)
rm(trad, digi)
# SELF
# dat <- prepare_data("Pix_Muni_Bank_self.dta",flood_week_after_balanced,mun_fe,mun_control,-26, 26)
# dat <- merge(dat, Cadastro_IF, by="bank", all = FALSE)
# dat <- dat %>%
#   select(-tipo_inst, -bank, -lvalue_send, -ltrans_send, -lsend_users, -lvalue_send_w, -lvalue_rec, -ltrans_rec, -lrec_users, -lvalue_rec_w) %>%
#   group_by_at(vars(-value_send, -trans_send, -send_users, -value_send_w, -value_rec, -trans_rec, -rec_users, -value_rec_w)) %>%
#   summarise(value_send_w = sum(value_send_w),
#             trans_send = sum(trans_send),
#             send_users = sum(send_users),
#             value_rec_w = sum(value_rec_w),
#             trans_rec = sum(trans_rec),
#             rec_users = sum(rec_users),
#             value_send = sum(value_send),
#             value_rec = sum(value_rec)) %>%
#   mutate(lvalue_send_w = log1p(value_send_w),
#          ltrans_send = log1p(trans_send),
#          lsend_users = log1p(send_users),
#          lvalue_rec_w = log1p(value_rec_w),
#          ltrans_rec = log1p(trans_rec),
#          lrec_users = log1p(rec_users),
#          lvalue_send = log1p(value_send),
#          lvalue_rec = log1p(value_rec)) %>%
#   ungroup()
# trad_rec <- dat %>%
#   filter(bank_type == 1, tipo == 1) %>%
#   rename(lvalue_w = lvalue_rec_w,
#          ltrans = ltrans_rec,
#          lusers = lrec_users)
# digi_rec <- dat %>%
#   filter(bank_type == 2, tipo == 1) %>%
#   rename(lvalue_w = lvalue_rec_w,
#          ltrans = ltrans_rec,
#          lusers = lrec_users)
# trad_sent <- dat %>%
#   filter(bank_type == 1, tipo == 1) %>%
#   rename(lvalue_w = lvalue_send_w,
#          ltrans = ltrans_send,
#          lusers = lsend_users)
# digi_sent <- dat %>%
#   filter(bank_type == 2, tipo == 1) %>%
#   rename(lvalue_w = lvalue_send_w,
#          ltrans = ltrans_send,
#          lusers = lsend_users)
# print_twfe("Pix_Muni_self_Bank_PF_balanced_","lusers","pre","internet_access","constant","Log Active Users",list(trad_rec,digi_rec,trad_sent,digi_sent), c("Traditional Receivers","Digital Receivers", "Traditional Senders", "Digital Senders"), -26, 26)
# print_twfe("Pix_Muni_self_Bank_PF_balanced_","ltrans","pre","internet_access","constant","Log Transactions",list(trad_rec,digi_rec,trad_sent,digi_sent), c("Traditional Receivers","Digital Receivers", "Traditional Senders", "Digital Senders"), -26, 26)
# print_twfe("Pix_Muni_self_Bank_PF_balanced_","lvalue_w","pre","internet_access","constant","Log Value",list(trad_rec,digi_rec,trad_sent,digi_sent), c("Traditional Receivers","Digital Receivers", "Traditional Senders", "Digital Senders"), -26, 26)
# rm(trad_rec, digi_rec, trad_sent, digi_sent)
# trad <- dat %>%
#   filter(bank_type == 1, tipo == 1) %>%
#   mutate(value_w = value_rec_w + value_send_w,
#          trans = trans_rec + trans_send) %>%
#   mutate(lvalue_w = log1p(value_w),
#          ltrans = log1p(trans))
# digi <- dat %>%
#   filter(bank_type == 2, tipo == 1) %>%
#   mutate(value_w = value_rec_w + value_send_w,
#          trans = trans_rec + trans_send) %>%
#   mutate(lvalue_w = log1p(value_w),
#          ltrans = log1p(trans))
# print_twfe("Pix_Muni_self_Bank_PF_balanced_","lrec_users","pre","internet_access","constant","Log Active Receivers",list(trad,digi), c("Traditional", "Digital"), -26, 26)
# print_twfe("Pix_Muni_self_Bank_PF_balanced_","lsend_users","pre","internet_access","constant","Log Active Receivers",list(trad,digi), c("Traditional", "Digital"), -26, 26)
# rm(trad, digi)
# trad_rec <- dat %>%
#   filter(bank_type == 1, tipo == 2) %>%
#   rename(lvalue_w = lvalue_rec_w,
#          ltrans = ltrans_rec,
#          lusers = lrec_users)
# digi_rec <- dat %>%
#   filter(bank_type == 2, tipo == 2) %>%
#   rename(lvalue_w = lvalue_rec_w,
#          ltrans = ltrans_rec,
#          lusers = lrec_users)
# trad_sent <- dat %>%
#   filter(bank_type == 1, tipo == 2) %>%
#   rename(lvalue_w = lvalue_send_w,
#          ltrans = ltrans_send,
#          lusers = lsend_users)
# digi_sent <- dat %>%
#   filter(bank_type == 2, tipo == 2) %>%
#   rename(lvalue_w = lvalue_send_w,
#          ltrans = ltrans_send,
#          lusers = lsend_users)
# print_twfe("Pix_Muni_Bank_self_PJ_balanced_","lusers","pre","internet_access","constant","Log Active Users",list(trad_rec,digi_rec,trad_sent,digi_sent), c("Traditional Receivers","Digital Receivers", "Traditional Senders", "Digital Senders"), -26, 26)
# print_twfe("Pix_Muni_Bank_self_PJ_balanced_","ltrans","pre","internet_access","constant","Log Transactions",list(trad_rec,digi_rec,trad_sent,digi_sent), c("Traditional Receivers","Digital Receivers", "Traditional Senders", "Digital Senders"), -26, 26)
# print_twfe("Pix_Muni_Bank_self_PJ_balanced_","lvalue_w","pre","internet_access","constant","Log Value",list(trad_rec,digi_rec,trad_sent,digi_sent), c("Traditional Receivers","Digital Receivers", "Traditional Senders", "Digital Senders"), -26, 26)
# rm(trad_rec, digi_rec, trad_sent, digi_sent)
# trad <- dat %>%
#   filter(bank_type == 1, tipo == 2) %>%
#   mutate(value_w = value_rec_w + value_send_w,
#          trans = trans_rec + trans_send) %>%
#   mutate(lvalue_w = log1p(value_w),
#          ltrans = log1p(trans))
# digi <- dat %>%
#   filter(bank_type == 2, tipo == 2) %>%
#   mutate(value_w = value_rec_w + value_send_w,
#          trans = trans_rec + trans_send) %>%
#   mutate(lvalue_w = log1p(value_w),
#          ltrans = log1p(trans))
# print_twfe("Pix_Muni_Bank_self_PJ_balanced_","lrec_users","pre","internet_access","constant","Log Active Receivers",list(trad,digi), c("Traditional", "Digital"), -26, 26)
# print_twfe("Pix_Muni_Bank_self_PJ_balanced_","lsend_users","pre","internet_access","constant","Log Active Receivers",list(trad,digi), c("Traditional", "Digital"), -26, 26)
# rm(trad, digi)

# Now with balanced_covid
dat <- prepare_data("Pix_Muni_Bank.dta",flood_week_after_balanced_covid,mun_fe,mun_control,xl_balanced_covid, xu_balanced_covid)
dat <- merge(dat, Cadastro_IF, by="bank", all = FALSE)
dat <- dat %>%
  select(-tipo_inst, -bank, -lvalue_send, -ltrans_send, -lsend_users, -lvalue_send_w, -lvalue_rec, -ltrans_rec, -lrec_users, -lvalue_rec_w) %>%
  group_by_at(vars(-value_send, -trans_send, -send_users, -value_send_w, -value_rec, -trans_rec, -rec_users, -value_rec_w)) %>%
  summarise(value_send_w = sum(value_send_w),
            trans_send = sum(trans_send),
            send_users = sum(send_users),
            value_rec_w = sum(value_rec_w),
            trans_rec = sum(trans_rec),
            rec_users = sum(rec_users),
            value_send = sum(value_send),
            value_rec = sum(value_rec)) %>%
  mutate(lvalue_send_w = log1p(value_send_w),
         ltrans_send = log1p(trans_send),
         lsend_users = log1p(send_users),
         lvalue_rec_w = log1p(value_rec_w),
         ltrans_rec = log1p(trans_rec),
         lrec_users = log1p(rec_users),
         lvalue_send = log1p(value_send),
         lvalue_rec = log1p(value_rec)) %>%
  ungroup()
trad <- dat %>%
  filter(bank_type == 1, tipo == 1) %>%
  mutate(value_w = value_rec_w + value_send_w,
         trans = trans_rec + trans_send,
         users = rec_users + send_users) %>%
  mutate(lvalue_w = log1p(value_w),
         ltrans = log1p(trans),
         lusers = log1p(users))
digi <- dat %>%
  filter(bank_type == 2, tipo == 1) %>%
  mutate(value_w = value_rec_w + value_send_w,
         trans = trans_rec + trans_send,
         users = rec_users + send_users) %>%
  mutate(lvalue_w = log1p(value_w),
         ltrans = log1p(trans),
         lusers = log1p(users))      
print_twfe("Pix_Muni_Bank_PF_balanced_covid_","ltrans","constant","constant","flood_risk5","Log Transactions",list(trad,digi), c("Traditional", "Digital"), xl_balanced_covid, xu_balanced_covid)
print_twfe("Pix_Muni_Bank_PF_balanced_covid_","lvalue_w","constant","constant","flood_risk5","Log Value",list(trad,digi), c("Traditional", "Digital"), xl_balanced_covid, xu_balanced_covid)
print_twfe("Pix_Muni_Bank_PF_balanced_covid_","lusers","constant","constant","flood_risk5","Log Active Users",list(trad,digi), c("Traditional", "Digital"), xl_balanced_covid, xu_balanced_covid)
print_twfe("Pix_Muni_Bank_PF_balanced_covid_","lrec_users","constant","constant","flood_risk5","Log Active Receivers",list(trad,digi), c("Traditional", "Digital"), xl_balanced_covid, xu_balanced_covid)
print_twfe("Pix_Muni_Bank_PF_balanced_covid_","lsend_users","constant","constant","flood_risk5","Log Active Receivers",list(trad,digi), c("Traditional", "Digital"), xl_balanced_covid, xu_balanced_covid)
rm(trad, digi)
trad <- dat %>%
  filter(bank_type == 1, tipo == 2) %>%
  mutate(value_w = value_rec_w + value_send_w,
         trans = trans_rec + trans_send,
         users = rec_users + send_users) %>%
  mutate(lvalue_w = log1p(value_w),
         ltrans = log1p(trans),
         lusers = log1p(users))
digi <- dat %>%
  filter(bank_type == 2, tipo == 2) %>%
  mutate(value_w = value_rec_w + value_send_w,
         trans = trans_rec + trans_send,
         users = rec_users + send_users) %>%
  mutate(lvalue_w = log1p(value_w),
         ltrans = log1p(trans),
         lusers = log1p(users))
print_twfe("Pix_Muni_Bank_PJ_balanced_covid_","ltrans","constant","constant","flood_risk5","Log Transactions",list(trad,digi), c("Traditional", "Digital"), xl_balanced_covid, xu_balanced_covid)
print_twfe("Pix_Muni_Bank_PJ_balanced_covid_","lvalue_w","constant","constant","flood_risk5","Log Value",list(trad,digi), c("Traditional", "Digital"), xl_balanced_covid, xu_balanced_covid)
print_twfe("Pix_Muni_Bank_PJ_balanced_covid_","lusers","constant","constant","flood_risk5","Log Active Users",list(trad,digi), c("Traditional", "Digital"), xl_balanced_covid, xu_balanced_covid)
print_twfe("Pix_Muni_Bank_PJ_balanced_covid_","lrec_users","constant","constant","flood_risk5","Log Active Receivers",list(trad,digi), c("Traditional", "Digital"), xl_balanced_covid, xu_balanced_covid)
print_twfe("Pix_Muni_Bank_PJ_balanced_covid_","lsend_users","constant","constant","flood_risk5","Log Active Receivers",list(trad,digi), c("Traditional", "Digital"), xl_balanced_covid, xu_balanced_covid)
rm(trad, digi)
rm(dat)
}, error = function(e) {
  print(paste("Error in Pix_Muni_Bank:", e))
})

# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# Pix_Muni_flow
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
tryCatch({
  #Send and Rec
  tryCatch({
    dat_rec <- prepare_data("Pix_Muni_flow_aggreg_rec.dta",flood_week_after,mun_fe,mun_control,xl, xu)
    # Variables: week, muni_cd, tipo, 
    #             trans, valor, valor_w
    #             ltrans, lvalor, lvalor_w
    dat_send <- prepare_data("Pix_Muni_flow_aggreg_send.dta",flood_week_after,mun_fe,mun_control,xl, xu)
    # Variables: week, muni_cd, tipo, 
    #             trans, valor, valor_w
    #             ltrans, lvalor, lvalor_w
    
    # Sent vs Rec
      # People
    dat_rec1 <- dat_rec %>%
      filter(tipo == 1)
    dat_send1 <- dat_send %>%
      filter(tipo == 1)
    print_twfe("Pix_Muni_PF_","ltrans","constant","constant","flood_risk5","Log Transactions",list(dat_rec1, dat_send1), c("Received","Sent"), xl, xu)
    print_twfe("Pix_Muni_PF_","lvalor_w","constant","constant","flood_risk5","Log Value",list(dat_rec1, dat_send1), c("Received","Sent"), xl, xu)
    rm(dat_rec1, dat_send1)
      # Firms
    dat_rec1 <- dat_rec %>%
      filter(tipo == 2)
    dat_send1 <- dat_send %>%
      filter(tipo == 2)
    print_twfe("Pix_Muni_PJ_","ltrans","constant","constant","flood_risk5","Log Transactions",list(dat_rec1, dat_send1), c("Received","Sent"), xl, xu)
    print_twfe("Pix_Muni_PJ_","lvalor_w","constant","constant","flood_risk5","Log Value",list(dat_rec1, dat_send1), c("Received","Sent"), xl, xu)
    rm(dat_rec1, dat_send1)
    rm(dat_rec, dat_send)
    
    # _balanced ###################
    dat_rec <- prepare_data("Pix_Muni_flow_aggreg_rec.dta",flood_week_after_balanced,mun_fe,mun_control,xl_balanced, xu_balanced)
    dat_send <- prepare_data("Pix_Muni_flow_aggreg_send.dta",flood_week_after_balanced,mun_fe,mun_control,xl_balanced, xu_balanced)
    dat_rec1 <- dat_rec %>%
      filter(tipo == 1)
    dat_send1 <- dat_send %>%
      filter(tipo == 1)
    print_twfe("Pix_Muni_PF_balanced_","ltrans","constant","constant","flood_risk5","Log Transactions",list(dat_rec1, dat_send1), c("Received","Sent"), xl_balanced, xu_balanced)
    print_twfe("Pix_Muni_PF_balanced_","lvalor_w","constant","constant","flood_risk5","Log Value",list(dat_rec1, dat_send1), c("Received","Sent"), xl_balanced, xu_balanced)
    rm(dat_rec1, dat_send1)
    dat_rec1 <- dat_rec %>%
      filter(tipo == 2)
    dat_send1 <- dat_send %>%
      filter(tipo == 2)
    print_twfe("Pix_Muni_PJ_balanced_","ltrans","constant","constant","flood_risk5","Log Transactions",list(dat_rec1, dat_send1), c("Received","Sent"), xl_balanced, xu_balanced)
    print_twfe("Pix_Muni_PJ_balanced_","lvalor_w","constant","constant","flood_risk5","Log Value",list(dat_rec1, dat_send1), c("Received","Sent"),xl_balanced, xu_balanced)
    rm(dat_rec1, dat_send1)
    rm(dat_rec, dat_send)
    
    # _balanced_covid ###################
    dat_rec <- prepare_data("Pix_Muni_flow_aggreg_rec.dta",flood_week_after_balanced_covid,mun_fe,mun_control,xl_balanced_covid, xu_balanced_covid)
    dat_send <- prepare_data("Pix_Muni_flow_aggreg_send.dta",flood_week_after_balanced_covid,mun_fe,mun_control,xl_balanced_covid, xu_balanced_covid)
    dat_rec1 <- dat_rec %>%
      filter(tipo == 1)
    dat_send1 <- dat_send %>%
      filter(tipo == 1)
    print_twfe("Pix_Muni_PF_balanced_covid_","ltrans","constant","constant","flood_risk5","Log Transactions",list(dat_rec1, dat_send1), c("Received","Sent"), xl_balanced_covid, xu_balanced_covid)
    print_twfe("Pix_Muni_PF_balanced_covid_","lvalor_w","constant","constant","flood_risk5","Log Value",list(dat_rec1, dat_send1), c("Received","Sent"), xl_balanced_covid, xu_balanced_covid)
    rm(dat_rec1, dat_send1)
    dat_rec1 <- dat_rec %>%
      filter(tipo == 2)
    dat_send1 <- dat_send %>%
      filter(tipo == 2)
    print_twfe("Pix_Muni_PJ_balanced_covid_","ltrans","constant","constant","flood_risk5","Log Transactions",list(dat_rec1, dat_send1), c("Received","Sent"), xl_balanced_covid, xu_balanced_covid)
    print_twfe("Pix_Muni_PJ_balanced_covid_","lvalor_w","constant","constant","flood_risk5","Log Value",list(dat_rec1, dat_send1), c("Received","Sent"),xl_balanced_covid, xu_balanced_covid)
    rm(dat_rec1, dat_send1)
    rm(dat_rec, dat_send)
    
  }, error = function(e) {
    print(paste("Error in Send Rec:", e))
  })

# Inflow vs Outflow + Self
  tryCatch({
    dat <- prepare_data("Pix_Muni_flow.dta",flood_week_after,mun_fe,mun_control,xl, xu)
    # Variables: flow_code, week, muni_cd, sender_type, receiver_type, senders, receivers, valor, trans, valor_w,
    #             lsenders, lreceivers, lvalor, ltrans, lvalor_w

    # Inflow/Outflow P2P, P2B, ...
    dat_inflow_p2p <- dat %>%
      filter(sender_type == 1, receiver_type == 1, flow_code == 1) %>%
      rename(lusers = lreceivers,
             lusers2 = lsenders)
    dat_outflow_p2p <- dat %>%
      filter(sender_type == 1, receiver_type == 1, flow_code == -1) %>%
      rename(lusers=lsenders,
             lusers2=lreceivers)
    print_twfe("Pix_Muni_flow_p2p_","lusers","constant","constant","flood_risk5","Log Active Users Inside the Municipaliy",list(dat_inflow_p2p, dat_outflow_p2p), c("Receiving","Sending"), xl, xu)
    print_twfe("Pix_Muni_flow_p2p_","lusers2","constant","constant","flood_risk5","Log Active Users Outside the Municipaliy",list(dat_outflow_p2p, dat_inflow_p2p), c("Receiving","Sending"), xl, xu)
    print_twfe("Pix_Muni_flow_p2p_","ltrans","constant","constant","flood_risk5","Log Transactions",list(dat_inflow_p2p, dat_outflow_p2p), c("Inflow","Outflow"), xl, xu)
    print_twfe("Pix_Muni_flow_p2p_","lvalor_w","constant","constant","flood_risk5","Log Value",list(dat_inflow_p2p, dat_outflow_p2p), c("Inflow","Outflow"), xl, xu)

    dat_inflow_b2b <- dat %>%
      filter(sender_type == 2, receiver_type == 2, flow_code == 1) %>%
      rename(lusers=lreceivers,
             lusers2=lsenders)
    dat_outflow_b2b <- dat %>%
      filter(sender_type == 2, receiver_type == 2, flow_code == -1) %>%
      rename(lusers=lsenders,
             lusers2=lreceivers)
    print_twfe("Pix_Muni_flow_b2b_","lusers","constant","constant","flood_risk5","Log Active Users Inside the Municipaliy",list(dat_inflow_b2b, dat_outflow_b2b), c("Receiving","Sending"), xl, xu)
    print_twfe("Pix_Muni_flow_b2b_","lusers2","constant","constant","flood_risk5","Log Active Users Outside the Municipaliy",list(dat_outflow_b2b, dat_inflow_b2b), c("Receiving","Sending"), xl, xu)
    print_twfe("Pix_Muni_flow_b2b_","ltrans","constant","constant","flood_risk5","Log Transactions",list(dat_inflow_b2b, dat_outflow_b2b), c("Inflow","Outflow"), xl, xu)
    print_twfe("Pix_Muni_flow_b2b_","lvalor_w","constant","constant","flood_risk5","Log Value",list(dat_inflow_b2b, dat_outflow_b2b), c("Inflow","Outflow"), xl, xu)
    rm(dat_inflow_p2p,dat_inflow_b2b,dat_outflow_p2p,dat_outflow_b2b)
    
    # # First, total inflow and total Outflow for people and then firms. 
    #   # People ------> SUM: THERE ARE b2P AND P2P HERE. 
    # dat_inflow <- dat %>%
    #   filter(receiver_type == 1, flow_code == 1) %>%
    #   select(-sender_type, -lsenders, -lreceivers, -lvalor, -ltrans, -lvalor_w) %>%
    #   group_by_at(vars(-senders, -receivers, -valor, -trans, -valor_w)) %>%
    #   summarise(senders = sum(senders, na.rm = TRUE), receivers = sum(receivers, na.rm = TRUE), valor = sum(valor, na.rm = TRUE), trans = sum(trans, na.rm = TRUE), valor_w = sum(valor_w, na.rm = TRUE)) %>%
    #   mutate(lsenders = log1p(senders), lreceivers = log1p(receivers), lvalor = log1p(valor), ltrans = log1p(trans), lvalor_w = log1p(valor_w)) %>%
    #   rename(lusers=lsenders) %>% ungroup()
    # dat_outflow <- dat %>%
    #   filter(sender_type == 1, flow_code == -1) %>%
    #   select(-receiver_type, -lsenders, -lreceivers, -lvalor, -ltrans, -lvalor_w) %>%
    #   group_by_at(vars(-senders, -receivers, -valor, -trans, -valor_w)) %>%
    #   summarise(senders = sum(senders, na.rm = TRUE), receivers = sum(receivers, na.rm = TRUE), valor = sum(valor, na.rm = TRUE), trans = sum(trans, na.rm = TRUE), valor_w = sum(valor_w, na.rm = TRUE)) %>%
    #   mutate(lsenders = log1p(senders), lreceivers = log1p(receivers), lvalor = log1p(valor), ltrans = log1p(trans), lvalor_w = log1p(valor_w)) %>%
    #   rename(lusers=lreceivers) %>% ungroup()
    # print_twfe("Pix_Muni_PF_flow_","ltrans","constant","constant","flood_risk5","Log Transactions",list(dat_inflow, dat_outflow), c("Inflow","Outflow"), xl, xu)
    # print_twfe("Pix_Muni_PF_flow_","lvalor_w","constant","constant","flood_risk5","Log Value",list(dat_inflow, dat_outflow), c("Inflow","Outflow"), xl, xu)
    # print_twfe("Pix_Muni_PF_flow_","lusers","constant","constant","flood_risk5","Log Active Users Outside the Municipaliy",list(dat_outflow, dat_inflow), c("Receiving","Sending"), xl, xu)
    # rm(dat_inflow, dat_outflow)
    # 
    #   # Firms
    # dat_inflow <- dat %>%
    #   filter(receiver_type == 2, flow_code == 1) %>%
    #   select(-sender_type, -lsenders, -lreceivers, -lvalor, -ltrans, -lvalor_w) %>%
    #   group_by_at(vars(-senders, -receivers, -valor, -trans, -valor_w)) %>%
    #   summarise(senders = sum(senders, na.rm = TRUE), receivers = sum(receivers, na.rm = TRUE), valor = sum(valor, na.rm = TRUE), trans = sum(trans, na.rm = TRUE), valor_w = sum(valor_w, na.rm = TRUE)) %>%
    #   mutate(lsenders = log1p(senders), lreceivers = log1p(receivers), lvalor = log1p(valor), ltrans = log1p(trans), lvalor_w = log1p(valor_w)) %>%
    #   rename(lusers=lsenders) %>% ungroup()
    # dat_outflow <- dat %>%
    #   filter(sender_type == 2, flow_code == -1) %>%
    #   select(-receiver_type, -lsenders, -lreceivers, -lvalor, -ltrans, -lvalor_w) %>%
    #   group_by_at(vars(-senders, -receivers, -valor, -trans, -valor_w)) %>%
    #   summarise(senders = sum(senders, na.rm = TRUE), receivers = sum(receivers, na.rm = TRUE), valor = sum(valor, na.rm = TRUE), trans = sum(trans, na.rm = TRUE), valor_w = sum(valor_w, na.rm = TRUE)) %>%
    #   mutate(lsenders = log1p(senders), lreceivers = log1p(receivers), lvalor = log1p(valor), ltrans = log1p(trans), lvalor_w = log1p(valor_w)) %>%
    #   rename(lusers=lreceivers) %>% ungroup()
    # print_twfe("Pix_Muni_PJ_flow_","ltrans","pre","internet_access","constant","Log Transactions",list(dat_inflow, dat_outflow), c("Inflow","Outflow"), xl, xu)
    # print_twfe("Pix_Muni_PJ_flow_","lvalor_w","pre","internet_access","constant","Log Value",list(dat_inflow, dat_outflow), c("Inflow","Outflow"), xl, xu)
    # print_twfe("Pix_Muni_PJ_flow_","lusers","pre","internet_access","constant","Log Active Users Outside the Municipaliy",list(dat_inflow, dat_outflow), c("Inflow","Outflow"), xl, xu)
    # rm(dat_inflow, dat_outflow)
    # 

    #### Self
    dat_self <- dat %>%
      filter(flow_code == 99)
    # Variables: flow_code, week, muni_cd, sender_type, receiver_type, senders, receivers, valor, trans, valor_w,
    #             lsenders, lreceivers, lvalor, ltrans, lvalor_w
    #  In self, only the receiver info is filled: receiver_type, receivers, lreceivers
    
    dat_self_PF <- dat_self %>%
      filter(receiver_type == 1)
    print_twfe("Pix_Muni_PF_self_","ltrans","constant","constant","flood_risk5","Log Transactions",list(dat_self_PF), c("Self"), xl, xu)
    print_twfe("Pix_Muni_PF_self_","lvalor_w","constant","constant","flood_risk5","Log Value",list(dat_self_PF), c("Self"), xl, xu)
    print_twfe("Pix_Muni_PF_self_","lreceivers","constant","constant","flood_risk5","Log Active Users",list(dat_self_PF), c("Self"), xl, xu)
    rm(dat_self_PF)
    
    dat_self_PJ <- dat_self %>%
      filter(receiver_type == 2)
    print_twfe("Pix_Muni_PJ_self_","ltrans","constant","constant","flood_risk5","Log Transactions",list(dat_self_PJ), c("Self"), xl, xu)
    print_twfe("Pix_Muni_PJ_self_","lvalor_w","constant","constant","flood_risk5","Log Value",list(dat_self_PJ), c("Self"), xl, xu)
    print_twfe("Pix_Muni_PJ_self_","lreceivers","constant","constant","flood_risk5","Log Active Users",list(dat_self_PJ), c("Self"), xl, xu)
    rm(dat_self_PJ)
    rm(dat_self)
    rm(dat)
    ####################################################################
    # _balanced
    dat <- prepare_data("Pix_Muni_flow.dta",flood_week_after_balanced,mun_fe,mun_control,xl_balanced, xu_balanced)
    # Inflow/Outflow P2P, P2B, ...
    dat_inflow_p2p <- dat %>%
      filter(sender_type == 1, receiver_type == 1, flow_code == 1) %>%
      rename(lusers = lreceivers,
             lusers2 = lsenders)
    dat_outflow_p2p <- dat %>%
      filter(sender_type == 1, receiver_type == 1, flow_code == -1) %>%
      rename(lusers=lsenders,
             lusers2=lreceivers)
    print_twfe("Pix_Muni_flow_p2p_balanced_","lusers","constant","constant","flood_risk5","Log Active Users Inside the Municipaliy",list(dat_inflow_p2p, dat_outflow_p2p), c("Receiving","Sending"), xl_balanced, xu_balanced)
    print_twfe("Pix_Muni_flow_p2p_balanced_","lusers2","constant","constant","flood_risk5","Log Active Users Outside the Municipaliy",list(dat_outflow_p2p, dat_inflow_p2p), c("Receiving","Sending"), xl_balanced, xu_balanced)
    print_twfe("Pix_Muni_flow_p2p_balanced_","ltrans","constant","constant","flood_risk5","Log Transactions",list(dat_inflow_p2p, dat_outflow_p2p), c("Inflow","Outflow"), xl_balanced, xu_balanced)
    print_twfe("Pix_Muni_flow_p2p_balanced_","lvalor_w","constant","constant","flood_risk5","Log Value",list(dat_inflow_p2p, dat_outflow_p2p), c("Inflow","Outflow"), xl_balanced, xu_balanced)
    rm(dat_inflow_p2p,dat_outflow_p2p)
    dat_inflow_b2b <- dat %>%
      filter(sender_type == 2, receiver_type == 2, flow_code == 1) %>%
      rename(lusers=lreceivers,
             lusers2=lsenders)
    dat_outflow_b2b <- dat %>%
      filter(sender_type == 2, receiver_type == 2, flow_code == -1) %>%
      rename(lusers=lsenders,
             lusers2=lreceivers)
    print_twfe("Pix_Muni_flow_b2b_balanced_","lusers","constant","constant","flood_risk5","Log Active Users Inside the Municipaliy",list(dat_inflow_b2b, dat_outflow_b2b), c("Receiving","Sending"), xl_balanced, xu_balanced)
    print_twfe("Pix_Muni_flow_b2b_balanced_","lusers2","constant","constant","flood_risk5","Log Active Users Outside the Municipaliy",list(dat_outflow_b2b, dat_inflow_b2b), c("Receiving","Sending"), xl_balanced, xu_balanced)
    print_twfe("Pix_Muni_flow_b2b_balanced_","ltrans","constant","constant","flood_risk5","Log Transactions",list(dat_inflow_b2b, dat_outflow_b2b), c("Inflow","Outflow"), xl_balanced, xu_balanced)
    print_twfe("Pix_Muni_flow_b2b_balanced_","lvalor_w","constant","constant","flood_risk5","Log Value",list(dat_inflow_b2b, dat_outflow_b2b), c("Inflow","Outflow"), xl_balanced, xu_balanced)
    rm(dat_inflow_b2b,dat_outflow_b2b)
    
    # Self _balanced
    dat_self <- dat %>%
      filter(flow_code == 99)
    dat_self_PF <- dat_self %>%
      filter(receiver_type == 1)
    print_twfe("Pix_Muni_PF_self_balanced_","ltrans","constant","constant","flood_risk5","Log Transactions",list(dat_self_PF), c("Self"), xl_balanced, xu_balanced)
    print_twfe("Pix_Muni_PF_self_balanced_","lvalor_w","constant","constant","flood_risk5","Log Value",list(dat_self_PF), c("Self"), xl_balanced, xu_balanced)
    print_twfe("Pix_Muni_PF_self_balanced_","lreceivers","constant","constant","flood_risk5","Log Active Users",list(dat_self_PF), c("Self"), xl_balanced, xu_balanced)
    rm(dat_self_PF)
    dat_self_PJ <- dat_self %>%
      filter(receiver_type == 2)
    print_twfe("Pix_Muni_PJ_self_balanced_","ltrans","constant","constant","flood_risk5","Log Transactions",list(dat_self_PJ), c("Self"), xl_balanced, xu_balanced)
    print_twfe("Pix_Muni_PJ_self_balanced_","lvalor_w","constant","constant","flood_risk5","Log Value",list(dat_self_PJ), c("Self"), xl_balanced, xu_balanced)
    print_twfe("Pix_Muni_PJ_self_balanced_","lreceivers","constant","constant","flood_risk5","Log Active Users",list(dat_self_PJ), c("Self"), xl_balanced, xu_balanced)
    rm(dat_self_PJ)
    rm(dat_self)
    rm(dat)
    
    # _balanced_covid
    dat <- prepare_data("Pix_Muni_flow.dta",flood_week_after_balanced_covid,mun_fe,mun_control,xl_balanced_covid, xu_balanced_covid)
    # Inflow/Outflow P2P, P2B, ...
    dat_inflow_p2p <- dat %>%
      filter(sender_type == 1, receiver_type == 1, flow_code == 1) %>%
      rename(lusers = lreceivers,
             lusers2 = lsenders)
    dat_outflow_p2p <- dat %>%
      filter(sender_type == 1, receiver_type == 1, flow_code == -1) %>%
      rename(lusers=lsenders,
             lusers2=lreceivers)
    print_twfe("Pix_Muni_flow_p2p_balanced_covid_","lusers","constant","constant","flood_risk5","Log Active Users Inside the Municipaliy",list(dat_inflow_p2p, dat_outflow_p2p), c("Receiving","Sending"), xl_balanced_covid, xu_balanced_covid)
    print_twfe("Pix_Muni_flow_p2p_balanced_covid_","lusers2","constant","constant","flood_risk5","Log Active Users Outside the Municipaliy",list(dat_outflow_p2p, dat_inflow_p2p), c("Receiving","Sending"), xl_balanced_covid, xu_balanced_covid)
    print_twfe("Pix_Muni_flow_p2p_balanced_covid_","ltrans","constant","constant","flood_risk5","Log Transactions",list(dat_inflow_p2p, dat_outflow_p2p), c("Inflow","Outflow"), xl_balanced_covid, xu_balanced_covid)
    print_twfe("Pix_Muni_flow_p2p_balanced_covid_","lvalor_w","constant","constant","flood_risk5","Log Value",list(dat_inflow_p2p, dat_outflow_p2p), c("Inflow","Outflow"), xl_balanced_covid, xu_balanced_covid)
    rm(dat_inflow_p2p,dat_outflow_p2p)
    dat_inflow_b2b <- dat %>%
      filter(sender_type == 2, receiver_type == 2, flow_code == 1) %>%
      rename(lusers=lreceivers,
             lusers2=lsenders)
    dat_outflow_b2b <- dat %>%
      filter(sender_type == 2, receiver_type == 2, flow_code == -1) %>%
      rename(lusers=lsenders,
             lusers2=lreceivers)
    print_twfe("Pix_Muni_flow_b2b_balanced_covid_","lusers","constant","constant","flood_risk5","Log Active Users Inside the Municipaliy",list(dat_inflow_b2b, dat_outflow_b2b), c("Receiving","Sending"), xl_balanced_covid, xu_balanced_covid)
    print_twfe("Pix_Muni_flow_b2b_balanced_covid_","lusers2","constant","constant","flood_risk5","Log Active Users Outside the Municipaliy",list(dat_outflow_b2b, dat_inflow_b2b), c("Receiving","Sending"), xl_balanced_covid, xu_balanced_covid)
    print_twfe("Pix_Muni_flow_b2b_balanced_covid_","ltrans","constant","constant","flood_risk5","Log Transactions",list(dat_inflow_b2b, dat_outflow_b2b), c("Inflow","Outflow"), xl_balanced_covid, xu_balanced_covid)
    print_twfe("Pix_Muni_flow_b2b_balanced_covid_","lvalor_w","constant","constant","flood_risk5","Log Value",list(dat_inflow_b2b, dat_outflow_b2b), c("Inflow","Outflow"), xl_balanced_covid, xu_balanced_covid)
    rm(dat_inflow_b2b,dat_outflow_b2b)
    
    # Self _balanced_covid
    dat_self <- dat %>%
      filter(flow_code == 99)
    dat_self_PF <- dat_self %>%
      filter(receiver_type == 1)
    print_twfe("Pix_Muni_PF_self_balanced_covid_","ltrans","constant","constant","flood_risk5","Log Transactions",list(dat_self_PF), c("Self"), xl_balanced_covid, xu_balanced_covid)
    print_twfe("Pix_Muni_PF_self_balanced_covid_","lvalor_w","constant","constant","flood_risk5","Log Value",list(dat_self_PF), c("Self"), xl_balanced_covid, xu_balanced_covid)
    print_twfe("Pix_Muni_PF_self_balanced_covid_","lreceivers","constant","constant","flood_risk5","Log Active Users",list(dat_self_PF), c("Self"), xl_balanced_covid, xu_balanced_covid)
    rm(dat_self_PF)
    dat_self_PJ <- dat_self %>%
      filter(receiver_type == 2)
    print_twfe("Pix_Muni_PJ_self_balanced_covid_","ltrans","constant","constant","flood_risk5","Log Transactions",list(dat_self_PJ), c("Self"), xl_balanced_covid, xu_balanced_covid)
    print_twfe("Pix_Muni_PJ_self_balanced_covid_","lvalor_w","constant","constant","flood_risk5","Log Value",list(dat_self_PJ), c("Self"), xl_balanced_covid, xu_balanced_covid)
    print_twfe("Pix_Muni_PJ_self_balanced_covid_","lreceivers","constant","constant","flood_risk5","Log Active Users",list(dat_self_PJ), c("Self"), xl_balanced_covid, xu_balanced_covid)
    rm(dat_self_PJ)
    rm(dat_self)
    rm(dat)
    
  }, error = function(e) {
    print(paste("Error in inflow and outflow:", e))
  })

# Total: P2P vs P2B vs B2P vs B2B - focus on sent variables since received variables are obvious (p2b sent = p2b rec)
  tryCatch({
    # dat_aggreg <- prepare_data("Pix_Muni_flow_aggreg.dta",flood_week_after,mun_fe,mun_control,xl, xu)
    # # Variables: week, muni_cd, sender_type, receiver_type, 
    # #             senders_rec, receivers_rec, valor_rec, trans_rec, valor_w_rec, 
    # #             senders_sent, receivers_sent, valor_sent, trans_sent, valor_w_sent
    # # Plus l variations. 
    # dat_p2p <- dat_aggreg %>%
    #   filter(sender_type == 1, receiver_type == 1) %>%
    #   rename(ltrans = ltrans_sent, lvalor_w = lvalor_w_sent, lsenders = lsenders_sent, lreceivers = lreceivers_rec)
    # dat_p2b <- dat_aggreg %>%
    #   filter(sender_type == 1, receiver_type == 2) %>%
    #   rename(ltrans = ltrans_sent, lvalor_w = lvalor_w_sent, lsenders = lsenders_sent, lreceivers = lreceivers_rec)
    # dat_b2p <- dat_aggreg %>%
    #   filter(sender_type == 2, receiver_type == 1) %>%
    #   rename(ltrans = ltrans_sent, lvalor_w = lvalor_w_sent, lsenders = lsenders_sent, lreceivers = lreceivers_rec)
    # dat_b2b <- dat_aggreg %>%
    #   filter(sender_type == 2, receiver_type == 2) %>%
    #   rename(ltrans = ltrans_sent, lvalor_w = lvalor_w_sent, lsenders = lsenders_sent, lreceivers = lreceivers_rec)
    # 
    # print_twfe("Pix_Muni_type_","ltrans","pre","internet_access","constant","Log Transactions",list(dat_p2p,dat_p2b,dat_b2p,dat_b2b), c("P2P","P2B", "B2P", "B2B"), xl, xu)
    # print_twfe("Pix_Muni_type_","lvalor_w","pre","internet_access","constant","Log Value",list(dat_p2p,dat_p2b,dat_b2p,dat_b2b), c("P2P","P2B", "B2P", "B2B"), xl, xu)
    # 
    # rm(dat_p2p,dat_p2b,dat_b2p,dat_b2b)
    # rm(dat_aggreg)
    # 
    # # Do balanced now! _balanced
    # 
    # dat_aggreg <- prepare_data("Pix_Muni_flow_aggreg.dta",flood_week_after_balanced,mun_fe,mun_control,-26, 26)
    # dat_p2p <- dat_aggreg %>%
    #   filter(sender_type == 1, receiver_type == 1) %>%
    #   rename(ltrans = ltrans_sent, lvalor_w = lvalor_w_sent, lsenders = lsenders_sent, lreceivers = lreceivers_rec)
    # dat_p2b <- dat_aggreg %>%
    #   filter(sender_type == 1, receiver_type == 2) %>%
    #   rename(ltrans = ltrans_sent, lvalor_w = lvalor_w_sent, lsenders = lsenders_sent, lreceivers = lreceivers_rec)
    # dat_b2p <- dat_aggreg %>%
    #   filter(sender_type == 2, receiver_type == 1) %>%
    #   rename(ltrans = ltrans_sent, lvalor_w = lvalor_w_sent, lsenders = lsenders_sent, lreceivers = lreceivers_rec)
    # dat_b2b <- dat_aggreg %>%
    #   filter(sender_type == 2, receiver_type == 2) %>%
    #   rename(ltrans = ltrans_sent, lvalor_w = lvalor_w_sent, lsenders = lsenders_sent, lreceivers = lreceivers_rec)
    # 
    # print_twfe("Pix_Muni_type_balanced_","ltrans","pre","internet_access","constant","Log Transactions",list(dat_p2p,dat_p2b,dat_b2p,dat_b2b), c("P2P","P2B", "B2P", "B2B"), -26, 26)
    # print_twfe("Pix_Muni_type_balanced_","lvalor_w","pre","internet_access","constant","Log Value",list(dat_p2p,dat_p2b,dat_b2p,dat_b2b), c("P2P","P2B", "B2P", "B2B"), -26, 26)
    # 
    # rm(dat_p2p,dat_p2b,dat_b2p,dat_b2b)
    # rm(dat_aggreg)
  }, error = function(e) {
    print(paste("Error in p2p, p2b, ...:", e))
  })

}, error = function(e) {
  print(paste("Error in Pix_Muni_flow:", e))
})

# ------------------------------------------------------------------------------
# Adoption_Pix
# ------------------------------------------------------------------------------
#filename <- c("adoption_ind")
#write_dta(data, paste0(dta_path,"adoption_ind",".dta"))
# Variables: dia, muni_cd, tipo, rec_adopters, send_adopters, self_adopters
#             lrec_adopters, lsend_adopters, lself_adopters

#Worked!
# Done!
# Ask Sean and Jacopo for ideas. On what to do with a lot of zeros in the data
# Maybe I can collapse by month. 

tryCatch({
  dat_after_PF <- prepare_data("adoption_ind.dta",flood_week_after,mun_fe,mun_control,xl, xu)
  rec_adopt <- dat_after_PF %>%
    rename(ladopt = lrec_adopters,
           adopt = rec_adopters)
  send_adopt <- dat_after_PF %>%
    rename(ladopt = lsend_adopters,
           adopt = send_adopters)
  self_adopt <- dat_after_PF %>%
    rename(ladopt = lself_adopters,
           adopt = self_adopters)
  # PF
  print_twfe("Pix_adoption_PF_","ladopt","constant","constant","flood_risk5","Log Pix Adoption",list(rec_adopt, send_adopt), c("Received","Sent"), xl, xu)
  print_twfe("Pix_adoption_PF_","adopt","constant","constant","flood_risk5","Pix Adoption",list(rec_adopt, send_adopt), c("Received","Sent"), xl, xu)
  print_twfe("Pix_adoption_PF_self_","ladopt","constant","constant","flood_risk5","Log Pix Adoption",list(self_adopt), c("Self"), xl, xu)
  print_twfe("Pix_adoption_PF_self_","adopt","constant","constant","flood_risk5","Pix Adoption",list(self_adopt), c("Self"), xl, xu)
  rm(dat_after_PF,rec_adopt,send_adopt,self_adopt)
  
  # now balanced
  
  dat_after_PF <- prepare_data("adoption_ind.dta",flood_week_after_balanced,mun_fe,mun_control,xl_balanced, xu_balanced)
  rec_adopt <- dat_after_PF %>%
    rename(ladopt = lrec_adopters,
           adopt = rec_adopters)
  send_adopt <- dat_after_PF %>%
    rename(ladopt = lsend_adopters,
           adopt = send_adopters)
  self_adopt <- dat_after_PF %>%
    rename(ladopt = lself_adopters,
           adopt = self_adopters)
  # PF
  print_twfe("Pix_adoption_PF_balanced_","ladopt","constant","constant","flood_risk5","Log Pix Adoption",list(rec_adopt, send_adopt), c("Received","Sent"), xl_balanced, xu_balanced)
  print_twfe("Pix_adoption_PF_balanced_","adopt","constant","constant","flood_risk5","Pix Adoption",list(rec_adopt, send_adopt), c("Received","Sent"), xl_balanced, xu_balanced)
  print_twfe("Pix_adoption_PF_self_balanced_","ladopt","constant","constant","flood_risk5","Log Pix Adoption",list(self_adopt), c("Self"), xl_balanced, xu_balanced)
  print_twfe("Pix_adoption_PF_self_balanced_","adopt","constant","constant","flood_risk5","Pix Adoption",list(self_adopt), c("Self"), xl_balanced, xu_balanced)
  rm(dat_after_PF,rec_adopt,send_adopt,self_adopt)
  
  # now balanced_covid
  
  dat_after_PF <- prepare_data("adoption_ind.dta",flood_week_after_balanced_covid,mun_fe,mun_control,xl_balanced_covid, xu_balanced_covid)
  rec_adopt <- dat_after_PF %>%
    rename(ladopt = lrec_adopters,
           adopt = rec_adopters)
  send_adopt <- dat_after_PF %>%
    rename(ladopt = lsend_adopters,
           adopt = send_adopters)
  self_adopt <- dat_after_PF %>%
    rename(ladopt = lself_adopters,
           adopt = self_adopters)
  # PF
  print_twfe("Pix_adoption_PF_balanced_covid_","ladopt","constant","constant","flood_risk5","Log Pix Adoption",list(rec_adopt, send_adopt), c("Received","Sent"), xl_balanced_covid, xu_balanced_covid)
  print_twfe("Pix_adoption_PF_balanced_covid_","adopt","constant","constant","flood_risk5","Pix Adoption",list(rec_adopt, send_adopt), c("Received","Sent"), xl_balanced_covid, xu_balanced_covid)
  print_twfe("Pix_adoption_PF_self_balanced_covid_","ladopt","constant","constant","flood_risk5","Log Pix Adoption",list(self_adopt), c("Self"), xl_balanced_covid, xu_balanced_covid)
  print_twfe("Pix_adoption_PF_self_balanced_covid_","adopt","constant","constant","flood_risk5","Pix Adoption",list(self_adopt), c("Self"), xl_balanced_covid, xu_balanced_covid)
  rm(dat_after_PF,rec_adopt,send_adopt,self_adopt)
  
}, error = function(e) {
  # Handle errors or print a message
  print(paste("Error in Adoption_Pix:", e))
})

# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# Pix_Muni_user
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------

# Variables: week, muni_cd, tipo, users
#             lusers
#expand later to send users, rec users. self users (already done)

tryCatch({
  dat <- prepare_data("Pix_Muni_user.dta",flood_week_after,mun_fe,mun_control,xl, xu)
  dat_PF <- dat %>%
    filter(tipo==1)
  dat_PJ <- dat %>%
    filter(tipo==2)
  print_twfe("Pix_Muni_user_PF_","lusers","constant","constant","flood_risk5","Log Active Users",list(dat_PF), c("Pix"), xl, xu)
  print_twfe("Pix_Muni_user_PJ_","lusers","constant","constant","flood_risk5","Log Active Users",list(dat_PJ), c("Pix"), xl, xu)
  rm(dat,dat_PF,dat_PJ)
  
  # Now with balanced data:
  dat <- prepare_data("Pix_Muni_user.dta",flood_week_after_balanced,mun_fe,mun_control,xl_balanced, xu_balanced)
  dat_PF <- dat %>%
    filter(tipo==1)
  dat_PJ <- dat %>%
    filter(tipo==2)
  print_twfe("Pix_Muni_user_PF_balanced_","lusers","constant","constant","flood_risk5","Log Active Users",list(dat_PF), c("Pix"), xl_balanced, xu_balanced)
  print_twfe("Pix_Muni_user_PJ_balanced_","lusers","constant","constant","flood_risk5","Log Active Users",list(dat_PJ), c("Pix"), xl_balanced, xu_balanced)
  rm(dat,dat_PF,dat_PJ)
  
  # Now with balanced_covid data:
  dat <- prepare_data("Pix_Muni_user.dta",flood_week_after_balanced_covid,mun_fe,mun_control,xl_balanced_covid, xu_balanced_covid)
  dat_PF <- dat %>%
    filter(tipo==1)
  dat_PJ <- dat %>%
    filter(tipo==2)
  print_twfe("Pix_Muni_user_PF_balanced_covid_","lusers","constant","constant","flood_risk5","Log Active Users",list(dat_PF), c("Pix"), xl_balanced_covid, xu_balanced_covid)
  print_twfe("Pix_Muni_user_PJ_balanced_covid_","lusers","constant","constant","flood_risk5","Log Active Users",list(dat_PJ), c("Pix"), xl_balanced_covid, xu_balanced_covid)
  rm(dat,dat_PF,dat_PJ)
  
}, error = function(e) {
  # Handle errors or print a message
  print(paste("Error in Pix_Muni_user:", e))
})



#-------------------------------------------------------------------------------
# Old Graphs redone. 
#-------------------------------------------------------------------------------
#Base_week_muni.dta
tryCatch({
dat_a <- prepare_data("Base_week_muni.dta",flood_week_after,mun_fe,mun_control,xl, xu)
dat_b <- prepare_data("Base_week_muni.dta",flood_week_before2019,mun_fe,mun_control,xl, xu)
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
# var_list <- c("valor_TED_intra","qtd_TED_intra","qtd_cli_TED_rec_PJ","qtd_cli_TED_pag_PJ","valor_boleto","qtd_boleto","qtd_cli_pag_pf_boleto","qtd_cli_pag_pj_boleto","qtd_cli_rec_pj_boleto","valor_cartao_credito","valor_cartao_debito","qtd_cli_cartao_debito","qtd_cli_cartao_credito")
# dat_b <- dat_b %>%
#   mutate(across(all_of(var_list), ~ log1p(.), .names ="log_{.col}"))

#TED
print_twfe("TED_","log_valor_TED_intra","constant","constant","flood_risk5","Log Value TED",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
print_twfe("TED_","log_qtd_TED_intra","constant","constant","flood_risk5","Log Transactions TED",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
print_twfe("TED_","log_qtd_cli_TED_rec_PJ","constant","constant","flood_risk5","Log Quantity of Firms Receiving TED",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
print_twfe("TED_","log_qtd_cli_TED_pag_PJ","constant","constant","flood_risk5","Log Quantity of Firms Sending TED",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
# + Uso da conta + "adocao"

#Boleto
print_twfe("Boleto_","log_valor_boleto","constant","constant","flood_risk5","Log Value Boleto",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
print_twfe("Boleto_","log_qtd_boleto","constant","constant","flood_risk5","Log Transactions Boleto",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
print_twfe("Boleto_","log_qtd_cli_pag_pf_boleto","constant","constant","flood_risk5","Log Quantity of People Sending Boleto",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
print_twfe("Boleto_","log_qtd_cli_pag_pj_boleto","constant","constant","flood_risk5","Log Quantity of Firms Sending Boleto",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
print_twfe("Boleto_","log_qtd_cli_rec_pj_boleto","constant","constant","flood_risk5","Log Quantity of Firms Receiving Boleto",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
# dat_a <- dat_a %>%
#   mutate(log_valor_boleto_eletronico = log1p(valor_boleto_eletronico),
#          log_valor_boleto_presencial = log1p(valor_boleto-valor_boleto_eletronico),
#          log_valor_boleto_dinheiro = log1p(valor_boleto_dinheiro),
#          log_qtd_boleto_eletronico = log1p(qtd_boleto_eletronico),
#          log_qtd_boleto_presencial = log1p(qtd_boleto-qtd_boleto_eletronico),
#          log_qtd_boleto_dinheiro = log1p(qtd_boleto_dinheiro))
# dat_b <- dat_b %>%
#   mutate(log_valor_boleto_eletronico = log1p(valor_boleto_eletronico),
#          log_valor_boleto_presencial = log1p(valor_boleto-valor_boleto_eletronico),
#          log_valor_boleto_dinheiro = log1p(valor_boleto_dinheiro),
#          log_qtd_boleto_eletronico = log1p(qtd_boleto_eletronico),
#          log_qtd_boleto_presencial = log1p(qtd_boleto-qtd_boleto_eletronico),
#          log_qtd_boleto_dinheiro = log1p(qtd_boleto_dinheiro))
# 
# print_twfe("Boleto_","log_qtd_boleto_eletronico","pre","internet_access","constant","Log Transactions Boleto - Eletronic",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
# print_twfe("Boleto_","log_qtd_boleto_presencial","pre","internet_access","constant","Log Transactions Boleto - In Person",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
# print_twfe("Boleto_","log_qtd_boleto_dinheiro","pre","internet_access","constant","Log Transactions Boleto - Money",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
# print_twfe("Boleto_","log_valor_boleto_eletronico","pre","internet_access","constant","Log Value Boleto - Eletronic",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
# print_twfe("Boleto_","log_valor_boleto_presencial","pre","internet_access","constant","Log Value Boleto - In Person",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
# print_twfe("Boleto_","log_valor_boleto_dinheiro","pre","internet_access","constant","Log Value Boleto - Money",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
# # + Uso da conta + "adocao"


#Cartao 
# * valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito 
print_twfe("Cartao_","log_valor_cartao_debito","constant","constant","flood_risk5","Log Value Debit Card",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
print_twfe("Cartao_","log_valor_cartao_credito","constant","constant","flood_risk5","Log Value Credit Card",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
print_twfe("Cartao_","log_qtd_cli_cartao_debito","constant","constant","flood_risk5","Log Quantity of Firms Accepting Debit Card",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
print_twfe("Cartao_","log_qtd_cli_cartao_credito","constant","constant","flood_risk5","Log Quantity of Firms Accepting Credit Card",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl, xu)
# + Adocao 
 
rm(dat_a, dat_b)

# _balanced

dat_a <- prepare_data("Base_week_muni.dta",flood_week_after_balanced,mun_fe,mun_control,xl_balanced, xu_balanced)
dat_b <- prepare_data("Base_week_muni.dta",flood_week_before_balanced2019,mun_fe,mun_control,xl_balanced, xu_balanced)
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
# var_list <- c("valor_TED_intra","qtd_TED_intra","qtd_cli_TED_rec_PJ","qtd_cli_TED_pag_PJ","valor_boleto","qtd_boleto","qtd_cli_pag_pf_boleto","qtd_cli_pag_pj_boleto","qtd_cli_rec_pj_boleto","valor_cartao_credito","valor_cartao_debito","qtd_cli_cartao_debito","qtd_cli_cartao_credito")
# dat_b <- dat_b %>%
#   mutate(across(all_of(var_list), ~ log1p(.), .names ="log_{.col}"))

#TED
print_twfe("TED_balanced_","log_valor_TED_intra","constant","constant","flood_risk5","Log Value TED",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
print_twfe("TED_balanced_","log_qtd_TED_intra","constant","constant","flood_risk5","Log Transactions TED",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
print_twfe("TED_balanced_","log_qtd_cli_TED_rec_PJ","constant","constant","flood_risk5","Log Quantity of Firms Receiving TED",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
print_twfe("TED_balanced_","log_qtd_cli_TED_pag_PJ","constant","constant","flood_risk5","Log Quantity of Firms Sending TED",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
# + Uso da conta + "adocao"

#Boleto
print_twfe("Boleto_balanced_","log_valor_boleto","constant","constant","flood_risk5","Log Value Boleto",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
print_twfe("Boleto_balanced_","log_qtd_boleto","constant","constant","flood_risk5","Log Transactions Boleto",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
print_twfe("Boleto_balanced_","log_qtd_cli_pag_pf_boleto","constant","constant","flood_risk5","Log Quantity of People Sending Boleto",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
print_twfe("Boleto_balanced_","log_qtd_cli_pag_pj_boleto","constant","constant","flood_risk5","Log Quantity of Firms Sending Boleto",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
print_twfe("Boleto_balanced_","log_qtd_cli_rec_pj_boleto","constant","constant","flood_risk5","Log Quantity of Firms Receiving Boleto",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
# dat_a <- dat_a %>%
#   mutate(log_valor_boleto_eletronico = log1p(valor_boleto_eletronico),
#          log_valor_boleto_presencial = log1p(valor_boleto-valor_boleto_eletronico),
#          log_valor_boleto_dinheiro = log1p(valor_boleto_dinheiro),
#          log_qtd_boleto_eletronico = log1p(qtd_boleto_eletronico),
#          log_qtd_boleto_presencial = log1p(qtd_boleto-qtd_boleto_eletronico),
#          log_qtd_boleto_dinheiro = log1p(qtd_boleto_dinheiro))
# dat_b <- dat_b %>%
#   mutate(log_valor_boleto_eletronico = log1p(valor_boleto_eletronico),
#          log_valor_boleto_presencial = log1p(valor_boleto-valor_boleto_eletronico),
#          log_valor_boleto_dinheiro = log1p(valor_boleto_dinheiro),
#          log_qtd_boleto_eletronico = log1p(qtd_boleto_eletronico),
#          log_qtd_boleto_presencial = log1p(qtd_boleto-qtd_boleto_eletronico),
#          log_qtd_boleto_dinheiro = log1p(qtd_boleto_dinheiro))
# 
# print_twfe("Boleto_balanced_","log_qtd_boleto_eletronico","constant","constant","flood_risk5","Log Transactions Boleto - Eletronic",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
# print_twfe("Boleto_balanced_","log_qtd_boleto_presencial","constant","constant","flood_risk5","Log Transactions Boleto - In Person",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
# print_twfe("Boleto_balanced_","log_qtd_boleto_dinheiro","constant","constant","flood_risk5","Log Transactions Boleto - Money",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
# print_twfe("Boleto_balanced_","log_valor_boleto_eletronico","constant","constant","flood_risk5","Log Value Boleto - Eletronic",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
# print_twfe("Boleto_balanced_","log_valor_boleto_presencial","constant","constant","flood_risk5","Log Value Boleto - In Person",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
# print_twfe("Boleto_balanced_","log_valor_boleto_dinheiro","constant","constant","flood_risk5","Log Value Boleto - Money",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
# # + Uso da conta + "adocao"

#Cartao 
# * valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito 
print_twfe("Cartao_balanced_","log_valor_cartao_debito","constant","constant","flood_risk5","Log Value Debit Card",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
print_twfe("Cartao_balanced_","log_valor_cartao_credito","constant","constant","flood_risk5","Log Value Credit Card",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
print_twfe("Cartao_balanced_","log_qtd_cli_cartao_debito","constant","constant","flood_risk5","Log Quantity of Firms Accepting Debit Card",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
print_twfe("Cartao_balanced_","log_qtd_cli_cartao_credito","constant","constant","flood_risk5","Log Quantity of Firms Accepting Credit Card",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), xl_balanced, xu_balanced)
# + Adocao 
rm(dat_a, dat_b)

# _balanced_covid

dat_a <- prepare_data("Base_week_muni.dta",flood_week_after_balanced_covid,mun_fe,mun_control,xl_balanced_covid, xu_balanced_covid)
dat_b <- prepare_data("Base_week_muni.dta",flood_week_before_balanced_covid,mun_fe,mun_control,xl_balanced_covid, xu_balanced_covid)
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
print_twfe("TED_balanced_covid_","log_valor_TED_intra","constant","constant","flood_risk5","Log Value TED",list(dat_b,dat_a), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
print_twfe("TED_balanced_covid_","log_qtd_TED_intra","constant","constant","flood_risk5","Log Transactions TED",list(dat_b,dat_a), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
print_twfe("TED_balanced_covid_","log_qtd_cli_TED_rec_PJ","constant","constant","flood_risk5","Log Quantity of Firms Receiving TED",list(dat_b,dat_a), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
print_twfe("TED_balanced_covid_","log_qtd_cli_TED_pag_PJ","constant","constant","flood_risk5","Log Quantity of Firms Sending TED",list(dat_b,dat_a), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
# + Uso da conta + "adocao"

#Boleto
print_twfe("Boleto_balanced_covid_","log_valor_boleto","constant","constant","flood_risk5","Log Value Boleto",list(dat_b,dat_a), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
print_twfe("Boleto_balanced_covid_","log_qtd_boleto","constant","constant","flood_risk5","Log Transactions Boleto",list(dat_b,dat_a), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
print_twfe("Boleto_balanced_covid_","log_qtd_cli_pag_pf_boleto","constant","constant","flood_risk5","Log Quantity of People Sending Boleto",list(dat_b,dat_a), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
print_twfe("Boleto_balanced_covid_","log_qtd_cli_pag_pj_boleto","constant","constant","flood_risk5","Log Quantity of Firms Sending Boleto",list(dat_b,dat_a), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
print_twfe("Boleto_balanced_covid_","log_qtd_cli_rec_pj_boleto","constant","constant","flood_risk5","Log Quantity of Firms Receiving Boleto",list(dat_b,dat_a), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)

#Cartao
print_twfe("Cartao_balanced_covid_","log_valor_cartao_debito","constant","constant","flood_risk5","Log Value Debit Card",list(dat_b,dat_a), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
print_twfe("Cartao_balanced_covid_","log_valor_cartao_credito","constant","constant","flood_risk5","Log Value Credit Card",list(dat_b,dat_a), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
print_twfe("Cartao_balanced_covid_","log_qtd_cli_cartao_debito","constant","constant","flood_risk5","Log Quantity of Firms Accepting Debit Card",list(dat_b,dat_a), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
print_twfe("Cartao_balanced_covid_","log_qtd_cli_cartao_credito","constant","constant","flood_risk5","Log Quantity of Firms Accepting Credit Card",list(dat_b,dat_a), c("2020.03 - 2020.10","2020.11 - 2021.06"), xl_balanced_covid, xu_balanced_covid)
# + Adocao
rm(dat_a, dat_b)

}, error = function(e) {
  print(paste("Error in Base_week_muni:", e))
})
# Credito
tryCatch({
dat_a <- read_dta(file.path(dta_path,"Base_credito_muni_flood.dta"))
setDT(dat_a)
dat_a$time <- dat_a$time_id
dat_a$month <- time_id_to_month(dat_a$time_id)
dat_a$year <- time_id_to_year(dat_a$time_id)
dat_a <- merge(dat_a, mun_fe, by="muni_cd", all.x = TRUE)
dat_a <- merge(dat_a, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
dat_a[, treat := ifelse(is.na(date_flood), 0, 1)]
dat_a[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
dat_a[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
xlimits <- seq(ceiling(-6*1.333),ceiling(12*1.333),by=1)
dat_a <- subset(dat_a,time_to_treat %in% xlimits)

dat_b <- read_dta(file.path(dta_path,"Base_credito_muni_flood_beforePIX.dta"))
setDT(dat_b)
dat_b$time <- dat_b$time_id
dat_b$month <- time_id_to_month(dat_b$time_id)
dat_b$year <- time_id_to_year(dat_b$time_id)
dat_b <- merge(dat_b, mun_fe, by="muni_cd", all.x = TRUE)
dat_b <- merge(dat_b, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
dat_b[, treat := ifelse(is.na(date_flood), 0, 1)]
dat_b[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
dat_b[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
xlimits <- seq(ceiling(-6*1.333),ceiling(12*1.333),by=1)
dat_b <- subset(dat_b,time_to_treat %in% xlimits)

print_twfe("Credito_","log_vol_cartao","constant","constant","flood_risk5","Log Credit Card Balance",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
print_twfe("Credito_","log_qtd_cli_cartao","constant","constant","flood_risk5","Log Quantity of Credit Cards Users",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
print_twfe("Credito_","log_vol_emprestimo_pessoal","constant","constant","flood_risk5","Log Volume Personal Loan",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
print_twfe("Credito_","log_qtd_cli_emp_pessoal","constant","constant","flood_risk5","Log Quantity Personal Loan",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
print_twfe("Credito_","log_vol_credito_total","constant","constant","flood_risk5","Log Volume Loan",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
print_twfe("Credito_","log_qtd_cli_total","constant","constant","flood_risk5","Log Quantity Loan",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
print_twfe("Credito_","log_vol_credito_total_PF","constant","constant","flood_risk5","Log Volume Loan",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
print_twfe("Credito_","log_qtd_cli_total_PF","constant","constant","flood_risk5","Log Quantity Loan",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
print_twfe("Credito_","log_vol_credito_total_PJ","constant","constant","flood_risk5","Log Volume Loan",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
print_twfe("Credito_","log_qtd_cli_total_PJ","constant","constant","flood_risk5","Log Quantity Loan",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
rm(dat_a, dat_b)

# _balanced

dat_a <- read_dta(file.path(dta_path,"Base_credito_muni_flood.dta"))
setDT(dat_a)
dat_a$time <- dat_a$time_id
dat_a$month <- time_id_to_month(dat_a$time_id)
dat_a$year <- time_id_to_year(dat_a$time_id)
dat_a <- merge(dat_a, mun_fe, by="muni_cd", all.x = TRUE)
dat_a <- merge(dat_a, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
# I need to delete the flood info (date_flood) and upload the new flood balanced. 
# flood_month_after_balanced
dat_a <- dat_a %>% select(-date_flood)
dat_a <- merge(dat_a, flood_month_after_balanced, by=c("muni_cd","time"), all=FALSE) 
#
dat_a[, treat := ifelse(is.na(date_flood), 0, 1)]
dat_a[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
dat_a[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
xlimits <- seq(ceiling(-6*1.333),ceiling(12*1.333),by=1)
dat_a <- subset(dat_a,time_to_treat %in% xlimits)

dat_b <- read_dta(file.path(dta_path,"Base_credito_muni_flood_beforePIX.dta"))
setDT(dat_b)
dat_b$time <- dat_b$time_id
dat_b$month <- time_id_to_month(dat_b$time_id)
dat_b$year <- time_id_to_year(dat_b$time_id)
dat_b <- merge(dat_b, mun_fe, by="muni_cd", all.x = TRUE)
dat_b <- merge(dat_b, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
dat_b <- dat_b %>% select(-date_flood)
dat_b <- merge(dat_b, flood_month_before_balanced2019, by=c("muni_cd","time"), all=FALSE) 
dat_b[, treat := ifelse(is.na(date_flood), 0, 1)]
dat_b[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
dat_b[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
xlimits <- seq(ceiling(-6*1.333),ceiling(12*1.333),by=1)
dat_b <- subset(dat_b,time_to_treat %in% xlimits)

print_twfe("Credito_balanced_","log_vol_cartao","constant","constant","flood_risk5","Log Credit Card Balance",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
print_twfe("Credito_balanced_","log_qtd_cli_cartao","constant","constant","flood_risk5","Log Quantity of Credit Cards Users",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
print_twfe("Credito_balanced_","log_vol_emprestimo_pessoal","constant","constant","flood_risk5","Log Volume Personal Loan",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
print_twfe("Credito_balanced_","log_qtd_cli_emp_pessoal","constant","constant","flood_risk5","Log Quantity Personal Loan",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
print_twfe("Credito_balanced_","log_vol_credito_total","constant","constant","flood_risk5","Log Volume Loan",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
print_twfe("Credito_balanced_","log_qtd_cli_total","constant","constant","flood_risk5","Log Quantity Loan",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
print_twfe("Credito_balanced_","log_vol_credito_total_PF","constant","constant","flood_risk5","Log Volume Loan",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
print_twfe("Credito_balanced_","log_qtd_cli_total_PF","constant","constant","flood_risk5","Log Quantity Loan",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
print_twfe("Credito_balanced_","log_vol_credito_total_PJ","constant","constant","flood_risk5","Log Volume Loan",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
print_twfe("Credito_balanced_","log_qtd_cli_total_PJ","constant","constant","flood_risk5","Log Quantity Loan",list(dat_b,dat_a), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)
rm(dat_a, dat_b)

# _balanced_covid

dat_a <- read_dta(file.path(dta_path,"Base_credito_muni_flood.dta"))
setDT(dat_a)
dat_a$time <- dat_a$time_id
dat_a$month <- time_id_to_month(dat_a$time_id)
dat_a$year <- time_id_to_year(dat_a$time_id)
dat_a <- merge(dat_a, mun_fe, by="muni_cd", all.x = TRUE)
dat_a <- merge(dat_a, mun_control, by=c("muni_cd","month","year"), all.x = TRUE)
dat_a <- dat_a %>% select(-date_flood)
dat_a <- merge(dat_a, flood_month_after_balanced_covid, by=c("muni_cd","time"), all=FALSE)
dat_a[, treat := ifelse(is.na(date_flood), 0, 1)]
dat_a[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
dat_a[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
xlimits <- seq(ceiling(-6*1.333),ceiling(12*1.333),by=1)
dat_a <- subset(dat_a,time_to_treat %in% xlimits)

dat_b <- read_dta(file.path(dta_path,"Base_credito_muni_flood_beforePIX.dta"))
setDT(dat_b)
dat_b$time <- dat_b$time_id
dat_b$month <- time_id_to_month(dat_b$time_id)
dat_b$year <- time_id_to_year(dat_b$time_id)
dat_b <- merge(dat_b, mun_fe, by="muni_cd", all.x = TRUE)
dat_b <- merge(dat_b, mun_control, by=c("muni_cd","month","year"), all.x = TRUE)
dat_b <- dat_b %>% select(-date_flood)
dat_b <- merge(dat_b, flood_month_before_balanced2019, by=c("muni_cd","time"), all=FALSE)
dat_b[, treat := ifelse(is.na(date_flood), 0, 1)]
dat_b[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
dat_b[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
xlimits <- seq(ceiling(-6*1.333),ceiling(12*1.333),by=1)
dat_b <- subset(dat_b,time_to_treat %in% xlimits)

print_twfe("Credito_balanced_covid_","log_vol_cartao","constant","constant","flood_risk5","Log Credit Card Balance",list(dat_b,dat_a), c("2020.03 - 2020.10","2020.11 - 2021.06"), -3, 3)
print_twfe("Credito_balanced_covid_","log_qtd_cli_cartao","constant","constant","flood_risk5","Log Quantity of Credit Cards Users",list(dat_b,dat_a), c("2020.03 - 2020.10","2020.11 - 2021.06"), -3, 3)
print_twfe("Credito_balanced_covid_","log_vol_emprestimo_pessoal","constant","constant","flood_risk5","Log Volume Personal Loan",list(dat_b,dat_a), c("2020.03 - 2020.10","2020.11 - 2021.06"), -3, 3)
print_twfe("Credito_balanced_covid_","log_qtd_cli_emp_pessoal","constant","constant","flood_risk5","Log Quantity Personal Loan",list(dat_b,dat_a), c("2020.03 - 2020.10","2020.11 - 2021.06"), -3, 3)
print_twfe("Credito_balanced_covid_","log_vol_credito_total","constant","constant","flood_risk5","Log Volume Loan",list(dat_b,dat_a), c("2020.03 - 2020.10","2020.11 - 2021.06"), -3, 3)
print_twfe("Credito_balanced_covid_","log_qtd_cli_total","constant","constant","flood_risk5","Log Quantity Loan",list(dat_b,dat_a), c("2020.03 - 2020.10","2020.11 - 2021.06"), -3, 3)
print_twfe("Credito_balanced_covid_","log_vol_credito_total_PF","constant","constant","flood_risk5","Log Volume Loan",list(dat_b,dat_a), c("2020.03 - 2020.10","2020.11 - 2021.06"), -3, 3)
print_twfe("Credito_balanced_covid_","log_qtd_cli_total_PF","constant","constant","flood_risk5","Log Quantity Loan",list(dat_b,dat_a), c("2020.03 - 2020.10","2020.11 - 2021.06"), -3, 3)
print_twfe("Credito_balanced_covid_","log_vol_credito_total_PJ","constant","constant","flood_risk5","Log Volume Loan",list(dat_b,dat_a), c("2020.03 - 2020.10","2020.11 - 2021.06"), -3, 3)
print_twfe("Credito_balanced_covid_","log_qtd_cli_total_PJ","constant","constant","flood_risk5","Log Quantity Loan",list(dat_b,dat_a), c("2020.03 - 2020.10","2020.11 - 2021.06"), -3, 3)
rm(dat_a, dat_b)

}, error = function(e) {
  # Handle errors or print a message
  print(paste("Error in Credito:", e))
})

# Pix_individual
tryCatch({
dat_a <- read_dta(file.path(dta_path,"Pix_individuo_sample_flood.dta"))
setDT(dat_a)
dat_a$time <- dat_a$time_id
dat_a$month <- time_id_to_month(dat_a$time_id)
dat_a$year <- time_id_to_year(dat_a$time_id)
dat_a <- merge(dat_a, mun_fe, by="muni_cd", all.x = TRUE)
dat_a <- merge(dat_a, mun_control, by=c("muni_cd","month","year"), all.x = TRUE) 
dat_a[, treat := ifelse(is.na(date_flood), 0, 1)]
dat_a[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
dat_a[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
xlimits <- seq(ceiling(-6*1.333),ceiling(12*1.333),by=1)
dat_a <- subset(dat,time_to_treat %in% xlimits)

dat_rec <- dat_a %>%
  rename(adoption=after_first_pix_rec,
         use = receiver,
         log_trans = log_trans_rec,
         log_valor = log_value_rec)
dat_sent <- dat_a %>%
  rename(adoption=after_first_pix_sent,
         use = sender,
         log_trans = log_trans_sent,
         log_valor = log_value_sent)
dat_self<- dat_a %>%
  rename(use = user,
         log_trans = log_trans_self,
         log_valor = log_value_self)


print_twfe("Pix_PF_","adoption","constant","constant","flood_risk5","Adoption",list(dat_rec,dat_sent), c("Receiver","Sender"), -6, 12)
print_twfe("Pix_PF_","use","constant","constant","flood_risk5","Active Use",list(dat_rec,dat_sent), c("Receivers","Senders"), -6, 12)
print_twfe("Pix_PF_","log_trans","constant","constant","flood_risk5","Log Transactions",list(dat_rec, dat_sent), c("Received","Sent"), -6, 12)
print_twfe("Pix_PF_","log_valor","constant","constant","flood_risk5","Log Value",list(dat_rec, dat_sent), c("Received","Sent"), -6, 12)
}, error = function(e) {
  # Handle errors or print a message
  print(paste("Error in Pix_individual:", e))
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
dat_a2 <- subset(dat,time_to_treat %in% xlimits)

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
xlimits <- seq(ceiling(-6*1.333),ceiling(12*1.333),by=1)
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
xlimits <- seq(ceiling(-6*1.333),ceiling(12*1.333),by=1)
dat_b2 <- subset(dat,time_to_treat %in% xlimits)

#log_caixa, log_total_deposits, log_poupanca, log_dep_prazo, log_dep_vista_PF, log_dep_vista_PJ
# Large vs Small
print_twfe("Estban_","log_caixa","constant","constant","flood_risk5","Log Monetary Inventory",list(dat_a_large,dat_a_small), c("Top 5 Bank","Others"), -6, 12)

# Before vs After
print_twfe("Estban_","log_caixa","constant","constant","flood_risk5","Log Monetary Inventory",list(dat_b2,dat_a2), c("2019.01 - 2020.10","2020.11 - 2022.12"), -6, 12)

rm(dat_a,dat_a2,dat_b,dat_b2)

# _balanced

}, error = function(e) {
  # Handle errors or print a message
  print(paste("Error in ESTBAN:", e))
})

# RAIS








# Archive

# twfe1 <- function(y,control1,control2,fe,dat){
#   dat$Y <- dat[[y]]
#   dat$C1 <- dat[[control1]]
#   dat$C2 <- dat[[control2]] 
#   dat$FE <- dat[[fe]]
#   mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) + ## Our key interaction: time ? treatment status
#                    C1 + C2 | ## Control variables
#                    muni_cd + time + FE:time,            ## FEs
#                    cluster = ~muni_cd,                          ## Clustered SEs
#                    data = dat)
#   return(mod_twfe)
# }
# print1 <- function(graphname,y,control1,control2,fe,main_title,dat,xlimit_l,xlimit_u){
#   png(file.path(output_path,paste0(graphname,y,".png")), width = 640*4, height = 480*4, res = 200)
#   par(cex.main = 1.75, cex.lab = 1.25, cex.axis = 1.75)
#   iplot(twfe1(y,control1,control2,fe,dat), sep = 0.5, ref.line = -1,
#         xlab = '',
#         main = main_title,
#         ci_level = 0.95, xlim = c(xlimit_l-0.1,xlimit_u+0.1)) 
#   legend("bottomleft", col = c(1), pch = c(20), 
#          legend = c("TWFE"), cex = 1.5)
#   dev.off()  
# }
# twfe2 <- function(y,control1,control2,fe,dat1,dat2){
#   dat1$Y <- dat1[[y]]
#   dat1$C1 <- dat1[[control1]]
#   dat1$C2 <- dat1[[control2]] 
#   dat1$FE <- dat1[[fe]]
#   
#   mod_twfe1 = feols(Y ~ i(time_to_treat, treat, ref = -1) + ## Our key interaction: time ? treatment status
#                       C1 + C2 | ## Control variables
#                       muni_cd + time + FE:time,            ## FEs
#                     cluster = ~muni_cd,                          ## Clustered SEs
#                     data = dat1)
#   
#   dat2$Y <- dat2[[y]]
#   dat2$C1 <- dat2[[control1]]
#   dat2$C2 <- dat2[[control2]] 
#   dat2$FE <- dat2[[fe]]
#   
#   mod_twfe2 = feols(Y ~ i(time_to_treat, treat, ref = -1) + ## Our key interaction: time ? treatment status
#                       C1 + C2 | ## Control variables
#                       muni_cd + time + FE:time,            ## FEs
#                     cluster = ~muni_cd,                          ## Clustered SEs
#                     data = dat2)
#   
#   return(list(mod_twfe1, mod_twfe2))
# }
# print2 <- function(graphname,y,control1,control2,fe,main_title,dat1,dat2,legend_list,xlimit_l,xlimit_u){
#   png(file.path(output_path,paste0(graphname,y,".png")), width = 640*4, height = 480*4, res = 200)
#   par(cex.main = 1.75, cex.lab = 1.25, cex.axis = 1.75)
#   iplot(twfe2(y,control1,control2,fe,dat1,dat2), sep = 0.5, ref.line = -1,
#         xlab = '',
#         main = main_title,
#         ci_level = 0.95, xlim = c(xlimit_l-0.1,xlimit_u+0.1)) 
#   legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
#          legend = legend_list, cex = 1.5)
#   dev.off()
# }

