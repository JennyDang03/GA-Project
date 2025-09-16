#temp_flood_event_studies.R
# Flood Event Studies

################################################################################

#https://lost-stats.github.io/Model_Estimation/Research_Design/event_study.html
options(download.file.method = "wininet")
rm(list = ls()) ## Clear workspace

# install.packages("coefplot")
# install.packages("magrittr")
library(readr)
library(stringr)
library(dplyr)
library(tidyr)
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
library(coefplot)
library(magrittr)
library(odbc)
library(RODBC)

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

xl_month <- -6
xu_month <- 12
xl_balanced_month <- -6
xu_balanced_month <- 12
xl_balanced_covid_month <- -3
xu_balanced_covid_month <- 3

################################################################################
#-------------------------------------------------------------------------------
# Load auxiliary data
#-------------------------------------------------------------------------------
source(paste0(R_path, "/auxiliary_data.r"))

#Idea: just load the data, no work on it. save it beforehand. (if exists, just load)

mun_convert <- read_dta(paste0(path_dta, "municipios.dta"))
mun_convert <- mun_convert %>%
  select(MUN_CD, MUN_CD_CADMU, MUN_CD_IBGE, MUN_NM, MUN_NM_NAO_FORMATADO) %>%
  rename(muni_cd = MUN_CD_CADMU,
         id_municipio = MUN_CD_IBGE,
         id_municipio_receita = MUN_CD,
         muni_nm = MUN_NM,
         muni_nm_nao_formatado = MUN_NM_NAO_FORMATADO)
mun_convert <- data.table(mun_convert)
mun_convert <- mun_convert %>%
  mutate(id_municipio = as.integer(id_municipio),
         muni_cd = as.integer(muni_cd),
         id_municipio_receita = as.integer(id_municipio_receita))
any(duplicated(mun_convert$id_municipio) | duplicated(mun_convert$muni_cd) | duplicated(mun_convert$id_municipio_receita))
setDT(mun_convert)
temp <- read_dta(file.path(dta_path, "mun_fe.dta"))
temp <- temp %>% select(muni_cd,pop2022)
mun_convert <- merge(mun_convert, temp, by= c("muni_cd"), all.x = TRUE, all.y = TRUE)
setorder(mun_convert, pop2022)
mun_convert3 <- mun_convert %>% select(muni_cd, id_municipio)

#-------------------------------------------------------------------------------
# Functions
#-------------------------------------------------------------------------------
source(paste0(R_path,"/functions/stata_week_number.R"))
source(paste0(R_path,"/functions/gen_week_list.R"))
source(paste0(R_path,"/functions/time_id_to_month.R"))
source(paste0(R_path,"/functions/time_id_to_year.R"))
source(paste0(R_path,"/functions/week_to_month.R"))
source(paste0(R_path,"/functions/week_to_year.R"))
source(paste0(R_path,"/functions/prepare_data.R"))
source(paste0(R_path,"/functions/twfe.R"))
source(paste0(R_path,"/functions/ylim_function.R"))
source(paste0(R_path,"/functions/print_twfe.R"))
source(paste0(R_path,"/functions/twfe2.R"))
source(paste0(R_path,"/functions/ylim_function2.R"))
source(paste0(R_path,"/functions/print_twfe2.R"))
source(paste0(R_path,"/functions/day_to_stata_month.R"))
source(paste0(R_path,"/functions/print_twfe_month.R"))
source(paste0(R_path,"/functions/print_twfe_week.R"))
flood_month_before_balanced_pre_covid <- balance_flood_data(flood_month_before, natural_disasters_month, 696+6, 730-12-1-7) # 696 = ym(2018,1), 730 = ym(2020,11) on stata
source(paste0(R_path,"/functions/print_twfe_month2.R"))

# Query -------------------------------------------------------------------
run_Estban <- 1
run_Estban5 <- 1
run_Rais <- 1


#-------------------------------------------------------------------------------
#ESTBAN
#-------------------------------------------------------------------------------



if(!file.exists(paste0(dta_path, "estban_2018_2023.dta"))){
  #ESTBAN
  #estban <- read_dta(paste0(dta_path, "estban_mun_basedosdados.dta"))
  #filter year, id_verbete, collapse by id_municipio, ano, mes, and id_verbete
  #estban <- estban %>% filter(ano >= 2018)
  
  estban0 <- read_dta(paste0(dta_path, "estban_mun_basedosdados_2018_2023.dta"))
  
  #estban <- estban %>% filter(id_verbete %in% c("110","120","130","160","180","184","190","200","420","432","430","440","460","480","490500","610","710","711","712","111", "399", "899", "400", "410", "470"))
  estban <- estban0 %>% group_by(ano, mes, id_municipio, id_verbete) %>% summarise(valor = sum(valor, na.rm = TRUE)) %>% ungroup()
  
  #Pivot wide id_verbete
  estban <- estban %>% pivot_wider(names_from = id_verbete, values_from = valor)
  
  # FIX 710,711, 712. 
  estban <- estban %>%
    group_by(id_municipio) %>% 
    arrange(ano, mes, .by_group = TRUE) %>%
    mutate(`d710` = c(NA,diff(`710`)),
           `d711` = c(NA, diff(`711`)),
           `d712` = c(NA, diff(`712`))) %>%
    mutate(`710` = ifelse(mes %% 6 == 1,
                          `710`,
                          `d710`),
           `711` = ifelse(mes %% 6 == 1,
                          `711`,
                          `d711`),
           `712` = ifelse(mes %% 6 == 1,
                          `712`,
                          `d712`)) %>% ungroup()
  
  #Define deposit
  estban$deposit <- estban$`420` + estban$`432`
  estban$deposit2 <- estban$`401402403404411412413414415416417418419` + estban$`420` + estban$`430`
  estban$assets <- estban$`110` + estban$`120` + estban$`130` + estban$`160` + estban$`180` + estban$`184` + estban$`190` + estban$`200`
  estban$assets2 <- estban$`110` + estban$`120` + estban$`130` + estban$`140` + estban$`160` + estban$`180` + estban$`184` + estban$`190` + estban$`200`
  estban$liab <- estban$`420` + estban$`432` + estban$`430` + estban$`440` + estban$`460` + estban$`480` + estban$`490500` + estban$`610` + estban$`710` + estban$`711` + estban$`712`
  estban$liab2 <- estban$`401402403404411412413414415416417418419` + estban$`420` + estban$`430` + estban$`440` + estban$`460` + estban$`470` + estban$`480` + estban$`490500` + estban$`610` + estban$`710`

  #Define caixa - 110 could be a better caixa. 
  estban$caixa <- estban$`111`
  estban$caixa2 <- estban$`110`
  
  estban$revenue <- estban$`711`
  estban$expenses <- estban$`712`
  estban$profit <- estban$`710`
  estban$total_do_ativo <- estban$`399`
  estban$total_do_passivo <- estban$`899`
  estban$patrimonio_liquido <- estban$`610`
  
  estban$loans <- estban$`160`
  
  # Create time_id, also, change id_municipio to muni_cd, delete ano, mes
  estban <- estban %>% mutate(time_id = (ano-1960)*12 + mes-1)
  estban <- estban %>% mutate(id_municipio = as.integer(id_municipio))
  estban <- merge(estban, mun_convert3, by="id_municipio", all.x = TRUE)
  
  #Create log variables for all except id_municipio, ano, mes, time_id, muni_cd
  estban<- estban %>% mutate(`712` = abs(`712`),
                             expenses = abs(expenses))
  estban <- estban %>%
    mutate(across(
      -c(id_municipio, ano, mes, time_id, muni_cd),
      ~ log1p(.),
      .names = "l{.col}"
    ))
  
  estban <- estban %>% select(-ano, -mes, -id_municipio)
  estban <- estban %>% arrange(muni_cd, time_id)
  #Save
  estban <- estban %>%
    rename_with(~ paste0("v", .), .cols = matches("^\\d"))
  estban <- estban %>%
    rename(v401_419 = v401402403404411412413414415416417418419,
           l401_419 = l401402403404411412413414415416417418419)
  write_dta(estban, paste0(dta_path, "estban_2018_2023.dta"))
}


# estban_2018_2023.dta
# Variables: muni_cd, time_id, deposit, assets, liab, caixa,
#            ldeposit, lassets, lliab, lcaixa

variables <- c("lloans", "ldeposit", "ldeposit2", "lassets", "lassets2", "lliab", "lliab2", "lcaixa", "lcaixa2", "lrevenue", "lexpenses", "ltotal_do_ativo", "ltotal_do_passivo")
variables_labels <- c("Log Loans", "Log Deposits", "Log Deposits", "Log Assets", "Log Assets", "Log Liabilities", "Log Liabilities", "Log Money Inventory", "Log Money Inventory", "Log Revenue", "Log Expenses", "Log Total Assets", "Log Total Liabilities")

variables <- c("lloans", "ldeposit", "ldeposit2", "lassets", "lassets2", "lliab", "lliab2", "lcaixa", "lcaixa2", "lrevenue", "lexpenses", "ltotal_do_ativo")
variables_labels <- c("Log Loans", "Log Deposits", "Log Deposits", "Log Assets", "Log Assets", "Log Liabilities", "Log Liabilities", "Log Money Inventory", "Log Money Inventory", "Log Revenue", "Log Expenses", "Log Total Assets")


variables <- c("l432")
variables_labels <- c("DEPÓSITOS prazo")
#estban$liab2 <- estban$`401402403404411412413414415416417418419` + estban$`420` + estban$`430` + estban$`440` + estban$`460` + estban$`470` + estban$`480` + estban$`490500` + estban$`610` + estban$`710`
#   estban$deposit <- estban$`420` + estban$`432`
#   estban$deposit2 <- estban$`401402403404411412413414415416417418419` + estban$`420` + estban$`430`
# l401_419 seems promissing
# l420 seems promissing <----
# l430 awful 
# l432 awful


variables <- c("ltotal_do_ativo", "lrevenue", "lexpenses", "lloans", "ldeposit3", "l420", "l401_419")
variables_labels <- c("Log Assets", "Log Revenue", "Log Expenses", "Log Loans","Log Deposits", "Log Savings", "Log Checking")
if(run_Estban == 1){
  tryCatch({
    for(i in 3:4){
      if(i==1){
        flood_a <- flood_month_after
        flood_b <- flood_month_before2019
        xll <- xl_month
        xuu <- xu_month
        ending <- "_"
        legend_name <- c("2019.01 - 2020.10", "2020.11 - 2022.12")
      }
      if(i==2){
        flood_a <- flood_month_after_balanced
        flood_b <- flood_month_before_balanced2019
        xll <- xl_balanced_month
        xuu <- xu_balanced_month
        ending <- "_balanced_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==3){
        flood_a <- flood_month_after_balanced_covid
        flood_b <- flood_month_before_balanced_covid
        xll <- xl_balanced_covid_month
        xuu <- xu_balanced_covid_month
        ending <- "_balanced_covid_"
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
      }
      if(i==4){
        flood_a <- flood_month_after_balanced
        flood_b <- flood_month_before_balanced
        xll <- xl_balanced_month
        xuu <- xu_balanced_month
        ending <- "_balanced18_"
        legend_name <- c("2018.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==5){
        flood_a <- flood_month_after_balanced
        flood_b <- flood_month_before_balanced_pre_covid
        xll <- xl_balanced_month
        xuu <- xu_balanced_month
        ending <- "_balanced18_pre_covid_"
        legend_name <- c("2018.01 - 2020.03","2020.11 - 2022.12")
      }
      #dat_a <- prepare_data("estban_2018_2023.dta",flood_a,mun_fe,mun_control)
      #dat_b <- prepare_data("estban_2018_2023.dta",flood_b,mun_fe,mun_control)
      #dat_a$ldeposit3 <- log1p(exp(dat_a$l420) - 1 + exp(dat_a$l401_419) - 1)
      #dat_b$ldeposit3 <- log1p(exp(dat_b$l420) - 1 + exp(dat_b$l401_419) - 1)
      
      beginning <- paste0("Estban_public",ending)
      legend_name <- c("Before Pix","After Pix")
      for(z in 1:length(variables)){
        #twfe2(beginning,variables[[z]],"constant","constant","flood_risk5", list(dat_b,dat_a), c("before","after"))
        print_twfe_month(beginning, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu) # Note that I am using a different print function.
      }
      rm(dat_a,dat_b)
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in Estban:", e))
  })
}

# Estban, top 5 vs others. (after Pix)

if(!file.exists(paste0(dta_path, "estban_2018_2023_top5.dta"))){
  #ESTBAN
  #main_data <- read_dta(paste0(dta_path, "estban_mun_basedosdados.dta"))
  #estban5 <- main_data %>% filter(ano >= 2018)
  #filter year, id_verbete, collapse by id_municipio, ano, mes, and id_verbete
  #estban5 <- estban5 %>% filter(id_verbete %in% c("110","120","130","160","180","184","190","200","420","432","430","440","460","480","490500","610","710","711","712","111", "399", "899"))
  estban0 <- read_dta(paste0(dta_path, "estban_mun_basedosdados_2018_2023.dta"))
  
  
  count_cnpj <- estban0 %>% group_by(cnpj_basico,instituicao) %>% summarise(n = n()) %>% ungroup()
  count_cnpj <- count_cnpj %>% arrange(desc(n))
  topp5 <- count_cnpj %>% slice(1:5) %>% select(cnpj_basico) %>% mutate(top5 = 1)
  estban5 <- estban0 %>% left_join(topp5, by = "cnpj_basico") %>% mutate(top5 = ifelse(is.na(top5), 0, top5))
  estban5 <- estban5 %>% group_by(ano, mes, id_municipio, id_verbete, top5) %>% summarise(valor = sum(valor, na.rm = TRUE)) %>% ungroup()
  
  #Clean
  estban5 <- estban5 %>% pivot_wider(names_from = id_verbete, values_from = valor)
  
  # FIX 710,711, 712. 
  estban5 <- estban5 %>%
    group_by(id_municipio) %>% 
    arrange(ano, mes, .by_group = TRUE) %>%
    mutate(`d710` = c(NA,diff(`710`)),
           `d711` = c(NA, diff(`711`)),
           `d712` = c(NA, diff(`712`))) %>%
    mutate(`710` = ifelse(mes %% 6 == 1,
                          `710`,
                          `d710`),
           `711` = ifelse(mes %% 6 == 1,
                          `711`,
                          `d711`),
           `712` = ifelse(mes %% 6 == 1,
                          `712`,
                          `d712`)) %>% ungroup()
  
  #Define deposit
  estban5$deposit <- estban5$`420` + estban5$`432`
  estban5$deposit2 <- estban5$`401402403404411412413414415416417418419` + estban5$`420` + estban5$`430`
  estban5$assets <- estban5$`110` + estban5$`120` + estban5$`130` + estban5$`160` + estban5$`180` + estban5$`184` + estban5$`190` + estban5$`200`
  estban5$assets2 <- estban5$`110` + estban5$`120` + estban5$`130` + estban5$`140` + estban5$`160` + estban5$`180` + estban5$`184` + estban5$`190` + estban5$`200`
  estban5$liab <- estban5$`420` + estban5$`432` + estban5$`430` + estban5$`440` + estban5$`460` + estban5$`480` + estban5$`490500` + estban5$`610` + estban5$`710` + estban5$`711` + estban5$`712`
  estban5$liab2 <- estban5$`401402403404411412413414415416417418419` + estban5$`420` + estban5$`430` + estban5$`440` + estban5$`460` + estban5$`470` + estban5$`480` + estban5$`490500` + estban5$`610` + estban5$`710`
  
  #Define caixa - 110 could be a better caixa. 
  estban5$caixa <- estban5$`111`
  estban5$caixa2 <- estban5$`110`
  
  estban5$revenue <- estban5$`711`
  estban5$expenses <- estban5$`712`
  estban5$profit <- estban5$`710`
  estban5$total_do_ativo <- estban5$`399`
  estban5$total_do_passivo <- estban5$`899`
  estban5$patrimonio_liquido <- estban5$`610`
  
  estban5$deposit <- estban5$`420` + estban5$`432`
  estban5$assets <- estban5$`110` + estban5$`120` + estban5$`130` + estban5$`160` + estban5$`180` + estban5$`184` + estban5$`190` + estban5$`200`
  estban5$liab <- estban5$`420` + estban5$`432` + estban5$`430` + estban5$`440` + estban5$`460` + estban5$`480` + estban5$`490500` + estban5$`610` + estban5$`710` + estban5$`711` + estban5$`712`
  estban5$caixa <- estban5$`111`
  
  estban5$revenue <- estban5$`711`
  estban5$expenses <- estban5$`712`
  estban5$profit <- estban5$`710`
  estban5$total_do_ativo <- estban5$`399`
  estban5$total_do_passivo <- estban5$`899`
  estban5$patrimonio_liquido <- estban5$`610`
  estban5$loans <- estban5$`160`
  #estban5 <- estban5 %>% select(top5, ano, mes, id_municipio, deposit, assets, liab, caixa)
  
  estban5 <- estban5 %>% mutate(time_id = (ano-1960)*12 + mes-1)
  estban5 <- estban5 %>% mutate(id_municipio = as.integer(id_municipio))
  estban5 <- merge(estban5, mun_convert3, by="id_municipio", all.x = TRUE)
  
  #Create log variables
  estban5 <- estban5 %>% mutate(`712` = abs(`712`),
                             expenses = abs(expenses))
  estban5 <- estban5 %>%
    mutate(across(
      -c(id_municipio, ano, mes, time_id, muni_cd, top5),
      ~ log1p(.),
      .names = "l{.col}"
    ))
  estban5 <- estban5 %>% select(-ano, -mes, -id_municipio)
  #sort by muni_cd, time_id, top5
  estban5 <- estban5 %>% arrange(muni_cd, time_id, top5)
  #Save
  estban5 <- estban5 %>%
    rename_with(~ paste0("v", .), .cols = matches("^\\d"))
  estban5 <- estban5 %>%
    rename(v401_419 = v401402403404411412413414415416417418419,
           l401_419 = l401402403404411412413414415416417418419)
  write_dta(estban5, paste0(dta_path, "estban_2018_2023_top5.dta"))
}

# estban_2018_2023_top5.dta
# Variables: top5, muni_cd, time_id, deposit, assets, liab, caixa,
#            ldeposit, lassets, lliab, lcaixa


variables <- c("lloans", "ldeposit", "ldeposit2", "lassets", "lassets2", "lliab", "lliab2", "lcaixa", "lcaixa2", "lrevenue", "lexpenses", "ltotal_do_ativo", "ltotal_do_passivo")
variables_labels <- c("Log Loans", "Log Deposits", "Log Deposits", "Log Assets", "Log Assets", "Log Liabilities", "Log Liabilities", "Log Money Inventory", "Log Money Inventory", "Log Revenue", "Log Expenses", "Log Total Assets", "Log Total Liabilities")

if(run_Estban5 == 1){
  tryCatch({
    for(i in 2:2){
      if(i==1){
        flood_a <- flood_month_after
        flood_b <- flood_month_before2019
        xll <- xl_month
        xuu <- xu_month
        ending <- "_"
        legend_name <- c("Top 5","Others")
      }
      if(i==2){
        flood_a <- flood_month_after_balanced
        flood_b <- flood_month_before_balanced2019
        xll <- xl_balanced_month
        xuu <- xu_balanced_month
        ending <- "_balanced_"
        legend_name <- c("Top 5","Others")
      }
      if(i==3){
        flood_a <- flood_month_after_balanced_covid
        flood_b <- flood_month_before_balanced_covid
        xll <- xl_balanced_covid_month
        xuu <- xu_balanced_covid_month
        ending <- "_balanced_covid_"
        legend_name <- c("Top 5","Others")
      }
      
      dat_a <- prepare_data("estban_2018_2023_top5.dta",flood_a,mun_fe,mun_control)
      dat_a5 <- dat_a %>% filter(top5 == 1)
      dat_a <- dat_a %>% filter(top5 == 0)
      
      dat_b <- prepare_data("estban_2018_2023_top5.dta",flood_b,mun_fe,mun_control)
      dat_b5 <- dat_b %>% filter(top5 == 1)
      dat_b <- dat_b %>% filter(top5 == 0)
      beginning <- paste0("Estban5_public",ending)
      beginning_b <- paste0("Estban5_public_before",ending)
      beginning_5 <- paste0("Estban5_public_top5",ending)
      for(z in 1:length(variables)){
        #twfe2(beginning,variables[[z]],"constant","constant","flood_risk5", list(dat_a5,dat_a), c("Top 5","Others"))
        #print_twfe_month(beginning, variables[[z]], variables_labels[[z]], c("Top 5","Others"), legend_name, xll, xuu)
        
        #twfe2(beginning_b,variables[[z]],"constant","constant","flood_risk5", list(dat_b5,dat_b), c("Top 5","Others"))
        #print_twfe_month(beginning_b, variables[[z]], variables_labels[[z]], c("Top 5","Others"), legend_name, xll, xuu)
        
        twfe2(beginning_5,variables[[z]],"constant","constant","flood_risk5", list(dat_b5, dat_a5), c("before","after"))
        print_twfe_month(beginning_5, variables[[z]], variables_labels[[z]], c("before","after"), c("2019.01 - 2020.10","2020.11 - 2022.12"), xll, xuu)
        }
      rm(dat_a,dat_b)
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in Estban5:", e))
  })
}














#-------------------------------------------------------------------------------
# Outliers
#-------------------------------------------------------------------------------
mun_convert2 <- read_dta(paste0(path_dta, "municipios.dta"))
mun_convert2 <- mun_convert2 %>%
  rename(muni_cd = MUN_CD_CADMU,
         id_municipio = MUN_CD_IBGE,
         id_municipio_receita = MUN_CD,
         muni_nm = MUN_NM,
         muni_nm_nao_formatado = MUN_NM_NAO_FORMATADO)
mun_convert2 <- data.table(mun_convert2)
mun_convert2 <- mun_convert2 %>%
  mutate(id_municipio = as.integer(id_municipio),
         muni_cd = as.integer(muni_cd),
         id_municipio_receita = as.integer(id_municipio_receita))
any(duplicated(mun_convert2$id_municipio) | duplicated(mun_convert2$muni_cd) | duplicated(mun_convert2$id_municipio_receita))
setDT(mun_convert2)

winsorize_data <- function(data) {
  
  data_outliers <- data %>%
    arrange(muni_cd, time, time_to_treat) %>%  # Step 1: Sort by muni_cd and time_to_treat
    group_by(muni_cd) %>%  # Step 2: Group by municipality
    mutate(
      d_ltotal_do_ativo = ltotal_do_ativo - lag(ltotal_do_ativo)
    ) %>%
    ungroup()  # Remove grouping after calculating differences
  data_outliers <- data_outliers %>%
    filter(time_to_treat == 1)
  
  # Step 2: Determine thresholds (e.g., 1st and 99th percentiles)
  lower_threshold <- quantile(data_outliers$d_ltotal_do_ativo, 0.025, na.rm = TRUE)
  upper_threshold <- quantile(data_outliers$d_ltotal_do_ativo, 0.975, na.rm = TRUE)
  
  # Step 3: Identify municipalities within the thresholds
  data_to_exclude <- data_outliers %>%
    filter(d_ltotal_do_ativo <= lower_threshold | d_ltotal_do_ativo >= upper_threshold) %>%
    select(muni_cd)
  
  data_to_exclude_with_names <- merge(data_to_exclude, mun_convert2, by="muni_cd", all.x = FALSE)
  
  data_outliers <- data %>%
    arrange(muni_cd, time, time_to_treat) %>%  # Step 1: Sort by muni_cd and time_to_treat
    group_by(muni_cd) %>%  # Step 2: Group by municipality
    mutate(
      d_l420 = l420 - lag(l420)
    ) %>%
    ungroup()  # Remove grouping after calculating differences
  data_outliers <- data_outliers %>%
    filter(time_to_treat == 8)
  
  # Step 2: Determine thresholds (e.g., 1st and 99th percentiles)
  lower_threshold <- quantile(data_outliers$d_l420, 0.025, na.rm = TRUE)
  upper_threshold <- quantile(data_outliers$d_l420, 0.975, na.rm = TRUE)
  
  # Step 3: Identify municipalities within the thresholds
  data_to_exclude2 <- data_outliers %>%
    filter(d_l420 <= lower_threshold | d_l420 >= upper_threshold) %>%
    select(muni_cd)
  
  data_to_exclude2_with_names <- merge(data_to_exclude2, mun_convert2, by="muni_cd", all.x = FALSE)
  
  
  # Step 4: Filter the original dataset based on selected municipalities
  data <- data %>%
    anti_join(data_to_exclude, by = "muni_cd")
  
  #data <- data %>%
  #  anti_join(data_to_exclude2, by = "muni_cd")
}

# Apply the function to both datasets

variables <- c("ltotal_do_ativo", "lrevenue", "lexpenses", "lloans", "ldeposit3", "l420", "l401_419")
variables_labels <- c("Log Assets", "Log Revenue", "Log Expenses", "Log Loans","Log Deposits", "Log Savings", "Log Checking")
if(run_Estban == 1){
  tryCatch({
    for(i in 4:4){
      if(i==1){
        flood_a <- flood_month_after
        flood_b <- flood_month_before2019
        xll <- xl_month
        xuu <- xu_month
        ending <- "_"
        legend_name <- c("2019.01 - 2020.10", "2020.11 - 2022.12")
      }
      if(i==2){
        flood_a <- flood_month_after_balanced
        flood_b <- flood_month_before_balanced2019
        xll <- xl_balanced_month
        xuu <- xu_balanced_month
        ending <- "_balanced_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==3){
        flood_a <- flood_month_after_balanced_covid
        flood_b <- flood_month_before_balanced_covid
        xll <- xl_balanced_covid_month
        xuu <- xu_balanced_covid_month
        ending <- "_balanced_covid_"
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
      }
      if(i==4){
        flood_a <- flood_month_after_balanced
        flood_b <- flood_month_before_balanced
        xll <- xl_balanced_month
        xuu <- xu_balanced_month
        ending <- "_balanced18_winsorize_"
        legend_name <- c("2018.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==5){
        flood_a <- flood_month_after_balanced
        flood_b <- flood_month_before_balanced_pre_covid
        xll <- xl_balanced_month
        xuu <- xu_balanced_month
        ending <- "_balanced18_pre_covid_"
        legend_name <- c("2018.01 - 2020.03","2020.11 - 2022.12")
      }
      dat_a <- prepare_data("estban_2018_2023.dta",flood_a,mun_fe,mun_control)
      dat_b <- prepare_data("estban_2018_2023.dta",flood_b,mun_fe,mun_control)
      dat_a$ldeposit3 <- log1p(exp(dat_a$l420) - 1 + exp(dat_a$l401_419) - 1)
      dat_b$ldeposit3 <- log1p(exp(dat_b$l420) - 1 + exp(dat_b$l401_419) - 1)
      
      dat_b <- dat_b %>% select(treat, flood_risk5, constant, time_to_treat, time, muni_cd, ltotal_do_ativo, lrevenue, lexpenses, lloans, ldeposit3, l420, l401_419)
      dat_a <- dat_a %>% select(treat, flood_risk5, constant, time_to_treat, time, muni_cd, ltotal_do_ativo, lrevenue, lexpenses, lloans, ldeposit3, l420, l401_419)
      
      dat_b <- winsorize_data(dat_b)
      dat_a <- winsorize_data(dat_a)
      
      beginning <- paste0("Estban_public",ending)
      legend_name <- c("Before Pix","After Pix")
      for(z in 1:length(variables)){
        twfe2(beginning,variables[[z]],"constant","constant","flood_risk5", list(dat_b,dat_a), c("before","after"))
        print_twfe_month2(beginning, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu) # Note that I am using a different print function.
      }
      rm(dat_a,dat_b)
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in Estban:", e))
  })
}






#-------------------------------------------------------------------------------
# RAIS
#-------------------------------------------------------------------------------

#rais <- read_dta(file.path(dta_path,"rais_jobs_salaries.dta"))
# variables: time_id, muni_cd, year, month, n_jobs, total_salary, ln_jobs, ltotal_salary

variables <- c("ln_jobs", "ltotal_salary")
variables_labels <- c("Log Number of Jobs", "Log Salary")
if(run_Rais == 1){
  tryCatch({
    for(i in 1:5){
      if(i==1){
        flood_a <- flood_month_after
        flood_b <- flood_month_before2019
        xll <- xl_month
        xuu <- xu_month
        ending <- "_"
        legend_name <- c("2019.01 - 2020.10", "2020.11 - 2022.12")
      }
      if(i==2){
        flood_a <- flood_month_after_balanced
        flood_b <- flood_month_before_balanced2019
        xll <- xl_balanced_month
        xuu <- xu_balanced_month
        ending <- "_balanced_"
        legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==3){
        flood_a <- flood_month_after_balanced_covid
        flood_b <- flood_month_before_balanced_covid
        xll <- xl_balanced_covid_month
        xuu <- xu_balanced_covid_month
        ending <- "_balanced_covid_"
        legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
      }
      if(i==4){
        flood_a <- flood_month_after_balanced
        flood_b <- flood_month_before_balanced
        xll <- xl_balanced_month
        xuu <- xu_balanced_month
        ending <- "_balanced18_"
        legend_name <- c("2018.01 - 2020.10","2020.11 - 2022.12")
      }
      if(i==5){
        flood_a <- flood_month_after_balanced
        flood_b <- flood_month_before_balanced_pre_covid
        xll <- xl_balanced_month
        xuu <- xu_balanced_month
        ending <- "_balanced18_pre_covid_"
        legend_name <- c("2018.01 - 2020.03","2020.11 - 2022.12")
      }
      
      dat_a <- prepare_data("rais_jobs_salaries.dta",flood_a,mun_fe,mun_control)
      dat_b <- prepare_data("rais_jobs_salaries.dta",flood_b,mun_fe,mun_control)
      
      beginning <- paste0("Rais_public",ending)
      for(z in 1:length(variables)){
        twfe2(beginning,variables[[z]],"constant","constant","flood_risk5", list(dat_b,dat_a), c("before","after"))
        print_twfe_month(beginning, variables[[z]], variables_labels[[z]], c("before","after"), legend_name, xll, xuu)
      }
      rm(dat_a,dat_b)
    }
  }, error = function(e) {
    # Handle errors or print a message
    print(paste("Error in RAIS:", e))
  })
}












  
  # ----------------------
  # Graphs
  
  dat <- read_dta(file.path(estban_path,"0-estban_raw.dta"))
  

  dat <- dat %>% 
    select(month,year, codmun, verbete_111_caixa) %>%
    group_by(month,year, codmun) %>%
    summarise(money_inv = sum(verbete_111_caixa, na.rm=TRUE)) %>%
    ungroup() %>%
    mutate(lmoney_inv = log1p(money_inv))
  setDT(dat)
  dat <- dat %>%
    mutate(time_id = (year-1960)*12 + month-1)
  dat$time <- dat$time_id
  dat <- dat %>%
    rename(muni_cd = codmun)
  
  # Collapse everything to the time_id level
  dat_collapse <- dat %>%
    group_by(month,year,time_id) %>%
    summarise(money_inv = sum(money_inv, na.rm=TRUE)) %>%
    ungroup()
  
  # create time_r variable that is month = month, year = year
  dat_collapse <- dat_collapse %>%
    mutate(time_r = paste0(year,"-",month))
  # Exclude if year <2000
  dat_collapse <- dat_collapse %>%
    filter(year >= 2018)
  
  
  # graph money_inv as y and x as time_id
  ggplot(dat_collapse, aes(x = time_id, y = money_inv)) +
    geom_line() +
    labs(title = "Money Inventory Over Time",
         x = "Year",
         y = "Money Inventory") +
    scale_x_continuous(breaks = seq(min(dat_collapse$time_id), max(dat_collapse$time_id), by = 12),  # Set breaks to every 12 months
                       labels = function(x) floor((x/12) + 1960)) +  # Custom labels showing years
    theme_minimal()
  
  
  
  # Collapse everything to the time_id level
  dat_collapse <- dat %>%
    group_by(month,year,time_id) %>%
    summarise(total_ativo_c399y = sum(total_ativo_c399y, na.rm=TRUE),
              deposits = sum(deposits, na.rm = TRUE),
              loans_total = sum(loans_total, na.rm = TRUE),
              assets = sum(assets, na.rm = TRUE),) %>%
    ungroup() %>%
    mutate(ltotal_ativo_c399y = log1p(total_ativo_c399y),
           ldeposits = log1p(deposits),
           lloans_total = log1p(loans_total),
           lassets = log1p(assets))
  dat_collapse <- dat_collapse %>%
    filter(year >= 2018)
  
  # graph money_inv as y and x as time_id
  ggplot(dat_collapse, aes(x = time_id, y = deposits)) +
    geom_line() +
    labs(title = "Money Inventory Over Time",
         x = "Year",
         y = "Money Inventory") +
    scale_x_continuous(breaks = seq(min(dat_collapse$time_id), max(dat_collapse$time_id), by = 12),  # Set breaks to every 12 months
                       labels = function(x) floor((x/12) + 1960)) +  # Custom labels showing years
    theme_minimal()
  
  
  
