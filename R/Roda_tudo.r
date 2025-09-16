#Roda_tudo

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
library(xtable)
library(stargazer)
library(data.table)
library(fixest)
library(haven)
library(ggplot2)
library(lfe)
library(AER)
library("ivreg")
library("plm")
library(wktmo)
library(lubridate)

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

##################################################
#Query
run_Download_sqls_more_data <- 1
run_Download_sqls_ind_sample <- 1
run_Download_TED <- 0
run_Processa_sqls.R <- 0
run_flood_event_studies_estimates <- 0
run_iv_municipality <- 1
##################################################
if(run_Download_sqls_more_data == 1){source(file.path(file.path(path_query, "Download_sqls_more_data.R")))} 
if(run_Download_sqls_ind_sample == 1){source(file.path(file.path(path_query, "Download_sqls_ind_sample.R")))}
if(run_Download_TED == 1){source(file.path(file.path(path_query, "Download_TED.R")))}
if(run_Processa_sqls.R == 1){source(file.path(file.path(R_path, "Processa_sqls.R")))}
if(run_flood_event_studies_estimates == 1){source(file.path(file.path(R_path, "flood_event_studies_estimates.R")))}
if(run_iv_municipality == 1){source(file.path(file.path(R_path, "iv_municipality.R")))}
