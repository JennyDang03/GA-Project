# flood_rais.R

################################################################################
#flood_rais.R
# Input: 
#
# Output: 
#         
# y: 


# The goal: 


# To do: 

################################################################################


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

log_file <- file.path(log_path, "flood_rais.log")
#sink(log_file) ## redirect R output to log file

################################################################################
prepare_data <- function(dta_path,file,xlimit_low, xlimit_up){
  dat_flood <- read_dta(file.path(dta_path,file))
  mun_fe <- read_dta(file.path(dta_path, "mun_fe.dta"))
  dat_flood <- merge(dat_flood, mun_fe)
  # converts to data.table
  setDT(dat_flood)
  
  dat_flood[, treat := ifelse(is.na(date_flood), 0, 1)]
  #table(dat_flood$treat)
  dat_flood[, time_to_treat := ifelse(treat==1, ano - date_flood, 0)]
  #table(dat_flood$time_to_treat)
  # Following Sun and Abraham, we give our never-treated units a fake "treatment" date far outside the relevant study period.
  dat_flood[, time_id_treated := ifelse(treat==0, 10000, date_flood)]
  #table(dat_flood$time_id_treated)
  
  # Set Limits
  xlimits <- seq(ceiling(xlimit_low*1.333-2),ceiling(xlimit_up*1.333+2),by=1)
  dat_flood <- subset(dat_flood,time_to_treat %in% xlimits)
  #
  return(dat_flood)
}

graph_function9 <- function(y,dat){
  dat$Y <- dat[[y]]
  
  mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                     muni_cd + ano,            ## FEs
                   cluster = ~muni_cd,                          ## Clustered SEs
                   data = dat)
  
  return(mod_twfe)
}
graph_function15 <- function(y,dat){
  dat$Y <- dat[[y]]
  
  mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                     muni_cd + ano + flood_risk:ano + rural_urban:ano + nome_regiao_code:ano + pop2010_quart:ano + capital_uf:ano,            ## FEs
                   cluster = ~muni_cd,                          ## Clustered SEs
                   data = dat)
  
  return(mod_twfe)
}
graph_function16 <- function(y,dat){
  dat$Y <- dat[[y]]
  
  mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                     muni_cd + ano + flood_risk:ano,            ## FEs
                   cluster = ~muni_cd,                          ## Clustered SEs
                   data = dat)
  
  return(mod_twfe)
}

graph_function18 <- function(y,dat,xlimit_l,xlimit_u, main_title){
  png(file.path(output_path,paste0("flood_rais_",y,"nocontrol.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function9(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Year',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5))
  
  legend("bottomleft", col = c(1), pch = c(20), 
         legend = c("TWFE"), cex = 0.8)
  dev.off()
  
  png(file.path(output_path,paste0("flood_rais_",y,"control.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function15(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Year',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5))
  
  legend("bottomleft", col = c(1), pch = c(20), 
         legend = c("TWFE"), cex = 0.8)
  dev.off()
  
  png(file.path(output_path,paste0("flood_rais_",y,"control1.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function16(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Year',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5))
  
  legend("bottomleft", col = c(1), pch = c(20), 
         legend = c("TWFE"), cex = 0.8)
  dev.off()
}
################################################################################
xlimit_low <- -7
xlimit_up <- 7

# After Pix - Large vs Small
dat_flood <- prepare_data(dta_path,"rais_flood.dta",xlimit_low, xlimit_up)
graph_function18("log_qt_firms",dat_flood, xlimit_low, xlimit_up, "Log # Firms")
graph_function18("log_qt_jobs",dat_flood, xlimit_low, xlimit_up, "Log # Jobs")

