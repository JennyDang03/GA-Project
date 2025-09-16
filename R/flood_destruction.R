#flood_destruction.R



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
log_path <- "//sbcdf176/PIX_Matheus$/Stata/log"
dta_path <- "//sbcdf176/PIX_Matheus$/Stata/dta"
output_path <- "//sbcdf176/PIX_Matheus$/Output"
origdata_path <- "//sbcdf176/PIX_Matheus$/DadosOriginais"
R_path <- "//sbcdf176/PIX_Matheus$/R"
#dta_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta"
#output_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output/tables"
#log_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/log"

log_file <- file.path(log_path, "flood_destruction.log")
#sink(log_file) ## redirect R output to log file

# Graph TWFE and SA
################################################################################
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
  png(file.path(output_path,paste0("flood_destruction_",y,"nocontrol.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function9(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Year',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5))
  
  legend("bottomleft", col = c(1), pch = c(20), 
         legend = c("TWFE"), cex = 0.8)
  dev.off()
  
  png(file.path(output_path,paste0("flood_destruction_",y,"control.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function15(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Year',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5))
  
  legend("bottomleft", col = c(1), pch = c(20), 
         legend = c("TWFE"), cex = 0.8)
  dev.off()
  
  png(file.path(output_path,paste0("flood_destruction_",y,"control1.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function16(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Year',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5))
  
  legend("bottomleft", col = c(1), pch = c(20), 
         legend = c("TWFE"), cex = 0.8)
  dev.off()
}

################################################################################
# Flood at the Municipal level - Monthly Level
################################################################################

#####################
# After Pix
#####################





# converts to data.table
setDT(dat_flood)

dat_flood[, treat := ifelse(is.na(date_flood), 0, 1)]
table(dat_flood$treat)
dat_flood[, time_to_treat := ifelse(treat==1, ano - date_flood, 0)]
table(dat_flood$time_to_treat)
# Following Sun and Abraham, we give our never-treated units a fake "treatment" date far outside the relevant study period.
dat_flood[, time_id_treated := ifelse(treat==0, 1000000, date_flood)]
table(dat_flood$time_id_treated)

# Set Limits
xlimit_low <- -7
xlimit_up <- 7
xlimits <- seq(ceiling(xlimit_low*1.1-10),ceiling(xlimit_up*1.1+10),by=1)
dat_flood <- subset(dat_flood,time_to_treat %in% xlimits)
#


dat_flood[, log_pib := log(pib + 1)]
graph_function18("log_pib",dat_flood, xlimit_low, xlimit_up, "Log GDP")
graph_function18("log_production",dat_flood, xlimit_low, xlimit_up, "Log Production")
graph_function18("log_production_animal",dat_flood, xlimit_low, xlimit_up, "Log Production")
graph_function18("log_production_fish",dat_flood, xlimit_low, xlimit_up, "Log Production")
graph_function18("log_production_perm",dat_flood, xlimit_low, xlimit_up, "Log Production")
graph_function18("log_production_temp",dat_flood, xlimit_low, xlimit_up, "Log Production")







# Load already prepared data!
dat_flood <- read_dta(file.path(dta_path,"flood_pib_mun.dta"))
mun_fe <- read_dta(file.path(dta_path, "mun_fe.dta"))
dat_flood <- merge(dat_flood, mun_fe)

ppm_pam <- read_dta(file.path(dta_path, "ppm_pam_flood.dta"))
ppm_pam <- ppm_pam %>%
  select(-after_flood, -date_flood, -flood, -number_disasters)
dat_flood <- merge(dat_flood, ppm_pam, by = c("ano","id_municipio"), all.x = TRUE, all.y = TRUE)

# converts to data.table
setDT(dat_flood)

dat_flood[, treat := ifelse(is.na(date_flood), 0, 1)]
table(dat_flood$treat)
dat_flood[, time_to_treat := ifelse(treat==1, ano - date_flood, 0)]
table(dat_flood$time_to_treat)
# Following Sun and Abraham, we give our never-treated units a fake "treatment" date far outside the relevant study period.
dat_flood[, time_id_treated := ifelse(treat==0, 1000000, date_flood)]
table(dat_flood$time_id_treated)
dat_flood[, time := ano]
dat_flood[, constant := 0]
dat_flood[, log_pib := log(pib + 1)]

twfe2 <- function(graphname,y,control1,control2,fe,dat_list,dat_list_name){
  #mod_list <- list()
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
                      lean =TRUE, mem.clean = TRUE,        ## Saves memory and RAM.
                      data = dat)
    
    #mod_twfe$fml <- NULL
    #saveRDS(mod_twfe, file = file.path(output_path,paste0("/coefficients/",graphname,y,dat_list_name[i],".rds")))
    #mod_list[[i]] <- mod_twfe
    # I tried saving the fixest file but it is too heavy. 
    
    coeftable_mod <- summary(mod_twfe)$coeftable
    coefficients <- coeftable_mod[, "Estimate"]
    standard_errors <- coeftable_mod[, "Std. Error"]
    variables <- rownames(coeftable_mod)
    variables <- as.numeric(gsub("time_to_treat::(.+):treat", "\\1", variables))
    ci95_u <- coefficients + qnorm(0.975) * standard_errors  # Upper bound
    ci95_l <- coefficients - qnorm(0.975) * standard_errors  # Lower bound
    #Put variables, coefficients, and standard_errors together and save in RDS
    data_list <- list()
    data_list <- list(variables = variables,
                      coefficients = coefficients,
                      standard_errors = standard_errors,
                      ci95_u = ci95_u,
                      ci95_l = ci95_l)
    # Save the list as an RDS file
    saveRDS(data_list, file = file.path(output_path, paste0("/coefficients/", graphname, y, dat_list_name[i], "2.rds")))
  }
  #return(mod_list)
  return()
}

print_twfe_year <- function(graphname, y, main_title, dat_list_name, legend_list, xlimit_l, xlimit_u){
  mod_list <- list()
  for (i in 1:length(dat_list_name)) {
    mod_list[[i]] <- readRDS(file = file.path(output_path,paste0("/coefficients/",graphname,y,dat_list_name[i],"2.rds")))
    # Maybe exclude the points after xlimit_u and before xlimit_l - Better looking graphs but ylim_function2 is doing that already.
  }
  pch_list = c(16,17,15,1,2,0,5,3) # 18 didnt work
  col_list = c("black", "red", "green", "blue", "purple", "orange", "cyan", "magenta", "yellow")
  png(file.path(output_path,paste0("graphs/",graphname,y,".png")), width = 640*4, height = 480*4, res = 200)
  par(cex.main = 2.5, cex.lab = 2, cex.axis = 2)
  #sep = 0.5, ref.line = -1
  plot(0, xlim = c(xlimit_l-0.1,xlimit_u+0.6), ylim = ylim_function2(mod_list, dat_list_name, xlimit_l, xlimit_u), 
       xlab = "Years", ylab = "", main = main_title, type = "n")
  grid()
  abline(h = 0)
  abline(v = -1, lty = 2)
  for (i in 1:length(dat_list_name)) {
    points(mod_list[[i]]$variables+(i-1)*0.5, mod_list[[i]]$coefficients, pch = pch_list[i], col = col_list[i], cex=1.2) # , type = "b"
    segments(mod_list[[i]]$variables+(i-1)*0.5, mod_list[[i]]$ci95_l, mod_list[[i]]$variables+(i-1)*0.5, mod_list[[i]]$ci95_u, col = col_list[i], lwd = 2)
    arrows(mod_list[[i]]$variables+(i-1)*0.5, mod_list[[i]]$ci95_l, mod_list[[i]]$variables+(i-1)*0.5, mod_list[[i]]$ci95_u, angle = 90, code = 3, length = 0.05, col = col_list[i], lwd = 2)
    # Add the missing dots at x=-1
    points(-1+(i-1)*0.5, 0, pch = pch_list[i], col = col_list[i], cex=1.2)
  }
  legend("bottomleft", col = col_list, pch = pch_list, 
         legend = legend_list, cex = 1.8)
  dev.off()
  
  
  
  for (j in 1:length(dat_list_name)) {
    mod_list <- list()
    mod_list[[1]] <- readRDS(file = file.path(output_path,paste0("/coefficients/",graphname,y,dat_list_name[j],"2.rds")))
    
    
    pch_list = c(16,17,15,1,2,0,5,3) # 18 didnt work
    col_list = c("black", "red", "green", "blue", "purple", "orange", "cyan", "magenta", "yellow")
    
    pch_list = pch_list[j]
    col_list = col_list[j]
    
    png(file.path(output_path,paste0("graphs/",graphname,y,"_", j,".png")), width = 640*4, height = 480*4, res = 200)
    par(cex.main = 2.5, cex.lab = 2, cex.axis = 2)
    #sep = 0.5, ref.line = -1
    plot(0, xlim = c(xlimit_l-0.1,xlimit_u+0.6), ylim = ylim_function2(mod_list, list(1), xlimit_l, xlimit_u), 
         xlab = "Years", ylab = "", main = main_title, type = "n")
    grid()
    abline(h = 0)
    abline(v = -1, lty = 2)
    for (i in 1:1) {
      points(mod_list[[i]]$variables+(i-1)*0.5, mod_list[[i]]$coefficients, pch = pch_list[i], col = col_list[i], cex=1.5) # , type = "b"
      segments(mod_list[[i]]$variables+(i-1)*0.5, mod_list[[i]]$ci95_l, mod_list[[i]]$variables+(i-1)*0.5, mod_list[[i]]$ci95_u, col = col_list[i], lwd = 2)
      arrows(mod_list[[i]]$variables+(i-1)*0.5, mod_list[[i]]$ci95_l, mod_list[[i]]$variables+(i-1)*0.5, mod_list[[i]]$ci95_u, angle = 90, code = 3, length = 0.05, col = col_list[i], lwd = 2)
      # Add the missing dots at x=-1
      points(-1+(i-1)*0.5, 0, pch = pch_list[i], col = col_list[i], cex=1)
    }
    legend("bottomleft", col = col_list, pch = pch_list, 
           legend = legend_list[j], cex = 1.8)
    dev.off()
  }
}

variables <- c("log_pib", "log_production", "log_production_animal", "log_production_fish", "log_production_perm", "log_production_temp")
variables_labels <- c("Log GDP", "Log Production", "Log Production Animal", "Log Production Fish", "Log Production Perm", "Log Production Temp")
tryCatch({
  for(i in 1:1){
    if(i==1){
      flood_a <- flood_week_after
      flood_b <- flood_week_before2019
      xll <- xl
      xuu <- xu
      ending <- ""
      beginning <- paste0("Test_", ending)
      legend_name <- c("")
    }
    if(i==2){
      flood_a <- flood_week_after_balanced
      flood_b <- flood_week_before_balanced2019
      xll <- xl_balanced
      xuu <- xu_balanced
      ending <- "balanced_"
      beginning <- paste0("Ted_", ending)
      beginning_PF <- paste0("Ted_PF_", ending)
      beginning_PJ <- paste0("Ted_PJ_", ending)
      beginning_PF2 <- paste0("Ted_PF_2", ending)
      beginning_PJ2 <- paste0("Ted_PJ_2", ending)
      legend_name <- c("2019.01 - 2020.10","2020.11 - 2022.12")
    }
    if(i==3){
      flood_a <- flood_week_after_balanced_covid
      flood_b <- flood_week_before_balanced_covid
      xll <- xl_balanced_covid
      xuu <- xu_balanced_covid
      ending <- "balanced_covid_"
      beginning <- paste0("Ted_", ending)
      beginning_PF <- paste0("Ted_PF_", ending)
      beginning_PJ <- paste0("Ted_PJ_", ending)
      beginning_PF2 <- paste0("Ted_PF_2", ending)
      beginning_PJ2 <- paste0("Ted_PJ_2", ending)
      legend_name <- c("2020.03 - 2020.10","2020.11 - 2021.06")
    }
    for(z in 1:length(variables)){
      twfe2(beginning,variables[[z]],"constant","constant","flood_risk5", list(dat_flood), c("test"))
      print_twfe_year(beginning, variables[[z]], variables_labels[[z]], c("test"), c("test"), -7, 7)
      
    }
  }
}, error = function(e) {
  print(paste("Error in Send Rec3:", e))
})
