# Testing y range function
#y_range_function

options(download.file.method = "wininet")

#install.packages(c("data.table","fixest","haven","ggplot2"))

library(data.table) ## For some minor data wrangling
library(fixest)     ## NB: Requires version >=0.9.0
library(haven)
library(ggplot2)
#library(modelsummary)
rm(list = ls()) ## Clear workspace

setwd("//sbcdf176/Pix_Matheus$")

# Set file paths
log_path <- "//sbcdf176/PIX_Matheus$/Stata/log"
dta_path <- "//sbcdf176/PIX_Matheus$/Stata/dta"
output_path <- "//sbcdf176/PIX_Matheus$/Output"
origdata_path <- "//sbcdf176/PIX_Matheus$/DadosOriginais"

log_file <- file.path(log_path, "y_range_function.log")
sink(log_file) ## redirect R output to log file

################################################################################
graph_function2 <- function(y,dat){
  dat$Y <- dat[[y]]
  
  dat_subset <- subset(dat,bank_type %in% c(1) & time_to_treat %in% c(1))
  
  mod_twfe_1 = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time × treatment status
                     muni_cd + week,            ## FEs
                   cluster = ~muni_cd,                          ## Clustered SEs
                   data = dat_subset)

  dat_subset <- dat[bank_type %in% c(2) & tipo %in% c(1)]
  
  mod_twfe_2 = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time × treatment status
                     muni_cd + week,            ## FEs
                   cluster = ~muni_cd,                          ## Clustered SEs
                   data = dat_subset)
  
  return(list(mod_twfe_1, mod_twfe_2))
}

graph_function4 <- function(y,dat,xlimit_l,xlimit_u, main_title){
  models <- graph_function2(y,dat)
  png(file.path(output_path,paste0("y_range_function",y,".png")), width = 640*4, height = 480*4, res = 200)
  iplot(models, sep = 0.5, ref.line = -1,
        xlab = 'Week',
        main = paste("Floods on", main_title),
        ci_level = 0.90, xlim = c(xlimit_l,xlimit_u)) 
  legend("bottomleft", col = c(1, 2, 3), pch = c(20, 17, 15), 
         legend = c("Traditional", "Digital", "Others"), cex = 0.8)
  dev.off()
  print(summary(models[1]))
  return(summary(models[2]))
}



################################################################################
# Flood at the municipality level - Weekly Level
################################################################################

################################
# After Pix
################################


# Load every municipality and every Pix

# Load already prepared data!
dat_flood <- read_dta(file.path(dta_path,"Base_muni_banco_flood_collapsed.dta"))
# converts to data.table
setDT(dat_flood)

dat_flood[, treat := ifelse(is.na(date_flood), 0, 1)]
table(dat_flood$treat)
dat_flood[, time_to_treat := ifelse(treat==1, week - date_flood, 0)]
table(dat_flood$time_to_treat)

# Following Sun and Abraham, we give our never-treated units a fake "treatment" date far outside the relevant study period.
dat_flood[, time_id_treated := ifelse(treat==0, 10000, date_flood)]
table(dat_flood$time_id_treated)

# Set Limits
xlimit_low <- -12
xlimit_up <- 24
xlimits <- seq(ceiling(xlimit_low*1.333),ceiling(xlimit_up*1.333),by=1)
dat_flood <- subset(dat_flood,time_to_treat %in% xlimits)
#

#Pix

# PF
graph_function4("log_valor_ratio",dat_flood, xlimit_low, xlimit_up, "Log Value Ratio")
