# Run Regressions

options(download.file.method = "wininet")

#install.packages("fixest")
#install.packages("stargazer")
#install.packages("haven")
#install.packages("modelsummary")

library(fixest)
library(stargazer)
library(haven)
library(modelsummary)

setwd("//sbcdf176/Pix_Matheus$")
# Clear workspace
rm(list = ls())

# Set file paths
log_path <- "//sbcdf176/PIX_Matheus$/Stata/log"
dta_path <- "//sbcdf176/PIX_Matheus$/Stata/dta"
output_path <- "//sbcdf176/PIX_Matheus$/Output"
origdata_path <- "//sbcdf176/PIX_Matheus$/DadosOriginais"

#dta_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta"
#output_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output/tables"
#log_path <- "C:/Users/mathe/Dropbox/RESEARCH/qualpix/pix-event-study/Stata/log"

# Load 
dataset <- read_dta(file.path(dta_path,"/Pix_PF_adoption.dta"))
dataset$diff_distance <- dataset$dist_caixa - dataset$expected_distance
dataset$after_event = ifelse(dataset$time_id >= 736, 1, 0)

sink(file.path(log_path,"/regressions_log.txt"))

fe_formula1 <- after_first_pix_sent ~ dist_caixa + expected_distance + after_event + dist_caixa:after_event + expected_distance:after_event | id + time_id
model1 <- feols(fe_formula1, data = dataset, cluster = "time_id")
summary(model1)

fe_formula2 <- after_first_pix_rec ~ dist_caixa + expected_distance + after_event + dist_caixa:after_event + expected_distance:after_event | id + time_id
model2 <- feols(fe_formula2, data = dataset, cluster = "time_id")
summary(model2)

fe_formula3 <- after_first_pix_sent ~ dist_caixa + expected_distance + after_event + dist_caixa:after_event + expected_distance:after_event | id + time_id
model3 <- feols(fe_formula3, data = dataset, cluster = "id")
summary(model3)

fe_formula4 <- after_first_pix_rec ~ dist_caixa + expected_distance + after_event + dist_caixa:after_event + expected_distance:after_event | id + time_id
model4 <- feols(fe_formula4, data = dataset, cluster = "id")
summary(model4)

etable(model1, model2,model3, model4,
       file = file.path(output_path,"/fe_model_results.tex"), 
       replace = TRUE)


etable(model1, model2, model3, model4)

modelsummary(list(model1, model2,model3, model4))

sink()

