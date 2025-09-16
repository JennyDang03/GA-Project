# Run Regressions

library(fixest)
library(stargazer)
library(haven)

# Clear workspace
rm(list = ls())

# Set file paths
log_path <- "//sbcdf176/PIX_Matheus$/Stata/log"
dta_path <- "//sbcdf176/PIX_Matheus$/Stata/dta"
output_path <- "//sbcdf176/PIX_Matheus$/Output"
origdata_path <- "//sbcdf176/PIX_Matheus$/DadosOriginais"

#dta_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta"
#output_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output/tables"

# Load 
dataset <- read_dta(file.path(dta_path,"/pix/fake_panel_data.dta"))
dataset$diff_distance <- dataset$dist_caixa - dataset$expected_distance

fe_formula1 <- after_adoption ~ dist_caixa + expected_distance + after_event + dist_caixa:after_event + expected_distance:after_event | i + t
model1 <- feols(fe_formula1, data = dataset, cluster = "i")
summary(model1)

fe_formula2 <- after_adoption ~ diff_distance + after_event + diff_distance:after_event | i + t
model2 <- feols(fe_formula2, data = dataset, cluster = "i")
summary(model2)

etable(model1, model2,
       file = file.path(output_path,"/fe_model_results.tex"), 
       replace = TRUE)


etable(model1, model2)

modelsummary(list(model1, model2))
