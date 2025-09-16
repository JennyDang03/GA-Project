# Run Regressions

#install.packages("fixest")
#install.packages("stargazer")
#install.packages("haven")
#install.packages("modelsummary")
install.packages("tidyverse")
install.packages("estimatr")


library(fixest)
library(stargazer)
library(haven)
library(modelsummary)
library(tidyverse)
library(estimatr)

# Clear workspace
rm(list = ls())

# Set file paths
log_path <- "//sbcdf176/PIX_Matheus$/Stata/log"
dta_path <- "//sbcdf176/PIX_Matheus$/Stata/dta"
output_path <- "//sbcdf176/PIX_Matheus$/Output"
origdata_path <- "//sbcdf176/PIX_Matheus$/DadosOriginais"

dta_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta"
output_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output/tables"
log_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/log"

sink(file.path(log_path,"/regressions_log_brainstorming.txt"))

# Variables in the original file:
# id index time_id after_first_pix_rec after_first_pix_sent date_first_pix_rec date_first_pix_sent
# after_adoption, date_adoption
# I add after_event later. 
#dist_addresses <- read_dta(file.path(dta_path,'/dist_caixa/dist_caixa_multiprocess7.dta'))

# Load 
print("Loading data")
dataset <- read_dta(file.path(dta_path,"/pix/fake_panel_data.dta"))
dataset$diff_distance <- dataset$dist_caixa - dataset$expected_distance
dataset$after_event = ifelse(dataset$time_id >= 736, 1, 0)
dataset$date_event = 736
# 736 = ym(2021,5) in stata

# Add confidence level 
print("Merging to add confidence level")
aux_address <- read_dta(file.path(dta_path,'/aux_address/aux_address_partial_results7_super_cleaned.dta'))
data_dims <- dim(dataset)
dims_msg <- paste("The dimensions of the dataset before merging are: ", data_dims[1], "and", data_dims[2])
print(dims_msg)
dataset <- merge(dataset, aux_address[, c('index', 'confidence')], by = 'index')
data_dims <- dim(dataset)
dims_msg <- paste("The dimensions of the dataset after merging are: ", data_dims[1], "and", data_dims[2])
print(dims_msg)
# filter out rows with confidence <= 7
dataset <- dataset[dataset$confidence > 7, ] 
data_dims <- dim(dataset)
dims_msg <- paste("The dimensions of the dataset after filtering are: ", data_dims[1], "and", data_dims[2])
print(dims_msg)




# Define Treatment as dist_caixa > expected_distance
print("Define Treatment as dist_caixa > expected_distance")
dataset$treatment <- ifelse(dataset$dist_caixa > dataset$expected_distance,1,0)

# Diff and Diff of after_adoption on Treatment
print("Diff and Diff of after_adoption on treatment")
fe_formula3 <- after_adoption ~ treatment + after_event + treatment:after_event | id + time_id
model3 <- feols(fe_formula3, data = dataset, cluster = "id")
summary(model3)



# Regression of after_adoption on dist_caixa and expected_distance
print("Diff and Diff of after_adoption on dist_caixa and expected_distance")
fe_formula1 <- after_adoption ~ dist_caixa + expected_distance + after_event + dist_caixa:after_event + expected_distance:after_event | id + time_id
model1 <- feols(fe_formula1, data = dataset, cluster = "id")
summary(model1)

#message("Regression on dist_caixa and expected_distance")
#print("Regression on dist_caixa and expected_distance")

# Try event study on dist_caixa controlling for expected_distance




# Regression of after_adoption on diff_distance
print("Diff and Diff of after_adoption on diff_distance")
fe_formula2 <- after_adoption ~ diff_distance + after_event + diff_distance:after_event | id + time_id
model2 <- feols(fe_formula2, data = dataset, cluster = "id")
summary(model2)





# Do the same for after_first_pix_rec after_first_pix_sent

etable(model1, model2, model3, 
       file = file.path(output_path,"/fe_model_results.tex"), 
       replace = TRUE)


etable(model1, model2, model3)

modelsummary(list(model1, model2, model3))


# 
# # Event Study on Treatment
# install.packages("did")
# library(did)
# out <- att_gt(yname = "after_adoption",
#               gname = "treatment",
#               idname = "countyreal",
#               tname = "year",
#               xformla = ~1,
#               data = mpdta,
#               est_method = "reg"
# )
# 
# # calculate average treatment effect for each event window
# event_windows <- 0:9
# ate_by_window <- sapply(event_windows, function(w) {
#   df <- subset(dataset, after_event == 1 & after_adoption == w)
#   mean(df[["dist_caixa"]][df[["treatment"]] == 1]) - 
#     mean(df[["dist_caixa"]][df[["treatment"]] == 0])
# })
# 
# # create a graph of the event study results
# event_study <- data.frame(
#   event_window = event_windows,
#   ate = ate_by_window
# )
# 
# ggplot(event_study, aes(x = event_window, y = ate)) +
#   geom_point() +
#   geom_line() +
#   labs(title = "Event Study Results",
#        x = "Event Window",
#        y = "Average Treatment Effect")



sink()