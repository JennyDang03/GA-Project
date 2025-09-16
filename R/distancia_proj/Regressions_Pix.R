#############################################################################
# Selected municipalities

pix_ted_boleto <- read.csv("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/CSV/regression_ready_pix_ted_boleto.csv")

log_transactions <- lm(log_transactions_pix ~ D*caixa_treatment + as.factor(week) + as.factor(id_municipio), data = pix_ted_boleto)

#options(max.print = 100) 
#summary(model)

log_value <- lm(log_value_pix ~ D*caixa_treatment + as.factor(week) + as.factor(id_municipio), data = pix_ted_boleto)

#options(max.print = 100) 
#summary(log_value)


library(stargazer)
stargazer(log_transactions, log_value, type = "latex", 
          column.labels = c("Log Transactions", "Log Value"),
          dep.var.labels.include = FALSE,
          covariate.labels=c("Time Dummy","Treatment Dummy", 
                             "Time x Treatment"),
          title = "Differences in Differences results",
          align = TRUE,
          single.row = TRUE,
          add.lines = list(c('Time FE', 'Yes','Yes'),
                           c('Municipality FE', 'Yes','Yes')),
          omit = "as.factor",
          omit.stat = c("rsq", "f", "ser", "LL"),
          out = "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output/tables/regression_pix_simple_diff_diff3.tex")

log_transactions_continuousdiff <- lm(log_transactions_pix ~ D*closest_bank_d + D*closest_caixa_d + as.factor(week) + as.factor(id_municipio), data = pix_ted_boleto)

#options(max.print = 100) 
#summary(model)

log_value_continuousdiff <- lm(log_value_pix ~ D*closest_bank_d + D*closest_caixa_d + as.factor(week) + as.factor(id_municipio), data = pix_ted_boleto)

#options(max.print = 100) 
#summary(log_value_continuousdiff)


library(stargazer)
stargazer(log_transactions_continuousdiff,log_value_continuousdiff, type = "latex", 
          column.labels = c("Log Transactions", "Log Value"),
          dep.var.labels.include = FALSE,
          covariate.labels=c("Time Dummy","Distance to closest bank", 
                             "Distance to closest Caixa", "Time x Distance to closest bank",
                             "Time x Distance to closest Caixa"),
          title = "Differences in Differences results",
          align = TRUE,
          single.row = TRUE,
          add.lines = list(c('Time FE', 'Yes','Yes'),
                           c('Municipality FE', 'Yes','Yes')),
          omit = "as.factor",
          omit.stat = c("rsq", "f", "ser", "LL"),
          out = "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output/tables/regression_pix_simple_diff_diff_continuous.tex")



############################################
# ALL MUN

#"/home/mcs038/Documents/Pix_regressions/csv/
#C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/csv/
gc(reset = TRUE)
rm(list = ls())


pix_ted_boleto <- read.csv("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/csv/regression_ready_all_mun.csv")
#"C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/csv/regression_ready_pix_ted_boleto_all_mun.csv"

log_transactions <- lm(log_transactions_pix ~ D*caixa_treatment + as.factor(week) + as.factor(id_municipio), data = pix_ted_boleto)

#options(max.print = 100) 
#summary(model)

log_value <- lm(log_value_pix ~ D*caixa_treatment + as.factor(week) + as.factor(id_municipio), data = pix_ted_boleto)

#options(max.print = 100) 
#summary(log_value)


library(stargazer)
stargazer(log_transactions, log_value, type = "latex", 
          column.labels = c("Log Transactions", "Log Value"),
          dep.var.labels.include = FALSE,
          covariate.labels=c("Time Dummy","Treatment Dummy", 
                             "Time x Treatment"),
          title = "Differences in Differences results",
          align = TRUE,
          single.row = TRUE,
          add.lines = list(c('Time FE', 'Yes','Yes'),
                           c('Municipality FE', 'Yes','Yes')),
          omit = "as.factor",
          omit.stat = c("rsq", "f", "ser", "LL"),
          out = "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output/tables/regression_pix_simple_diff_diff3_all_mun.tex")


gc(reset = TRUE)
rm(list = ls())

pix_ted_boleto <- read.csv("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/CSV/regression_ready_pix_ted_boleto_all_mun.csv")


log_transactions_continuousdiff <- lm(log_transactions_pix ~ D*closest_bank_d + D*closest_caixa_d + as.factor(week) + as.factor(id_municipio), data = pix_ted_boleto)

#options(max.print = 100) 
#summary(model)

log_value_continuousdiff <- lm(log_value_pix ~ D*closest_bank_d + D*closest_caixa_d + as.factor(week) + as.factor(id_municipio), data = pix_ted_boleto)

#options(max.print = 100) 
#summary(log_value_continuousdiff)


library(stargazer)
stargazer(log_transactions_continuousdiff,log_value_continuousdiff, type = "latex", 
          column.labels = c("Log Transactions", "Log Value"),
          dep.var.labels.include = FALSE,
          covariate.labels=c("Time Dummy","Distance to closest bank", 
                             "Distance to closest Caixa", "Time x Distance to closest bank",
                             "Time x Distance to closest Caixa"),
          title = "Differences in Differences results",
          align = TRUE,
          single.row = TRUE,
          add.lines = list(c('Time FE', 'Yes','Yes'),
                           c('Municipality FE', 'Yes','Yes')),
          omit = "as.factor",
          omit.stat = c("rsq", "f", "ser", "LL"),
          out = "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output/tables/regression_pix_simple_diff_diff_continuous_all_mun.tex")




############################################
# ALL MUN - Non-Random Exposure to Exogenous Shocks

#"/home/mcs038/Documents/Pix_regressions/csv/
#C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/csv/

gc(reset = TRUE)
rm(list = ls())

pix_ted_boleto <- read.csv("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/csv/regression_ready_all_mun.csv")
#pix_ted_boleto <- read.csv("/home/mcs038/Documents/Pix_regressions/csv/regression_ready_all_mun.csv")


#n_sample <- 1000
#sampled_pix <- pix_ted_boleto[sample(nrow(pix_ted_boleto),n_sample)]


# Fraction of the subjects to sample
sampling_pct = 0.5
# Obtain an array with unique subject IDs
subject_ids = unique(pix_ted_boleto$id_municipio)
# Sample from the subject ids
sample_subject_ids = sample(subject_ids, round(sampling_pct * length(subject_ids)))
# Get the rows for the sampled subjects
sample_df = subset(pix_ted_boleto, id_municipio %in% sample_subject_ids)









log_transactions_continuousdiff <- lm(log_transactions_pix ~ D*closest_bank_d + D*closest_Caixa_d + D*expected_distance + as.factor(week) + as.factor(id_municipio), data = sample_df)

#options(max.print = 100) 
#summary(model)

log_value_continuousdiff <- lm(log_value_pix ~ D*closest_bank_d + D*closest_Caixa_d + D*expected_distance + as.factor(week) + as.factor(id_municipio), data = sample_df)

#options(max.print = 100) 
#summary(log_value_continuousdiff)

library(stargazer)
stargazer(log_transactions_continuousdiff,log_value_continuousdiff, type = "latex", 
          column.labels = c("Log Transactions", "Log Value"),
          dep.var.labels.include = FALSE,
          title = "Differences in Differences results",
          align = TRUE,
          single.row = TRUE,
          add.lines = list(c('Time FE', 'Yes','Yes'),
                           c('Municipality FE', 'Yes','Yes')),
          omit = "as.factor",
          omit.stat = c("rsq", "f", "ser", "LL"),
          out = "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output/tables/regression_pix_BH_sample1.tex")

#/home/mcs038/Documents/Pix_regressions/Output/tables/regression_pix_BH_sample.tex"

#covariate.labels=c("Time Dummy","Distance to closest bank", 
#                   "Distance to closest Caixa", "Expected distance" "Time x Distance to closest bank",
#                   "Time x Distance to closest Caixa", "Time x Expected distance),
#          out = "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output/tables/regression_pix_simple_diff_diff_continuous_all_mun.tex")



log_transactions_continuousdiff <- lm(log_transactions_pix ~ D*closest_bank_d + D*centered_closest_Caixa_d + as.factor(week) + as.factor(id_municipio), data = sample_df)

#options(max.print = 100) 
#summary(model)

log_value_continuousdiff <- lm(log_value_pix ~ D*closest_bank_d + D*centered_closest_Caixa_d + as.factor(week) + as.factor(id_municipio), data = sample_df)

#options(max.print = 100) 
#summary(log_value_continuousdiff)

library(stargazer)
stargazer(log_transactions_continuousdiff,log_value_continuousdiff, type = "latex", 
          column.labels = c("Log Transactions", "Log Value"),
          dep.var.labels.include = FALSE,
          title = "Differences in Differences results",
          align = TRUE,
          single.row = TRUE,
          add.lines = list(c('Time FE', 'Yes','Yes'),
                           c('Municipality FE', 'Yes','Yes')),
          omit = "as.factor",
          omit.stat = c("rsq", "f", "ser", "LL"),
          out = "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output/tables/regression_pix_BH_sample2.tex")





log_transactions_continuousdiff <- lm(log_transactions_pix ~ D*centered_closest_Caixa_d + as.factor(week) + as.factor(id_municipio), data = sample_df)

#options(max.print = 100) 
#summary(model)

log_value_continuousdiff <- lm(log_value_pix ~ D*centered_closest_Caixa_d + as.factor(week) + as.factor(id_municipio), data = sample_df)

#options(max.print = 100) 
#summary(log_value_continuousdiff)

library(stargazer)
stargazer(log_transactions_continuousdiff,log_value_continuousdiff, type = "latex", 
          column.labels = c("Log Transactions", "Log Value"),
          dep.var.labels.include = FALSE,
          title = "Differences in Differences results",
          align = TRUE,
          single.row = TRUE,
          add.lines = list(c('Time FE', 'Yes','Yes'),
                           c('Municipality FE', 'Yes','Yes')),
          omit = "as.factor",
          omit.stat = c("rsq", "f", "ser", "LL"),
          out = "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output/tables/regression_pix_BH_sample3.tex")








log_transactions_continuousdiff <- lm(log_transactions_pix ~ D*closest_Caixa_d + D*expected_distance + as.factor(week) + as.factor(id_municipio), data = sample_df)

#options(max.print = 100) 
#summary(model)

log_value_continuousdiff <- lm(log_value_pix ~ D*closest_Caixa_d + D*expected_distance + as.factor(week) + as.factor(id_municipio), data = sample_df)

#options(max.print = 100) 
#summary(log_value_continuousdiff)

library(stargazer)
stargazer(log_transactions_continuousdiff,log_value_continuousdiff, type = "latex", 
          column.labels = c("Log Transactions", "Log Value"),
          dep.var.labels.include = FALSE,
          title = "Differences in Differences results",
          align = TRUE,
          single.row = TRUE,
          add.lines = list(c('Time FE', 'Yes','Yes'),
                           c('Municipality FE', 'Yes','Yes')),
          omit = "as.factor",
          omit.stat = c("rsq", "f", "ser", "LL"),
          out = "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output/tables/regression_pix_BH_sample4.tex")















































