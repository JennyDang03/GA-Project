#https://lost-stats.github.io/Model_Estimation/Research_Design/event_study.html

options(download.file.method = "wininet")

#install.packages("data.table")
#install.packages("fixest")
#install.packages("haven")
#install.packages("ggplot2")

library(data.table) ## For some minor data wrangling
library(fixest)     ## NB: Requires version >=0.9.0
library(haven)
library(ggplot2)

# Clear workspace
rm(list = ls())

setwd("//sbcdf176/Pix_Matheus$")
# Set file paths
log_path <- "//sbcdf176/PIX_Matheus$/Stata/log"
dta_path <- "//sbcdf176/PIX_Matheus$/Stata/dta"
output_path <- "//sbcdf176/PIX_Matheus$/Output"
origdata_path <- "//sbcdf176/PIX_Matheus$/DadosOriginais"
#dta_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta"
#output_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output/tables"
#log_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/log"

log_file <- file.path(log_path, "flood_SA_v1.log")
sink(log_file) # redirect R output to log file

# Load already prepared data!
dat_flood <- read_dta(file.path(dta_path,"/flood_pix_fake.dta"))
setDT(dat_flood)

dat_flood$time_id <- as.numeric(dat_flood$time_id)
dat_flood$date_flood <- as.numeric(dat_flood$date_flood)

dat_flood[, treat := ifelse(is.na(`date_flood`), 0, 1)]
dat_flood[, time_to_treat := ifelse(treat==1, time_id - `date_flood`, 0)]

# Following Sun and Abraham, we give our never-treated units a fake "treatment"
# date far outside the relevant study period.
dat_flood[, time_id_treated := ifelse(treat==0, 1000000, `date_flood`)]

dat_flood$time_to_treat <- as.numeric(dat_flood$time_to_treat)
dat_flood$treat <- as.numeric(dat_flood$treat)
dat_flood$id <- as.numeric(dat_flood$id)
dat_flood$muni_cd <- as.numeric(dat_flood$muni_cd)
dat_flood$time_id_treated <- as.numeric(dat_flood$time_id_treated)

#dat_flood$time_to_treat <- factor(dat_flood$time_to_treat)
#dat_flood$treat <- factor(dat_flood$treat)
#dat_flood$id <- factor(dat_flood$id)
#dat_flood$muni_cd <- factor(dat_flood$muni_cd)
#dat_flood$time_id_treated <- factor(dat_flood$time_id_treated)

################################################################################
# after_first_pix_rec
################################################################################
dat_flood$Y <- dat_flood$after_first_pix_rec

#mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time × treatment status
#                   id + time_id,            ## FEs
#                 cluster = ~muni_cd,                          ## Clustered SEs
#                 data = dat_flood)
# See `?sunab`.
mod_sa = feols(Y ~ sunab(time_id_treated, time_id) | ## The only thing that's changed
                 id + time_id,            ## FEs
               cluster = ~muni_cd,
               data = dat_flood)

png(file.path(output_path,"/pix_flood_receiver_adoption.png"), width = 640*4, height = 480*4, res = 200)
iplot(list(mod_sa), sep = 0.5, ref.line = -1,
      xlab = 'Time to treatment',
      main = 'Event study: Floods on Receiver Adoption',
      ci_level = 0.90)
legend("bottomleft", col = c(2), pch = c(17), 
       legend = c("Sun & Abraham (2020)"))
dev.off()


################################################################################
# after_first_pix_sent
################################################################################
dat_flood$Y <- dat_flood$after_first_pix_sent

#mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time × treatment status
#                   id + time_id,            ## FEs
#                 cluster = ~muni_cd,                          ## Clustered SEs
#                 data = dat_flood)
# See `?sunab`.
mod_sa = feols(Y ~ sunab(time_id_treated, time_id) | ## The only thing that's changed
                 id + time_id,            ## FEs
               cluster = ~muni_cd,
               data = dat_flood)

png(file.path(output_path,"/pix_flood_sender_adoption.png"), width = 640*4, height = 480*4, res = 200)
iplot(list(mod_sa), sep = 0.5, ref.line = -1,
      xlab = 'Time to treatment',
      main = 'Event study: Floods on Sender Adoption',
      ci_level = 0.90)
legend("bottomleft", col = c(1), pch = c(20), 
       legend = c("Sun & Abraham (2020)"))
dev.off()
################################################################################
# receiver 
################################################################################
dat_flood$Y <- dat_flood$receiver

#mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time × treatment status
#                   id + time_id,            ## FEs
#                 cluster = ~muni_cd,                          ## Clustered SEs
#                 data = dat_flood)
# See `?sunab`.
mod_sa = feols(Y ~ sunab(time_id_treated, time_id) | ## The only thing that's changed
                 id + time_id,            ## FEs
               cluster = ~muni_cd,
               data = dat_flood)

png(file.path(output_path,"/pix_flood_receiver.png"), width = 640*4, height = 480*4, res = 200)
iplot(list(mod_sa), sep = 0.5, ref.line = -1,
      xlab = 'Time to treatment',
      main = 'Event study: Floods on Receivers',
      ci_level = 0.90)
legend("bottomleft", col = c(1), pch = c(20), 
       legend = c("Sun & Abraham (2020)"))
dev.off()

################################################################################
# trans_rec
################################################################################
dat_flood$Y <- dat_flood$trans_rec

#mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time × treatment status
#                   id + time_id,            ## FEs
#                 cluster = ~muni_cd,                          ## Clustered SEs
#                 data = dat_flood)
# See `?sunab`.
mod_sa = feols(Y ~ sunab(time_id_treated, time_id) | ## The only thing that's changed
                 id + time_id,            ## FEs
               cluster = ~muni_cd,
               data = dat_flood)

png(file.path(output_path,"/pix_flood_trans_rec.png"), width = 640*4, height = 480*4, res = 200)
iplot(list(mod_sa), sep = 0.5, ref.line = -1,
      xlab = 'Time to treatment',
      main = 'Event study: Floods on Transactions Received',
      ci_level = 0.90)
legend("bottomleft", col = c(1), pch = c(20), 
       legend = c("Sun & Abraham (2020)"))
dev.off()

################################################################################
# value_rec
################################################################################
dat_flood$Y <- dat_flood$value_rec

#mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time × treatment status
#                   id + time_id,            ## FEs
#                 cluster = ~muni_cd,                          ## Clustered SEs
#                 data = dat_flood)
# See `?sunab`.
mod_sa = feols(Y ~ sunab(time_id_treated, time_id) | ## The only thing that's changed
                 id + time_id,            ## FEs
               cluster = ~muni_cd,
               data = dat_flood)

png(file.path(output_path,"/pix_flood_value_rec.png"), width = 640*4, height = 480*4, res = 200)
iplot(list(mod_sa), sep = 0.5, ref.line = -1,
      xlab = 'Time to treatment',
      main = 'Event study: Floods on Value Received',
      ci_level = 0.90)
legend("bottomleft", col = c(1), pch = c(20), 
       legend = c("Sun & Abraham (2020)"))
dev.off()

sink()