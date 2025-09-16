#https://lost-stats.github.io/Model_Estimation/Research_Design/event_study.html

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
#dta_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta"
#output_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output/tables"
#log_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/log"

log_file <- file.path(log_path, "flood_SA_v1_R.log")
sink(log_file) ## redirect R output to log file

# Load already prepared data!
dat_flood <- read_dta(file.path(dta_path,"/flood_pix_fake.dta"))
dat_flood$time_id <- as.numeric(dat_flood$time_id)
dat_flood$date_flood <- as.numeric(dat_flood$date_flood)
setDT(dat_flood)
dat_flood[, treat := ifelse(is.na(date_flood), 0, 1)]
table(dat_flood$treat)
dat_flood[, time_to_treat := ifelse(treat==1, time_id - date_flood, 0)]
table(dat_flood$time_to_treat)
# Following Sun and Abraham, we give our never-treated units a fake "treatment" date far outside the relevant study period.
dat_flood[, time_id_treated := ifelse(treat==0, 1000000, date_flood)]
table(dat_flood$time_id_treated)

dat_flood$time_to_treat <- as.numeric(dat_flood$time_to_treat)
dat_flood$treat <- as.numeric(dat_flood$treat)
dat_flood$id <- as.numeric(dat_flood$id)
dat_flood$muni_cd <- as.numeric(dat_flood$muni_cd)
dat_flood$time_id_treated <- as.numeric(dat_flood$time_id_treated)

##
# Function to determine range of the Y graph
y_range_function <- function(x_range_min,x_range_max,mod_twfe,mod_sa){
  coef_list_twfe <- mod_twfe[["model_matrix_info"]][[1]][["items"]]
  #coef_list_sa <- mod_sa[["model_matrix_info"]][[1]][["items"]]
  min_integer <- coef_list_twfe[1]
  max_integer <- coef_list_twfe[length(coef_list_twfe)]
  if (x_range_min<min_integer){
    x_range_min <- min_integer
  }
  if (x_range_max>=max_integer){
    x_range_max <- max_integer-0.5 ## Strange things happen when x_range_max = max_integer
  }
  x_twfe_min <- x_range_min-min_integer+1
  x_twfe_max <- x_range_max-min_integer+1-1 ## There is no coef -1
  x_sa_min <- (x_range_min-min_integer)*2
  x_sa_max <- (x_range_max-min_integer)*2-1 ## Except when max = max_integer
  
  coefficients_twfe <- mod_twfe[["coefficients"]]
  se_twfe <- mod_twfe[["se"]]
  
  coefficients_sa <- mod_sa[["coefficients"]]
  se_sa <- mod_sa[["se"]]
  
  # Calculate the upper bounds for each predicted value
  upper_bounds_twfe <- coefficients_twfe + 2 * se_twfe
  upper_bounds_sa <- coefficients_sa + 2 * se_sa
  lower_bounds_twfe <- coefficients_twfe - 2 * se_twfe
  lower_bounds_sa <- coefficients_sa - 2 * se_sa
  
  # Determine the maximum value among the upper bounds
  max_value <- max(max(upper_bounds_twfe[x_twfe_min:x_twfe_max]), max(upper_bounds_sa[x_sa_min:x_sa_max]))
  min_value <- min(min(lower_bounds_twfe[x_twfe_min:x_twfe_max]), min(lower_bounds_sa[x_sa_min:x_sa_max]))
  y_range <- c(min_value, max_value)
  return(y_range)
}
##


################################################################################
# after_first_pix_rec
################################################################################
dat_flood$Y <- dat_flood$after_first_pix_rec

mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time × treatment status
                   id + time_id,            ## FEs
                 cluster = ~muni_cd,                          ## Clustered SEs
                 data = dat_flood)
# See `?sunab`.
mod_sa = feols(Y ~ sunab(time_id_treated, time_id) | ## The only thing that's changed
                 id + time_id,            ## FEs
               cluster = ~muni_cd,
               data = dat_flood)

png(file.path(output_path,"/pix_flood_receiver_adoption.png"), width = 640*4, height = 480*4, res = 200)
iplot(list(mod_twfe, mod_sa), sep = 0.5, ref.line = -1,
      xlab = 'Time to treatment',
      main = 'Event study: Floods on Receiver Adoption',
      ci_level = 0.90, xlim = c(-6,6), ylim = c(-0.025,0.025))
# y_range_function(-6,6,mod_twfe,mod_sa)
legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
       legend = c("TWFE", "Sun & Abraham (2020)"), cex = 0.8)
dev.off()


################################################################################
# after_first_pix_sent
################################################################################
dat_flood$Y <- dat_flood$after_first_pix_sent

mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time × treatment status
                   id + time_id,            ## FEs
                 cluster = ~muni_cd,                          ## Clustered SEs
                 data = dat_flood)
# See `?sunab`.
mod_sa = feols(Y ~ sunab(time_id_treated, time_id) | ## The only thing that's changed
                 id + time_id,            ## FEs
               cluster = ~muni_cd,
               data = dat_flood)

png(file.path(output_path,"/pix_flood_sender_adoption.png"), width = 640*4, height = 480*4, res = 200)
iplot(list(mod_twfe, mod_sa), sep = 0.5, ref.line = -1,
      xlab = 'Time to treatment',
      main = 'Event study: Floods on Sender Adoption',
      ci_level = 0.90, xlim = c(-6,6), ylim = c(-0.025,0.025))
legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
       legend = c("TWFE", "Sun & Abraham (2020)"), cex = 0.8)
dev.off()
################################################################################
# receiver 
################################################################################
dat_flood$Y <- dat_flood$receiver

mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time × treatment status
                   id + time_id,            ## FEs
                 cluster = ~muni_cd,                          ## Clustered SEs
                 data = dat_flood)
# See `?sunab`.
mod_sa = feols(Y ~ sunab(time_id_treated, time_id) | ## The only thing that's changed
                 id + time_id,            ## FEs
               cluster = ~muni_cd,
               data = dat_flood)

png(file.path(output_path,"/pix_flood_receiver.png"), width = 640*4, height = 480*4, res = 200)
iplot(list(mod_twfe, mod_sa), sep = 0.5, ref.line = -1,
      xlab = 'Time to treatment',
      main = 'Event study: Floods on Receivers',
      ci_level = 0.90, xlim = c(-6,6), ylim = c(-0.025,0.025))
legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
       legend = c("TWFE", "Sun & Abraham (2020)"), cex = 0.8)
dev.off()

################################################################################
# sender 
################################################################################
dat_flood$Y <- dat_flood$sender

mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time × treatment status
                   id + time_id,            ## FEs
                 cluster = ~muni_cd,                          ## Clustered SEs
                 data = dat_flood)
# See `?sunab`.
mod_sa = feols(Y ~ sunab(time_id_treated, time_id) | ## The only thing that's changed
                 id + time_id,            ## FEs
               cluster = ~muni_cd,
               data = dat_flood)

png(file.path(output_path,"/pix_flood_sender.png"), width = 640*4, height = 480*4, res = 200)
iplot(list(mod_twfe, mod_sa), sep = 0.5, ref.line = -1,
      xlab = 'Time to treatment',
      main = 'Event study: Floods on Senders',
      ci_level = 0.90, xlim = c(-6,6), ylim = c(-0.025,0.025))
legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
       legend = c("TWFE", "Sun & Abraham (2020)"), cex = 0.8)
dev.off()

################################################################################
# trans_rec
################################################################################
dat_flood$Y <- dat_flood$trans_rec

mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time × treatment status
                   id + time_id,            ## FEs
                 cluster = ~muni_cd,                          ## Clustered SEs
                 data = dat_flood)
# See `?sunab`.
mod_sa = feols(Y ~ sunab(time_id_treated, time_id) | ## The only thing that's changed
                 id + time_id,            ## FEs
               cluster = ~muni_cd,
               data = dat_flood)

png(file.path(output_path,"/pix_flood_trans_rec.png"), width = 640*4, height = 480*4, res = 200)
iplot(list(mod_twfe, mod_sa), sep = 0.5, ref.line = -1,
      xlab = 'Time to treatment',
      main = 'Event study: Floods on Transactions Received',
      ci_level = 0.90, xlim = c(-6,6), ylim = c(-1,2))
legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
       legend = c("TWFE", "Sun & Abraham (2020)"), cex = 0.8)
dev.off()

################################################################################
# trans_send
################################################################################
dat_flood$Y <- dat_flood$trans_sent

mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time × treatment status
                   id + time_id,            ## FEs
                 cluster = ~muni_cd,                          ## Clustered SEs
                 data = dat_flood)
# See `?sunab`.
mod_sa = feols(Y ~ sunab(time_id_treated, time_id) | ## The only thing that's changed
                 id + time_id,            ## FEs
               cluster = ~muni_cd,
               data = dat_flood)

png(file.path(output_path,"/pix_flood_trans_sent.png"), width = 640*4, height = 480*4, res = 200)
iplot(list(mod_twfe, mod_sa), sep = 0.5, ref.line = -1,
      xlab = 'Time to treatment',
      main = 'Event study: Floods on Transactions Sent',
      ci_level = 0.90, xlim = c(-6,6), ylim = c(-1,2))
legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
       legend = c("TWFE", "Sun & Abraham (2020)"), cex = 0.8)
dev.off()

################################################################################
# value_rec
################################################################################
dat_flood$Y <- dat_flood$value_rec

mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time × treatment status
                   id + time_id,            ## FEs
                 cluster = ~muni_cd,                          ## Clustered SEs
                 data = dat_flood)
# See `?sunab`.
mod_sa = feols(Y ~ sunab(time_id_treated, time_id) | ## The only thing that's changed
                 id + time_id,            ## FEs
               cluster = ~muni_cd,
               data = dat_flood)

png(file.path(output_path,"/pix_flood_value_rec.png"), width = 640*4, height = 480*4, res = 200)
iplot(list(mod_twfe, mod_sa), sep = 0.5, ref.line = -1,
      xlab = 'Time to treatment',
      main = 'Event study: Floods on Value Received',
      ci_level = 0.90, xlim = c(-6,6), ylim = c(-500,1000))
legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
       legend = c("TWFE", "Sun & Abraham (2020)"), cex = 0.8)
dev.off()

################################################################################
# value_sent
################################################################################
dat_flood$Y <- dat_flood$value_sent

mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time × treatment status
                   id + time_id,            ## FEs
                 cluster = ~muni_cd,                          ## Clustered SEs
                 data = dat_flood)
# See `?sunab`.
mod_sa = feols(Y ~ sunab(time_id_treated, time_id) | ## The only thing that's changed
                 id + time_id,            ## FEs
               cluster = ~muni_cd,
               data = dat_flood)

png(file.path(output_path,"/pix_flood_value_sent.png"), width = 640*4, height = 480*4, res = 200)
iplot(list(mod_twfe, mod_sa), sep = 0.5, ref.line = -1,
      xlab = 'Time to treatment',
      main = 'Event study: Floods on Value Sent',
      ci_level = 0.90, xlim = c(-6,6), ylim = c(-500,1000))
legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
       legend = c("TWFE", "Sun & Abraham (2020)"), cex = 0.8)
dev.off()

sink()