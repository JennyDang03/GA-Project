#https://lost-stats.github.io/Model_Estimation/Research_Design/event_study.html

options(download.file.method = "wininet")

#install.packages("data.table")
#install.packages("fixest")
#install.packages("haven")

library(data.table) ## For some minor data wrangling
library(fixest)     ## NB: Requires version >=0.9.0
library(haven)

# Clear workspace
rm(list = ls())

setwd("//sbcdf176/Pix_Matheus$/R")
# Set file paths
log_path <- "//sbcdf176/PIX_Matheus$/Stata/log"
dta_path <- "//sbcdf176/PIX_Matheus$/Stata/dta"
output_path <- "//sbcdf176/PIX_Matheus$/Output"
origdata_path <- "//sbcdf176/PIX_Matheus$/DadosOriginais"
#dta_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta"
#output_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output/tables"
#log_path <- "C:/Users/mathe/Dropbox/RESEARCH/qualpix/pix-event-study/Stata/log"

# Load already prepared data!
dat <- read_dta(file.path(dta_path,"/Pix_PF_adoption_account.dta"))
setDT(dat)
# Let's create a more user-friendly indicator of which states received treatment
dat[, treat := ifelse(is.na(`date_first_pix_sent`), 0, 1)]

# Create a "time_to_treatment" variable for each state, so that treatment is
# relative for all treated units. For the never-treated (i.e. control) units,
# we'll arbitrarily set the "time_to_treatment" value at 0. This value 
# doesn't really matter, since it will be canceled by the treat==0 interaction
# anyway. But we do want to make sure they aren't NA, otherwise feols would drop 
# these never-treated observations at estimation time and our results will be 
# off.
dat[, time_to_treat := ifelse(treat==1, time_id - `date_first_pix_sent`, 0)]

mod_twfe = feols(n_account_stock ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time × treatment status
                   id + time_id + muni_cd:time_id,            ## FEs
                 cluster = ~id,                          ## Clustered SEs
                 data = dat)

iplot(mod_twfe, 
      xlab = 'Time to treatment',
      main = 'Event study: Staggered treatment (TWFE)')

# Following Sun and Abraham, we give our never-treated units a fake "treatment"
# date far outside the relevant study period.
dat[, time_id_treated := ifelse(treat==0, 1000000, `date_first_pix_sent`)]

# Now we re-run our model from earlier, but this time swapping out 
# `i(time_to_treat, treat, ref = -1)` for `sunab(year_treated, year)`.
# See `?sunab`.
mod_sa = feols(n_account_stock ~ sunab(time_id_treated, time_id) | ## The only thing that's changed
                 id + time_id + muni_cd:time_id,            ## FEs
               cluster = ~id,
               data = dat)

png(file.path(output_path,"/bank_accounts_pix.png"), width = 640*4, height = 480*4, res = 200)
iplot(list(mod_twfe, mod_sa), sep = 0.5, ref.line = -1,
        xlab = 'Time to treatment',
        main = 'Event study: Pix Adoption on Bank Accounts',
        ci_level = 0.90)
  legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
         legend = c("TWFE", "Sun & Abraham (2020)"))
dev.off()



################################################################################

# Eliminate anything before November 2020




