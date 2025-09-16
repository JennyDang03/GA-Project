# https://lost-stats.github.io/Model_Estimation/Research_Design/instrumental_variables.html

options(download.file.method = "wininet")

#install.packages(c('AER','lfe',"data.table", "fixest", "haven", "ggplot2"))

library(lfe)
library(AER)
library(haven)

#library(data.table) ## For some minor data wrangling
#library(fixest)     ## NB: Requires version >=0.9.0
#library(ggplot2)

# Clear workspace
rm(list = ls())

setwd("//sbcdf176/Pix_Matheus$")
# Set file paths
log_path <- "//sbcdf176/PIX_Matheus$/R/log"
dta_path <- "//sbcdf176/PIX_Matheus$/Stata/dta"
output_path <- "//sbcdf176/PIX_Matheus$/Output"
origdata_path <- "//sbcdf176/PIX_Matheus$/DadosOriginais"
#dta_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta"
#output_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output/tables"
#log_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/R/log"

log_file <- file.path(log_path, "iv_v1.log")
sink(log_file) # redirect R output to log file

# Load already prepared data!
dat_iv <- read_dta(file.path(dta_path,"/Pix_bank_account_bf.dta"))
#setDT(dat)
dat_iv$id <- factor(dat_iv$id)
dat_iv$time_id <- factor(dat_iv$time_id)
dat_iv$muni_cd <- factor(dat_iv$muni_cd)

## Limit to BF only
dat_iv <- dat_iv[dat_iv$bf==1,]
dat_iv_limit <- dat_iv[dat_iv$accounts_oct_2020>0,]
##


##############################
# General IV - no cluster SE - exactDOF = TRUE:
cat("\n General IV - cluster id - exactDOF = TRUE: \n")
ivmodel2 <- felm(n_account_stock ~ 0 | id + time_id + muni_cd:time_id | 
                   (after_first_pix_sent ~ road_dist_caixa + road_expected_distance), data = dat_iv, exactDOF = TRUE)
summary(ivmodel2)

# General IV - cluster id - exactDOF = TRUE:
cat("\n General IV - cluster id - exactDOF = TRUE: \n")
ivmodel2 <- felm(n_account_stock ~ 0 | id + time_id + muni_cd:time_id | 
                   (after_first_pix_sent ~ road_dist_caixa + road_expected_distance) | 
                   id, data = dat_iv, exactDOF = TRUE)
summary(ivmodel2)

### -> Now controlling for road_expected_distance in the second stage

# General IV - no cluster SE - exactDOF = TRUE:
cat("\n General IV - cluster id - exactDOF = TRUE: \n")
ivmodel2 <- felm(n_account_stock ~ road_expected_distance | id + time_id + muni_cd:time_id | 
                   (after_first_pix_sent ~ road_dist_caixa + road_expected_distance), data = dat_iv, exactDOF = TRUE)
summary(ivmodel2)

# General IV - cluster id - exactDOF = TRUE:
cat("\n General IV - cluster id - exactDOF = TRUE: \n")
ivmodel2 <- felm(n_account_stock ~ road_expected_distance | id + time_id + muni_cd:time_id | 
                   (after_first_pix_sent ~ road_dist_caixa + road_expected_distance) | 
                   id, data = dat_iv, exactDOF = TRUE)
summary(ivmodel2)








##############################
# SENDER
# General IV - no cluster SE - exactDOF = TRUE:
cat("\n General IV - cluster id - exactDOF = TRUE: \n")
ivmodel2 <- felm(n_account_stock ~ 0 | id + time_id + muni_cd:time_id | 
                   (sender ~ road_dist_caixa + road_expected_distance), data = dat_iv, exactDOF = TRUE)
summary(ivmodel2)

# General IV - cluster id - exactDOF = TRUE:
cat("\n General IV - cluster id - exactDOF = TRUE: \n")
ivmodel2 <- felm(n_account_stock ~ 0 | id + time_id + muni_cd:time_id | 
                   (sender ~ road_dist_caixa + road_expected_distance) | 
                   id, data = dat_iv, exactDOF = TRUE)
summary(ivmodel2)


##############################
# trans_sent
# General IV - no cluster SE - exactDOF = TRUE:
cat("\n General IV - cluster id - exactDOF = TRUE: \n")
ivmodel2 <- felm(n_account_stock ~ 0 | id + time_id + muni_cd:time_id | 
                   (trans_sent ~ road_dist_caixa + road_expected_distance), data = dat_iv, exactDOF = TRUE)
summary(ivmodel2)

# General IV - cluster id - exactDOF = TRUE:
cat("\n General IV - cluster id - exactDOF = TRUE: \n")
ivmodel2 <- felm(n_account_stock ~ 0 | id + time_id + muni_cd:time_id | 
                   (trans_sent ~ road_dist_caixa + road_expected_distance) | 
                   id, data = dat_iv, exactDOF = TRUE)
summary(ivmodel2)
##############################









# Limit to those with accounts in October 2020.
cat("\n Limit to those with accounts in October 2020. - cluster id - exactDOF = TRUE: \n")
ivmodel2 <- felm(n_account_stock ~ 0 | id + time_id + muni_cd:time_id | 
                   (after_first_pix_sent ~ road_dist_caixa + road_expected_distance) | 
                   id, data = dat_iv_limit, exactDOF = TRUE)
summary(ivmodel2)

##############################
# General IV - no cluster SE - exactDOF = TRUE:
cat("\n General IV - cluster id - exactDOF = TRUE: \n")
ivmodel2 <- felm(n_account_stock ~ 0 | id + time_id + muni_cd:time_id | 
                   (after_first_pix_sent ~ road_dist_expec), data = dat_iv, exactDOF = TRUE)
summary(ivmodel2)

# General IV - cluster id - exactDOF = TRUE:
cat("\n General IV - cluster id - exactDOF = TRUE: \n")
ivmodel2 <- felm(n_account_stock ~ 0 | id + time_id + muni_cd:time_id | 
                   (after_first_pix_sent ~ road_dist_expec) | 
                   id, data = dat_iv, exactDOF = TRUE)
summary(ivmodel2)

# Limit to those with accounts in October 2020.
cat("\n Limit to those with accounts in October 2020. - cluster id - exactDOF = TRUE: \n")
ivmodel2 <- felm(n_account_stock ~ 0 | id + time_id + muni_cd:time_id | 
                   (after_first_pix_sent ~ road_dist_expec) | 
                   id, data = dat_iv_limit, exactDOF = TRUE)
summary(ivmodel2)



# Now we will run the same model with lfe::felm

# The regression formula takes the format
# dependent vairable ~ 
#    controls |
#    fixed.effects | 
#    (endogenous.variables ~ instruments) |
#    clusters.for.standard.errors
# So if need be it is straightforward to adjust this example to account for
# fixed effects and clustering.
# Note the 0 indicating no fixed effects

# ivregress 2sls n_account_stock (after_first_pix_sent = road_dist_caixa road_expected_distance i.time_id##i.muni_cd) if bf == 1, vce(cluster id)
#ivmodel2 <- felm(n_account_stock ~ 0 | id + time_id + muni_cd:time_id | 
#                 (after_first_pix_sent ~ road_dist_caixa + road_expected_distance) | 
#                 id, data = dat_iv)
#summary(ivmodel2)

# felm can also use several k-class estimation methods; see help(felm) for the full list.
# Let's run it with a limited-information maximum likelihood estimator with 
# the fuller adjustment set to minimize squared error (4).
#ivmodel3 <- felm(n_account_stock ~ 0 | id + time_id + muni_cd:time_id | (after_first_pix_sent ~ road_dist_caixa + road_expected_distance) | id, data = dat, kclass = 'liml', fuller = 4)
#summary(ivmodel3)

# The regression formula takes the format
# dependent.variable ~ endogenous.variables + controls | instrumental.variables + controls
#ivmodel <- ivreg(n_account_stock ~ after_first_pix_sent | road_dist_caixa + road_expected_distance 
#                 + id + time_id + muni_cd:time_id,
#                 data = dat)
#summary(ivmodel)

sink()