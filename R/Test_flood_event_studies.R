rm(list = ls()) ## Clear workspace

library(readr)
library(stringr)
library(odbc)
library(dplyr)
library(tidyr)
library(RODBC)
library(futile.logger)
library(data.table)
library(bit64)
library(haven)
library(gdata)
library("arrow")
library(stargazer)
library(lubridate)
library(fixest)     
library(ggplot2)


setwd("//sbcdf176/Pix_Matheus$")
path_main <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/"
path_main <- "//sbcdf176/PIX_Matheus$/"

path_query <- paste0(path_main, "R/DataExtraction/")
path_data <- paste0(path_main, "DadosOriginais/")
path_dta <- paste0(path_main, "Stata/dta/")
path_output <- paste0(path_main, "Output/")
log_path <- paste0(path_main, "Stata/log/")
dta_path <- paste0(path_main, "Stata/dta/")
output_path <- paste0(path_main, "Output/")
origdata_path <- paste0(path_main, "DadosOriginais/")
R_path <- paste0(path_main, "R/")

# Constants
xl <- -26
xu <- 52
xl_balanced <- -26
xu_balanced <- 52
xl_balanced_covid <- -13
xu_balanced_covid <- 13


#-------------------------------------------------------------------------------
# Load auxiliary data
#-------------------------------------------------------------------------------
source(paste0(R_path, "/auxiliary_data.r"))

#-------------------------------------------------------------------------------
# Functions
#-------------------------------------------------------------------------------
source(paste0(R_path,"/functions/stata_week_number.R"))
source(paste0(R_path,"/functions/gen_week_list.R"))
source(paste0(R_path,"/functions/time_id_to_month.R"))
source(paste0(R_path,"/functions/time_id_to_year.R"))
source(paste0(R_path,"/functions/week_to_month.R"))
source(paste0(R_path,"/functions/week_to_year.R"))






prepare_data <- function(file,flood_data,fe,controls,xlimit_low, xlimit_up){
  dat <- read_dta(file.path(dta_path,file))
  setDT(dat)
  if ("time_id" %in% names(dat)) {
    dat$time <- dat$time_id
    dat$month <- time_id_to_month(dat$time_id)
    dat$year <- time_id_to_year(dat$time_id)
  } else if ("week" %in% names(dat)) {
    dat$time <- dat$week
    dat$month <- week_to_month(dat$week)
    dat$year <- week_to_year(dat$week)
  }
  
  # Flood, FE, and Control Variables
  dat <- merge(dat, flood_data, by=c("muni_cd","time"), all=FALSE) # it deletes if no match.
  dat <- merge(dat, fe, by="muni_cd", all.x = TRUE) 
  dat <- merge(dat, controls, by=c("muni_cd","month","year"), all.x = TRUE) 
  
  # Event Study Variables
  dat[, treat := ifelse(is.na(date_flood), 0, 1)]
  dat[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
  dat[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
  dat[, after_flood := ifelse(is.na(date_flood), 0, ifelse(time >= date_flood, 1, 0))]
  
  # Set Limits <- Temporary measure - The right way is to run for everything and choose the limits by hand (which takes time and effort)
  xlimits <- seq(ceiling(xlimit_low*1.333),ceiling(xlimit_up*1.333),by=1)
  dat <- subset(dat,time_to_treat %in% xlimits)
  #
  return(dat)
}

twfe <- function(y,control1,control2,fe,dat_list){
  mod_list <- list()
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
                      data = dat)
    
    mod_list[[i]] <- mod_twfe
  }
  return(mod_list)
}
print_twfe <- function(graphname,y,control1,control2,fe,main_title,dat_list,legend_list,xlimit_l,xlimit_u){
  pch_list = c(16,17,15,1,2,0,5,3) # 18 didnt work
  col_list = c("black", "red", "green", "blue", "purple", "orange", "cyan", "magenta", "yellow")
  mod_list <- twfe(y,control1,control2,fe,dat_list)
  if(length(dat_list) == 1){mod_list <- mod_list[[1]]}
  png(file.path(output_path,paste0(graphname,y,".png")), width = 640*4, height = 480*4, res = 200)
  par(cex.main = 1.75, cex.lab = 1.5, cex.axis = 1.75)
  iplot(mod_list, sep = 0.5, ref.line = -1,
        xlab = '',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.1,xlimit_u+0.1),
        col = col_list, pch = pch_list, cex=0.7) 
  legend("bottomleft", col = col_list, pch = pch_list, 
         legend = legend_list, cex = 1)
  dev.off()
}




# Test_flood_event_studies.R
# Test
# dat_flood <- prepare_data("flood_pix_weekly_fake.dta",flood_week_after,mun_fe,mun_control,xl, xu)
# print_twfe("Pix_fake_pre+int_test1","log_valor_PIX_inflow","pre","internet_access","constant","Log Value Pix Inflow",list(dat_flood),c("TWFE"), xl, xu)
# dat2 <- dat_flood %>% filter(muni_cd < 3000)
# print_twfe("Pix_fake_pre+int_test2","log_valor_PIX_inflow","pre","internet_access","constant","Log Value Pix Inflow",list(dat_flood,dat2),c("test1","test2"), xl, xu)
# dat3 <- dat_flood %>% filter(muni_cd < 4000)
# print_twfe("Pix_fake_pre+int_test3","log_valor_PIX_inflow","pre","internet_access","constant","Log Value Pix Inflow",list(dat_flood,dat2,dat3),c("test1","test2","test3"), xl, xu)
# print_twfe("Pix_fake_pre+int_test4","log_valor_PIX_inflow","pre","internet_access","constant","Log Value Pix Inflow",list(dat_flood,dat2,dat3,dat_flood),c("test1","test2","test3","test4"), xl, xu)

dat_function <- function(data,week,fe,control,xlow,xup){
  dat <- prepare_data(data,week,fe,control,xlow, xup)
  dat <- dat %>%
    mutate(log_valor_TED_intra = log1p(valor_TED_intra),
           log_qtd_TED_intra = log1p(qtd_TED_intra),
           log_qtd_cli_TED_rec_PJ = log1p(qtd_cli_TED_rec_PJ),
           log_qtd_cli_TED_pag_PJ = log1p(qtd_cli_TED_pag_PJ),
           log_valor_boleto = log1p(valor_boleto),
           log_qtd_boleto = log1p(qtd_boleto),
           log_qtd_cli_pag_pf_boleto = log1p(qtd_cli_pag_pf_boleto),
           log_qtd_cli_pag_pj_boleto = log1p(qtd_cli_pag_pj_boleto),
           log_qtd_cli_rec_pj_boleto = log1p(qtd_cli_rec_pj_boleto),
           log_valor_cartao_credito = log1p(valor_cartao_credito),
           log_valor_cartao_debito = log1p(valor_cartao_debito),
           log_qtd_cli_cartao_debito = log1p(qtd_cli_cartao_debito),
           log_qtd_cli_cartao_credito = log1p(qtd_cli_cartao_credito)
           )
  #,log_valor_PIX_intra = log1p(valor_PIX_intra),log_qtd_PIX_intra = log1p(qtd_PIX_intra)
  #log_valor_PIX_intra = log1p(valor_PIX_intra),
  #log_qtd_PIX_intra = log1p(qtd_PIX_intra),
  #log_qtd_cli_PIX_intra = log1p(qtd_cli_PIX_intra)
  return(dat)
}
run_boleto_cartao <- function(prename,dat_b,dat_a,fe,xl, xu){
  #Boleto
  print_twfe(paste0(prename, "Boleto_"),"log_valor_boleto","constant","constant",fe,"Log Value Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
  print_twfe(paste0(prename, "Boleto_"),"log_qtd_boleto","constant","constant",fe,"Log Transactions Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
  print_twfe(paste0(prename, "Boleto_"),"log_qtd_cli_pag_pf_boleto","constant","constant",fe,"Log Quantity of People Sending Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
  #print_twfe(paste0(prename, "Boleto_"),"log_qtd_cli_pag_pj_boleto","constant","constant",fe,"Log Quantity of Firms Sending Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
  print_twfe(paste0(prename, "Boleto_"),"log_qtd_cli_rec_pj_boleto","constant","constant",fe,"Log Quantity of Firms Receiving Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
  
  #Cartao 
  # * valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito 
  print_twfe(paste0(prename, "Cartao_"),"log_valor_cartao_debito","constant","constant",fe,"Log Value Debit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
  print_twfe(paste0(prename, "Cartao_"),"log_valor_cartao_credito","constant","constant",fe,"Log Value Credit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
  print_twfe(paste0(prename, "Cartao_"),"log_qtd_cli_cartao_debito","constant","constant",fe,"Log Quantity of Firms accepting Debit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
  print_twfe(paste0(prename, "Cartao_"),"log_qtd_cli_cartao_credito","constant","constant",fe,"Log Quantity of Firms accepting Credit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
  # + Adocao 
  
  #Pix
  # * valor_PIX_intra qtd_PIX_intra qtd_cli_PIX_intra 
  #print_twfe(paste0(prename, "Pix_"),"log_valor_PIX_intra","constant","constant",fe,"Log Value Pix Inflow",list(dat_a), c("After Pix"), xl, xu)
  #print_twfe(paste0(prename, "Pix_"),"log_qtd_PIX_intra","constant","constant",fe,"Log Transactions Pix Inflow",list(dat_a), c("After Pix"), xl, xu)
  #print_twfe(paste0(prename, "Pix_"),"","constant","constant",fe,"Log Quantity of Firms Receiving Pix",list(dat_a), c("After Pix"), xl, xu)
  return()
}

dat_a <- dat_function("Base_week_muni_fake.dta",flood_week_after,mun_fe,mun_control,xl, xu)
dat_b <- dat_function("Base_week_muni_fake.dta",flood_week_before,mun_fe,mun_control,xl, xu)



twfe <- function(y,control1,control2,fe,dat_list){
  mod_list <- list()
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
    
    mod_list[[i]] <- mod_twfe
  }
  return(mod_list)
}
print_twfe <- function(graphname,y,control1,control2,fe,main_title,dat_list,legend_list,xlimit_l,xlimit_u){
  pch_list = c(16,17,15,1,2,0,5,3) # 18 didnt work
  col_list = c("black", "red", "green", "blue", "purple", "orange", "cyan", "magenta", "yellow")
  mod_list <- twfe(y,control1,control2,fe,dat_list)
  if(length(dat_list) == 1){mod_list <- mod_list[[1]]}
  png(file.path(output_path,paste0(graphname,y,".png")), width = 640*4, height = 480*4, res = 200)
  par(cex.main = 1.75, cex.lab = 1.5, cex.axis = 1.75)
  iplot(mod_list, sep = 0.5, ref.line = -1,
        xlab = '',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.1,xlimit_u+0.1),
        col = col_list, pch = pch_list, cex=0.7) 
  legend("bottomleft", col = col_list, pch = pch_list, 
         legend = legend_list, cex = 1)
  dev.off()
}

tw <- twfe("log_valor_TED_intra","constant","constant","constant",list(dat_b,dat_a))
# save tw to file
saveRDS(tw, file = file.path(output_path,paste0("/coeficients/","twfe_test",".rds")))
# load tw from file
tw2 <- readRDS(file = file.path(output_path,paste0("/coeficients/","twfe_test",".rds")))

#clean tw
# Loop
if(length(dat_list) == 1){mod_list <- mod_list[[1]]}
for (i in 1:2) {
  tw1 <- tw[[i]]
  
  #$cov.scaled
  #[1] 107768 bytes
  #$cov.unscaled
  #[1] 105320 bytes
  #$cov.iid
  #[1] 105320 bytes
  #$hessian
  #[1] 86744 bytes
  
  tw1$linear.predictors <- NULL
  tw1$working_residuals <- NULL
  tw1$family <- NULL
  tw1$fml <- NULL
  tw1$fml_all <- NULL
  
  tw1$cov.scaled <- NULL
  tw1$cov.unscaled <- NULL
  tw1$cov.iid <- NULL
  tw1$hessian <- NULL

  tw[[i]] <- tw1
}
saveRDS(tw, file = file.path(output_path,paste0("/coeficients/","twfe_test",".rds")))
#f1.1$family$family = "binomial"
#f1.1$family$family = "logit"
#f1.1[["fml"]][[2]] = "emigration"


coeftable_tw1 <- summary(tw1)$coeftable
variables <- rownames(coeftable_tw1)
variables <- as.numeric(gsub("time_to_treat::(.+):treat", "\\1", variables))
coefficients <- coeftable_tw1[, "Estimate"]
standard_errors <- coeftable_tw1[, "Std. Error"]

filtered_coefficients <- coefficients[variables >= xl & variables <= xu]
filtered_standard_errors <- standard_errors[variables >= xl & variables <= xu]

ylim = range(filtered_coefficients - 2 * filtered_standard_errors, filtered_coefficients + 2 * filtered_standard_errors)


library(coefplot)

coef_data <- data.frame(
  Coef = coefficients,
  SE = standard_errors
)

# Plot coefficients with standard errors
coefplot(
  coef_data,
  yintercept = 0,  # Show a reference line at y = 0
  main = "Estimates with Standard Errors",
  ylab = "Estimate",
  xlab = "Variables",
  col.lines = col_list[1],  # Color of lines
  col.intervals = col_list[2],  # Color of confidence intervals
  pch = pch_list[1],  # Point character for coefficients
  ci = TRUE  # Show confidence intervals
)
png(file.path(output_path,paste0("test",".png")), width = 640*4, height = 480*4, res = 200)
dev.off()  # Close the PNG device





# Extract variable names
variables <- rownames(coeftable_tw1)

png(file.path(output_path,paste0("test",".png")), width = 640*4, height = 480*4, res = 200)
plot(coefficients, 
     pch = pch_list[1], 
     col = col_list[1], 
     xlab = 'Variables',
     ylab = 'Estimate',
     ylim = range(coefficients - 2 * standard_errors, coefficients + 2 * standard_errors),
     main = 'Estimates with Standard Errors',
     type = "b")

# Add error bars
arrows(x0 = 1:length(coefficients), y0 = coefficients - standard_errors, 
       x1 = 1:length(coefficients), y1 = coefficients + standard_errors, 
       angle = 90, code = 3, length = 0.1)

# Plot standard errors as points with NA values to avoid showing them in the legend
points(rep(NA, length(coefficients)), 
       pch = pch_list[2], 
       col = col_list[2])

# Add legend
legend("bottomleft", 
       legend = c("Estimate", "Standard Error"), 
       col = c(col_list[1], col_list[2]), 
       pch = c(pch_list[1], pch_list[2]), 
       cex = 1)

dev.off()  # Close the PNG device


# Extract coefficient table and standard errors
coeftable_tw1 <- summary(tw1)$coeftable
se_tw1 <- summary(tw1)$se

# Create a new fixest object with only the coefficient table and standard errors
tw1_small <- fixest::feols(y ~ x1 + x2, data = your_data_frame)

# Replace the coefficient table and standard errors in the new fixest object
summary(tw1_small)$coeftable <- coeftable_tw1
summary(tw1_small)$se <- se_tw1

# Save the smaller fixest object
saveRDS(tw1_small, "tw1_small.rds")















# Save coeftable_tw1
saveRDS(coeftable_tw1, "coeftable_tw1.rds")
# Load coeftable_tw1
coeftable_tw1 <- readRDS("coeftable_tw1.rds")

str(coeftable_tw1)

# If 'coeftable_tw1' is a data frame, convert it to a fixest object
tw1_small <- as_fe(coeftable_tw1)


pch_list = c(16,17,15,1,2,0,5,3) # 18 didnt work
col_list = c("black", "red", "green", "blue", "purple", "orange", "cyan", "magenta", "yellow")
png(file.path(output_path,paste0("test",".png")), width = 640*4, height = 480*4, res = 200)
par(cex.main = 1.75, cex.lab = 1.5, cex.axis = 1.75)
iplot(coeftable_tw1, sep = 0.5, ref.line = -1,
      xlab = '',
      main = "main_title",
      ci_level = 0.95, xlim = c(xl-0.1,xu+0.1),
      col = col_list, pch = pch_list, cex=0.7) 
legend("bottomleft", col = col_list, pch = pch_list, 
       legend = c("Before Pix","After Pix"), cex = 1)
dev.off()





# Create a new fixest object with only the coefficient table and standard errors
tw1_small <- fixest::feols(y ~ x1 + x2, data = your_data_frame)

# Replace the coefficient table and standard errors in the new fixest object
summary(tw1_small)$coeftable <- coeftable_tw1
summary(tw1_small)$se <- se_tw1

# Save the smaller fixest object
saveRDS(tw1_small, "tw1_small.rds")




#save tw to file
saveRDS(tw, file = file.path(output_path,paste0("/coeficients/","twfe_test",".rds")))
#load tw from
tw2 <- readRDS(file = file.path(output_path,paste0("/coeficients/","twfe_test",".rds")))
# print 
pch_list = c(16,17,15,1,2,0,5,3) # 18 didnt work
col_list = c("black", "red", "green", "blue", "purple", "orange", "cyan", "magenta", "yellow")
mod_list <- tw2
if(length(list(dat_b,dat_a)) == 1){mod_list <- mod_list[[1]]}
png(file.path(output_path,paste0("test",".png")), width = 640*4, height = 480*4, res = 200)
par(cex.main = 1.75, cex.lab = 1.5, cex.axis = 1.75)
iplot(mod_list, sep = 0.5, ref.line = -1,
      xlab = '',
      main = "main_title",
      ci_level = 0.95, xlim = c(xl-0.1,xu+0.1),
      col = col_list, pch = pch_list, cex=0.7) 
legend("bottomleft", col = col_list, pch = pch_list, 
       legend = c("Before Pix","After Pix"), cex = 1)
dev.off()



#TED
print_twfe("fake_TED_","log_valor_TED_intra","constant","constant","constant","Log Value TED",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake_TED_","log_qtd_TED_intra","constant","constant","constant","Log Transactions TED",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake_TED_","log_qtd_cli_TED_rec_PJ","constant","constant","constant","Log Quantity of Firms Receiving TED",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake_TED_","log_qtd_cli_TED_pag_PJ","constant","constant","constant","Log Quantity of Firms Sending TED",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
# + Uso da conta + "adocao"

#Boleto
print_twfe("fake_Boleto_","log_valor_boleto","constant","constant","constant","Log Value Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake_Boleto_","log_qtd_boleto","constant","constant","constant","Log Transactions Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake_Boleto_","log_qtd_cli_pag_pf_boleto","constant","constant","constant","Log Quantity of People Sending Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
#print_twfe("fake_Boleto_","log_qtd_cli_pag_pj_boleto","constant","constant","constant","Log Quantity of Firms Sending Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake_Boleto_","log_qtd_cli_rec_pj_boleto","constant","constant","constant","Log Quantity of Firms Receiving Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)

#Cartao 
# * valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito 
print_twfe("fake_Cartao_","log_valor_cartao_debito","constant","constant","constant","Log Value Debit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake_Cartao_","log_valor_cartao_credito","constant","constant","constant","Log Value Credit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake_Cartao_","log_qtd_cli_cartao_debito","constant","constant","constant","Log Quantity of Firms accepting Debit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake_Cartao_","log_qtd_cli_cartao_credito","constant","constant","constant","Log Quantity of Firms accepting Credit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
# + Adocao 

# Controls

#Boleto
print_twfe("fake2_Boleto_","log_valor_boleto","constant","constant","flood_risk5","Log Value Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake2_Boleto_","log_qtd_boleto","constant","constant","flood_risk5","Log Transactions Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake2_Boleto_","log_qtd_cli_pag_pf_boleto","constant","constant","flood_risk5","Log Quantity of People Sending Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
#print_twfe("fake2_Boleto_","log_qtd_cli_pag_pj_boleto","constant","constant","flood_risk5","Log Quantity of Firms Sending Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake2_Boleto_","log_qtd_cli_rec_pj_boleto","constant","constant","flood_risk5","Log Quantity of Firms Receiving Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)

#Cartao 
# * valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito 
print_twfe("fake2_Cartao_","log_valor_cartao_debito","constant","constant","flood_risk5","Log Value Debit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake2_Cartao_","log_valor_cartao_credito","constant","constant","flood_risk5","Log Value Credit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake2_Cartao_","log_qtd_cli_cartao_debito","constant","constant","flood_risk5","Log Quantity of Firms accepting Debit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake2_Cartao_","log_qtd_cli_cartao_credito","constant","constant","flood_risk5","Log Quantity of Firms accepting Credit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
# + Adocao 

# Control 2
#Boleto
print_twfe("fake3_Boleto_","log_valor_boleto","constant","constant","flood_risk4","Log Value Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake3_Boleto_","log_qtd_boleto","constant","constant","flood_risk4","Log Transactions Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake3_Boleto_","log_qtd_cli_pag_pf_boleto","constant","constant","flood_risk4","Log Quantity of People Sending Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
#print_twfe("fake3_Boleto_","log_qtd_cli_pag_pj_boleto","constant","constant","flood_risk4","Log Quantity of Firms Sending Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake3_Boleto_","log_qtd_cli_rec_pj_boleto","constant","constant","flood_risk4","Log Quantity of Firms Receiving Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)

#Cartao 
# * valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito 
print_twfe("fake3_Cartao_","log_valor_cartao_debito","constant","constant","flood_risk4","Log Value Debit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake3_Cartao_","log_valor_cartao_credito","constant","constant","flood_risk4","Log Value Credit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake3_Cartao_","log_qtd_cli_cartao_debito","constant","constant","flood_risk4","Log Quantity of Firms accepting Debit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake3_Cartao_","log_qtd_cli_cartao_credito","constant","constant","flood_risk4","Log Quantity of Firms accepting Credit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
# + Adocao 




dat_a <- dat_function("Base_week_muni_fake.dta",flood_week_after2023,mun_fe,mun_control,xl, xu)
dat_b <- dat_function("Base_week_muni_fake.dta",flood_week_before2019,mun_fe,mun_control,xl, xu)

# Controls

#Boleto
print_twfe("fake4_Boleto_","log_valor_boleto","constant","constant","flood_risk5","Log Value Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake4_Boleto_","log_qtd_boleto","constant","constant","flood_risk5","Log Transactions Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake4_Boleto_","log_qtd_cli_pag_pf_boleto","constant","constant","flood_risk5","Log Quantity of People Sending Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
#print_twfe("fake4_Boleto_","log_qtd_cli_pag_pj_boleto","constant","constant","flood_risk5","Log Quantity of Firms Sending Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake4_Boleto_","log_qtd_cli_rec_pj_boleto","constant","constant","flood_risk5","Log Quantity of Firms Receiving Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)

#Cartao 
# * valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito 
print_twfe("fake4_Cartao_","log_valor_cartao_debito","constant","constant","flood_risk5","Log Value Debit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake4_Cartao_","log_valor_cartao_credito","constant","constant","flood_risk5","Log Value Credit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake4_Cartao_","log_qtd_cli_cartao_debito","constant","constant","flood_risk5","Log Quantity of Firms accepting Debit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake4_Cartao_","log_qtd_cli_cartao_credito","constant","constant","flood_risk5","Log Quantity of Firms accepting Credit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
# + Adocao 





dat_a <- dat_function("Base_week_muni_fake.dta",flood_week_after_covid,mun_fe,mun_control,xl, xu)
dat_b <- dat_function("Base_week_muni_fake.dta",flood_week_before_covid,mun_fe,mun_control,xl, xu)

# Controls

#Boleto
print_twfe("fake5_Boleto_","log_valor_boleto","constant","constant","flood_risk5","Log Value Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake5_Boleto_","log_qtd_boleto","constant","constant","flood_risk5","Log Transactions Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake5_Boleto_","log_qtd_cli_pag_pf_boleto","constant","constant","flood_risk5","Log Quantity of People Sending Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
#print_twfe("fake5_Boleto_","log_qtd_cli_pag_pj_boleto","constant","constant","flood_risk5","Log Quantity of Firms Sending Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake5_Boleto_","log_qtd_cli_rec_pj_boleto","constant","constant","flood_risk5","Log Quantity of Firms Receiving Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)

#Cartao 
# * valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito 
print_twfe("fake5_Cartao_","log_valor_cartao_debito","constant","constant","flood_risk5","Log Value Debit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake5_Cartao_","log_valor_cartao_credito","constant","constant","flood_risk5","Log Value Credit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake5_Cartao_","log_qtd_cli_cartao_debito","constant","constant","flood_risk5","Log Quantity of Firms accepting Debit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake5_Cartao_","log_qtd_cli_cartao_credito","constant","constant","flood_risk5","Log Quantity of Firms accepting Credit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
# + Adocao 

dat_a <- dat_function("Base_week_muni_fake.dta",flood_week_after_balanced_covid,mun_fe,mun_control,xl, xu)
dat_b <- dat_function("Base_week_muni_fake.dta",flood_week_before_balanced_covid,mun_fe,mun_control,xl, xu)

#Boleto
print_twfe("fake6_Boleto_","log_valor_boleto","constant","constant","flood_risk5","Log Value Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake6_Boleto_","log_qtd_boleto","constant","constant","flood_risk5","Log Transactions Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake6_Boleto_","log_qtd_cli_pag_pf_boleto","constant","constant","flood_risk5","Log Quantity of People Sending Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
#print_twfe("fake6_Boleto_","log_qtd_cli_pag_pj_boleto","constant","constant","flood_risk5","Log Quantity of Firms Sending Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake6_Boleto_","log_qtd_cli_rec_pj_boleto","constant","constant","flood_risk5","Log Quantity of Firms Receiving Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)

#Cartao 
# * valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito 
print_twfe("fake6_Cartao_","log_valor_cartao_debito","constant","constant","flood_risk5","Log Value Debit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake6_Cartao_","log_valor_cartao_credito","constant","constant","flood_risk5","Log Value Credit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake6_Cartao_","log_qtd_cli_cartao_debito","constant","constant","flood_risk5","Log Quantity of Firms accepting Debit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake6_Cartao_","log_qtd_cli_cartao_credito","constant","constant","flood_risk5","Log Quantity of Firms accepting Credit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
# + Adocao 


dat_a <- dat_function("Base_week_muni_fake.dta",flood_week_after_balanced,mun_fe,mun_control,xl, xu)
dat_b <- dat_function("Base_week_muni_fake.dta",flood_week_before_balanced,mun_fe,mun_control,xl, xu)

#Boleto
print_twfe("fake7_Boleto_","log_valor_boleto","constant","constant","flood_risk5","Log Value Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake7_Boleto_","log_qtd_boleto","constant","constant","flood_risk5","Log Transactions Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake7_Boleto_","log_qtd_cli_pag_pf_boleto","constant","constant","flood_risk5","Log Quantity of People Sending Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
#print_twfe("fake7_Boleto_","log_qtd_cli_pag_pj_boleto","constant","constant","flood_risk5","Log Quantity of Firms Sending Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake7_Boleto_","log_qtd_cli_rec_pj_boleto","constant","constant","flood_risk5","Log Quantity of Firms Receiving Boleto",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)

#Cartao 
# * valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito 
print_twfe("fake7_Cartao_","log_valor_cartao_debito","constant","constant","flood_risk5","Log Value Debit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake7_Cartao_","log_valor_cartao_credito","constant","constant","flood_risk5","Log Value Credit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake7_Cartao_","log_qtd_cli_cartao_debito","constant","constant","flood_risk5","Log Quantity of Firms accepting Debit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
print_twfe("fake7_Cartao_","log_qtd_cli_cartao_credito","constant","constant","flood_risk5","Log Quantity of Firms accepting Credit Card",list(dat_b,dat_a), c("Before Pix","After Pix"), xl, xu)
# + Adocao 

dat_a <- dat_function("Base_week_muni_fake.dta",flood_week_after_balanced2023,mun_fe,mun_control,xl, xu)
dat_b <- dat_function("Base_week_muni_fake.dta",flood_week_before_balanced2019,mun_fe,mun_control,xl, xu)
run_boleto_cartao("fake8_",dat_b,dat_a,"flood_risk5",xl, xu)


dat_a <- dat_function("Base_week_muni_fake.dta",flood_week_after_balanced,mun_fe,mun_control,xl, xu)
dat_b <- dat_function("Base_week_muni_fake.dta",flood_week_before_balanced2019,mun_fe,mun_control,xl, xu)
run_boleto_cartao("fake9_",dat_b,dat_a,"flood_risk5",xl, xu)


dat_a <- dat_function("Base_week_muni_fake.dta",flood_week_after_balanced2023,mun_fe,mun_control,xl, xu)
dat_b <- dat_function("Base_week_muni_fake.dta",flood_week_before_balanced2019,mun_fe,mun_control,xl, xu)
run_boleto_cartao("fake8_",dat_b,dat_a,"flood_risk5",xl, xu)



