# Create types of transactions by municipality

# Library
options(download.file.method = "wininet")
rm(list = ls()) ## Clear workspace

library(data.table)  
library(haven)       
library(dplyr)
library(readr)
library(stringr)
library(tidyr)

setwd("//sbcdf176/Pix_Matheus$")
path_main <- "//sbcdf176/PIX_Matheus$/"
dta_path <- paste0(path_main, "Stata/dta/")

################################################################################
#-------------------------------------------------------------------------------
# Functions
#-------------------------------------------------------------------------------
source(paste0(R_path,"/functions/time_id_to_month.R"))
source(paste0(R_path,"/functions/time_id_to_year.R"))
source(paste0(R_path,"/functions/week_to_month.R"))
source(paste0(R_path,"/functions/week_to_year.R"))


# Load the data
#Pix_Muni_flow_aggreg.dta
# Variables: week, muni_cd, sender_type, receiver_type, 
#             senders_rec, receivers_rec, valor_rec, trans_rec, valor_w_rec, 
#             senders_sent, receivers_sent, valor_sent, trans_sent, valor_w_sent
# Plus l variations.

# Pix_Muni_flow.dta
# Variables: flow_code, week, muni_cd, sender_type, receiver_type, senders, receivers, valor, trans, valor_w,
#             lsenders, lreceivers, lvalor, ltrans, lvalor_w


file <- "Pix_Muni_flow.dta"
dat <- read_dta(file.path(dta_path,file))
setDT(dat)
dat$time <- dat$week
dat$month <- week_to_month(dat$week)
dat$year <- week_to_year(dat$week)


# Group by flow_code, year, muni_cd, sender_type, receiver_type. Get the sum by  valor, trans, valor_w
dat <- dat %>%
  group_by(flow_code, year, muni_cd, sender_type, receiver_type) %>%
  summarise(
    valor = sum(valor, na.rm = TRUE),
    trans = sum(trans, na.rm = TRUE),
    valor_w = sum(valor_w, na.rm = TRUE)
  ) %>%
  ungroup()

#Add the name of the city

# Load the municipality names
mun_convert <- read_dta(paste0(path_dta, "municipios.dta"))
mun_convert <- mun_convert %>%
  select(MUN_CD, MUN_CD_CADMU, MUN_CD_IBGE, MUN_NM, MUN_NM_NAO_FORMATADO) %>%
  rename(muni_cd = MUN_CD_CADMU,
         id_municipio = MUN_CD_IBGE,
         id_municipio_receita = MUN_CD,
         muni_nm = MUN_NM,
         muni_nm_nao_formatado = MUN_NM_NAO_FORMATADO)
mun_convert <- data.table(mun_convert)
mun_convert <- mun_convert %>%
  mutate(id_municipio = as.integer(id_municipio),
         muni_cd = as.integer(muni_cd),
         id_municipio_receita = as.integer(id_municipio_receita))
any(duplicated(mun_convert$id_municipio) | duplicated(mun_convert$muni_cd) | duplicated(mun_convert$id_municipio_receita))
setDT(mun_convert)

# Merge the municipality names with the data
dat <- merge(dat, mun_convert, by = "muni_cd", all.x = TRUE)

# Save
write_dta(dat, paste0(dta_path, "Pix_Muni_flow_yearly.dta"))
