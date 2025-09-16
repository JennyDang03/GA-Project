# mun_control

library(data.table)
library(haven)
library(data.table)
library(dplyr)
path_dta <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/"
# Create a data table with a sequence of years and months

# Create dates. 
mun_control <- data.table(
  year = rep(2018:2022, each = 12),
  month = rep(1:12, times = 5)
)
#mun_control <- mun_control %>%
#  mutate(time_id = (year-1960)*12 + month - 1)

# Cross Join with muni_cd
mun_convert <- read_dta(paste0(path_dta,"municipios2.dta"))
mun_convert <- mun_convert %>%
  select(id_municipio, id_municipio_bcb) %>%
  rename(muni_cd=id_municipio_bcb) %>%
  mutate(id_municipio = as.integer(id_municipio),
         muni_cd = as.integer(muni_cd))
mun_convert <- data.table(mun_convert)

mun_control <- setkey(mun_control[,c(k=1,.SD)],k)[mun_convert[,c(k=1,.SD)],allow.cartesian=TRUE][,k:=NULL]

# Merge with rain by month
rain <- read_dta(paste0(path_dta, "pre_mun_risk.dta"))
rain <- data.table(rain)
rain <- rain %>%
  mutate(id_municipio = as.integer(id_municipio)) 
mun_control <- merge(mun_control, rain, by=c("id_municipio","month"))

# internet by time.
internet <- read_dta(paste0(path_dta,"mobile+internet_access.dta"))
internet <- data.table(internet)
internet <- internet %>%
  mutate(id_municipio = as.integer(id_municipio)) 
mun_control <- merge(mun_control, internet, by=c("id_municipio","month","year"))

# make variables constant and Na
mun_control <- mun_control %>%
  mutate(constant = 0,
         Na = NA) %>%
  write_dta(paste0(path_dta,"mun_control.dta"))