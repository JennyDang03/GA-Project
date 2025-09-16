#CCS_analysis

################################################################################
options(download.file.method = "wininet")
rm(list = ls()) ## Clear workspace

# install.packages("coefplot")
# install.packages("magrittr")
library(readr)
library(stringr)
library(dplyr)
library(tidyr)
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
library(coefplot)
library(magrittr)
library(odbc)
library(RODBC)

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

xl_month <- -6
xu_month <- 12
xl_balanced_month <- -6
xu_balanced_month <- 12
xl_balanced_covid_month <- -3
xu_balanced_covid_month <- 3
################################################################################
#-------------------------------------------------------------------------------
# Load auxiliary data
#-------------------------------------------------------------------------------
source(paste0(R_path, "/auxiliary_data.r"))


Cadastro_IF <- read_parquet(paste0(path_data, "Cadastro_IF", ".parquet"))
Cadastro_IF <- Cadastro_IF %>%
  select(bank, tipo_inst, bank_type, macroseg_IF, cong_id) 
Cadastro_IF <- data.table(Cadastro_IF)
Cadastro_IF <- Cadastro_IF %>% rename(macroseg_if_txt = macroseg_IF)

mun_fe <- mun_fe %>% select(muni_cd, pop2022)

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
temp <- read_dta(file.path(dta_path, "mun_fe.dta"))
temp <- temp %>% select(muni_cd,pop2022,pop2010)
mun_convert <- merge(mun_convert, temp, by= c("muni_cd"), all.x = FALSE, all.y = FALSE)
setorder(mun_convert, pop2022)
mun_convert <- mun_convert %>% select(muni_cd, id_municipio, pop2022)

pix_flag <- read_dta(file.path(dta_path,"pixdata_updated.dta"))
dec2020_code <- (2020 - 1960) * 12 + 12 - 1 # Equals 731
pix_flag <- pix_flag %>%
  filter(date == dec2020_code) %>%
  rename(id_municipio = cod_munic) %>%
  select(id_municipio, value_receiver_individual) %>%
  rename(pix = value_receiver_individual)
pix_flag <- merge(pix_flag, mun_convert, by = "id_municipio", all.x = FALSE, all.y = FALSE)
pix_flag <- pix_flag %>%
  mutate(avpix = pix / pop2022 / 1000)
median_avpix <- median(pix_flag$avpix, na.rm = TRUE)
pix_flag <- pix_flag %>%
  mutate(highpix = if_else(avpix > median_avpix, 1, 0))
pix_flag <- pix_flag %>% select(muni_cd, highpix)
mun_fe <- merge(mun_fe, pix_flag, by = "muni_cd", all.x = TRUE, all.y = FALSE)


#-------------------------------------------------------------------------------
# Functions
#-------------------------------------------------------------------------------
source(paste0(R_path,"/functions/stata_week_number.R"))
source(paste0(R_path,"/functions/gen_week_list.R"))
source(paste0(R_path,"/functions/time_id_to_month.R"))
source(paste0(R_path,"/functions/time_id_to_year.R"))
source(paste0(R_path,"/functions/week_to_month.R"))
source(paste0(R_path,"/functions/week_to_year.R"))
source(paste0(R_path,"/functions/prepare_data.R"))
source(paste0(R_path,"/functions/twfe.R"))
source(paste0(R_path,"/functions/ylim_function.R"))
source(paste0(R_path,"/functions/print_twfe.R"))
source(paste0(R_path,"/functions/twfe2.R"))
source(paste0(R_path,"/functions/ylim_function2.R"))
source(paste0(R_path,"/functions/print_twfe2.R"))
source(paste0(R_path,"/functions/twfe_ind.R"))
source(paste0(R_path,"/functions/print_twfe_week.R"))
source(paste0(R_path,"/functions/print_twfe_month.R"))
source(paste0(R_path,"/functions/day_to_stata_month.R"))
source(paste0(R_path,"/functions/prepare_data_syn_month.R"))

# Query -------------------------------------------------------------------

read_dta_function <- function(file,fe){
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
  # FE, and Control Variables
  dat <- merge(dat, fe, by="muni_cd", all.x = TRUE) 
  return(dat)
}
#CCS_Muni_stock_v2.dta
# Variables: week, muni_cd, tipo, muni_stock, muni_stock_w, banked_pop
#             lmuni_stock, lmuni_stock_w, lbanked_pop

# CCS_Muni_IF_PF_v2.dta and CCS_Muni_IF_PJ_v2.dta
# Variables: week, muni_cd, tipo, bank, tipo_inst, bank_type,
#             stock,
#             lstock

#CCS_Muni_first_account_v2.dta
# Variables: week, muni_cd, tipo, first_account
#             lfirst_account


# Load the datasets
CCS_Muni_stock <- read_dta_function("CCS_Muni_stock_v2.dta",mun_fe)
CCS_Muni_first_account <- read_dta_function("CCS_Muni_first_account_v2.dta",mun_fe)

whole_country0 <- merge(CCS_Muni_stock, CCS_Muni_first_account, by = c("week", "muni_cd", "tipo", "pop2022", "highpix", "time", "month", "year"), all = TRUE)
whole_country <- whole_country0 %>% 
  select(week, tipo, pop2022, muni_stock, muni_stock_w, banked_pop, first_account, month, year) %>%
  filter(tipo == 1) %>%  # Only PF
  group_by(week, month, year) %>%
  summarise(across(c(muni_stock, muni_stock_w, banked_pop, first_account, pop2022), sum, na.rm = TRUE))

whole_country <- whole_country %>%
  mutate(accounts_per_capita = muni_stock / pop2022,
         accounts_per_banked = muni_stock / banked_pop,
         accounts_per_capita_w = muni_stock_w / pop2022,
         accounts_per_banked_w = muni_stock_w / banked_pop)

high_low_pix <- whole_country0 %>%
  select(week, tipo, highpix, muni_stock, muni_stock_w, banked_pop, first_account, pop2022, month, year) %>%
  filter(tipo == 1) %>%  # Only PF
  group_by(week, highpix, month, year) %>%
  summarise(across(c(muni_stock, muni_stock_w, banked_pop, first_account, pop2022), sum, na.rm = TRUE))

high_low_pix <- high_low_pix %>%
  mutate(accounts_per_capita = muni_stock / pop2022,
         accounts_per_banked = muni_stock / banked_pop,
         accounts_per_capita_w = muni_stock_w / pop2022,
         accounts_per_banked_w = muni_stock_w / banked_pop)
# calculate the sum of all rows in mun_fe
pop <- sum(mun_fe$pop2022, na.rm = TRUE)

high_low_pix <- high_low_pix %>%
  filter(!is.na(week) & !is.na(accounts_per_capita) & !is.na(highpix) & !is.na(muni_stock) & !is.na(banked_pop))


#####
#Test
# set.seed(42)
# weeks <- 3068:3327
# muni_stock <- ((runif(length(weeks), 0, 1)) + weeks) * (weeks^0.1)
# whole_country <- data.frame(
#   week = weeks,
#   muni_stock = muni_stock,
#   highpix = 1
# )
# whole_country$month <- week_to_month(whole_country$week)
# whole_country$year <- week_to_year(whole_country$week)
# set.seed(41)
# weeks <- 3068:3327
# muni_stock <- ((runif(length(weeks), 0, 1)) + weeks) * (weeks^0.2)
# whole_country2 <- data.frame(
#   week = weeks,
#   muni_stock = muni_stock,
#   highpix = 0
# )
# whole_country2$month <- week_to_month(whole_country2$week)
# whole_country2$year <- week_to_year(whole_country2$week)
# high_low_pix <- rbind(whole_country, whole_country2)
#######################################

#### Now, create graphs. 
title_size = 24
x_size = 0.1
y_size = 20
axis_size = 18
line_size = 2
legend_key_size = 2
legend_size = 20
breaks <- whole_country$week[whole_country$week %% 52 == 26]
labels <- whole_country$year[whole_country$week %% 52 == 0]
# week = 3165 is the start of Pix. 


gg <- ggplot(whole_country %>% filter(week %% 4 == 0), aes(x = week, y = muni_stock / 10^6)) +
  geom_line(size = line_size, color = "#1f77b4") +  # Single line with consistent color
  geom_vline(xintercept = 3165, linetype = "dotted", color = "black") +
  theme_minimal() +
  scale_x_continuous(breaks = breaks, labels = labels) +
  labs(title = "Financial Accounts Over Time",
       x = "Year", y = "Financial Accounts (Millions)") +
  theme(panel.grid.major.x = element_blank(),
        panel.grid.major.y = element_line(color = "lightgrey"),
        panel.grid.minor = element_blank(),
        panel.background = element_blank(),
        axis.line = element_line(color = "black"),
        plot.title = element_text(hjust = 0.5, size = title_size),
        axis.title.x = element_text(size = y_size),
        axis.title.y = element_text(size = y_size),
        axis.text.x = element_text(size = axis_size),
        axis.text.y = element_text(size = axis_size)) 
print(gg)
ggsave(paste0(output_path, "images/whole_country_muni_stock.png"), gg, width = 8, height = 6, dpi = 300, bg = "white")



# Banked Population Over Time
gg <- ggplot(whole_country %>% filter(week %% 4 == 0), aes(x = week, y = banked_pop / 10^6)) +
  geom_line(size = line_size, color = "#1f77b4") +  # Single line with consistent color
  geom_vline(xintercept = 3165, linetype = "dotted", color = "black") +
  theme_minimal() +
  scale_x_continuous(breaks = breaks, labels = labels) +
  labs(title = "Banked Population Over Time",
       x = "Year", y = "Banked Population (Millions)") +
  theme(panel.grid.major.x = element_blank(),
        panel.grid.major.y = element_line(color = "lightgrey"),
        panel.grid.minor = element_blank(),
        panel.background = element_blank(),
        axis.line = element_line(color = "black"),
        plot.title = element_text(hjust = 0.5, size = title_size),
        axis.title.x = element_text(size = y_size),
        axis.title.y = element_text(size = y_size),
        axis.text.x = element_text(size = axis_size),
        axis.text.y = element_text(size = axis_size)) 
print(gg)
ggsave(paste0(output_path, "images/whole_country_banked_pop.png"), gg, width = 8, height = 6, dpi = 300, bg = "white")


# Accounts Per Capita Over Time
gg <- ggplot(whole_country %>% filter(week %% 4 == 0), aes(x = week, y = accounts_per_capita)) +
  geom_line(size = line_size, color = "#1f77b4") +  # Single line with consistent color
  geom_vline(xintercept = 3165, linetype = "dotted", color = "black") +
  theme_minimal() +
  scale_x_continuous(breaks = breaks, labels = labels) +
  labs(title = "Accounts Per Capita Over Time",
       x = "Year", y = "Accounts Per Capita") +
  theme(panel.grid.major.x = element_blank(),
        panel.grid.major.y = element_line(color = "lightgrey"),
        panel.grid.minor = element_blank(),
        panel.background = element_blank(),
        axis.line = element_line(color = "black"),
        plot.title = element_text(hjust = 0.5, size = title_size),
        axis.title.x = element_text(size = y_size),
        axis.title.y = element_text(size = y_size),
        axis.text.x = element_text(size = axis_size),
        axis.text.y = element_text(size = axis_size)) 
print(gg)
ggsave(paste0(output_path, "images/whole_country_accounts_per_capita.png"), gg, width = 8, height = 6, dpi = 300, bg = "white")

# High vs Low
legend_labels <- c("Low Pix", "High Pix")  # Custom legend names
line_types <- c( "dotted", "solid")         # Custom line types

# Municipal Stock Over Time (High vs Low Pix)
gg <- ggplot(high_low_pix %>% filter(week %% 4 == 0), aes(x = week, y = muni_stock/10^6, 
                               color = as.factor(highpix), 
                               linetype = as.factor(highpix))) +
  geom_line(size = line_size) +
  geom_vline(xintercept = 3165, linetype = "dotted", color = "black") +
  theme_minimal() +
  scale_x_continuous(breaks = breaks, labels = labels) +
  scale_color_manual(values = c("#1f77b4", "#ff7f0e"), labels = legend_labels) +  # Custom legend names and colors
  scale_linetype_manual(values = line_types, labels = legend_labels) +     # Custom line types
  labs(title = "Financial Accounts Over Time",
       x = "Year", y = "Financial Accounts (Millions)", color = "High Pix", linetype = "High Pix") +
  theme(panel.grid.major.x = element_blank(),  # Remove vertical grid lines
        panel.grid.major.y = element_line(color = "lightgrey"),  # Light grey horizontal grid lines
        panel.grid.minor = element_blank(),  # Remove minor grid lines
        panel.background = element_blank(),  # Remove background panel
        axis.line = element_line(color = "black"),  # Black axis lines
        plot.title = element_text(hjust = 0.5, size = title_size),  # Align title in the middle and increase size
        axis.title.x = element_text(size = y_size),  # Increase x-axis title size
        axis.title.y = element_text(size = y_size),  # Increase y-axis title size
        axis.text.x = element_text(size = axis_size),  # Increase x-axis text size
        axis.text.y = element_text(size = axis_size),  # Increase y-axis text size
        legend.position = "bottom",  # Position of the legend at the bottom
        legend.title = element_blank(),  # Remove legend title
        legend.key.size = unit(legend_key_size, "lines"),  # Increase the size of the legend keys
        legend.text = element_text(size = legend_size),  # Increase the size of the legend text
        legend.background = element_rect(fill = "transparent")) +  # Transparent background for legend
  guides(color = guide_legend(nrow = 1, byrow = TRUE), 
         linetype = guide_legend(nrow = 1, byrow = TRUE))  # Break legend into 2 rows
print(gg)
ggsave(paste0(output_path,"images/HL_muni_stock.png"), gg, width = 8, height = 6, dpi = 300, bg = "white")


# Banked Population Over Time (High vs Low Pix)
gg <- ggplot(high_low_pix, aes(x = week, y = banked_pop/10^6, 
                               color = as.factor(highpix), 
                               linetype = as.factor(highpix))) +
  geom_line(size = line_size) +
  geom_vline(xintercept = 3165, linetype = "dotted", color = "black") +
  theme_minimal() +
  scale_x_continuous(breaks = breaks, labels = labels) +
  scale_color_manual(values = c("#1f77b4", "#ff7f0e"), labels = legend_labels) +  # Custom legend names and colors
  scale_linetype_manual(values = line_types, labels = legend_labels) +     # Custom line types
  labs(title = "Individuals with Accounts Over Time",
       x = "Year", y = "Individuals with Accounts (millions)", color = "High Pix", linetype = "High Pix") +
  theme(panel.grid.major.x = element_blank(),  # Remove vertical grid lines
        panel.grid.major.y = element_line(color = "lightgrey"),  # Light grey horizontal grid lines
        panel.grid.minor = element_blank(),  # Remove minor grid lines
        panel.background = element_blank(),  # Remove background panel
        axis.line = element_line(color = "black"),  # Black axis lines
        plot.title = element_text(hjust = 0.5, size = title_size),  # Align title in the middle and increase size
        axis.title.x = element_text(size = y_size),  # Increase x-axis title size
        axis.title.y = element_text(size = y_size),  # Increase y-axis title size
        axis.text.x = element_text(size = axis_size),  # Increase x-axis text size
        axis.text.y = element_text(size = axis_size),  # Increase y-axis text size
        legend.position = "bottom",  # Position of the legend at the bottom
        legend.title = element_blank(),  # Remove legend title
        legend.key.size = unit(legend_key_size, "lines"),  # Increase the size of the legend keys
        legend.text = element_text(size = legend_size),  # Increase the size of the legend text
        legend.background = element_rect(fill = "transparent")) +  # Transparent background for legend
  guides(color = guide_legend(nrow = 1, byrow = TRUE), 
         linetype = guide_legend(nrow = 1, byrow = TRUE))  # Break legend into 2 rows
print(gg)
ggsave(paste0(output_path,"images/HL_banked_pop.png"), gg, width = 8, height = 6, dpi = 300, bg = "white")


# Accounts Per Capita Over Time (High vs Low Pix)
gg <- ggplot(high_low_pix, aes(x = week, y = accounts_per_capita, 
                               color = as.factor(highpix), 
                               linetype = as.factor(highpix))) +
  geom_line(size = line_size) +
  geom_vline(xintercept = 3165, linetype = "dotted", color = "black") +
  theme_minimal() +
  scale_x_continuous(breaks = breaks, labels = labels) +
  scale_color_manual(values = c("#1f77b4", "#ff7f0e"), labels = legend_labels) +  # Custom legend names and colors
  scale_linetype_manual(values = line_types, labels = legend_labels) +     # Custom line types
  labs(title = "Accounts per Individuals Over Time",
       x = "Year", y = "Accounts per Individuals", color = "High Pix", linetype = "High Pix") +
  theme(panel.grid.major.x = element_blank(),  # Remove vertical grid lines
        panel.grid.major.y = element_line(color = "lightgrey"),  # Light grey horizontal grid lines
        panel.grid.minor = element_blank(),  # Remove minor grid lines
        panel.background = element_blank(),  # Remove background panel
        axis.line = element_line(color = "black"),  # Black axis lines
        plot.title = element_text(hjust = 0.5, size = title_size),  # Align title in the middle and increase size
        axis.title.x = element_text(size = y_size),  # Increase x-axis title size
        axis.title.y = element_text(size = y_size),  # Increase y-axis title size
        axis.text.x = element_text(size = axis_size),  # Increase x-axis text size
        axis.text.y = element_text(size = axis_size),  # Increase y-axis text size
        legend.position = "bottom",  # Position of the legend at the bottom
        legend.title = element_blank(),  # Remove legend title
        legend.key.size = unit(legend_key_size, "lines"),  # Increase the size of the legend keys
        legend.text = element_text(size = legend_size),  # Increase the size of the legend text
        legend.background = element_rect(fill = "transparent")) +  # Transparent background for legend
  guides(color = guide_legend(nrow = 1, byrow = TRUE), 
         linetype = guide_legend(nrow = 1, byrow = TRUE))  # Break legend into 2 rows
print(gg)
ggsave(paste0(output_path,"images/HL_accounts_per_capita.png"), gg, width = 8, height = 6, dpi = 300, bg = "white")




#### Now, look at conglomerates. 
#CCS_Muni_IF_PF <- read_dta_function("CCS_Muni_IF_PF_v2.dta",mun_fe)
#CCS_Muni_IF_PJ <- read_dta_function("CCS_Muni_IF_PJ_v2.dta",mun_fe)


