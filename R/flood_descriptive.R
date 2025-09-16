# Summary Stats, Descriptive evidence, Balance stats

# https://cran.r-project.org/web/packages/cobalt/vignettes/cobalt.html
# https://ngreifer.github.io/cobalt/
# https://www.youtube.com/watch?v=tQredW74x_o&ab_channel=Econometrics%2CCausality%2CandCodingwithDr.HK
# https://rdrr.io/cran/RCT/man/balance_table.html

#install.packages("cobalt")

library(data.table) ## For some minor data wrangling
library(fixest)     ## NB: Requires version >=0.9.0
library(haven)
library(ggplot2)
library(dplyr)
library("cobalt")
library(vtable)
library(skimr)
library(stargazer)
library(tidyr)
library(scales)
library(modelsummary)
library(gt)
library(kableExtra)
library(flextable)
rm(list = ls()) ## Clear workspace

options(modelsummary_format_numeric_latex = "plain")


setwd("//sbcdf176/Pix_Matheus$")
path_main <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/"
path_main <- "//sbcdf176/PIX_Matheus$/"
# Set file paths
path_query <- paste0(path_main, "R/DataExtraction/")
path_data <- paste0(path_main, "DadosOriginais/")
path_dta <- paste0(path_main, "Stata/dta/")
path_output <- paste0(path_main, "Output/")
log_path <- paste0(path_main, "Stata/log/")
dta_path <- paste0(path_main, "Stata/dta/")
output_path <- paste0(path_main, "Output/")
origdata_path <- paste0(path_main, "DadosOriginais/")
R_path <- paste0(path_main, "R/")

xl <- -39
xu <- 52
################################################################################
#-------------------------------------------------------------------------------
# Load auxiliary data
#-------------------------------------------------------------------------------

#pop 2019, pib 2019, mobile_access 2019, precipitation 2019, ted 2019, boleto 2019, card 2019, pix 2022, ted 2022, boleto 2022, card 2022
# Rural, share agriculture, manufactoring, services. literacy rate. 
# Bank accounts, banked population, 
# loans??
# deposits, branches, deposits hhi, cash inventory, service stations,
# observations
#number of floods in 30 years

data <- read_dta(paste0(dta_path,"flood_weekly_2020_2022.dta")) %>%
  select(muni_cd, date_flood)
data$flood <- ifelse(!is.na(data$date_flood), 1, 0)
data$treatment <- ifelse(data$flood == 1, "Flooded Municipalities", "Non-flooded Municipalities")
data <- select(data, -date_flood)
data <- distinct(data)

# Load static Fixed Effects
mun_fe <- read_dta(file.path(dta_path, "mun_fe.dta")) %>% 
  mutate(rural = ifelse(rural_urban == 3 | rural_urban == 4, 1, 0)) %>%
  select(muni_cd, flood_risk5, pop2010, rural, pib_2017, pop2022)

# Load time varying Fixed Effects
mun_control <- read_dta(paste0(dta_path,"mun_control.dta")) %>%
  filter(year == 2019) %>%
  select(muni_cd, pre, mobile_access) %>% 
  group_by(muni_cd) %>%
  summarise(pre = mean(pre), mobile_access = mean(mobile_access)) %>% ungroup()

data <- merge(data, mun_fe, by="muni_cd", all.x = TRUE)
data <- merge(data, mun_control, by="muni_cd", all.x = TRUE)

# Now, number of floods
# read "C:\Users\mathe\Dropbox\RESEARCH\Natural Disasters\dta\nat_dis_monthly_1991_2022.dta"
floods <- read_dta(paste0(dta_path,"nat_dis_monthly_1991_2022.dta")) 
# get time id to year and month. get everything up until 2020 10. or until  ym(2017,12) = 695
floods <- floods %>%
  filter(time_id <= 695) %>%
  select(muni_cd, flood_n) %>%
  group_by(muni_cd) %>%
  summarise(flood_number = sum(flood_n, na.rm = TRUE)) %>%
  ungroup()
floods <- floods %>% select(muni_cd, flood_number)

data <- merge(data, floods, by="muni_cd", all.x = TRUE)

# Estban 2019
estban <- read_dta(paste0(dta_path, "estban_2018_2023.dta"))
estban <- estban %>%
  select(muni_cd, time_id, deposit, assets, liab, caixa) %>%
  filter(time_id >= 708, time_id <= 719) %>% # that's 2019
  group_by(muni_cd) %>%
  summarise(deposit = mean(deposit, na.rm = TRUE),
            assets = mean(assets, na.rm = TRUE),
            liab = mean(liab, na.rm = TRUE),
            caixa = mean(caixa, na.rm = TRUE)) %>%
  ungroup()

data <- merge(data, estban, by="muni_cd", all.x = TRUE)

#data <- data %>%  mutate(across(c(deposit, assets, liab, caixa), ~ replace_na(., 0)))
# Pix, ted, boleto, card


# CCS

# Credito


# make Table!


variables <- c("treatment", "pop2010", "pop2022", "pib_2017", "pre", "mobile_access", "flood_number", "deposit", "assets", "liab", "caixa")
variables_labels <- c("Treatment", "Population 2010", "Population", "GDP", "Precipitation", "Mobile Access", "Flood Number", "Deposit", "Assets", "Liabilities", "Cash Inventory")
selected_vars <- data %>%
  select(all_of(variables)) %>%
  rename_with(~ variables_labels, all_of(variables))

flooded_n <- comma(nrow(data %>% filter(flood == 1)))
non_flooded_n <- comma(nrow(data %>% filter(flood == 0)))
new_rows <- data.frame('Observations', "", flooded_n, "", "", non_flooded_n, "")


datasummary(`Population` * Arguments(fmt = "%.0f") + `GDP` * Arguments(fmt = "%.1e")
                   + `Precipitation` * Arguments(fmt = "%.1f") + `Mobile Access` * Arguments(fmt = "%.0f%%") 
                   + `Flood Number` * Arguments(fmt = "%.1f") + `Deposit` * Arguments(fmt = "%.1e")
                   + `Assets` * Arguments(fmt = "%.1e") + `Liabilities` * Arguments(fmt = "%.1e") 
                   + `Cash Inventory`  * Arguments(fmt = "%.1e")
                   ~ `Treatment` * (Mean + Median + SD),
                   data = selected_vars, add_rows = new_rows, 
            output = paste0(output_path, "tables/combined_summary_stats.tex"))


tab <- datasummary(`Population` * Arguments(fmt = "%.0f") + `GDP` * Arguments(fmt = "%.1e")
                   + `Precipitation` * Arguments(fmt = "%.1f") + `Mobile Access` * Arguments(fmt = "%.0f%%")
                   + `Flood Number` * Arguments(fmt = "%.1f") + `Deposit` * Arguments(fmt = "%.1e")
                   + `Assets` * Arguments(fmt = "%.1e") + `Liabilities` * Arguments(fmt = "%.1e")
                   + `Cash Inventory`  * Arguments(fmt = "%.1e")
                   ~ `Treatment` * (Mean + Median + SD),
                   data = selected_vars, add_rows = new_rows, output = 'gt') %>%
  tab_options(table_body.hlines.width = 0, table.border.top.width = 0, table.border.bottom.width = 0) %>%
  tab_style(cell_borders(sides = "bottom", color = "black", weight = px(1)), locations = cells_body(rows = 9))
print(tab)
gt::gtsave(tab, filename = paste0(output_path, "tables/combined_summary_stats2.tex"))



# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# CCS_Muni
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# CCS_Muni_stock
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# CCS_first_account
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# CCS_HHI
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# Adoption_Pix
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# CCS_Muni_IF
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# Pix_Muni_Bank
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# Pix_Muni_flow
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------



#-------------------------------------------------------------------------------
# Old Graphs redone. 
#-------------------------------------------------------------------------------
#Base_week_muni.dta

# Credito

# Pix_individual

# ESTBAN

#RAIS







estban_agencia <- read_dta(paste0(dta_path, "estban_agencia_basedosdados_2020_10.dta"))






estban <- estban %>%
  select(muni_cd, time_id, deposit, assets, liab, caixa) %>%
  filter(time_id >= 708, time_id <= 719) %>% # that's 2019
  group_by(muni_cd) %>%
  summarise(deposit = mean(deposit, na.rm = TRUE),
            assets = mean(assets, na.rm = TRUE),
            liab = mean(liab, na.rm = TRUE),
            caixa = mean(caixa, na.rm = TRUE)) %>%
  ungroup()
