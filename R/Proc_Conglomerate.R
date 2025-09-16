
options(download.file.method = "wininet")

#install.packages(c("data.table","fixest","haven","ggplot2"))

library(data.table) ## For some minor data wrangling
library(fixest)     ## NB: Requires version >=0.9.0
library(haven)
library(ggplot2)
library(dplyr)
library(tidyverse)
library("arrow")

rm(list = ls()) ## Clear workspace

setwd("//sbcdf176/Pix_Matheus$")

# Set global variables
log_dir <- "//sbcdf176/PIX_Matheus$/R/"
path_data <- "//sbcdf176/PIX_Matheus$/DadosOriginais/"

dta_dir <- "//sbcdf060/depep$/DEPEPCOPEF/Projetos/BranchExplosion/Stata/dta/"
origdata_dir <- "//sbcdf060/depep$/DEPEPCOPEF/Projetos/BranchExplosion/OrigData/"

path_query <- "//sbcdf176/PIX_Matheus$/R/DataExtraction/"
path_data <- "//sbcdf176/PIX_Matheus$/DadosOriginais/"
path_dta <- "//sbcdf176/PIX_Matheus$/Stata/dta/" 
path_output <- "//sbcdf176/PIX_Matheus$/Output/"

tempSCR <- read_parquet(paste0(path_data, "Cong_SCR_2022", ".parquet"), as_tibble = TRUE)
tempSCR <- tempSCR %>% select(-dt_ini_ativ, -time_id)

tempUnicad <-  read_parquet(paste0(path_data, "Cong_Unicad_2022", ".parquet"), as_tibble = TRUE)
tempUnicad <- tempUnicad %>% 
  filter(!is.na(CNPJ8_IF)) %>% 
  rename(controle = controle_Unicad,
         macroseg_IF = macroseg_unicad) %>% select(-time_id)

temp <- merge(tempSCR,tempUnicad,  by = c("CNPJ8_IF","controle","macroseg_IF"), all = TRUE) 
temp <- temp %>%
  mutate(belong_cong = if_else(is.na(belong_cong),0,belong_cong),
         digbank = ifelse(cod_cong_prud %in% c("C0084693", "C0084813", "C0084820", "C0085702", "C0085317", "C0084655",
                                               "C0084844", "C0083694", "C0080422", "C0080996", "C0080903") |
                            segmento_IF == 43 | segmento_unicad == 43 | segmento_IF == 44 | segmento_unicad == 44, 1, 0))

# Generate conglomerate ID
temp <- temp %>%
  mutate(tempp = as.character(CNPJ8_IF),
         cod_cong_prud = ifelse(belong_cong == 0, tempp, cod_cong_prud))
temp$cong_id <- as.factor(temp$cod_cong_prud)
temp <- temp %>%
  select(-tempp, -cod_cong_prud)

# Write  to disk?

#temp2 <- read.csv("//sbcdf176/PIX_Matheus$/DadosOriginais/NBranchesCongLevel.csv", fileEncoding = "ISO-8859-2")
temp2 <- read_parquet(paste0(path_data, "NBranchesCongLevel", ".parquet"), as_tibble = TRUE)

temp2<- temp2 %>%
  rename(CNPJ8_IF = cnpj)

temp <- merge(temp, temp2, by = "CNPJ8_IF", all.x = TRUE, all.y = FALSE)

# Ad-hoc bank typology
temp <- temp %>% mutate(public = controle <= 2,
                        big_bank = ifelse(!is.na(number_branches) & number_branches > 1000, 1, 0))

temp <- temp %>% 
  group_by(cong_id) %>% 
  mutate(tempp = max(big_bank)) %>%
  ungroup() 
temp <- temp %>%
  mutate(big_bank = ifelse(tempp == 1, 1, big_bank)) %>%
  select(-tempp)

temp <- temp %>% 
  mutate(tipo_inst = case_when(
    macroseg_IF == "b1" & public == 1 ~ 1,
    big_bank == 1 & macroseg_IF == "b1" & public == 0 ~ 2,
    macroseg_IF == "b3" ~ 4,
    digbank == 1 | macroseg_IF == "n4" ~ 5,
    TRUE ~ 6))

###
#temp  <- read_parquet(paste0(path_data, "Cadastro_IF", ".parquet"))
temp <- temp %>%
  mutate(bank_type = case_when(
    tipo_inst %in% c(1, 2) ~ 1,
    tipo_inst == 5 ~ 2,
    TRUE ~ 3))
temp <- temp %>%
  rename(bank=CNPJ8_IF)
temp <- temp %>%
  mutate(bank = as.integer(bank))
###
write_parquet(temp, sink = paste0(path_data, "Cadastro_IF", ".parquet"))
table(temp$tipo_inst)


old_cadastro_IF <- read_dta(paste0(path_dta, "Cadastro_IF", ".dta"))
table(old_cadastro_IF$tipo_inst)


# // tipo_inst = 1   bancos comerciais Publicos - Federais ou estaduais 
# // tipo_inst = 2   bancos comerciais grandes Privados
# // tipo_inst = 4   cooperativas de credito
# // tipo_inst = 5   bancos digitais ou IPs 
# // tipo_inst = 6   o resto: b1 não-grande e não-digital, n1, b2, etc
# gen bank_type = 3
# replace bank_type = 1 if tipo_inst == 1 | tipo_inst == 2
# replace bank_type = 2 if tipo_inst == 5

