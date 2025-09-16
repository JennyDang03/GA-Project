# Importa_ppm_pam


#------------------------------------------#
# Workshop: Base dos Dados no R
#------------------------------------------#

# PASSO 1: criar usuário e projeto no BigQuery

# PASSO 2: criar arquivo de credenciais e salvar numa pasta
# https://console.cloud.google.com/apis/credentials/serviceaccountkey?project=handy-resolver-318101   (<project_id>)
# service account name: admin
# role: project owner

#------------------------------------------------------------------------------#
# prefacio
#------------------------------------------------------------------------------#

rm(list = ls())

library(foreign)
library(DBI)
library(bigrquery)
library(dplyr)
library(tidyr)
library(haven)
#library(ggplot2)

# PASSO 3: apontar a autenticação para o arquivo json
bq_auth(path = "C:/Users/mathe/Dropbox/RESEARCH/BigQuery/handy-resolver-318101-5cda1a68e683.json")

# "C:/Users/mathe/Dropbox/- Research/BigQuery/handy-resolver-318101-a8a45d864bea.json"
# "/home/mcs038/Documents/Auxilio/handy-resolver-318101-a8a45d864bea.json"

# PASSO 4: criar conexão com o BigQuery
con <- dbConnect(
  bigrquery::bigquery(),
  billing = "handy-resolver-318101",
  project = "basedosdados"
)

#------------------------------------------------------------------------------#
# Download
#------------------------------------------------------------------------------#

query = "
SELECT ano, id_municipio, valor_producao FROM `basedosdados.br_ibge_ppm.producao_origem_animal`
WHERE ano >= 2013;
"
df.download0 = dbGetQuery(con, query)
df.download0 <- df.download0 %>%
  group_by(ano, id_municipio) %>%
  summarize(production_animal = sum(valor_producao, na.rm = TRUE))

query = "
SELECT ano, id_municipio, valor_producao FROM `basedosdados.br_ibge_ppm.producao_origem_animal`
WHERE ano >= 2013;
"
df.download1 = dbGetQuery(con, query)
df.download1 <- df.download1 %>%
  group_by(ano, id_municipio) %>%
  summarize(production_fish = sum(valor_producao, na.rm = TRUE))

query = "
SELECT ano, id_municipio, valor_producao FROM `basedosdados.br_ibge_pam.municipio_lavouras_permanentes`
WHERE ano >= 2013;
"
df.download2 = dbGetQuery(con, query)

df.download2 <- df.download2 %>%
  group_by(ano, id_municipio) %>%
  summarize(production_perm = sum(valor_producao, na.rm = TRUE))

query = "
SELECT ano, id_municipio, valor_producao FROM `basedosdados.br_ibge_pam.municipio_lavouras_temporarias`
WHERE ano >= 2013;
"
df.download3 = dbGetQuery(con, query)

df.download3 <- df.download3 %>%
  group_by(ano, id_municipio) %>%
  summarize(production_temp = sum(valor_producao, na.rm = TRUE))

df <- merge(df.download0,df.download1)
df <- merge(df,df.download2)
df <- merge(df,df.download3)
df$production <- rowSums(df[, c("production_animal", "production_fish", "production_perm", "production_temp")])
df$log_production <- log(df$production)
df$log_production_animal <- log(df$production_animal)
df$log_production_fish <- log(df$production_fish)
df$log_production_perm <- log(df$production_perm)
df$log_production_temp <- log(df$production_temp)

# Now, lets add Floods
flood <- read_dta(file.path(dta_path,"annualflood_2013_2022.dta"))
df <- merge(df,flood)

write.dta(df, "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/ppm_pam_flood.dta")


##################
# New Import on R
##################

#https://medium.com/basedosdados/como-usar-a-bd-com-r-427aded95448
#https://basedosdados.github.io/mais/api_reference_r/

# instalando a biblioteca
#install.packages('basedosdados')
# carregando a biblioteca na sess?o
library(basedosdados)
set_billing_id("handy-resolver-318101")
#download(query = 'SELECT * FROM `basedosdados.br_ibge_pnadc.microdados` WHERE ano = 2021 ', path = 'C:/Users/mathe/Dropbox/Covid_Estban/raw/pnad/pnad_2021.csv')
rm(list = ls())
download(query = 'SELECT * FROM `basedosdados.br_ibge_pnad_covid.dicionario`', path = 'C:/Users/mathe/Dropbox/Covid_Estban/raw/covid_survey_pnad/survey_pnad_dic.csv')

