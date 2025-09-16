

# Importa_ESTBAN


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
library(wk)

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
SELECT * FROM `basedosdados.br_bcb_estban.agencia` WHERE ano >= 2018;
"
df.download0 = dbGetQuery(con, query)

write.dta(df.download0, "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/estban_agencia_basedosdados.dta")
#write.dta(df.download0, "C:/Users/mathe/Dropbox/RESEARCH/Municipios/dta/diretorio_brasileiro_mun.dta")


query = "
SELECT * FROM `basedosdados.br_bcb_estban.municipio` WHERE ano >= 2011;
"
df.download0 = dbGetQuery(con, query)

write.dta(df.download0, "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/estban_mun_basedosdados.dta")

##################
# New Import on R
##################

#https://medium.com/basedosdados/como-usar-a-bd-com-r-427aded95448
#https://basedosdados.github.io/mais/api_reference_r/
#install.packages("basedosdados")
#library("basedosdados")

# Defina o seu projeto no Google Cloud
#set_billing_id("<YOUR_PROJECT_ID>")

# Para carregar o dado direto no R
#query <- bdplyr("br_bcb_estban.agencia")
#df <- bd_collect(query)

# instalando a biblioteca
#install.packages('basedosdados')
# carregando a biblioteca na sess?o
library(basedosdados)
set_billing_id("handy-resolver-318101")
#download(query = 'SELECT * FROM `basedosdados.br_ibge_pnadc.microdados` WHERE ano = 2021 ', path = 'C:/Users/mathe/Dropbox/Covid_Estban/raw/pnad/pnad_2021.csv')
rm(list = ls())
download(query = 'SELECT * FROM `basedosdados.br_ibge_pnad_covid.dicionario`', path = 'C:/Users/mathe/Dropbox/Covid_Estban/raw/covid_survey_pnad/survey_pnad_dic.csv')






query = "
SELECT * FROM `basedosdados.br_bcb_estban.agencia` WHERE ano = 2020 AND mes = 10;
"
df.download0 = dbGetQuery(con, query)

write.dta(df.download0, "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/estban_agencia_basedosdados_2020_10.dta")
#write.dta(df.download0, "C:/Users/mathe/Dropbox/RESEARCH/Municipios/dta/diretorio_brasileiro_mun.dta")
