# Importa_pib_mun


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
SELECT * FROM `basedosdados.br_ibge_pib.municipio`;
"
df.download = dbGetQuery(con, query)

write.dta(df.download, "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/Importa_pib_mun.dta")

# "C:/Users/mathe/Dropbox/- Research/BigQuery/Auxilio2020_sem_extensao.dta"
# "/home/mcs038/Documents/Auxilio/Auxilio2020_sem_extensao.dta"



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

