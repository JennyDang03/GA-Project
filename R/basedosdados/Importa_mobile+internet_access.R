# Importa_internet


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
SELECT ano AS year, 
        mes AS month, 
        id_municipio, 
        densidade AS mobile_access 
FROM `basedosdados.br_anatel_telefonia_movel.densidade_municipio`
WHERE ano >= 2018 AND ano<2023;
"
df.download0 = dbGetQuery(con, query)
query = "
SELECT ano AS year, 
        mes AS month, 
        id_municipio, 
        densidade AS internet_access 
FROM `basedosdados.br_anatel_banda_larga_fixa.densidade_municipio`
WHERE ano >= 2018 AND ano<2023;
"
df.download1 = dbGetQuery(con, query)

df <- merge(df.download0,df.download1, all=TRUE)

df2019 <- df %>%
  filter(year >= 2019)

query = "
SELECT ano AS year, 
        mes AS month, 
        id_municipio, 
        sinal,
        pessoa,
        SUM(acessos) AS mobile_internet 
FROM `basedosdados.br_anatel_telefonia_movel.microdados`
WHERE 
  ano >= 2018 AND 
  ano<2023 AND
  (produto = 'dados' OR produto = 'voz+dados')
GROUP BY
  sinal, pessoa, id_municipio, year, month
ORDER BY
  id_municipio, pessoa, year, month, sinal;
"
df.download2 = dbGetQuery(con, query)

query = "
SELECT ano AS year, 
        id_municipio, 
        populacao AS pop
FROM `basedosdados.br_ibge_populacao.municipio`
WHERE 
  ano >= 2018 AND 
  ano<2023
ORDER BY
  id_municipio, year;
"
df.download3 = dbGetQuery(con, query)

df2 <- merge(df.download2, df.download3, all.x = TRUE)
df2 <- df2 %>%
  filter(pessoa != "Pessoa Jurídica") %>%
  group_by(id_municipio, year, month, pop) %>%
  summarize(mobile_internet = sum(mobile_internet)) %>%
  mutate(mobile_internet = mobile_internet/pop) %>%
  arrange(id_municipio, year, month, pop)

df <- merge(df,df2, all=TRUE)
df <- df %>%
  arrange(id_municipio, year, month)
df <- df %>%
  select(-pop)

write.dta(df, "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/mobile+internet_access.dta")

