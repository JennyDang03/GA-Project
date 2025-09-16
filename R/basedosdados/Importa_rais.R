# Importa_rais


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
SELECT * FROM `basedosdados.br_me_rais.dicionario`;
"
df.download0 = dbGetQuery(con, query)
df.download01 <- df.download0 %>%
 filter(!(nome_coluna %in% c("regioes_administrativas_df","bairros_rj","bairros_fortaleza","distritos_sp","bairros_sp","cep", "subatividade_ibge", "subsetor_ibge")))
#filter(id_tabela %in% c("microdados_estabelecimentos")) %>%
#filter(id_tabela %in% c("microdados_vinculos")) %>%

query = "
SELECT *  
FROM `basedosdados.br_me_rais.microdados_estabelecimentos`
WHERE ano >= 2018
LIMIT 1000;
"
df.download1 = dbGetQuery(con, query)



query = "
SELECT id_municipio,
    ano,
    SUM(quantidade_vinculos_ativos) AS qt_jobs,
    SUM(quantidade_vinculos_clt) AS qt_jobs_clt,
    SUM(quantidade_vinculos_estatutarios) AS qt_jobs_estatutarios,
    COUNT(*) AS qt_firms,  
FROM `basedosdados.br_me_rais.microdados_estabelecimentos`
WHERE ano >= 2013
GROUP BY id_municipio, ano;
"
df.download1 = dbGetQuery(con, query)

df.download1$lqt_firms <- log(df.download1$qt_firms)
df.download1$lqt_jobs <- log(df.download1$qt_jobs)
df.download1$lqt_jobs_clt <- log(df.download1$qt_jobs_clt)
df.download1$lqt_jobs_estatutarios <- log(df.download1$qt_jobs_estatutarios)


# Now, lets add Floods
flood <- read_dta(file.path(dta_path,"annualflood_2013_2022.dta"))
flood <- flood %>%
  filter(ano >= 2013)
df.download1 <- merge(df.download1,flood)

write.dta(df.download1, "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/rais_flood.dta")



for(year in 2018:2022) {
  for(month in 1:12) {
    # Determine the column name for the monthly remuneration
    remuneration_column <- switch(month,
                                  "valor_remuneracao_janeiro",
                                  "valor_remuneracao_fevereiro",
                                  "valor_remuneracao_marco",
                                  "valor_remuneracao_abril",
                                  "valor_remuneracao_maio",
                                  "valor_remuneracao_junho",
                                  "valor_remuneracao_julho",
                                  "valor_remuneracao_agosto",
                                  "valor_remuneracao_setembro",
                                  "valor_remuneracao_outubro",
                                  "valor_remuneracao_novembro",
                                  "valor_remuneracao_dezembro")
    
    query = paste0("
    SELECT ", year, " AS year,
           ", month, " AS month,
           COALESCE(id_municipio_trabalho, id_municipio) AS id_municipio,
           COUNT(*) AS n_jobs,
           SUM(", remuneration_column, ") AS total_salary
    FROM `basedosdados.br_me_rais.microdados_vinculos`
    WHERE ano = ", year, " 
      AND (mes_admissao IS NULL OR mes_admissao <= ", month, ") 
      AND (mes_desligamento IS NULL OR mes_desligamento >= ", month, ")
    GROUP BY year, month, COALESCE(id_municipio_trabalho, id_municipio);
    ")
    df.download = dbGetQuery(con, query)
    write.dta(df.download, paste0("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/rais_jobs_salaries_", year, month, ".dta"))
    assign(paste0("df.rais.", year, month), df.download)
  }
}

# Initialize an empty list to store the data frames
df_list <- list()

# Loop through the years and months to load and bind the data
for(year in 2018:2022) {
  for(month in 1:12) {
    # Create the file path for the .dta file
    file_path <- paste0("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/rais_jobs_salaries_", year, month, ".dta")
    
    # Create the name of the data frame to be assigned
    df_name <- paste0("df.rais.", year, month)
    
    # Check if the data frame is already loaded
    if (!exists(df_name)) {
      # Check if the .dta file exists
      if (file.exists(file_path)) {
        # Read the .dta file if it exists
        df_list[[length(df_list) + 1]] <- read_dta(file_path)
        assign(df_name, df_list[[length(df_list)]])  # Assign the data frame to the environment
      } else {
        message("File not found for year ", year, " month ", month)
      }
    } else {
      # Add the already loaded data frame to the list
      df_list[[length(df_list) + 1]] <- get(df_name)
    }
  }
}
# Combine all data frames in the list into a single data frame
final_df <- do.call(rbind, df_list)
# Save the final combined data frame into a .dta file
final_file_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/rais_jobs_salaries.dta"
write_dta(final_df, final_file_path)
message("Final combined data has been saved to: ", final_file_path)


# Now, lets add time_id. Also, convert to muni_cd. then make sure it is ready for flood rais event study ------------------------------------------------------
path_dta <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/"
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
temp <- temp %>% select(muni_cd,pop2022)
mun_convert <- merge(mun_convert, temp, by= c("muni_cd"), all.x = TRUE, all.y = TRUE)
setorder(mun_convert, pop2022)
mun_convert3 <- mun_convert %>% select(muni_cd, id_municipio)


# Create time_id, also, change id_municipio to muni_cd, delete year, month
final_df <- final_df %>% mutate(time_id = (year-1960)*12 + month-1)
final_df <- final_df %>% mutate(id_municipio = as.integer(id_municipio))
final_df <- merge(final_df, mun_convert3, by="id_municipio", all.x = TRUE)
#Create log variables for all except id_municipio, year, month, time_id, muni_cd
final_df <- final_df %>%
  mutate(across(
    -c(id_municipio, year, month, time_id, muni_cd),
    ~ log1p(.),
    .names = "l{.col}"
  ))

final_df <- final_df %>% select(-id_municipio)
final_df <- final_df %>% arrange(muni_cd, time_id)
# variables: time_id, muni_cd, year, month, n_jobs, total_salary, ln_jobs, ltotal_salary
final_file_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/rais_jobs_salaries.dta"
write_dta(final_df, final_file_path)
message("Final combined data has been saved to: ", final_file_path)




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




