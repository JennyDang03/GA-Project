rm(list = ls())
options(download.file.method = "wininet")
library(readr)
library(stringr)
library(odbc)
library(dplyr)
library(tidyr)
library(RODBC)
library(futile.logger)
library(data.table)
library(bit64)

setwd("//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer")
source("./functions/parametrizeQuery.R")

# Global settings ---------------------------------------------------------
mapIndividuals = fread("./data/hash_persons_DE.csv")
mapCNPJ8 = fread("./data/hash_CNPJ8_DE.csv")


##### PF CPF  #######
# Query -------------------------------------------------------------------
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")


# Pega Todos os CPFS nascidos antes de 2015
standardQuery <- "SELECT CPF_CD FROM GGGDWPRO_ACC.GGGTB_CPF_RECEITA WHERE CPF_CD_SITUACAO = 0 AND  CPF_DT_NASC < '2015-01-01' SAMPLE 2000000"

if(exists("cpf")) {
  rm(cpf)
}

cpf <- as.data.table(dbGetQuery(connection, standardQuery))

# Grava arquivo com cpj original
fwrite(x = cpf, file = paste0("./results/random_sample", ".csv"))

############ GERA LISTA ANONIMIZADA ############
# Query -------------------------------------------------------------------
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

standardQuery <- read_file('./queries/Pix_lista_amostra.SQL', 
                           locale = locale(encoding = "latin1"))

lista <- as.data.table(dbGetQuery(connection, standardQuery))

################ ANONIMIZACAO IDs ###############
lista$id = trunc(as.integer64(trim(lista$id))/100)
lista = merge(x = lista, y = mapIndividuals,
             by.x = "id", by.y = "cpf",
             all.x = T)

# Check numero de nulls
flog.info(sum(is.na (lista$cpf_hash)))

# Drop identified columns and keep hashed ones ----------------------------
lista$id = data$cpf_hash
lista$cpf_hash       = NULL

# Grava arquivo com cnpj original e respectivo hash
fwrite(x = lista, file = paste0("./results/random_sample_lista_hash", ".csv"))



##### PJ CNPJ 14  - HEADQUARTERS #######
# Query -------------------------------------------------------------------
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

# Pega Todos os CNPJs ativos ou baixados depois de 2019
standardQuery <- "with MEI AS
(
	SELECT  
		TO_NUMBER(EMPRESA.PEJ_CD_CNPJ14) as firm_id
	FROM	BCBDWPRO_ACC.PESTB_PEJ_PESSOA_JURIDICA AS EMPRESA
	LEFT JOIN BCBDWPRO_ACC.PESTB_SIM_SIMPLES_MEI as MEI
	  	ON TO_NUMBER(MEI.SIM_CD_CNPJ14) = TO_NUMBER(EMPRESA.PEJ_CD_CNPJ14)
	WHERE 
	 (SIM_CD_OPCAO_SIMPLES_MEI = 'ME')
  ) 
SELECT  
		TO_NUMBER(EMPRESA.PEJ_CD_CNPJ14) as firm_id14,
		TO_NUMBER(SUBSTR(EMPRESA.PEJ_CD_CNPJ14,1,8)) as firm_id8
FROM	BCBDWPRO_ACC.PESTB_PEJ_PESSOA_JURIDICA AS EMPRESA
LEFT JOIN MEI
ON MEI.firm_id = TO_NUMBER(EMPRESA.PEJ_CD_CNPJ14)
WHERE
  (EMPRESA.SPJ_CD_SITUACAO_PJ_RFB not in (8,3,4) OR (EMPRESA.SPJ_CD_SITUACAO_PJ_RFB in (8,3,4) and YEAR(EMPRESA.PEJ_DT_ALTERACAO_SITUACAO) >= 2019))
  AND EMPRESA.PEJ_IB_MATRIZ = 1
  AND EMPRESA.SPJ_CD_SITUACAO_PJ_RFB <> 1 -- tira empresas com cadastro nulo
  AND EMPRESA.PEJ_DT_ABERTURA <= '2019-01-01'
  AND MEI.firm_id is NULL
   SAMPLE 2000000"

if(exists("cnpj")) {
  rm(cnpj)
}

cnpj <- as.data.table(dbGetQuery(connection, standardQuery))

# Grava arquivo com cnpj original
fwrite(x = cnpj, file = paste0("./results/random_sample_PJ", ".csv"))

########### CADASTRO PF 
########### PEGA IDADE E GENERO

# Query -------------------------------------------------------------------
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")


# Pega genero e idade da amostra de CPFs
standardQuery <- "SELECT PEF_CD_CPF as id, MUN_CD as muni, PEF_DT_NASCIMENTO as birthdate, SEX_ID as gender FROM DL_DEPEP_ESTABILIDADE_FINANCEIRA.Random_sample_Pix_Fin_Rev as RNDSAMPLE LEFT JOIN BCBDWPRO_ACC.PESTB_PEF_PESSOA_FISICA  as CADASTRO ON (RNDSAMPLE.CPF_CD = CADASTRO.PEF_CD_CPF)"

if(exists("lista")) {
  rm(lista)
}

lista <- as.data.table(dbGetQuery(connection, standardQuery))

############### ANONIMIZACAO IDs ###############
lista$id = trunc(as.integer64(trim(lista$id))/100)
lista = merge(x = lista, y = mapIndividuals,
              by.x = "id", by.y = "cpf",
              all.x = T)

# Check numero de nulls
flog.info(sum(is.na (lista$cpf_hash)))

# Drop identified columns and keep hashed ones ----------------------------
lista$id = lista$cpf_hash
lista$cpf_hash       = NULL

# Grava arquivo com cpf hash e dados cadastrais
fwrite(x = lista, file = paste0("./results/random_sample_PF_cadastro", ".csv"))




########### CADASTRO PJ 
########### PEGA CNAE, MUNI, CEP, etc

# Query -------------------------------------------------------------------
DATA_SOURCE_NAME = "teradata-t"
connection <- dbConnect(odbc::odbc(), DATA_SOURCE_NAME, timeout = Inf, Encoding= 'UTF-8')
dbSendQuery(connection,"set role all")

# Pega dados cadastrais da amostra de CNPJ8
standardQuery <- " SELECT firm_id8, MUN_CD, CEP_CD, PEJ_DT_ABERTURA, PEJ_VL_CAPITAL_SOCIAL, CNA_CD, NJR_CD, SPJ_CD_SITUACAO_PJ_RFB
FROM DL_DEPEP_ESTABILIDADE_FINANCEIRA.Random_sample_Pix_Fin_Rev_PJ as Amostra
LEFT JOIN BCBDWPRO_ACC.PESTB_PEJ_PESSOA_JURIDICA as RFB
ON Amostra.firm_id14 = TO_NUMBER(PEJ_CD_CNPJ14)"

if(exists("lista")) {
  rm(lista)
}

lista <- as.data.table(dbGetQuery(connection, standardQuery))

############### ANONIMIZACAO IDs ###############
#lista$firm_id8 = as.integer64(lista$firm_id8)
lista = merge(x = lista, y = mapCNPJ8,
              by.x = "firm_id8", by.y = "CNPJ8",
              all.x = T)

# Check numero de nulls
flog.info(sum(is.na (lista$CNPJ8_HASH)))

# Drop identified columns and keep hashed ones ----------------------------
lista$firm_id8 = lista$CNPJ8_HASH
lista$CNPJ8_HASH       = NULL

# Grava arquivo com CNPJ8 hash e dados cadastrais
fwrite(x = lista, file = paste0("./results/random_sample_PJ_cadastro", ".csv"))



