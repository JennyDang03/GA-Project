# Run SQL test

rm(list = ls())

library(DBI)
library(readr)


# Create a data frame with the inserted values
data1 <- data.frame(
  LAF_DT_LIQUIDACAO = as.Date(c('2023-10-01', '2023-10-01', '2023-10-02', '2023-10-03', '2023-10-03', '2023-10-04', '2023-10-04', '2023-10-05', '2023-10-05', '2023-10-06', '2023-10-06', '2023-10-07', '2023-10-08', '2023-10-08', '2023-10-09', '2023-10-09')),
  TPP_CD_TIPO_PESSOA_PAGADOR = c(1, 1, 2, 2, 1, 2, 1, 1, 2, 1, 2, 2, 1, 2, 1, 2),
  TPP_CD_TIPO_PESSOA_RECEBEDOR = c(1, 2, 1, 2, 1, 1, 2, 2, 1, 1, 2, 2, 1, 1, 2, 2),
  LAF_VL = c(45.0, 70.0, 90.0, 32.0, 60.0, 42.0, 75.0, 55.0, 25.0, 12.0, 88.0, 51.0, 78.0, 41.0, 63.0, 28.0),
  PES_NU_CPF_CNPJ_PAGADOR = c(1, 2, 7, 8, 4, 10, 3, 2, 5, 1, 10, 9, 4, 8, 6, 10),
  PES_NU_CPF_CNPJ_RECEBEDOR = c(3, 9, 5, 10, 1, 2, 7, 9, 5, 4, 10, 7, 2, 6, 8, 9)
)
data2 <- data.frame(
  PEG_CD_CPF_CNPJ14 = c(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
  TPE_CD = c(1, 1, 1, 1, 1, 2, 2, 2, 2, 2),
  MUN_CD = c(101, 102, 101, 102, 101, 102, 101, 102, 101, 102)
)

R_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/R/"
CSV_path  <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/CSV/"

csv1 <- "SPITB_LAF_LANCAMENTO_FATO_fake.csv"
csv2 <- "SPIVW_PES_PESSOA_FIS_JUR_fake.csv"

# Write the data to a CSV file
write.csv(data1, file = paste0(CSV_path,csv1), row.names = TRUE)
write.csv(data2, file = paste0(CSV_path,csv2), row.names = TRUE)

con <- dbConnect(RSQLite::SQLite(), dbname = ":memory:")
dbWriteTable(con, "SPITB_LAF_LANCAMENTO_FATO", read.csv(paste0(CSV_path,csv1)))
dbWriteTable(con, "SPIVW_PES_PESSOA_FIS_JUR", read.csv(paste0(CSV_path,csv2)))

# query <- read_file(paste0(R_path, "Pix_inflow_new_test.sql"), 
#                            locale = locale(encoding = "latin1"))

query = "
SELECT	
    PIX.LAF_DT_LIQUIDACAO as DIA, 
    PIX.TPP_CD_TIPO_PESSOA_PAGADOR AS pag_tipo,
    PIX.TPP_CD_TIPO_PESSOA_RECEBEDOR AS rec_tipo,
    1 AS flow_code,
    SUM(LAF_VL) as VALOR, 
    COUNT(LAF_VL) as QTD,
    SUM(
        CASE
            WHEN LAF_VL < 50 THEN 50
            WHEN LAF_VL > 51 THEN 51
            ELSE LAF_VL
        END
    ) AS VALOR_WINSORIZED
    COUNT(DISTINCT PIX.PES_NU_CPF_CNPJ_PAGADOR) AS senders,
    COUNT(DISTINCT PIX.PES_NU_CPF_CNPJ_RECEBEDOR) AS receivers
FROM
    SPITB_LAF_LANCAMENTO_FATO PIX
LEFT JOIN SPIVW_PES_PESSOA_FIS_JUR CLI_PAG ON (PIX.PES_NU_CPF_CNPJ_PAGADOR = CLI_PAG.PEG_CD_CPF_CNPJ14 AND PIX.TPP_CD_TIPO_PESSOA_PAGADOR = CLI_PAG.TPE_CD)
LEFT JOIN SPIVW_PES_PESSOA_FIS_JUR CLI_REC ON (PIX.PES_NU_CPF_CNPJ_RECEBEDOR = CLI_REC.PEG_CD_CPF_CNPJ14 AND PIX.TPP_CD_TIPO_PESSOA_RECEBEDOR = CLI_REC.TPE_CD)
WHERE
    PES_NU_CPF_CNPJ_RECEBEDOR <> PES_NU_CPF_CNPJ_PAGADOR
    AND CLI_PAG.MUN_CD <> CLI_REC.MUN_CD
GROUP BY
    CLI_REC.MUN_CD,
    PIX.LAF_DT_LIQUIDACAO,
    PIX.TPP_CD_TIPO_PESSOA_PAGADOR,
    PIX.TPP_CD_TIPO_PESSOA_RECEBEDOR
"
dbGetQuery("SELECT * FROM SPITB_LAF_LANCAMENTO_FATO LIMIT 1")
dbGetQuery(query)



