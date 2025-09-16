--#Codigo para sample das pessoas
--# standardQuery <- "SELECT CPF_CD FROM GGGDWPRO_ACC.GGGTB_CPF_RECEITA WHERE CPF_CD_SITUACAO = 0 AND  CPF_DT_NASC < '2015-01-01' SAMPLE 2000000"
--# I try to improve it by setting a seed. 
--# standardQuery <- "SELECT CPF_CD FROM GGGDWPRO_ACC.GGGTB_CPF_RECEITA WHERE CPF_CD_SITUACAO = 0 AND  CPF_DT_NASC < '2015-01-01' ORDER BY RAND(12345) LIMIT 2000000"


-- standardQuery <- "SELECT CPF_CD 
-- FROM GGGDWPRO_ACC.GGGTB_CPF_RECEITA 
-- WHERE CPF_CD_SITUACAO = 0 AND  CPF_DT_NASC < '2015-01-01' 
-- SAMPLE 2000000"

-- "SELECT LEFT(NUM_CNPJ,8) as CNPJ FROM GGGDWPRO_ACC.GGGTB_CNPJ_RECEITA WHERE SITUACAO_CNPJ = 1 AND  DATA_ABERTURA_CNPJ < '2019-01-01' SAMPLE 1000000"
--SELECT 
--    NUM_CNPJ AS CNPJ14,
--    LEFT(NUM_CNPJ,8) AS CNPJ8
--FROM GGGDWPRO_ACC.GGGTB_CNPJ_RECEITA 
--WHERE SITUACAO_CNPJ = 1 
--    AND  DATA_ABERTURA_CNPJ < '2019-01-01' 
--SAMPLE 1000000





WITH ID_list_PJ AS (
  SELECT
    TO_NUMBER(PEJ_CD_CNPJ14) as id,
    MUN_CD as id_municipio_receita,
    2 as tipo,
    PEJ_DT_ABERTURA as open_date,
    (CASE WHEN EMPRESA.SPJ_CD_SITUACAO_PJ_RFB IN (8,3,4)
          THEN EMPRESA.PEJ_DT_ALTERACAO_SITUACAO ELSE NULL
          END) AS close_date
  FROM
    BCBDWPRO_ACC.PESTB_PEJ_PESSOA_JURIDICA
    WHERE
        open_date < '2023-01-01' AND
        (close_date IS NULL OR close_date > '2019-01-01')
-- 8 = baixada, 3 = suspensa, 4 = inapta
)
  SELECT *
  FROM ID_list_PJ
  TABLESAMPLE SYSTEM (2000000) REPEATABLE (42) -- Change the seed value (42) for reproducibility








WITH ID_list AS (
  SELECT
    RFB.PEF_CD_CPF as id,   -- RFB.PEF_CD_CPF is a string. 
    RFB.MUN_CD as id_municipio,
    RFB.SEX_ID as sex_id
  FROM 
    BCBDWPRO_ACC.PESTB_PEF_PESSOA_FISICA AS RFB 
)
SELECT
  id,
  id_municipio,
  sex_id
FROM
  (
    SELECT
      id,
      id_municipio,
      sex_id,
      ROW_NUMBER() OVER (PARTITION BY id_municipio, sex_id ORDER BY RAND(1 * CAST(id_municipio AS BIGINT) * CAST(sex_id AS BIGINT))) as row_num
      --,COUNT(*) OVER (PARTITION BY id_municipio, sex_id) as count_per_group
    FROM
      ID_list
  ) AS numbered
WHERE
  MOD(row_num, 200) = 1
ORDER BY
  id_municipio, sex_id, id;
