-- First Account por Municipio x IF ---> PJ

-- # Variables: day, id_municipio_receita, tipo, first_account
WITH FirstAccount AS (
  SELECT
    REL_CD_CPF_CNPJ AS id,
    RFB.MUN_CD AS id_municipio_receita,
    REL_CD_TIPO_PESSOA AS tipo,
    MIN(REL_DT_INICIO) AS dia
  FROM CCSDWPRO_ACC.CCSTB_FRE_FATO_RELACIONAMENTO AS CCS
  LEFT JOIN BCBDWPRO_ACC.PESTB_PEJ_PESSOA_JURIDICA AS RFB 
    ON TO_NUMBER(CCS.REL_CD_CPF_CNPJ) = TO_NUMBER(RFB.PEJ_CD_CNPJ14)
  WHERE 
    REL_CD_TIPO_PESSOA = 2
  GROUP BY RFB.MUN_CD, REL_CD_CPF_CNPJ, REL_CD_TIPO_PESSOA
)

SELECT 
  dia, id_municipio_receita, tipo, COUNT(id) AS first_account
FROM FirstAccount
GROUP BY dia, id_municipio_receita, tipo
