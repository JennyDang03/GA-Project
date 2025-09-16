-- First Account por Municipio x IF 

-- # Variables: day, id_municipio_receita, tipo, first_account
WITH FirstAccount AS (
  SELECT
    REL_CD_CPF_CNPJ AS id,
    RFB.MUN_CD AS id_municipio_receita,
    REL_CD_TIPO_PESSOA AS tipo,
    MIN(REL_DT_INICIO) AS dia
  FROM CCSDWPRO_ACC.CCSTB_FRE_FATO_RELACIONAMENTO AS CCS
  LEFT JOIN BCBDWPRO_ACC.PESTB_PEF_PESSOA_FISICA AS RFB 
    ON TO_NUMBER(CCS.REL_CD_CPF_CNPJ) = TO_NUMBER(RFB.PEF_CD_CPF)
  WHERE 
    REL_CD_TIPO_PESSOA = 1
  GROUP BY RFB.MUN_CD, REL_CD_CPF_CNPJ, REL_CD_TIPO_PESSOA
)
SELECT 
  dia, id_municipio_receita, tipo, COUNT(id) AS first_account
FROM FirstAccount
GROUP BY dia, id_municipio_receita, tipo
