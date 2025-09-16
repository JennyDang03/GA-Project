-- First Account por Municipio x IF 

-- # Variables: day, muni_cd, tipo, first_account
WITH FirstAccount(
  SELECT
    REL_CD_CPF_CNPJ AS id,
    RFB.MUN_CD AS muni_cd,
    REL_CD_TIPO_PESSOA AS tipo,
    MIN(REL_DT_INICIO) AS day
  FROM CCSDWPRO_ACC.CCSTB_FRE_FATO_RELACIONAMENTO AS CCS
  LEFT JOIN PIXDWPRO_ACC.SPIVW_PES_PESSOA_FIS_JUR AS RFB ---------> NEED TO CHANGE THIS!!!
    ON CCS.REL_CD_CPF_CNPJ = TO_NUMBER(RFB.PEG_CD_CPF_CNPJ14)
  GROUP BY RFB.MUN_CD, REL_CD_CPF_CNPJ, REL_CD_TIPO_PESSOA
)

SELECT 
  day, muni_cd, tipo, COUNT(id) AS first_account
FROM FirstAccount
GROUP BY day, muni_cd, tipo
