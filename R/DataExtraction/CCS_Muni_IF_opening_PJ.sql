-- Accounts Opening (CCS) por Municipio x IF 
-- # Variables: week, id_municipio_receita, tipo, bank, opening, stock, closing

  SELECT
    COUNT(DISTINCT REL_CD_CPF_CNPJ) AS opening,
    PAR_CD_CNPJ_PAR AS bank, 
    RFB.MUN_CD AS id_municipio_receita,
    REL_CD_TIPO_PESSOA AS tipo,
    @WEEK AS week
  FROM CCSDWPRO_ACC.CCSTB_FRE_FATO_RELACIONAMENTO AS CCS
  LEFT JOIN BCBDWPRO_ACC.PESTB_PEJ_PESSOA_JURIDICA AS RFB 
    ON TO_NUMBER(CCS.REL_CD_CPF_CNPJ) = TO_NUMBER(RFB.PEJ_CD_CNPJ14)
  WHERE 
    REL_CD_TIPO_PESSOA = 2 AND
    REL_DT_INICIO >= '@selectedDateSTART' AND REL_DT_INICIO < '@selectedDateEND'
  GROUP BY RFB.MUN_CD , PAR_CD_CNPJ_PAR, REL_CD_TIPO_PESSOA
