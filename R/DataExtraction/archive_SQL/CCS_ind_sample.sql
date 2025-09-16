
WITH ID_list_PF AS (
  SELECT
    TO_NUMBER(RFB.PEF_CD_CPF) as id,   -- RFB.PEF_CD_CPF is a string. 
    RFB.MUN_CD as id_municipio_receita,
    1 as tipo
  FROM 
    DL_DEPEP_ESTABILIDADE_FINANCEIRA.Random_sample_Pix_Fin_Rev as RNDSAMPLE
    INNER JOIN BCBDWPRO_ACC.PESTB_PEF_PESSOA_FISICA AS RFB 
    ON RNDSAMPLE.CPF_CD = RFB.PEF_CD_CPF
),
ID_list_PJ AS (
  SELECT
    firm_id as id,
    RFB.MUN_CD as id_municipio_receita,
    2 as tipo
  FROM 
    DL_DEPEP_ESTABILIDADE_FINANCEIRA.Random_sample_Pix_Fin_Rev_PJ AS RNDSAMPLE
    INNER JOIN BCBDWPRO_ACC.PESTB_PEJ_PESSOA_JURIDICA AS RFB
    ON RNDSAMPLE.firm_id = TO_NUMBER(RFB.PEJ_CD_CNPJ14)
),
ID_list AS (
  SELECT * FROM ID_list_PF UNION ALL SELECT * FROM ID_list_PJ
)
SELECT
    ID_list.id AS id,
    ID_list.tipo AS tipo,
    ID_list.id_municipio_receita AS id_municipio_receita,
    CCS.PAR_CD_CNPJ_PAR AS bank, 
    CCS.REL_DT_INICIO AS dia_inicio, 
    (CASE WHEN CCS.REL_ST_STATUS_RELACIONAMENTO = 'A' THEN NULL
        ELSE CCS.REL_DT_FIM END) AS dia_fim
FROM ID_list 
    LEFT JOIN CCSDWPRO_ACC.CCSTB_FRE_FATO_RELACIONAMENTO as CCS
    ON ID_list.id = TO_NUMBER(CCS.REL_CD_CPF_CNPJ) AND ID_list.tipo = CCS.REL_CD_TIPO_PESSOA
WHERE 
    (dia_fim >= '2018-01-01' OR dia_fim IS NULL) 
    AND dia_inicio < '2024-01-01';