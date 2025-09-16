
-- Variables: id, tipo, bank, dia_inicio, dia_fim

WITH ID_list AS (
  SELECT CPF_CD AS id
  FROM DL_DEPEP_ESTABILIDADE_FINANCEIRA.Random_sample_Pix_Fin_Rev
)
SELECT
    ID_list.id AS id,
    1 AS tipo,
    CCS.PAR_CD_CNPJ_PAR AS bank, 
    CCS.REL_DT_INICIO AS dia_inicio, 
    (CASE WHEN CCS.REL_ST_STATUS_RELACIONAMENTO = 'A' THEN NULL
        ELSE CCS.REL_DT_FIM END) AS dia_fim
FROM ID_list 
    LEFT JOIN CCSDWPRO_ACC.CCSTB_FRE_FATO_RELACIONAMENTO as CCS
    ON ID_list.id = TO_NUMBER(CCS.REL_CD_CPF_CNPJ) 
WHERE 
    (dia_fim >= '2019-01-01' OR dia_fim IS NULL) 
    AND dia_inicio < '2023-01-01'
    --AND ((dia_inicio >= '@selectedDateSTART' AND dia_inicio < '@selectedDateEND') OR (dia_fim >= '@selectedDateSTART' AND dia_fim < '@selectedDateEND'));