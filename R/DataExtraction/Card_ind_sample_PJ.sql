-- Variables: week, id, tipo, value_credit, trans_credit, value_debit, trans_debit
WITH ID_list AS (
  SELECT firm_id14 AS id
  FROM DL_DEPEP_ESTABILIDADE_FINANCEIRA.Random_sample_Pix_Fin_Rev_PJ
),
CREDIT AS (
    SELECT 
    	A.CRE_CD_NULIQUID,
        TO_NUMBER(A.CRE_CD_CNPJ_CPF_PONTOVENDA) AS id,
        MAX(A.CRE_DT_DTPGTO) AS dia,        
        MAX(A.CRE_VL_VLR_PGTO) AS valor,
        MAX(CAST(CASE WHEN A.CRE_CD_COD_OCOR =  'N/A' then 999 ELSE A.CRE_CD_COD_OCOR END AS INT)) AS max_ocor
    FROM EACDWPRO_ACC.EAC_cre_CREDITO_TGT AS A
    WHERE 
        A.CRE_DT_DTPGTO >= '@selectedDateSTART' AND A.CRE_DT_DTPGTO < '@selectedDateEND'
        AND A.CRE_CD_TP_PESSOA_PONTOVENDA = 2
        AND A.CRE_CD_SIT_SOLICTCLIQUID IN (5,6,7) 
        AND A.CRE_CD_SITUACAO_RESPINSTDOMCL NOT IN ('002', '006') --filtros CIP (notar 6 e não 3 no respinstdomcl)
        AND EXISTS(SELECT 1 FROM ID_list WHERE ID_list.id = TO_NUMBER(A.CRE_CD_CNPJ_CPF_PONTOVENDA))
    GROUP BY
        TO_NUMBER(A.CRE_CD_CNPJ_CPF_PONTOVENDA),
        A.CRE_CD_NULIQUID
        HAVING max_ocor <=1
),
DEBIT AS (
    SELECT 
        A.DEB_CD_NULIQUID,
    	TO_NUMBER(A.DEB_CD_CNPJ_CPF_PONTOVENDA) AS id,
        MAX(A.DEB_DT_DT_PGTO) AS dia,        
        MAX(A.DEB_VL_VLR_PGTO) AS valor,
        MAX(CAST(CASE WHEN A.DEB_CD_COD_OCOR =  'N/A' then 999 ELSE A.DEB_CD_COD_OCOR END AS INT)) AS max_ocor  
    FROM EACDWPRO_ACC.EAC_DEB_DEBITO_TGT AS A
    WHERE 
        A.DEB_DT_DT_PGTO >= '@selectedDateSTART' AND A.DEB_DT_DT_PGTO < '@selectedDateEND'
        AND A.DEB_CD_TP_PESSOA_PONTOVENDA = 2
        AND A.DEB_CD_SIT_SOLICTCLIQUID IN (5,6,7) 
        AND A.DEB_CD_SITUACAO_RESPINSTDOMCL NOT IN ('002', '006') --filtros CIP (notar 6 e não 3 no respinstdomcl)
        AND EXISTS(SELECT 1 FROM ID_list WHERE ID_list.id = TO_NUMBER(A.DEB_CD_CNPJ_CPF_PONTOVENDA))
        GROUP BY
            A.DEB_CD_NULIQUID, 
            TO_NUMBER(A.DEB_CD_CNPJ_CPF_PONTOVENDA)
       HAVING max_ocor<=1
),
CREDIT_rec AS (
    SELECT
        id,
        SUM(valor) AS value_credit,
        COUNT(*) AS trans_credit
    FROM CREDIT GROUP BY id
),
DEBIT_rec AS (
    SELECT
        id AS id,
        SUM(valor) AS value_debit,
        COUNT(*) AS trans_debit
    FROM DEBIT GROUP BY id
)
SELECT
    @WEEK AS week,
    2 AS tipo,
    ID_list.id AS id,
    COALESCE(CREDIT_rec.value_credit, 0) AS value_credit,
    COALESCE(CREDIT_rec.trans_credit, 0) AS trans_credit,
    COALESCE(DEBIT_rec.value_debit, 0) AS value_debit,
    COALESCE(DEBIT_rec.trans_debit, 0) AS trans_debit
FROM ID_list
LEFT JOIN CREDIT_rec ON (CREDIT_rec.id = ID_list.id)
LEFT JOIN DEBIT_rec ON (DEBIT_rec.id = ID_list.id)
