-- Variables: week, id, id_municipio_receita, tipo, value_credit, trans_credit, value_debit, trans_debit
WITH ID_list_PF AS (
  SELECT
    TO_NUMBER(RFB.PEF_CD_CPF) AS id,   -- RFB.PEF_CD_CPF is a string. 
    RFB.MUN_CD AS id_municipio_receita,
    1 AS tipo
  FROM 
    DL_DEPEP_ESTABILIDADE_FINANCEIRA.Random_sample_Pix_Fin_Rev as RNDSAMPLE
    INNER JOIN BCBDWPRO_ACC.PESTB_PEF_PESSOA_FISICA AS RFB 
    ON RNDSAMPLE.CPF_CD = RFB.PEF_CD_CPF
),
ID_list_PJ AS ( 
  SELECT
    firm_id AS id,
    RFB.MUN_CD AS id_municipio_receita,
    2 AS tipo
  FROM 
    DL_DEPEP_ESTABILIDADE_FINANCEIRA.Random_sample_Pix_Fin_Rev_PJ AS RNDSAMPLE
    INNER JOIN BCBDWPRO_ACC.PESTB_PEJ_PESSOA_JURIDICA AS RFB
    ON RNDSAMPLE.firm_id = TO_NUMBER(RFB.PEJ_CD_CNPJ14)
),
ID_list AS (
  SELECT * FROM ID_list_PF UNION ALL SELECT * FROM ID_list_PJ
),
CREDIT AS (
    SELECT 
    	A.CRE_CD_NULIQUID,
        TO_NUMBER(A.CRE_CD_CNPJ_CPF_PONTOVENDA) AS id,
        A.CRE_CD_TP_PESSOA_PONTOVENDA AS tipo,
        MAX(A.CRE_DT_DTPGTO) AS dia,        
        MAX(A.CRE_VL_VLR_PGTO) AS valor,
        MAX(CAST(CASE WHEN A.CRE_CD_COD_OCOR =  'N/A' then 999 ELSE A.CRE_CD_COD_OCOR END AS INT)) AS max_ocor
    FROM EACDWPRO_ACC.EAC_cre_CREDITO_TGT AS A
    WHERE 
        A.CRE_DT_DTPGTO >= '@selectedDateSTART' AND A.CRE_DT_DTPGTO < '@selectedDateEND'
        AND A.CRE_CD_SIT_SOLICTCLIQUID IN (5,6,7) 
        AND A.CRE_CD_SITUACAO_RESPINSTDOMCL NOT IN ('002', '006') --filtros CIP (notar 6 e não 3 no respinstdomcl)
        AND EXISTS(SELECT 1 FROM ID_list WHERE ID_list.id = TO_NUMBER(A.CRE_CD_CNPJ_CPF_PONTOVENDA) AND ID_list.tipo = A.CRE_CD_TP_PESSOA_PONTOVENDA)
    GROUP BY
        TO_NUMBER(A.CRE_CD_CNPJ_CPF_PONTOVENDA),
        A.CRE_CD_TP_PESSOA_PONTOVENDA,
        A.CRE_CD_NULIQUID
        HAVING max_ocor <=1
),
DEBIT AS (
    SELECT 
        A.DEB_CD_NULIQUID,
    	TO_NUMBER(A.DEB_CD_CNPJ_CPF_PONTOVENDA) AS id,
        A.DEB_CD_TP_PESSOA_PONTOVENDA AS tipo,
        MAX(A.DEB_DT_DT_PGTO) AS dia,        
        MAX(A.DEB_VL_VLR_PGTO) AS valor,
        MAX(CAST(CASE WHEN A.DEB_CD_COD_OCOR =  'N/A' then 999 ELSE A.DEB_CD_COD_OCOR END AS INT)) AS max_ocor  
    FROM EACDWPRO_ACC.EAC_DEB_DEBITO_TGT AS A
    WHERE 
        A.DEB_DT_DT_PGTO >= '@selectedDateSTART' AND A.DEB_DT_DT_PGTO < '@selectedDateEND'
        AND A.DEB_CD_SIT_SOLICTCLIQUID IN (5,6,7) 
        AND A.DEB_CD_SITUACAO_RESPINSTDOMCL NOT IN ('002', '006') --filtros CIP (notar 6 e não 3 no respinstdomcl)
        AND EXISTS(SELECT 1 FROM ID_list WHERE ID_list.id = TO_NUMBER(A.DEB_CD_CNPJ_CPF_PONTOVENDA) AND ID_list.tipo = A.DEB_CD_TP_PESSOA_PONTOVENDA)
        GROUP BY
            A.DEB_CD_NULIQUID, 
            A.DEB_CD_TP_PESSOA_PONTOVENDA,
            TO_NUMBER(A.DEB_CD_CNPJ_CPF_PONTOVENDA)
       HAVING max_ocor<=1
),
CREDIT_rec AS (
    SELECT
        ID_list.id AS id,
        ID_list.id_municipio_receita AS id_municipio_receita,
        ID_list.tipo AS tipo,
        COALESCE(B.valor, 0) AS value_credit,
        COALESCE(B.trans, 0) AS trans_credit
    FROM 
        ID_list
        LEFT JOIN (SELECT id, tipo, SUM(valor) AS valor, COUNT(valor) AS trans FROM CREDIT GROUP BY id, tipo) AS B 
        ON (B.id = ID_list.id AND B.tipo = ID_list.tipo)
),
DEBIT_rec AS (
    SELECT
        ID_list.id AS id,
        ID_list.id_municipio_receita AS id_municipio_receita,
        ID_list.tipo AS tipo,
        COALESCE(B.valor, 0) AS value_debit,
        COALESCE(B.trans, 0) AS trans_debit
    FROM 
        ID_list
        LEFT JOIN (SELECT id, tipo, SUM(valor) AS valor, COUNT(valor) AS trans FROM DEBIT GROUP BY id, tipo) AS B 
        ON (B.id = ID_list.id AND B.tipo = ID_list.tipo)
)
SELECT
    @WEEK as week,
    CREDIT_rec.id AS id,
    CREDIT_rec.id_municipio_receita AS id_municipio_receita,
    CREDIT_rec.tipo AS tipo,
    CREDIT_rec.value_credit AS value_credit,
    CREDIT_rec.trans_credit AS trans_credit,
    DEBIT_rec.value_debit AS value_debit,
    DEBIT_rec.trans_debit AS trans_debit
FROM CREDIT_rec
    LEFT JOIN DEBIT_rec
    ON (CREDIT_rec.id = DEBIT_rec.id AND CREDIT_rec.tipo = DEBIT_rec.tipo);

