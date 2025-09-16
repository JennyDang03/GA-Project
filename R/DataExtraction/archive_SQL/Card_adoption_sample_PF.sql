
WITH ID_list AS (
  SELECT
    TO_NUMBER(RFB.PEF_CD_CPF) as id,   -- RFB.PEF_CD_CPF is a string. 
    RFB.MUN_CD as id_municipio_receita,
    1 as tipo
  FROM 
    DL_DEPEP_ESTABILIDADE_FINANCEIRA.Random_sample_Pix_Fin_Rev as RNDSAMPLE
    INNER JOIN BCBDWPRO_ACC.PESTB_PEF_PESSOA_FISICA AS RFB 
    ON RNDSAMPLE.CPF_CD = RFB.PEF_CD_CPF
),
CREDIT AS (
    SELECT 
    	TO_NUMBER(A.CRE_CD_CNPJ_CPF_PONTOVENDA) AS id,
        A.CRE_DT_DTPGTO AS dia,        
        A.CRE_CD_TP_PESSOA_PONTOVENDA AS tipo
    FROM EACDWPRO_ACC.EAC_cre_CREDITO_TGT AS A
    WHERE 
        A.CRE_DT_DTPGTO < '2024-01-01'
        AND A.CRE_CD_SIT_SOLICTCLIQUID IN (5,6,7) 
        AND A.CRE_CD_SITUACAO_RESPINSTDOMCL NOT IN ('002', '006') --filtros CIP (notar 6 e não 3 no respinstdomcl)
        AND EXISTS(SELECT 1 FROM ID_list WHERE ID_list.id = TO_NUMBER(A.CRE_CD_CNPJ_CPF_PONTOVENDA) AND ID_list.tipo = A.CRE_CD_TP_PESSOA_PONTOVENDA)
),
DEBIT AS (
    SELECT 
    	TO_NUMBER(A.DEB_CD_CNPJ_CPF_PONTOVENDA) AS id,
        A.DEB_DT_DT_PGTO AS dia,        
        A.DEB_CD_TP_PESSOA_PONTOVENDA AS tipo
    FROM EACDWPRO_ACC.EAC_DEB_DEBITO_TGT AS A
    WHERE 
        A.DEB_DT_DT_PGTO < '2024-01-01'
        AND A.DEB_CD_SIT_SOLICTCLIQUID IN (5,6,7) 
        AND A.DEB_CD_SITUACAO_RESPINSTDOMCL NOT IN ('002', '006') --filtros CIP (notar 6 e não 3 no respinstdomcl)
        AND EXISTS(SELECT 1 FROM ID_list WHERE ID_list.id = TO_NUMBER(A.DEB_CD_CNPJ_CPF_PONTOVENDA) AND ID_list.tipo = A.DEB_CD_TP_PESSOA_PONTOVENDA)
),
--FirstCREDIT_rec AS (
--    SELECT ID_list.id as id, ID_list.id_municipio_receita as id_municipio_receita, ID_list.tipo as tipo, MIN(CREDIT.dia) as dia
--    FROM ID_list LEFT JOIN CREDIT ON (CREDIT.id = ID_list.id AND CREDIT.tipo = ID_list.tipo)
--    GROUP BY ID_list.id, ID_list.id_municipio_receita, ID_list.tipo
--),
FirstCREDIT_rec AS (
    SELECT
        ID_list.id AS id,
        ID_list.id_municipio_receita AS id_municipio_receita,
        ID_list.tipo AS tipo,
        B.dia AS dia
    FROM ID_list
        LEFT JOIN (SELECT id, tipo, MIN(dia) AS dia FROM CREDIT GROUP BY id, tipo) AS B ON (B.id = ID_list.id AND B.tipo = ID_list.tipo)
),
FirstDEBIT_rec AS (
    SELECT
        ID_list.id AS id,
        ID_list.id_municipio_receita AS id_municipio_receita,
        ID_list.tipo AS tipo,
        B.dia AS dia
    FROM ID_list
        LEFT JOIN (SELECT id, tipo, MIN(dia) AS dia FROM DEBIT GROUP BY id, tipo) AS B ON (B.id = ID_list.id AND B.tipo = ID_list.tipo)
),
FirstCARD_rec AS (
    SELECT
        id as id,
        id_municipio_receita as id_municipio_receita,
        tipo as tipo,
        MIN(dia) as dia
    FROM (SELECT * FROM FirstCREDIT_rec UNION ALL SELECT * FROM FirstDEBIT_rec) AS CARD_REC
    GROUP BY
        id,
        id_municipio_receita,
        tipo
),
CARD_rec_count AS (
    SELECT 
        dia,
        id_municipio_receita,
        tipo,
        COUNT(id) AS card_adopters
    FROM FirstCARD_rec
    GROUP BY 
        dia, id_municipio_receita, tipo 
),
CREDIT_rec_count AS (
    SELECT 
        dia,
        id_municipio_receita,
        tipo,
        COUNT(id) AS credit_adopters
    FROM FirstCREDIT_rec
    GROUP BY 
        dia, id_municipio_receita, tipo 
),
DEBIT_rec_count AS (
    SELECT 
        dia,
        id_municipio_receita,
        tipo,
        COUNT(id) AS debit_adopters
    FROM FirstDEBIT_rec
    GROUP BY 
        dia, id_municipio_receita, tipo 
)
SELECT 
    CARD_rec_count.dia AS dia,
    CARD_rec_count.id_municipio_receita AS id_municipio_receita,
    CARD_rec_count.tipo AS tipo,
    CARD_rec_count.card_adopters AS card_adopters,
    COALESCE(CREDIT_rec_count.credit_adopters, 0) AS credit_adopters,
    COALESCE(DEBIT_rec_count.debit_adopters, 0) AS debit_adopters
FROM CARD_rec_count
    LEFT JOIN CREDIT_rec_count ON CARD_rec_count.dia = CREDIT_rec_count.dia AND CARD_rec_count.id_municipio_receita = CREDIT_rec_count.id_municipio_receita AND CARD_rec_count.tipo = CREDIT_rec_count.tipo 
    LEFT JOIN DEBIT_rec_count ON CARD_rec_count.dia = DEBIT_rec_count.dia AND CARD_rec_count.id_municipio_receita = DEBIT_rec_count.id_municipio_receita AND CARD_rec_count.tipo = DEBIT_rec_count.tipo;


