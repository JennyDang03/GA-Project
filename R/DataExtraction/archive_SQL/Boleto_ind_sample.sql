-- Variables: week, id, id_municipio_receita, tipo, value_send, trans_send, value_rec, trans_rec, value_self, trans_self

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
),
MAIN AS (
    SELECT 
    	TO_NUMBER(A.PEB_CD_CNPJ_CPF_PAGDR) AS id_send,
    	TO_NUMBER(COALESCE(A.PEB_CD_CNPJ_CPF_BENFCRIO_FINL, A.PEB_CD_CNPJ_CPF_BENFCRIO_OR)) AS id_rec,
        A.DTE_DT_BASE_MVTO AS dia,        
        (CASE
            WHEN A.PEB_CD_TP_PESSOA_PAGDR = 'F' THEN 1
            WHEN A.PEB_CD_TP_PESSOA_PAGDR = 'J' THEN 2
            ELSE NULL
        END) AS tipo_send,
        (CASE 
            WHEN COALESCE(A.PEB_CD_TP_PESSOA_BENFCRIO_FINL, A.PEB_CD_TP_PESSOA_BENFCRIO_OR) ='F' THEN 1
            WHEN COALESCE(A.PEB_CD_TP_PESSOA_BENFCRIO_FINL, A.PEB_CD_TP_PESSOA_BENFCRIO_OR) ='J' THEN 2
            ELSE NULL
        END) AS tipo_rec,
        A.BBC_VL_TIT AS vl_nom,
        A.BBC_VL_BAIXA_EFT_TIT AS vl_baixa_orig,
        CASE WHEN A.BBC_VL_TIT > 0 THEN
		      CASE WHEN A.BBC_VL_BAIXA_EFT_TIT / A.BBC_VL_TIT > 10 THEN A.BBC_VL_TIT
		      ELSE A.BBC_VL_BAIXA_EFT_TIT END
		    ELSE A.BBC_VL_BAIXA_EFT_TIT END AS valor
    FROM CIPDWPRO_ACC.CIPTB_BBC_BOLETO_BAIXA_DIARIA_CIP AS A
    WHERE 
        A.DTE_DT_BASE_MVTO >= '@selectedDateSTART' AND A.DTE_DT_BASE_MVTO < '@selectedDateEND'
        AND EXISTS(SELECT 1 FROM ID_list WHERE ID_list.id = TO_NUMBER(A.PEB_CD_CNPJ_CPF_PAGDR) AND ID_list.tipo = (CASE WHEN A.PEB_CD_TP_PESSOA_PAGDR = 'F' THEN 1 WHEN A.PEB_CD_TP_PESSOA_PAGDR = 'J' THEN 2 ELSE NULL END))
        AND EXISTS(SELECT 1 FROM ID_list WHERE ID_list.id = TO_NUMBER(COALESCE(A.PEB_CD_CNPJ_CPF_BENFCRIO_FINL, A.PEB_CD_CNPJ_CPF_BENFCRIO_OR)) AND ID_list.tipo = (CASE WHEN COALESCE(A.PEB_CD_TP_PESSOA_BENFCRIO_FINL, A.PEB_CD_TP_PESSOA_BENFCRIO_OR) ='F' THEN 1 WHEN COALESCE(A.PEB_CD_TP_PESSOA_BENFCRIO_FINL, A.PEB_CD_TP_PESSOA_BENFCRIO_OR) ='J' THEN 2 ELSE NULL END))
),
Send_main AS (
  SELECT
      ID_list.id AS id,
      ID_list.id_municipio_receita AS id_municipio_receita,
      ID_list.tipo AS tipo,
      COALESCE(B.valor, 0) AS value_send,
      COALESCE(B.trans, 0) AS trans_send
  FROM ID_list
    LEFT JOIN (SELECT id_send, tipo_send, SUM(valor) AS valor, COUNT(valor) AS trans FROM MAIN WHERE id_send <> id_rec  GROUP BY id_send, tipo_send) AS B
    ON (B.id_send = ID_list.id AND B.tipo_send = ID_list.tipo)
),
Rec_main AS (
  SELECT
      ID_list.id AS id,
      ID_list.id_municipio_receita AS id_municipio_receita,
      ID_list.tipo AS tipo,
      COALESCE(B.valor, 0) AS value_rec,
      COALESCE(B.trans, 0) AS trans_rec
  FROM
    ID_list
    LEFT JOIN (SELECT id_rec, tipo_rec, SUM(valor) AS valor, COUNT(valor) AS trans FROM MAIN WHERE id_send <> id_rec  GROUP BY id_rec, tipo_rec) AS B
    ON (B.id_rec = ID_list.id AND B.tipo_rec = ID_list.tipo)
),
Self_main AS (
  SELECT
      ID_list.id AS id,
      ID_list.id_municipio_receita AS id_municipio_receita,
      ID_list.tipo AS tipo,
      COALESCE(B.valor, 0) AS value_self,
      COALESCE(B.trans, 0) AS trans_self
    FROM ID_list
    LEFT JOIN (SELECT id_send, tipo_send, SUM(valor) AS valor, COUNT(valor) AS trans FROM MAIN WHERE id_send = id_rec GROUP BY id_send, tipo_send) AS B
    ON (B.id_send = ID_list.id AND B.tipo_send = ID_list.tipo)
)
SELECT
    @WEEK as week,
    Send_main.id AS id,
    Send_main.id_municipio_receita AS id_municipio_receita,
    Send_main.tipo AS tipo,
    Send_main.value_send AS value_send,
    Send_main.trans_send AS trans_send,
    Rec_main.value_rec AS value_rec,
    Rec_main.trans_rec AS trans_rec,
    Self_main.value_self AS value_self,
    Self_main.trans_self AS trans_self
FROM Send_main
    LEFT JOIN Rec_main
    ON (Send_main.id = Rec_main.id AND Send_main.tipo = Rec_main.tipo)
    LEFT JOIN Self_main
    ON (Send_main.id = Self_main.id AND Send_main.tipo = Self_main.tipo)

-- Variables: week, id, id_municipio_receita, tipo, value_send, trans_send, value_rec, trans_rec, value_self, trans_self
