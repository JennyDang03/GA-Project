-- Variables: week, id, id_municipio_receita, tipo, value_send, trans_send, value_rec, trans_rec, value_self, trans_self

WITH ID_list_PF AS (
  SELECT
    TO_NUMBER(RFB.PEF_CD_CPF) as id,   -- RFB.PEF_CD_CPF is a string. 
    RFB.MUN_CD as id_municipio_receita,
    1 as tipo
  FROM 
    Jose_database.Random_sample as RNDSAMPLE
    INNER JOIN [BCBASE_DP].[bcb].[PES_PESSOA_FISICA] AS RFB
    ON TO_NUMBER(RNDSAMPLE.CPF_CD)= TO_NUMBER(RFB.CODIGO_CPF)
),
ID_list_PJ AS (
  SELECT
    firm_id as id,
    RFB.CODIGO_DO_MUNICIPIO_NO_BCBASE as id_municipio_receita,
    2 as tipo
  FROM 
    Jose_database.Random_sample_PJ AS RNDSAMPLE
    INNER JOIN [BCBASE_DP].[bcb].[PES_PESSOA_JURIDICA] AS RFB
    ON RNDSAMPLE.firm_id = TO_NUMBER(RFB.CODIGO_CNPJ_14)
),
ID_list AS (
  SELECT * FROM ID_list_PF UNION ALL SELECT * FROM ID_list_PJ
),
MAIN AS (
SELECT 
    TO_NUMBER(RTRIM(LTRIM(COALESCE(tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB],tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB_T1], tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB_T2])))) 
    AS id_send,
    TO_NUMBER(RTRIM(LTRIM(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DST, tableCIPDetails.[PAT_CD_CPF_CNPJ_CRD],tableCIPDetails.[PAT_CD_CPF_CNPJ_CRD_T1], tableCIPDetails.[PAT_CD_CPF_CNPJ_CRD_T2])))) 
    AS id_rec,
    tableCIPMain.PAL_DT_MOVTO AS dia, 
    (CASE WHEN COALESCE(tableCIPDetails.[PAT_CD_TP_PES_REM], tableCIPDetails.[PAT_CD_TP_PES_DEB]) = 'F' THEN 1
          WHEN COALESCE(tableCIPDetails.[PAT_CD_TP_PES_REM], tableCIPDetails.[PAT_CD_TP_PES_DEB]) = 'J' THEN 2
          ELSE NULL
    END) AS tipo_send,
    (CASE WHEN COALESCE(tableCIPDetails.[PAT_CD_TP_PES_DST], tableCIPDetails.[PAT_CD_TP_PES_CRD]) = 'F' THEN 1
          WHEN COALESCE(tableCIPDetails.[PAT_CD_TP_PES_DST], tableCIPDetails.[PAT_CD_TP_PES_CRD]) = 'J' THEN 2
          ELSE NULL
    END) AS tipo_rec,
    tableCIPMain.[PAL_VL_LANC] AS valor
FROM [CIP].[dbo].[CMC_PAL_PAG_LAN] AS tableCIPMain
    INNER JOIN 	[CIP].[dbo].[CMC_PAT_PAG_TRANSF] AS tableCIPDetails
    ON tableCIPMain.PAL_CD_NUOP = tableCIPDetails.PAL_CD_NUOP AND tableCIPMain.PAL_CD_MSG = tableCIPDetails.PAT_CD_MSG
WHERE
    tableCIPMain.PAL_DT_MOVTO >= '@selectedDateSTART' AND tableCIPMain.PAL_DT_MOVTO < '@selectedDateEND'
    AND ISNUMERIC(COALESCE(tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB], 
                        tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB_T1], 
                        tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB_T2])) = 1
    AND ISNUMERIC(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DST,
                        tableCIPDetails.[PAT_CD_CPF_CNPJ_CRD], 
                        tableCIPDetails.[PAT_CD_CPF_CNPJ_CRD_T1], 
                        tableCIPDetails.[PAT_CD_CPF_CNPJ_CRD_T2])) = 1
    AND	tableCIPMain.Comp = 1
    AND EXISTS(SELECT 1 FROM ID_list WHERE ID_list.id = TO_NUMBER(RTRIM(LTRIM(COALESCE(tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB],tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB_T1], tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB_T2])))) AND ID_list.tipo = (CASE WHEN COALESCE(tableCIPDetails.[PAT_CD_TP_PES_REM], tableCIPDetails.[PAT_CD_TP_PES_DEB]) = 'F' THEN 1 WHEN COALESCE(tableCIPDetails.[PAT_CD_TP_PES_REM], tableCIPDetails.[PAT_CD_TP_PES_DEB]) = 'J' THEN 2 ELSE NULL END))
    AND EXISTS(SELECT 1 FROM ID_list WHERE ID_list.id = TO_NUMBER(RTRIM(LTRIM(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DST, tableCIPDetails.[PAT_CD_CPF_CNPJ_CRD],tableCIPDetails.[PAT_CD_CPF_CNPJ_CRD_T1], tableCIPDetails.[PAT_CD_CPF_CNPJ_CRD_T2]))) ) AND ID_list.tipo = (CASE WHEN COALESCE(tableCIPDetails.[PAT_CD_TP_PES_DST], tableCIPDetails.[PAT_CD_TP_PES_CRD]) = 'F' THEN 1 WHEN COALESCE(tableCIPDetails.[PAT_CD_TP_PES_DST], tableCIPDetails.[PAT_CD_TP_PES_CRD]) = 'J' THEN 2 ELSE NULL END))
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


