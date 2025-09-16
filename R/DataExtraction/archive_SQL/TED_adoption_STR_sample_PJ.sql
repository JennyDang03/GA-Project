
-- First I need to create the random sample in the Database that they are. 


WITH ID_list AS (
  SELECT
    firm_id as id,
    RFB.CODIGO_DO_MUNICIPIO_NO_BCBASE as id_municipio_receita,
    2 as tipo
  FROM 
    Jose_database.Random_sample_PJ AS RNDSAMPLE
    INNER JOIN [BCBASE_DP].[bcb].[PES_PESSOA_JURIDICA] AS RFB
    ON RNDSAMPLE.firm_id = TO_NUMBER(RFB.CODIGO_CNPJ_14)
),
TED AS (
    SELECT 
    	TO_NUMBER(tableSTRDetails.[LIF_CD_CNPJCPFDEB1]) AS id_send,
    	TO_NUMBER(tableSTRDetails.[LIF_CD_CNPJCPFCRE1]) AS id_rec,
        HIL_DT_MOVTO AS dia,        
        (CASE
            WHEN tableSTRMain.[LIF_CD_TP_PES_DEB] = 'F' THEN 1
            WHEN tableSTRMain.[LIF_CD_TP_PES_DEB] = 'J' THEN 2
            ELSE NULL
        END) AS tipo_send,
        (CASE 
            WHEN tableSTRMain.[LIF_CD_TP_PES_CRED] ='F' THEN 1
            WHEN tableSTRMain.[LIF_CD_TP_PES_CRED] ='J' THEN 2
            ELSE NULL
        END) AS tipo_rec
    FROM [STR].[dbo].[STR_HIL_HIST_LAN] AS tableSTRMain
    INNER JOIN 
    [STR].[dbo].[STR_LIF_LAN_IF] AS tableSTRDetails
    ON	tableSTRMain.HIL_CD_NU_OPER = tableSTRDetails.LIF_CD_NUM_OPER
    WHERE 
        HIL_DT_MOVTO < '2024-01-01'
        AND  tableSTRDetails.[LIF_CD_CNPJCPFDEB1] <> tableSTRDetails.[LIF_CD_CNPJCPFCRE1]
        AND EXISTS(SELECT 1 FROM ID_list WHERE ID_list.id = TO_NUMBER(tableSTRDetails.[LIF_CD_CNPJCPFDEB1]) AND ID_list.tipo = (CASE WHEN tableSTRMain.[LIF_CD_TP_PES_DEB] = 'F' THEN 1 WHEN tableSTRMain.[LIF_CD_TP_PES_DEB] = 'J' THEN 2 ELSE NULL END))
        AND EXISTS(SELECT 1 FROM ID_list WHERE ID_list.id = TO_NUMBER(tableSTRDetails.[LIF_CD_CNPJCPFCRE1]) AND ID_list.tipo = (CASE WHEN tableSTRMain.[LIF_CD_TP_PES_CRED] = 'F' THEN 1 WHEN tableSTRMain.[LIF_CD_TP_PES_CRED] = 'J' THEN 2 ELSE NULL END))
),
TED_self AS (
    SELECT 
    	TO_NUMBER(tableSTRDetails.[LIF_CD_CNPJCPFDEB1]) AS id,
        HIL_DT_MOVTO AS dia,        
        (CASE
            WHEN tableSTRMain.[LIF_CD_TP_PES_DEB] = 'F' THEN 1
            WHEN tableSTRMain.[LIF_CD_TP_PES_DEB] = 'J' THEN 2
            ELSE NULL
        END) AS tipo
    FROM [STR].[dbo].[STR_HIL_HIST_LAN] AS tableSTRMain
    INNER JOIN 
    [STR].[dbo].[STR_LIF_LAN_IF] AS tableSTRDetails
    ON	tableSTRMain.HIL_CD_NU_OPER = tableSTRDetails.LIF_CD_NUM_OPER
    WHERE 
        HIL_DT_MOVTO < '2024-01-01'
        AND  tableSTRDetails.[LIF_CD_CNPJCPFDEB1] = tableSTRDetails.[LIF_CD_CNPJCPFCRE1]
        AND EXISTS(SELECT 1 FROM ID_list WHERE ID_list.id = TO_NUMBER(tableSTRDetails.[LIF_CD_CNPJCPFDEB1]) AND ID_list.tipo = (CASE WHEN tableSTRMain.[LIF_CD_TP_PES_DEB] = 'F' THEN 1 WHEN tableSTRMain.[LIF_CD_TP_PES_DEB] = 'J' THEN 2 ELSE NULL END))
),
FirstSend AS (
  SELECT
      ID_list.id as id,
      ID_list.id_municipio_receita as id_municipio_receita,
      ID_list.tipo as tipo,
      B.dia AS dia
  FROM  
    ID_list
    LEFT JOIN (SELECT id_send, tipo_send, MIN(dia) AS dia FROM TED GROUP BY id_send, tipo_send) AS B
    ON (B.id_send = ID_list.id AND B.tipo_send = ID_list.tipo)
),
FirstReceive AS (
  SELECT
      ID_list.id as id,
      ID_list.id_municipio_receita as id_municipio_receita,
      ID_list.tipo as tipo,
      B.dia AS dia
  FROM  
    ID_list
    LEFT JOIN (SELECT id_rec, tipo_rec, MIN(dia) AS dia FROM TED GROUP BY id_rec, tipo_rec) AS B
    ON (B.id_rec = ID_list.id AND B.tipo_rec = ID_list.tipo)
),
FirstSelf AS (
  SELECT
      ID_list.id as id,
      ID_list.id_municipio_receita as id_municipio_receita,
      ID_list.tipo as tipo,
      B.dia AS dia
  FROM  
    ID_list
    LEFT JOIN (SELECT id, tipo, MIN(dia) AS dia FROM TED_self GROUP BY id, tipo) AS B
    ON (B.id = ID_list.id AND B.tipo = ID_list.tipo)
),
FirstTED AS (
SELECT
    id,
    id_municipio_receita,
    tipo,
    MIN(dia) AS dia
FROM (SELECT * FROM FirstSend UNION ALL
    SELECT * FROM FirstReceive UNION ALL
    SELECT * FROM FirstSelf) AS Combined
GROUP BY 
  id, id_municipio_receita, tipo
),
TED_count AS (
SELECT 
    dia,
    id_municipio_receita,
    tipo,
    COUNT(id) AS adopters
FROM FirstTED
GROUP BY 
    dia, id_municipio_receita, tipo 
), 
SelfTED_count AS (
SELECT 
    dia,
    id_municipio_receita,
    tipo,
    COUNT(id) AS self_adopters
FROM FirstSelf
GROUP BY 
    dia, id_municipio_receita, tipo 
), 
SendTED_count AS (
SELECT 
    dia,
    id_municipio_receita,
    tipo,
    COUNT(id) AS send_adopters
FROM FirstSend
GROUP BY 
    dia, id_municipio_receita, tipo 
), 
ReceiveTED_count AS (
SELECT 
    dia,
    id_municipio_receita,
    tipo,
    COUNT(id) AS rec_adopters
FROM FirstReceive
GROUP BY 
    dia, id_municipio_receita, tipo 
)   
SELECT 
    TED_count.dia AS dia,
    TED_count.id_municipio_receita AS id_municipio_receita,
    TED_count.tipo AS tipo,
    TED_count.adopters AS adopters,
    COALESCE(SelfTED_count.self_adopters, 0) AS self_adopters,
    COALESCE(SendTED_count.send_adopters, 0) AS send_adopters,
    COALESCE(ReceiveTED_count.rec_adopters, 0) AS rec_adopters
FROM TED_count 
    LEFT JOIN SelfTED_count ON TED_count.dia = SelfTED_count.dia AND TED_count.id_municipio_receita = SelfTED_count.id_municipio_receita AND TED_count.tipo = SelfTED_count.tipo 
    LEFT JOIN SendTED_count ON TED_count.dia = SendTED_count.dia AND TED_count.id_municipio_receita = SendTED_count.id_municipio_receita AND TED_count.tipo = SendTED_count.tipo 
    LEFT JOIN ReceiveTED_count ON TED_count.dia = ReceiveTED_count.dia AND TED_count.id_municipio_receita = ReceiveTED_count.id_municipio_receita AND TED_count.tipo = ReceiveTED_count.tipo;
    