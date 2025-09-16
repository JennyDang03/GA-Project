
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
MAIN AS (
    SELECT 
    	TO_NUMBER(A.PES_NU_CPF_CNPJ_PAGADOR) AS id_send,
    	TO_NUMBER(A.PES_NU_CPF_CNPJ_RECEBEDOR) AS id_rec,
        A.LAF_DT_LIQUIDACAO AS dia,        
        A.TPP_CD_TIPO_PESSOA_PAGADOR AS tipo_send,
        A.TPP_CD_TIPO_PESSOA_RECEBEDOR AS tipo_rec
    FROM PIXDWPRO_ACC.SPITB_LAF_LANCAMENTO_FATO AS A
    WHERE 
        A.LAF_DT_LIQUIDACAO BETWEEN '2020-11-01' AND '2024-01-01'
        AND  A.STA_CD_LIQUIDADA = 'S'
        AND  A.STA_CD_REJEICAO = 'N'
        AND EXISTS(SELECT 1 FROM ID_list WHERE ID_list.id = TO_NUMBER(A.PES_NU_CPF_CNPJ_PAGADOR) AND ID_list.tipo = A.TPP_CD_TIPO_PESSOA_PAGADOR)
        AND EXISTS(SELECT 1 FROM ID_list WHERE ID_list.id = TO_NUMBER(A.PES_NU_CPF_CNPJ_RECEBEDOR) AND ID_list.tipo = A.TPP_CD_TIPO_PESSOA_RECEBEDOR)
),
FirstSend AS (
  SELECT
      ID_list.id AS id,
      ID_list.id_municipio_receita AS id_municipio_receita,
      ID_list.tipo AS tipo,
      B.dia AS dia
  FROM ID_list
    LEFT JOIN (SELECT id_send, tipo_send, MIN(dia) AS dia FROM MAIN WHERE id_send <> id_rec GROUP BY id_send, tipo_send) AS B
    ON (B.id_send = ID_list.id AND B.tipo_send = ID_list.tipo)
),
FirstReceive AS (
  SELECT
      ID_list.id AS id,
      ID_list.id_municipio_receita AS id_municipio_receita,
      ID_list.tipo AS tipo,
      B.dia AS dia
  FROM  
    ID_list
    LEFT JOIN (SELECT id_rec, tipo_rec, MIN(dia) AS dia FROM MAIN WHERE id_send <> id_rec GROUP BY id_rec, tipo_rec) AS B
    ON (B.id_rec = ID_list.id AND B.tipo_rec = ID_list.tipo)
),
FirstSelf AS (
  SELECT
      ID_list.id AS id,
      ID_list.id_municipio_receita AS id_municipio_receita,
      ID_list.tipo AS tipo,
      B.dia AS dia
  FROM ID_list
    LEFT JOIN (SELECT id_send, tipo_send, MIN(dia) AS dia FROM MAIN WHERE id_send = id_rec GROUP BY id_send, tipo_send) AS B
    ON (B.id_send = ID_list.id AND B.tipo_send = ID_list.tipo)
),
FirstPix AS (
SELECT
    B.id,
    B.id_municipio_receita,
    B.tipo,
    MIN(B.dia) AS dia
FROM (SELECT * FROM FirstSend UNION ALL
    SELECT * FROM FirstReceive UNION ALL
    SELECT * FROM FirstSelf) AS B
GROUP BY 
  id, id_municipio_receita, tipo
),
Pix_count AS (
SELECT 
    dia,
    id_municipio_receita,
    tipo,
    COUNT(id) AS adopters
FROM FirstPix
GROUP BY 
    dia, id_municipio_receita, tipo 
), 
SelfPix_count AS (
SELECT 
    dia,
    id_municipio_receita,
    tipo,
    COUNT(id) AS self_adopters
FROM FirstSelf
GROUP BY 
    dia, id_municipio_receita, tipo 
), 
SendPix_count AS (
SELECT 
    dia,
    id_municipio_receita,
    tipo,
    COUNT(id) AS send_adopters
FROM FirstSend
GROUP BY 
    dia, id_municipio_receita, tipo 
), 
ReceivePix_count AS (
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
    Pix_count.dia AS dia,
    Pix_count.id_municipio_receita AS id_municipio_receita,
    Pix_count.tipo AS tipo,
    Pix_count.adopters AS adopters,
    COALESCE(SelfPix_count.self_adopters, 0) AS self_adopters,
    COALESCE(SendPix_count.send_adopters, 0) AS send_adopters,
    COALESCE(ReceivePix_count.rec_adopters, 0) AS rec_adopters
FROM Pix_count 
    LEFT JOIN SelfPix_count ON Pix_count.dia = SelfPix_count.dia AND Pix_count.id_municipio_receita = SelfPix_count.id_municipio_receita AND Pix_count.tipo = SelfPix_count.tipo 
    LEFT JOIN SendPix_count ON Pix_count.dia = SendPix_count.dia AND Pix_count.id_municipio_receita = SendPix_count.id_municipio_receita AND Pix_count.tipo = SendPix_count.tipo 
    LEFT JOIN ReceivePix_count ON Pix_count.dia = ReceivePix_count.dia AND Pix_count.id_municipio_receita = ReceivePix_count.id_municipio_receita AND Pix_count.tipo = ReceivePix_count.tipo;
    