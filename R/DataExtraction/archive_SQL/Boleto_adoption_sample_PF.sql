
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
    	TO_NUMBER(CASE WHEN A.PEB_CD_TP_PESSOA_PAGDR = '' THEN NULL
						        ELSE A.PEB_CD_CNPJ_CPF_PAGDR END) AS id_send,
        TO_NUMBER(CASE WHEN A.PEB_CD_TP_PESSOA_BENFCRIO_FINL = '' THEN A.PEB_CD_CNPJ_CPF_BENFCRIO_OR
			              ELSE A.PEB_CD_CNPJ_CPF_BENFCRIO_FINL END) AS id_rec, 
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
        END) AS tipo_rec
    FROM CIPDWPRO_ACC.CIPTB_BBC_BOLETO_BAIXA_DIARIA_CIP AS A
    WHERE 
        A.DTE_DT_BASE_MVTO < '2024-01-01'
        AND EXISTS(SELECT 1 FROM ID_list WHERE ID_list.id = TO_NUMBER(CASE WHEN A.PEB_CD_TP_PESSOA_PAGDR = '' THEN NULL ELSE A.PEB_CD_CNPJ_CPF_PAGDR END) AND ID_list.tipo = (CASE WHEN A.PEB_CD_TP_PESSOA_PAGDR = 'F' THEN 1 WHEN A.PEB_CD_TP_PESSOA_PAGDR = 'J' THEN 2 ELSE NULL END))
        AND EXISTS(SELECT 1 FROM ID_list WHERE ID_list.id = TO_NUMBER(CASE WHEN A.PEB_CD_TP_PESSOA_BENFCRIO_FINL = '' THEN A.PEB_CD_CNPJ_CPF_BENFCRIO_OR ELSE A.PEB_CD_CNPJ_CPF_BENFCRIO_FINL END) AND ID_list.tipo = (CASE WHEN COALESCE(A.PEB_CD_TP_PESSOA_BENFCRIO_FINL, A.PEB_CD_TP_PESSOA_BENFCRIO_OR) ='F' THEN 1 WHEN COALESCE(A.PEB_CD_TP_PESSOA_BENFCRIO_FINL, A.PEB_CD_TP_PESSOA_BENFCRIO_OR) ='J' THEN 2 ELSE NULL END))
        AND tipo_send <> NULL AND tipo_rec <> NULL AND A.PEB_CD_CNPJ_CPF_PAGDR <> 'N/I'
),
FirstSend AS (
  SELECT
      ID_list.id as id,
      ID_list.id_municipio_receita as id_municipio_receita,
      ID_list.tipo as tipo,
      B.dia AS dia
  FROM  
    ID_list
    LEFT JOIN (SELECT id_send, tipo_send, MIN(dia) AS dia FROM MAIN WHERE id_send <> id_rec GROUP BY id_send, tipo_send) AS B
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
    LEFT JOIN (SELECT id_rec, tipo_rec, MIN(dia) AS dia FROM MAIN WHERE id_send <> id_rec GROUP BY id_rec, tipo_rec) AS B
    ON (B.id_rec = ID_list.id AND B.tipo_rec = ID_list.tipo)
),
FirstSelf AS (
  SELECT
      ID_list.id as id,
      ID_list.id_municipio_receita as id_municipio_receita,
      ID_list.tipo as tipo,
      B.dia AS dia
  FROM ID_list
    LEFT JOIN (SELECT id_send, tipo_send, MIN(dia) AS dia FROM MAIN WHERE id_send = id_rec GROUP BY id_send, tipo_send) AS B
    ON (B.id_send = ID_list.id AND B.tipo_send = ID_list.tipo)
),
FirstBoleto AS (
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
Boleto_count AS (
SELECT 
    dia,
    id_municipio_receita,
    tipo,
    COUNT(id) AS adopters
FROM FirstBoleto
GROUP BY 
    dia, id_municipio_receita, tipo 
), 
SelfBoleto_count AS (
SELECT 
    dia,
    id_municipio_receita,
    tipo,
    COUNT(id) AS self_adopters
FROM FirstSelf
GROUP BY 
    dia, id_municipio_receita, tipo 
), 
SendBoleto_count AS (
SELECT 
    dia,
    id_municipio_receita,
    tipo,
    COUNT(id) AS send_adopters
FROM FirstSend
GROUP BY 
    dia, id_municipio_receita, tipo 
), 
ReceiveBoleto_count AS (
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
    Boleto_count.dia AS dia,
    Boleto_count.id_municipio_receita AS id_municipio_receita,
    Boleto_count.tipo AS tipo,
    Boleto_count.adopters AS adopters,
    COALESCE(SelfBoleto_count.self_adopters, 0) AS self_adopters,
    COALESCE(SendBoleto_count.send_adopters, 0) AS send_adopters,
    COALESCE(ReceiveBoleto_count.rec_adopters, 0) AS rec_adopters
FROM Boleto_count 
    LEFT JOIN SelfBoleto_count ON Boleto_count.dia = SelfBoleto_count.dia AND Boleto_count.id_municipio_receita = SelfBoleto_count.id_municipio_receita AND Boleto_count.tipo = SelfBoleto_count.tipo 
    LEFT JOIN SendBoleto_count ON Boleto_count.dia = SendBoleto_count.dia AND Boleto_count.id_municipio_receita = SendBoleto_count.id_municipio_receita AND Boleto_count.tipo = SendBoleto_count.tipo 
    LEFT JOIN ReceiveBoleto_count ON Boleto_count.dia = ReceiveBoleto_count.dia AND Boleto_count.id_municipio_receita = ReceiveBoleto_count.id_municipio_receita AND Boleto_count.tipo = ReceiveBoleto_count.tipo;
    