
-- This left join is kinda wrong, since it is actually a inner join. Those that never did pix are not in the PIX data so we dont have their municipality. 
--  LEFT JOIN BCBDWPRO_ACC.PESTB_PEF_PESSOA_FISICA AS RFB 
--    ON CCS.REL_CD_CPF_CNPJ = TO_NUMBER(RFB.PEF_CD_CPF)

-- tipo = 1
-- '@selectedDateSTART' is for pix and boleto. they need to match. 
-- I believe that boleto rec is incorrect. we are doing the left because it is implied to be PJ


WITH ID_list AS (
  SELECT
    RFB.PEF_CD_CPF as id,   -- RFB.PEF_CD_CPF is a string. 
    RFB.MUN_CD as id_municipio_receita
  FROM 
    DL_DEPEP_ESTABILIDADE_FINANCEIRA.Random_sample_Pix_Fin_Rev as RNDSAMPLE
    INNER JOIN BCBDWPRO_ACC.PESTB_PEF_PESSOA_FISICA AS RFB 
    ON RNDSAMPLE.CPF_CD = RFB.PEF_CD_CPF
),
PIX_send AS (
  SELECT 
    ID_list.id AS id_send,
    COALESCE(SUM(PIX.LAF_VL), 0) as pix_value_send,
    COALESCE(COUNT(PIX.LAF_VL), 0) as pix_trans_send
  FROM ID_list
    INNER JOIN PIXDWPRO_ACC.SPITB_LAF_LANCAMENTO_FATO AS PIX 
    ON (PIX.PES_NU_CPF_CNPJ_PAGADOR = ID_list.id)
  WHERE
    PIX.LAF_DT_LIQUIDACAO >= '@selectedDateSTART' 
    AND PIX.LAF_DT_LIQUIDACAO < '@selectedDateEND'
    --LAF_DT_LIQUIDACAO >= @selectedDateSTART AND LAF_DT_LIQUIDACAO < @selectedDateEND
    AND PIX.STA_CD_LIQUIDADA = 'S'
    AND PIX.STA_CD_REJEICAO = 'N'
    AND PIX.TPP_CD_TIPO_PESSOA_PAGADOR = 1
    AND PIX.PES_NU_CPF_CNPJ_PAGADOR <> PIX.PES_NU_CPF_CNPJ_RECEBEDOR
  GROUP BY
  ID_list.id
),
PIX_rec AS (
  SELECT 
    ID_list.id AS id_rec,
    COALESCE(SUM(PIX.LAF_VL), 0) AS pix_value_rec,
    COALESCE(COUNT(PIX.LAF_VL), 0) AS pix_trans_rec
  FROM ID_list
    INNER JOIN PIXDWPRO_ACC.SPITB_LAF_LANCAMENTO_FATO AS PIX 
    ON (PIX.PES_NU_CPF_CNPJ_RECEBEDOR = ID_list.id)
  WHERE
    PIX.LAF_DT_LIQUIDACAO >= '@selectedDateSTART' 
    AND PIX.LAF_DT_LIQUIDACAO < '@selectedDateEND'
    AND PIX.STA_CD_LIQUIDADA = 'S'
    AND PIX.STA_CD_REJEICAO = 'N'
    AND PIX.TPP_CD_TIPO_PESSOA_RECEBEDOR = 1
    AND PIX.PES_NU_CPF_CNPJ_PAGADOR <> PIX.PES_NU_CPF_CNPJ_RECEBEDOR
  GROUP BY
  ID_list.id
),
PIX_self AS (
  SELECT
    ID_list.id AS id_self,
    COALESCE(SUM(PIX.LAF_VL), 0) AS pix_value_self,
    COALESCE(COUNT(PIX.LAF_VL), 0) AS pix_trans_self
  FROM ID_list
    INNER JOIN PIXDWPRO_ACC.SPITB_LAF_LANCAMENTO_FATO AS PIX 
    ON (PIX.PES_NU_CPF_CNPJ_PAGADOR = ID_list.id)
  WHERE
    PIX.LAF_DT_LIQUIDACAO >= '@selectedDateSTART' 
    AND PIX.LAF_DT_LIQUIDACAO < '@selectedDateEND'
    AND PIX.STA_CD_LIQUIDADA = 'S'
    AND PIX.STA_CD_REJEICAO = 'N'
    AND PIX.TPP_CD_TIPO_PESSOA_RECEBEDOR = 1
    AND PIX.PES_NU_CPF_CNPJ_PAGADOR = PIX.PES_NU_CPF_CNPJ_RECEBEDOR
  GROUP BY
    ID_list.id
)
-- Boleto - done, 
-- Credito - done,
-- CCS - done,

-- TED_rec_sintraf AS ()

-- RAIS? for pj, maybe? I dont know what is interesting there. 
-- Estban in the future. 

-- PJ
-- pix, ted, boleto, credito, ccs, credit and debit card

-- Banks
-- Pix muni bank, ted, boleto, credito, ccs


SELECT
  ID_list.id as id,
  ID_list.id_municipio as id_municipio,
  1 as tipo,
  @MONTH AS time_id,
  -- PIX
  COALESCE(PIX_send.pix_value_send, 0) as pix_value_send,
  COALESCE(PIX_send.pix_trans_send, 0) as pix_trans_send,
  COALESCE(PIX_rec.pix_value_rec, 0) AS pix_value_rec,
  COALESCE(PIX_rec.pix_trans_rec, 0) AS pix_trans_rec,
  COALESCE(PIX_self.pix_value_self, 0) AS pix_value_self,
  COALESCE(PIX_self.pix_trans_self, 0) AS pix_trans_self
FROM ID_list
  LEFT JOIN PIX_send
  ON (PIX_send.id_send = ID_list.id)
  LEFT JOIN PIX_rec
  ON (PIX_rec.id_rec = ID_list.id)
  LEFT JOIN PIX_self
  ON (PIX_self.id_self = ID_list.id)

