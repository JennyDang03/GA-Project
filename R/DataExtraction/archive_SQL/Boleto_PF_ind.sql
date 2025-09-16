-- Boleto_PF_ind

WITH ID_list AS (
  SELECT
    RFB.PEF_CD_CPF as id,
    RFB.MUN_CD as id_municipio
  FROM 
    DL_DEPEP_ESTABILIDADE_FINANCEIRA.Random_sample_Pix_Fin_Rev as RNDSAMPLE
    INNER JOIN BCBDWPRO_ACC.PESTB_PEF_PESSOA_FISICA AS RFB 
    ON RNDSAMPLE.CPF_CD = RFB.PEF_CD_CPF
),
BOLETO_send AS (
    SELECT 
        id_send
        ,COALESCE(SUM(vl_baixa), 0) AS boleto_value_send
        ,COALESCE(COUNT(vl_baixa), 0) AS boleto_trans_send
    FROM (
        SELECT 
            ID_list.id AS id_send
            ,BBC_VL_TIT AS vl_nom
            -- Alguns vl de baixa eram trecho cod barras. Valores bilionarios...
            ,BBC_VL_BAIXA_EFT_TIT AS vl_baixa_orig
            ,CASE WHEN vl_nom > 0 THEN
                CASE WHEN BBC_VL_BAIXA_EFT_TIT / BBC_VL_TIT > 10 THEN BBC_VL_TIT
                ELSE BBC_VL_BAIXA_EFT_TIT END
            ELSE BBC_VL_BAIXA_EFT_TIT END AS vl_baixa	
        FROM CIPDWPRO_ACC.CIPTB_BBC_BOLETO_BAIXA_DIARIA_CIP as boleto
            INNER JOIN ID_list ON (boleto.PEB_CD_CNPJ_CPF_PAGDR = ID_list.id)
        WHERE  
            DTE_DT_BASE_MVTO >= DATE '@selectedDateSTART'   
            AND DTE_DT_BASE_MVTO < DATE '@selectedDateEND'
            AND PEB_CD_TP_PESSOA_PAGDR = 'F' -- boletos pagos por PF
    ) AS dados1
    GROUP BY 
        id_send
),
BOLETO_rec AS (
    SELECT 
        id_rec
        ,COALESCE(SUM(vl_baixa),0) AS boleto_value_rec
        ,COALESCE(COUNT(vl_baixa),0) AS boleto_trans_rec
    FROM (
        SELECT
            ID_list.id AS id_rec
            ,BBC_VL_TIT AS vl_nom
            -- Alguns vl de baixa eram trecho cod barras. Valores bilionarios...
            ,BBC_VL_BAIXA_EFT_TIT AS vl_baixa_orig
            ,CASE WHEN vl_nom > 0 THEN
                CASE WHEN BBC_VL_BAIXA_EFT_TIT / BBC_VL_TIT > 10 THEN BBC_VL_TIT
                ELSE BBC_VL_BAIXA_EFT_TIT END
            ELSE BBC_VL_BAIXA_EFT_TIT END AS vl_baixa
        FROM CIPDWPRO_ACC.CIPTB_BBC_BOLETO_BAIXA_DIARIA_CIP as boleto
            INNER JOIN ID_list ON (COALESCE(PEB_CD_CNPJ_CPF_BENFCRIO_FINL, PEB_CD_CNPJ_CPF_BENFCRIO_OR) = ID_list.id)
        WHERE  
            DTE_DT_BASE_MVTO >= DATE '@selectedDateSTART'   
            AND DTE_DT_BASE_MVTO < DATE '@selectedDateEND'
            AND COALESCE(PEB_CD_TP_PESSOA_BENFCRIO_FINL, PEB_CD_TP_PESSOA_BENFCRIO_OR) = 'F' -- boletos emitidos por PF
    ) AS dados2
    GROUP BY
        id_rec
)
SELECT
  ID_list.id as id,
  ID_list.id_municipio as id_municipio,
  1 as tipo,
  @MONTH AS time_id,
  -- BOLETO
  COALESCE(BOLETO_send.boleto_value_send, 0) as boleto_value_send,
  COALESCE(BOLETO_send.boleto_trans_send, 0) as boleto_trans_send,
  COALESCE(BOLETO_rec.boleto_value_rec, 0) AS boleto_value_rec,
  COALESCE(BOLETO_rec.boleto_trans_rec, 0) AS boleto_trans_rec
FROM ID_list
  LEFT JOIN BOLETO_send
  ON (BOLETO_send.id_send = ID_list.id)
  LEFT JOIN BOLETO_rec
  ON (BOLETO_rec.id_rec = ID_list.id)




