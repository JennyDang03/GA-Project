-- Adoption

WITH FirstReceive AS (
  SELECT
      PES_NU_CPF_CNPJ_RECEBEDOR as id,
      MIN(LAF_DT_LIQUIDACAO) as day,
      CLI_REC.MUN_CD as muni_cd,
      TPP_CD_TIPO_PESSOA_RECEBEDOR AS tipo
  FROM  
    DL_DEPEP_ESTABILIDADE_FINANCEIRA.Random_sample_Pix_Fin_Rev as RNDSAMPLE
  LEFT JOIN	PIXDWPRO_ACC.SPITB_LAF_LANCAMENTO_FATO PIX ON (PIX.PES_NU_CPF_CNPJ_RECEBEDOR = RNDSAMPLE.CPF_CD)
  LEFT JOIN PIXDWPRO_ACC.SPIVW_PES_PESSOA_FIS_JUR CLI_REC ON (PIX.PES_NU_CPF_CNPJ_RECEBEDOR = CLI_REC.PEG_CD_CPF_CNPJ14 AND PIX.TPP_CD_TIPO_PESSOA_RECEBEDOR = CLI_REC.TPE_CD)
  WHERE
    	STA_CD_LIQUIDADA = 'S'
    	AND STA_CD_REJEICAO = 'N'
      AND PES_NU_CPF_CNPJ_RECEBEDOR <> PES_NU_CPF_CNPJ_PAGADOR
      AND TPP_CD_TIPO_PESSOA_RECEBEDOR = 1
  GROUP BY
      PES_NU_CPF_CNPJ_RECEBEDOR,
      CLI_REC.MUN_CD,
      TPP_CD_TIPO_PESSOA_RECEBEDOR
),
FirstSend AS (
  SELECT
      PES_NU_CPF_CNPJ_PAGADOR as id,
      MIN(LAF_DT_LIQUIDACAO) as day,
      CLI_PAG.MUN_CD as muni_cd,
      TPP_CD_TIPO_PESSOA_PAGADOR AS tipo
  FROM  
    DL_DEPEP_ESTABILIDADE_FINANCEIRA.Random_sample_Pix_Fin_Rev as RNDSAMPLE
  LEFT JOIN	PIXDWPRO_ACC.SPITB_LAF_LANCAMENTO_FATO PIX
  LEFT JOIN PIXDWPRO_ACC.SPIVW_PES_PESSOA_FIS_JUR CLI_PAG ON (PIX.PES_NU_CPF_CNPJ_PAGADOR = CLI_PAG.PEG_CD_CPF_CNPJ14 AND PIX.TPP_CD_TIPO_PESSOA_PAGADOR = CLI_PAG.TPE_CD)
  WHERE
    	STA_CD_LIQUIDADA = 'S'
    	AND STA_CD_REJEICAO = 'N'
      AND PES_NU_CPF_CNPJ_RECEBEDOR <> PES_NU_CPF_CNPJ_PAGADOR
      AND TPP_CD_TIPO_PESSOA_PAGADOR = 1
  GROUP BY
      PES_NU_CPF_CNPJ_PAGADOR,
      CLI_PAG.MUN_CD,
      TPP_CD_TIPO_PESSOA_PAGADOR
),
FirstSelf AS (
  SELECT
      PES_NU_CPF_CNPJ_PAGADOR as id,
      MIN(LAF_DT_LIQUIDACAO) as day,
      CLI_PAG.MUN_CD as muni_cd,
      TPP_CD_TIPO_PESSOA_PAGADOR AS tipo
  FROM  
    DL_DEPEP_ESTABILIDADE_FINANCEIRA.Random_sample_Pix_Fin_Rev as RNDSAMPLE
  LEFT JOIN	PIXDWPRO_ACC.SPITB_LAF_LANCAMENTO_FATO PIX
  LEFT JOIN PIXDWPRO_ACC.SPIVW_PES_PESSOA_FIS_JUR CLI_PAG ON (PIX.PES_NU_CPF_CNPJ_PAGADOR = CLI_PAG.PEG_CD_CPF_CNPJ14 AND PIX.TPP_CD_TIPO_PESSOA_PAGADOR = CLI_PAG.TPE_CD)
  WHERE
    	STA_CD_LIQUIDADA = 'S'
    	AND STA_CD_REJEICAO = 'N'
      AND PES_NU_CPF_CNPJ_RECEBEDOR = PES_NU_CPF_CNPJ_PAGADOR
      AND TPP_CD_TIPO_PESSOA_PAGADOR = 1
  GROUP BY
      PES_NU_CPF_CNPJ_PAGADOR,
      CLI_PAG.MUN_CD,
      TPP_CD_TIPO_PESSOA_PAGADOR
),
Adoption_send AS (
  SELECT day, muni_cd, COUNT(id) AS send_adopters, tipo
  FROM FirstSend
  GROUP BY day, muni_cd, tipo
),
Adoption_rec AS (
  SELECT day, muni_cd, COUNT(id) AS rec_adopters, tipo
  FROM FirstReceive
  GROUP BY day, muni_cd, tipo
),
Adoption_self AS (
  SELECT day, muni_cd, COUNT(id) AS self_adopters, tipo
  FROM FirstSelf
  GROUP BY day, muni_cd, tipo
),
Adoption AS (
  SELECT day, muni_cd, COUNT(id) AS adopters, tipo
  FROM (SELECT id, MIN(day) as day, muni_cd, tipo
        FROM FirstReceive UNION FirstSend
        GROUP BY id, muni_cd, tipo)
  GROUP BY day, muni_cd, tipo
)

SELECT *
FROM Adoption
  FULL OUTER JOIN Adoption_rec  ON (Adoption.day = Adoption_rec.day  AND Adoption.muni_cd = Adoption_rec.muni_cd  AND Adoption.tipo = Adoption_rec.tipo) 
  FULL OUTER JOIN Adoption_send ON (Adoption.day = Adoption_send.day AND Adoption.muni_cd = Adoption_send.muni_cd AND Adoption.tipo = Adoption_send.tipo) 
  FULL OUTER JOIN Adoption_self ON (Adoption.day = Adoption_self.day AND Adoption.muni_cd = Adoption_self.muni_cd AND Adoption.tipo = Adoption_self.tipo) 
