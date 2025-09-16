-- Adoption

WITH FirstReceive AS (
  SELECT
      PES_NU_CPF_CNPJ_RECEBEDOR as id,
      MIN(LAF_DT_LIQUIDACAO) as DIA,
      CLI_REC.MUN_CD as muni_cd,
      TPP_CD_TIPO_PESSOA_RECEBEDOR AS tipo
  FROM PIXDWPRO_ACC.SPITB_LAF_LANCAMENTO_FATO PIX
    LEFT JOIN PIXDWPRO_ACC.SPIVW_PES_PESSOA_FIS_JUR CLI_REC ON (PIX.PES_NU_CPF_CNPJ_RECEBEDOR = CLI_REC.PEG_CD_CPF_CNPJ14 AND PIX.TPP_CD_TIPO_PESSOA_RECEBEDOR = CLI_REC.TPE_CD)
  WHERE
    	STA_CD_LIQUIDADA = 'S'
    	AND STA_CD_REJEICAO = 'N'
      AND PES_NU_CPF_CNPJ_RECEBEDOR <> PES_NU_CPF_CNPJ_PAGADOR
  GROUP BY
      PES_NU_CPF_CNPJ_RECEBEDOR,
      CLI_REC.MUN_CD,
      TPP_CD_TIPO_PESSOA_RECEBEDOR
),
FirstSend AS (
  SELECT
      PES_NU_CPF_CNPJ_PAGADOR as id,
      MIN(LAF_DT_LIQUIDACAO) as DIA,
      CLI_PAG.MUN_CD as muni_cd,
      TPP_CD_TIPO_PESSOA_PAGADOR AS tipo
  FROM PIXDWPRO_ACC.SPITB_LAF_LANCAMENTO_FATO PIX
    LEFT JOIN PIXDWPRO_ACC.SPIVW_PES_PESSOA_FIS_JUR CLI_PAG ON (PIX.PES_NU_CPF_CNPJ_PAGADOR = CLI_PAG.PEG_CD_CPF_CNPJ14 AND PIX.TPP_CD_TIPO_PESSOA_PAGADOR = CLI_PAG.TPE_CD)
  WHERE
    	STA_CD_LIQUIDADA = 'S'
    	AND STA_CD_REJEICAO = 'N'
      AND PES_NU_CPF_CNPJ_RECEBEDOR <> PES_NU_CPF_CNPJ_PAGADOR
  GROUP BY
      PES_NU_CPF_CNPJ_PAGADOR,
      CLI_PAG.MUN_CD,
      TPP_CD_TIPO_PESSOA_PAGADOR
),
FirstSelf AS (
  SELECT
      PES_NU_CPF_CNPJ_PAGADOR as id,
      MIN(LAF_DT_LIQUIDACAO) as DIA,
      CLI_PAG.MUN_CD as muni_cd,
      TPP_CD_TIPO_PESSOA_PAGADOR AS tipo
  FROM PIXDWPRO_ACC.SPITB_LAF_LANCAMENTO_FATO PIX
    LEFT JOIN PIXDWPRO_ACC.SPIVW_PES_PESSOA_FIS_JUR CLI_PAG ON (PIX.PES_NU_CPF_CNPJ_PAGADOR = CLI_PAG.PEG_CD_CPF_CNPJ14 AND PIX.TPP_CD_TIPO_PESSOA_PAGADOR = CLI_PAG.TPE_CD)
  WHERE
    	STA_CD_LIQUIDADA = 'S'
    	AND STA_CD_REJEICAO = 'N'
      AND PES_NU_CPF_CNPJ_RECEBEDOR = PES_NU_CPF_CNPJ_PAGADOR
  GROUP BY
      PES_NU_CPF_CNPJ_PAGADOR,
      CLI_PAG.MUN_CD,
      TPP_CD_TIPO_PESSOA_PAGADOR
),
Adoption_send AS (
  SELECT DIA, muni_cd, COUNT(id) AS send_adopters, tipo
  FROM FirstSend
  GROUP BY DIA, muni_cd, tipo
),
Adoption_rec AS (
  SELECT DIA, muni_cd, COUNT(id) AS rec_adopters, tipo
  FROM FirstReceive
  GROUP BY DIA, muni_cd, tipo
),
Adoption_self AS (
  SELECT DIA, muni_cd, COUNT(id) AS self_adopters, tipo
  FROM FirstSelf
  GROUP BY DIA, muni_cd, tipo
),
Adoption AS (
  SELECT DIA, muni_cd, COUNT(id) AS adopters, tipo
  FROM (SELECT id, MIN(DIA) as DIA, muni_cd, tipo
        FROM FirstReceive UNION FirstSend
        GROUP BY id, muni_cd, tipo)
  GROUP BY DIA, muni_cd, tipo
)

SELECT *
FROM Adoption
  FULL OUTER JOIN Adoption_rec  ON (Adoption.DIA = Adoption_rec.DIA  AND Adoption.muni_cd = Adoption_rec.muni_cd  AND Adoption.tipo = Adoption_rec.tipo) 
  FULL OUTER JOIN Adoption_send ON (Adoption.DIA = Adoption_send.DIA AND Adoption.muni_cd = Adoption_send.muni_cd AND Adoption.tipo = Adoption_send.tipo) 
  FULL OUTER JOIN Adoption_self ON (Adoption.DIA = Adoption_self.DIA AND Adoption.muni_cd = Adoption_self.muni_cd AND Adoption.tipo = Adoption_self.tipo) 
