-- Adoption - Sender
-- AND LAF_VL <> 0 -- Exclude cases where LAF_VL is zero
-- Assuming that the CPF shows up only when they make a transaction.  

-- SUM(COUNT(send_id)) OVER (PARTITION BY muni_cd ORDER BY DIA) AS total_send_adopters
    
WITH FirstSend AS (
  SELECT
      PES_NU_CPF_CNPJ_PAGADOR as send_id,
      MIN(LAF_DT_LIQUIDACAO) as DIA,
      CLI_PAG.MUN_CD as muni_cd
  FROM
      PIXDWPRO_ACC.SPITB_LAF_LANCAMENTO_FATO PIX
  LEFT JOIN PIXDWPRO_ACC.SPIVW_PES_PESSOA_FIS_JUR CLI_PAG ON (PIX.PES_NU_CPF_CNPJ_PAGADOR = CLI_PAG.PEG_CD_CPF_CNPJ14 AND PIX.TPP_CD_TIPO_PESSOA_PAGADOR = CLI_PAG.TPE_CD)
  WHERE
    	AND STA_CD_LIQUIDADA = 'S'
    	AND STA_CD_REJEICAO = 'N'
      AND PES_NU_CPF_CNPJ_RECEBEDOR <> PES_NU_CPF_CNPJ_PAGADOR
  GROUP BY
      PES_NU_CPF_CNPJ_PAGADOR,
      CLI_PAG.MUN_CD
),
AdoptionsByCityAndDate AS (
  SELECT
    DIA,
    muni_cd,
    COUNT(send_id) AS send_adopters
  FROM
    FirstSend
  GROUP BY
    DIA,
    muni_cd
)
SELECT
  DIA,
  muni_cd,
  send_adopters
FROM
  AdoptionsByCityAndDate
ORDER BY
  muni_cd,
  DIA;
