-- Adoption - Receiver
-- AND LAF_VL <> 0 -- Exclude cases where LAF_VL is zero
-- Assuming that the CPF shows up only when they make a transaction.  

-- SUM(COUNT(rec_id)) OVER (PARTITION BY muni_cd ORDER BY DIA) AS total_rec_adopters
    
WITH FirstReceive AS (
  SELECT
      PES_NU_CPF_CNPJ_RECEBEDOR as rec_id,
      MIN(LAF_DT_LIQUIDACAO) as DIA,
      CLI_REC.MUN_CD as muni_cd
  FROM
      PIXDWPRO_ACC.SPITB_LAF_LANCAMENTO_FATO PIX
  LEFT JOIN PIXDWPRO_ACC.SPIVW_PES_PESSOA_FIS_JUR CLI_REC ON (PIX.PES_NU_CPF_CNPJ_RECEBEDOR = CLI_REC.PEG_CD_CPF_CNPJ14 AND PIX.TPP_CD_TIPO_PESSOA_RECEBEDOR = CLI_REC.TPE_CD)
  WHERE
    	AND STA_CD_LIQUIDADA = 'S'
    	AND STA_CD_REJEICAO = 'N'
      AND PES_NU_CPF_CNPJ_RECEBEDOR <> PES_NU_CPF_CNPJ_PAGADOR
  GROUP BY
      PES_NU_CPF_CNPJ_RECEBEDOR,
      CLI_REC.MUN_CD
),
AdoptionsByCityAndDate AS (
  SELECT
    DIA,
    muni_cd,
    COUNT(rec_id) AS rec_adopters
  FROM
    FirstReceive
  GROUP BY
    DIA,
    muni_cd
)
SELECT
  DIA,
  muni_cd,
  rec_adopters
FROM
  AdoptionsByCityAndDate
ORDER BY
  muni_cd,
  DIA;
