-- Winsorize
-- Get random dates, calculate for them the top 5% transactions and bottom 5% transactions. 
--    LAF_DT_LIQUIDACAO in (, , , , , , , )

WITH PercentileData AS (
  SELECT
    LAF_VL,
    PERCENT_RANK() OVER (ORDER BY LAF_VL) AS PercentileRank
  FROM PIXDWPRO_ACC.SPITB_LAF_LANCAMENTO_FATO
  WHERE 
    STA_CD_LIQUIDADA = 'S'
  	AND STA_CD_REJEICAO = 'N'
    AND PES_NU_CPF_CNPJ_RECEBEDOR <> PES_NU_CPF_CNPJ_PAGADOR
)
SELECT
  MIN(CASE WHEN PercentileRank >= 0.05 THEN LAF_VL END) AS LowerLimit,
  MIN(CASE WHEN PercentileRank >= 0.95 THEN LAF_VL END) AS UpperLimit
FROM PercentileData;


--SELECT
--    PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY LAF_VL) OVER () AS LowerLimit,
--    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY LAF_VL) OVER () AS UpperLimit
--FROM PIXDWPRO_ACC.SPITB_LAF_LANCAMENTO_FATO
--WHERE
--    LAF_DT_LIQUIDACAO in (, , , , , , , )
--  	AND STA_CD_LIQUIDADA = 'S'
--  	AND STA_CD_REJEICAO = 'N'
--    AND PES_NU_CPF_CNPJ_RECEBEDOR <> PES_NU_CPF_CNPJ_PAGADOR
