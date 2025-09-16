-- Winsorized values of number of accounts. For individuals
-- # Variables: week, id_municipio_receita, tipo, muni_stock, muni_stock_w, banked_pop

WITH Stock_person AS (
  SELECT	
    COUNT(DISTINCT PAR_CD_CNPJ_PAR) as stock_unique,
    RFB.MUN_CD as id_municipio_receita,
    REL_CD_TIPO_PESSOA AS tipo
  FROM	CCSDWPRO_ACC.CCSTB_FRE_FATO_RELACIONAMENTO as CCS
  LEFT JOIN BCBDWPRO_ACC.PESTB_PEF_PESSOA_FISICA AS RFB 
    ON TO_NUMBER(CCS.REL_CD_CPF_CNPJ) = TO_NUMBER(RFB.PEF_CD_CPF)
  WHERE 
    REL_CD_TIPO_PESSOA = 1 AND
    REL_DT_INICIO < TO_DATE('@selectedDateEND','YYYY-MM-DD') AND 
    (REL_DT_FIM >= TO_DATE('@selectedDateEND','YYYY-MM-DD')  OR REL_ST_STATUS_RELACIONAMENTO = 'A')
  GROUP BY RFB.MUN_CD, REL_CD_CPF_CNPJ, REL_CD_TIPO_PESSOA
),
PercentileData1 AS (
  SELECT
    stock_unique,
    PERCENT_RANK() OVER (ORDER BY stock_unique) AS PercentileRank
  FROM Stock_person
  WHERE tipo = 1
),
Limits1(high) AS (
  SELECT MIN(CASE WHEN PercentileRank >= 0.95 THEN stock_unique END)
  FROM PercentileData1
)
SELECT 
  SUM(stock_unique) AS muni_stock,
  SUM(CASE
        WHEN stock_unique > Limits1.high THEN Limits1.high
        ELSE stock_unique
  END) AS muni_stock_w,
  SUM(CASE
      WHEN stock_unique >= 1 THEN 1
      ELSE 0
  END) AS banked_pop,
  id_municipio_receita,
  tipo,
  @WEEK AS week
FROM Stock_person, Limits1
GROUP BY id_municipio_receita, tipo;


