-- Winsorized values of number of accounts. 

-- # Variables: week, muni_cd, tipo, muni_stock, muni_stock_w, banked_pop

-- IDEA: 
-- Make list of people that had accounts during the start of Pix. Then, create dummy 1/0


WITH Stock_person AS (
  SELECT	
    COUNT(DISTINCT PAR_CD_CNPJ_PAR) as stock_unique,
    RFB.MUN_CD as muni_cd,
    REL_CD_TIPO_PESSOA AS tipo,
    @WEEK AS week
  FROM	CCSDWPRO_ACC.CCSTB_FRE_FATO_RELACIONAMENTO as CCS
  
  
  LEFT JOIN PIXDWPRO_ACC.SPIVW_PES_PESSOA_FIS_JUR as RFB ---------> NEED TO CHANGE THIS!!!
    ON CCS.REL_CD_CPF_CNPJ = TO_NUMBER(RFB.PEG_CD_CPF_CNPJ14)
  
  
  
  WHERE 
    REL_DT_INICIO < TO_DATE('@selectedDateEND','YYYY-MM-DD') AND 
    (REL_DT_FIM >= TO_DATE('@selectedDateEND','YYYY-MM-DD')  OR REL_ST_STATUS_RELACIONAMENTO = 'A')
  GROUP BY RFB.MUN_CD,REL_CD_CPF_CNPJ, REL_CD_TIPO_PESSOA
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
),
PercentileData2 AS (
  SELECT
    stock_unique,
    PERCENT_RANK() OVER (ORDER BY stock_unique) AS PercentileRank
  FROM Stock_person
  WHERE tipo = 2
),
Limits2(high) AS (
  SELECT MIN(CASE WHEN PercentileRank >= 0.95 THEN stock_unique END)
  FROM PercentileData2
)
SELECT 
  SUM(stock_unique) AS muni_stock,
  SUM(CASE
    WHEN tipo = 1 THEN
      CASE
        WHEN stock_unique > Limits1.high THEN Limits1.high
        ELSE stock_unique
      END
    WHEN tipo = 2 THEN
      CASE
        WHEN stock_unique > Limits2.high THEN Limits2.high
        ELSE stock_unique
      END
  END) AS muni_stock_w,
  SUM(CASE
      WHEN stock_unique >= 1 THEN 1
      ELSE 0
  END) AS banked_pop,
  muni_cd,
  tipo,
  week
FROM Stock_person, Limits1, Limits2
GROUP BY muni_cd, tipo, week


