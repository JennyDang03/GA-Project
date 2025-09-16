WITH stock_jan2018 AS (
  SELECT
    RFB.MUN_CD AS id_municipio_receita,
    COUNT(DISTINCT REL_CD_CPF_CNPJ) AS stock
  FROM CCSDWPRO_ACC.CCSTB_FRE_FATO_RELACIONAMENTO AS CCS
  LEFT JOIN BCBDWPRO_ACC.PESTB_PEF_PESSOA_FISICA AS RFB 
    ON TO_NUMBER(CCS.REL_CD_CPF_CNPJ) = TO_NUMBER(RFB.PEF_CD_CPF)
  WHERE 
    REL_CD_TIPO_PESSOA = 1 AND
    REL_DT_INICIO < TO_DATE('2018-01-01', 'YYYY-MM-DD') AND
    (REL_DT_FIM IS NULL OR REL_DT_FIM >= TO_DATE('2018-01-01', 'YYYY-MM-DD'))
  GROUP BY RFB.MUN_CD
),

monthly_openings AS (
  SELECT
    RFB.MUN_CD AS id_municipio_receita,
    TO_CHAR(REL_DT_INICIO, 'YYYY-MM') AS year_month,
    COUNT(DISTINCT REL_CD_CPF_CNPJ) AS openings
  FROM CCSDWPRO_ACC.CCSTB_FRE_FATO_RELACIONAMENTO AS CCS
  LEFT JOIN BCBDWPRO_ACC.PESTB_PEF_PESSOA_FISICA AS RFB 
    ON TO_NUMBER(CCS.REL_CD_CPF_CNPJ) = TO_NUMBER(RFB.PEF_CD_CPF)
  WHERE 
    REL_CD_TIPO_PESSOA = 1 AND
    REL_DT_INICIO >= TO_DATE('2018-01-01', 'YYYY-MM-DD') AND
    REL_DT_INICIO < TO_DATE('2024-01-01', 'YYYY-MM-DD')
  GROUP BY RFB.MUN_CD, TO_CHAR(REL_DT_INICIO, 'YYYY-MM')
),

monthly_closings AS (
  SELECT
    RFB.MUN_CD AS id_municipio_receita,
    TO_CHAR(REL_DT_FIM, 'YYYY-MM') AS year_month,
    COUNT(DISTINCT REL_CD_CPF_CNPJ) AS closings
  FROM CCSDWPRO_ACC.CCSTB_FRE_FATO_RELACIONAMENTO AS CCS
  LEFT JOIN BCBDWPRO_ACC.PESTB_PEF_PESSOA_FISICA AS RFB 
    ON TO_NUMBER(CCS.REL_CD_CPF_CNPJ) = TO_NUMBER(RFB.PEF_CD_CPF)
  WHERE 
    REL_CD_TIPO_PESSOA = 1 AND
    REL_DT_FIM IS NOT NULL AND
    REL_DT_FIM >= TO_DATE('2018-01-01', 'YYYY-MM-DD') AND
    REL_DT_FIM < TO_DATE('2024-01-01', 'YYYY-MM-DD')
  GROUP BY RFB.MUN_CD, TO_CHAR(REL_DT_FIM, 'YYYY-MM')
)

-- Final output: combine everything
SELECT
  COALESCE(o.id_municipio_receita, c.id_municipio_receita) AS id_municipio_receita,
  o.year_month,
  s.stock,
  o.openings,
  c.closings
FROM monthly_openings o
FULL OUTER JOIN monthly_closings c
  ON o.id_municipio_receita = c.id_municipio_receita AND o.year_month = c.year_month
LEFT JOIN stock_jan2018 s
  ON s.id_municipio_receita = COALESCE(o.id_municipio_receita, c.id_municipio_receita)
ORDER BY id_municipio_receita, year_month;
