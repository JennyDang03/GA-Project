-- Credit_card_rec_adoption

WITH credit AS (
SELECT
  CRE_CD_CNPJ_CPF_PONTOVENDA AS id,
  ((EXTRACT(YEAR FROM CRE_DT_DTPGTO)-1960)*12 
    + EXTRACT(MONTH FROM CRE_DT_DTPGTO)) AS time_id,
  CRE_CD_TP_PESSOA_PONTOVENDA AS tipo
FROM EACDWPRO_ACC.EAC_cre_CREDITO_TGT
WHERE 
  CRE_CD_SIT_SOLICTCLIQUID IN (5,6,7) 
  AND CRE_CD_SITUACAO_RESPINSTDOMCL NOT IN ('002', '006') --filtros CIP (notar 6 e não 3 no respinstdomcl)
  AND CRE_MUN_IBGE_CD_PONTOVENDA = @MUNI_CD_LOOP
  AND CRE_DT_DTPGTO < '2023-01-01'
),
debit AS (
SELECT
  DEB_CD_CNPJ_CPF_PONTOVENDA AS id,
  ((EXTRACT(YEAR FROM DEB_DT_DT_PGTO)-1960)*12 
    + EXTRACT(MONTH FROM DEB_DT_DT_PGTO)) AS time_id,
  DEB_CD_TP_PESSOA_PONTOVENDA AS tipo
FROM EACDWPRO_ACC.EAC_DEB_DEBITO_TGT
WHERE 
  DEB_CD_SIT_SOLICTCLIQUID IN (5,6,7) 
  AND DEB_CD_SITUACAO_RESPINSTDOMCL NOT IN ('002', '006') --filtros CIP (notar 6 e não 3 no respinstdomcl)
  AND DEB_MUN_IBGE_CD_PONTOVENDA = @MUNI_CD_LOOP
  AND DEB_DT_DT_PGTO < '2023-01-01'
),
Adoption_credit AS (
SELECT
    id,
    tipo,
    MIN(time_id) AS time_id
FROM credit
GROUP BY 
  id, tipo
),
Adoption_debit AS (
SELECT
    id,
    tipo,
    MIN(time_id) AS time_id
FROM debit
GROUP BY 
  id, tipo
),
Adoption AS (
SELECT 
    id,
    tipo,
    MIN(time_id) AS time_id
FROM (
  SELECT * FROM Adoption_credit
  UNION ALL
  SELECT * FROM Adoption_debit
) AS Combined
GROUP BY id, tipo
),
-- Get adopters, adopters_credit, adopters_debit
Adopters AS (
SELECT 
  time_id, 
  tipo,
  COUNT(id) AS adopters
FROM Adoption
GROUP BY time_id, tipo
),
Adopters_credit AS (
SELECT 
  time_id, 
  tipo,
  COUNT(id) AS adopters_credit
FROM Adoption_credit
GROUP BY time_id, tipo
),
Adopters_debit AS (
SELECT 
  time_id, 
  tipo,
  COUNT(id) AS adopters_debit
FROM Adoption_debit
GROUP BY time_id, tipo
)
SELECT 
  COALESCE(Adopters.time_id, Adopters_credit.time_id, Adopters_debit.time_id) AS time_id,
  COALESCE(Adopters.tipo, Adopters_credit.tipo, Adopters_debit.tipo) AS tipo,
  COALESCE(Adopters.adopters, 0) AS adopters,
  COALESCE(Adopters_credit.adopters_credit, 0) AS adopters_credit,
  COALESCE(Adopters_debit.adopters_debit, 0) AS adopters_debit,
  @MUNI_CD_LOOP as id_municipio
FROM Adopters
FULL JOIN Adopters_credit ON Adopters.time_id = Adopters_credit.time_id AND Adopters.tipo = Adopters_credit.tipo
FULL JOIN Adopters_debit ON Adopters.time_id = Adopters_debit.time_id AND Adopters.tipo = Adopters_debit.tipo