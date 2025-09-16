-- Boleto_adoption_PJ
WITH IDs AS (
  SELECT 
    TO_NUMBER(LEFT(PEJ_CD_CNPJ14, 14)) AS id, 'J' AS tipo2, 2 AS tipo,
    PEJ_DT_ABERTURA as open_date,
    (CASE WHEN SPJ_CD_SITUACAO_PJ_RFB IN (8,3,4)
          THEN PEJ_DT_ALTERACAO_SITUACAO ELSE NULL
          END) AS close_date
  FROM BCBDWPRO_ACC.PESTB_PEJ_PESSOA_JURIDICA
  WHERE 
    MUN_CD = @MUNI_CD_LOOP AND
    open_date < '2023-01-01' AND
    (close_date IS NULL OR close_date > '2019-01-01')
-- 8 = baixada, 3 = suspensa, 4 = inapta
),
MAIN AS (
  SELECT
    ((EXTRACT(YEAR FROM boleto.DTE_DT_BASE_MVTO)-1960)*12 
      + EXTRACT(MONTH FROM boleto.DTE_DT_BASE_MVTO)) AS time_id,
    (CASE WHEN IDs_send.tipo IS NULL THEN NULL
        ELSE IDs_send.id END) AS id_send,
    (CASE WHEN IDs_rec.tipo IS NULL THEN NULL
        ELSE IDs_rec.id END) AS id_rec,
    IDs_send.tipo AS tipo_send,
    IDs_rec.tipo AS tipo_rec
  FROM
    CIPDWPRO_ACC.CIPTB_BBC_BOLETO_BAIXA_DIARIA_CIP as boleto
    LEFT JOIN IDs AS IDs_send
      ON (TO_NUMBER(CASE WHEN boleto.PEB_CD_TP_PESSOA_PAGDR = '' THEN NULL
                          WHEN boleto.PEB_CD_CNPJ_CPF_PAGDR = 'N/I' THEN NULL
						              ELSE boleto.PEB_CD_CNPJ_CPF_PAGDR END) = IDs_send.id 
        AND boleto.PEB_CD_TP_PESSOA_PAGDR = IDs_send.tipo2)
    LEFT JOIN IDs AS IDs_rec
      ON (TO_NUMBER(CASE WHEN (boleto.PEB_CD_TP_PESSOA_BENFCRIO_FINL = '' OR boleto.PEB_CD_CNPJ_CPF_BENFCRIO_FINL = 'N/I' OR boleto.PEB_CD_CNPJ_CPF_BENFCRIO_FINL = '') 
                          THEN (CASE WHEN (boleto.PEB_CD_CNPJ_CPF_BENFCRIO_OR = '' OR boleto.PEB_CD_CNPJ_CPF_BENFCRIO_OR = 'N/I') THEN NULL ELSE boleto.PEB_CD_CNPJ_CPF_BENFCRIO_OR END)
                          ELSE boleto.PEB_CD_CNPJ_CPF_BENFCRIO_FINL END) = IDs_rec.id 
        AND COALESCE(PEB_CD_TP_PESSOA_BENFCRIO_FINL, PEB_CD_TP_PESSOA_BENFCRIO_OR) = IDs_rec.tipo2)
  WHERE
    boleto.DTE_DT_BASE_MVTO < '2023-01-01'
    AND (IDs_send.tipo IS NOT NULL OR IDs_rec.tipo IS NOT NULL)
),
Adoption AS (
SELECT
    id,
    tipo,
    MIN(time_id) AS time_id
FROM (SELECT id_send AS id, tipo_send AS tipo, time_id FROM MAIN UNION ALL SELECT id_rec AS id, tipo_rec AS tipo, time_id FROM MAIN) AS Combined2
GROUP BY 
  id, tipo
)
SELECT 
  time_id, 
  tipo,
  COUNT(id) AS adopters, 
  @MUNI_CD_LOOP as id_municipio_receita
FROM Adoption
GROUP BY time_id, tipo