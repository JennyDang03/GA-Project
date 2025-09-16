-- TED_adoption_STR

WITH IDs AS (
  SELECT 
    id,
    tipo,
    tipo2
  FROM
    (SELECT CONVERT(BIGINT, CODIGO_CPF) AS id, 'F' AS tipo2, 1 AS tipo 
      FROM [BCBASE_DP].[bcb].[PES_PESSOA_FISICA]
      WHERE CODIGO_DO_MUNICIPIO_NO_BCBASE = @MUNI_CD_LOOP
      UNION ALL 
      SELECT CONVERT(BIGINT, CODIGO_CNPJ_14) AS id, 'J' AS tipo2, 2 AS tipo
      FROM [BCBASE_DP].[bcb].[PES_PESSOA_JURIDICA] 
      WHERE CODIGO_DO_MUNICIPIO_NO_BCBASE = @MUNI_CD_LOOP) AS Combined
),
MAIN AS (
    SELECT
        ((EXTRACT(YEAR FROM tableSTRMain.HIL_DT_MOVTO)-1960)*12 
        + EXTRACT(MONTH FROM tableSTRMain.HIL_DT_MOVTO)) AS time_id,
        (CASE WHEN IDs_send.tipo IS NULL THEN NULL
            ELSE IDs_send.id END) AS id_send,
        (CASE WHEN IDs_rec.tipo IS NULL THEN NULL
            ELSE IDs_rec.id END) AS id_rec,
        IDs_send.tipo AS tipo_send,
        IDs_rec.tipo AS tipo_rec
    FROM [STR].[dbo].[STR_HIL_HIST_LAN] AS tableSTRMain
	INNER JOIN [STR].[dbo].[STR_LIF_LAN_IF] AS tableSTRDetails
	ON	tableSTRMain.HIL_CD_NU_OPER = tableSTRDetails.LIF_CD_NUM_OPER
    LEFT JOIN IDs AS IDs_send
    ON (CONVERT(BIGINT, tableSTRDetails.[LIF_CD_CNPJCPFDEB1]) = IDs_send.id 
        AND tableSTRDetails.[LIF_CD_TP_PES_DEB] = IDs_send.tipo2)
    LEFT JOIN IDs AS IDs_rec
    ON (CONVERT(BIGINT, tableSTRDetails.[LIF_CD_CNPJCPFCRE1]) = IDs_rec.id 
        AND tableSTRDetails.[LIF_CD_TP_PES_CRED] = IDs_rec.tipo2)
    WHERE
        tableSTRMain.HIL_DT_MOVTO < '2023-01-01'
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