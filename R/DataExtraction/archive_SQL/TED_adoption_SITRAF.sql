-- TED_adoption_SITRAF
WITH IDs AS (
  SELECT 
    id,
    tipo,
    tipo2
  FROM
    (SELECT CODIGO_CPF AS id, 'F' AS tipo2, 1 AS tipo 
      FROM [BCBASE_DP].[bcb].[PES_PESSOA_FISICA]
      WHERE CODIGO_DO_MUNICIPIO_NO_BCBASE = @MUNI_CD_LOOP
      UNION ALL 
      SELECT CODIGO_CNPJ_14 AS id, 'J' AS tipo2, 2 AS tipo
      FROM [BCBASE_DP].[bcb].[PES_PESSOA_JURIDICA] 
      WHERE CODIGO_DO_MUNICIPIO_NO_BCBASE = @MUNI_CD_LOOP) AS Combined
),
MAIN AS (
    SELECT
        ((EXTRACT(YEAR FROM tableCIPMain.PAL_DT_MOVTO)-1960)*12 
        + EXTRACT(MONTH FROM tableCIPMain.PAL_DT_MOVTO)) AS time_id,
        (CASE WHEN IDs_send.tipo IS NULL THEN NULL
            ELSE IDs_send.id END) AS id_send,
        (CASE WHEN IDs_rec.tipo IS NULL THEN NULL
            ELSE IDs_rec.id END) AS id_rec,
        IDs_send.tipo AS tipo_send,
        IDs_rec.tipo AS tipo_rec
    FROM [CIP].[dbo].[CMC_PAL_PAG_LAN] AS tableCIPMain
    INNER JOIN 	[CIP].[dbo].[CMC_PAT_PAG_TRANSF] AS tableCIPDetails
    ON tableCIPMain.PAL_CD_NUOP = tableCIPDetails.PAL_CD_NUOP AND tableCIPMain.PAL_CD_MSG = tableCIPDetails.PAT_CD_MSG
    LEFT JOIN IDs AS IDs_send
    ON (RTRIM(LTRIM(COALESCE(tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB],tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB_T1], tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB_T2]))) 
            COLLATE DATABASE_DEFAULT = IDs_send.id AND COALESCE(tableCIPDetails.[PAT_CD_TP_PES_REM], tableCIPDetails.[PAT_CD_TP_PES_DEB]) = IDs_send.tipo2)
    LEFT JOIN IDs AS IDs_rec
    ON (RTRIM(LTRIM(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DST, tableCIPDetails.[PAT_CD_CPF_CNPJ_CRD],tableCIPDetails.[PAT_CD_CPF_CNPJ_CRD_T1], tableCIPDetails.[PAT_CD_CPF_CNPJ_CRD_T2])))
            COLLATE DATABASE_DEFAULT = IDs_rec.id AND COALESCE(tableCIPDetails.[PAT_CD_TP_PES_DST], tableCIPDetails.[PAT_CD_TP_PES_CRD]) = IDs_rec.tipo2)
    WHERE
        tableCIPMain.PAL_DT_MOVTO < '2023-01-01'
        AND (IDs_send.tipo IS NOT NULL OR IDs_rec.tipo IS NOT NULL)
        AND
            ISNUMERIC(COALESCE(tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB], 
                                tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB_T1], 
                                tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB_T2])) = 1
        AND
            ISNUMERIC(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DST,
                                tableCIPDetails.[PAT_CD_CPF_CNPJ_CRD], 
                                tableCIPDetails.[PAT_CD_CPF_CNPJ_CRD_T1], 
                                tableCIPDetails.[PAT_CD_CPF_CNPJ_CRD_T2])) = 1
        AND	tableCIPMain.Comp = 1
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