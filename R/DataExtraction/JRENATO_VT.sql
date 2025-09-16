CREATE MULTISET VOLATILE TABLE "depep.jrenato".VT_tableCIPDetails, NO LOG 
AS (
SELECT 
    TRYCAST(RTRIM(LTRIM(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DEB,tableCIPDetails.PAT_CD_CPF_CNPJ_DEB_T1, tableCIPDetails.PAT_CD_CPF_CNPJ_DEB_T2))) AS BIGINT) AS id_send,
    TRYCAST(RTRIM(LTRIM(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DST, tableCIPDetails.PAT_CD_CPF_CNPJ_CRD,tableCIPDetails.PAT_CD_CPF_CNPJ_CRD_T1, tableCIPDetails.PAT_CD_CPF_CNPJ_CRD_T2))) AS BIGINT) AS id_rec,
    PAL_CD_NUOP,
    PAT_CD_MSG 
FROM
	STRDWPRO_ACC.CMC_PAT_PAG_TRANSF AS tableCIPDetails
WHERE	
     REGEXP_SIMILAR(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DEB, 
                        tableCIPDetails.PAT_CD_CPF_CNPJ_DEB_T1, 
                        tableCIPDetails.PAT_CD_CPF_CNPJ_DEB_T2), '^[0-9]+$', 'i') = 1
     AND REGEXP_SIMILAR(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DST,
                        tableCIPDetails.PAT_CD_CPF_CNPJ_CRD, 
                        tableCIPDetails.PAT_CD_CPF_CNPJ_CRD_T1, 
                        tableCIPDetails.PAT_CD_CPF_CNPJ_CRD_T2), '^[0-9]+$', 'i') = 1	
    AND	COALESCE(tableCIPDetails.PAT_CD_TP_PES_DST, tableCIPDetails.PAT_CD_TP_PES_CRD) = 'J' 
    AND COALESCE(tableCIPDetails.PAT_CD_TP_PES_REM, tableCIPDetails.PAT_CD_TP_PES_DEB) = 'J'
	)
WITH DATA PRIMARY INDEX (ID_SEND, ID_REC) ON COMMIT PRESERVE ROWS;
--Elapsed time = 00:25:51.367 

COLLECT STATISTICS COLUMN (ID_REC), 
				   COLUMN (ID_SEND),
				   COLUMN (ID_SEND ,ID_REC),
				   COLUMN (PAL_CD_NUOP ,PAT_CD_MSG) ON "depep.jrenato".VT_tableCIPDetails;
--Elapsed time = 00:05:44.242 

				   
				   
				   
WITH CT_tableCIPDetails 
AS (
SELECT 
    ID_SEND,
    ID_REC,
    PAL_CD_NUOP,
    PAT_CD_MSG,
    MUNI_REC.MUN_CD AS mun_rec,
    MUNI_SEND.MUN_CD AS mun_send
FROM
	"depep.jrenato".VT_tableCIPDetails AS tableCIPDetails	
	INNER JOIN BCBDWPRO_ACC.PESTB_PEJ_PESSOA_JURIDICA AS MUNI_REC ON id_rec = MUNI_REC.PEJ_CD_CNPJ14
    INNER JOIN BCBDWPRO_ACC.PESTB_PEJ_PESSOA_JURIDICA AS MUNI_SEND ON id_send = MUNI_SEND.PEJ_CD_CNPJ14    
	--INNER JOIN BCBDWPRO_ACC.PESTB_PEJ_PESSOA_JURIDICA AS MUNI_REC ON id_rec = TRYCAST(LEFT(MUNI_REC.PEJ_CD_CNPJ14, 14) AS BIGINT)
    --INNER JOIN BCBDWPRO_ACC.PESTB_PEJ_PESSOA_JURIDICA AS MUNI_SEND ON id_send = TRYCAST(LEFT(MUNI_SEND.PEJ_CD_CNPJ14, 14) AS BIGINT)
                       
),
--Elapsed time = 00:05:38.288 


CT_tableCIPMain
AS (
SELECT
	PAL_VL_LANC AS valor,
	PAL_CD_NUOP,
    PAL_CD_MSG
FROM STRDWPRO_ACC.CMC_PAL_PAG_LAN AS tableCIPMain
WHERE
    tableCIPMain.PAL_DT_MOVTO >= '2022-12-20' AND tableCIPMain.PAL_DT_MOVTO < '2022-12-27'
    AND	tableCIPMain.COMP = 1
),

MAIN AS (
SELECT 
    id_send,
    id_rec,
    mun_rec,
    mun_send,
    valor
FROM CT_tableCIPMain AS tableCIPMain
    INNER JOIN CT_tableCIPDetails AS tableCIPDetails ON tableCIPMain.PAL_CD_NUOP = tableCIPDetails.PAL_CD_NUOP AND tableCIPMain.PAL_CD_MSG = tableCIPDetails.PAT_CD_MSG

/* 
    --INNER JOIN BCBDWPRO_ACC.PESTB_PEJ_PESSOA_JURIDICA AS MUNI_REC ON id_rec = TRYCAST(LEFT(MUNI_REC.PEJ_CD_CNPJ14, 14) AS BIGINT)
    --INNER JOIN BCBDWPRO_ACC.PESTB_PEJ_PESSOA_JURIDICA AS MUNI_SEND ON id_send = TRYCAST(LEFT(MUNI_SEND.PEJ_CD_CNPJ14, 14) AS BIGINT)   
WHERE
    tableCIPMain.PAL_DT_MOVTO >= '2022-12-20' AND tableCIPMain.PAL_DT_MOVTO < '2022-12-27'
    AND REGEXP_SIMILAR(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DEB, 
                        tableCIPDetails.PAT_CD_CPF_CNPJ_DEB_T1, 
                        tableCIPDetails.PAT_CD_CPF_CNPJ_DEB_T2), '^[0-9]+$', 'i') = 1
    AND REGEXP_SIMILAR(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DST,
                        tableCIPDetails.PAT_CD_CPF_CNPJ_CRD, 
                        tableCIPDetails.PAT_CD_CPF_CNPJ_CRD_T1, 
                        tableCIPDetails.PAT_CD_CPF_CNPJ_CRD_T2), '^[0-9]+$', 'i') = 1
    AND	tableCIPMain.COMP = 1
    AND	COALESCE(tableCIPDetails.PAT_CD_TP_PES_DST, tableCIPDetails.PAT_CD_TP_PES_CRD) = 'J' 
    AND COALESCE(tableCIPDetails.PAT_CD_TP_PES_REM, tableCIPDetails.PAT_CD_TP_PES_DEB) = 'J'
*/
),
--Elapsed time = 00:01:24.094 


INFLOW AS (
	SELECT
        1 AS flow_code,
		A.mun_rec AS id_municipio_receita,
        COUNT(DISTINCT A.id_send) AS senders,
        COUNT(DISTINCT A.id_rec) AS receivers,
        SUM(A.valor) AS valor,
        COUNT(A.valor) AS trans
    FROM MAIN AS A
	WHERE 
		A.mun_send <> A.mun_rec AND A.id_send IS NOT NULL AND A.id_rec IS NOT NULL
	GROUP BY A.mun_rec
),
OUTFLOW AS (
    SELECT
        -1 AS flow_code,
        A.mun_send AS id_municipio_receita,
        COUNT(DISTINCT A.id_send) AS senders,
        COUNT(DISTINCT A.id_rec) AS receivers,
        SUM(A.valor) AS valor,
        COUNT(A.valor) AS trans
    FROM MAIN AS A
    WHERE 
        A.mun_send <> A.mun_rec AND A.id_send IS NOT NULL AND A.id_rec IS NOT NULL
    GROUP BY A.mun_send
),
INTRA AS (
    SELECT
        0 AS flow_code,
        A.mun_send AS id_municipio_receita,
        COUNT(DISTINCT A.id_send) AS senders,
        COUNT(DISTINCT A.id_rec) AS receivers,
        SUM(A.valor) AS valor,
        COUNT(A.valor) AS trans 
    FROM MAIN AS A
    WHERE 
        A.mun_send = A.mun_rec AND A.id_send IS NOT NULL AND A.id_rec IS NOT NULL
    GROUP BY A.mun_send
),
SELF_INTRA AS (
    SELECT
        99 AS flow_code,
        A.mun_send AS id_municipio_receita,
        COUNT(DISTINCT A.id_send) AS senders,
        NULL AS receivers,
        SUM(A.valor) AS valor,
        COUNT(A.valor) AS trans 
    FROM MAIN AS A
    WHERE 
        A.mun_send = A.mun_rec
        AND A.id_send = A.id_rec AND A.id_send IS NOT NULL AND A.id_rec IS NOT NULL
    GROUP BY A.mun_send
)
SELECT 2 AS sender_type, 2 AS receiver_type, flow_code, id_municipio_receita, senders,
       receivers, valor, trans, 2000 AS week FROM INFLOW
UNION ALL
SELECT 2 AS sender_type, 2 AS receiver_type, flow_code, id_municipio_receita, senders,
       receivers, valor, trans, 2000 AS week FROM OUTFLOW
UNION ALL
SELECT 2 AS sender_type, 2 AS receiver_type, flow_code, id_municipio_receita, senders,
       receivers, valor, trans, 2000 AS week FROM INTRA
UNION ALL
SELECT 2 AS sender_type, 2 AS receiver_type, flow_code, id_municipio_receita, senders,
       receivers, valor, trans, 2000 AS week FROM SELF_INTRA; 
       
--Elapsed time = 00:03:25.208 