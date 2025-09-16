-- TED_SITRAF_B2B
WITH MAIN AS (
SELECT 
    TRYCAST(RTRIM(LTRIM(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DEB,tableCIPDetails.PAT_CD_CPF_CNPJ_DEB_T1, tableCIPDetails.PAT_CD_CPF_CNPJ_DEB_T2))) AS BIGINT) 
    AS id_send,
    TRYCAST(RTRIM(LTRIM(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DST, tableCIPDetails.PAT_CD_CPF_CNPJ_CRD,tableCIPDetails.PAT_CD_CPF_CNPJ_CRD_T1, tableCIPDetails.PAT_CD_CPF_CNPJ_CRD_T2))) AS BIGINT) 
    AS id_rec,
    MUNI_REC.MUN_CD AS mun_rec,
    MUNI_SEND.MUN_CD AS mun_send,
    tableCIPMain.PAL_VL_LANC AS valor
FROM STRDWPRO_ACC.CMC_PAL_PAG_LAN AS tableCIPMain
    INNER JOIN 	STRDWPRO_ACC.CMC_PAT_PAG_TRANSF AS tableCIPDetails
    ON tableCIPMain.PAL_CD_NUOP = tableCIPDetails.PAL_CD_NUOP AND tableCIPMain.PAL_CD_MSG = tableCIPDetails.PAT_CD_MSG
    INNER JOIN BCBDWPRO_ACC.PESTB_PEF_PESSOA_FISICA AS MUNI_REC
    ON id_rec = TRYCAST(LEFT(MUNI_REC.PEF_CD_CPF, 11) AS BIGINT)
    INNER JOIN BCBDWPRO_ACC.PESTB_PEJ_PESSOA_JURIDICA AS MUNI_SEND
    ON id_send = TRYCAST(LEFT(MUNI_SEND.PEJ_CD_CNPJ14, 14) AS BIGINT)
WHERE
    tableCIPMain.PAL_DT_MOVTO >= '@selectedDateSTART' AND tableCIPMain.PAL_DT_MOVTO < '@selectedDateEND'
    AND REGEXP_SIMILAR(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DEB, 
                        tableCIPDetails.PAT_CD_CPF_CNPJ_DEB_T1, 
                        tableCIPDetails.PAT_CD_CPF_CNPJ_DEB_T2), '^[0-9]+$', 'i') = 1
    AND REGEXP_SIMILAR(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DST,
                        tableCIPDetails.PAT_CD_CPF_CNPJ_CRD, 
                        tableCIPDetails.PAT_CD_CPF_CNPJ_CRD_T1, 
                        tableCIPDetails.PAT_CD_CPF_CNPJ_CRD_T2), '^[0-9]+$', 'i') = 1
    AND	tableCIPMain.COMP = 1
    AND	COALESCE(tableCIPDetails.PAT_CD_TP_PES_DST, tableCIPDetails.PAT_CD_TP_PES_CRD) = 'F' 
    AND COALESCE(tableCIPDetails.PAT_CD_TP_PES_REM, tableCIPDetails.PAT_CD_TP_PES_DEB) = 'J'
    AND mun_rec = mun_send AND id_send IS NOT NULL AND id_rec IS NOT NULL
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
    WHERE A.id_send = A.id_rec
    GROUP BY A.mun_send
)
SELECT 2 AS sender_type, 1 AS receiver_type, flow_code, id_municipio_receita, senders,
       receivers, valor, trans, @WEEK AS week FROM INTRA
UNION ALL
SELECT 2 AS sender_type, 1 AS receiver_type, flow_code, id_municipio_receita, senders,
       receivers, valor, trans, @WEEK AS week FROM SELF_INTRA;
