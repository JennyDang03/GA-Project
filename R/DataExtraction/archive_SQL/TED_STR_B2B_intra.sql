-- TED_STR_B2B

WITH MAIN AS (
    SELECT
        TRYCAST(tableSTRDetails.LIF_CD_CNPJCPFDEB1 AS BIGINT) AS id_send,
    	TRYCAST(tableSTRDetails.LIF_CD_CNPJCPFCRE1 AS BIGINT) AS id_rec,
        MUNI_REC.MUN_CD AS mun_rec,
        MUNI_SEND.MUN_CD AS mun_send,
        tableSTRMain.HIL_VL AS valor
    FROM STRDWPRO_ACC.STRTB_HIL_HIST_LAN AS tableSTRMain
        INNER JOIN STRDWPRO_ACC.STRTB_LIF_LAN_IF AS tableSTRDetails
            ON	tableSTRMain.HIL_CD_NU_OPER = tableSTRDetails.LIF_CD_NUM_OPER
        INNER JOIN BCBDWPRO_ACC.PESTB_PEJ_PESSOA_JURIDICA AS MUNI_REC
            ON id_rec = TRYCAST(LEFT(MUNI_REC.PEJ_CD_CNPJ14, 14) AS BIGINT)
        INNER JOIN BCBDWPRO_ACC.PESTB_PEJ_PESSOA_JURIDICA AS MUNI_SEND
            ON id_send = TRYCAST(LEFT(MUNI_SEND.PEJ_CD_CNPJ14, 14) AS BIGINT)
    WHERE 
        HIL_DT_MOVTO >= '@selectedDateSTART' AND HIL_DT_MOVTO < '@selectedDateEND'
		AND LIF_CD_TP_PES_CRED = 'J'
        AND LIF_CD_TP_PES_DEB = 'J'
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
SELECT 2 AS sender_type, 2 AS receiver_type, flow_code, id_municipio_receita, senders,
       receivers, valor, trans, @WEEK AS week FROM INTRA
UNION ALL
SELECT 2 AS sender_type, 2 AS receiver_type, flow_code, id_municipio_receita, senders,
       receivers, valor, trans, @WEEK AS week FROM SELF_INTRA;
