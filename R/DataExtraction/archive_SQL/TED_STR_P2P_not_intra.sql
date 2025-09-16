-- TED_STR_P2P

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
        INNER JOIN BCBDWPRO_ACC.PESTB_PEF_PESSOA_FISICA AS MUNI_REC
            ON id_rec = TRYCAST(LEFT(MUNI_REC.PEF_CD_CPF, 11) AS BIGINT)
        INNER JOIN BCBDWPRO_ACC.PESTB_PEF_PESSOA_FISICA AS MUNI_SEND
            ON id_send = TRYCAST(LEFT(MUNI_SEND.PEF_CD_CPF, 11) AS BIGINT)
    WHERE 
        HIL_DT_MOVTO >= '@selectedDateSTART' AND HIL_DT_MOVTO < '@selectedDateEND'
		AND LIF_CD_TP_PES_CRED = 'F'
        AND LIF_CD_TP_PES_DEB = 'F'
        AND mun_rec <> mun_send AND id_send IS NOT NULL AND id_rec IS NOT NULL
), 
INFLOW AS (
	SELECT
        1 AS flow_code,
		A.mun_rec AS id_municipio_receita,
        COUNT(DISTINCT A.id_send) AS senders,
        COUNT(DISTINCT A.id_rec) AS receivers,
        SUM(A.valor) AS valor,
        COUNT(A.valor) AS trans
    FROM MAIN AS A
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
    GROUP BY A.mun_send
)
SELECT 1 AS sender_type, 1 AS receiver_type, flow_code, id_municipio_receita, senders,
       receivers, valor, trans, @WEEK AS week FROM INFLOW
UNION ALL
SELECT 1 AS sender_type, 1 AS receiver_type, flow_code, id_municipio_receita, senders,
       receivers, valor, trans, @WEEK AS week FROM OUTFLOW;