-- TED_STR_P2P
WITH CT_tableSTRDetails AS 
(
SELECT 
    id_send,
    id_rec,
    LIF_CD_NUM_OPER,
    REC.MUN_CD AS mun_rec,
    SEND.MUN_CD AS mun_send
FROM
	"depep.jrenato".VT_tableSTRDetails AS tableSTRDetails	
	INNER JOIN BCBDWPRO_ACC.PESTB_PEF_PESSOA_FISICA AS REC ON id_rec = REC.PEF_CD_CPF
    INNER JOIN BCBDWPRO_ACC.PESTB_PEF_PESSOA_FISICA AS SEND ON id_send = SEND.PEF_CD_CPF
),
CT_tableSTRMain AS (
SELECT
	tableSTRMain.HIL_VL AS valor,
	HIL_CD_NU_OPER
FROM SSTRDWPRO_ACC.STRTB_HIL_HIST_LAN AS tableSTRMain
WHERE
    tableSTRMain.HIL_DT_MOVTO >= '@selectedDateSTART' AND tableSTRMain.HIL_DT_MOVTO < '@selectedDateEND'
),
MAIN AS (
SELECT 
    id_send,
    id_rec,
    mun_rec,
    mun_send,
    valor
FROM CT_tableSTRMain AS tableSTRMain
    INNER JOIN CT_tableSTRDetails AS tableSTRDetails ON tableSTRMain.HIL_CD_NU_OPER = tableSTRDetails.LIF_CD_NUM_OPER
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
SELECT 1 AS sender_type, 1 AS receiver_type, flow_code, id_municipio_receita, senders,
       receivers, valor, trans, @WEEK AS week FROM INFLOW
UNION ALL
SELECT 1 AS sender_type, 1 AS receiver_type, flow_code, id_municipio_receita, senders,
       receivers, valor, trans, @WEEK AS week FROM OUTFLOW
UNION ALL
SELECT 1 AS sender_type, 1 AS receiver_type, flow_code, id_municipio_receita, senders,
       receivers, valor, trans, @WEEK AS week FROM INTRA
UNION ALL
SELECT 1 AS sender_type, 1 AS receiver_type, flow_code, id_municipio_receita, senders,
       receivers, valor, trans, @WEEK AS week FROM SELF_INTRA;