-- TED_SITRAF_B2B 
WITH CT_tableCIPDetails AS 
(
SELECT 
    ID_SEND,
    ID_REC,
    PAL_CD_NUOP,
    PAT_CD_MSG,
    REC.MUN_CD AS mun_rec,
    SEND.MUN_CD AS mun_send
FROM
	"depep.jrenato".VT_tableCIPDetails AS tableCIPDetails	
	INNER JOIN BCBDWPRO_ACC.PESTB_PEJ_PESSOA_JURIDICA AS REC ON id_rec = REC.PEJ_CD_CNPJ14
    INNER JOIN BCBDWPRO_ACC.PESTB_PEJ_PESSOA_JURIDICA AS SEND ON id_send = SEND.PEJ_CD_CNPJ14   
WHERE
    -- Excludes finance and insurance
	(REC.CNA_CD < 6400000 OR REC.CNA_CD >= 6700000) and
	(SEND.CNA_CD < 6400000 OR SEND.CNA_CD >= 6700000) 
	
),

CT_tableCIPMain AS (
SELECT
	PAL_VL_LANC AS valor,
	PAL_CD_NUOP,
    PAL_CD_MSG
FROM STRDWPRO_ACC.CMC_PAL_PAG_LAN AS tableCIPMain
WHERE
    tableCIPMain.PAL_DT_MOVTO >= '@selectedDateSTART' AND tableCIPMain.PAL_DT_MOVTO < '@selectedDateEND'
    AND	tableCIPMain.COMP = 1
),

MAIN AS 
(
SELECT 
    id_send,
    id_rec,
    mun_rec,
    mun_send,
    valor
FROM CT_tableCIPMain AS tableCIPMain
    INNER JOIN CT_tableCIPDetails AS tableCIPDetails ON tableCIPMain.PAL_CD_NUOP = tableCIPDetails.PAL_CD_NUOP AND tableCIPMain.PAL_CD_MSG = tableCIPDetails.PAT_CD_MSG
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

SELECT 2 AS sender_type, 2 AS receiver_type, flow_code, id_municipio_receita, senders,
       receivers, valor, trans, @WEEK AS week FROM INFLOW
UNION ALL
SELECT 2 AS sender_type, 2 AS receiver_type, flow_code, id_municipio_receita, senders,
       receivers, valor, trans, @WEEK AS week FROM OUTFLOW
UNION ALL
SELECT 2 AS sender_type, 2 AS receiver_type, flow_code, id_municipio_receita, senders,
       receivers, valor, trans, @WEEK AS week FROM INTRA
UNION ALL
SELECT 2 AS sender_type, 2 AS receiver_type, flow_code, id_municipio_receita, senders,
       receivers, valor, trans, @WEEK AS week FROM SELF_INTRA; 
