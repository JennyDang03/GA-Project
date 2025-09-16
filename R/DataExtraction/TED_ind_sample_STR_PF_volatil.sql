-- Variables: week, id, tipo, value_send, trans_send, value_rec, trans_rec, value_self, trans_self

WITH ID_list AS (
  SELECT CPF_CD AS id
  FROM DL_DEPEP_ESTABILIDADE_FINANCEIRA.Random_sample_Pix_Fin_Rev
),
CT_tableSTRDetails AS (
SELECT 
    id_send,
    id_rec,
    LIF_CD_TP_PES_DEB,
    LIF_CD_TP_PES_CRED,
    LIF_CD_NUM_OPER
FROM
	"depep.jrenato".VT_tableSTRDetails AS tableSTRDetails  
WHERE
    ((EXISTS(SELECT 1 FROM ID_list WHERE ID_list.id = tableSTRDetails.id_send)
        AND tableSTRDetails.LIF_CD_TP_PES_DEB = 'F') OR 
     (EXISTS(SELECT 1 FROM ID_list WHERE ID_list.id = tableSTRDetails.id_rec)
        AND tableSTRDetails.LIF_CD_TP_PES_CRED = 'F'))
),
CT_tableSTRMain AS (
SELECT
	tableSTRMain.HIL_VL AS valor,
	HIL_CD_NU_OPER
FROM STRDWPRO_ACC.STRTB_HIL_HIST_LAN AS tableSTRMain
WHERE
    tableSTRMain.HIL_DT_MOVTO >= '@selectedDateSTART' AND tableSTRMain.HIL_DT_MOVTO < '@selectedDateEND'
),
MAIN_send AS (
    SELECT 
      tableSTRDetails.id_send AS id,
      SUM(tableSTRMain.valor) AS valor,
      COUNT(tableSTRMain.valor) AS trans
    FROM CT_tableSTRMain AS tableSTRMain
      INNER JOIN CT_tableSTRDetails AS tableSTRDetails
        ON	tableSTRMain.HIL_CD_NU_OPER = tableSTRDetails.LIF_CD_NUM_OPER
    WHERE 
        tableSTRDetails.LIF_CD_TP_PES_DEB = 'F'
        AND tableSTRDetails.id_send <> tableSTRDetails.id_rec
    GROUP BY id
),
MAIN_rec AS (
    SELECT 
      tableSTRDetails.id_rec AS id,
      SUM(tableSTRMain.valor) AS valor,
      COUNT(tableSTRMain.valor) AS trans
    FROM CT_tableSTRMain AS tableSTRMain
      INNER JOIN CT_tableSTRDetails AS tableSTRDetails
        ON	tableSTRMain.HIL_CD_NU_OPER = tableSTRDetails.LIF_CD_NUM_OPER
    WHERE 
        tableSTRDetails.LIF_CD_TP_PES_CRED = 'F'
        AND tableSTRDetails.id_send <> tableSTRDetails.id_rec
    GROUP BY id
),
MAIN_self AS (
    SELECT 
      tableSTRDetails.id_rec AS id,
      SUM(tableSTRMain.valor) AS valor,
      COUNT(tableSTRMain.valor) AS trans
    FROM CT_tableSTRMain AS tableSTRMain
      INNER JOIN CT_tableSTRDetails AS tableSTRDetails
        ON	tableSTRMain.HIL_CD_NU_OPER = tableSTRDetails.LIF_CD_NUM_OPER
    WHERE 
        tableSTRDetails.LIF_CD_TP_PES_CRED = 'F'
        AND tableSTRDetails.LIF_CD_TP_PES_DEB = 'F'
        AND tableSTRDetails.id_send = tableSTRDetails.id_rec
    GROUP BY id
)
SELECT
    @WEEK as week,
    1 AS tipo,
    ID_list.id AS id,
    COALESCE(MAIN_send.valor, 0) AS value_send,
    COALESCE(MAIN_send.trans, 0) AS trans_send,
    COALESCE(MAIN_rec.valor, 0) AS value_rec,
    COALESCE(MAIN_rec.trans, 0) AS trans_rec,
    COALESCE(MAIN_self.valor, 0) AS value_self,
    COALESCE(MAIN_self.trans, 0) AS trans_self
FROM ID_list 
LEFT JOIN MAIN_send ON (MAIN_send.id = ID_list.id)
LEFT JOIN MAIN_rec ON (MAIN_rec.id = ID_list.id)
LEFT JOIN MAIN_self ON (MAIN_self.id = ID_list.id)