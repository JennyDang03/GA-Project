-- Variables: week, id, tipo, value_send, trans_send, value_rec, trans_rec, value_self, trans_self
WITH ID_list AS (
  SELECT firm_id14 AS id
  FROM DL_DEPEP_ESTABILIDADE_FINANCEIRA.Random_sample_Pix_Fin_Rev_PJ
),
MAIN_send AS (
SELECT 
    TRYCAST(RTRIM(LTRIM(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DEB,tableCIPDetails.PAT_CD_CPF_CNPJ_DEB_T1, tableCIPDetails.PAT_CD_CPF_CNPJ_DEB_T2))) AS BIGINT) 
    AS id,
    SUM(tableCIPMain.PAL_VL_LANC) AS valor,
    COUNT(tableCIPMain.PAL_VL_LANC) AS trans
FROM STRDWPRO_ACC.CMC_PAL_PAG_LAN AS tableCIPMain
    INNER JOIN 	STRDWPRO_ACC.CMC_PAT_PAG_TRANSF AS tableCIPDetails
    ON tableCIPMain.PAL_CD_NUOP = tableCIPDetails.PAL_CD_NUOP AND tableCIPMain.PAL_CD_MSG = tableCIPDetails.PAT_CD_MSG
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
    --AND	COALESCE(tableCIPDetails.PAT_CD_TP_PES_DST, tableCIPDetails.PAT_CD_TP_PES_CRD) = 'J' 
    AND COALESCE(tableCIPDetails.PAT_CD_TP_PES_REM, tableCIPDetails.PAT_CD_TP_PES_DEB) = 'J'
    AND EXISTS(SELECT 1 FROM ID_list WHERE ID_list.id = TRYCAST(RTRIM(LTRIM(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DEB,tableCIPDetails.PAT_CD_CPF_CNPJ_DEB_T1, tableCIPDetails.PAT_CD_CPF_CNPJ_DEB_T2))) AS BIGINT))
    AND TRYCAST(RTRIM(LTRIM(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DEB,tableCIPDetails.PAT_CD_CPF_CNPJ_DEB_T1, tableCIPDetails.PAT_CD_CPF_CNPJ_DEB_T2))) AS BIGINT) 
        <> TRYCAST(RTRIM(LTRIM(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DST, tableCIPDetails.PAT_CD_CPF_CNPJ_CRD,tableCIPDetails.PAT_CD_CPF_CNPJ_CRD_T1, tableCIPDetails.PAT_CD_CPF_CNPJ_CRD_T2))) AS BIGINT)
GROUP BY id
),
MAIN_rec AS (
SELECT 
    TRYCAST(RTRIM(LTRIM(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DST, tableCIPDetails.PAT_CD_CPF_CNPJ_CRD,tableCIPDetails.PAT_CD_CPF_CNPJ_CRD_T1, tableCIPDetails.PAT_CD_CPF_CNPJ_CRD_T2))) AS BIGINT)
    AS id,
    SUM(tableCIPMain.PAL_VL_LANC) AS valor,
    COUNT(tableCIPMain.PAL_VL_LANC) AS trans
FROM STRDWPRO_ACC.CMC_PAL_PAG_LAN AS tableCIPMain
    INNER JOIN 	STRDWPRO_ACC.CMC_PAT_PAG_TRANSF AS tableCIPDetails
    ON tableCIPMain.PAL_CD_NUOP = tableCIPDetails.PAL_CD_NUOP AND tableCIPMain.PAL_CD_MSG = tableCIPDetails.PAT_CD_MSG
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
    AND	COALESCE(tableCIPDetails.PAT_CD_TP_PES_DST, tableCIPDetails.PAT_CD_TP_PES_CRD) = 'J' 
    --AND COALESCE(tableCIPDetails.PAT_CD_TP_PES_REM, tableCIPDetails.PAT_CD_TP_PES_DEB) = 'J'
    AND EXISTS(SELECT 1 FROM ID_list WHERE ID_list.id = TRYCAST(RTRIM(LTRIM(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DST, tableCIPDetails.PAT_CD_CPF_CNPJ_CRD,tableCIPDetails.PAT_CD_CPF_CNPJ_CRD_T1, tableCIPDetails.PAT_CD_CPF_CNPJ_CRD_T2))) AS BIGINT))
    AND TRYCAST(RTRIM(LTRIM(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DEB,tableCIPDetails.PAT_CD_CPF_CNPJ_DEB_T1, tableCIPDetails.PAT_CD_CPF_CNPJ_DEB_T2))) AS BIGINT) 
        <> TRYCAST(RTRIM(LTRIM(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DST, tableCIPDetails.PAT_CD_CPF_CNPJ_CRD,tableCIPDetails.PAT_CD_CPF_CNPJ_CRD_T1, tableCIPDetails.PAT_CD_CPF_CNPJ_CRD_T2))) AS BIGINT)
GROUP BY id
),
MAIN_self AS (
SELECT 
    TRYCAST(RTRIM(LTRIM(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DST, tableCIPDetails.PAT_CD_CPF_CNPJ_CRD,tableCIPDetails.PAT_CD_CPF_CNPJ_CRD_T1, tableCIPDetails.PAT_CD_CPF_CNPJ_CRD_T2))) AS BIGINT)
    AS id,
    SUM(tableCIPMain.PAL_VL_LANC) AS valor,
    COUNT(tableCIPMain.PAL_VL_LANC) AS trans
FROM STRDWPRO_ACC.CMC_PAL_PAG_LAN AS tableCIPMain
    INNER JOIN 	STRDWPRO_ACC.CMC_PAT_PAG_TRANSF AS tableCIPDetails
    ON tableCIPMain.PAL_CD_NUOP = tableCIPDetails.PAL_CD_NUOP AND tableCIPMain.PAL_CD_MSG = tableCIPDetails.PAT_CD_MSG
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
    AND	COALESCE(tableCIPDetails.PAT_CD_TP_PES_DST, tableCIPDetails.PAT_CD_TP_PES_CRD) = 'J' 
    --AND COALESCE(tableCIPDetails.PAT_CD_TP_PES_REM, tableCIPDetails.PAT_CD_TP_PES_DEB) = 'J'
    AND EXISTS(SELECT 1 FROM ID_list WHERE ID_list.id = TRYCAST(RTRIM(LTRIM(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DST, tableCIPDetails.PAT_CD_CPF_CNPJ_CRD,tableCIPDetails.PAT_CD_CPF_CNPJ_CRD_T1, tableCIPDetails.PAT_CD_CPF_CNPJ_CRD_T2))) AS BIGINT))
    AND TRYCAST(RTRIM(LTRIM(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DEB,tableCIPDetails.PAT_CD_CPF_CNPJ_DEB_T1, tableCIPDetails.PAT_CD_CPF_CNPJ_DEB_T2))) AS BIGINT) 
        = TRYCAST(RTRIM(LTRIM(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DST, tableCIPDetails.PAT_CD_CPF_CNPJ_CRD,tableCIPDetails.PAT_CD_CPF_CNPJ_CRD_T1, tableCIPDetails.PAT_CD_CPF_CNPJ_CRD_T2))) AS BIGINT)
GROUP BY id
)
SELECT
    @WEEK as week,
    2 AS tipo,
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