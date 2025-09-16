-- TED_SITRAF_B2B 
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
    AND	COALESCE(tableCIPDetails.PAT_CD_TP_PES_DST, tableCIPDetails.PAT_CD_TP_PES_CRD) = '@TIPO_REC' 
    AND COALESCE(tableCIPDetails.PAT_CD_TP_PES_REM, tableCIPDetails.PAT_CD_TP_PES_DEB) = '@TIPO_SEND'
	)
WITH DATA PRIMARY INDEX (ID_SEND, ID_REC) ON COMMIT PRESERVE ROWS;
--Elapsed time = 00:25:51.367 


