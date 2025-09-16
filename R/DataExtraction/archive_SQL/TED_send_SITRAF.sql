-- TED por municipio RECEBEDOR = PAGADOR, dia e tipo de pessoa, RECEBEDOR
WITH TED_send_SITRAF_PJ AS (
    SELECT 
        cadastroReceitaDEB.CODIGO_DO_MUNICIPIO_NO_BCBASE AS id_municipio_receita,
        2 as tipo,
        SUM(tableCIPMain.[PAL_VL_LANC]) AS valor_send,
		COUNT(tableCIPMain.[PAL_VL_LANC]) AS trans_send,
        COUNT(DISTINCT cadastroReceitaDEB.CODIGO_CNPJ_14) AS senders				
    FROM [CIP].[dbo].[CMC_PAL_PAG_LAN] AS tableCIPMain
        INNER JOIN 	[CIP].[dbo].[CMC_PAT_PAG_TRANSF] AS tableCIPDetails
        ON tableCIPMain.PAL_CD_NUOP = tableCIPDetails.PAL_CD_NUOP AND tableCIPMain.PAL_CD_MSG = tableCIPDetails.PAT_CD_MSG
        INNER JOIN [BCBASE_DP].[bcb].[PES_PESSOA_JURIDICA] AS cadastroReceitaDEB
        ON REPLACE(STR(RTRIM(LTRIM(COALESCE(tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB],tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB_T1], tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB_T2]))), 14, 0), ' ', '0') 
            COLLATE DATABASE_DEFAULT = cadastroReceitaDEB.CODIGO_CNPJ_14	
    WHERE 
        tableCIPMain.PAL_DT_MOVTO >= '@selectedDateSTART' AND tableCIPMain.PAL_DT_MOVTO < '@selectedDateEND'
        AND COALESCE(tableCIPDetails.[PAT_CD_TP_PES_REM], tableCIPDetails.[PAT_CD_TP_PES_DEB]) = 'J'
        AND
            ISNUMERIC(COALESCE(tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB], 
                                tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB_T1], 
                                tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB_T2])) = 1
        AND
            ISNUMERIC(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DST,
                                tableCIPDetails.[PAT_CD_CPF_CNPJ_CRD], 
                                tableCIPDetails.[PAT_CD_CPF_CNPJ_CRD_T1], 
                                tableCIPDetails.[PAT_CD_CPF_CNPJ_CRD_T2])) = 1
        AND	Comp = 1
    GROUP BY  
        cadastroReceitaDEB.CODIGO_DO_MUNICIPIO_NO_BCBASE
),
TED_send_SITRAF_PF AS (
    SELECT 
        cadastroReceitaDEB.CODIGO_DO_MUNICIPIO_NO_BCBASE AS id_municipio_receita,
        1 as tipo,
        SUM(tableCIPMain.[PAL_VL_LANC]) AS valor_send,
		COUNT(tableCIPMain.[PAL_VL_LANC]) AS trans_send,
        COUNT(DISTINCT cadastroReceitaDEB.CODIGO_CPF) AS senders	
    FROM [CIP].[dbo].[CMC_PAL_PAG_LAN] AS tableCIPMain
        INNER JOIN 	[CIP].[dbo].[CMC_PAT_PAG_TRANSF] AS tableCIPDetails
        ON tableCIPMain.PAL_CD_NUOP = tableCIPDetails.PAL_CD_NUOP AND tableCIPMain.PAL_CD_MSG = tableCIPDetails.PAT_CD_MSG
        INNER JOIN [BCBASE_DP].[bcb].[PES_PESSOA_FISICA] AS cadastroReceitaDEB 
     	ON RTRIM(LTRIM(COALESCE(tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB],tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB_T1], tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB_T2]))) 
            COLLATE DATABASE_DEFAULT = cadastroReceitaDEB.CODIGO_CPF
        --ON REPLACE(STR(RTRIM(LTRIM(COALESCE(tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB],tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB_T1], tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB_T2]))), 11, 0), ' ', '0')
        --  COLLATE DATABASE_DEFAULT = cadastroReceitaDEB.CODIGO_CPF -- PF
        -- ON TO_NUMBER(COALESCE(tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB],tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB_T1], tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB_T2])) 
        -- = TO_NUMBER(cadastroReceitaDEB.CODIGO_CPF) -- PF
    WHERE 
        tableCIPMain.PAL_DT_MOVTO >= '@selectedDateSTART' AND tableCIPMain.PAL_DT_MOVTO < '@selectedDateEND'
        AND COALESCE(tableCIPDetails.[PAT_CD_TP_PES_REM], tableCIPDetails.[PAT_CD_TP_PES_DEB]) = 'F'
        AND
            ISNUMERIC(COALESCE(tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB], 
                                tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB_T1], 
                                tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB_T2])) = 1
        AND
            ISNUMERIC(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DST,
                                tableCIPDetails.[PAT_CD_CPF_CNPJ_CRD], 
                                tableCIPDetails.[PAT_CD_CPF_CNPJ_CRD_T1], 
                                tableCIPDetails.[PAT_CD_CPF_CNPJ_CRD_T2])) = 1
        AND	Comp = 1
    GROUP BY  
        cadastroReceitaDEB.CODIGO_DO_MUNICIPIO_NO_BCBASE
)
SELECT 
	@WEEK AS week,
	id_municipio_receita,
	tipo,
	valor_send,
	trans_send,
	senders
FROM (SELECT * FROM TED_send_SITRAF_PJ UNION ALL SELECT * FROM TED_send_SITRAF_PF) AS Combined;


