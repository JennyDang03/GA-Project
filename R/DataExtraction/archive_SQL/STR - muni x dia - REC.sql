-- TED por municipio RECEBEDOR, dia e tipo de pessoa
SELECT
		HIL_DT_MOVTO as dia,
		cadastroReceita.CODIGO_DO_MUNICIPIO_NO_BCBASE as muni_REC,
		[LIF_CD_TP_PES_CRED] as tipo_pessoa_REC, 
		[LIF_CD_TP_PES_DEB] as tipo_pessoa_PAG, 
		SUM(tableSTRMain.[HIL_VL]) AS TotalPayment,
		COUNT(tableSTRMain.[HIL_VL]) AS QuantityPayment
	FROM [STR].[dbo].[STR_HIL_HIST_LAN] AS tableSTRMain
	INNER JOIN 
	[STR].[dbo].[STR_LIF_LAN_IF] AS tableSTRDetails
	ON	tableSTRMain.HIL_CD_NU_OPER = tableSTRDetails.LIF_CD_NUM_OPER
	INNER JOIN [BCBASE_DP].[bcb].[PES_PESSOA_JURIDICA] AS cadastroReceita
	ON tableSTRDetails.[LIF_CD_CNPJCPFCRE1] COLLATE DATABASE_DEFAULT = cadastroReceita.CODIGO_CNPJ_14	
	WHERE 
		year(tableSTRMain.HIL_DT_MOVTO) = @selectedYEAR
	GROUP BY CODIGO_DO_MUNICIPIO_NO_BCBASE, HIL_DT_MOVTO, [LIF_CD_TP_PES_CRED], [LIF_CD_TP_PES_DEB] 
