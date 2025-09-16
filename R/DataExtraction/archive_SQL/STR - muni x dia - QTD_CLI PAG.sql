-- QTD CLI de TEDs pagadoras intramunicipio por municipio, dia e tipo de pessoa
SELECT
				HIL_DT_MOVTO as dia,
				cadastroReceitaDEB.CODIGO_DO_MUNICIPIO_NO_BCBASE as muni_cd,
	    	[LIF_CD_TP_PES_DEB] as tipo_pessoa_PAG, 
  --- 	[LIF_CD_TP_PES_CRED] as tipo_pessoa_REC, 
				COUNT(DISTINCT cadastroReceitaDEB.CODIGO_CNPJ_14) AS Qtd_Cli_Pag		

	FROM [STR].[dbo].[STR_HIL_HIST_LAN] AS tableSTRMain
	INNER JOIN 
	[STR].[dbo].[STR_LIF_LAN_IF] AS tableSTRDetails
	ON	tableSTRMain.HIL_CD_NU_OPER = tableSTRDetails.LIF_CD_NUM_OPER
	INNER JOIN [BCBASE_DP].[bcb].[PES_PESSOA_JURIDICA] AS cadastroReceitaCRE
	ON tableSTRDetails.[LIF_CD_CNPJCPFCRE1] COLLATE DATABASE_DEFAULT = cadastroReceitaCRE.CODIGO_CNPJ_14	
	INNER JOIN [BCBASE_DP].[bcb].[PES_PESSOA_JURIDICA] AS cadastroReceitaDEB
	ON tableSTRDetails.[LIF_CD_CNPJCPFDEB1] COLLATE DATABASE_DEFAULT = cadastroReceitaDEB.CODIGO_CNPJ_14	
	WHERE 
		year(tableSTRMain.HIL_DT_MOVTO) = @selectedYEAR
		and cadastroReceitaDEB.CODIGO_DO_MUNICIPIO_NO_BCBASE = cadastroReceitaCRE.CODIGO_DO_MUNICIPIO_NO_BCBASE
	GROUP BY 
HIL_DT_MOVTO,
cadastroReceitaDEB.CODIGO_DO_MUNICIPIO_NO_BCBASE,
[LIF_CD_TP_PES_DEB] 
--[LIF_CD_TP_PES_DEB] 		
