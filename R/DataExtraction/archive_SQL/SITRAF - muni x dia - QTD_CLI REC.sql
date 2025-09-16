-- TED por municipio RECEBEDOR = PAGADOR, dia e tipo de pessoa, RECEBEDOR
		SELECT 
				PAL_DT_MOVTO as dia,
	    	cadastroReceitaRec.CODIGO_DO_MUNICIPIO_NO_BCBASE AS muni_cd,
				COALESCE(tableCIPDetails.[PAT_CD_TP_PES_DST], tableCIPDetails.[PAT_CD_TP_PES_CRD]) as tipo_pessoa_REC,
				COUNT(DISTINCT cadastroReceitaRec.CODIGO_CNPJ_14) AS Qtd_Cli_Rec				
			FROM [CIP].[dbo].[CMC_PAL_PAG_LAN] AS tableCIPMain
			INNER JOIN 	[CIP].[dbo].[CMC_PAT_PAG_TRANSF] AS tableCIPDetails
			ON
				tableCIPMain.PAL_CD_NUOP = tableCIPDetails.PAL_CD_NUOP AND
				tableCIPMain.PAL_CD_MSG = tableCIPDetails.PAT_CD_MSG
			INNER JOIN [BCBASE_DP].[bcb].[PES_PESSOA_JURIDICA] AS cadastroReceitaRec
			ON 
			REPLACE(STR(RTRIM(LTRIM(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DST, tableCIPDetails.[PAT_CD_CPF_CNPJ_CRD],tableCIPDetails.[PAT_CD_CPF_CNPJ_CRD_T1], 
				   tableCIPDetails.[PAT_CD_CPF_CNPJ_CRD_T2]))), 14, 0), ' ', '0') 
			COLLATE DATABASE_DEFAULT = cadastroReceitaRec.CODIGO_CNPJ_14	
			INNER JOIN [BCBASE_DP].[bcb].[PES_PESSOA_JURIDICA] AS cadastroReceitaPag
			ON 
			REPLACE(STR(RTRIM(LTRIM(COALESCE(tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB],tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB_T1], 
				   tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB_T2]))), 14, 0), ' ', '0') 
			COLLATE DATABASE_DEFAULT = cadastroReceitaPag.CODIGO_CNPJ_14	
			WHERE 
				  year(tableCIPMain.PAL_DT_MOVTO)=@selectedYEAR
				--AND	COALESCE(tableCIPDetails.[PAT_CD_TP_PES_DST], tableCIPDetails.[PAT_CD_TP_PES_CRD]) = 'J' 
				--AND COALESCE(tableCIPDetails.[PAT_CD_TP_PES_REM], tableCIPDetails.[PAT_CD_TP_PES_DEB]) = 'J'
				AND
					ISNUMERIC(COALESCE(tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB], 
									   tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB_T1], 
									   tableCIPDetails.[PAT_CD_CPF_CNPJ_DEB_T2])) = 1
				AND
					ISNUMERIC(COALESCE(tableCIPDetails.PAT_CD_CPF_CNPJ_DST,
									   tableCIPDetails.[PAT_CD_CPF_CNPJ_CRD], 
									   tableCIPDetails.[PAT_CD_CPF_CNPJ_CRD_T1], 
									   tableCIPDetails.[PAT_CD_CPF_CNPJ_CRD_T2])) = 1
				AND
					Comp = 1
				AND cadastroReceitaRec.CODIGO_DO_MUNICIPIO_NO_BCBASE = cadastroReceitaPag.CODIGO_DO_MUNICIPIO_NO_BCBASE	

		GROUP BY
		PAL_DT_MOVTO,
		cadastroReceitaRec.CODIGO_DO_MUNICIPIO_NO_BCBASE,
		COALESCE(tableCIPDetails.[PAT_CD_TP_PES_DST], tableCIPDetails.[PAT_CD_TP_PES_CRD])

		