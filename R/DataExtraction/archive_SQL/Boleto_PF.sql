-- Boletos Pagos por PF
SELECT 
	dia
	,COUNT(*) AS qtd_paga
	--	,COUNT(DISTINCT cd_pag) as qtd_pagadores
	,SUM(vl_baixa) AS valor_pago
	,muni_pagador
	,meio_pag
	,canal_pag
FROM (
SELECT 
	DTE_DT_BASE_MVTO AS dia
	,LEFT(PEB_CD_CNPJ_CPF_PAGDR, 9) AS cd_pag
	,REC.MUN_CD AS muni_pagador
	,BBC_VL_TIT AS vl_nom
	-- Alguns vl de baixa eram trecho cod barras. Valores bilionarios...
	,BBC_VL_BAIXA_EFT_TIT AS vl_baixa_orig
	,CASE WHEN vl_nom > 0 THEN
		CASE WHEN BBC_VL_BAIXA_EFT_TIT / BBC_VL_TIT > 10 THEN BBC_VL_TIT
		ELSE BBC_VL_BAIXA_EFT_TIT END
		ELSE BBC_VL_BAIXA_EFT_TIT END AS vl_baixa	
	,MPB_CD_EFT AS meio_pag
	,CPB_CD_EFT AS canal_pag
	FROM CIPDWPRO_ACC.CIPTB_BBC_BOLETO_BAIXA_DIARIA_CIP as boleto
	LEFT JOIN PIXDWPRO_ACC.SPIVW_PES_PESSOA_FIS_JUR as REC
ON (LEFT(boleto.PEB_CD_CNPJ_CPF_PAGDR, 9) = LEFT(REC.PEG_CD_CPF_CNPJ14, 9)       )
	WHERE  
  DTE_DT_BASE_MVTO >= DATE '@selectedDateSTART'   AND DTE_DT_BASE_MVTO <  DATE '@selectedDateEND'
	AND PEB_CD_TP_PESSOA_PAGDR = 'F' -- boletos pagos por PF
	AND COALESCE(PEB_CD_TP_PESSOA_BENFCRIO_FINL , PEB_CD_TP_PESSOA_BENFCRIO_OR ) = 'J' -- boletos emitidos por PJ
	AND REC.TPE_CD = 1 -- PF da tabela de cadastro da Receita
) as dados
GROUP BY 
	dia
	,muni_pagador
	,meio_pag
	,canal_pag

