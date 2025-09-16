-- TED_rec_STR
-- TED  por municipio , dia e tipo de pessoa

WITH TED_rec_STR_PJ AS (
	SELECT
		cadastroReceitaCRE.CODIGO_DO_MUNICIPIO_NO_BCBASE as id_municipio_receita,
		2 as tipo, 
		SUM(tableSTRMain.[HIL_VL]) AS valor_rec,
		COUNT(tableSTRMain.[HIL_VL]) AS trans_rec,
		COUNT(DISTINCT cadastroReceitaCRE.CODIGO_CNPJ_14) AS receivers
	FROM [STR].[dbo].STR_HIL_HIST_LAN AS tableSTRMain
	INNER JOIN 
	[STR].[dbo].STR_LIF_LAN_IF AS tableSTRDetails
	ON	tableSTRMain.HIL_CD_NU_OPER = tableSTRDetails.LIF_CD_NUM_OPER
	INNER JOIN [BCBASE_DP].[bcb].PES_PESSOA_JURIDICA AS cadastroReceitaCRE
	ON tableSTRDetails.LIF_CD_CNPJCPFCRE1 COLLATE DATABASE_DEFAULT = cadastroReceitaCRE.CODIGO_CNPJ_14	
	--ON TO_NUMBER(tableSTRDetails.LIF_CD_CNPJCPFCRE1) = TO_NUMBER(cadastroReceitaCRE.CODIGO_CNPJ_14)
	WHERE 
		HIL_DT_MOVTO >= '@selectedDateSTART' AND HIL_DT_MOVTO < '@selectedDateEND'
		AND LIF_CD_TP_PES_CRED = 'J'
	GROUP BY cadastroReceitaCRE.CODIGO_DO_MUNICIPIO_NO_BCBASE
),
TED_rec_STR_PF AS (
	SELECT
		cadastroReceitaCRE.CODIGO_DO_MUNICIPIO_NO_BCBASE as id_municipio_receita,
		1 as tipo, 
		SUM(tableSTRMain.HIL_VL) AS valor_rec,
		COUNT(tableSTRMain.HIL_VL) AS trans_rec,
		COUNT(DISTINCT cadastroReceitaCRE.CODIGO_CPF) AS receivers
	FROM [STR].[dbo].STR_HIL_HIST_LAN AS tableSTRMain
	INNER JOIN 
	[STR].[dbo].STR_LIF_LAN_IF AS tableSTRDetails
	ON	tableSTRMain.HIL_CD_NU_OPER = tableSTRDetails.LIF_CD_NUM_OPER
	INNER JOIN [BCBASE_DP].[bcb].PES_PESSOA_FISICA AS cadastroReceitaCRE
	ON tableSTRDetails.LIF_CD_CNPJCPFCRE1 COLLATE DATABASE_DEFAULT = cadastroReceitaCRE.CODIGO_CPF	
	--ON TO_NUMBER(tableSTRDetails.LIF_CD_CNPJCPFCRE1) = TO_NUMBER(cadastroReceitaCRE.CODIGO_CPF)
	WHERE 
		HIL_DT_MOVTO >= '@selectedDateSTART' AND HIL_DT_MOVTO < '@selectedDateEND'
		AND LIF_CD_TP_PES_CRED = 'F'
	GROUP BY cadastroReceitaCRE.CODIGO_DO_MUNICIPIO_NO_BCBASE
)
SELECT 
	@WEEK AS week,
	id_municipio_receita,
	tipo,
	valor_rec,
	trans_rec,
	receivers
FROM (SELECT * FROM TED_rec_STR_PJ UNION ALL SELECT * FROM TED_rec_STR_PF) AS Combined;
