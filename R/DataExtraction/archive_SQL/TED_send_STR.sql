
WITH TED_send_STR_PJ AS (
    SELECT
		cadastroReceitaDEB.CODIGO_DO_MUNICIPIO_NO_BCBASE as id_municipio_receita,
		2 as tipo, 
		SUM(tableSTRMain.[HIL_VL]) AS valor_send,
		COUNT(tableSTRMain.[HIL_VL]) AS trans_send,
		COUNT(DISTINCT cadastroReceitaDEB.CODIGO_CNPJ_14) AS senders
	FROM [STR].[dbo].[STR_HIL_HIST_LAN] AS tableSTRMain
	INNER JOIN 
	[STR].[dbo].[STR_LIF_LAN_IF] AS tableSTRDetails
	ON	tableSTRMain.HIL_CD_NU_OPER = tableSTRDetails.LIF_CD_NUM_OPER
	INNER JOIN [BCBASE_DP].[bcb].[PES_PESSOA_JURIDICA] AS cadastroReceitaDEB
	ON tableSTRDetails.[LIF_CD_CNPJCPFDEB1] COLLATE DATABASE_DEFAULT = cadastroReceitaDEB.CODIGO_CNPJ_14	
	--ON TO_NUMBER(tableSTRDetails.[LIF_CD_CNPJCPFDEB1]) = TO_NUMBER(cadastroReceitaDEB.CODIGO_CNPJ_14)
	WHERE 
		HIL_DT_MOVTO >= '@selectedDateSTART' AND HIL_DT_MOVTO < '@selectedDateEND'
		AND [LIF_CD_TP_PES_DEB] = 'J'
	GROUP BY cadastroReceitaDEB.CODIGO_DO_MUNICIPIO_NO_BCBASE
),
TED_send_STR_PF AS (
    SELECT
		cadastroReceitaDEB.CODIGO_DO_MUNICIPIO_NO_BCBASE as id_municipio_receita,
		1 as tipo, 
		SUM(tableSTRMain.[HIL_VL]) AS valor_send,
		COUNT(tableSTRMain.[HIL_VL]) AS trans_send,
		COUNT(DISTINCT cadastroReceitaDEB.CODIGO_CPF) AS senders
	FROM [STR].[dbo].[STR_HIL_HIST_LAN] AS tableSTRMain
	INNER JOIN 
	[STR].[dbo].[STR_LIF_LAN_IF] AS tableSTRDetails
	ON	tableSTRMain.HIL_CD_NU_OPER = tableSTRDetails.LIF_CD_NUM_OPER
	INNER JOIN [BCBASE_DP].[bcb].[PES_PESSOA_FISICA] AS cadastroReceitaDEB
	ON tableSTRDetails.[LIF_CD_CNPJCPFDEB1] COLLATE DATABASE_DEFAULT = cadastroReceitaDEB.CODIGO_CPF	
	--ON TO_NUMBER(tableSTRDetails.[LIF_CD_CNPJCPFDEB1]) = TO_NUMBER(cadastroReceitaDEB.CODIGO_CPF)	
	WHERE 
		HIL_DT_MOVTO >= '@selectedDateSTART' AND HIL_DT_MOVTO < '@selectedDateEND'
		AND [LIF_CD_TP_PES_DEB] = 'F'
	GROUP BY cadastroReceitaDEB.CODIGO_DO_MUNICIPIO_NO_BCBASE
)
SELECT 
    @WEEK AS week,
    id_municipio_receita,
    tipo,
    valor_send,
    trans_send,
    senders
FROM (SELECT * FROM TED_send_STR_PJ
      UNION ALL
      SELECT * FROM TED_send_STR_PF) AS Combined;
