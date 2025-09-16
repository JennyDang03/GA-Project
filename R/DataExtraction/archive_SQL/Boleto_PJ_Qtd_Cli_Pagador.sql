-- Boletos Pagos por PJ - Qtd Clientes unicos
SELECT 
	dia
	,COUNT(DISTINCT cd_pag) AS qtd_cli_pag_PJ
	,muni_pagador
FROM (
  SELECT 
  	DTE_DT_BASE_MVTO AS dia
  	,LEFT(PEB_CD_CNPJ_CPF_PAGDR, 8) AS cd_pag
  	,REC.MUN_CD AS muni_pagador
  	FROM CIPDWPRO_ACC.CIPTB_BBC_BOLETO_BAIXA_DIARIA_CIP as boleto
  	LEFT JOIN PIXDWPRO_ACC.SPIVW_PES_PESSOA_FIS_JUR as REC
    ON (LEFT(boleto.PEB_CD_CNPJ_CPF_PAGDR, 8) = LEFT(REC.PEG_CD_CPF_CNPJ14, 8)       )
  	WHERE  
      DTE_DT_BASE_MVTO >= DATE '@selectedDateSTART'   AND DTE_DT_BASE_MVTO <  DATE '@selectedDateEND'
    	AND PEB_CD_TP_PESSOA_PAGDR = 'J' -- boletos pagos por PJ
    	AND COALESCE(PEB_CD_TP_PESSOA_BENFCRIO_FINL , PEB_CD_TP_PESSOA_BENFCRIO_OR ) = 'J' -- boletos emitidos por PJ
    	AND REC.TPE_CD = 2 -- PJ da tabela de cadastro da Receita
) as dados
GROUP BY 
	dia
	,muni_pagador

