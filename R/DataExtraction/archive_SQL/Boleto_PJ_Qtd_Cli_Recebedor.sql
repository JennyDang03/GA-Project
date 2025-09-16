-- Boletos Recbidos por PJ - Qtd Clientes unicos
SELECT 
	dia
	,COUNT(DISTINCT cnpj_rec) AS qtd_rec_PJ
	,muni_recebedor
FROM (
  SELECT 
	  	DTE_DT_BASE_MVTO AS dia
	  	,COALESCE(LEFT(PEB_CD_CNPJ_CPF_BENFCRIO_FINL, 8) , LEFT(PEB_CD_CNPJ_CPF_BENFCRIO_OR, 8) ) AS cnpj_rec
	  	,REC.MUN_CD AS muni_recebedor
  	FROM CIPDWPRO_ACC.CIPTB_BBC_BOLETO_BAIXA_DIARIA_CIP as boleto
  	LEFT JOIN PIXDWPRO_ACC.SPIVW_PES_PESSOA_FIS_JUR as REC
    ON COALESCE(LEFT(PEB_CD_CNPJ_CPF_BENFCRIO_FINL, 8) , LEFT(PEB_CD_CNPJ_CPF_BENFCRIO_OR, 8) ) = LEFT(REC.PEG_CD_CPF_CNPJ14, 8)       
  	WHERE  
      DTE_DT_BASE_MVTO >= DATE '@selectedDateSTART'   AND DTE_DT_BASE_MVTO <  DATE '@selectedDateEND'
    	AND COALESCE(PEB_CD_TP_PESSOA_BENFCRIO_FINL , PEB_CD_TP_PESSOA_BENFCRIO_OR ) = 'J' -- boletos emitidos por PJ
    	AND REC.TPE_CD = 2 -- PJ da tabela de cadastro da Receita
) as dados
GROUP BY 
	dia
	,muni_recebedor

