-- Boleto_send

-- boleto user, adoption.

WITH Send_PF AS (
SELECT 
	id_municipio_receita
	,tipo
	,COUNT(DISTINCT id) AS senders
	,COUNT(vl_baixa) AS trans_send
	,SUM(vl_baixa) AS valor_send
FROM (
  SELECT 
	    boleto.DTE_DT_BASE_MVTO AS dia
		,TO_NUMBER(CASE WHEN boleto.PEB_CD_TP_PESSOA_PAGDR = '' THEN NULL
						ELSE LEFT(boleto.PEB_CD_CNPJ_CPF_PAGDR, 11) END) AS id -- the id instead of empty, it is 'N/I', which makes life difficult
	    ,RFB.MUN_CD AS id_municipio_receita
	    ,1 AS tipo
	  	,boleto.BBC_VL_TIT AS vl_nom
	    -- Alguns vl de baixa eram trecho cod barras. Valores bilionarios...
	    ,boleto.BBC_VL_BAIXA_EFT_TIT AS vl_baixa_orig
	    ,CASE WHEN vl_nom > 0 THEN
		      CASE WHEN boleto.BBC_VL_BAIXA_EFT_TIT / boleto.BBC_VL_TIT > 10 THEN boleto.BBC_VL_TIT
		      ELSE boleto.BBC_VL_BAIXA_EFT_TIT END
		    ELSE boleto.BBC_VL_BAIXA_EFT_TIT END AS vl_baixa		
	FROM CIPDWPRO_ACC.CIPTB_BBC_BOLETO_BAIXA_DIARIA_CIP as boleto
	LEFT JOIN BCBDWPRO_ACC.PESTB_PEF_PESSOA_FISICA AS RFB 
	ON id = TO_NUMBER(LEFT(RFB.PEF_CD_CPF,11)) -- PF
	WHERE  
      boleto.DTE_DT_BASE_MVTO >= '@selectedDateSTART' AND boleto.DTE_DT_BASE_MVTO < '@selectedDateEND'
	    AND boleto.PEB_CD_TP_PESSOA_PAGDR = 'F' -- boletos pagos por PF
		AND boleto.PEB_CD_CNPJ_CPF_PAGDR <> 'N/I'
) as dados1
GROUP BY 
	id_municipio_receita, tipo
),
Send_PJ AS (
SELECT 
	id_municipio_receita
	,tipo
	,COUNT(DISTINCT id) AS senders
	,COUNT(vl_baixa) AS trans_send
	,SUM(vl_baixa) AS valor_send
FROM (
  SELECT 
	  	boleto.DTE_DT_BASE_MVTO AS dia
		,TO_NUMBER(CASE WHEN boleto.PEB_CD_TP_PESSOA_PAGDR = '' THEN NULL
						ELSE LEFT(boleto.PEB_CD_CNPJ_CPF_PAGDR, 14) END) AS id -- the id instead of empty, it is 'N/I', which makes life difficult
	  	,RFB.MUN_CD AS id_municipio_receita
	  	,2 AS tipo
	  	,boleto.BBC_VL_TIT AS vl_nom
	    -- Alguns vl de baixa eram trecho cod barras. Valores bilionarios...
	    ,boleto.BBC_VL_BAIXA_EFT_TIT AS vl_baixa_orig
	    ,CASE WHEN vl_nom > 0 THEN
		      CASE WHEN boleto.BBC_VL_BAIXA_EFT_TIT / boleto.BBC_VL_TIT > 10 THEN boleto.BBC_VL_TIT
		      ELSE boleto.BBC_VL_BAIXA_EFT_TIT END
		    ELSE boleto.BBC_VL_BAIXA_EFT_TIT END AS vl_baixa	
  	FROM CIPDWPRO_ACC.CIPTB_BBC_BOLETO_BAIXA_DIARIA_CIP as boleto
	LEFT JOIN BCBDWPRO_ACC.PESTB_PEJ_PESSOA_JURIDICA AS RFB 
	ON id = TO_NUMBER(LEFT(RFB.PEJ_CD_CNPJ14,14))  -- PJ     
  	WHERE  
      boleto.DTE_DT_BASE_MVTO >= '@selectedDateSTART' AND boleto.DTE_DT_BASE_MVTO < '@selectedDateEND'
    	AND boleto.PEB_CD_TP_PESSOA_PAGDR = 'J' -- boletos pagos por PJ
		AND boleto.PEB_CD_CNPJ_CPF_PAGDR <> 'N/I'
) as dados2
GROUP BY 
  id_municipio_receita, tipo
)

SELECT
  @WEEK AS week
  ,id_municipio_receita
  ,tipo
	,senders
	,trans_send
	,valor_send
FROM (SELECT * FROM Send_PF UNION ALL SELECT * FROM Send_PJ) AS Combined;
