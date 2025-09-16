-- Boleto_rec

-- boleto user, adoption.

WITH Rec_PF AS (
SELECT 
	id_municipio_receita
	,tipo
	,COUNT(DISTINCT id) AS receivers
	,COUNT(vl_baixa) AS trans_rec
	,SUM(vl_baixa) AS valor_rec
FROM (
  SELECT 
	    boleto.DTE_DT_BASE_MVTO AS dia
		,TO_NUMBER(CASE WHEN boleto.PEB_CD_TP_PESSOA_BENFCRIO_FINL = '' THEN LEFT(boleto.PEB_CD_CNPJ_CPF_BENFCRIO_OR, 11)
			ELSE LEFT(boleto.PEB_CD_CNPJ_CPF_BENFCRIO_FINL, 11) END) AS id -- the id instead of empty, it is 'N/I', which makes life difficult
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
	ON id = TO_NUMBER(LEFT(RFB.PEF_CD_CPF, 11))	
	WHERE  
        boleto.DTE_DT_BASE_MVTO >= '@selectedDateSTART' AND boleto.DTE_DT_BASE_MVTO < '@selectedDateEND'
		--DTE_DT_BASE_MVTO >= '2019-11-28' AND DTE_DT_BASE_MVTO < '2019-11-29'
		AND COALESCE(boleto.PEB_CD_TP_PESSOA_BENFCRIO_FINL, boleto.PEB_CD_TP_PESSOA_BENFCRIO_OR) = 'F' -- boletos emitidos por PF
) as dados1
GROUP BY 
	id_municipio_receita, tipo
),
Rec_PJ AS (
SELECT 
	id_municipio_receita
	,tipo
	,COUNT(DISTINCT id) AS receivers
	,COUNT(vl_baixa) AS trans_rec
	,SUM(vl_baixa) AS valor_rec
FROM (
  SELECT 
	  	boleto.DTE_DT_BASE_MVTO AS dia
		,TO_NUMBER(CASE WHEN boleto.PEB_CD_TP_PESSOA_BENFCRIO_FINL = '' THEN LEFT(boleto.PEB_CD_CNPJ_CPF_BENFCRIO_OR, 14)
			ELSE LEFT(boleto.PEB_CD_CNPJ_CPF_BENFCRIO_FINL, 14) END) AS id -- the id instead of empty, it is 'N/I', which makes life difficult
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
	ON id = TO_NUMBER(LEFT(RFB.PEJ_CD_CNPJ14, 14)) -- PJ
	WHERE  
        boleto.DTE_DT_BASE_MVTO >= '@selectedDateSTART' AND boleto.DTE_DT_BASE_MVTO < '@selectedDateEND'
    	AND COALESCE(boleto.PEB_CD_TP_PESSOA_BENFCRIO_FINL, boleto.PEB_CD_TP_PESSOA_BENFCRIO_OR) = 'J' -- boletos emitidos por PJ
) as dados2
GROUP BY 
  id_municipio_receita, tipo
)

SELECT
  @WEEK AS week
  ,id_municipio_receita
  ,tipo
	,receivers
	,trans_rec
	,valor_rec
FROM (SELECT * FROM Rec_PF UNION ALL SELECT * FROM Rec_PJ) AS Combined;
