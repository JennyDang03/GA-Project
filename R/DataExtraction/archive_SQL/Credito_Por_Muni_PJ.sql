-- Novas operacoes de PJ Por Municipio 
SELECT  
	fato.MEE_CD_MES ANO_MES, 
	municipio.MUN_CD_IBGE MUNICIPIO,
	count(fato.FOC_CD_HASH_OPER) as QTD_OP,
 	count(distinct fato.CLI_CD)   as QTD_CLI_TOTA,
	sum(fato.FOC_VL_CARTEIRA_ATIVA) as CREDITO_TOTAL,
-- Capital de Giro
	sum(case when mod_Desig.MDR_CD = 15 then fato.FOC_VL_CARTEIRA_ATIVA else 0 end) VOLUME_WC,	
	count(distinct case when mod_Desig.MDR_CD = 15 then fato.CLI_CD else NULL end) QTD_CLI_WC,
	count(case when mod_Desig.MDR_CD = 15 then fato.FOC_CD_HASH_OPER else NULL end) QTD_OP_WC,
-- Investimento
	sum(case when mod_Desig.MDR_CD = 16 then fato.FOC_VL_CARTEIRA_ATIVA else 0 end) VOLUME_INVEST,	
	count(distinct case when mod_Desig.MDR_CD = 16 then fato.CLI_CD else NULL end) QTD_CLI_INVEST,
	count(case when mod_Desig.MDR_CD = 16 then fato.FOC_CD_HASH_OPER else NULL end) QTD_OP_INVEST,
-- Cheque Especial e Conta Garantida
	sum(case when mod_Desig.MDR_CD = 17 then fato.FOC_VL_CARTEIRA_ATIVA else 0 end) VOLUME_DESC_OVDRAFT,	
	count(distinct case when mod_Desig.MDR_CD = 17 then fato.CLI_CD else NULL end) QTD_CLI_DESC_OVDRAFT,
	count(case when mod_Desig.MDR_CD = 17 then fato.FOC_CD_HASH_OPER else NULL end) QTD_OP_DESC_OVDRAFT,
-- Desconto de recebiveis em geral
	sum(case when mod_Desig.MDR_CD = 18 then fato.FOC_VL_CARTEIRA_ATIVA else 0 end) VOLUME_DESC_REB,	
	count(distinct case when mod_Desig.MDR_CD = 18 then fato.CLI_CD else NULL end) QTD_CLI_DESC_REB,
	count(case when mod_Desig.MDR_CD = 18 then fato.FOC_CD_HASH_OPER else NULL end) QTD_OP_DESC_REB,
-- COMEX
	sum(case when mod_Desig.MDR_CD = 19 then fato.FOC_VL_CARTEIRA_ATIVA else 0 end) VOLUME_COMEX,	
	count(distinct case when mod_Desig.MDR_CD = 19 then fato.CLI_CD else NULL end) QTD_CLI_COMEX,
	count(case when mod_Desig.MDR_CD = 19 then fato.FOC_CD_HASH_OPER else NULL end) QTD_OP_COMEX,
-- OUTROS
	sum(case when mod_Desig.MDR_CD = 20 then fato.FOC_VL_CARTEIRA_ATIVA else 0 end) VOLUME_OUTROS,	
	count(distinct case when mod_Desig.MDR_CD = 20 then fato.CLI_CD else NULL end) QTD_CLI_OUTROS,
	count(case when mod_Desig.MDR_CD = 20 then fato.FOC_CD_HASH_OPER else NULL end) QTD_OP_OUTROS


FROM SCRDWPRO_ACC.SCRTB_FOC_FATO_OPERACAO_CREDITO fato
LEFT JOIN SCRDWPRO_ACC.SCRTB_MDC_MOD_NATUREZA_TIPO_CLIENTE mod_Desig
ON fato.DMO_CD = mod_Desig.DMO_CD and fato.TPC_CD = mod_Desig.TPC_CD and fato.NOP_CD = mod_Desig.NOP_CD and floor(fato.ORE_CD/100) = mod_Desig.TOR_CD 
left outer join 
     SCRDWPRO_ACC.SCRTB_HCL_HISTORICO_CLIENTE as hcl on 
		 fato.CLI_CD = hcl.CLI_CD and 
		 floor(fato.MEE_CD_MES / 100) = hcl.ANE_CD_ANO
left join GGGDWPRO_ACC.GGGTB_MUN_MUNICIPIO municipio
on hcl.HCL_CD_MUNICIPIO = municipio.MUN_CD_RECEITA

WHERE 
	 fato.TPC_CD = 2 and -- PJ
	 fato.NOP_CD not in (4,16,32,33) and 
 	 mod_Desig.MDR_CD in (15,16,17,18,19,20) and 
	 fato.MEE_CD_MES = @selectedDate  and	
	 fato.DMO_CD < 1400 and
	 fato.FOC_VL_CARTEIRA_ATIVA > 0  and
   fato.FOC_ST_CLIENTE_TIPO_IF = 'N' and  -- exclui repasses de centrais/bancos cooperativos para cooperativas singulares
	 fato.ORE_CD >= 0 and fato.ORE_CD <= 199 and
   fato.FOC_ST_OP_NOVA in ('S', 's') 

GROUP BY
	fato.MEE_CD_MES, 
	municipio.MUN_CD_IBGE
	