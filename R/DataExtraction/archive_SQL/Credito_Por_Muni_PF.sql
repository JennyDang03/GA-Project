-- Novas operacoes de PF Por Municipio 
SELECT  
	fato.MEE_CD_MES ANO_MES, 
	municipio.MUN_CD_IBGE MUNICIPIO,
	count(case when mod_Desig.MDR_CD not in (4, 6)  then fato.FOC_CD_HASH_OPER else NULL end) QTD_OP,
 	count(distinct case when mod_Desig.MDR_CD not in (4,  6) then fato.CLI_CD else NULL end) QTD_CLI_TOTAL,
	sum(case when mod_Desig.MDR_CD not in (4, 6)  then fato.FOC_VL_CARTEIRA_ATIVA else 0 end) VOL_CREDITO_TOTAL,

	sum(case when mod_Desig.MDR_CD = 1  then fato.FOC_VL_CARTEIRA_ATIVA else 0 end) VOL_CONSIGNADO,	
	count(distinct case when mod_Desig.MDR_CD = 1  then fato.CLI_CD else NULL end) QTD_CLI_CONSIGNADO,	

	sum(case when mod_Desig.MDR_CD = 2  then fato.FOC_VL_CARTEIRA_ATIVA else 0 end) VOL_EMPRESTIMO_PESSOAL,	
	count(distinct case when mod_Desig.MDR_CD = 2  then fato.CLI_CD else NULL end) QTD_CLI_EMP_PESSOAL,	
	
	sum(case when mod_Desig.MDR_CD = 3  then fato.FOC_VL_CARTEIRA_ATIVA else 0 end) VOL_VEICULO,
	count(distinct case when mod_Desig.MDR_CD = 3  then fato.CLI_CD else NULL end) QTD_CLI_VEICULO,
	
	sum(case when mod_Desig.MDR_CD = 4  then fato.FOC_VL_CARTEIRA_ATIVA else 0 end) VOL_IMOB,
	count(distinct case when mod_Desig.MDR_CD = 4  then fato.CLI_CD else NULL end) QTD_CLI_IMOB,
	
	sum(case when mod_Desig.MDR_CD = 5  then fato.FOC_VL_CARTEIRA_ATIVA else 0 end) VOL_CARTAO,
	count(distinct case when mod_Desig.MDR_CD = 5  then fato.CLI_CD else NULL end) QTD_CLI_CARTAO,

	sum(case when mod_Desig.MDR_CD = 6  then fato.FOC_VL_CARTEIRA_ATIVA else 0 end) VOL_RURAL,
	count(distinct case when mod_Desig.MDR_CD = 6  then fato.CLI_CD else NULL end) QTD_CLI_RURAL,
	
	sum(case when mod_Desig.MDR_CD = 7  then fato.FOC_VL_CARTEIRA_ATIVA else 0 end) VOL_OUTROS_CRED,
	count(distinct case when mod_Desig.MDR_CD = 7  then fato.CLI_CD else NULL end) QTD_CLI_OUTROS_CRED

FROM SCRDWPRO_ACC.SCRTB_FOC_FATO_OPERACAO_CREDITO fato
LEFT JOIN SCRDWPRO_ACC.SCRTB_MDC_MOD_NATUREZA_TIPO_CLIENTE mod_Desig
ON fato.DMO_CD = mod_Desig.DMO_CD and fato.TPC_CD = mod_Desig.TPC_CD and fato.NOP_CD = mod_Desig.NOP_CD and floor(fato.ORE_CD/100) = mod_Desig.TOR_CD 
left outer join 
     SCRDWPRO_ACC.SCRTB_HIF_HISTORICO_INSTITUICAO_FINANCEIRA as hif on
	     fato.MEE_CD_MES = hif.MEE_CD_MES and
	     fato.HIF_ID = hif.HIF_ID	 
left outer join 
     SCRDWPRO_ACC.SCRTB_HCL_HISTORICO_CLIENTE as hcl on 
		 fato.CLI_CD = hcl.CLI_CD and 
		 floor(fato.MEE_CD_MES / 100) = hcl.ANE_CD_ANO
left join GGGDWPRO_ACC.GGGTB_MUN_MUNICIPIO municipio
on hcl.HCL_CD_MUNICIPIO = municipio.MUN_CD_RECEITA

WHERE 
	 fato.TPC_CD = 1 and -- pessoa fisica com CPF
	 mod_Desig.MDR_CD in (1,2,3,4,5,6,7) and 
	 fato.NOP_CD not in (4,16,32,33) and 
--	 hif.SIT_ID not in (4, 7, 8, 10) and 
	 fato.MEE_CD_MES = @selectedDate  and	
	 fato.DMO_CD < 1400 and
	 fato.FOC_VL_CARTEIRA_ATIVA > 0  and
     fato.FOC_ST_CLIENTE_TIPO_IF = 'N' and  -- exclui repasses de centrais/bancos cooperativos para cooperativas singulares
	 fato.ORE_CD >= 0 and fato.ORE_CD <= 199 and
   	 fato.FOC_ST_OP_NOVA in ('S', 's') 

GROUP BY
	fato.MEE_CD_MES, 
	municipio.MUN_CD_IBGE
