-- Novas operacoes de PF Por Municipio 

-- CLI_CD - CPF OR CNPJ (Acho que é cnpj8)
-- CCJ_CD - CNPJ14 do cliente tipo PJ
-- INF_CD - CNPJ da Instituição Financeira (IF/BANK)
-- FOC_ST_CLIENTE_NOVO = S/N FLAG FOR NEW IN SCR
-- FOC_ST_CLIENTE_NOVO_IF = S/N  new on the IF (BANK)
-- FOC_ST_CLIENTE_NOVO_CG = S/N  new on the Conglomerado
-- FOC_VL_CARTEIRA_ATIVA = Valor da carteira ativa
-- FOC_DT_CONTRATO = Data de contratação da operação - Podemos fazer por semana. 
-- FOC_VL_LIMITE_CREDITO = Valor do limite de crédito. 
-- Limite fica no DMO_CD > 1900 (global sendo 1901)

-- Variables: time_id, id_municipio, new_users, new_users_if, new_users_cg, valor, valor_ativo, users, qtd, banks, limite_de_credito, valor_w, users_w, qtd_w, valor_cartao, users_cartao 

SELECT  
	@TIME_ID AS time_id,
	municipio.MUN_CD_IBGE AS id_municipio,
-- Everything
	sum(case when fato.FOC_ST_CLIENTE_NOVO = 'S' THEN 1 ELSE 0 END) AS new_users, 
	sum(case when fato.FOC_ST_CLIENTE_NOVO_IF = 'S' THEN 1 ELSE 0 END) AS new_users_if, 
	sum(case when fato.FOC_ST_CLIENTE_NOVO_CG = 'S' THEN 1 ELSE 0 END) AS new_users_cg,
	--sum(fato.FOC_VL_CARTEIRA_ATIVA) AS valor_ativo, 
	sum(fato.FOC_VL_CONTRATADO) AS valor,
	sum(fato.FOC_VL_CARTEIRA_ATIVA) AS valor_ativo,
	count(distinct fato.CLI_CD) AS users,
	count(fato.FOC_CD_HASH_OPER) AS qtd,
	count(distinct fato.INF_CD) AS banks,
-- Why NOT IN 4,6? 4 is mortgage and 6 is rural (those are outliers)
	sum(case when mod_Desig.MDR_CD NOT IN (4, 6)  THEN fato.FOC_VL_CARTEIRA_ATIVA ELSE 0 END) AS valor_w,
	count(distinct case when mod_Desig.MDR_CD NOT IN (4,  6) THEN fato.CLI_CD ELSE NULL END) AS users_w,
	count(case when mod_Desig.MDR_CD NOT IN (4, 6)  THEN fato.FOC_CD_HASH_OPER ELSE NULL END) AS qtd_w,
-- Cartao
  sum(case when mod_Desig.MDR_CD = 5  THEN fato.FOC_VL_CARTEIRA_ATIVA ELSE 0 END) AS valor_cartao,
	count(distinct case when mod_Desig.MDR_CD = 5 THEN fato.CLI_CD ELSE NULL END) AS users_cartao,
	count(case when mod_Desig.MDR_CD = 5 THEN fato.FOC_CD_HASH_OPER ELSE NULL END) AS qtd_cartao
FROM SCRDWPRO_ACC.SCRTB_FOC_FATO_OPERACAO_CREDITO AS fato
LEFT JOIN SCRDWPRO_ACC.SCRTB_MDC_MOD_NATUREZA_TIPO_CLIENTE AS mod_Desig
ON fato.DMO_CD = mod_Desig.DMO_CD AND fato.TPC_CD = mod_Desig.TPC_CD AND fato.NOP_CD = mod_Desig.NOP_CD AND floor(fato.ORE_CD/100) = mod_Desig.TOR_CD 
--left outer join SCRDWPRO_ACC.SCRTB_HIF_HISTORICO_INSTITUICAO_FINANCEIRA as hif on fato.MEE_CD_MES = hif.MEE_CD_MES and fato.HIF_ID = hif.HIF_ID	 
LEFT OUTER JOIN 
     SCRDWPRO_ACC.SCRTB_HCL_HISTORICO_CLIENTE AS hcl ON 
		 fato.CLI_CD = hcl.CLI_CD AND 
		 floor(fato.MEE_CD_MES / 100) = hcl.ANE_CD_ANO
LEFT JOIN GGGDWPRO_ACC.GGGTB_MUN_MUNICIPIO AS municipio
ON hcl.HCL_CD_MUNICIPIO = municipio.MUN_CD_RECEITA
WHERE 
	 fato.TPC_CD = 1 AND -- Tipo de cliente. pessoa fisica com CPF = 1. CNPJ = 2
	 mod_Desig.MDR_CD IN (1,2,3,4,5,6,7) AND -- Modalidade
	 fato.NOP_CD NOT IN (4,16,32,33) AND -- Natureza da Operação. (Eu acho que exclui repasses)
	 fato.MEE_CD_MES = @selectedDate  AND	-------- Mês e Ano da Data-base em Referência. It is an integer. Like 202001, YYYYMM
	 --fato.FOC_DT_CONTRATO >= '@selectedDateSTART' AND fato.FOC_DT_CONTRATO < '@selectedDateEND' AND 
	 fato.DMO_CD < 1400 AND -- Submodalidade. 
	 fato.FOC_VL_CARTEIRA_ATIVA > 0 AND -- Valor carteira ativa.
   fato.FOC_ST_CLIENTE_TIPO_IF = 'N' AND  -- Cliente é instituição financeira? exclui repasses de centrais/bancos cooperativos para cooperativas singulares
	 fato.ORE_CD >= 0 AND fato.ORE_CD <= 199 -- Origem dos recursos
   AND fato.FOC_ST_OP_NOVA = 'S' -- Only new operations. Needs to be here otherwise there will be too much data.
GROUP BY
	municipio.MUN_CD_IBGE;

