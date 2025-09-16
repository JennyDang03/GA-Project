-- Credito_Muni_PJ.sql

-- Novas operacoes de PJ Por Municipio 

-- Variables: time_id, id_municipio, new_users, new_users_if, new_users_cg, valor, valor_ativo, users, qtd, banks, valor_cartao, users_cartao, qtd_cartao

SELECT  
	@TIME_ID AS time_id,
	municipio.MUN_CD_IBGE AS id_municipio,
-- Everything
	sum(case when fato.FOC_ST_CLIENTE_NOVO = 'S' THEN 1 ELSE 0 END) AS new_users, 
	sum(case when fato.FOC_ST_CLIENTE_NOVO_IF = 'S' THEN 1 ELSE 0 END) AS new_users_if, 
	sum(case when fato.FOC_ST_CLIENTE_NOVO_CG = 'S' THEN 1 ELSE 0 END) AS new_users_cg,
	sum(fato.FOC_VL_CONTRATADO) AS valor,
	sum(fato.FOC_VL_CARTEIRA_ATIVA) AS valor_ativo,
	count(distinct fato.CLI_CD) AS users,
	count(fato.FOC_CD_HASH_OPER) AS qtd,
	count(distinct fato.INF_CD) AS banks,
-- Cartao
  sum(case when mod_Desig.MDR_CD = 5  THEN fato.FOC_VL_CARTEIRA_ATIVA ELSE 0 END) AS valor_cartao,
	count(distinct case when mod_Desig.MDR_CD = 5 THEN fato.CLI_CD ELSE NULL END) AS users_cartao,
	count(case when mod_Desig.MDR_CD = 5 THEN fato.FOC_CD_HASH_OPER ELSE NULL END) AS qtd_cartao
FROM SCRDWPRO_ACC.SCRTB_FOC_FATO_OPERACAO_CREDITO AS fato
LEFT JOIN SCRDWPRO_ACC.SCRTB_MDC_MOD_NATUREZA_TIPO_CLIENTE AS mod_Desig
ON fato.DMO_CD = mod_Desig.DMO_CD AND fato.TPC_CD = mod_Desig.TPC_CD AND fato.NOP_CD = mod_Desig.NOP_CD AND floor(fato.ORE_CD/100) = mod_Desig.TOR_CD 
LEFT OUTER JOIN 
    SCRDWPRO_ACC.SCRTB_HCL_HISTORICO_CLIENTE AS hcl ON 
		fato.CLI_CD = hcl.CLI_CD AND 
		floor(fato.MEE_CD_MES / 100) = hcl.ANE_CD_ANO
LEFT JOIN GGGDWPRO_ACC.GGGTB_MUN_MUNICIPIO AS municipio
ON hcl.HCL_CD_MUNICIPIO = municipio.MUN_CD_RECEITA
WHERE 
	fato.TPC_CD = 2 AND -- PJ
	fato.NOP_CD NOT IN (4,16,32,33) AND 
 	mod_Desig.MDR_CD IN (5,15,16,17,18,19,20) AND 
	fato.MEE_CD_MES = @selectedDate  AND	--- Integer. Like 202101. 
  --fato.FOC_DT_CONTRATO >= '@selectedDateSTART' AND fato.FOC_DT_CONTRATO < '@selectedDateEND' AND
	fato.DMO_CD < 1400 AND
	fato.FOC_VL_CARTEIRA_ATIVA > 0  AND
  fato.FOC_ST_CLIENTE_TIPO_IF = 'N' AND  -- exclui repasses de centrais/bancos cooperativos para cooperativas singulares
	fato.ORE_CD >= 0 AND fato.ORE_CD <= 199
  AND fato.FOC_ST_OP_NOVA ='S' 
GROUP BY
	municipio.MUN_CD_IBGE


