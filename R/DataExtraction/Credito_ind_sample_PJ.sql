
WITH ID_list AS (
  SELECT floor(firm_id14/1000000) AS id
  FROM DL_DEPEP_ESTABILIDADE_FINANCEIRA.Random_sample_Pix_Fin_Rev_PJ
)
SELECT
    @TIME_ID AS time_id,
    ID_list.id AS id,
    2 AS tipo,
    fato.INF_CD AS bank,
    SUM(CASE WHEN fato.FOC_ST_CLIENTE_NOVO = 'S' THEN 1 ELSE 0 END) AS new_users, 
	SUM(CASE WHEN fato.FOC_ST_CLIENTE_NOVO_IF = 'S' THEN 1 ELSE 0 END) AS new_users_if, 
	SUM(CASE WHEN fato.FOC_ST_CLIENTE_NOVO_CG = 'S' THEN 1 ELSE 0 END) AS new_users_cg,
	SUM(fato.FOC_VL_CONTRATADO) AS valor,
	SUM(fato.FOC_VL_CARTEIRA_ATIVA) AS valor_ativo,
	COUNT(fato.FOC_CD_HASH_OPER) AS qtd
FROM ID_list 
    LEFT JOIN SCRDWPRO_ACC.SCRTB_FOC_FATO_OPERACAO_CREDITO AS fato
        ON ID_list.id = fato.CLI_CD 
    LEFT JOIN SCRDWPRO_ACC.SCRTB_MDC_MOD_NATUREZA_TIPO_CLIENTE AS mod_Desig
        ON fato.DMO_CD = mod_Desig.DMO_CD AND fato.TPC_CD = mod_Desig.TPC_CD AND fato.NOP_CD = mod_Desig.NOP_CD AND floor(fato.ORE_CD/100) = mod_Desig.TOR_CD
    LEFT JOIN SCRDWPRO_ACC.SCRTB_HCL_HISTORICO_CLIENTE AS hcl 
    ON fato.CLI_CD = hcl.CLI_CD AND floor(fato.MEE_CD_MES / 100) = hcl.ANE_CD_ANO
WHERE 
	fato.TPC_CD = 2 AND -- PJ
	fato.NOP_CD NOT IN (4,16,32,33) AND 
 	mod_Desig.MDR_CD IN (5,15,16,17,18,19,20) AND 
	fato.MEE_CD_MES = @selectedDate  AND	--- Integer. Like 202101. 
	fato.DMO_CD < 1400 AND
	fato.FOC_VL_CARTEIRA_ATIVA > 0  AND
    fato.FOC_ST_CLIENTE_TIPO_IF = 'N' AND  -- exclui repasses de centrais/bancos cooperativos para cooperativas singulares
	fato.ORE_CD >= 0 AND fato.ORE_CD <= 199
    AND fato.FOC_ST_OP_NOVA ='S' 
GROUP BY id, bank