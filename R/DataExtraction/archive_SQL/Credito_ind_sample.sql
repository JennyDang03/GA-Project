WITH ID_list_PF AS (
  SELECT
    TO_NUMBER(RFB.PEF_CD_CPF) as id,   -- RFB.PEF_CD_CPF is a string. 
    RFB.MUN_CD as id_municipio_receita,
    1 as tipo
  FROM 
    DL_DEPEP_ESTABILIDADE_FINANCEIRA.Random_sample_Pix_Fin_Rev as RNDSAMPLE
    INNER JOIN BCBDWPRO_ACC.PESTB_PEF_PESSOA_FISICA AS RFB 
    ON RNDSAMPLE.CPF_CD = RFB.PEF_CD_CPF
),
ID_list_PJ AS (
  SELECT
    firm_id as id,
    RFB.MUN_CD as id_municipio_receita,
    2 as tipo
  FROM 
    DL_DEPEP_ESTABILIDADE_FINANCEIRA.Random_sample_Pix_Fin_Rev_PJ AS RNDSAMPLE
    INNER JOIN BCBDWPRO_ACC.PESTB_PEJ_PESSOA_JURIDICA AS RFB
    ON RNDSAMPLE.firm_id = TO_NUMBER(RFB.PEJ_CD_CNPJ14)
),
ID_list AS (
  SELECT * FROM ID_list_PF UNION ALL SELECT * FROM ID_list_PJ
)
SELECT
    @TIME_ID AS time_id,
    ID_list.id AS id,
    ID_list.tipo AS tipo,
    ID_list.id_municipio_receita AS id_municipio_receita,
    TO_NUMBER(fato.CCJ_CD) AS bank, ----??????
    fato.FOC_CD_HASH_OPER AS operation_id,
    mod_Desig.MDR_CD AS mod_Desig,
    fato.FOC_VL_CARTEIRA_ATIVA AS valor,
    (CASE WHEN fato.FOC_ST_CLIENTE_NOVO IN ('S','s') THEN 1
        WHEN fato.FOC_ST_CLIENTE_NOVO IN ('N','n') THEN 0 
        ELSE NULL END) AS new_client,
    (CASE WHEN fato.FOC_ST_OP_NOVA IN ('S','s') THEN 1
        WHEN fato.FOC_ST_OP_NOVA IN ('N','n') THEN 0 
        ELSE NULL END) AS new_operation,
    (CASE WHEN fato.FOC_ST_CLIENTE_NOVO_IF IN ('S','s') THEN 1
        WHEN fato.FOC_ST_CLIENTE_NOVO_IF IN ('N','n') THEN 0 
        ELSE NULL END) AS new_bank,
    (CASE WHEN fato.FOC_ST_CLIENTE_NOVO_CG IN ('S','s') THEN 1
        WHEN fato.FOC_ST_CLIENTE_NOVO_CG IN ('N','n') THEN 0 
        ELSE NULL END) AS new_cg
FROM ID_list 
    LEFT JOIN SCRDWPRO_ACC.SCRTB_FOC_FATO_OPERACAO_CREDITO AS fato
        ON ID_list.id = fato.CLI_CD AND ID_list.tipo = TO_NUMBER(fato.TPC_CD)
    LEFT JOIN SCRDWPRO_ACC.SCRTB_MDC_MOD_NATUREZA_TIPO_CLIENTE mod_Desig
        ON fato.DMO_CD = mod_Desig.DMO_CD AND fato.TPC_CD = mod_Desig.TPC_CD AND fato.NOP_CD = mod_Desig.NOP_CD AND floor(fato.ORE_CD/100) = mod_Desig.TOR_CD
    LEFT JOIN SCRDWPRO_ACC.SCRTB_HCL_HISTORICO_CLIENTE as hcl on 
		 fato.CLI_CD = hcl.CLI_CD AND 
		 floor(fato.MEE_CD_MES / 100) = hcl.ANE_CD_ANO
WHERE 
    fato.NOP_CD NOT IN (4,16,32,33) AND 
	mod_Desig.MDR_CD IN (1,2,3,4,5,6,7,15,16,17,18,19,20) AND 
    fato.MEE_CD_MES = @selectedDate AND -------- It is an integer. Maybe ano_mes like 202001
	fato.DMO_CD < 1400 AND
	fato.FOC_VL_CARTEIRA_ATIVA > 0  AND
    fato.FOC_ST_CLIENTE_TIPO_IF = 'N' AND  -- exclui repasses de centrais/bancos cooperativos para cooperativas singulares
	fato.ORE_CD >= 0 AND fato.ORE_CD <= 199
