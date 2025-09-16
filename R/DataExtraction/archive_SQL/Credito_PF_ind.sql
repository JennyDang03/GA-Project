-- Who are the banks cnpjs? CGF_CD [CHAR(8) LATIN], CGP_CD [CHAR(8) LATIN], CCJ_CD [CHAR(14) LATIN],


-- Credito_PF_ind
WITH ID_list AS (
  SELECT
    RFB.PEF_CD_CPF as id,
    RFB.MUN_CD as id_municipio
  FROM 
    DL_DEPEP_ESTABILIDADE_FINANCEIRA.Random_sample_Pix_Fin_Rev as RNDSAMPLE
    INNER JOIN BCBDWPRO_ACC.PESTB_PEF_PESSOA_FISICA AS RFB 
    ON RNDSAMPLE.CPF_CD = RFB.PEF_CD_CPF
)
SELECT  
    ID_list.id AS id,
	ID_list.id_municipio AS id_municipio,
	@TIME_ID AS time_id,
	sum(case when fato.FOC_ST_CLIENTE_NOVO in ('S', 's') then 1 else 0 end) new_users, 
	sum(case when fato.FOC_ST_CLIENTE_NOVO_IF in ('S', 's') then 1 else 0 end) new_users_if, 
	sum(case when fato.FOC_ST_CLIENTE_NOVO_CG in ('S', 's') then 1 else 0 end) new_users_cg,
	sum(case when fato.FOC_ST_OP_NOVA in ('S', 's') then 1 else 0 end) new_operation,
-- how to count the number of banks/conglomerates operating? We need the bank ID
	sum(fato.FOC_VL_CARTEIRA_ATIVA) valor,
	count(fato.FOC_CD_HASH_OPER) qtd,
-- Credito Consignado
	sum(case when mod_Desig.MDR_CD = 1  then fato.FOC_VL_CARTEIRA_ATIVA else 0 end) valor_consignado,	
	count(case when mod_Desig.MDR_CD = 1 then fato.FOC_CD_HASH_OPER else NULL end) qtd_consignado,
-- Emprestimo Pessoal
	sum(case when mod_Desig.MDR_CD = 2  then fato.FOC_VL_CARTEIRA_ATIVA else 0 end) valor_emp_pessoal,	
	count(case when mod_Desig.MDR_CD = 2 then fato.FOC_CD_HASH_OPER else NULL end) qtd_emp_pessoal,
-- Veiculo
	sum(case when mod_Desig.MDR_CD = 3  then fato.FOC_VL_CARTEIRA_ATIVA else 0 end) valor_veiculo,
	count(case when mod_Desig.MDR_CD = 3 then fato.FOC_CD_HASH_OPER else NULL end) qtd_veiculo,
-- Imobiliario
	sum(case when mod_Desig.MDR_CD = 4  then fato.FOC_VL_CARTEIRA_ATIVA else 0 end) valor_imob,
	count(case when mod_Desig.MDR_CD = 4 then fato.FOC_CD_HASH_OPER else NULL end) qtd_imob,
-- Cartao
	sum(case when mod_Desig.MDR_CD = 5  then fato.FOC_VL_CARTEIRA_ATIVA else 0 end) valor_cartao,
	count(case when mod_Desig.MDR_CD = 5 then fato.FOC_CD_HASH_OPER else NULL end) qtd_cartao,
-- Rural
	sum(case when mod_Desig.MDR_CD = 6  then fato.FOC_VL_CARTEIRA_ATIVA else 0 end) valor_rural,
	count(case when mod_Desig.MDR_CD = 6 then fato.FOC_CD_HASH_OPER else NULL end) qtd_rural,
-- Outros
	sum(case when mod_Desig.MDR_CD = 7  then fato.FOC_VL_CARTEIRA_ATIVA else 0 end) valor_outros,
	count(case when mod_Desig.MDR_CD = 7 then fato.FOC_CD_HASH_OPER else NULL end) qtd_outros
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
    INNER JOIN ID_list ON fato.CLI_CD = ID_list.id
WHERE 
	 fato.TPC_CD = 1 and -- pessoa fisica com CPF
	 mod_Desig.MDR_CD in (1,2,3,4,5,6,7) and 
	 fato.NOP_CD not in (4,16,32,33) and 
--	 hif.SIT_ID not in (4, 7, 8, 10) and 
	 fato.MEE_CD_MES = @selectedDate  and	
	 fato.DMO_CD < 1400 and
	 fato.FOC_VL_CARTEIRA_ATIVA > 0  and
     fato.FOC_ST_CLIENTE_TIPO_IF = 'N' and  -- exclui repasses de centrais/bancos cooperativos para cooperativas singulares
	 fato.ORE_CD >= 0 and fato.ORE_CD <= 199 
   	 --and fato.FOC_ST_OP_NOVA in ('S', 's') 
GROUP BY
    ID_list.id

