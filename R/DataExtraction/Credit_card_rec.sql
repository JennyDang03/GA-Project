-- Credit_card

-- Dados de Cartão de credito
-- Winsorize card. (hard and complicated since big numbers are not necessarily outliers)

-- CRE_CD_CNPJ_CPF_PONTOVENDA [CHAR(14) LATIN Not Null], CRE_CD_CNPJ8_CPF11_PONTOVENDA [CHAR(11) LATIN],
--CRE_CD_TP_PESSOA_PONTOVENDA = 1 PF 2 PARA PJ

with tab AS (
SELECT 
  --CRE_CD_CNPJ8_CPF11_PONTOVENDA AS id,
  CRE_CD_CNPJ_CPF_PONTOVENDA AS id,
  CRE_CD_NULIQUID,
  max(CRE_DT_DTPGTO) AS dia,
  max(CRE_VL_VLR_PGTO) AS vlr, --tomando apenas um valor por NULIQUIDS (devem ser todos iguais)
	CRE_CD_TP_PESSOA_PONTOVENDA as tipo,
	CRE_MUN_IBGE_CD_PONTOVENDA as id_municipio,
	max(cast(CASE WHEN CRE_CD_COD_OCOR =  'N/A' then 999 ELSE CRE_CD_COD_OCOR END AS int)) as max_ocor  
FROM EACDWPRO_ACC.EAC_cre_CREDITO_TGT
WHERE 
    CRE_DT_DTPGTO >= '@selectedDateSTART' AND CRE_DT_DTPGTO < '@selectedDateEND' AND
    CRE_CD_SIT_SOLICTCLIQUID IN (5,6,7) AND 
    CRE_CD_SITUACAO_RESPINSTDOMCL NOT IN ('002', '006') --filtros CIP (notar 6 e não 3 no respinstdomcl)
GROUP BY 
    CRE_CD_NULIQUID, 
    CRE_MUN_IBGE_CD_PONTOVENDA,  
    CRE_CD_TP_PESSOA_PONTOVENDA,
    --CRE_CD_CNPJ8_CPF11_PONTOVENDA 
    CRE_CD_CNPJ_CPF_PONTOVENDA
       having max_ocor<=1
)
SELECT  
  @WEEK AS week,     
  id_municipio,
  tipo,
  COUNT(DISTINCT id) AS receivers,
  SUM(vlr) AS valor
FROM tab
GROUP BY id_municipio, tipo;    
