-- Debit_card_rec
WITH tab_debit AS (
SELECT 
  --DEB_CD_CNPJ8_CPF11_PONTOVENDA AS id, -- this is cnpj8, not cnpj14
  DEB_CD_CNPJ_CPF_PONTOVENDA AS id,
  DEB_CD_NULIQUID,
  max(DEB_DT_DT_PGTO) AS dia,
  max(DEB_VL_VLR_PGTO) AS vlr, --tomando apenas um valor por NULIQUIDS (devem ser todos iguais)
	DEB_CD_TP_PESSOA_PONTOVENDA as tipo,
	DEB_MUN_IBGE_CD_PONTOVENDA as id_municipio,
	max(cast(CASE WHEN DEB_CD_COD_OCOR =  'N/A' then 999 ELSE DEB_CD_COD_OCOR END AS int)) as max_ocor  
FROM EACDWPRO_ACC.EAC_DEB_DEBITO_TGT
WHERE 
    DEB_DT_DT_PGTO >= '@selectedDateSTART' AND DEB_DT_DT_PGTO < '@selectedDateEND' AND
    DEB_CD_SIT_SOLICTCLIQUID IN (5,6,7) AND 
    DEB_CD_SITUACAO_RESPINSTDOMCL NOT IN ('002', '006') --filtros CIP (notar 6 e não 3 no respinstdomcl)
GROUP BY
    DEB_CD_NULIQUID, 
    DEB_MUN_IBGE_CD_PONTOVENDA,  
    DEB_CD_TP_PESSOA_PONTOVENDA,
    --DEB_CD_CNPJ8_CPF11_PONTOVENDA
    DEB_CD_CNPJ_CPF_PONTOVENDA
       having max_ocor<=1
)
SELECT
  @WEEK AS week,       
  id_municipio,
  tipo,
  COUNT(DISTINCT id) AS receivers,
  SUM(vlr) AS valor
FROM tab_debit
GROUP BY id_municipio, tipo;

