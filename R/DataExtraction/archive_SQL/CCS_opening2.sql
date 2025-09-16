-- Accounts Opening, closing, and stock (CCS) por Municipio x IF 

-- # Variables: week, muni_cd, tipo, bank, opening, stock, closing

-- IDEA: 
-- Make list of people that had accounts during the start of Pix. Then, create dummy 1/0


-- TROCAR TODOS OS CCS, adicionar tabela certa da receita e fazer por tipo


  SELECT
    COUNT(DISTINCT REL_CD_CPF_CNPJ) AS opening,
    PAR_CD_CNPJ_PAR AS bank, 
    
    RFB_CPF.MUN_CD AS muni_cd1,
    
    REL_CD_TIPO_PESSOA AS tipo,
    @WEEK AS week
  FROM CCSDWPRO_ACC.CCSTB_FRE_FATO_RELACIONAMENTO AS CCS
  
  
  LEFT JOIN RECEITA_CPF AS RFB_CPF ---------> NEED TO CHANGE THIS!!!
    ON (CCS.REL_CD_CPF_CNPJ = TO_NUMBER(RFB.PEG_CD_CPF_CNPJ14) AND CCS.REL_CD_TIPO_PESSOA = 1)
    
    
    
  LEFT JOIN RECEITA_CNPJ AS RFB_CNPJ ---------> NEED TO CHANGE THIS!!!
    ON (CCS.REL_CD_CPF_CNPJ = TO_NUMBER(RFB.PEG_CD_CPF_CNPJ14) AND CCS.REL_CD_TIPO_PESSOA = 2)
    
    
    
    
  LEFT JOIN PIXDWPRO_ACC.SPIVW_PES_PESSOA_FIS_JUR as CLI_PAG 
    ON (PIX.PES_NU_CPF_CNPJ_PAGADOR = CLI_PAG.PEG_CD_CPF_CNPJ14 AND PIX.TPP_CD_TIPO_PESSOA_PAGADOR = CLI_PAG.TPE_CD)
    
  WHERE 
    REL_DT_INICIO >= TO_DATE('@selectedDateSTART', 'YYYY-MM-DD') AND REL_DT_INICIO < TO_DATE('@selectedDateEND', 'YYYY-MM-DD')
  GROUP BY RFB.MUN_CD, PAR_CD_CNPJ_PAR, REL_CD_TIPO_PESSOA
