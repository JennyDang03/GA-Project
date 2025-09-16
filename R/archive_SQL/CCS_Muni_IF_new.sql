-- Accounts Opening, closing, and stock (CCS) por Municipio x IF 

-- # Variables: week, muni_cd, tipo, bank, opening, stock, closing

-- IDEA: 
-- Make list of people that had accounts during the start of Pix. Then, create dummy 1/0


WITH Opening AS (
  SELECT
    COUNT(DISTINCT REL_CD_CPF_CNPJ) AS opening,
    PAR_CD_CNPJ_PAR AS bank, 
    RFB.MUN_CD AS muni_cd,
    REL_CD_TIPO_PESSOA AS tipo,
    @WEEK AS week
  FROM CCSDWPRO_ACC.CCSTB_FRE_FATO_RELACIONAMENTO AS CCS
  LEFT JOIN PIXDWPRO_ACC.SPIVW_PES_PESSOA_FIS_JUR AS RFB ---------> NEED TO CHANGE THIS!!!
    ON CCS.REL_CD_CPF_CNPJ = TO_NUMBER(RFB.PEG_CD_CPF_CNPJ14)
  WHERE 
    REL_DT_INICIO >= TO_DATE('@selectedDateSTART', 'YYYY-MM-DD') AND REL_DT_INICIO < TO_DATE('@selectedDateEND', 'YYYY-MM-DD')
  GROUP BY RFB.MUN_CD, PAR_CD_CNPJ_PAR, REL_CD_TIPO_PESSOA
),
Closing AS (
  SELECT
    COUNT(DISTINCT REL_CD_CPF_CNPJ) AS closing,
    PAR_CD_CNPJ_PAR AS bank, 
    RFB.MUN_CD AS muni_cd,
    REL_CD_TIPO_PESSOA AS tipo,
    @WEEK AS week
  FROM CCSDWPRO_ACC.CCSTB_FRE_FATO_RELACIONAMENTO AS CCS
  LEFT JOIN PIXDWPRO_ACC.SPIVW_PES_PESSOA_FIS_JUR AS RFB ---------> NEED TO CHANGE THIS!!!
    ON CCS.REL_CD_CPF_CNPJ = TO_NUMBER(RFB.PEG_CD_CPF_CNPJ14)
  WHERE 
    REL_DT_FIM >= TO_DATE('@selectedDateSTART', 'YYYY-MM-DD') AND REL_DT_FIM < TO_DATE('@selectedDateEND', 'YYYY-MM-DD')
  GROUP BY RFB.MUN_CD, PAR_CD_CNPJ_PAR, REL_CD_TIPO_PESSOA
),
Stock AS (
  SELECT	
    COUNT(DISTINCT REL_CD_CPF_CNPJ) as stock,
    PAR_CD_CNPJ_PAR AS bank, 
    RFB.MUN_CD as muni_cd,
    REL_CD_TIPO_PESSOA AS tipo,
    @WEEK AS week
  FROM	CCSDWPRO_ACC.CCSTB_FRE_FATO_RELACIONAMENTO as CCS
  LEFT JOIN PIXDWPRO_ACC.SPIVW_PES_PESSOA_FIS_JUR as RFB ---------> NEED TO CHANGE THIS!!!
    ON CCS.REL_CD_CPF_CNPJ = TO_NUMBER(RFB.PEG_CD_CPF_CNPJ14)
  WHERE 
    REL_DT_INICIO < TO_DATE('@selectedDateEND','YYYY-MM-DD')
    AND (REL_DT_FIM >= TO_DATE('@selectedDateEND','YYYY-MM-DD')  OR REL_ST_STATUS_RELACIONAMENTO = 'A')
  GROUP BY RFB.MUN_CD,PAR_CD_CNPJ_PAR, REL_CD_TIPO_PESSOA;
)
SELECT *
FROM Opening
FULL JOIN Closing ON (Opening.bank = Closing.bank AND Opening.muni_cd = Closing.muni_cd AND Opening.tipo = Closing.tipo AND Opening.week = Closing.week)
FULL JOIN Stock ON (Opening.bank = Stock.bank AND Opening.muni_cd = Stock.muni_cd AND Opening.tipo = Stock.tipo AND Opening.week = Stock.week)

