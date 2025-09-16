-- Variables: week, id, id_municipio_receita, tipo, value_send, trans_send, value_rec, trans_rec, value_self, trans_self

WITH ID_list AS (
  SELECT CPF_CD AS id
  FROM DL_DEPEP_ESTABILIDADE_FINANCEIRA.Random_sample_Pix_Fin_Rev
),
MAIN_send AS (
    SELECT 
    	TO_NUMBER(A.PES_NU_CPF_CNPJ_PAGADOR) AS id,
      SUM(A.LAF_VL) AS valor, 
      COUNT(A.LAF_VL) AS trans
    FROM PIXDWPRO_ACC.SPITB_LAF_LANCAMENTO_FATO AS A
    WHERE 
        A.LAF_DT_LIQUIDACAO >= '@selectedDateSTART' 
        AND A.LAF_DT_LIQUIDACAO < '@selectedDateEND'
        AND  A.STA_CD_LIQUIDADA = 'S'
        AND  A.STA_CD_REJEICAO = 'N'
        AND A.PES_NU_CPF_CNPJ_PAGADOR <> A.PES_NU_CPF_CNPJ_RECEBEDOR
        AND A.TPP_CD_TIPO_PESSOA_PAGADOR = 1
        AND (EXISTS(SELECT 1 FROM ID_list WHERE ID_list.id = TO_NUMBER(A.PES_NU_CPF_CNPJ_PAGADOR)))
    GROUP BY id
),
MAIN_rec AS (
    SELECT
    	TO_NUMBER(A.PES_NU_CPF_CNPJ_RECEBEDOR) AS id,
      SUM(A.LAF_VL) AS valor, 
      COUNT(A.LAF_VL) AS trans
    FROM PIXDWPRO_ACC.SPITB_LAF_LANCAMENTO_FATO AS A
    WHERE 
        A.LAF_DT_LIQUIDACAO >= '@selectedDateSTART' 
        AND A.LAF_DT_LIQUIDACAO < '@selectedDateEND'
        AND  A.STA_CD_LIQUIDADA = 'S'
        AND  A.STA_CD_REJEICAO = 'N'
        AND A.PES_NU_CPF_CNPJ_PAGADOR <> A.PES_NU_CPF_CNPJ_RECEBEDOR
        AND A.TPP_CD_TIPO_PESSOA_RECEBEDOR = 1
        AND (EXISTS(SELECT 1 FROM ID_list WHERE ID_list.id = TO_NUMBER(A.PES_NU_CPF_CNPJ_RECEBEDOR)))
    GROUP BY id
),
MAIN_self AS (
    SELECT 
    	TO_NUMBER(A.PES_NU_CPF_CNPJ_PAGADOR) AS id,
      SUM(A.LAF_VL) AS valor, 
      COUNT(A.LAF_VL) AS trans
    FROM PIXDWPRO_ACC.SPITB_LAF_LANCAMENTO_FATO AS A
    WHERE 
        A.LAF_DT_LIQUIDACAO >= '@selectedDateSTART' 
        AND A.LAF_DT_LIQUIDACAO < '@selectedDateEND'
        AND  A.STA_CD_LIQUIDADA = 'S'
        AND  A.STA_CD_REJEICAO = 'N'
        AND A.PES_NU_CPF_CNPJ_PAGADOR = A.PES_NU_CPF_CNPJ_RECEBEDOR
        AND A.TPP_CD_TIPO_PESSOA_PAGADOR = 1
        AND (EXISTS(SELECT 1 FROM ID_list WHERE ID_list.id = TO_NUMBER(A.PES_NU_CPF_CNPJ_PAGADOR)))
    GROUP BY id
)
SELECT
    @WEEK as week,
    1 AS tipo,
    ID_list.id AS id,
    COALESCE(MAIN_send.valor, 0) AS value_send,
    COALESCE(MAIN_send.trans, 0) AS trans_send,
    COALESCE(MAIN_rec.valor, 0) AS value_rec,
    COALESCE(MAIN_rec.trans, 0) AS trans_rec,
    COALESCE(MAIN_self.valor, 0) AS value_self,
    COALESCE(MAIN_self.trans, 0) AS trans_self
FROM ID_list 
LEFT JOIN MAIN_send ON (MAIN_send.id = ID_list.id)
LEFT JOIN MAIN_rec ON (MAIN_rec.id = ID_list.id)
LEFT JOIN MAIN_self ON (MAIN_self.id = ID_list.id)


