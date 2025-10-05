* Esse codigo importa o cadastro do bolsa família
* Os dados de input são gerados pelo Familias_BF.R
* Input:   $origdata\Familias_BF_202010.csv
* Output: "$dta\Cad_bolsa_familia.dta"

clear all
set more off, permanently 

set emptycells drop

global log "\\sbcdf176\PIX_Matheus$\Stata\log"
global dta "\\sbcdf176\PIX_Matheus$\Stata\dta"
global output "\\sbcdf176\PIX_Matheus$\Output"
global origdata "\\sbcdf176\PIX_Matheus$\DadosOriginais"

* ADO 
adopath ++ "D:\ADO"
 adopath ++ "//sbcdf060/depep01$/ADO"
 adopath ++ "//sbcdf060/depep01$/ado-776e"

capture log close
log using "$log\Importa_cad_bolsa_familia.log", replace 

import delimited "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\Familias_BF_202010.csv", clear
rename cpf id
drop if id == .

*duplicates drop
 

replace dat_atual_fam = subinstr(dat_atual_fam,"-","",.)
gen dia_atual_fam = daily(dat_atual_fam,"YMD")
format %td dia_atual_fam

replace dat_cadastramento_fam = subinstr(dat_cadastramento_fam,"-","",.)
gen dia_cadastramento_fam = daily(dat_cadastramento_fam,"YMD")
format %td dia_cadastramento_fam

replace dta_atual_memb = subinstr(dta_atual_memb,"-","",.)
gen dia_atual_memb = daily(dta_atual_memb,"YMD")
format %td dia_atual_memb

replace dta_nasc_pessoa = subinstr(dta_nasc_pessoa,"-","",.)
gen dia_nasc_pessoa = daily(dta_nasc_pessoa,"YMD")
format %td dia_nasc_pessoa

drop dat_atual_fam dat_cadastramento_fam dta_atual_memb dta_nasc_pessoa

*gen m_opening_date = monthly(rel_dt_inicio,"YM")
*format %tm m_opening_date
 
sum * 

tab cod_parentesco_rf_pessoa
tab cod_raca_cor_pessoa
tab cod_sabe_ler_escrever_memb
tab cod_sexo_pessoa
 
save "\\sbcdf176\PIX_Matheus$\Stata\dta\Cad_bolsa_familia.dta", replace

log close

