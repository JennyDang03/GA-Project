* Esse codigo importa flag com pertencimento do individuo a 3 grupos:
*  1) CadUnico (mas não bolsa família)
*  2) ExtraCad
*  3) Bolsa Familia 
* Do universo de recipientes do auxilio emergencial de abril de 2021
* Os dados de input são gerados pelo cpf_mes_Aux.R
* Input:   $origdata\cpf_bolsa_familia.csv
* Output: "$dta\cpf_bolsa_familia.dta"

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
log using "$log\Importa_individuos_bolsa_familia.log", replace 

import delimited "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\cpf_bolsa_familia.csv", clear 
drop if id == .

encode no_grupo_pgto , generate(grupo)
tab grupo

drop no_grupo_pgto

duplicates drop
duplicates drop id, force
 
save "\\sbcdf176\PIX_Matheus$\Stata\dta\cpf_bolsa_familia.dta", replace


log close

