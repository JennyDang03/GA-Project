* Esse codigo importa todos os endereços do auxilio emergencial de abril de 2021
* Os dados de input são gerados pelo Enderecos_auxilio.R
* Input:   $origdata\Todos_enderecos_aux_emerg_amostra_muni.csv
* Output: "$dta\Todos_enderecos_aux_emerg_amostra_muni.dta"

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
log using "$log\Importa_todos_end_aux_emerg.log", replace 

import delimited "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\Todos_enderecos_aux_emerg_amostra_muni.csv", clear
* Has 10.8 million addresses
gen index0 = _n

* Error reading the CSV
replace mun_cd = 56119 in 2050459
replace cd_cep = "58238000" in 2050459
drop if _n == 2050460

recast long index0
format %12.0g index0

destring cd_cep, replace force
duplicates drop peg_ds_logradouro peg_an_numero mun_cd cd_cep, force

unique index
unique peg_ds_logradouro peg_an_numero mun_cd cd_cep
 
save "\\sbcdf176\PIX_Matheus$\Stata\dta\Todos_enderecos_aux_emerg_amostra_muni.dta", replace


log close

