* Esse codigo importa  os endereços do auximo emergencial de abril de 2021 com identificacao anonimizada
* e faz o match com os index de endereços
* Input: 
* -  $origdata\Enderecos_aux_emerg_com_id_anonimo.csv   (gerados por Enderecos_auxilio.R)
* -  "$dta\Todos_enderecos_aux_emerg_amostra_muni.dta"  (gerados por Importa_todos_end_aux_emerg.do) 
* Output:
*  id_index_address.dta

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
log using "$log\Gera_id_addres_index_match.log", replace 

import delimited "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\Enderecos_aux_emerg_com_id_anonimo.csv", clear
* Has 9,4 million individuals

ren groupfirst_valuepeg_ds_logradour peg_ds_logradouro
ren groupfirst_valuepeg_an_numero  peg_an_numero
ren groupfirst_valuemun_cd mun_cd
ren groupfirst_valuecd_cep cd_cep

merge m:1 peg_ds_logradouro peg_an_numero mun_cd cd_cep using "\\sbcdf176\PIX_Matheus$\Stata\dta\Todos_enderecos_aux_emerg_amostra_muni.dta"
keep if _merge == 3 
drop _merge
drop qtd  peg_ds_logradouro peg_an_numero mun_cd cd_cep

duplicates drop

sum *

save "\\sbcdf176\PIX_Matheus$\Stata\dta\id_index_address.dta", replace

 log close
 
