******************************************
* Importa_Cadastro_IF.do
* Input: 
*	1) "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\CaracConglomerados_202112.csv"
*   2) "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\CaracInstUnicad_202203.csv"
*	3) "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/NBranchesCongLevel.csv"

* Output:
*   1) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\Cadastro_IF.dta"

* Variables: cnpj8_if tipo_inst digbank belong_cong controle macroseg_if segmento_if macroseg_if_txt public big_bank porte_cong_prud cong_id

* The goal: To classigy each bank and conglomerates into tipo_inst and other categories. 

* To do: 

******************************************

* Paths and clears 
clear all
set more off, permanently 
set matsize 2000
set emptycells drop

global log "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\log\"
global dta "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\"
global output "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Output\"
global origdata "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\OrigData\"

* ADO
 adopath ++ "//sbcdf060/depep01$/ADO"
 adopath ++ "//sbcdf060/depep01$/ado-776e"

capture log close
log using "$log\Importa_Cadastro_IF.log", replace 

*** Pega dados de Dez de 2021
cap drop temp
import delimited "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\CaracConglomerados_202112.csv", encoding(ISO-8859-2) clear

drop dt_ini_ativ

save "$dta\Cadastro_IF.dta", replace 

import delimited "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\CaracInstUnicad_202203.csv", encoding(ISO-8859-2) clear
drop if cnpj8_if == .
 

merge 1:1 cnpj8_if using "$dta\Cadastro_IF.dta"
replace belong_cong = 0 if _merge == 1
replace controle = controle_unicad if _merge == 1
drop controle_unicad
replace macroseg_if = macroseg_unicad if _merge == 1
drop macroseg_unicad

gen digbank = 0
replace digbank = 1 if cod_cong_prud == "C0084693"
replace digbank = 1 if cod_cong_prud == "C0084813"
replace digbank = 1 if cod_cong_prud == "C0084820"
replace digbank = 1 if cod_cong_prud == "C0085702"
replace digbank = 1 if cod_cong_prud == "C0085317"
replace digbank = 1 if cod_cong_prud == "C0084655"

replace digbank = 1 if cod_cong_prud == "C0084844"
replace digbank = 1 if cod_cong_prud == "C0083694"
replace digbank = 1 if cod_cong_prud == "C0080422"
replace digbank = 1 if cod_cong_prud == "C0080996"
replace digbank = 1 if cod_cong_prud == "C0080903"

replace digbank = 1 if segmento_if == 43 | segmento_unicad == 43
replace digbank = 1 if segmento_if == 44 | segmento_unicad == 44

* Gera id de conglomerado
* igual ao cnpj8 quando é IF isolada 
tostring cnpj8_if, gen(temp)
replace cod_cong_prud = temp if belong_cong == 0
tab belong_cong
help encode
encode cod_cong_prud, gen(cong_id)
drop temp cod_cong_prud
drop time_id
drop _merge

save "$dta\Cadastro_IF.dta", replace 

import delimited "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/NBranchesCongLevel.csv", encoding(ISO-8859-2) clear
ren cnpj cnpj8_if
merge 1:1 cnpj8_if using "$dta\Cadastro_IF.dta", keep(2 3) nogenerate


*** Tipologia ad-hoc de bancos 
ren macroseg_if macroseg_if_txt
gen public = controle  <= 2
*gen big_bank = porte_cong_prud == "S1"
gen big_bank = number_branches ~= . & number_branches > 1000
tab big_bank porte_cong_prud
bysort cong_id: egen temp = max(big_bank)
replace big_bank = 1 if temp == 1
cap drop temp 

gen tipo_inst = 1 	  if macroseg_if_txt == "b1" & public == 1 // bancos comerciais Publicos - Federais ou estaduais 
replace tipo_inst = 2 if big_bank == 1 & macroseg_if_txt == "b1" & public == 0 // bancos comerciais grandes Privados
*replace tipo_inst = 3 if macroseg_if_txt == "n4"  // instituições de pagamentos
replace tipo_inst = 4 if macroseg_if_txt == "b3"  // cooperativas de credito
replace tipo_inst = 5 if digbank == 1 |  macroseg_if_txt == "n4"   // bancos digitais ou IPs 
replace tipo_inst = 6 if tipo_inst == .   // o resto: b1 não-grande e não-digital, n1, b2, etc

save "$dta\Cadastro_IF.dta", replace 
***********************************
codebook
sum *
tab belong_cong
tab controle
tab macroseg_cong_prud
tab macroseg_if
tab digbank
tab tipo_inst

log close


