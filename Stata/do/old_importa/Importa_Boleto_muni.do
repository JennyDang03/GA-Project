******************************************
* Os dados de input são gerados pelo BoletosMuni.R
*Importa_Boleto_muni.do
* Input: 
*   1) "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\Boleto_MUNI_PF_`year'`m'.csv"
*   2) "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\Boleto_MUNI_PJ_`year'`m'.csv"

* Output:
*   1) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\Boleto_week_muni.dta"

* Variables: week muni_cd valor_boleto_deb valor_boleto_dinheiro qtd_boleto_deb qtd_boleto_dinheiro valor_boleto valor_boleto_eletronico valor_boleto_ATM valor_boleto_age valor_boleto_corban qtd_boleto qtd_boleto_eletronico qtd_boleto_ATM qtd_boleto_age qtd_boleto_corban

* The goal: * Importa dados dos Boletos  por Municipio por dia, considerando tipo de pagador 
* Agrupa por semana e grava

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
log using "$log\Importa_Boleto_Muni.log", replace 


*** Importa dados de Boleto POR MUNICIPIO DO PAGADOR PF e PJ
cap drop temp

forvalues year = 2018/2022{
	local meses  01 02 03 04 05 06 07 08 09 10 11 12
	foreach m of local meses {
		if  ~(`year'==2018 & `m'<11) & ~((`year'==2022 & `m'>6)) { qtd_paga valor_pago muni_pagador meio_pag canal_pag
		di `year' `m'
		import delimited "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\Boleto_MUNI_PF_`year'`m'.csv", encoding(ISO-8859-2) clear 
			if  ~(`year'==2018 & `m'==11) {
				append using temp
			}
		save temp, replace 	
		import delimited "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\Boleto_MUNI_PJ_`year'`m'.csv", encoding(ISO-8859-2) clear 
		append using temp
		save temp, replace 			
		}
	}  // fim loop dos meses
} // Fim loop dos anos 

replace dia = subinstr(dia,"-","",.)
gen dia_stata = daily(dia,"YMD")
format %td dia_stata

gen week = wofd(dia_stata)
format %tw week

rename muni_pagador muni_cd

cap erase tempboletoPF.dta
save tempboletoPF 

collapse (sum) qtd_paga valor_pago  , by(week muni_cd meio_pag canal_pag)


* Muito missing para o meio de pagamento
tab meio_pag
* 1 --> Dinheiro
* 2 --> debito em conta

tab canal_pag
* 1	Agências - Postos tradicionais
* 2	Terminal de Auto-atendimento
* 3	Internet (home / office banking)
* 5	Correspondente bancário
* 6	Central de atendimento (Call Center)
* 7	Arquivo Eletrônico
* 8	DDA

preserve 
	collapse (sum) qtd_paga valor_pago  , by(canal_pag)
	list 
restore 

* Salva para agregar de outra forma mais adiante 
cap erase tempboleto.dta
save tempboleto 
* Agrega sem o meio de pagamento 
collapse (sum) qtd_paga valor_pago  , by(week muni_cd canal_pag)

gen valor_boleto_eletronico = valor_pago if canal_pag == 3 | canal_pag == 7 | canal_pag == 8
gen qtd_boleto_eletronico = qtd_paga if canal_pag == 3 | canal_pag == 7 | canal_pag == 8

gen valor_boleto_ATM = valor_pago if    canal_pag == 2
gen qtd_boleto_ATM = qtd_paga if    canal_pag == 2

gen valor_boleto_age = valor_pago if   canal_pag == 1
gen qtd_boleto_age = qtd_paga if   canal_pag == 1 

gen valor_boleto_corban  = valor_pago if canal_pag == 5 
gen qtd_boleto_corban  = qtd_paga if canal_pag == 5 

rename valor_pago valor_boleto
rename qtd_paga qtd_boleto

collapse (sum) valor_boleto* qtd_boleto* , by(week muni_cd)

* Elimina semans incompletas
* ALTERAR QUANDO ATUALIZAR DATAS
*drop if week == 3059 | week == 3241
drop if muni_cd<=0 

save "$dta\Boleto_week_muni", replace 
erase temp.dta

use tempboleto , clear 

collapse (sum) qtd_paga valor_pago  , by(week muni_cd meio_pag)

gen valor_boleto_deb = valor_pago if meio_pag == 2 
gen qtd_boleto_deb = qtd_paga if meio_pag == 2 

gen valor_boleto_dinheiro = valor_pago if   meio_pag == 1 
gen qtd_boleto_dinheiro = qtd_paga if   meio_pag == 1 

collapse (sum) valor_boleto* qtd_boleto* , by(week muni_cd)

merge m:m week muni_cd using "$dta\Boleto_week_muni", nogen 

foreach var in valor_boleto_deb valor_boleto_dinheiro qtd_boleto_deb qtd_boleto_dinheiro valor_boleto valor_boleto_eletronico valor_boleto_ATM valor_boleto_age valor_boleto_corban qtd_boleto qtd_boleto_eletronico qtd_boleto_ATM qtd_boleto_age qtd_boleto_corban {
	replace `var'=0 if missing(`var')
}

save "$dta\Boleto_week_muni", replace 

cap erase temp.dta
cap erase tempboleto.dta


***********************************
codebook

sum *

tab week

log close
