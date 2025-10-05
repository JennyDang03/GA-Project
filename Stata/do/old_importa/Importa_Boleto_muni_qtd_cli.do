******************************************
* Os dados de input são gerados pelo BoletosMuni.R
*Importa_Boleto_muni_qtd_cli.do
* Input: 
*   1) "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\Boleto_MUNI_PJ_QTD_CLI_REC_`year'`m'.csv"
*   2) "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\Boleto_MUNI_PJ_QTD_CLI_`year'`m'.csv"
*   3) "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\Boleto_MUNI_PF_QTD_CLI_`year'`m'.csv"

* Output:
*   1) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\Boleto_week_muni_qtd_cli.dta"

* Variables: week muni_cd qtd_cli_pag_pf_boleto qtd_cli_pag_pj_boleto qtd_cli_rec_pj_boleto 

* The goal: * Importa qtd de clientes dos Boletos por Municipio por dia, considerando tipo de pagador 
* Agrupa por semana e grava

* To do: missing Recebedor PF
 
******************************************
* Paths and clears 
clear all
set more off, permanently 
set emptycells drop

global log "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\log\"
global dta "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\"
global output "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Output\"
global origdata "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\OrigData\"

* ADO
 adopath ++ "//sbcdf060/depep01$/ADO"
 adopath ++ "//sbcdf060/depep01$/ado-776e"

capture log close
log using "$log\Importa_Boleto_muni_qtd_cli.log", replace 

* PARTE 1 : Importa dados de Boleto POR MUNICIPIO DO RECEBEDOR PJ
* PARTE 2 : Importa dados de Boleto POR MUNICIPIO DO PAGADOR PJ
* PARTE 3 : Importa dados de Boleto POR MUNICIPIO DO PAGADOR PF

*** 1) Importa dados de Boleto POR MUNICIPIO DO RECEBEDOR PJ
cap drop temp

forvalues year = 2018/2022{
	local meses  01 02 03 04 05 06 07 08 09 10 11 12
	foreach m of local meses {
		if  ~(`year'==2018 & `m'<11)  { 
		di `year' `m'
		import delimited "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\Boleto_MUNI_PJ_QTD_CLI_REC_`year'`m'.csv", encoding(ISO-8859-2) clear 
			if  ~(`year'==2018 & `m'==11) {
				append using temp
			}
		save temp, replace 	

*		import delimited "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\Boleto_MUNI_PJ_QTD_CLI_REC_`year'`m'.csv", encoding(ISO-8859-2) clear 
*		append using temp
*		save temp, replace 			
		}
	}  // fim loop dos meses
} // Fim loop dos anos 

replace dia = subinstr(dia,"-","",.)
gen dia_stata = daily(dia,"YMD")
format %td dia_stata

gen week = wofd(dia_stata)
format %tw week

rename muni_recebedor muni_cd

cap erase tempboletoPF.dta
save tempboletoPF 

collapse (sum) qtd_rec_pj , by(week muni_cd)

rename qtd_rec_pj qtd_cli_rec_pj_boleto

drop if muni_cd<=0 

save "$dta\Boleto_week_muni_qtd_cli", replace 
erase temp.dta

* PARTE 2 : Importa dados de Boleto POR MUNICIPIO DO PAGADOR PJ
cap drop temp

forvalues year = 2018/2022{
	local meses  01 02 03 04 05 06 07 08 09 10 11 12
	foreach m of local meses {
		if  ~(`year'==2018 & `m'<11)   { 
		di `year' `m'
		import delimited "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\Boleto_MUNI_PJ_QTD_CLI_`year'`m'.csv", encoding(ISO-8859-2) clear 
			if  ~(`year'==2018 & `m'==11) {
				append using temp
			}
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

collapse (sum) qtd_cli_pag_pj , by(week muni_cd)

rename qtd_cli_pag_pj qtd_cli_pag_pj_boleto
drop if muni_cd<=0 

merge 1:1 week muni_cd using "$dta\Boleto_week_muni_qtd_cli", nogen 

save "$dta\Boleto_week_muni_qtd_cli", replace 

* PARTE 3 : Importa dados de Boleto POR MUNICIPIO DO PAGADOR PF
cap drop temp

forvalues year = 2018/2022{
	local meses  01 02 03 04 05 06 07 08 09 10 11 12
	foreach m of local meses {
		if  ~(`year'==2018 & `m'<11) { 
		di `year' `m'
		import delimited "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\Boleto_MUNI_PF_QTD_CLI_`year'`m'.csv", encoding(ISO-8859-2) clear 
			if  ~(`year'==2018 & `m'==11) {
				append using temp
			}
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

collapse (sum) qtd_cli_pag_pf , by(week muni_cd)

rename qtd_cli_pag_pf qtd_cli_pag_pf_boleto

drop if muni_cd<=0 

merge 1:1 week muni_cd using "$dta\Boleto_week_muni_qtd_cli", nogen 

**** Parte 4 - Limpeza final 
foreach var in qtd_cli_pag_pf_boleto qtd_cli_pag_pj_boleto qtd_cli_rec_pj_boleto {
	replace `var'=0 if missing(`var')
}

save "$dta\Boleto_week_muni_qtd_cli", replace 

cap erase temp.dta
cap erase tempboleto.dta

***********************************
codebook

sum *

tab week

log close
