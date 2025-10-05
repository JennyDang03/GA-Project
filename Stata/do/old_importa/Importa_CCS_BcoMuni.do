******************************************
*Importa_CCS_BcoMuni.do
* Input: 
*	0) origdata "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results"
*   1) "$origdata\CCS_Muni_IF_PF_estoque`year'`m'.csv"

* Output:
*	1) "$dta\CCS_muni_banco_PF.dta"

* Variables: IF qtd muni_cd time_id

* The goal: to download and create a base with stock of bank accounts per Bank x Muni_cd x Month

* To do: 

******************************************

clear all
set more off, permanently 

set emptycells drop

global log "\\sbcdf176\PIX_Matheus$\Stata\log"
global dta "\\sbcdf176\PIX_Matheus$\Stata\dta"
global output "\\sbcdf176\PIX_Matheus$\Output"
global origdata "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results"

* ADO 
adopath ++ "D:\ADO"
 adopath ++ "//sbcdf060/depep01$/ADO"
 adopath ++ "//sbcdf060/depep01$/ado-776e"

capture log close
log using "$log\Importa_CCS_BcoMuni.log", replace 

forvalues year = 2018/2022 {
	local meses  01 02 03 04 05 06 07 08 09 10 11 12
	foreach m of local meses {
			dis `year' `m'
			import delimited "$origdata\CCS_Muni_IF_PF_estoque`year'`m'.csv", clear
			gen time_id = `year'`m'
			save "$dta\temp_`year'`m'.dta",replace 
	}
}

use "$dta\temp_201801.dta", clear 
forvalues year = 2018/2022 {
	local meses  01 02 03 04 05 06 07 08 09 10 11 12
	foreach m of local meses {
		if  ~(`year'==2018 & `m'<=01) {
			dis `year' `m'
			append using "$dta\temp_`year'`m'.dta"
			erase  "$dta\temp_`year'`m'.dta"
		}
	}
}

compress
drop if muni_cd<=0
drop if muni_cd == .
ren instituicao IF
drop if IF == .

unique IF muni_cd time_id

save "$dta\CCS_muni_banco_PF.dta", replace

