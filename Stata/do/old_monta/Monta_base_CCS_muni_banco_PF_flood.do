******************************************
*Monta_base_CCS_muni_banco_PF_flood
* Input: 
*   1) "$dta\CCS_muni_banco_PF.dta"
*   2) "$dta\flood_monthly_2020_2022.dta"
*	3) "$dta\flood_monthly_2019_2020.dta"
*	4) "$dta\Cadastro_IF.dta"

* Output:
*   1) "$dta\CCS_muni_banco_PF_flood.dta"
*   2) "$dta\CCS_muni_banco_PF_flood_collapsed.dta"
*	3) "$dta\CCS_muni_banco_PF_flood_beforePIX.dta"
*	4) "$dta\CCS_muni_banco_PF_flood_collapsed_beforePIX.dta"


* Variables: IF qtd muni_cd time_id id_mun_bank log_qtd date_flood tipo_inst bank_type

* The goal: To add date_flood to CCS_muni_banco_PF.dta and prepare it for flood_contas_bancarias_muni.R

* To do: Maybe we can create other types of collapse.

******************************************

clear all
set more off, permanently 
set matsize 2000
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
log using "$log\Monta_base_Muni_Banco_flood.log", replace 

*************************
* After Pix
*************************

use "$dta\CCS_muni_banco_PF.dta", replace

* Fix time_id
replace time_id = ym(floor(time_id/100),mod(time_id,100))
format time_id %tm

* check for duplicates
sort IF muni_cd time_id
egen id_mun_bank = group(IF muni_cd)
duplicates tag id_mun_bank time_id, gen(dup)
tab dup
drop if dup == 1
drop dup

*tsfill
tsset id_mun_bank time_id 
tsfill, full

*Replace constant values 
foreach var of varlist IF muni_cd {
	egen temp = min(`var'), by(id_mun_bank)
	replace `var' = temp if `var' == .
	drop temp
}

*Replace missing with zeros and creates log var
foreach var of varlist qtd {
	replace `var' = 0 if `var' == . 
	cap drop log_`var'
	gen log_`var'=log(`var' + 1)
}

* merge with flood
merge m:1 muni_cd time_id using "$dta\flood_monthly_2020_2022.dta", keep(3) keepusing(date_flood) nogen

* Create new variables

* Add cadastro_if
rename IF cnpj8_if
merge m:1 cnpj8_if using "$dta\Cadastro_IF.dta", keep(3) keepusing(tipo_inst) nogenerate
rename cnpj8_if IF

gen bank_type = 3
replace bank_type = 1 if tipo_inst == 1 | tipo_inst == 2
replace bank_type = 2 if tipo_inst == 5

label define bank_type 1 "Traditional" 2 "Digital" 3 "Others"

* save 
save "$dta\CCS_muni_banco_PF_flood.dta", replace

use "$dta\CCS_muni_banco_PF_flood.dta", replace
*collapse
collapse (sum) qtd, by(bank_type muni_cd time_id date_flood)
gen log_qtd = log(qtd+1)
save "$dta\CCS_muni_banco_PF_flood_collapsed.dta", replace

*************************
* Before Pix
*************************


use "$dta\CCS_muni_banco_PF.dta", replace

* Fix time_id
replace time_id = ym(floor(time_id/100),mod(time_id,100))
format time_id %tm

* check for duplicates
sort IF muni_cd time_id
egen id_mun_bank = group(IF muni_cd)
duplicates tag id_mun_bank time_id, gen(dup)
tab dup
drop if dup == 1
drop dup

*tsfill
tsset id_mun_bank time_id 
tsfill, full

*Replace constant values 
foreach var of varlist IF muni_cd {
	egen temp = min(`var'), by(id_mun_bank)
	replace `var' = temp if `var' == .
	drop temp
}

*Replace missing with zeros and creates log var
foreach var of varlist qtd {
	replace `var' = 0 if `var' == . 
	cap drop log_`var'
	gen log_`var'=log(`var' + 1)
}

* merge with flood
merge m:1 muni_cd time_id using "$dta\flood_monthly_2018_2020.dta", keep(3) keepusing(date_flood) nogen

* Create new variables

* Add cadastro_if
rename IF cnpj8_if
merge m:1 cnpj8_if using "$dta\Cadastro_IF.dta", keep(3) keepusing(tipo_inst) nogenerate
rename cnpj8_if IF

gen bank_type = 3
replace bank_type = 1 if tipo_inst == 1 | tipo_inst == 2
replace bank_type = 2 if tipo_inst == 5

label define bank_type 1 "Traditional" 2 "Digital" 3 "Others"

* save 
save "$dta\CCS_muni_banco_PF_flood_beforePIX.dta", replace

use "$dta\CCS_muni_banco_PF_flood_beforePIX.dta", replace
*collapse
collapse (sum) qtd, by(bank_type muni_cd time_id date_flood)
gen log_qtd = log(qtd+1)
save "$dta\CCS_muni_banco_PF_flood_collapsed_beforePIX.dta", replace
