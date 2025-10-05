******************************************
* Monta_base_credito_flood.do
* Input: 
*   1) "$dta\Base_credito_muni.dta"
*   2) "$dta\flood_monthly_2020_2022.dta"
*	3) "$dta\flood_monthly_2019_2020.dta"
*	4) "$dta\municipios2.dta"

* Output:
*   1) "$dta\Base_credito_muni_flood.dta"
*   2) "$dta\Base_credito_muni_flood_beforePIX.dta"

* Variables: muni_cd time_id date_flood
*			 qtd_cli_total qtd_cli_total_PF qtd_cli_total_PJ
*			 vol_credito_total vol_credito_total_PF vol_credito_total_PJ
* 			 vol_emprestimo_pessoal qtd_cli_emp_pessoal
* 			 vol_cartao qtd_cli_cartao
* 			 + Log variations of it + other variables not so important

* The goal: To add date_flood to Base_credito_muni.dta and prepare it for flood_credito_muni_month.R

* To do: 

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
log using "$log\Monta_base_credito_flood.log", replace 

*************************************************
* agora vamos preparar a base para usar em R
*************************************************
use "$dta\Base_credito_muni.dta", replace
* Fix ano_mes to time_id
gen time_id = ym(floor(ano_mes/100),mod(ano_mes,100))
format time_id %tm
drop ano_mes

* create muni_cd
rename codmun_ibge id_municipio
destring id_municipio, replace
merge m:1 id_municipio using "$dta\municipios2.dta", keepusing(id_municipio_bcb)
keep if _merge == 3
drop _merge
rename id_municipio_bcb muni_cd
drop id_municipio
order muni_cd time_id

*check for duplicates
duplicates tag muni_cd time_id, gen(dup)
tab dup
drop if dup == 1
drop dup

*check if need tsfill
sort muni_cd time_id
tsset muni_cd time_id
tsfill, full

*Replace missing with zeros and creates log var
foreach var of varlist qtd_op - vol_credito_total{
	replace `var' = 0 if `var' == .
	cap drop log_`var'
	gen log_`var'=log(`var' + 1)
}

* merge with flood
merge m:1 muni_cd time_id using "$dta\flood_monthly_2020_2022.dta", keep(3) keepusing(date_flood)
drop _merge

cap drop post_pix

save "$dta\Base_credito_muni_flood.dta", replace

**********************
* Before Pix
**********************
use "$dta\Base_credito_muni.dta", replace
* Fix ano_mes to time_id
gen time_id = ym(floor(ano_mes/100),mod(ano_mes,100))
format time_id %tm
drop ano_mes

* create muni_cd
rename codmun_ibge id_municipio
destring id_municipio, replace
merge m:1 id_municipio using "$dta\municipios2.dta", keepusing(id_municipio_bcb)
keep if _merge == 3
drop _merge
rename id_municipio_bcb muni_cd
drop id_municipio
order muni_cd time_id

*check for duplicates
duplicates tag muni_cd time_id, gen(dup)
tab dup
drop if dup == 1
drop dup

*check if need tsfill
sort muni_cd time_id
tsset muni_cd time_id
tsfill, full

*Replace missing with zeros and creates log var
foreach var of varlist qtd_op - vol_credito_total{
	replace `var' = 0 if `var' == .
	cap drop log_`var'
	gen log_`var'=log(`var' + 1)
}

* merge with flood
merge m:1 muni_cd time_id using "$dta\flood_monthly_2018_2020.dta", keep(3) keepusing(date_flood)
drop _merge

cap drop post_pix

save "$dta\Base_credito_muni_flood_beforePIX.dta", replace
log close