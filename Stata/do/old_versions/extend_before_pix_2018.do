* Extend Before Pix analysis.

* Changing codes in:

*monta_base_pix_mun_week.do
* Monta_base_credito_flood.do
*Monta_base_CCS_muni_banco_PF_flood
*Estban_detalhado_flood


*************************************


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



*monta_base_pix_mun_week.do
************
* Before Pix
************
*use "$dta\Base_week_muni_fake.dta", replace
use "$dta\Base_week_muni.dta", replace

keep if week < wofd(mdy(11, 16, 2020)) & week >= wofd(mdy(1, 1, 2018))

* Cria painel balanceado
sort muni_cd week
tsset muni_cd week
tsfill, full // Not necessary because it is strongly balanced
sum *

*Create some new variables:
	* Log valor_cartao_credito - qtd_boleto 
foreach var of varlist valor_cartao_credito - n_cli_rec_pj_intra {
	cap drop log_`var'
	gen log_`var'=log(`var' + 1)
}

* Label them
* Best variables: 
* valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito 
* valor_TED_intra qtd_TED_intra qtd_cli_TED_rec_PJ qtd_cli_TED_rec_PF qtd_cli_TED_pag_PJ qtd_cli_TED_pag_PF 
* valor_boleto qtd_boleto qtd_cli_pag_pf_boleto qtd_cli_pag_pj_boleto qtd_cli_rec_pj_boleto 
* valor_PIX_inflow qtd_PIX_inflow valor_PIX_outflow qtd_PIX_outflow valor_PIX_intra qtd_PIX_intra n_cli_pag_pf_intra n_cli_rec_pf_intra n_cli_pag_pj_intra n_cli_rec_pj_intra
*****

merge m:1 muni_cd week using "$dta\flood_weekly_2018_2020.dta", keep(3) keepusing(date_flood)
drop _merge

* Save
*save "$dta\flood_pix_weekly_fake.dta", replace
save "$dta\Base_week_muni_flood_beforePIX.dta", replace


* Monta_base_credito_flood.do

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




*Monta_base_CCS_muni_banco_PF_flood

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





*Estban_detalhado_flood

*************************
* Before Pix
*************************

use "$dta\Estban_detalhado.dta", replace // this is already aggregated to the muni_cd level
drop codmun_ibge

* fix data_base
gen time = mdy(data_base - int(data_base/100) * 100,1 ,int(data_base/100) )
drop data_base
gen time_id = mofd(time)
format %tmNN/CCYY time_id
tab time_id
drop time 

* Elimina duplicates
duplicates tag cnpj muni_cd time_id, gen(dup)
tab dup
drop if dup >= 1
drop dup

* Cria painel balanceado
drop if cnpj == .
drop if muni_cd == .
sort cnpj muni_cd time_id
egen cnpj_mun = group(cnpj muni_cd)
tsset cnpj_mun time_id
tsfill, full

* Fill missing pieces
sort cnpj_mun muni_cd // make all the muni_cd that are missing to go up. 
bysort cnpj_mun (muni_cd): replace muni_cd = muni_cd[_n-1] if missing(muni_cd)
sort cnpj_mun cnpj // make all the muni_cd that are missing to go up. 
bysort cnpj_mun (cnpj): replace cnpj = cnpj[_n-1] if missing(cnpj)	
sort cnpj muni_cd time_id
order cnpj muni_cd time_id cnpj_mun

foreach var of varlist caixa - dep_prazo {
	replace `var'  = 0 if `var'  == .
}

egen temp = count(muni_cd), by(cnpj time_id)
egen n_muni = max(temp), by(cnpj)
unique cnpj if n_muni >500

gen large_bank = n_muni >500

gen total_deposits = dep_vista_PF + dep_vista_PJ + poupanca + dep_prazo

	*Merge with flood
	merge m:1 muni_cd time_id using "$dta\flood_monthly_2018_2020.dta", keep(3) keepusing(date_flood)
	* Add only date_flood variable to save on memory
	cap drop _merge

	save "$dta\Estban_detalhado_flood_beforePIX.dta", replace

	collapse (sum) total_deposits caixa-dep_prazo, by(large_bank muni_cd time_id date_flood)
	
	foreach var of varlist total_deposits caixa-dep_prazo{
		gen log_`var' = log(`var' + 1)
	}
	
	save "$dta\Estban_detalhado_flood_beforePIX_collapsed.dta", replace
	
	collapse (sum) total_deposits caixa-dep_prazo, by(muni_cd time_id date_flood)
	
	foreach var of varlist total_deposits caixa-dep_prazo{
		gen log_`var' = log(`var' + 1)
	}
	
	save "$dta\Estban_detalhado_flood_beforePIX_collapsed2.dta", replace
		
use "$dta\Estban_detalhado_HHI.dta", replace
*Merge with flood
merge m:1 muni_cd time_id using "$dta\flood_monthly_2018_2020.dta", keep(3) keepusing(date_flood)
* Add only date_flood variable to save on memory
cap drop _merge
save "$dta\Estban_detalhado_HHI_flood_beforePIX.dta", replace

