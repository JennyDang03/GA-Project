* Monta base juntando dados do PIB, roubos, internet 3g, etc
* Monta tambem o CEM pra ser usado na regressao dyn
* Input: 
*    - XXXXXXXXXXXXXXXXXX.dta
*    - pib_2019.dta
*    - cobertura de internet 3g 
* Output:
*  mun_weights.dta


clear all
set more off, permanently 
set matsize 2000
set emptycells drop


global log "D:\PIX_Matheus\Stata\log"
global dta "D:\PIX_Matheus\Stata\dta"
global output "D:\PIX_Matheus\Output"
global origdata "D:\PIX_Matheus\DadosOriginais"

* ADO - Problem Here
adopath ++ "D:\ADO\"
adopath ++ "D:\ADO\plus\"
adopath ++ "D:\ADO\cem\"


capture log close
log using "$log\Monta_CEM.log", replace



*******
* change "$dta\ibge_bcb_`state'.dta"
*
*******


import delimited "$origdata\IDH\municipio.csv", clear 
save "$dta\idh.dta", replace
keep if ano==2010
drop ano
save "$dta\idh_2010.dta", replace

import delimited "$origdata\PIB\municipio.csv", clear 
save "$dta\pib.dta", replace
keep if ano==2019
save "$dta\pib_2019.dta", replace

import delimited "$origdata\Pop\municipio.csv", clear 
save "$dta\pop.dta", replace
keep if ano==2021
drop ano
rename populacao pop
save "$dta\pop_2021.dta", replace

use "$dta\3g_coverage.dta", replace
rename codmun_ibge id_municipio
destring id_municipio, replace 
destring pc_area_3g pc_moradores_3g pc_domicilios_3g  area moradores domicilios, replace force
save "$dta\3g_coverage_clean.dta", replace

use "$dta\disaster_weekly_flood.dta", clear
drop week number_disasters
sort id_municipio id_municipio_bcb
by id_municipio id_municipio_bcb: gen dup = cond(_N==1,0,_n)
drop if dup>1
drop dup
merge 1:1 id_municipio using "$dta\pop_2021.dta"
keep if _merge ==3
drop _merge pop 
save "$dta\ibge_bcb.dta", replace

********************************************************************************
*Merge
use "$dta\idh_2010.dta", replace
merge 1:1 id_municipio using "$dta\pib_2019.dta", keep(3) nogen
merge 1:1 id_municipio using "$dta\pop_2021.dta", keep(3) nogen
merge 1:1 id_municipio using "$dta\3g_coverage_clean.dta", keep(3) nogen
save "$dta\mun_info.dta", replace
********************************************************************************

foreach disaster in flood drought {
	use "$dta\disaster_weekly_`disaster'.dta", clear
	merge m:1 id_municipio using "$dta\mun_info.dta"
	unique id_municipio_bcb if _merge == 2
	keep if _merge == 3
	drop _merge
	drop Município UF

	sum idhm idhm_e idhm_l idhm_r pib impostos_liquidos va va_agropecuaria va_industria va_servicos va_adespss pc_area_3g pc_moradores_3g pc_domicilios_3g area moradores domicilios pop

	gen date_w_disaster = week if number_disasters >= 1 
	bys id_municipio: egen date_w_disaster_mun=min(date_w_disaster)
	gen disaster_flag = date_w_disaster_mun ~= .
	drop week date_w_disaster

	collapse (min) disaster_flag date_w_disaster_mun idhm idhm_e idhm_l idhm_r pib impostos_liquidos va va_agropecuaria va_industria va_servicos va_adespss pc_area_3g pc_moradores_3g pc_domicilios_3g area moradores domicilios pop,by(id_municipio id_municipio_bcb sigla_uf)

	save "$dta\disaster_collapsed_`disaster'.dta", replace

	cem idhm idhm_e idhm_l idhm_r pib impostos_liquidos va va_agropecuaria va_industria va_servicos va_adespss pc_area_3g pc_moradores_3g pc_domicilios_3g area moradores domicilios pop, tr(disaster_flag)= 

	save "$dta\cem_brasil_`disaster'.dta", replace
	
	foreach state in CE BA {
		use "$dta\disaster_collapsed_`disaster'.dta", replace
		keep if sigla_uf == "`state'"
		cem idhm idhm_e idhm_l idhm_r pib impostos_liquidos va va_agropecuaria va_industria va_servicos va_adespss pc_area_3g pc_moradores_3g pc_domicilios_3g area moradores domicilios pop, tr(disaster_flag)

		save "$dta\cem_`state'_`disaster'.dta", replace
	}
}
********************************************************************************


use "$dta\disaster_weekly_flood.dta", clear
merge m:1 id_municipio id_municipio_bcb using "$dta\ibge_bcb.dta"
collapse (sum) number_disasters, by(sigla_uf)

use "$dta\disaster_weekly_drought.dta", clear
merge m:1 id_municipio id_municipio_bcb using "$dta\ibge_bcb.dta"
collapse (sum) number_disasters, by(sigla_uf)

use "$dta\disaster_weekly_covid.dta", clear
merge m:1 id_municipio id_municipio_bcb using "$dta\ibge_bcb.dta"
collapse (sum) number_disasters, by(sigla_uf)


log close


