******************************************
*Estban_detalhado_flood
* Input: 
*   1) "\\sbcdf176\PIX_Matheus$\DadosOriginais\ESTBAN_DETA.csv"

* Output:
*   1) "$dta\Estban_detalhado_HHI.dta"
*	2) "$dta\Estban_detalhado_HHI_flood.dta"
*	3) "$dta\Estban_detalhado_HHI_flood_beforePIX.dta"
*   2) "$dta\Estban_detalhado_flood.dta"
*   3) "$dta\Estban_detalhado_flood_collapsed.dta"
*   3) "$dta\Estban_detalhado_flood_collapsed2.dta"
*   4) "$dta\Estban_detalhado_flood_beforePIX.dta"
*   5) "$dta\Estban_detalhado_flood_beforePIX_collapsed.dta"
*   5) "$dta\Estban_detalhado_flood_beforePIX_collapsed2.dta"


* Variables: muni_cd time_id hhi_dep_vista_PF hhi_dep_vista_PJ hhi_poupanca hhi_dep_prazo hhi_total_deposits hhi_lending hhi_imobiliario hhi_caixa date_flood
*			cnpj muni_cd time_id cnpj_mun caixa lending imobiliario dep_vista_PF dep_vista_PJ poupanca dep_prazo temp n_muni large_bank date_flood total_deposits date_flood
*			muni_cd time_id large_bank caixa lending imobiliario dep_vista_PF dep_vista_PJ poupanca dep_prazo total_deposits date_flood
*		+ log_`var'

* The goal: Get Estban data and prepare it for an event study. Run flood_estban.R after this.
 
* To do: Alex Nery research on Journal of Banking and Finance does the HHI a little bit different. The HHI he makes is divided by 10000 and they calculate the index for each bank branch then to each bank (maybe we need estban by branch? instead of estban branches aggregated to the municipal level). 


******************************************


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
log using "$log\Estban_detalhado_flood.log", replace 

*** Pega dados de 2018 a 2022
cap drop temp
import delimited "\\sbcdf176\PIX_Matheus$\DadosOriginais\ESTBAN_DETA.csv", encoding(ISO-8859-2) clear

ren mun_cd_ibge codmun_ibge 
ren mun_cd_cadmu muni_cd

collapse (sum) sdag_vlr_sdo, by(cnpj data_base conta_id codmun_ibge muni_cd)
reshape wide sdag_vlr_sdo, i(cnpj data_base codmun_ibge muni_cd) j(conta_id)

foreach var of varlist sdag_vlr_sdo*{
	replace `var'=0 if missing(`var')
}

ren sdag_vlr_sdo111 caixa
ren sdag_vlr_sdo160 lending
ren sdag_vlr_sdo169 imobiliario
ren sdag_vlr_sdo411 dep_vista_PF
ren sdag_vlr_sdo412 dep_vista_PJ
ren sdag_vlr_sdo420 poupanca
ren sdag_vlr_sdo432 dep_prazo

tostring codmun_ibge, replace 

*save "$dta\Estban_detalhado.dta", replace 
****
*use "$dta\Estban_detalhado.dta", replace // this is already aggregated to the muni_cd level
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

preserve
	*Merge with flood
	merge m:1 muni_cd time_id using "$dta\flood_monthly_2020_2022.dta", keep(3) keepusing(date_flood)
	* Add only date_flood variable to save on memory
	cap drop _merge

	save "$dta\Estban_detalhado_flood.dta", replace

	collapse (sum) total_deposits caixa-dep_prazo, by(large_bank muni_cd time_id date_flood)
	
	foreach var of varlist total_deposits caixa-dep_prazo{
		gen log_`var' = log(`var' + 1)
	}
	
	save "$dta\Estban_detalhado_flood_collapsed.dta", replace
	
	collapse (sum) total_deposits caixa-dep_prazo, by(muni_cd time_id date_flood)
	
	foreach var of varlist total_deposits caixa-dep_prazo{
		gen log_`var' = log(`var' + 1)
	}
	
	save "$dta\Estban_detalhado_flood_collapsed2.dta", replace

restore

preserve
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
restore



*Calculate the HHI index for firms and People

egen cnpj_id = group(cnpj)
drop large_bank
save "$dta\temp.dta", replace

clear 
foreach var of newlist dep_vista_PF dep_vista_PJ poupanca dep_prazo total_deposits lending imobiliario caixa {
	use "$dta\temp.dta", replace
	keep `var' muni_cd time_id cnpj_id
	reshape wide `var', i(muni_cd time_id) j(cnpj_id)
	
	
	gen total_`var' = 0
	forval i=1/122{
		replace `var'`i' = 0 if `var'`i' == .
		replace total_`var' = total_`var' + `var'`i'
	}
	
	gen hhi_`var' = 0
	forval i=1/122{
		replace hhi_`var' = hhi_`var' + (`var'`i'/total_`var'*100)^2
	}
	keep muni_cd time_id hhi_`var'
	save "$dta\temp_`var'.dta", replace
}

use "$dta\temp_dep_vista_PF.dta", replace
foreach var of newlist dep_vista_PJ poupanca dep_prazo total_deposits lending imobiliario caixa{
	merge 1:1 muni_cd time_id using "$dta\temp_`var'.dta", nogen
	erase "$dta\temp_`var'.dta"
}
erase "$dta\temp_dep_vista_PF.dta"
erase "$dta\temp.dta"

save "$dta\Estban_detalhado_HHI.dta", replace
*Need to add before and after pix with flood

use "$dta\Estban_detalhado_HHI.dta", replace
*Merge with flood
merge m:1 muni_cd time_id using "$dta\flood_monthly_2020_2022.dta", keep(3) keepusing(date_flood)
* Add only date_flood variable to save on memory
cap drop _merge
save "$dta\Estban_detalhado_HHI_flood.dta", replace

use "$dta\Estban_detalhado_HHI.dta", replace
*Merge with flood
merge m:1 muni_cd time_id using "$dta\flood_monthly_2018_2020.dta", keep(3) keepusing(date_flood)
* Add only date_flood variable to save on memory
cap drop _merge
save "$dta\Estban_detalhado_HHI_flood_beforePIX.dta", replace



log close