******************************************
*Monta_base_Muni_Banco_self_flood.do
* Input: 
*   1) "$dta\Base_muni_banco_self.dta"
*   2) "$dta\flood_weekly_2020_2022.dta"

* Output:
*   1) "$dta\Base_muni_banco_self_flood.dta"
*   2) "$dta\Base_muni_banco_self_flood_collapsed.dta"

* Variables: 
*			tipo muni_cd week date_flood bank_type
*			valor_self_pag qtd_self_pag valor_self_rec qtd_self_rec valor_self_netflow qtd_self_netflow valor_self_ratio qtd_self_ratio log_valor_self_ratio log_qtd_self_ratio

* The goal: To add date_flood to Base_muni_banco_self_flood.dta and prepare it for flood_flow_banco_muni_self.R

* To do: it would need TED, Boleto for us to do a Before Pix analysis. 

******************************************

* Paths and clears 
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
log using "$log\Monta_base_Muni_Banco_self_flood.log", replace 

*************************
* After Pix
*************************

use "$dta\Base_muni_banco_self.dta", replace 
tab week

// tipo_inst = 1   bancos comerciais Publicos - Federais ou estaduais 
// tipo_inst = 2   bancos comerciais grandes Privados
// tipo_inst = 4   cooperativas de credito
// tipo_inst = 5   bancos digitais ou IPs 
// tipo_inst = 6   o resto: b1 não-grande e não-digital, n1, b2, etc

*check for duplicates
duplicates tag id_mun_bank_tipo week, gen(dup)
tab dup
drop if dup == 1
drop dup

*check if need tsfill - Foi feito no outro dta (Monta_base_Muni_Banco_self) mas parece que precisa
sort IF muni_cd tipo week
tsset id_mun_bank_tipo week 
tsfill, full

order id_mun_bank_tipo IF tipo muni_cd week

*Replace constant values 
foreach var of varlist IF tipo muni_cd number_branches-controle belong_cong-tipo_inst {
	egen temp = min(`var'), by(id_mun_bank_tipo)
	replace `var' = temp if `var' == .
	drop temp
}

*Replace missing with zeros and creates log var
foreach var of varlist valor_self_pag-qtd_self_rec {
	replace `var' = 0 if `var' == . 
	cap drop log_`var'
	gen log_`var'=log(`var' + 1)
}

* Create new variables
gen valor_self_netflow = valor_self_rec - valor_self_pag
gen qtd_self_netflow = qtd_self_rec - qtd_self_pag

gen valor_self_ratio = valor_self_rec/(valor_self_pag+0.01)
gen qtd_self_ratio = qtd_self_rec/(qtd_self_pag+0.01)


* merge with flood
merge m:1 muni_cd week using "$dta\flood_weekly_2020_2022.dta", keep(3) keepusing(date_flood)
drop _merge

gen bank_type = 3
replace bank_type = 1 if tipo_inst == 1 | tipo_inst == 2
replace bank_type = 2 if tipo_inst == 5

label define bank_type 1 "Traditional" 2 "Digital" 3 "Others"

* Make it light
*keep id_mun_bank_tipo IF tipo muni_cd week date_flood tipo_inst valor_self_netflow qtd_self_netflow bank_type

save "$dta\Base_muni_banco_self_flood.dta", replace 

use "$dta\Base_muni_banco_self_flood.dta", replace 
* collapse
collapse (sum) valor_self_pag qtd_self_pag valor_self_rec qtd_self_rec valor_self_netflow qtd_self_netflow, by(bank_type tipo muni_cd week date_flood)

gen valor_self_totalflow = valor_self_rec + valor_self_pag
gen qtd_self_totalflow = qtd_self_pag + qtd_self_rec
gen log_valor_self_totalflow = log(valor_self_totalflow+1)
gen log_qtd_self_totalflow = log(qtd_self_totalflow+1)

gen valor_self_ratio = valor_self_rec/(valor_self_pag+0.01)
gen qtd_self_ratio = qtd_self_rec/(qtd_self_pag+0.01)
gen log_valor_self_ratio = log(valor_self_ratio+1)
gen log_qtd_self_ratio = log(qtd_self_ratio+1)

save "$dta\Base_muni_banco_self_flood_collapsed.dta", replace 

use "$dta\Base_muni_banco_self_flood_collapsed.dta", replace 
* collapse
collapse (sum) valor_self_pag qtd_self_pag valor_self_rec qtd_self_rec valor_self_netflow qtd_self_netflow, by(tipo muni_cd week date_flood)

gen valor_self_totalflow = valor_self_rec + valor_self_pag
gen qtd_self_totalflow = qtd_self_pag + qtd_self_rec
gen log_valor_self_totalflow = log(valor_self_totalflow+1)
gen log_qtd_self_totalflow = log(qtd_self_totalflow+1)

gen valor_self_ratio = valor_self_rec/(valor_self_pag+0.01)
gen qtd_self_ratio = qtd_self_rec/(qtd_self_pag+0.01)
gen log_valor_self_ratio = log(valor_self_ratio+1)
gen log_qtd_self_ratio = log(qtd_self_ratio+1)

save "$dta\Base_muni_banco_self_flood_collapsed2.dta", replace

use "$dta\Base_muni_banco_self_flood_collapsed.dta", replace 
* collapse
collapse (sum) valor_self_pag qtd_self_pag valor_self_rec qtd_self_rec valor_self_netflow qtd_self_netflow, by(bank_type muni_cd week date_flood)

gen valor_self_totalflow = valor_self_rec + valor_self_pag
gen qtd_self_totalflow = qtd_self_pag + qtd_self_rec
gen log_valor_self_totalflow = log(valor_self_totalflow+1)
gen log_qtd_self_totalflow = log(qtd_self_totalflow+1)

gen valor_self_ratio = valor_self_rec/(valor_self_pag+0.01)
gen qtd_self_ratio = qtd_self_rec/(qtd_self_pag+0.01)
gen log_valor_self_ratio = log(valor_self_ratio+1)
gen log_qtd_self_ratio = log(qtd_self_ratio+1)

save "$dta\Base_muni_banco_self_flood_collapsed3.dta", replace

*************************
* Before Pix
*************************
* There is only Pix variables, it would need TED, Boleto, ...


log close
