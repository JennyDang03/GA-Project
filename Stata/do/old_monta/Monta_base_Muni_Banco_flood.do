******************************************
*Monta_base_Muni_Banco_flood
* Input: 
*   1) "$dta\Base_week_muni_banco.dta"
*   2) "$dta\flood_weekly_2020_2022.dta"

* Output:
*   1) "$dta\Base_muni_banco_flood.dta"
*   2) "$dta\Base_muni_banco_flood_collapsed.dta"
*	3) "$dta\Base_muni_banco_flood_collapsed2.dta"
* Variables: id_mun_bank_tipo IF tipo muni_cd week date_flood
*			tipo muni_cd week date_flood bank_type valor_netflow qtd_netflow n_cli_rec n_cli_pag valor_rec qtd_rec valor_pag qtd_pag valor_ratio qtd_ratio log_valor_ratio log_qtd_ratio
* log_valor_totalflow log_qtd_totalflow valor_totalflow qtd_totalflow

* The goal: To add date_flood to Base_week_muni_banco.dta and prepare it for flood_flow_banco_muni.R

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
log using "$log\Monta_base_Muni_Banco_flood.log", replace 

*************************
* After Pix
*************************

use "$dta\Base_week_muni_banco.dta", replace 
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

foreach var of varlist IF tipo muni_cd number_branches-controle belong_cong-post_pix {
	egen temp = min(`var'), by(id_mun_bank_tipo)
	replace `var' = temp if `var' == .
	drop temp
}

*Replace missing with zeros and creates log var
foreach var of varlist n_cli_rec-qtd_pag {
	replace `var' = 0 if `var' == . 
	cap drop log_`var'
	gen log_`var'=log(`var' + 1)
}

* merge with flood
merge m:1 muni_cd week using "$dta\flood_weekly_2020_2022.dta", keep(3) keepusing(date_flood)
drop _merge

gen bank_type = 3
replace bank_type = 1 if tipo_inst == 1 | tipo_inst == 2
replace bank_type = 2 if tipo_inst == 5

label define bank_type 1 "Traditional" 2 "Digital" 3 "Others"

* Create new variables
gen valor_netflow = valor_rec - valor_pag
gen qtd_netflow = qtd_pag - qtd_rec

save "$dta\Base_muni_banco_flood.dta", replace 

use "$dta\Base_muni_banco_flood.dta", replace 
* collapse
collapse (sum) valor_netflow qtd_netflow n_cli_rec n_cli_pag valor_rec qtd_rec valor_pag qtd_pag, by(bank_type tipo muni_cd week date_flood)

gen valor_totalflow = valor_rec + valor_pag
gen qtd_totalflow = qtd_pag + qtd_rec
gen log_valor_totalflow = log(valor_totalflow+1)
gen log_qtd_totalflow = log(qtd_totalflow+1)

gen valor_ratio = valor_rec/(valor_pag+0.01)
gen qtd_ratio = qtd_rec/(qtd_pag+0.01)
gen log_valor_ratio = log(valor_ratio+1)
gen log_qtd_ratio = log(qtd_ratio+1)

* Create a collapse for all banks so I can have a variable of total quantities. 

save "$dta\Base_muni_banco_flood_collapsed.dta", replace 



use "$dta\Base_muni_banco_flood_collapsed.dta", replace 
* Create a collapse for all banks so I can have a variable of total quantities.

* collapse
collapse (sum) valor_netflow qtd_netflow valor_totalflow qtd_totalflow n_cli_rec n_cli_pag valor_rec qtd_rec valor_pag qtd_pag, by(tipo muni_cd week date_flood)

*Gen logs
gen valor_ratio = valor_rec/(valor_pag+0.01)
gen qtd_ratio = qtd_rec/(qtd_pag+0.01)

gen log_valor_ratio = log(valor_ratio+1)
gen log_qtd_ratio = log(qtd_ratio+1)

gen log_valor_totalflow = log(valor_totalflow+1)
gen log_qtd_totalflow = log(qtd_totalflow+1)

*save
save "$dta\Base_muni_banco_flood_collapsed2.dta", replace 


use "$dta\Base_muni_banco_flood_collapsed.dta", replace 
* Create a collapse for all people/firms so I can have a variable of total quantities.

* collapse
collapse (sum) valor_netflow qtd_netflow valor_totalflow qtd_totalflow n_cli_rec n_cli_pag valor_rec qtd_rec valor_pag qtd_pag, by(bank_type muni_cd week date_flood)

*Gen logs
gen valor_ratio = valor_rec/(valor_pag+0.01)
gen qtd_ratio = qtd_rec/(qtd_pag+0.01)

gen log_valor_ratio = log(valor_ratio+1)
gen log_qtd_ratio = log(qtd_ratio+1)

gen log_valor_totalflow = log(valor_totalflow+1)
gen log_qtd_totalflow = log(qtd_totalflow+1)

*save
save "$dta\Base_muni_banco_flood_collapsed3.dta", replace 



*************************
* Before Pix
*************************
* There is only Pix variables, it would need TED, Boleto, ...


log close
