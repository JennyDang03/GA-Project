******************************************
* monta_base_pix_mun_week_self.do
* Input: 
*   1) "$dta\PIX_week_muni_self.dta"
*   2) "$dta\flood_weekly_2020_2022.dta"
*	3) "$dta\flood_weekly_2019_2020.dta"

* Output:
*   1) "$dta\PIX_week_muni_self_flood.dta"
*   2) "$dta\PIX_week_muni_self_flood_sample10.dta"

* Variables: muni_cd week date_flood valor_self_pf valor_self_pj qtd_self_pf qtd_self_pj n_cli_self_pf n_cli_self_pj log_valor_self_pf log_valor_self_pj log_qtd_self_pf log_qtd_self_pj log_n_cli_self_pf log_n_cli_self_pj

* The goal: To add date_flood to PIX_week_muni_self.dta and prepare it for flood_SA_muni_self_v1.R

* To do: 

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
log using "$log\monta_base_pix_mun_week.log", replace 


********************************************************************************
* Weekly at the Municipal level. 
********************************************************************************

************
* After Pix
************

use "$dta\PIX_week_muni_self.dta", replace

keep if week >= wofd(mdy(11, 16, 2020)) & week <= wofd(mdy(12, 31, 2022))
rename mun_cd muni_cd

sort muni_cd week
tsset muni_cd week
tsfill, full // Not necessary because it is strongly balanced
sum *

foreach var of varlist  valor_self_pf-n_cli_self_pj {
    replace `var' = 0 if `var' == .
	cap drop log_`var'
	gen log_`var'=log(`var' + 1)
}


merge m:1 muni_cd week using "$dta\flood_weekly_2020_2022.dta", keep(3) keepusing(date_flood)
drop _merge

* Save
save "$dta\PIX_week_muni_self_flood.dta", replace



use "$dta\PIX_week_muni_self_flood.dta", replace

tempfile holding
save `holding'

keep muni_cd
duplicates drop

set seed 1234
sample 10

merge 1:m muni_cd using `holding', assert(match using) keep(match) nogenerate

sort muni_cd week
*format %tm date_flood
save "$dta\PIX_week_muni_self_flood_sample10.dta", replace


log close
