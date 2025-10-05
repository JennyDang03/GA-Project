*IV

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

* Prepare data before:
*do "$do\monta_base_reg_v3_road.do"

capture log close
log using "$log\iv_v1.log", replace 

use "$dta\Pix_bank_account_bf.dta", replace
/*
global dta "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta"
global output "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output"
use "$dta\fake_panel_data_new_variables.dta", replace
merge 1:1 id time_id using "$dta\id_accounts.dta"
rename accounts n_account_stock
replace n_account_stock = 0 if n_account_stock == .
keep if _merge ==3
gen road_dist_expec = road_dist_caixa - road_expected_distance

keep if bf==1
save "$dta\Pix_bank_account_bf_fake.dta", replace
*/

* eststo m1: reghdfe `y' road_dist_caixa road_expected_distance if `type' == 1, absorb(muni_cd##time_id) vce(robust)

eststo m1: ivregress 2sls n_account_stock (after_first_pix_sent = road_dist_caixa road_expected_distance i.time_id##i.muni_cd) if bf == 1, vce(cluster id) // vce(robust)

quietly estadd local fixedmm "Yes", replace
estadd ysumm, replace

esttab m*, label se ///
			star(* 0.10 ** 0.05 *** 0.01) drop(_cons) ///
			s(fixedmm N ymean, label("Mun. - Month FE" ///
			"Observations" "Mean of Dep. Var."))			
cap esttab m* using "$output\2sls_account_sent.tex", label se ///
			star(* 0.10 ** 0.05 *** 0.01) drop(_cons) ///
			s(fixedmm N ymean, label("Mun. - Month FE" ///
			"Observations" "Mean of Dep. Var.")) replace			

capture log close
