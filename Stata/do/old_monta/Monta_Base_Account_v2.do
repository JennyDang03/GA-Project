*
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

* Fake data
/*
global dta "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta"
use "$dta\fake_panel_data_new_variables.dta", replace
merge 1:1 id time_id using "$dta\id_accounts.dta"
rename accounts n_account_stock
replace n_account_stock = 0 if n_account_stock == .


* Create date_adoption from after_adoption

rename date_adoption date_first_pix_sent
*/

capture log close
log using "$log\Monta_base_Account_v2.log", replace 

******************************************************

use "$dta\Pix_PF_adoption_new_variables.dta", clear

*merge 1:1 id time_id using "\\sbcdf176\PIX_Matheus$\Stata\dta\CCS_aux_emerg.dta", keep(1 3) nogenerate
* This is droping all months before Nov 2020

merge 1:1 id time_id using "\\sbcdf176\PIX_Matheus$\Stata\dta\CCS_aux_emerg.dta"
drop _merge
*--------------------------------------
* This has months before Pix existed!
* We got to fill the missing variables
cap drop dist_event 
cap drop id_dist
cap drop after_event

* KEEP until 2022,6 - We didnt download the rest
keep if time_id <= ym(2022,6)
*-----------------------------------------------

/*
sort id time_id
tsset id time_id
tsfill, full
*/

cap drop temp 
bysort id : egen temp = min(index)
replace index = temp if index == .

******************
drop if index == .
******************

cap drop temp 
bysort id : egen temp = min(bf)
replace bf = temp if bf == .

*****************
*To save space
keep if bf == 1
*****************

drop index0 latitude longitude confidence mun_cd

foreach var of varlist dist* previous* treat* index expected_distance grupo muni_cd cad extracad all non_bf all0 aux_emerg_jan_mar21 road_* date_* {

	cap drop temp 
	bysort id : egen temp = min(`var')
	replace `var' = temp if `var' == .
	
}
cap drop temp 

foreach var of varlist n_acc* after_* trans_rec trans_sent value_sent value_rec sender receiver user {
	replace `var' = 0 if missing(`var') 
}

gen temp = 0
replace temp = n_account_stock if time_id == ym(2020,10)
bysort id: egen accounts_oct_2020 = max(temp)
drop temp

sum * // We need that all variables have the same amount of observations so we can make sure that the filling in worked

sum n_account_stock if after_first_pix_sent == 0
sum n_account_stock if after_first_pix_sent == 1
sum n_account_stock if bf == 1 & accounts_oct_2020>0 & after_first_pix_sent == 0
sum n_account_stock if bf == 1 & accounts_oct_2020>0 & after_first_pix_sent == 1
sum n_account_stock if bf == 1 & accounts_oct_2020>0, d

sum n_acc*, d
sum n_acc* if bf == 1, d

* Delete Outliers
bysort id: egen max_accounts = max(n_account_stock)
sum max_accounts, d
drop if max_accounts > r(p99)

save "$dta\Pix_bank_account_bf.dta", replace
*--------------------------------------------------
* Making simple graphs
preserve

	collapse (mean) n_account_stock, by(time_id)
	line n_account_stock time_id, ///
		title("Number of Accounts by Time") ///
		xtitle("Month") ///
		ytitle("Number of Accounts") ///
		xline(723 730 735) ///
		xmlabel(723 "Covid" 730 "Pix Launch" , angle(90))
	graph export "$output\accounts_time_bf.png", replace
*735 "Caixa TEM"
restore 

preserve
	
	keep if accounts_oct_2020 > 0
	
	collapse (mean) n_account_stock, by(time_id)
	line n_account_stock time_id, ///
		title("Number of Accounts by Time") ///
		xtitle("Month") ///
		ytitle("Number of Accounts") ///
		xline(723 730 735) ///
		xmlabel(723 "Covid" 730 "Pix Launch", angle(90))
	graph export "$output\accounts_time_conditional_oct_bf.png", replace
*735 "Caixa TEM"
restore 

log close




use "$dta\Pix_bank_account_bf.dta", replace

foreach var of varlist road_* date_*{

	cap drop temp 
	bysort id : egen temp = min(`var')
	replace `var' = temp if `var' == .
	
}
cap drop temp 
save "$dta\Pix_bank_account_bf.dta", replace



/*
******************************************************
* How to solve problem with eventstudyinteract:
ssc install reghdfe
ssc install avar
ssc install ftools
ssc install eventstudyinteract


* If that does not work, read this link and try:
* https://www.statalist.org/forums/forum/general-stata-discussion/general/1669522-eventstudyinteract-returns-struct-ms_vcvorthog-undefined-error-message-on-secure-server
capture mata: mata drop m_calckw()
capture mata: mata drop m_omega()
capture mata: mata drop ms_vcvorthog()
capture mata: mata drop s_vkernel()
mata: mata mlib index

viewsource avar.ado
******************************************************
*/

