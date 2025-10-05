
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
log using "$log\Monta_base_Account_v1.log", replace 
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
* Fake data
/*
global dta "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta"
use "$dta\fake_panel_data_new_variables.dta", replace
merge 1:1 id time_id using "$dta\id_accounts.dta", keep(1 3) nogenerate
rename accounts n_account_stock
replace n_account_stock = 0 if n_account_stock == .
rename date_adoption date_first_pix_sent
rename mun_cd muni_cd
*/

******************************************************
use  "$dta\Pix_PF_adoption.dta", clear
merge 1:1 id time_id using "\\sbcdf176\PIX_Matheus$\Stata\dta\CCS_aux_emerg.dta", keep(1 3) nogenerate
sum n_acc*
sum n_account_stock, d
sum n_account_new, d

bysort after_first_pix_sent: sum n_account_stock

replace n_account_new = 0 if n_account_new == .
replace n_account_stock = 0 if n_account_stock == .

sum n_acc*

* Delete Outliers
bysort id: egen max_accounts = max(n_account_stock)
sum max_accounts, d
drop if max_accounts > r(p99)

save "$dta\Pix_PF_adoption_bank_account.dta", replace
*--------------------------------------------------
* Making simple graphs

collapse (mean) n_account_stock, by(time_id)

line n_account_stock time_id, ///
    title("Number of Accounts by Time") ///
    xtitle("Month") ///
    ytitle("Number of Accounts") ///
	xline(723 730 735) ///
	xmlabel(723 "Covid" 730 "Pix Launch" 735 "Caixa TEM", angle(90))
graph export "$output\accounts_time.png", replace


log close

