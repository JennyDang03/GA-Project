
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
log using "$log\Reg_account_Ini_v2.log", replace 
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

save "$dta\Pix_PF_adoption_bank_account.dta", clear
*--------------------------------------------------
/*
*-------------------------------------------------------------------------------
* Sun and Abraham Implementation with Sent as event
* https://lost-stats.github.io/Model_Estimation/Research_Design/event_study.html
* https://www.princeton.edu/~otorres/DID101.pdf
*-------------------------------------------------------------------------------

g time_to_treat = time_id - date_first_pix_sent
replace time_to_treat = 0 if missing(date_first_pix_sent)
* JOSE: vc pode checar se date_first_pix_sent está missing para nao tratados?
g treat = !missing(date_first_pix_sent)
g never_treat = missing(date_first_pix_sent)


* Sun/Abraham suggest to not include the extreme leads/lags that have smaller number of cases.
* Create relative-time indicators for treated groups by hand
* ignore distant leads and lags due to lack of observations
* (note this assumes any effects outside these leads/lags is 0)
tab time_to_treat
forvalues t = -12(1)12 {
	if `t' < -1 {
		local tname = abs(`t')
		g g_m`tname' = time_to_treat == `t'
	}
	else if `t' >= 0 {
		g g_`t' = time_to_treat == `t'
	}
}

eventstudyinteract n_account_stock g_*, cohort(date_first_pix_sent) control_cohort(never_treat) absorb(i.id i.time_id i.muni_cd#i.time_id) vce(cluster id) // covariates()


*https://www.princeton.edu/~otorres/DID101.pdf
matrix C = e(b_iw)
mata st_matrix("A",sqrt(diagonal(st_matrix("e(V_iw)"))))
matrix C = C \ A'
matrix list C
coefplot matrix(C[1]), se(C[2]) title("Event Study ") subtitle(`"`reg_equation'"', size(vsmall)) order(g_m12 g_m11 g_m10 g_m9 g_m8 g_m7 g_m6 g_m5 g_m4 g_m3 g_m2 g_0 g_1 g_2 g_3 g_4 g_5 g_6 g_7 g_8 g_9 g_10 g_11 g_12) rename(g_m* = "-" g_* = "") vertical yline(0, lcolor(black) lwidth(thin)) xline(11.5, lcolor(black) lwidth(thin) lpattern(dash)) levels(90) graphregion(color(white)) xtitle("Months") ytitle("Bank Accounts") note("The effect of learning to send Pix on Bank Accounts following Sun and Abraham (2020)", size(vsmall))
graph export "$output\bank_account_sender.png", replace

/*
* https://lost-stats.github.io/Model_Estimation/Research_Design/event_study.html
* Get effects and plot
* as of this writing, the coefficient matrix is unlabeled and so we can't do _b[] and _se[]
* instead we'll work with the results table
matrix T = r(table)
g coef = 0 if time_to_treat == -1
g se = 0 if time_to_treat == -1
forvalues t = -12(1)12 {
	if `t' < -1 {
		local tname = abs(`t')
		replace coef = T[1,colnumb(T,"g_m`tname'")] if time_to_treat == `t'
		replace se = T[2,colnumb(T,"g_m`tname'")] if time_to_treat == `t'
	}
	else if `t' >= 0 {
		replace coef =  T[1,colnumb(T,"g_`t'")] if time_to_treat == `t'
		replace se = T[2,colnumb(T,"g_`t'")] if time_to_treat == `t'
	}
}

* Make confidence intervals
g ci_top = coef+1.645*se // 1.96 for 95%
g ci_bottom = coef - 1.645*se // 1.96 for 95%

keep time_to_treat coef se ci_*
duplicates drop

sort time_to_treat
keep if inrange(time_to_treat, -12, 12)

* Create connected scatterplot of coefficients
* with CIs included with rcap
* and a line at 0 both horizontally and vertically
summ ci_top
local top_range = r(max)
summ ci_bottom
local bottom_range = r(min)

twoway (sc coef time_to_treat, connect(line)) ///
	(rcap ci_top ci_bottom time_to_treat)	///
	(function y = 0, range(time_to_treat)) ///
	(function y = 0, range(`bottom_range' `top_range') horiz), ///
	xtitle("Time to Treatment with Sun and Abraham (2020) Estimation") caption("90% Confidence Intervals Shown")
*/

*/
log close

