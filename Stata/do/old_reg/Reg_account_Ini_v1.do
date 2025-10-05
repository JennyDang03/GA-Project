
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
log using "$log\Reg_account_Ini_v1.log", replace 

use  "$dta\Pix_PF_adoption.dta", clear
merge 1:1 id time_id using "\\sbcdf176\PIX_Matheus$\Stata\dta\CCS_aux_emerg.dta", keep(1 3) nogenerate
sum n_acc*
sum n_account_stock, d
sum n_account_new, d

bysort after_first_pix_sent: sum n_account_stock

replace n_account_new = 0 if n_account_new == .
replace n_account_stock = 0 if n_account_stock == .

sum n_acc*


gen after_event  =  time_id >= mofd(mdy(5, 1, 2021))
gen dist_event = time_id- mofd(mdy(5, 1, 2021))  // MAIO/21 é o tempo zero
egen id_dist=group(dist_event)
format %tmNN/CCYY time_id
labmask id_dist, values(time_id)

drop if confidence <= 8
drop if aux_emerg_jan_mar21 == 1
keep if grupo == 2
drop if time_id>=750

gen treat = dist_caixa > expected_distance


cap drop dist_expec
gen dist_expec = dist_caixa - expected_distance

foreach y of varlist n_account_new n_account_stock {

			* Event Study
			est clear 
			eststo SENT:reghdfe `y' ib6.id_dist##1.treat , absorb(id time_id i.muni_cd#i.time_id) vce(cluster id) allbaselevels
			local reg_equation "reghdfe `y' ib6.id_dist##1.treat , absorb(id time_id i.muni_cd#i.time_id) vce(cluster id) allbaselevels"
			
			* Publish Graph
			coefplot SENT, title("Event Study ") subtitle(`"`reg_equation'"', size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) xline(12, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'')  drop(_cons)  coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22" 26.* = "Dec 22", wrap(4)) order(1.* 2.* 3.* 4.* 5.* 6.*) note(`"Regression of `l`y'' on Distance to Caixa > Expected Distance. 90% Confidence Intervals shown."', size(vsmall))
			* Individual and time fixed effects. Standard Errors clustered at the individual level.
			graph export "$output\did_dummy_dist_`y'_extracad.png", replace		
}

log close

