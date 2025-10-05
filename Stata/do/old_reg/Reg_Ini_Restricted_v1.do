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
log using "$log\Reg_Ini_Restricted_v1.log", replace 

*-------------------------------------------------------------------------------
* PART 2: Diff and Diff and Event Studies 
*-------------------------------------------------------------------------------
use "$dta\Pix_PF_adoption_new_variables.dta", replace

*use "$dta\fake_panel_data_new_variables.dta", replace

*** In this version we will condition the regressions on having send no Pix before april.  
gen temp = 0
replace temp = 1 if after_first_pix_sent == 1 & time_id == mofd(mdy(4, 30, 2021))
bysort id: egen delete = max(temp)
drop if delete == 1
drop temp
drop delete
* ------------------------------------------------------------------------------

foreach var1 of varlist * { 
	local l`var1' : variable label `var1'
	if `"`l`var1''"' == "" {
		local l`var1' "`var1'"
		}
}

global list_of_y after_first_pix_sent trans_sent 
*after_adoption after_first_pix_rec after_first_pix
global list_of_treat expected_distance dist_bank
global list_of_types non_bf bf
* bf cad extracad all non_bf

*-----------------------------------------------
* 2.2. Diff and Diff with (DistCaixa - ExpectedDist)
*-----------------------------------------------

/*
use "$dta\fake_panel_data_new_variables.dta", replace
global list_of_types bf
global list_of_y after_adoption
global list_of_treat expected_distance

foreach var1 of varlist * { 
	local l`var1' : variable label `var1'
	if `"`l`var1''"' == "" {
		local l`var1' "`var1'"
		}
}
rename mun_cd muni_cd
gen after_first_pix_sent = after_adoption

*/
cap drop dist_expec
gen dist_expec = dist_caixa - expected_distance
foreach type of global list_of_types {
	foreach y of global list_of_y {
			/*
			* Diff and Diff
			local reg_equation "reghdfe `y' c.dist_expec##1.after_event if `type' == 1, absorb(id time_id i.muni_cd#i.time_id) vce(cluster id)"
			reghdfe `y' c.dist_expec##1.after_event if `type' == 1, absorb(id time_id i.muni_cd#i.time_id) vce(cluster id)
			*/

			* Event Study
			est clear 
			eststo SENT:reghdfe `y' ib6.id_dist##c.dist_expec if `type' == 1, absorb(id time_id i.muni_cd#i.time_id) vce(cluster id) allbaselevels
			local reg_equation "reghdfe `y' ib6.id_dist##c.dist_expec if `type' == 1, absorb(id time_id i.muni_cd#i.time_id) vce(cluster id) allbaselevels"
			
			* Publish Graph
			coefplot SENT, title(`"Event Study for `l`type''"') subtitle(`"`reg_equation'"', size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) xline(12, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'')  drop(_cons)  coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22" 26.* = "Dec 22", wrap(4)) order(1.* 2.* 3.* 4.* 5.* 6.*) note(`"Regression of `l`y'' on Distance to Caixa - Expected Distance. 90% Confidence Intervals shown."', size(vsmall))
			* Individual and time fixed effects. Standard Errors clustered at the individual level.
			graph export "$output\did_dist_expec_`y'_`type'_restricted.png", replace		
	}
}

*-----------------------------------------------
* 2.4. Diff and Diff with DistCaixa, no other controls
*-----------------------------------------------

use "$dta\Pix_PF_adoption_new_variables.dta", replace

*** In this version we will condition the regressions on having send no Pix before april.  
gen temp = 0
replace temp = 1 if after_first_pix_sent == 1 & time_id == mofd(mdy(4, 30, 2021))
bysort id: egen delete = max(temp)
drop if delete == 1
drop temp
drop delete
* ------------------------------------------------------------------------------

foreach var1 of varlist * { 
	local l`var1' : variable label `var1'
	if `"`l`var1''"' == "" {
		local l`var1' "`var1'"
		}
}

/*
use "$dta\fake_panel_data_new_variables.dta", replace
global list_of_types bf
global list_of_y after_adoption
global list_of_treat expected_distance

foreach var1 of varlist * { 
	local l`var1' : variable label `var1'
	if `"`l`var1''"' == "" {
		local l`var1' "`var1'"
		}
}
rename mun_cd muni_cd
gen after_first_pix_sent = after_adoption

*/

foreach type of global list_of_types {
	foreach y of global list_of_y {
			/*
			* Diff and Diff
			local reg_equation "reghdfe `y' c.dist_caixa##1.after_event if `type' == 1, absorb(id time_id i.muni_cd#i.time_id) vce(cluster id)"
			reghdfe `y' c.dist_caixa##1.after_event if `type' == 1, absorb(id time_id i.muni_cd#i.time_id) vce(cluster id)
			*/
			* Event Study
			est clear 
			eststo SENT:reghdfe `y' ib6.id_dist##c.dist_caixa if `type' == 1, absorb(id time_id i.muni_cd#i.time_id) vce(cluster id) allbaselevels
			local reg_equation "reghdfe `y' ib6.id_dist##c.dist_caixa if `type' == 1, absorb(id time_id i.muni_cd#i.time_id) vce(cluster id) allbaselevels"
			
			* Publish Graph
			coefplot SENT, title(`"Event Study for `l`type''"') subtitle(`"`reg_equation'"', size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) xline(12, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'')  drop(_cons *expected_distance)  coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22" 26.* = "Dec 22", wrap(4)) order(1.* 2.* 3.* 4.* 5.* 6.*) note(`"Regression of `l`y'' on Distance to Caixa. 90% Confidence Intervals shown."', size(vsmall))
			* Individual and time fixed effects. Standard Errors clustered at the individual level.
			* rename(*#c.dist_caixa ="")
			graph export "$output\did_distcaixaonly_`y'_`type'_restricted.png", replace		
	}
}

*-------------------------------------------------------------------------------
* PART 3: Normal Regressions 
*-------------------------------------------------------------------------------
use "$dta\Pix_PF_adoption_new_variables.dta", replace

foreach var1 of varlist * { 
	local l`var1' : variable label `var1'
	if `"`l`var1''"' == "" {
		local l`var1' "`var1'"
		}
}

*** In this version we will condition the regressions on having send no Pix before april.  
gen temp = 0
replace temp = 1 if after_first_pix_sent == 1 & time_id == ym(2021,4)
bysort id: egen delete = max(temp)
drop if delete == 1
drop temp
drop delete
* ------------------------------------------------------------------------------

/*
use "$dta\fake_panel_data_new_variables.dta", replace
global list_of_types bf
global list_of_y after_adoption
global list_of_treat expected_distance

foreach var1 of varlist * { 
	local l`var1' : variable label `var1'
	if `"`l`var1''"' == "" {
		local l`var1' "`var1'"
		}
}
gen dist_bank = expected_distance
gen muni_cd = mun_cd
*/

* Adicionar limitacoes de tempo
keep if time_id >= ym(2021,5) & time_id <= ym(2021,10) 

cap drop dist_expec
gen dist_expec = dist_caixa - expected_distance
cap drop dist_expec2
gen dist_expec2 = dist_expec*abs(dist_expec)
cap drop dist_expec3
gen dist_expec3 = dist_expec^3
cap drop dist_caixa2
gen dist_caixa2 = dist_caixa^2
cap drop dist_caixa_bank
gen dist_caixa_bank = dist_caixa - dist_bank
cap drop dist_caixa_bank2
gen dist_caixa_bank2 = dist_caixa_bank*abs(dist_caixa_bank)
cap drop dist_caixa_bank3
gen dist_caixa_bank3 = dist_caixa_bank^3
cap label variable dist_expec "Recentered Dist"
cap label variable dist_expec2 "Recentered Dist^2"
cap label variable dist_expec3 "Recentered Dist^3"
cap label variable dist_caixa2  "Dist to Caixa^2"
cap label variable dist_caixa3  "Dist to Caixa^3"
cap label variable dist_caixa_bank "DistCaixa-DistBank"
cap label variable dist_caixa_bank2 "(DistCaixa-DistBank)^2"
cap label variable dist_caixa_bank3 "(DistCaixa-DistBank)^3"
cap label var trans_rec "Transactions Received"
cap label var trans_sent "Transactions Sent"
cap label var value_sent "Value Received"
cap label var value_rec "Value Sent"
cap label var dist_bank "Dist to Bank"
cap label var dist_caixa "Dist to Caixa"

foreach type of global list_of_types {
	foreach y of global list_of_y {
		display "Regression for Type = `type'. Y = `y'"
		* Normal Regressions
		eststo m0: reghdfe `y' dist_caixa if `type' == 1, absorb(muni_cd#time_id) vce(robust)
		quietly estadd local fixedmm "Yes", replace	
		estadd ysumm, replace
		
		eststo m1: reghdfe `y' dist_caixa dist_caixa2 if `type' == 1, absorb(muni_cd#time_id) vce(robust)
		quietly estadd local fixedmm "Yes", replace	
		estadd ysumm, replace
		
		eststo m2: reghdfe `y' dist_expec if `type' == 1, absorb(muni_cd#time_id) vce(robust)
		quietly estadd local fixedmm "Yes", replace
		estadd ysumm, replace
		
		eststo m3: reghdfe `y' dist_expec dist_expec2 if `type' == 1, absorb(muni_cd#time_id) vce(robust)
		quietly estadd local fixedmm "Yes", replace
		estadd ysumm, replace
		
		eststo m4: reghdfe `y' dist_expec dist_expec3 if `type' == 1, absorb(muni_cd#time_id) vce(robust)
		quietly estadd local fixedmm "Yes", replace
		estadd ysumm, replace
		
		eststo m5: reghdfe `y' dist_caixa_bank if `type' == 1, absorb(muni_cd#time_id) vce(robust)
		quietly estadd local fixedmm "Yes", replace
		estadd ysumm, replace

		eststo m6: reghdfe `y' dist_caixa_bank dist_caixa_bank2 if `type' == 1, absorb(muni_cd#time_id) vce(robust)
		quietly estadd local fixedmm "Yes", replace
		estadd ysumm, replace

		eststo m7: reghdfe `y' dist_caixa_bank dist_caixa_bank3 if `type' == 1, absorb(muni_cd#time_id) vce(robust)
		quietly estadd local fixedmm "Yes", replace	
		estadd ysumm, replace
		
		display "Regression for Type = `type'. Y = `y'"
		
		
		esttab m*, label se ///
					star(* 0.10 ** 0.05 *** 0.01) drop(_cons) ///
					s(fixedmm N ymean, label("Mun. - Month FE" ///
					"Observations" "Mean of Dep. Var."))			
		display "Regression for Type = `type'. Y = `y'"
		
		cap esttab m* using "$output\reg_`type'_`y'_restricted.tex", label se ///
					star(* 0.10 ** 0.05 *** 0.01) drop(_cons) ///
					s(fixedmm N ymean, label("Mun. - Month FE" ///
					"Observations" "Mean of Dep. Var.")) replace
		* https://dariotoman.com/teaching/eco403/using_esttab
		* http://scorreia.com/software/reghdfe/faq.html
		
	}
}




*-------------------------------------------------------------------------------
* PART 4: Produce Tables 
*-------------------------------------------------------------------------------



log close
