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
log using "$log\Reg_Ini_version5.log", replace 

/*
*Fake data
global dta "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta"
use "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/pix/fake_panel_data.dta", clear
set seed 1234
gen random=runiform()
bysort id: gen grupo=cond(random[1]<=1/3,1,cond(random[1]>2/3,3,2))
drop random
set seed 1235
gen random=runiform()
cap drop mun_cd
bysort id: gen mun_cd=cond(random[1]<=1/5,1100015,cond(random[1]>2/5,1100023,cond(random[1]>3/5,1100031,cond(random[1]>4/5,1100049,1100056))))
drop random
*/

use "$dta\Pix_PF_adoption.dta", clear 

*-------------------------------------------------------------------------------
* PART 1: Creating new variables and preparing dataset 
*-------------------------------------------------------------------------------
merge m:1 id using "\\sbcdf176\PIX_Matheus$\Stata\dta\cpf_bolsa_familia.dta", keep(1 3) nogenerate

tab confidence
drop if confidence <= 8
drop if aux_emerg_jan_mar21 == 1

*-----------------------------------------------
* Can you double check whether this is correct?
gen bf = 0
replace bf = 1 if grupo == 3
label var bf "Bolsa Familia"

gen cad = 0
replace cad = 1 if grupo == 1
label var cad "Cad"

gen extracad = 0
replace extracad = 1 if grupo == 2
label var extracad "ExtraCad"

keep if bf == 1 | cad == 1 | extracad == 1
gen all = 1
gen non_bf = 0
replace non_bf = 1 if cad == 1 | extracad == 1
label var non_bf "Cad and ExtraCad"
label var all "All"

*-----------------------------------------------

gen after_event  =  time_id >= mofd(mdy(5, 1, 2021))
gen dist_event = time_id- mofd(mdy(5, 1, 2021))  // MAIO/21 é o tempo zero
egen id_dist=group(dist_event)
format %tmNN/CCYY time_id
labmask id_dist, values(time_id)

*-----------------------------------------------

gen after_adoption = 0
replace after_adoption = 1 if after_first_pix_rec == 1 & after_first_pix_sent == 1
bysort id : egen temp = min(time_id) if after_adoption > 0
bysort id : egen date_adoption = max(temp) 
drop temp
format %tmNN/CCYY date_adoption

*-----------------------------------------------
cap label var after_adoption "Adoption"
cap label var after_first_pix_sent "Senders"
cap label var after_first_pix_rec "Receivers"
cap label var expected_distance "Expected Distance"
cap label var dist_bank "Distance to the Closest Bank"
cap label var dist_caixa "Distance to the Closest Caixa"
cap label var id "ID"
cap label var time_id "Month"

save "$dta\Pix_PF_adoption_new_variables.dta", replace

*save "$dta\fake_panel_data_new_variables.dta", replace
/*
* Bring back transactions and values
value_sent
value_rec
transactions_sent
transactions_rec

* Do logit
* Do triple difference - with people not on covid relief. 
*/

*-------------------------------------------------------------------------------
* PART 2: Diff and Diff and Event Studies 
*-------------------------------------------------------------------------------
use "$dta\Pix_PF_adoption_new_variables.dta", replace

foreach var1 of varlist * { 
	local l`var1' : variable label `var1'
	if `"`l`var1''"' == "" {
		local l`var1' "`var1'"
		}
}

global list_of_y after_adoption after_first_pix_sent after_first_pix_rec
global list_of_treat expected_distance dist_bank
global list_of_types cad extracad non_bf
* bf cad extracad all non_bf

*-----------------------------------------------
* 2.1. Diff and Diff with DistCaixa controling for ExpectedDist
*-----------------------------------------------

use "$dta\Pix_PF_adoption_new_variables.dta", replace

foreach var1 of varlist * { 
	local l`var1' : variable label `var1'
	if `"`l`var1''"' == "" {
		local l`var1' "`var1'"
		}
}

global list_of_y after_adoption after_first_pix_sent after_first_pix_rec
global list_of_treat expected_distance dist_bank
global list_of_types cad extracad non_bf
* bf cad extracad all non_bf

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
*/

foreach type of global list_of_types {
	foreach y of global list_of_y {
			
			/*
			* Diff and Diff
			local reg_equation "reghdfe `y' c.dist_caixa##1.after_event c.expected_distance##1.after_event if `type' == 1, absorb(id time_id i.mun_cd#i.time_id) vce(cluster id)"
			reghdfe `y' c.dist_caixa##1.after_event c.expected_distance##1.after_event if `type' == 1, absorb(id time_id i.mun_cd#i.time_id) vce(cluster id)
			*/
			
			/*
			eststo `y'_`type'_1 // Store Estimates
			quietly estadd local fixedm "Yes", replace  // Month FE
			quietly estadd local fixedi "Yes", replace  // Individual FE
			estadd ysumm, replace
			* Publish Table later
			#delimit ;
			esttab `y'_`type'_1, 
				label se star(* 0.10 ** 0.05 *** 0.01) 
				s(fixedm fixedi N ymean, 
				label("Month FE" "Individual FE" "Observations" "Mean of Dep. Var."));
			#delimit cr
			* usign "$output\tables\did.tex", replace booktabs
			* save as well on overleaf folder and put the command \include on the latex to make the tables update automatically in the future. 
			* Improve this table in the future. I am having problems storing the results. 
			*/
			* Event Study
			est clear 
			eststo SENT:reghdfe `y' ib6.id_dist##c.dist_caixa ib6.id_dist##c.expected_distance if `type' == 1, absorb(id time_id i.mun_cd#i.time_id) vce(cluster id) allbaselevels
			local reg_equation "reghdfe `y' ib6.id_dist##c.dist_caixa ib6.id_dist##c.expected_distance if `type' == 1, absorb(id time_id i.mun_cd#i.time_id) vce(cluster id) allbaselevels"
			
			* Publish Graph
			coefplot SENT, title(`"Event Study for `l`type''"') subtitle(`"`reg_equation'"', size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) xline(12, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'')  drop(_cons *expected_distance)  coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22" 26.* = "Dec 22", wrap(4)) order(1.* 2.* 3.* 4.* 5.* 6.*) note(`"Regression of `l`y'' on Distance to Caixa, controlled by Expected Distance. 90% Confidence Intervals shown."', size(vsmall))
			* Individual and time fixed effects. Standard Errors clustered at the individual level.
			* rename(*#c.dist_caixa ="")
			graph export "$output\did_distcaixa_`y'_`type'.png", replace		
	}
}


*-----------------------------------------------
* 2.2. Diff and Diff with (DistCaixa - ExpectedDist)
*-----------------------------------------------

use "$dta\Pix_PF_adoption_new_variables.dta", replace

foreach var1 of varlist * { 
	local l`var1' : variable label `var1'
	if `"`l`var1''"' == "" {
		local l`var1' "`var1'"
		}
}

global list_of_y after_adoption after_first_pix_sent after_first_pix_rec
global list_of_treat expected_distance dist_bank
global list_of_types cad extracad non_bf
* bf cad extracad all non_bf

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

*/
cap drop dist_expec
gen dist_expec = dist_caixa - expected_distance
foreach type of global list_of_types {
	foreach y of global list_of_y {
			/*
			* Diff and Diff
			local reg_equation "reghdfe `y' c.dist_expec##1.after_event if `type' == 1, absorb(id time_id i.mun_cd#i.time_id) vce(cluster id)"
			reghdfe `y' c.dist_expec##1.after_event if `type' == 1, absorb(id time_id i.mun_cd#i.time_id) vce(cluster id)
			*/
			/*
			eststo `y'_`type'_2 // Store Estimates
			quietly estadd local fixedm "Yes", replace  // Month FE
			quietly estadd local fixedi "Yes", replace  // Individual FE
			estadd ysumm, replace
			* Publish Table later
			#delimit ;
			esttab `y'_`type'_2, 
				label se star(* 0.10 ** 0.05 *** 0.01) 
				s(fixedm fixedi N ymean, 
				label("Month FE" "Individual FE" "Observations" "Mean of Dep. Var."));
			#delimit cr
			* usign "$output\tables\did.tex", replace booktabs
			* save as well on overleaf folder and put the command \include on the latex to make the tables update automatically in the future. 
			* Improve this table in the future. I am having problems storing the results. 
			*/
			* Event Study
			est clear 
			eststo SENT:reghdfe `y' ib6.id_dist##c.dist_expec if `type' == 1, absorb(id time_id i.mun_cd#i.time_id) vce(cluster id) allbaselevels
			local reg_equation "reghdfe `y' ib6.id_dist##c.dist_expec if `type' == 1, absorb(id time_id i.mun_cd#i.time_id) vce(cluster id) allbaselevels"
			
			* Publish Graph
			coefplot SENT, title(`"Event Study for `l`type''"') subtitle(`"`reg_equation'"', size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) xline(12, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'')  drop(_cons)  coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22" 26.* = "Dec 22", wrap(4)) order(1.* 2.* 3.* 4.* 5.* 6.*) note(`"Regression of `l`y'' on Distance to Caixa - Expected Distance. 90% Confidence Intervals shown."', size(vsmall))
			* Individual and time fixed effects. Standard Errors clustered at the individual level.
			graph export "$output\did_dist_expec_`y'_`type'.png", replace		
	}
}


*-----------------------------------------------
* 2.3. Diff and Diff with Treatment
*-----------------------------------------------

use "$dta\Pix_PF_adoption_new_variables.dta", replace

foreach var1 of varlist * { 
	local l`var1' : variable label `var1'
	if `"`l`var1''"' == "" {
		local l`var1' "`var1'"
		}
}

global list_of_y after_adoption after_first_pix_sent after_first_pix_rec
global list_of_treat expected_distance dist_bank
global list_of_types cad extracad non_bf
* bf cad extracad all non_bf

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
*/

*Define Treatment

foreach var_treat of global list_of_treat{
	cap drop treat
	gen treat = 0
	replace treat = 1 if dist_caixa > `var_treat'
	if "`var_treat'" == "expected_distance" {
	    local treatment_name "exp"
	}
	if "`var_treat'" == "dist_bank" {
	    local treatment_name "bank"
	}
	display "`treatment_name'"
	foreach type of global list_of_types {
		foreach y of global list_of_y {
				
				/*
				* Diff and Diff
				local reg_equation "reghdfe `y' 1.treat##1.after_event if `type' == 1, absorb(id time_id i.mun_cd#i.time_id) vce(cluster id)"
				reghdfe `y' 1.treat##1.after_event if `type' == 1, absorb(id time_id i.mun_cd#i.time_id) vce(cluster id)
				*/
				/*
				estimates store m_`treatment_name'_`y'_`type'_3
				estfe m_`treatment_name'_`y'_`type'_3, labels(id "Individual FE" time_id "Month FE" after_event "Time" treat "Distance to Caixa > `l`var_treat''" treat#after_event "test")
				esttab m_`treatment_name'_`y'_`type'_3, indicate(`r(indicate_fe)') label se star(* 0.10 ** 0.05 *** 0.01)
				display "4"
				* http://scorreia.com/software/reghdfe/faq.html#how-can-i-combine-reghdfe-with-esttab-or-estout
				*/
				/*
				* https://dariotoman.com/teaching/eco403/using_esttab
				eststo `treatment_name'_`y'_`type'_3 // Store Estimates
				quietly estadd local fixedm "Yes", replace  // Month FE
				quietly estadd local fixedi "Yes", replace  // Individual FE
				estadd ysumm, replace
				* Publish Table later
				#delimit ;
				esttab `treatment_name'_`y'_`type'_3, 
					label se star(* 0.10 ** 0.05 *** 0.01) 
					s(fixedm fixedi N ymean, 
					label("Month FE" "Individual FE" "Observations" "Mean of Dep. Var."));
				#delimit cr
				* usign "$output\tables\did.tex", replace booktabs
				* save as well on overleaf folder and put the command \include on the latex to make the tables update automatically in the future. 
				* Improve this table in the future. I am having problems storing the results. 
				*/
				
				* Event Study
				est clear 
				eststo SENT:reghdfe `y' ib6.id_dist##1.treat if `type' == 1, absorb(id time_id i.mun_cd#i.time_id) vce(cluster id) allbaselevels
				local reg_equation "reghdfe `y' ib6.id_dist##1.treat if `type' == 1, absorb(id time_id i.mun_cd#i.time_id) vce(cluster id) allbaselevels"
				
				* Publish Graph
				coefplot SENT, title(`"Event Study for `l`type''"') subtitle(`"`reg_equation'"', size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) xline(12, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'')  drop(_cons)  coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22" 26.* = "Dec 22", wrap(4)) order(1.* 2.* 3.* 4.* 5.* 6.*) note(`"Regression of `l`y'' on Treatment defined as Distance to Caixa > `l`var_treat''. 90% Confidence Intervals shown."', size(vsmall))
				* Individual and time fixed effects. Standard Errors clustered at the individual level.
				graph export "$output\did_treat_`treatment_name'_`y'_`type'.png", replace		
		}
	}

}


*-----------------------------------------------
* 2.4. Diff and Diff with DistCaixa, no other controls
*-----------------------------------------------
use "$dta\Pix_PF_adoption_new_variables.dta", replace

foreach var1 of varlist * { 
	local l`var1' : variable label `var1'
	if `"`l`var1''"' == "" {
		local l`var1' "`var1'"
		}
}

global list_of_y after_adoption after_first_pix_sent after_first_pix_rec
global list_of_treat expected_distance dist_bank
global list_of_types cad extracad non_bf
* bf cad extracad all non_bf

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
*/

foreach type of global list_of_types {
	foreach y of global list_of_y {
			/*
			* Diff and Diff
			local reg_equation "reghdfe `y' c.dist_caixa##1.after_event if `type' == 1, absorb(id time_id i.mun_cd#i.time_id) vce(cluster id)"
			reghdfe `y' c.dist_caixa##1.after_event if `type' == 1, absorb(id time_id i.mun_cd#i.time_id) vce(cluster id)
			*/
			/*
			eststo `y'_`type'_1 // Store Estimates
			quietly estadd local fixedm "Yes", replace  // Month FE
			quietly estadd local fixedi "Yes", replace  // Individual FE
			estadd ysumm, replace
			* Publish Table later
			#delimit ;
			esttab `y'_`type'_1, 
				label se star(* 0.10 ** 0.05 *** 0.01) 
				s(fixedm fixedi N ymean, 
				label("Month FE" "Individual FE" "Observations" "Mean of Dep. Var."));
			#delimit cr
			* usign "$output\tables\did.tex", replace booktabs
			* save as well on overleaf folder and put the command \include on the latex to make the tables update automatically in the future. 
			* Improve this table in the future. I am having problems storing the results. 
			*/
			* Event Study
			est clear 
			eststo SENT:reghdfe `y' ib6.id_dist##c.dist_caixa if `type' == 1, absorb(id time_id i.mun_cd#i.time_id) vce(cluster id) allbaselevels
			local reg_equation "reghdfe `y' ib6.id_dist##c.dist_caixa if `type' == 1, absorb(id time_id i.mun_cd#i.time_id) vce(cluster id) allbaselevels"
			
			* Publish Graph
			coefplot SENT, title(`"Event Study for `l`type''"') subtitle(`"`reg_equation'"', size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) xline(12, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'')  drop(_cons *expected_distance)  coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22" 26.* = "Dec 22", wrap(4)) order(1.* 2.* 3.* 4.* 5.* 6.*) note(`"Regression of `l`y'' on Distance to Caixa. 90% Confidence Intervals shown."', size(vsmall))
			* Individual and time fixed effects. Standard Errors clustered at the individual level.
			* rename(*#c.dist_caixa ="")
			graph export "$output\did_distcaixaonly_`y'_`type'.png", replace		
	}
}


*-----------------------------------------------
* 2.5. Diff and Diff with DistCaixa controling for DistCaixa^2
*-----------------------------------------------

use "$dta\Pix_PF_adoption_new_variables.dta", replace

foreach var1 of varlist * { 
	local l`var1' : variable label `var1'
	if `"`l`var1''"' == "" {
		local l`var1' "`var1'"
		}
}

global list_of_y after_adoption after_first_pix_sent after_first_pix_rec
global list_of_treat expected_distance dist_bank
global list_of_types cad extracad non_bf
* bf cad extracad all non_bf

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
*/

cap drop dist_caixa2
gen dist_caixa2 = dist_caixa^2
foreach type of global list_of_types {
	foreach y of global list_of_y {
			/*
			* Diff and Diff
			local reg_equation "reghdfe `y' c.dist_caixa##1.after_event c.dist_caixa2##1.after_event if `type' == 1, absorb(id time_id i.mun_cd#i.time_id) vce(cluster id)"
			reghdfe `y' c.dist_caixa##1.after_event c.dist_caixa2##1.after_event if `type' == 1, absorb(id time_id i.mun_cd#i.time_id) vce(cluster id)
			*/
			/*
			eststo `y'_`type'_1 // Store Estimates
			quietly estadd local fixedm "Yes", replace  // Month FE
			quietly estadd local fixedi "Yes", replace  // Individual FE
			estadd ysumm, replace
			* Publish Table later
			#delimit ;
			esttab `y'_`type'_1, 
				label se star(* 0.10 ** 0.05 *** 0.01) 
				s(fixedm fixedi N ymean, 
				label("Month FE" "Individual FE" "Observations" "Mean of Dep. Var."));
			#delimit cr
			* usign "$output\tables\did.tex", replace booktabs
			* save as well on overleaf folder and put the command \include on the latex to make the tables update automatically in the future. 
			* Improve this table in the future. I am having problems storing the results. 
			*/
			* Event Study
			est clear 
			eststo SENT:reghdfe `y' ib6.id_dist##c.dist_caixa ib6.id_dist##c.dist_caixa2 if `type' == 1, absorb(id time_id i.mun_cd#i.time_id) vce(cluster id) allbaselevels
			local reg_equation "reghdfe `y' ib6.id_dist##c.dist_caixa ib6.id_dist##c.dist_caixa2 if `type' == 1, absorb(id time_id i.mun_cd#i.time_id) vce(cluster id) allbaselevels"
			
			* Publish Graph
			coefplot SENT, title(`"Event Study for `l`type''"') subtitle(`"`reg_equation'"', size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) xline(12, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'')  drop(_cons *dist_caixa2)  coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22" 26.* = "Dec 22", wrap(4)) order(1.* 2.* 3.* 4.* 5.* 6.*) note(`"Regression of `l`y'' on Distance to Caixa, controlled by Distance to Caixa^2. 90% Confidence Intervals shown."', size(vsmall))
			* Individual and time fixed effects. Standard Errors clustered at the individual level.
			* rename(*#c.dist_caixa ="")
			graph export "$output\did_distcaixa2_`y'_`type'.png", replace		
	}
}

/*

*-----------------------------------------------
* 2.6. Diff and Diff with DistCaixa controling for local Pix adoption
*-----------------------------------------------
use "$dta\Pix_PF_adoption_new_variables.dta", replace
* not the right file. Needs to be the entire mun
collapse (mean) after_adoption after_first_pix_sent after_first_pix_rec, by(mun_cd time_id)

use "$dta\Pix_PF_adoption_new_variables.dta", replace

foreach var1 of varlist * { 
	local l`var1' : variable label `var1'
	if `"`l`var1''"' == "" {
		local l`var1' "`var1'"
		}
}

global list_of_y after_adoption after_first_pix_sent after_first_pix_rec
global list_of_treat expected_distance dist_bank
global list_of_types bf cad extracad all non_bf

*/

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

global list_of_y after_adoption after_first_pix_sent after_first_pix_rec
global list_of_treat expected_distance dist_bank
global list_of_types cad extracad non_bf
* bf cad extracad all non_bf

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
*/

cap drop dist_expec
gen dist_expec = dist_caixa - expected_distance
cap drop dist_caixa2
gen dist_caixa2 = dist_caixa^2

* Adicionar limitacoes de tempo e pessoas 

* I will not control for individual fixed effect but for Mun_cd and other variables
foreach type of global list_of_types {
	foreach y of global list_of_y {
			* Normal Regressions
			
			reghdfe `y' dist_caixa if `type' == 1, absorb(i.mun_cd##i.time_id) vce(cluster mun_cd)
			
			reghdfe `y' dist_caixa expected_distance if `type' == 1, absorb(i.mun_cd##i.time_id) vce(cluster mun_cd)
			
			reghdfe `y' dist_expec if `type' == 1, absorb(i.mun_cd##i.time_id) vce(cluster mun_cd)
			
			reghdfe `y' dist_caixa dist_bank if `type' == 1, absorb(i.mun_cd##i.time_id) vce(cluster mun_cd)
			
			reghdfe `y' dist_caixa dist_caixa2 expected_distance if `type' == 1, absorb(i.mun_cd##i.time_id) vce(cluster mun_cd)
			
			/*
			eststo `y'_`type'_1 // Store Estimates
			quietly estadd local fixedm "Yes", replace  // Month FE
			quietly estadd local fixedi "Yes", replace  // Individual FE
			estadd ysumm, replace
			* Publish Table later
			#delimit ;
			esttab `y'_`type'_1, 
				label se star(* 0.10 ** 0.05 *** 0.01) 
				s(fixedm fixedi N ymean, 
				label("Month FE" "Individual FE" "Observations" "Mean of Dep. Var."));
			#delimit cr
			* usign "$output\tables\did.tex", replace booktabs
			* save as well on overleaf folder and put the command \include on the latex to make the tables update automatically in the future. 
			* Improve this table in the future. I am having problems storing the results. 
			*/
			
	}
}








*-------------------------------------------------------------------------------
* PART 4: Floods 
*-------------------------------------------------------------------------------


use "$dta\danos_informados_monthly_filled_flood.dta", clear
keep id_municipio date number_disasters
rename id_municipio mun_cd
rename date time_id
rename number_disasters flood 
keep if time_id >= ym(2020,11) & time_id <= ym(2022,12) 
sort mun_cd time_id

* Create Event Study variables -----------------------
gen after_flood = 0
bysort mun_cd : egen temp = min(time_id) if flood > 0
bysort mun_cd : egen date_flood = max(temp) 
drop temp
format %tmNN/CCYY date_flood
replace after_flood = 1 if time_id >= date_flood

gen dist_flood = time_id - date_flood 
replace dist_flood = 0 if missing(date_flood)
egen id_dist_flood=group(dist_flood)
labmask id_dist_flood, values(dist_flood)

cap drop shifted_dist_flood
summ dist_flood
gen shifted_dist_flood = dist_flood - r(min)
*summ shifted_dist_flood if dist_flood == 0
*local true_neg11 = r(mean)

merge 1:m mun_cd time_id using "$dta\Pix_PF_adoption_new_variables.dta" // I saved a new version with the new variables we created so we dont need to create again. 
drop if _merge == 1
global list_of_y after_adoption after_first_pix_sent after_first_pix_rec
global list_of_treat expected_distance dist_bank
global list_of_types cad extracad non_bf bf all

/*
merge 1:m mun_cd time_id using "$dta\fake_panel_data_new_variables.dta"
drop if _merge == 1
global list_of_types bf
global list_of_y after_adoption
global list_of_treat expected_distance
*/

foreach var1 of varlist *
	local l`var1' : variable label `var1'
	if `"`l`var1''"' == "" {
		local l`var1' "`var1'"
		}
}

local xline = 16 // I dont know why stata is behaving like this
local base = 25 // I dont know why stata is behaving like this 
foreach type of global list_of_types {
	foreach y of global list_of_y {
		* Event study
		est clear 
		eststo SENT:reghdfe `y' ib`base'.id_dist_flood if `type' == 1, absorb(id time_id) vce(cluster mun_cd) allbaselevels // Cluster at mun_cd because the treatment was taken at mun_cd
		* I tried absorbing i.mun_cd#i.time_id but it didnt work
		local reg_equation "reghdfe `y' ib`base'.id_dist_flood if `type' == 1, absorb(id time_id) vce(cluster mun_cd) allbaselevels"
		*absorb(i.mun_cd##i.time_id) vce(cluster mun_cd)
		
		* Limit the graph to include fewer months. 
		coefplot SENT, title(`"Event Study of Floods for `l`type''"') subtitle(`"`reg_equation'"', size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) levels(90) xline(`xline', lcolor(black) lwidth(thin) lpattern(dash)) graphregion(color(white)) xtitle("Months") xlab(, labsize(vsmall))  ytitle(`l`y'') note("90% Confidence Intervals Shown")  drop(_cons) baselevels
		graph export "$output\flood_`y'_`type'.png", replace
	}
}



*-------------------------------------------------------------------------------
* PART 4: Produce Tables 
*-------------------------------------------------------------------------------



log close
