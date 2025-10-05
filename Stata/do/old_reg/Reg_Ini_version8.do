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
log using "$log\Reg_Ini_version8.log", replace 

/*
*Fake data
global dta "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta"
global output "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output"

use "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/pix/fake_panel_data.dta", clear
set seed 1234
gen random=runiform()
bysort id: gen grupo=cond(random[1]<=1/3,1,cond(random[1]>2/3,3,2))
drop random
set seed 1235
gen random=runiform()
cap drop muni_cd
bysort id: gen muni_cd=cond(random[1]<=1/5,1100015,cond(random[1]>2/5,1100023,cond(random[1]>3/5,1100031,cond(random[1]>4/5,1100049,1100056))))
drop random
*/

use "$dta\Pix_PF_adoption.dta", clear 

*-------------------------------------------------------------------------------
* PART 1: Creating new variables and preparing dataset 
*-------------------------------------------------------------------------------
*merge m:1 id using "\\sbcdf176\PIX_Matheus$\Stata\dta\cpf_bolsa_familia.dta", keep(1 3) nogenerate

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

*cap drop log_trans_sent
*gen log_trans_sent = log(trans_sent) 
* trans_sent sometimes is 0

*-----------------------------------------------
cap label var after_adoption "Adoption"
cap label var after_first_pix_sent "Sender"
cap label var after_first_pix_rec "Receiver"
cap label var expected_distance "Expected Distance"
cap label var dist_bank "Distance to Bank"
cap label var dist_caixa "Distance to Caixa"
cap label var id "ID"
cap label var time_id "Month"
cap label var trans_rec "Transactions Received"
cap label var trans_sent "Transactions Sent"
cap label var value_sent "Value Received"
cap label var value_rec "Value Sent"
cap label var log_trans_sent "Log Transactions Sent"

save "$dta\Pix_PF_adoption_new_variables.dta", replace

*save "$dta\fake_panel_data_new_variables.dta", replace
/*
* Bring back transactions and values
*trans_rec trans_sent value_sent value_rec

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

global list_of_y after_first_pix_sent trans_sent 
*after_adoption after_first_pix_rec after_first_pix
global list_of_treat expected_distance dist_bank
global list_of_types non_bf bf
* bf cad extracad all non_bf

*-----------------------------------------------
* 2.1. Diff and Diff with DistCaixa controling for ExpectedDist
*-----------------------------------------------
* I am not doing this because DistCaixa is highly collinear with ExpectedDist
/*

use "$dta\Pix_PF_adoption_new_variables.dta", replace

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
*/

foreach type of global list_of_types {
	foreach y of global list_of_y {
			
			/*
			* Diff and Diff
			local reg_equation "reghdfe `y' c.dist_caixa##1.after_event c.expected_distance##1.after_event if `type' == 1, absorb(id time_id i.muni_cd#i.time_id) vce(cluster id)"
			reghdfe `y' c.dist_caixa##1.after_event c.expected_distance##1.after_event if `type' == 1, absorb(id time_id i.muni_cd#i.time_id) vce(cluster id)
			*/
			
			* Event Study
			est clear 
			eststo SENT:reghdfe `y' ib6.id_dist##c.dist_caixa ib6.id_dist##c.expected_distance if `type' == 1, absorb(id time_id i.muni_cd#i.time_id) vce(cluster id) allbaselevels
			local reg_equation "reghdfe `y' ib6.id_dist##c.dist_caixa ib6.id_dist##c.expected_distance if `type' == 1, absorb(id time_id i.muni_cd#i.time_id) vce(cluster id) allbaselevels"
			
			* Publish Graph
			coefplot SENT, title(`"Event Study for `l`type''"') subtitle(`"`reg_equation'"', size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) xline(12, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'')  drop(_cons *expected_distance)  coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22" 26.* = "Dec 22", wrap(4)) order(1.* 2.* 3.* 4.* 5.* 6.*) note(`"Regression of `l`y'' on Distance to Caixa, controlled by Expected Distance. 90% Confidence Intervals shown."', size(vsmall))
			* Individual and time fixed effects. Standard Errors clustered at the individual level.
			* rename(*#c.dist_caixa ="")
			graph export "$output\did_distcaixa_`y'_`type'.png", replace		
	}
}
*/

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
				local reg_equation "reghdfe `y' 1.treat##1.after_event if `type' == 1, absorb(id time_id i.muni_cd#i.time_id) vce(cluster id)"
				reghdfe `y' 1.treat##1.after_event if `type' == 1, absorb(id time_id i.muni_cd#i.time_id) vce(cluster id)
				*/
				
				* Event Study
				est clear 
				eststo SENT:reghdfe `y' ib6.id_dist##1.treat if `type' == 1, absorb(id time_id i.muni_cd#i.time_id) vce(cluster id) allbaselevels
				local reg_equation "reghdfe `y' ib6.id_dist##1.treat if `type' == 1, absorb(id time_id i.muni_cd#i.time_id) vce(cluster id) allbaselevels"
				
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
			local reg_equation "reghdfe `y' c.dist_caixa##1.after_event c.dist_caixa2##1.after_event if `type' == 1, absorb(id time_id i.muni_cd#i.time_id) vce(cluster id)"
			reghdfe `y' c.dist_caixa##1.after_event c.dist_caixa2##1.after_event if `type' == 1, absorb(id time_id i.muni_cd#i.time_id) vce(cluster id)
			*/

			* Event Study
			est clear 
			eststo SENT:reghdfe `y' ib6.id_dist##c.dist_caixa ib6.id_dist##c.dist_caixa2 if `type' == 1, absorb(id time_id i.muni_cd#i.time_id) vce(cluster id) allbaselevels
			local reg_equation "reghdfe `y' ib6.id_dist##c.dist_caixa ib6.id_dist##c.dist_caixa2 if `type' == 1, absorb(id time_id i.muni_cd#i.time_id) vce(cluster id) allbaselevels"
			
			* Publish Graph
			coefplot SENT, title(`"Event Study for `l`type''"') subtitle(`"`reg_equation'"', size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) xline(12, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'')  drop(_cons *dist_caixa2)  coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22" 26.* = "Dec 22", wrap(4)) order(1.* 2.* 3.* 4.* 5.* 6.*) note(`"Regression of `l`y'' on Distance to Caixa, controlled by Distance to Caixa^2. 90% Confidence Intervals shown."', size(vsmall))
			* Individual and time fixed effects. Standard Errors clustered at the individual level.
			* rename(*#c.dist_caixa ="")
			graph export "$output\did_distcaixa2_`y'_`type'.png", replace	
			
			
			coefplot SENT, title(`"Event Study for `l`type''"') subtitle(`"`reg_equation'"', size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) xline(12, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'')  drop(_cons *dist_caixa)  coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22" 26.* = "Dec 22", wrap(4)) order(1.* 2.* 3.* 4.* 5.* 6.*) note(`"Regression of `l`y'' on Distance to Caixa^2, controlled by Distance to Caixa. 90% Confidence Intervals shown."', size(vsmall))
			* Individual and time fixed effects. Standard Errors clustered at the individual level.
			* rename(*#c.dist_caixa ="")
			graph export "$output\did_distcaixa22_`y'_`type'.png", replace
			
	}
}

/*

*-----------------------------------------------
* 2.6. Diff and Diff with DistCaixa controling for local Pix adoption
*-----------------------------------------------
use "$dta\Pix_PF_adoption_new_variables.dta", replace
* not the right file. Needs to be the entire mun
collapse (mean) after_adoption after_first_pix_sent after_first_pix_rec, by(muni_cd time_id)

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

*-----------------------------------------------
* 2.7. Diff and Diff with (DistCaixa - ExpectedDist) and (DistCaixa - ExpectedDist)^3
*-----------------------------------------------

use "$dta\Pix_PF_adoption_new_variables.dta", replace

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

*/
cap drop dist_expec
gen dist_expec = dist_caixa - expected_distance
cap drop dist_expec3
gen dist_expec3 = dist_expec^3
foreach type of global list_of_types {
	foreach y of global list_of_y {
			/*
			* Diff and Diff
			local reg_equation "reghdfe `y' c.dist_expec##1.after_event if `type' == 1, absorb(id time_id i.muni_cd#i.time_id) vce(cluster id)"
			reghdfe `y' c.dist_expec##1.after_event if `type' == 1, absorb(id time_id i.muni_cd#i.time_id) vce(cluster id)
			*/

			* Event Study
			est clear 
			eststo SENT:reghdfe `y' ib6.id_dist##c.dist_expec ib6.id_dist##c.dist_expec3 if `type' == 1, absorb(id time_id i.muni_cd#i.time_id) vce(cluster id) allbaselevels
			local reg_equation "reghdfe `y' ib6.id_dist##c.dist_expec ib6.id_dist##c.dist_expec3 if `type' == 1, absorb(id time_id i.muni_cd#i.time_id) vce(cluster id) allbaselevels"
			
			* Publish Graph
			coefplot SENT, title(`"Event Study for `l`type''"') subtitle(`"`reg_equation'"', size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) xline(12, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'')  drop(_cons *dist_expec3)  coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22" 26.* = "Dec 22", wrap(4)) order(1.* 2.* 3.* 4.* 5.* 6.*) note(`"Regression of `l`y'' on Distance to Caixa - Expected Distance, controlled by (Distance to Caixa - Expected Distance)^3."', size(vsmall))
			* Individual and time fixed effects. Standard Errors clustered at the individual level.
			graph export "$output\did_dist_expec3_`y'_`type'.png", replace		
			
			coefplot SENT, title(`"Event Study for `l`type''"') subtitle(`"`reg_equation'"', size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) xline(12, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'')  drop(_cons *dist_expec)  coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22" 26.* = "Dec 22", wrap(4)) order(1.* 2.* 3.* 4.* 5.* 6.*) note(`"Regression of `l`y'' on (Distance to Caixa - Expected Distance)^3, controlled by (Distance to Caixa - Expected Distance)."', size(vsmall))
			* Individual and time fixed effects. Standard Errors clustered at the individual level.
			graph export "$output\did_dist_expec33_`y'_`type'.png", replace		
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
		
		esttab m* using "$output\tables\reg_`type'_`y'.tex", label se star(* 0.10 ** 0.05 *** 0.01) drop(_cons) ///
					s(fixedmm N ymean, label("Mun. - Month FE" ///
					"Observations" "Mean of Dep. Var.")) replace
		esttab m*, label se star(* 0.10 ** 0.05 *** 0.01) drop(_cons) ///
					s(fixedmm N ymean, label("Mun. - Month FE" ///
					"Observations" "Mean of Dep. Var."))			
		display "Regression for Type = `type'. Y = `y'"

		* https://dariotoman.com/teaching/eco403/using_esttab
		* http://scorreia.com/software/reghdfe/faq.html
		
	}
}


*-------------------------------------------------------------------------------
* PART 4: Floods 
*-------------------------------------------------------------------------------

*-------------------------------------------------------------------------------
* Sun and Abraham Implementation with Flood as event
* https://lost-stats.github.io/Model_Estimation/Research_Design/event_study.html
* https://www.princeton.edu/~otorres/DID101.pdf
*-------------------------------------------------------------------------------

use "$dta\danos_informados_monthly_filled_flood.dta", clear
keep id_municipio date number_disasters
rename id_municipio muni_cd
rename date time_id
rename number_disasters flood 
keep if time_id >= ym(2020,11) & time_id <= ym(2022,12) 
sort muni_cd time_id

* Create Event Study variables -----------------------
gen after_flood = 0
bysort muni_cd : egen temp = min(time_id) if flood > 0
bysort muni_cd : egen date_flood = max(temp) 
drop temp
format %tmNN/CCYY date_flood
replace after_flood = 1 if time_id >= date_flood

merge 1:m muni_cd time_id using "$dta\Pix_PF_adoption_new_variables.dta" // I saved a new version with the new variables we created so we dont need to create again. 
drop if _merge == 1

save "$dta\flood_pix.dta", clear



/*

g time_to_treat = time_id - date_flood
replace time_to_treat = 0 if missing(date_flood)
* JOSE: vc pode checar se date_first_pix_sent está missing para nao tratados?
g treat = !missing(date_flood)
g never_treat = missing(date_flood)

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

global list_of_y after_adoption after_first_pix_sent after_first_pix_rec
global list_of_treat expected_distance dist_bank
global list_of_types cad extracad non_bf bf all

/*
merge 1:m muni_cd time_id using "$dta\fake_panel_data_new_variables.dta"
drop if _merge == 1
global list_of_types bf
global list_of_y after_adoption
global list_of_treat expected_distance
*/

foreach var1 of varlist * { 
	local l`var1' : variable label `var1'
	if `"`l`var1''"' == "" {
		local l`var1' "`var1'"
		}
}

foreach type of global list_of_types {
	foreach y of global list_of_y {
		
		eventstudyinteract `y' g_* if `type' == 1, cohort(date_flood) control_cohort(never_treat) absorb(i.id i.time_id) vce(cluster muni_cd) // covariates()

		*https://www.princeton.edu/~otorres/DID101.pdf
		matrix C = e(b_iw)
		mata st_matrix("A",sqrt(diagonal(st_matrix("e(V_iw)"))))
		matrix C = C \ A'
		matrix list C
		coefplot matrix(C[1]), se(C[2]) title("Event Study") subtitle(`"`reg_equation'"', size(vsmall)) order(g_m12 g_m11 g_m10 g_m9 g_m8 g_m7 g_m6 g_m5 g_m4 g_m3 g_m2 g_0 g_1 g_2 g_3 g_4 g_5 g_6 g_7 g_8 g_9 g_10 g_11 g_12) rename(g_m* = "-" g_* = "") vertical yline(0, lcolor(black) lwidth(thin)) xline(11.5, lcolor(black) lwidth(thin) lpattern(dash)) levels(90) graphregion(color(white)) xtitle("Months") ytitle("Bank Accounts") note("The effect of a flood on `l`y'' for `l`type'' following Sun and Abraham (2020)", size(vsmall))
		graph export "$output\flood_`y'_`type'.png", replace
	}
}
*/



*-------------------------------------------------------------------------------
* PART 4: Produce Tables 
*-------------------------------------------------------------------------------



log close
