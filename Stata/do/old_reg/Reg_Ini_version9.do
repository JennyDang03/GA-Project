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
do "$do\monta_base_reg_v2.do"

* More Sugestions: -------------------------------------------------------------
* Do logit
* Limit to municipalities with access to internet. to increase power. 
*-------------------------------------------------------------------------------

capture log close
log using "$log\Reg_Ini_version9.log", replace 

*-------------------------------------------------------------------------------
* PART 1: Differences in Differences 
*-------------------------------------------------------------------------------

use "$dta\Pix_PF_adoption_new_variables.dta", replace

*use "$dta\fake_panel_data_new_variables.dta", replace
*global dta "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta"
*global output "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output"

global list_of_y after_first_pix_sent sender trans_sent value_sent  // after_adoption after_first_pix_rec after_first_pix
global list_of_treat expected_distance dist_bank
global list_of_types bf // non_bf cad extracad all
global list_restriction all0 previous_takers // Important that all0 comes earlier in the sequence of the list

foreach var1 of varlist * { 
	local l`var1' : variable label `var1'
	if `"`l`var1''"' == "" {
		local l`var1' "`var1'"
		}
}
local version "v9"
	foreach restrict of global list_restriction {
	
		if "`restrict'" == "all0" {
			keep if `restrict' == 0
			local april2021 5
			local october2021 11
			local base 1
		}
		if "`restrict'" == "previous_takers" {
			* Drop observations before april 2021 if restricted 
			drop if time_id < ym(2021,4)
			keep if `restrict' == 0
			local april2021 0.5
			local october2021 6
			local base 6
		}
		
		foreach y of global list_of_y {
		*---------------------------------------------------------------------------
		* Triple Difference:
		*---------------------------------------------------------------------------	
			*-----------------------------------------------------------------------
			* Recentered Distance
			*-----------------------------------------------------------------------
			est clear 
			eststo SENT:reghdfe `y' ib`base'.id_dist##c.dist_expec##1.non_bf, absorb(id time_id) vce(cluster id) allbaselevels
			* absorb -> i.muni_cd#i.time_id
			local reg_equation "reghdfe `y' ib`base'.id_dist##c.dist_expec##i.non_bf, absorb(id time_id) vce(cluster id) allbaselevels"
			
			* Publish Graph
			coefplot SENT, vertical title("Triple Differences") subtitle(`"`reg_equation'"', size(vsmall))  yline(0, lcolor(black) lwidth(thin)) xline(`april2021', lcolor(black) lwidth(thin) lpattern(dash)) xline(`october2021', lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white))  xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'')    coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22" 26.* = "Dec 22", wrap(4)) order(1.* 2.* 3.* 4.* 5.* 6.*) note(`"Triple Difference of `l`y'' on Recentered Distance, ExtraCad and Cad vs. Bolsa Familia. 90% Confidence Intervals shown."', size(vsmall)) baselevels keep(*.id_dist#*.non_bf#*.dist_expec) drop(_cons)
			
			graph export "$output\triple_dist_expec_`y'_`restrict'_`version'.png", replace	
			
			*-----------------------------------------------------------------------
			* Treatment
			*-----------------------------------------------------------------------
					
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
				
				est clear 
				eststo SENT:reghdfe `y' ib`base'.id_dist##1.treat##1.non_bf, absorb(id time_id) vce(cluster id) allbaselevels
				* absorb -> i.muni_cd#i.time_id
				local reg_equation "reghdfe `y' ib`base'.id_dist##i.treat##i.non_bf, absorb(id time_id) vce(cluster id) allbaselevels"
				
				* Publish Graph
				coefplot SENT, vertical title("Triple Differences") subtitle(`"`reg_equation'"', size(vsmall))  yline(0, lcolor(black) lwidth(thin)) xline(`april2021', lcolor(black) lwidth(thin) lpattern(dash)) xline(`october2021', lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white))  xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'')    coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22" 26.* = "Dec 22", wrap(4)) order(1.* 2.* 3.* 4.* 5.* 6.*) note(`"Triple Difference of `l`y'' on Treatment defined as Distance to Caixa > `l`var_treat''. 90% Confidence Intervals shown."', size(vsmall)) baselevels keep(*.id_dist#*.non_bf#*.treat) drop(_cons)
				
				graph export "$output\triple_treat_`treatment_name'_`y'_`restrict'_`version'.png", replace
			}

		*---------------------------------------------------------------------------
		* Difference in Differences:
		*---------------------------------------------------------------------------
			foreach type of global list_of_types {
				if "`restrict'" == "all0" {
					local april2021 6
					local october2021 12
				}
				if "`restrict'" == "previous_takers" {
					* Drop observations before april 2021 if restricted 
					drop if time_id < ym(2021,4)
					keep if `restrict' == 0
					local april2021 0.5
					local october2021 7
				}
				
				*-----------------------------------------------------------------------
				* DistCaixa controling for ExpectedDist
				*-----------------------------------------------------------------------
				/*
				* Diff and Diff
				local reg_equation "reghdfe `y' c.dist_caixa##1.after_event c.expected_distance##1.after_event if `type' == 1, absorb(id time_id) vce(cluster id)"
				reghdfe `y' c.dist_caixa##1.after_event c.expected_distance##1.after_event if `type' == 1, absorb(id time_id) vce(cluster id)
				*/
				
				* Event Study
				est clear 
				eststo SENT:reghdfe `y' ib`base'.id_dist##c.dist_caixa ib`base'.id_dist##c.expected_distance if `type' == 1, absorb(id time_id) vce(cluster id) allbaselevels
				local reg_equation "reghdfe `y' ib`base'.id_dist##c.dist_caixa ib`base'.id_dist##c.expected_distance if `type' == 1, absorb(id time_id) vce(cluster id) allbaselevels"
				
				* Publish Graph
				coefplot SENT, title(`"Event Study for `l`type''"') subtitle(`"`reg_equation'"', size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(`april2021', lcolor(black) lwidth(thin) lpattern(dash)) xline(`october2021', lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'')  drop(_cons *expected_distance)  coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22" 26.* = "Dec 22", wrap(4)) order(1.* 2.* 3.* 4.* 5.* 6.*) note(`"Regression of `l`y'' on Distance to Caixa, controlled by Expected Distance. 90% Confidence Intervals shown."', size(vsmall))
				graph export "$output\did_distcaixa_`y'_`type'_`restrict'_`version'.png", replace	
				
				*-----------------------------------------------------------------------
				* Recentered Distance
				*-----------------------------------------------------------------------
				
				/*
				* Diff and Diff
				local reg_equation "reghdfe `y' c.dist_expec##1.after_event if `type' == 1, absorb(id time_id) vce(cluster id)"
				reghdfe `y' c.dist_expec##1.after_event if `type' == 1, absorb(id time_id) vce(cluster id)
				*/

				* Event Study
				est clear 
				eststo SENT:reghdfe `y' ib`base'.id_dist##c.dist_expec if `type' == 1, absorb(id time_id) vce(cluster id) allbaselevels
				local reg_equation "reghdfe `y' ib`base'.id_dist##c.dist_expec if `type' == 1, absorb(id time_id) vce(cluster id) allbaselevels"
				
				* Publish Graph
				coefplot SENT, title(`"Event Study for `l`type''"') subtitle(`"`reg_equation'"', size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(`april2021', lcolor(black) lwidth(thin) lpattern(dash)) xline(`october2021', lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'')  drop(_cons)  coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22" 26.* = "Dec 22", wrap(4)) order(1.* 2.* 3.* 4.* 5.* 6.*) note(`"Regression of `l`y'' on Distance to Caixa - Expected Distance. 90% Confidence Intervals shown."', size(vsmall))
				* Individual and time fixed effects. Standard Errors clustered at the individual level.
				graph export "$output\did_dist_expec_`y'_`type'_`restrict'_`version'.png", replace	
				
				*-----------------------------------------------------------------------
				* DistCaixa
				*-----------------------------------------------------------------------
				
				/*
				* Diff and Diff
				local reg_equation "reghdfe `y' c.dist_caixa##1.after_event if `type' == 1, absorb(id time_id) vce(cluster id)"
				reghdfe `y' c.dist_caixa##1.after_event if `type' == 1, absorb(id time_id) vce(cluster id)
				*/
				
				* Event Study
				est clear 
				eststo SENT:reghdfe `y' ib`base'.id_dist##c.dist_caixa if `type' == 1, absorb(id time_id) vce(cluster id) allbaselevels
				local reg_equation "reghdfe `y' ib`base'.id_dist##c.dist_caixa if `type' == 1, absorb(id time_id) vce(cluster id) allbaselevels"
				
				* Publish Graph
				coefplot SENT, title(`"Event Study for `l`type''"') subtitle(`"`reg_equation'"', size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(`april2021', lcolor(black) lwidth(thin) lpattern(dash)) xline(`october2021', lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'')  drop(_cons)  coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22" 26.* = "Dec 22", wrap(4)) order(1.* 2.* 3.* 4.* 5.* 6.*) note(`"Regression of `l`y'' on Distance to Caixa. 90% Confidence Intervals shown."', size(vsmall))
				* Individual and time fixed effects. Standard Errors clustered at the individual level.
				* rename(*#c.dist_caixa ="")
				graph export "$output\did_distcaixaonly_`y'_`type'_`restrict'_`version'.png", replace
			
				*-----------------------------------------------------------------------
				* Treatment
				*-----------------------------------------------------------------------
						
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
					/*
					* Diff and Diff
					local reg_equation "reghdfe `y' 1.treat##1.after_event if `type' == 1, absorb(id time_id) vce(cluster id)"
					reghdfe `y' 1.treat##1.after_event if `type' == 1, absorb(id time_id) vce(cluster id)
					*/
					
					* Event Study
					est clear 
					eststo SENT:reghdfe `y' ib`base'.id_dist##1.treat if `type' == 1, absorb(id time_id) vce(cluster id) allbaselevels
					local reg_equation "reghdfe `y' ib`base'.id_dist##1.treat if `type' == 1, absorb(id time_id) vce(cluster id) allbaselevels"
					
					* Publish Graph
					coefplot SENT, title(`"Event Study for `l`type''"') subtitle(`"`reg_equation'"', size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(`april2021', lcolor(black) lwidth(thin) lpattern(dash)) xline(`october2021', lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'')  drop(_cons)  coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22" 26.* = "Dec 22", wrap(4)) order(1.* 2.* 3.* 4.* 5.* 6.*) note(`"Regression of `l`y'' on Treatment defined as Distance to Caixa > `l`var_treat''. 90% Confidence Intervals shown."', size(vsmall))
					* Individual and time fixed effects. Standard Errors clustered at the individual level.
					graph export "$output\did_treat_`treatment_name'_`y'_`type'_`restrict'_`version'.png", replace
				}
			}
		}	
	}


*-------------------------------------------------------------------------------
* PART 2: Regressions 
*-------------------------------------------------------------------------------
/*
use "$dta\Pix_PF_adoption_new_variables.dta", replace

*use "$dta\fake_panel_data_new_variables.dta", replace
*global dta "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta"
*global output "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output"

global list_of_y after_first_pix_sent sender trans_sent value_sent  // after_adoption after_first_pix_rec after_first_pix
global list_of_treat expected_distance dist_bank
global list_of_types non_bf bf // cad extracad all
global list_restriction all0 previous_takers // Important that all0 comes earlier in the sequence of the list

foreach var1 of varlist * { 
	local l`var1' : variable label `var1'
	if `"`l`var1''"' == "" {
		local l`var1' "`var1'"
		}
}

local version "v9"
***********************************************
* Adicionar limitacoes de tempo
keep if time_id >= ym(2021,5) & time_id <= ym(2021,10) 
***********************************************

foreach restrict of global list_restriction {
	keep if `restrict' == 0
	foreach type of global list_of_types {
		foreach y of global list_of_y {
			eststo clear
			display "Regression for Type = `type'. Y = `y'"
			* Normal Regressions
			eststo m0: reghdfe `y' dist_caixa if `type' == 1, absorb(muni_cd##time_id) vce(robust)
			quietly estadd local fixedmm "Yes", replace	
			estadd ysumm, replace
			
			eststo m1: reghdfe `y' dist_caixa expected_distance if `type' == 1, absorb(muni_cd##time_id) vce(robust)
			quietly estadd local fixedmm "Yes", replace
			estadd ysumm, replace
			
			eststo m2: reghdfe `y' dist_expec if `type' == 1, absorb(muni_cd##time_id) vce(robust)
			quietly estadd local fixedmm "Yes", replace
			estadd ysumm, replace
			/*
					
			eststo m3: reghdfe `y' dist_expec dist_expec2 if `type' == 1, absorb(muni_cd##time_id) vce(robust)
			quietly estadd local fixedmm "Yes", replace
			estadd ysumm, replace
			
			eststo m4: reghdfe `y' dist_expec dist_expec3 if `type' == 1, absorb(muni_cd##time_id) vce(robust)
			quietly estadd local fixedmm "Yes", replace
			estadd ysumm, replace
			
			eststo m5: reghdfe `y' dist_caixa_bank if `type' == 1, absorb(muni_cd##time_id) vce(robust)
			quietly estadd local fixedmm "Yes", replace
			estadd ysumm, replace

			eststo m6: reghdfe `y' dist_caixa_bank dist_caixa_bank2 if `type' == 1, absorb(muni_cd##time_id) vce(robust)
			quietly estadd local fixedmm "Yes", replace
			estadd ysumm, replace
		
			eststo m7: reghdfe `y' dist_caixa_bank dist_caixa_bank3 if `type' == 1, absorb(muni_cd##time_id) vce(robust)
			quietly estadd local fixedmm "Yes", replace	
			estadd ysumm, replace
			
			eststo m8: reghdfe `y' dist_caixa dist_caixa2 if `type' == 1, absorb(muni_cd##time_id) vce(robust)
			quietly estadd local fixedmm "Yes", replace	
			estadd ysumm, replace
			
			*/
			
			eststo m9: reghdfe `y' treat_exp if `type' == 1, absorb(muni_cd##time_id) vce(robust)
			quietly estadd local fixedmm "Yes", replace
			estadd ysumm, replace
			
			eststo m10: reghdfe `y' treat_bank if `type' == 1, absorb(muni_cd##time_id) vce(robust)
			quietly estadd local fixedmm "Yes", replace
			estadd ysumm, replace
			
			display "Regression for Type = `type'. Y = `y'"
			
			esttab m*, label se ///
						star(* 0.10 ** 0.05 *** 0.01) drop(_cons) ///
						s(fixedmm N ymean, label("Mun. - Month FE" ///
						"Observations" "Mean of Dep. Var."))			
			display "Regression for Type = `type'. Y = `y'"
			
			cap esttab m* using "$output\reg_`type'_`y'__`restrict'_`version'.tex", label se ///
						star(* 0.10 ** 0.05 *** 0.01) drop(_cons) ///
						s(fixedmm N ymean, label("Mun. - Month FE" ///
						"Observations" "Mean of Dep. Var.")) replace
			* https://dariotoman.com/teaching/eco403/using_esttab
			* http://scorreia.com/software/reghdfe/faq.html
			
		}
	}
}
*/

*-------------------------------------------------------------------------------
* PART 3: Differences in Differences with extra controls
*-------------------------------------------------------------------------------

use "$dta\Pix_PF_adoption_new_variables.dta", replace

*use "$dta\fake_panel_data_new_variables.dta", replace
*global dta "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta"
*global output "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output"

global list_of_y after_first_pix_sent sender trans_sent value_sent  // after_adoption after_first_pix_rec after_first_pix
global list_of_treat expected_distance dist_bank
global list_of_types bf non_bf // bf non_bf cad extracad all
global list_restriction all0 previous_takers // Important that all0 comes earlier in the sequence of the list

foreach var1 of varlist * { 
	local l`var1' : variable label `var1'
	if `"`l`var1''"' == "" {
		local l`var1' "`var1'"
		}
}
local version "v9extracontrols"
	foreach restrict of global list_restriction {
	
		if "`restrict'" == "all0" {
			keep if `restrict' == 0
			local april2021 5
			local october2021 11
			local base 1
		}
		if "`restrict'" == "previous_takers" {
			* Drop observations before april 2021 if restricted 
			drop if time_id < ym(2021,4)
			keep if `restrict' == 0
			local april2021 0.5
			local october2021 6
			local base 6
		}
		
		foreach y of global list_of_y {
		*---------------------------------------------------------------------------
		* Triple Difference:
		*---------------------------------------------------------------------------	
			*-----------------------------------------------------------------------
			* Recentered Distance
			*-----------------------------------------------------------------------
			est clear 
			eststo SENT:reghdfe `y' ib`base'.id_dist##c.dist_expec##1.non_bf, absorb(id time_id muni_cd#time_id) vce(cluster id) allbaselevels
			* absorb -> i.muni_cd#i.time_id
			local reg_equation "reghdfe `y' ib`base'.id_dist##c.dist_expec##i.non_bf, absorb(id time_id muni_cd#time_id) vce(cluster id) allbaselevels"
			
			* Publish Graph
			coefplot SENT, vertical title("Triple Differences") subtitle(`"`reg_equation'"', size(vsmall))  yline(0, lcolor(black) lwidth(thin)) xline(`april2021', lcolor(black) lwidth(thin) lpattern(dash)) xline(`october2021', lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white))  xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'')    coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22" 26.* = "Dec 22", wrap(4)) order(1.* 2.* 3.* 4.* 5.* 6.*) note(`"Triple Difference of `l`y'' on Recentered Distance, ExtraCad and Cad vs. Bolsa Familia. 90% Confidence Intervals shown."', size(vsmall)) baselevels keep(*.id_dist#*.non_bf#*.dist_expec) drop(_cons)
			
			graph export "$output\triple_dist_expec_`y'_`restrict'_`version'.png", replace	
			
			*-----------------------------------------------------------------------
			* Treatment
			*-----------------------------------------------------------------------
					
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
				
				est clear 
				eststo SENT:reghdfe `y' ib`base'.id_dist##1.treat##1.non_bf, absorb(id time_id muni_cd#time_id) vce(cluster id) allbaselevels
				* absorb -> i.muni_cd#i.time_id
				local reg_equation "reghdfe `y' ib`base'.id_dist##i.treat##i.non_bf, absorb(id time_id muni_cd#time_id) vce(cluster id) allbaselevels"
				
				* Publish Graph
				coefplot SENT, vertical title("Triple Differences") subtitle(`"`reg_equation'"', size(vsmall))  yline(0, lcolor(black) lwidth(thin)) xline(`april2021', lcolor(black) lwidth(thin) lpattern(dash)) xline(`october2021', lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white))  xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'')    coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22" 26.* = "Dec 22", wrap(4)) order(1.* 2.* 3.* 4.* 5.* 6.*) note(`"Triple Difference of `l`y'' on Treatment defined as Distance to Caixa > `l`var_treat''. 90% Confidence Intervals shown."', size(vsmall)) baselevels keep(*.id_dist#*.non_bf#*.treat) drop(_cons)
				
				graph export "$output\triple_treat_`treatment_name'_`y'_`restrict'_`version'.png", replace
			}

		*---------------------------------------------------------------------------
		* Difference in Differences:
		*---------------------------------------------------------------------------
			foreach type of global list_of_types {
				if "`restrict'" == "all0" {
					local april2021 6
					local october2021 12
				}
				if "`restrict'" == "previous_takers" {
					* Drop observations before april 2021 if restricted 
					drop if time_id < ym(2021,4)
					keep if `restrict' == 0
					local april2021 0.5
					local october2021 7
				}
				
				*-----------------------------------------------------------------------
				* DistCaixa controling for ExpectedDist
				*-----------------------------------------------------------------------
				/*
				* Diff and Diff
				local reg_equation "reghdfe `y' c.dist_caixa##1.after_event c.expected_distance##1.after_event if `type' == 1, absorb(id time_id) vce(cluster id)"
				reghdfe `y' c.dist_caixa##1.after_event c.expected_distance##1.after_event if `type' == 1, absorb(id time_id) vce(cluster id)
				*/
				
				* Event Study
				est clear 
				eststo SENT:reghdfe `y' ib`base'.id_dist##c.dist_caixa ib`base'.id_dist##c.expected_distance if `type' == 1, absorb(id time_id muni_cd#time_id) vce(cluster id) allbaselevels
				local reg_equation "reghdfe `y' ib`base'.id_dist##c.dist_caixa ib`base'.id_dist##c.expected_distance if `type' == 1, absorb(id time_id muni_cd#time_id) vce(cluster id) allbaselevels"
				
				* Publish Graph
				coefplot SENT, title(`"Event Study for `l`type''"') subtitle(`"`reg_equation'"', size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(`april2021', lcolor(black) lwidth(thin) lpattern(dash)) xline(`october2021', lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'')  drop(_cons *expected_distance)  coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22" 26.* = "Dec 22", wrap(4)) order(1.* 2.* 3.* 4.* 5.* 6.*) note(`"Regression of `l`y'' on Distance to Caixa, controlled by Expected Distance. 90% Confidence Intervals shown."', size(vsmall))
				graph export "$output\did_distcaixa_`y'_`type'_`restrict'_`version'.png", replace	
				
				*-----------------------------------------------------------------------
				* Recentered Distance
				*-----------------------------------------------------------------------
				
				/*
				* Diff and Diff
				local reg_equation "reghdfe `y' c.dist_expec##1.after_event if `type' == 1, absorb(id time_id) vce(cluster id)"
				reghdfe `y' c.dist_expec##1.after_event if `type' == 1, absorb(id time_id) vce(cluster id)
				*/

				* Event Study
				est clear 
				eststo SENT:reghdfe `y' ib`base'.id_dist##c.dist_expec if `type' == 1, absorb(id time_id muni_cd#time_id) vce(cluster id) allbaselevels
				local reg_equation "reghdfe `y' ib`base'.id_dist##c.dist_expec if `type' == 1, absorb(id time_id muni_cd#time_id) vce(cluster id) allbaselevels"
				
				* Publish Graph
				coefplot SENT, title(`"Event Study for `l`type''"') subtitle(`"`reg_equation'"', size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(`april2021', lcolor(black) lwidth(thin) lpattern(dash)) xline(`october2021', lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'')  drop(_cons)  coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22" 26.* = "Dec 22", wrap(4)) order(1.* 2.* 3.* 4.* 5.* 6.*) note(`"Regression of `l`y'' on Distance to Caixa - Expected Distance. 90% Confidence Intervals shown."', size(vsmall))
				* Individual and time fixed effects. Standard Errors clustered at the individual level.
				graph export "$output\did_dist_expec_`y'_`type'_`restrict'_`version'.png", replace	
				
				*-----------------------------------------------------------------------
				* DistCaixa
				*-----------------------------------------------------------------------
				
				/*
				* Diff and Diff
				local reg_equation "reghdfe `y' c.dist_caixa##1.after_event if `type' == 1, absorb(id time_id) vce(cluster id)"
				reghdfe `y' c.dist_caixa##1.after_event if `type' == 1, absorb(id time_id) vce(cluster id)
				*/
				
				* Event Study
				est clear 
				eststo SENT:reghdfe `y' ib`base'.id_dist##c.dist_caixa if `type' == 1, absorb(id time_id muni_cd#time_id) vce(cluster id) allbaselevels
				local reg_equation "reghdfe `y' ib`base'.id_dist##c.dist_caixa if `type' == 1, absorb(id time_id muni_cd#time_id) vce(cluster id) allbaselevels"
				
				* Publish Graph
				coefplot SENT, title(`"Event Study for `l`type''"') subtitle(`"`reg_equation'"', size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(`april2021', lcolor(black) lwidth(thin) lpattern(dash)) xline(`october2021', lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'')  drop(_cons)  coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22" 26.* = "Dec 22", wrap(4)) order(1.* 2.* 3.* 4.* 5.* 6.*) note(`"Regression of `l`y'' on Distance to Caixa. 90% Confidence Intervals shown."', size(vsmall))
				* Individual and time fixed effects. Standard Errors clustered at the individual level.
				* rename(*#c.dist_caixa ="")
				graph export "$output\did_distcaixaonly_`y'_`type'_`restrict'_`version'.png", replace
			
				*-----------------------------------------------------------------------
				* Treatment
				*-----------------------------------------------------------------------
						
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
					/*
					* Diff and Diff
					local reg_equation "reghdfe `y' 1.treat##1.after_event if `type' == 1, absorb(id time_id) vce(cluster id)"
					reghdfe `y' 1.treat##1.after_event if `type' == 1, absorb(id time_id) vce(cluster id)
					*/
					
					* Event Study
					est clear 
					eststo SENT:reghdfe `y' ib`base'.id_dist##1.treat if `type' == 1, absorb(id time_id muni_cd#time_id) vce(cluster id) allbaselevels
					local reg_equation "reghdfe `y' ib`base'.id_dist##1.treat if `type' == 1, absorb(id time_id muni_cd#time_id) vce(cluster id) allbaselevels"
					
					* Publish Graph
					coefplot SENT, title(`"Event Study for `l`type''"') subtitle(`"`reg_equation'"', size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(`april2021', lcolor(black) lwidth(thin) lpattern(dash)) xline(`october2021', lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'')  drop(_cons)  coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22" 26.* = "Dec 22", wrap(4)) order(1.* 2.* 3.* 4.* 5.* 6.*) note(`"Regression of `l`y'' on Treatment defined as Distance to Caixa > `l`var_treat''. 90% Confidence Intervals shown."', size(vsmall))
					* Individual and time fixed effects. Standard Errors clustered at the individual level.
					graph export "$output\did_treat_`treatment_name'_`y'_`type'_`restrict'_`version'.png", replace
				}
			}
		}	
	}



log close