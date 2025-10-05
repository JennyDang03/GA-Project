* Reg_Ini_version11

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

* Redo the fake variable part, in the same do file create the base and fake base
* At the same time, lets do use normal variables with STD and Mean the same as the original. 

capture log close
log using "$log\Reg_Ini_version11.log", replace 

*-------------------------------------------------------------------------------
* PART 1: Panel Regression + Robustness check
*-------------------------------------------------------------------------------

use "$dta\Pix_PF_adoption_new_variables.dta", replace

/*
global dta "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta"
global output "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output"
use "$dta\fake_panel_data_new_variables.dta", replace
gen road_dist_expec = road_dist_caixa - road_expected_distance

global list_of_y after_first_pix_sent after_first_pix
global list_of_types non_bf bf 
*/
cap drop log_trans_sent
gen log_trans_sent = log(trans_sent+1)
cap drop log_value_sent
gen log_value_sent = log(value_sent+1)

global list_of_y after_first_pix_sent sender trans_sent value_sent log_trans_sent log_value_sent
global list_of_types non_bf bf 


foreach var1 of varlist * { 
	local l`var1' : variable label `var1'
	if `"`l`var1''"' == "" {
		local l`var1' "`var1'"
		}
}

local version "v11_fake"

***********************************************
* Adicionar limitacoes de tempo
*keep if time_id >= ym(2021,5) & time_id <= ym(2021,10) 
***********************************************

cd $output
local i = 1
foreach type of global list_of_types {
	eststo clear
	foreach y of global list_of_y {
		display "Regression for Type = `type'. Y = `y'"
		
		* Dist Caixa and Expected Dist 
		local estname : word `i' of a1 a2 a3 a4 a5 a6 a7 a8 a9 
		eststo `estname': reghdfe `y' road_dist_caixa road_expected_distance if `type' == 1, absorb(muni_cd##time_id) vce(robust)
		quietly estadd local fixedmm "Yes", replace	
		estadd ysumm, replace
		
		* Recentered Distance
		local estname : word `i' of b1 b2 b3 b4 b5 b6 b7 b8 b9 
		eststo `estname': reghdfe `y' road_dist_expec if `type' == 1, absorb(muni_cd##time_id) vce(robust)
		quietly estadd local fixedmm "Yes", replace
		estadd ysumm, replace
		
		* LOG Dist Caixa and Expected Dist 
		local estname : word `i' of c1 c2 c3 c4 c5 c6 c7 c8 c9 
		eststo `estname': reghdfe `y' log_road_dist_caixa log_road_expected_distance if `type' == 1, absorb(muni_cd##time_id) vce(robust)
		quietly estadd local fixedmm "Yes", replace	
		estadd ysumm, replace
		
		* LOG Recentered Distance
		local estname : word `i' of d1 d2 d3 d4 d5 d6 d7 d8 d9 
		eststo `estname': reghdfe `y' log_road_dist_expec if `type' == 1, absorb(muni_cd##time_id) vce(robust)
		quietly estadd local fixedmm "Yes", replace
		estadd ysumm, replace
		
		* Treatment Dist Caixa > Expected Dist
		local estname : word `i' of e1 e2 e3 e4 e5 e6 e7 e8 e9 
		eststo `estname': reghdfe `y' treat_exp if `type' == 1, absorb(muni_cd##time_id) vce(robust)
		quietly estadd local fixedmm "Yes", replace
		estadd ysumm, replace
		
		* Treatment Dist Caixa > Dist Bank
		local estname : word `i' of f1 f2 f3 f4 f5 f6 f7 f8 f9 
		eststo `estname': reghdfe `y' treat_bank if `type' == 1, absorb(muni_cd##time_id) vce(robust)
		quietly estadd local fixedmm "Yes", replace
		estadd ysumm, replace
		
				
		* Transform Dist to caixa in bins 1 to 10, and Expected dist the same
		
		
		
		* Dist to caixa, Dist to closest bank
		
		
		
		* Multiply beta so it is meaning full
		
		
		display "Regression for Type = `type'. Y = `y'"
		
		local estname : word `i' of *1 *2 *3 *4 *5 *6 *7 *8 *9 
		esttab `estname', label se ///
				star(* 0.10 ** 0.05 *** 0.01) drop(_cons) ///
				s(fixedmm N ymean, label("Mun. - Month FE" ///
				"Observations" "Mean of Dep. Var."))			
		cap esttab `estname' using "reg_`type'_`y'_`version'.tex", label se ///
				star(* 0.10 ** 0.05 *** 0.01) drop(_cons) ///
				s(fixedmm N ymean, label("Mun. - Month FE" ///
				"Observations" "Mean of Dep. Var.")) replace
		
		local i = `i' + 1
	}
	display "Regression for Type = `type'."
	
	* Dist Caixa and Expected Dist 
	esttab a*, label se ///
				star(* 0.10 ** 0.05 *** 0.01) drop(_cons) ///
				s(fixedmm N ymean, label("Mun. - Month FE" ///
				"Observations" "Mean of Dep. Var."))			
	cap esttab a* using "reg_`type'_distcaixa_`version'.tex", label se ///
				star(* 0.10 ** 0.05 *** 0.01) drop(_cons) ///
				s(fixedmm N ymean, label("Mun. - Month FE" ///
				"Observations" "Mean of Dep. Var.")) replace
	* Recentered Distance
	esttab b*, label se ///
				star(* 0.10 ** 0.05 *** 0.01) drop(_cons) ///
				s(fixedmm N ymean, label("Mun. - Month FE" ///
				"Observations" "Mean of Dep. Var."))			
	cap esttab b* using "reg_`type'_dist_expec_`version'.tex", label se ///
				star(* 0.10 ** 0.05 *** 0.01) drop(_cons) ///
				s(fixedmm N ymean, label("Mun. - Month FE" ///
				"Observations" "Mean of Dep. Var.")) replace
	* LOG Dist Caixa and LOG Expected Dist 
	esttab c*, label se ///
				star(* 0.10 ** 0.05 *** 0.01) drop(_cons) ///
				s(fixedmm N ymean, label("Mun. - Month FE" ///
				"Observations" "Mean of Dep. Var."))			
	cap esttab c* using "reg_`type'_log_distcaixa_`version'.tex", label se ///
				star(* 0.10 ** 0.05 *** 0.01) drop(_cons) ///
				s(fixedmm N ymean, label("Mun. - Month FE" ///
				"Observations" "Mean of Dep. Var.")) replace
	* LOG Recentered Distance
	esttab d*, label se ///
				star(* 0.10 ** 0.05 *** 0.01) drop(_cons) ///
				s(fixedmm N ymean, label("Mun. - Month FE" ///
				"Observations" "Mean of Dep. Var."))			
	cap esttab d* using "reg_`type'_log_dist_expec_`version'.tex", label se ///
				star(* 0.10 ** 0.05 *** 0.01) drop(_cons) ///
				s(fixedmm N ymean, label("Mun. - Month FE" ///
				"Observations" "Mean of Dep. Var.")) replace
	* Treatment Dist Caixa > Expected Dist
	esttab e*, label se ///
				star(* 0.10 ** 0.05 *** 0.01) drop(_cons) ///
				s(fixedmm N ymean, label("Mun. - Month FE" ///
				"Observations" "Mean of Dep. Var."))			
	cap esttab e* using "reg_`type'_`y'__`restrict'_`version'.tex", label se ///
				star(* 0.10 ** 0.05 *** 0.01) drop(_cons) ///
				s(fixedmm N ymean, label("Mun. - Month FE" ///
				"Observations" "Mean of Dep. Var.")) replace
	* Treatment Dist Caixa > Dist Bank
	esttab f*, label se ///
				star(* 0.10 ** 0.05 *** 0.01) drop(_cons) ///
				s(fixedmm N ymean, label("Mun. - Month FE" ///
				"Observations" "Mean of Dep. Var."))			
	cap esttab f* using "reg_`type'_`y'__`restrict'_`version'.tex", label se ///
				star(* 0.10 ** 0.05 *** 0.01) drop(_cons) ///
				s(fixedmm N ymean, label("Mun. - Month FE" ///
				"Observations" "Mean of Dep. Var.")) replace
	/*			
	esttab g*, label se ///
				star(* 0.10 ** 0.05 *** 0.01) drop(_cons) ///
				s(fixedmm N ymean, label("Mun. - Month FE" ///
				"Observations" "Mean of Dep. Var."))			
	cap esttab g* using "reg_`type'_`y'__`restrict'_`version'.tex", label se ///
				star(* 0.10 ** 0.05 *** 0.01) drop(_cons) ///
				s(fixedmm N ymean, label("Mun. - Month FE" ///
				"Observations" "Mean of Dep. Var.")) replace
	*/			
				
	
	* https://dariotoman.com/teaching/eco403/using_esttab
	* http://scorreia.com/software/reghdfe/faq.html
	
}



*-------------------------------------------------------------------------------
* PART 2: Cross-sectional Regression
*-------------------------------------------------------------------------------

use "$dta\Pix_PF_adoption_new_variables.dta", replace

/*
global dta "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta"
global output "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output"
use "$dta\fake_panel_data_new_variables.dta", replace
gen road_dist_expec = road_dist_caixa - road_expected_distance

global list_of_y after_first_pix_sent after_first_pix
global list_of_types non_bf bf 
*/




cap drop log_trans_sent
gen log_trans_sent = log(trans_sent+1)
cap drop log_value_sent
gen log_value_sent = log(value_sent+1)

global list_of_y after_first_pix_sent sender trans_sent value_sent log_trans_sent log_value_sent
global list_of_types non_bf bf 

display(ym(2020,11))
display(ym(2022,12))

foreach var1 of varlist * { 
	local l`var1' : variable label `var1'
	if `"`l`var1''"' == "" {
		local l`var1' "`var1'"
		}
}

local version "v11_fake"
cd $output

* Sum road_dist_caixa and other important variables

sum road_dist_caixa, detail
local std_dist_caixa = r(sd)
display(`std_dist_caixa')
***********************

foreach type of global list_of_types {
	eststo clear
	foreach y of global list_of_y {
		
		* ----------------------------------------------------------------------
		* Dist Caixa and Expected Dist 
		* ----------------------------------------------------------------------
		cap drop coeff
		gen coeff = .
		cap drop lower
		gen lower = .
		cap drop upper
		gen upper = .

		forvalues t = 730(1)755{
			sum `y' if `type' == 1 & time_id == `t'
			display "Regression for Type = `type'. Y = `y'. Month == `t'"
			
			reghdfe `y' road_dist_caixa road_expected_distance if `type' == 1 & time_id == `t', absorb(muni_cd) vce(robust)
			matrix coef = e(b)
			matrix Var = e(V)
			scalar coefficient = coef[1, 1]  // For road_dist_caixa, for road_expected_distance it would be [1,2]
			scalar std_error = sqrt(Var[1, 1]) // For road_dist_caixa, for road_expected_distance it would be [2,2]
			
			* For 90% CI - 1.645, 95% CI - 1.96, 99% CI - 2.576
		
			*MULTIPLY FOR STANDARD DEV OF DIST TO CAIXA
			replace coeff = `std_dist_caixa'*(coefficient) if time_id == `t'
			replace lower = `std_dist_caixa'*(coefficient - 1.645 * std_error) if time_id == `t'
			replace upper = `std_dist_caixa'*(coefficient + 1.645 * std_error)  if time_id == `t'
		}
		
		
		// Line plot with confidence intervals
		twoway (line coeff time_id, sort) ///
			   (rcap lower upper time_id, lcolor(gs10) lwidth(thin)), ///
			   xtitle("Months", size(medsmall)) ytitle("`l`y''", size(medsmall)) legend(off) ///
			   title("Cross-sectional Regression", size(medium)) ///
			   ylabel(, labsize(vsmall)) ///
			   xlabel(, labsize(vsmall)) ///
			   yline(0, lcolor(black) lwidth(thin)) ///
			   graphregion(color(white)) ///
			   note(`"Regression of `l`y'' on Distance to Caixa, controlled by Expected Distance, for `type'. 90% Confidence Intervals shown."', size(vsmall))
		graph export "distcaixa_`y'_`type'_`version'.png", replace
		
		* ----------------------------------------------------------------------
		* Recentered Distance
		* ----------------------------------------------------------------------
		cap drop coeff
		gen coeff = .
		cap drop lower
		gen lower = .
		cap drop upper
		gen upper = .

		forvalues t = 730(1)755{
			display "Regression for Type = `type'. Y = `y'. Month == `t'"
			
			reghdfe `y' road_dist_expec if `type' == 1 & time_id == `t', absorb(muni_cd) vce(robust)
			
			matrix coef = e(b)
			matrix Var = e(V)
			scalar coefficient = coef[1, 1]  // For road_dist_expec
			scalar std_error = sqrt(Var[1, 1]) // For road_dist_expec
			
			* For 90% CI - 1.645, 95% CI - 1.96, 99% CI - 2.576
			
			*MULTIPLY FOR STANDARD DEV OF DIST TO CAIXA
			replace coeff = `std_dist_caixa'*(coefficient) if time_id == `t'
			replace lower = `std_dist_caixa'*(coefficient - 1.645 * std_error) if time_id == `t'
			replace upper = `std_dist_caixa'*(coefficient + 1.645 * std_error)  if time_id == `t'
		}
		// Line plot with confidence intervals
		twoway (line coeff time_id, sort) ///
			   (rcap lower upper time_id, lcolor(gs10) lwidth(thin)), ///
			   xtitle("Months", size(medsmall)) ytitle("`l`y''", size(medsmall)) legend(off) ///
			   title("Cross-sectional Regression", size(medium)) ///
			   ylabel(, labsize(vsmall)) ///
			   xlabel(, labsize(vsmall)) ///
			   yline(0, lcolor(black) lwidth(thin)) ///
			   graphregion(color(white)) ///
			   note(`"Regression of `l`y'' on Recentered Distance, for `type'. 90% Confidence Intervals shown."', size(vsmall))
		graph export "dist_expec_`y'_`type'_`version'.png", replace
		
		
		
		* LOG Dist Caixa and LOG Expected Dist	
		* LOG Recentered Distance
		* Treatment Dist Caixa > Expected Dist
		* Treatment Dist Caixa > Dist Bank
			
	}
}

log close


* More Sugestions: -------------------------------------------------------------

* "-	Even for distance to Caixa and expected distance, I’d suggest winsorizing at 95th percentile or taking logs, as this variable also likely has a long right tail and those observations are going to have high leverage in your regression. It would be nice to see both of these to see how robust the first stage is."




* Do logit
* Limit to municipalities with access to internet. to increase power. 

* Do only essential regressions

* I need to organized how I did the distances and latitude and longitude. Organize all files. 

*-------------------------------------------------------------------------------



log close