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

* More Sugestions: -------------------------------------------------------------

* "-	Even for distance to Caixa and expected distance, I’d suggest winsorizing at 95th percentile or taking logs, as this variable also likely has a long right tail and those observations are going to have high leverage in your regression. It would be nice to see both of these to see how robust the first stage is."




* Do logit
* Limit to municipalities with access to internet. to increase power. 

* Do only essential regressions

* I need to organized how I did the distances and latitude and longitude. Organize all files. 

*-------------------------------------------------------------------------------

capture log close
log using "$log\Reg_Ini_version10_ExtraREGs.log", replace 

*-------------------------------------------------------------------------------
* PART 2: Regressions 
*-------------------------------------------------------------------------------

use "$dta\Pix_PF_adoption_new_variables.dta", replace

*use "$dta\fake_panel_data_new_variables.dta", replace
*global dta "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta"
*global output "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output"

cap drop log_trans_sent
gen log_trans_sent = log(trans_sent+1)
cap drop log_value_sent
gen log_value_sent = log(value_sent+1)

global list_of_y log_trans_sent log_value_sent  
global list_of_types bf 
global list_restriction all0

foreach var1 of varlist * { 
	local l`var1' : variable label `var1'
	if `"`l`var1''"' == "" {
		local l`var1' "`var1'"
		}
}

local version "v10_road_extraregs"

***********************************************
* Adicionar limitacoes de tempo
*keep if time_id >= ym(2021,5) & time_id <= ym(2021,10) 
***********************************************

foreach restrict of global list_restriction {
	keep if `restrict' == 0
	foreach type of global list_of_types {
		foreach y of global list_of_y {
			eststo clear
			display "Regression for Type = `type'. Y = `y'"
			
			eststo m1: reghdfe `y' road_dist_caixa road_expected_distance if `type' == 1, absorb(muni_cd##time_id) vce(robust)
			quietly estadd local fixedmm "Yes", replace
			estadd ysumm, replace
			
			display "Regression for Type = `type'. Y = `y'"
			
			esttab m*, label se ///
						star(* 0.10 ** 0.05 *** 0.01) drop(_cons) ///
						s(fixedmm N ymean, label("Mun. - Month FE" ///
						"Observations" "Mean of Dep. Var."))			
			display "Regression for Type = `type'. Y = `y'"
			
			cd $output
			cap esttab m* using "reg_`type'_`y'__`restrict'_`version'.tex", label se ///
						star(* 0.10 ** 0.05 *** 0.01) drop(_cons) ///
						s(fixedmm N ymean, label("Mun. - Month FE" ///
						"Observations" "Mean of Dep. Var.")) replace
			* https://dariotoman.com/teaching/eco403/using_esttab
			* http://scorreia.com/software/reghdfe/faq.html
			
		}
	}
}

display(ym(2020,11))
display(ym(2021,12))
eststo clear
forvalues t = 730(1)743{
	display "Regression for Type = bf. Y = after_first_pix_sent. Month == `t'"
	
	eststo m`t': reghdfe after_first_pix_sent road_dist_caixa road_expected_distance if bf == 1 & time_id == `t', absorb(muni_cd) vce(robust)
	quietly estadd local fixedmm "Yes", replace
	estadd ysumm, replace
}
esttab m*, label se ///
			star(* 0.10 ** 0.05 *** 0.01) drop(_cons) ///
			s(fixedmm N ymean, label("Mun. FE" ///
			"Observations" "Mean of Dep. Var."))			

cd $output
esttab m* using "reg_sender_adoption.tex", label se star(* 0.10 ** 0.05 *** 0.01) drop(_cons) s(fixedmm N ymean, label("Mun. FE" "Observations" "Mean of Dep. Var.")) replace


* ChatGPT helping me to create a graph with repeated cross-sectional regressions

tempfile coefficients
tempfile lower_bound
tempfile upper_bound

// Create empty datasets to store coefficients, lower bounds, and upper bounds
foreach var of varlist independent_var1 independent_var2 {
    gen `var'_coeff = .
    gen `var'_lower = .
    gen `var'_upper = .
}

forval year = `start_year'/`end_year' {
    // Step 3: Subset the data for the current time period
    keep if year == `year'
    
    // Step 4: Run a cross-sectional regression
    regress dependent_var independent_var1 independent_var2
    
    // Step 5: Store regression results
    estimates store model_`year'
    
    // Get coefficients, lower bounds, and upper bounds for each variable
    estimates restore model_`year'
    
    foreach var of varlist independent_var1 independent_var2 {
        quietly replace `var'_coeff = _b[`var'] in `year'
        quietly replace `var'_lower = _b[`var'] - 1.96 * _se[`var'] in `year'
        quietly replace `var'_upper = _b[`var'] + 1.96 * _se[`var'] in `year'
    }
}

// Step 6: Create the graph
local vars independent_var1 independent_var2

foreach var of local vars {
    gen year_str = string(year)
    
    // Line plot with confidence intervals
    twoway (line `var'_coeff year_str, sort) ///
           (rcap `var'_lower `var'_upper year_str, lcolor(gs10) lwidth(thin)), ///
           xtitle("Year") ytitle("Coefficient") legend(off) ///
           title("Coefficient of `var' Over Time")
           
    // Save the graph for each variable
    graph export "`var'_graph.png", replace
}






log close