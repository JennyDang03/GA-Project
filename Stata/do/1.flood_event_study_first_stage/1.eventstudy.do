
/******************************************************************************

*       Last revisor: 	MS		

*		Clean Pix transaction data from the Central Bank

				
*******************************************************************************/
		
		
	clear
	clear mata
	clear matrix
	

	foreach path in D:\Dropbox C:\Dropbox C:\Users\mathe\Dropbox {
		capture cd `path'
		if _rc == 0 macro def path `path'
	}
	
	
	****************************************************************************	
	
	* Prepare for the event study
	
	****************************************************************************
	
	use "$path\RESEARCH\Central Bank - Pix\dta\transactions_natural_disasters.dta",replace
	
	
	*https://lost-stats.github.io/Model_Estimation/Research_Design/event_study.html
	
	* create the lag/lead for treated codmun
	* fill in control obs with 0
	* This allows for the interaction between `treat` and `time_to_treat` to occur for each codmun.
	* Otherwise, there may be some NAs and the estimations will be off.
	gen dummy_disaster = 0 
	replace dummy_disaster=1 if number_disasters >= 1
	sort codmun date
	
	by codmun: gen post = 1 if dummy_disaster == 1
	*fill to posterior dates
	bysort codmun: carryforward post, replace
	replace post = 0 if post == .
	
	by codmun: gen treatment_date = date if dummy_disaster == 1
	*fill to posterior and anterior dates
	bysort codmun: carryforward treatment_date, replace
	gsort codmun - date
	bysort codmun: carryforward treatment_date, replace
	
	
	
	***** Maybe I should Delete municipalities that got more than 2 disasters?!?!?!?!?!
	
	sort codmun date
	by codmun: gen earliest_treatment_date = date if post == 1 & post[_n-1] == 0
	replace earliest_treatment_date = 730 if post == 1 & date == 730 // 730 is the earliest month
	bysort codmun: carryforward earliest_treatment_date, replace
	gsort codmun - date
	bysort codmun: carryforward earliest_treatment_date, replace
	sort codmun date
	** Right now it is 1 for the earliest date.

	gen time_to_treat = date - earliest_treatment_date
	replace time_to_treat = 0 if missing(earliest_treatment_date)
	* this will determine the difference btw controls and treated states
	gen treat = !missing(treatment_date)
	
	* Stata won't allow factors with negative values, so let's shift
	* time-to-treat to start at 0, keeping track of where the true -1 is
	summ time_to_treat
	gen shifted_ttt = time_to_treat - r(min)
		
	gen logvalue = log(value)
	gen logquantity = log(quantity)
	
	save "$path\RESEARCH\Central Bank - Pix\dta\transactions_natural_disasters_event study.dta",replace
	
	
	
	****************************************************************************	
	
	*Do the Event Study
	
	****************************************************************************
	
	
	
	use "$path\RESEARCH\Central Bank - Pix\dta\transactions_natural_disasters_event study.dta",replace
	
	* Regress on our interaction terms with FEs for group and year,
	* clustering at the group (codmun) level
	* use ib# to specify our reference group

	summ shifted_ttt if time_to_treat == -1
	local true_neg1 = r(mean)
	reghdfe logquantity ib`true_neg1'.shifted_ttt, a(codmun date) vce(cluster codmun)
	
	*control for quantity?
	*control for population?
	*control for state fixed effects?
	
	*log value
	*log quantity
		
	*** GRAPH
	
	* Pull out the coefficients and SEs
	g coef = .
	g se = .
	levelsof shifted_ttt, l(times)
	foreach t in `times' {
		replace coef = _b[`t'.shifted_ttt] if shifted_ttt == `t'
		replace se = _se[`t'.shifted_ttt] if shifted_ttt == `t'
	}

	* Make confidence intervals
	g ci_top = coef+1.96*se
	g ci_bottom = coef - 1.96*se

	* Limit ourselves to one observation per quarter
	* now switch back to time_to_treat to get original timing
	keep time_to_treat coef se ci_*
	duplicates drop

	sort time_to_treat

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
		xtitle("Months after Treatment") ytitle("Log Quantity") caption("95% Confidence Intervals Shown")
	
	graph save "Graph" "C:\Users\mathe\Dropbox\RESEARCH\Central Bank - Pix\figures\log_quantity.gph", replace
	
	graph export "C:\Users\mathe\Dropbox\RESEARCH\Central Bank - Pix\figures\log_quantity.jpg", as(jpg) name("Graph") quality(90) replace
	
	
	* Distinguish by type of disaster?
	* more controls?
	
	
	
	
	
	
	
	
	
	/*
	* Now for another Y
	
	use "$path\RESEARCH\Central Bank - Pix\dta\transactions_natural_disasters_event study.dta",replace
	
	* Regress on our interaction terms with FEs for group and year,
	* clustering at the group (codmun) level
	* use ib# to specify our reference group

	summ shifted_ttt if time_to_treat == -1
	local true_neg1 = r(mean)
	reghdfe logvalue ib`true_neg1'.shifted_ttt, a(codmun date) vce(cluster codmun)
	
	*control for quantity?
	*control for population?
	*control for state fixed effects?
	
	*log value
	*log quantity
		
	*** GRAPH
	
	* Pull out the coefficients and SEs
	g coef = .
	g se = .
	levelsof shifted_ttt, l(times)
	foreach t in `times' {
		replace coef = _b[`t'.shifted_ttt] if shifted_ttt == `t'
		replace se = _se[`t'.shifted_ttt] if shifted_ttt == `t'
	}

	* Make confidence intervals
	g ci_top = coef+1.96*se
	g ci_bottom = coef - 1.96*se

	* Limit ourselves to one observation per quarter
	* now switch back to time_to_treat to get original timing
	keep time_to_treat coef se ci_*
	duplicates drop

	sort time_to_treat

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
		xtitle("Months after Treatment") ytitle("Log Value") caption("95% Confidence Intervals Shown")
	
	graph save "Graph" "C:\Users\mathe\Dropbox\RESEARCH\Central Bank - Pix\figures\log_value.gph", replace
	
	graph export "C:\Users\mathe\Dropbox\RESEARCH\Central Bank - Pix\figures\log_value.jpg", as(jpg) name("Graph") quality(90) replace
	
	*/
	
	
		