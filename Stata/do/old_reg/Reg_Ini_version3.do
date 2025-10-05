

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
log using "$log\Reg_Ini_version3.log", replace 

use "$dta\Pix_PF_adoption.dta", clear 
/*
use "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/pix/fake_panel_data.dta", clear
set seed 1234
gen random=runiform()
bysort id: gen grupo=cond(random[1]<=1/3,1,cond(random[1]>2/3,3,2))
*/
merge m:1 id using "\\sbcdf176\PIX_Matheus$\Stata\dta\cpf_bolsa_familia.dta", keep(1 3) nogenerate

tab confidence
drop if confidence <= 8
drop if aux_emerg_jan_mar21 == 1

cap drop after_event 
gen after_event  =  time_id >= mofd(mdy(5, 1, 2021))
gen dist_event = time_id- mofd(mdy(5, 1, 2021))  // MAIO/21 é o tempo zero
egen id_dist=group(dist_event)
format %tmNN/CCYY time_id
labmask id_dist, values(time_id)

* New variables! ---------------------------------------------------------------

* Create Pix Adoption - Dummy for first time that individual learned to do both
gen after_adoption = 0
replace after_adoption = 1 if after_first_pix_rec == 1 & after_first_pix_sent == 1
bysort id : egen temp = min(time_id) if after_adoption > 0
bysort id : egen date_adoption = max(temp) 
drop temp
format %tmNN/CCYY date_adoption
gen new_adoption = 1 if time_id == date_adoption
replace new_adoption = 0 if new_adoption == .

bysort id : egen temp = min(time_id) if after_first_pix_sent == 1
gen new_sent = 0
replace new_sent = 1 if temp == time_id
drop temp

bysort id : egen temp = min(time_id) if after_first_pix_rec == 1
gen new_rec = 0
replace new_rec = 1 if temp == time_id
drop temp

/*
* Bring back transactions and values
value_sent
value_rec
transactions_sent
transactions_rec
gen log_value_sent = log(value_sent)
gen log_transactions_sent = log()

* Do logit
* Do triple difference 

*/

* ------------------------------------------------------------------------------
global list_of_y after_adoption new_adoption after_first_pix_sent new_sent after_first_pix_rec new_rec
global list_of_treat expected_distance dist_bank
cap label var after_adoption "Adoption"
cap label var new_adoption "New users"
cap label var after_first_pix_sent "Senders"
cap label var new_sent "New senders"
cap label var after_first_pix_rec "Receivers"
cap label var new_rec "New receivers"
cap label var expected_distance "Expected Distance"
cap label var dist_bank "Distance to the Closest Bank"
cap label var dist_caixa "Distance to the Closest Caixa"
cap label var id "ID"
cap label var time_id "Month"
foreach var1 of var * { 
	local l`var1' : variable label `var1'
	if `"`l`var1''"' == "" {
		global l`var1' "`var1'"
		}
	}

*-------------------------------------------------------------------------------
* Diff and Diff with Treatment: dist_caixa > expected_distance
*-------------------------------------------------------------------------------

* Define Treatment

foreach var_treat of global list_of_treat{
	cap drop treat
	gen treat = 0
	replace treat = 1 if dist_caixa > `var_treat'

	foreach y of global list_of_y {
		preserve
			collapse (mean) `y', by(treat time_id)
			format %tmNN/CCYY time_id
			twoway (line `y' time_id if treat==0, sort) ///
				   (line `y' time_id if treat==1, sort) ///
				   , xline(736) xtitle(Time) ytitle("Percentage of `y'") legend(order(1 "Control" 2 "Treatement")) note("Treatment defined as dist_caixa > `var_treat'")
			*graph export "$output\treat_`var_treat'_`y'.png", replace
		restore	
				
		* Diff and Diff:
		*$Adoption_{i,t} = \alpha_i + \alpha_t + \beta_{0} D_{t} + \beta_1 Treatment_i + \beta_2 (D_{t} \times Treatment_i) + \epsilon_{i,t}$

		reghdfe `y' 1.treat#i.grupo#1.after_event, absorb(id time_id) vce(cluster id) base
		
		* Event study
		est clear 
		eststo SENT:reghdfe `y' ib7.id_dist#i.grupo#1.treat , absorb(id time_id) vce(cluster id) base
				
		coefplot SENT, title("Event Study for ExtraCad") subtitle("reghdfe `y' ib7.id_dist#i.grupo#1.treat , absorb(id time_id) vce(cluster id) base", size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall))  ytitle(`l`y'') coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22", wrap(4)) drop(_cons *.id_dist#1.grupo#1.treat *.id_dist#3.grupo#1.treat) note("Treatment defined as Distance to the Closest Caixa > `l`var_treat''")
		graph export "$output\did_`var_treat'_`y'_extracad.png", replace
		coefplot SENT, title("Event Study for Cad") subtitle("reghdfe `y' ib7.id_dist#i.grupo#1.treat , absorb(id time_id) vce(cluster id) base", size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22", wrap(4)) drop(_cons *.id_dist#2.grupo#1.treat *.id_dist#3.grupo#1.treat) note("Treatment defined as Distance to the Closest Caixa > `l`var_treat''")
		graph export "$output\did_`var_treat'_`y'_cad.png", replace
		
		coefplot SENT, title("Event Study for BF") subtitle("reghdfe `y' ib7.id_dist#i.grupo#1.treat , absorb(id time_id) vce(cluster id) base", size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22", wrap(4)) drop(_cons *.id_dist#1.grupo#1.treat *.id_dist#2.grupo#1.treat) note("Treatment defined as Distance to the Closest Caixa > `l`var_treat''")
		graph export "$output\did_`var_treat'_`y'_bf.png", replace		
	}
}

*-------------------------------------------------------------------------------
* Diff and Diff with DistCaixa controling for ExpectedDist
*-------------------------------------------------------------------------------
* Diff and Diff:
*$Adoption_{i,t} = \alpha_i + \alpha_t + \beta_{0} D_{t} + \beta_1 DistCaixa_{i} + \beta_2 ExpectedDist_i + \beta_3 (D_{t} \times DistCaixa_{i}) + \beta_4 (D_{t} \times ExpectedDist_i) + \epsilon_{i,t}$

foreach y of global list_of_y {
	reghdfe `y' c.dist_caixa#i.grupo#1.after_event c.expected_distance#1.after_event, absorb(id time_id) vce(cluster id) base
	
	* Event study
	est clear 
	eststo SENT:reghdfe `y' ib7.id_dist#i.grupo#c.dist_caixa ib7.id_dist#i.grupo#c.expected_distance , absorb(id time_id) vce(cluster id) base
	
	coefplot SENT, title("Event Study for ExtraCad") subtitle("reghdfe `y' ib7.id_dist#i.grupo#c.dist_caixa ib7.id_dist#i.grupo#c.expected_distance , absorb(id time_id) vce(cluster id) base", size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'') coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22", wrap(4)) drop(_cons *#c.expected_distance *.id_dist#1.grupo#c.dist_caixa *.id_dist#3.grupo#c.dist_caixa) 
		graph export "$output\did_distcaixa_`y'_extracad.png", replace
		coefplot SENT, title("Event Study for Cad") subtitle("reghdfe `y' ib7.id_dist#i.grupo#c.dist_caixa ib7.id_dist#i.grupo#c.expected_distance , absorb(id time_id) vce(cluster id) base", size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'') coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22", wrap(4)) drop(_cons *#c.expected_distance *.id_dist#2.grupo#c.dist_caixa *.id_dist#3.grupo#c.dist_caixa) 
		graph export "$output\did_distcaixa_`y'_cad.png", replace
		
		coefplot SENT, title("Event Study for BF") subtitle("reghdfe `y' ib7.id_dist#i.grupo#c.dist_caixa ib7.id_dist#i.grupo#c.expected_distance , absorb(id time_id) vce(cluster id) base", size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'') coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22", wrap(4)) drop(_cons *#c.expected_distance *.id_dist#1.grupo#c.dist_caixa *.id_dist#2.grupo#c.dist_caixa)
		graph export "$output\did_distcaixa_`y'_bf.png", replace	
}

*-------------------------------------------------------------------------------
* Diff and Diff with (DistCaixa - ExpectedDist)
*-------------------------------------------------------------------------------
* Diff and Diff:
*$Adoption_{i,t} = \alpha_i + \alpha_t + \beta_{0} D_{t} + \beta_1 (DistCaixa_{i} - ExpectedDist_i) + \beta_2 (D_{t} \times (DistCaixa_{i} - ExpectedDist_i)) + \epsilon_{i,t}$

gen dist_over_expec = dist_caixa - expected_distance

foreach y of global list_of_y {
	reghdfe `y' c.dist_over_expec#i.grupo#1.after_event, absorb(id time_id) vce(cluster id) base

	* Event study
	est clear 
	eststo SENT:reghdfe `y' ib7.id_dist#i.grupo#c.dist_over_expec, absorb(id time_id) vce(cluster id) base
	
	coefplot SENT, title("Event Study for ExtraCad") subtitle("reghdfe `y' ib7.id_dist#i.grupo#c.dist_over_expec, absorb(id time_id) vce(cluster id) base", size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'') coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22", wrap(4)) drop(_cons *.id_dist#1.grupo#c.dist_over_expec *.id_dist#3.grupo#c.dist_over_expec) 
		graph export "$output\did_dist_over_expec_`y'_extracad.png", replace
		coefplot SENT, title("Event Study for Cad") subtitle("reghdfe `y' ib7.id_dist#i.grupo#c.dist_over_expec, absorb(id time_id) vce(cluster id) base", size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'') coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22", wrap(4)) drop(_cons *.id_dist#2.grupo#c.dist_over_expec *.id_dist#3.grupo#c.dist_over_expec) 
		graph export "$output\did_dist_over_expec_`y'_cad.png", replace
		
		coefplot SENT, title("Event Study for BF") subtitle("reghdfe `y' ib7.id_dist#i.grupo#c.dist_over_expec, absorb(id time_id) vce(cluster id) base", size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'') coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22", wrap(4)) drop(_cons *.id_dist#1.grupo#c.dist_over_expec *.id_dist#2.grupo#c.dist_over_expec)
		graph export "$output\did_dist_over_expec_`y'_bf.png", replace	
}


*-------------------------------------------------------------------------------
* Diff and Diff with LOG DistCaixa controling for LOG ExpectedDist
*-------------------------------------------------------------------------------
* Diff and Diff:
*$Adoption_{i,t} = \alpha_i + \alpha_t + \beta_{0} D_{t} + \beta_1 LogDistCaixa_{i} + \beta_2 LogExpectedDist_i + \beta_3 (D_{t} \times LogDistCaixa_{i}) + \beta_4 (D_{t} \times LogExpectedDist_i) + \epsilon_{i,t}$

gen log_dist_caixa = log(dist_caixa + 0.1)
gen log_expec_dist = log(expected_distance + 0.1)

foreach y of global list_of_y {

	reghdfe `y' c.log_dist_caixa#i.grupo#1.after_event c.log_expec_dist#i.grupo#1.after_event, absorb(id time_id) vce(cluster id) base

	* Event study
	est clear 
	eststo SENT:reghdfe `y' ib7.id_dist#i.grupo#c.log_dist_caixa ib7.id_dist#i.grupo#c.log_expec_dist , absorb(id time_id) vce(cluster id) base

	coefplot SENT, title("Event Study for ExtraCad") subtitle("reghdfe `y' ib7.id_dist#i.grupo#c.log_dist_caixa ib7.id_dist#i.grupo#c.log_expec_dist , absorb(id time_id) vce(cluster id) base", size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'') coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22", wrap(4)) drop(_cons *#c.log_expec_dist *.id_dist#1.grupo#c.log_dist_caixa *.id_dist#3.grupo#c.log_dist_caixa) 
		graph export "$output\did_logdistcaixa_`y'_extracad.png", replace
		coefplot SENT, title("Event Study for Cad") subtitle("reghdfe `y' ib7.id_dist#i.grupo#c.log_dist_caixa ib7.id_dist#i.grupo#c.log_expec_dist , absorb(id time_id) vce(cluster id) base", size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'') coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22", wrap(4)) drop(_cons *#c.log_expec_dist *.id_dist#1.grupo#c.log_dist_caixa *.id_dist#3.grupo#c.log_dist_caixa)  
		graph export "$output\did_logdistcaixa_`y'_cad.png", replace
		
		coefplot SENT, title("Event Study for BF") subtitle("reghdfe `y' ib7.id_dist#i.grupo#c.log_dist_caixa ib7.id_dist#i.grupo#c.log_expec_dist , absorb(id time_id) vce(cluster id) base", size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'') coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22", wrap(4)) drop(_cons *#c.log_expec_dist *.id_dist#1.grupo#c.log_dist_caixa *.id_dist#3.grupo#c.log_dist_caixa) 
		graph export "$output\did_logdistcaixa_`y'_bf.png", replace	
}

*-------------------------------------------------------------------------------
* Diff and Diff with LOG (DistCaixa) - LOG (ExpectedDist)
*-------------------------------------------------------------------------------
* Diff and Diff:
*$Adoption_{i,t} = \alpha_i + \alpha_t + \beta_{0} D_{t} + \beta_1 LogDistCaixa_{i} + \beta_2 LogExpectedDist_i + \beta_3 (D_{t} \times LogDistCaixa_{i}) + \beta_4 (D_{t} \times LogExpectedDist_i) + \epsilon_{i,t}$

gen log_dist_expec = log_dist_caixa - log_expec_dist

foreach y of global list_of_y {

	reghdfe `y' c.log_dist_expec#i.grupo#1.after_event, absorb(id time_id) vce(cluster id) base

	* Event study
	est clear 
	eststo SENT:reghdfe `y' ib7.id_dist#i.grupo#c.log_dist_expec, absorb(id time_id) vce(cluster id) base

		coefplot SENT, title("Event Study for ExtraCad") subtitle("reghdfe `y' ib7.id_dist#i.grupo#c.log_dist_expec, absorb(id time_id) vce(cluster id) base", size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'') coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22", wrap(4)) drop(_cons *.id_dist#1.grupo#c.log_dist_expec *.id_dist#3.grupo#c.log_dist_expec) 
		graph export "$output\did_log_dist_expec_`y'_extracad.png", replace
		coefplot SENT, title("Event Study for Cad") subtitle("reghdfe `y' ib7.id_dist#i.grupo#c.log_dist_expec, absorb(id time_id) vce(cluster id) base", size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'') coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22", wrap(4)) drop(_cons *.id_dist#1.grupo#c.log_dist_expec *.id_dist#3.grupo#c.log_dist_expec)  
		graph export "$output\did_log_dist_expec_`y'_cad.png", replace
		
		coefplot SENT, title("Event Study for BF") subtitle("reghdfe `y' ib7.id_dist#i.grupo#c.log_dist_expec, absorb(id time_id) vce(cluster id) base", size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'') coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22", wrap(4)) drop(_cons *.id_dist#1.grupo#c.log_dist_expec *.id_dist#3.grupo#c.log_dist_expec) 
		graph export "$output\did_log_dist_expec_`y'_bf.png", replace
}

*-------------------------------------------------------------------------------
* Diff and Diff with LOG (DistCaixa) - LOG (ExpectedDist)
*-------------------------------------------------------------------------------
* Diff and Diff:
*$Adoption_{i,t} = \alpha_i + \alpha_t + \beta_{0} D_{t} + \beta_1 LogDistCaixa_{i} + \beta_2 LogExpectedDist_i + \beta_3 (D_{t} \times LogDistCaixa_{i}) + \beta_4 (D_{t} \times LogExpectedDist_i) + \epsilon_{i,t}$

gen prop_dist = dist_caixa / expected_distance
foreach y of global list_of_y {

	reghdfe `y' c.prop_dist#i.grupo#1.after_event, absorb(id time_id) vce(cluster id) base

	* Event study
	est clear 
	eststo SENT:reghdfe `y' ib7.id_dist#i.grupo#c.prop_dist, absorb(id time_id) vce(cluster id) base
	
		coefplot SENT, title("Event Study for ExtraCad") subtitle("reghdfe `y' ib7.id_dist#i.grupo#c.prop_dist, absorb(id time_id) vce(cluster id) base", size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'') coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22", wrap(4)) drop(_cons *.id_dist#1.grupo#c.prop_dist *.id_dist#3.grupo#c.prop_dist) 
		graph export "$output\did_prop_dist_`y'_extracad.png", replace
		coefplot SENT, title("Event Study for Cad") subtitle("reghdfe `y' ib7.id_dist#i.grupo#c.prop_dist, absorb(id time_id) vce(cluster id) base", size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'') coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22", wrap(4)) drop(_cons *.id_dist#1.grupo#c.prop_dist *.id_dist#3.grupo#c.prop_dist)  
		graph export "$output\did_prop_dist_`y'_cad.png", replace
		coefplot SENT, title("Event Study for BF") subtitle("reghdfe `y' ib7.id_dist#i.grupo#c.prop_dist, absorb(id time_id) vce(cluster id) base", size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) xline(6, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) baselevels xtitle("Months") xlab(, labsize(vsmall)) ytitle(`l`y'') coeflabels(1.* = "Nov 20" 2.* = "Dec 20" 3.* = "Jan 21" 4.* = "Feb 21" 5.* = "Mar 21" 6.* = "Apr 21" 7.* = "May 21" 8.* = "Jun 21" 9.* = "Jul 21" 10.* = "Aug 21" 11.* = "Sep 21" 12.* = "Oct 21" 13.* = "Nov 21" 14.* = "Dec 21" 15.* = "Jan 22" 16.* = "Feb 22" 17.* = "Mar 22" 18.* = "Apr 22" 19.* = "May 22" 20.* = "Jun 22" 21.* = "Jul 22" 22.* = "Aug 22" 23.* = "Sep 22" 24.* = "Oct 22" 25.* = "Nov 22", wrap(4)) drop(_cons *.id_dist#1.grupo#c.prop_dist *.id_dist#3.grupo#c.prop_dist) 
		graph export "$output\did_prop_dist_`y'_bf.png", replace
	
}

*-------------------------------------------------------------------------------
* Floods
*-------------------------------------------------------------------------------

* Add floods until the end of 2022
* Merge with data from pix on time_id and mun_cd

* C:\Users\mathe\Dropbox\RESEARCH\Natural Disasters\code
* C:\Users\mathe\Dropbox\RESEARCH\Natural Disasters\dta\danos_informados_monthly_filled_flood.dta

*-------------------------------------------------------------------------------
* Aggregate by municipality to take the log

* Aggregate by treatment and control? 

* Do Transactions or Value
*-------------------------------------------------------------------------------

log close 