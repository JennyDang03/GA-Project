

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
log using "$log\Reg_Ini_version2.log", replace 

use "$dta\Pix_PF_adoption.dta", clear 
*use "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/pix/fake_panel_data.dta", clear

tab confidence
drop if confidence <= 8
drop if aux_emerg_jan_mar21 == 1
merge m:1 id using "\\sbcdf176\PIX_Matheus$\Stata\dta\cpf_bolsa_familia.dta", keep(1 3)
*keep if grupo == 3 // somente Bolsa Familia
*drop if grupo == 2 // somente CadUnico
*keep if grupo == 2 // somente fora do CadUnico



* Tira Nov/2020 e Dez/2020 e vai até ...
* keep if time_id>731 & time_id<=747
* Why??

cap drop after_event 
gen after_event  =  time_id >= mofd(mdy(5, 1, 2021))
gen dist_event = time_id- mofd(mdy(5, 1, 2021))  // MAIO/21 é o tempo zero
egen id_dist=group(dist_event)
labmask id_dist, values(dist_event)

*-------------------------------------------------------------------------------
* Diff and Diff with Treatment
*-------------------------------------------------------------------------------

* Define Treatment
gen treat = 0
replace treat = 1 if dist_caixa > expected_distance

* Graph 
preserve
	collapse (mean) after_first_pix_sent, by(treat time_id)
	format %tmNN/CCYY time_id
	gen diff = after_first_pix_sent if treat == 1
	replace diff = - after_first_pix_sent if treat == 0
	collapse (sum) after_first_pix_sent, by(time_id)
	
	twoway (line after_first_pix_sent time_id, sort) ///
		   , xline(736) xtitle(Time) ytitle(Adoption) legend(order(1 "Control" 2 "Treatement"))
	graph export "$output\adoption_sent_time.png", replace
restore	

* Graph 
preserve
	collapse (mean) after_first_pix_rec, by(treat time_id)
	format %tmNN/CCYY time_id
	twoway (line after_first_pix_rec time_id if treat==0, sort) ///
		   (line after_first_pix_rec time_id if treat==1, sort) ///
		   , xline(736) xtitle(Time) ytitle(Adoption) legend(order(1 "Control" 2 "Treatement"))
	graph export "$output\adoption_rec_time.png", replace
restore	


* Diff and Diff:
*$Adoption_{i,t} = \alpha_i + \alpha_t + \beta_{0} D_{t} + \beta_1 Treatment_i + \beta_2 (D_{t} \times Treatment_i) + \epsilon_{i,t}$
reghdfe after_first_pix_sent 1.treat#1.after_event, absorb(id time_id)  vce(robust) base
reghdfe after_first_pix_sent 1.treat#i.grupo#1.after_event, absorb(id time_id)  vce(robust) base

* Event study
est clear 
eststo SENT:reghdfe after_first_pix_sent ib7.id_dist#1.treat, absorb(id time_id)  vce(robust) base

coefplot SENT, vertical yline(0, lcolor(black) lwidth(thin)) xline(7, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) omitted baselevels  xtitle("Months from Nov/20") xlab(, labsize(vsmall)) drop(_cons *expected_distance)  headings(`fazlabel', labsize(vsmall) ) rename(*.id_dist#1.treat = "") 

graph export "$output\did_treat.png", replace

* Por grupo 
est clear 
eststo SENT:reghdfe after_first_pix_sent ib7.id_dist#i.grupo#1.treat, absorb(id time_id)  vce(robust) base

coefplot SENT, vertical yline(0, lcolor(black) lwidth(thin)) xline(7, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) omitted baselevels  xtitle("Months from Nov/20") xlab(, labsize(vsmall)) drop(_cons *.id_dist#1.grupo#1.treat *.id_dist#2.grupo#1.treat) rename(*.id_dist#3.grupo#1.treat = "") 

graph export "$output\did_treat_bf.png", replace

coefplot SENT, vertical yline(0, lcolor(black) lwidth(thin)) xline(7, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) omitted baselevels  xtitle("Months from Nov/20") xlab(, labsize(vsmall)) drop(_cons *.id_dist#1.grupo#1.treat *.id_dist#3.grupo#1.treat) rename(*.id_dist#2.grupo#1.treat = "") 

graph export "$output\did_treat_extracad.png", replace

*-------------------------------------------------------------------------------
* Diff and Diff with (DistCaixa - ExpectedDist)
*-------------------------------------------------------------------------------
* Diff and Diff:
*$Adoption_{i,t} = \alpha_i + \alpha_t + \beta_{0} D_{t} + \beta_1 (DistCaixa_{i} - ExpectedDist_i) + \beta_2 (D_{t} \times (DistCaixa_{i} - ExpectedDist_i)) + \epsilon_{i,t}$

cap drop  dist_over_expec
gen dist_over_expec = dist_caixa / expected_distance


reghdfe after_first_pix_sent c.dist_over_expec#1.after_event, absorb(id time_id)  vce(robust) base
reghdfe after_first_pix_sent c.dist_over_expec#i.grupo#1.after_event, absorb(id time_id)  vce(robust) base

* Event study
est clear 
eststo SENT:reghdfe after_first_pix_sent ib7.id_dist#c.dist_over_expec, absorb(id time_id)  vce(robust) base

coefplot SENT, vertical yline(0, lcolor(black) lwidth(thin)) xline(7, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) omitted baselevels  xtitle("Months from jan/21") xlab(, labsize(vsmall)) drop(_cons)  headings(`fazlabel', labsize(vsmall) ) rename(*.id_dist#c.dist_over_expec = "") 
	
graph export "$output\did_diff_dist.png", replace

* Por grupo 
est clear 
eststo SENT:reghdfe after_first_pix_sent ib7.id_dist#i.grupo#c.dist_over_expec, absorb(id time_id)  vce(robust) base

coefplot SENT, vertical yline(0, lcolor(black) lwidth(thin)) xline(7, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) omitted baselevels  xtitle("Months from Nov/20") xlab(, labsize(vsmall)) drop(_cons *.id_dist#1.grupo#c.dist_over_expec *.id_dist#2.grupo#c.dist_over_expec) rename(*.id_dist#3.grupo#c.dist_over_expec = "") 

graph export "$output\did_diff_dist_bf.png", replace

coefplot SENT, vertical yline(0, lcolor(black) lwidth(thin)) xline(7, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) omitted baselevels  xtitle("Months from Nov/20") xlab(, labsize(vsmall)) drop(_cons *.id_dist#1.grupo#c.dist_over_expec *.id_dist#3.grupo#c.dist_over_expec) rename(*.id_dist#2.grupo#c.dist_over_expec = "") 

graph export "$output\did_diff_dist_extracad.png", replace


*-------------------------------------------------------------------------------
* Diff and Proportinal distance (DistCaixa / ExpectedDist)
*-------------------------------------------------------------------------------
* Diff and Diff:
*$Adoption_{i,t} = \alpha_i + \alpha_t + \beta_{0} D_{t} + \beta_1 (DistCaixa_{i} - ExpectedDist_i) + \beta_2 (D_{t} \times (DistCaixa_{i} - ExpectedDist_i)) + \epsilon_{i,t}$

cap drop  dist_prop
gen dist_prop = dist_caixa / expected_distance

reghdfe after_first_pix_sent c.dist_prop#1.after_event, absorb(id time_id)  vce(robust) base
reghdfe after_first_pix_sent c.dist_prop#i.grupo#1.after_event, absorb(id time_id)  vce(robust) base

* Event study
est clear 
eststo SENT:reghdfe after_first_pix_sent ib7.id_dist#c.dist_prop, absorb(id time_id)  vce(robust) base

coefplot SENT, vertical yline(0, lcolor(black) lwidth(thin)) xline(7, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) omitted baselevels  xtitle("Months from jan/21") xlab(, labsize(vsmall)) drop(_cons)  headings(`fazlabel', labsize(vsmall) ) rename(*.id_dist#c.dist_prop = "") 
	
graph export "$output\did_dist_prop.png", replace

* Por grupo 
est clear 
eststo SENT:reghdfe after_first_pix_sent ib7.id_dist#i.grupo#c.dist_prop, absorb(id time_id)  vce(robust) base

coefplot SENT, vertical yline(0, lcolor(black) lwidth(thin)) xline(7, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) omitted baselevels  xtitle("Months from Nov/20") xlab(, labsize(vsmall)) drop(_cons *.id_dist#1.grupo#c.dist_prop *.id_dist#2.grupo#c.dist_prop) rename(*.id_dist#3.grupo#c.dist_prop = "") 

graph export "$output\did_dist_prop_bf.png", replace

coefplot SENT, vertical yline(0, lcolor(black) lwidth(thin)) xline(7, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) omitted baselevels  xtitle("Months from Nov/20") xlab(, labsize(vsmall)) drop(_cons *.id_dist#1.grupo#c.dist_prop *.id_dist#3.grupo#c.dist_prop) rename(*.id_dist#2.grupo#c.dist_prop = "") 

graph export "$output\did_dist_prop_extracad.png", replace

*-------------------------------------------------------------------------------
* Diff and Diff with DistCaixa controling for ExpectedDist
*-------------------------------------------------------------------------------
* Diff and Diff:
*$Adoption_{i,t} = \alpha_i + \alpha_t + \beta_{0} D_{t} + \beta_1 DistCaixa_{i} + \beta_2 ExpectedDist_i + \beta_3 (D_{t} \times DistCaixa_{i}) + \beta_4 (D_{t} \times ExpectedDist_i) + \epsilon_{i,t}$

reghdfe after_first_pix_sent c.dist_caixa#1.after_event c.expected_distance#1.after_event, absorb(id time_id)  vce(robust) base

* Event study
est clear 
eststo SENT:reghdfe after_first_pix_sent id_dist#c.dist_caixa ib7.id_dist#c.expected_distance , absorb(id time_id)  vce(robust) base

coefplot SENT, vertical yline(0, lcolor(black) lwidth(thin)) xline(7, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) omitted baselevels  xtitle("Months from jan/21") xlab(, labsize(vsmall)) drop(_cons *expected_distance*)  headings(`fazlabel', labsize(vsmall) ) rename(*.id_dist#c.dist_caixa = "") 

graph export "$output\did_distcaixa.png", replace

coefplot SENT, vertical yline(0, lcolor(black) lwidth(thin)) xline(7, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) omitted baselevels  xtitle("Months from jan/21") xlab(, labsize(vsmall)) drop(_cons *dist_caixa*)  headings(`fazlabel', labsize(vsmall) ) rename(*.id_dist#c.expected_distance = "") 
	
graph export "$output\did_expectedist.png", replace


log close 