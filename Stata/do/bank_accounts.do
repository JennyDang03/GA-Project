* Esse codigo transforma as informacoes de abertura e fechamento de contas
* em numeros de contas abertas por mes 
* Input:   
* Output: "$dta\id_accounts.dta"

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
log using "$log\bank_accounts.log", replace 

* Nao sei o endereco dos dados
use "$dta/DADOS_BANCARIOS.dta"
*-------------------------------------------------------------------------------
* PART 1: Creating new variables and preparing dataset 
*-------------------------------------------------------------------------------
gen m_closing_date = mofd(closing_date)
gen m_opening_date = mofd(opening_date)

* Generate sample data para testar algoritmo
/*
clear
global dta "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta"
* Set seed for reproducibility
set seed 123
set obs 1000
gen id = ceil(100*runiform())
bysort id: gen bank_id = _n
gen m_opening_date = ym(2019,1) + ceil(36*runiform())
format %tmNN/CCYY m_opening_date
gen m_closing_date = m_opening_date + ceil(12*runiform()) if m_opening_date < ym(2022,1)
format %tmNN/CCYY m_closing_date
sort id m_opening_date m_closing_date
*/


preserve
	* Create variable accounts, 1 if open, missing if not
	gen accounts = 1 if m_closing_date >= ym(2019,1) & m_opening_date <= ym(2019,1)
	gen accounts_opened = 1 if m_opening_date <= ym(2019,1)
	collapse (count) accounts accounts_opened, by(id) // Counts the non missing accounts
	gen time_id = ym(2019,1)
	save "$dta\id_accounts.dta", replace
restore
local start=ym(2019,1)+1
local end=ym(2022,12)
forvalues m = `start'/`end' {
    preserve
		* Create variable accounts
		gen accounts = 1 if m_closing_date >= `m' & m_opening_date <= `m'
		gen accounts_opened = 1 if m_opening_date <= `m'
		collapse (count) accounts accounts_opened, by(id) // Counts the non missing accounts
		gen time_id = `m'
		append using "$dta\id_accounts.dta", force
		save "$dta\id_accounts.dta", replace
		sleep 1000
		* IF STATA GIVES YOU THIS ERROR: "file cannot be modified or erased; likely cause is read-only directory or file"
		* THEN INCREASE SLEEP - THAT IS BECAUSE STATA DID NOT FINISH SAVING THE FILE.
	restore
}
use "$dta\id_accounts.dta", replace
label var accounts_opened "Accounts opened between January 2019 and now"
label var accounts "Stock of accounts remaining open that were created after 2019"
sort id time_id
format %tmNN/CCYY time_id
save "$dta\id_accounts.dta", replace

*-------------------------------------------------------------------------------
* PART 2: Diff and Diff and Event Studies 
*-------------------------------------------------------------------------------

/*
use "$dta\Pix_PF_adoption.dta", clear 
use "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/pix/fake_panel_data.dta", clear
set seed 1234
gen random=runiform()
bysort id: gen grupo=cond(random[1]<=1/3,1,cond(random[1]>2/3,3,2))
*/

*Regressions
* Bank accounts on Pix Adoption

use "$dta\id_accounts.dta", replace
sort id time_id
format %tmNN/CCYY time_id

merge 1:1 id time_id using "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/pix/fake_panel_data.dta"
drop if _merge == 2
drop _merge

*fill the rest of the missing data.
sort id time_id
by id: carryforward dist_caixa, replace
by id: carryforward expected_distance, replace
by id: carryforward index, replace
by id: carryforward confidence, replace
by id: carryforward aux_emerg_jan_mar21, replace
by id: carryforward after_first_pix_rec, replace
by id: carryforward after_first_pix_sent, replace

gsort id - time_id
by id: carryforward dist_caixa, replace
by id: carryforward expected_distance, replace
by id: carryforward index, replace
by id: carryforward confidence, replace
by id: carryforward aux_emerg_jan_mar21, replace
by id: carryforward after_first_pix_rec, replace
by id: carryforward after_first_pix_sent, replace
sort id time_id
****************

*-------------------------------------------------------
* PART 2.1: Creating new variables and preparing dataset 
*-------------------------------------------------------
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

gen after_pix  =  time_id >= mofd(mdy(5, 1, 2021))
gen dist_pix = time_id- mofd(mdy(5, 1, 2021))  // NOV/21 é o tempo zero
egen id_dist_pix=group(dist_pix)
format %tmNN/CCYY time_id
labmask id_dist_pix, values(time_id)

*-----------------------------------------------

gen after_adoption = 0
replace after_adoption = 1 if after_first_pix_rec == 1 & after_first_pix_sent == 1
bysort id : egen temp = min(time_id) if after_adoption > 0
bysort id : egen date_adoption = max(temp) 
drop temp
format %tmNN/CCYY date_adoption

gen dist_adoption = time_id - date_adoption
replace dist_adoption = 0 if missing(date_adoption)
egen id_dist_adoption = group(dist_adoption)
labmask id_dist_adoption, values(dist_adoption)

summ dist_adoption
gen shifted_dist_adoption = dist_adoption - r(min)
summ shifted_dist_adoption if dist_adoption == -1
local base_adoption = r(mean)+1

*-----------------------------------------------
bysort id : egen temp = min(time_id) if after_first_pix_rec > 0
cap drop date_rec
bysort id : egen date_rec = max(temp) 
drop temp
format %tmNN/CCYY date_rec

gen dist_rec = time_id - date_rec
replace dist_rec = 0 if missing(date_rec)
egen id_dist_rec = group(dist_rec)
labmask id_dist_rec, values(dist_rec)

summ dist_rec
gen shifted_dist_rec = dist_rec - r(min)
summ shifted_dist_rec if dist_rec == -1
local base_rec = r(mean)+1

*-----------------------------------------------
bysort id : egen temp = min(time_id) if after_first_pix_sent > 0
cap drop date_sent
bysort id : egen date_sent = max(temp) 
drop temp
format %tmNN/CCYY date_sent

gen dist_sent = time_id - date_sent
replace dist_sent = 0 if missing(date_sent)
egen id_dist_sent = group(dist_sent)
labmask id_dist_sent, values(dist_sent)

summ dist_sent
gen shifted_dist_sent = dist_sent - r(min)
summ shifted_dist_sent if dist_sent == -1
local base_sent = r(mean)+1

*-----------------------------------------------
cap label var after_adoption "Adoption"
cap label var after_first_pix_sent "Senders"
cap label var after_first_pix_rec "Receivers"
cap label var expected_distance "Expected Distance"
cap label var dist_bank "Distance to the Closest Bank"
cap label var dist_caixa "Distance to the Closest Caixa"
cap label var id "ID"
cap label var time_id "Month"
cap label var id_dist_adoption "Adoption"
cap label var id_dist_sent "Senders"
cap label var id_dist_rec "Receivers"

save "$dta\Pix_PF_adoption_bank_account.dta", replace

*-------------------------------------------------------
* PART 2.2: Event Study with November and April as events 
*-------------------------------------------------------

foreach var1 of varlist * { 
	local l`var1' : variable label `var1'
	if `"`l`var1''"' == "" {
		local l`var1' "`var1'"
		}
}

global list_of_y accounts
global list_of_x adoption sent rec
global list_of_treat expected_distance dist_bank
global list_of_types bf cad extracad all non_bf

keep if time_id >= ym(2020,1) & time_id <= ym(2022,12)  

*-------------------------------------------------------
* PART 2.2.1: Impact of Adoption on Y
*-------------------------------------------------------

foreach type of global list_of_types {
	foreach y of global list_of_y {
		foreach x of global list_of_x{
			* Event study
			est clear 
			eststo SENT:reghdfe `y' ib`base_`x''.id_dist_`x' if `type' == 1, absorb(id time_id) vce(cluster mun_cd) allbaselevels // Cluster at mun_cd because the treatment was taken at mun_cd
			local reg_equation "reghdfe `y' ib`base_`x''.id_dist_`x' if `type' == 1, absorb(id time_id) vce(cluster mun_cd) allbaselevels"
			
			* Limit the graph to include fewer months. 
			coefplot SENT, title(`"Event Study of `lid_dist_`x'' for `l`type''"') subtitle(`"`reg_equation'"', size(vsmall)) vertical yline(0, lcolor(black) lwidth(thin)) levels(90) xline(`base_`x'', lcolor(black) lwidth(thin) lpattern(dash)) graphregion(color(white)) xtitle("Months") xlab(, labsize(vsmall))  ytitle(`l`y'') note("90% Confidence Intervals Shown")  drop(_cons) baselevels
			graph export "$output\2_`y'_`x'_`type'.png", replace
		}
	}
}

*-------------------------------------------------------
* PART 2.2.2: Impact of Treatment/DistCaixa/Dist-Exp on Y
*-------------------------------------------------------




log close 
