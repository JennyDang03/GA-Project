******************************************
*Monta_base_Pix_individuo_sample_flood.do
* Input: 
*   1) "$dta\Pix_individuo_sample.dta"

* Output:
*   1) "$dta\Pix_individuo_sample_flood.dta"
*	2) "$dta\Pix_individuo_sample_flood25.dta"

* Variables: id muni_cd time_id value_sent trans_sent value_rec trans_rec value_self trans_self 
*			log_value_sent log_trans_sent log_value_rec log_trans_rec log_value_self log_trans_self 
*			after_first_pix_rec after_first_pix_sent sender receiver user date_flood

* The goal: Prepare Pix_individuo_sample to run on R: flood_SA_individual_sample.R

* To do: Add PJ

******************************************


clear all
set more off, permanently 
set emptycells drop

global log "\\sbcdf176\PIX_Matheus$\Stata\log"
global dta "\\sbcdf176\PIX_Matheus$\Stata\dta"
global output "\\sbcdf176\PIX_Matheus$\Output"
global origdata "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results"

* ADO 
adopath ++ "D:\ADO"
 adopath ++ "//sbcdf060/depep01$/ADO"
 adopath ++ "//sbcdf060/depep01$/ado-776e"

capture log close
log using "$log\Monta_base_Pix_individuo_sample_flood.log", replace 
use "$dta\Pix_individuo_sample.dta", replace

* Fix time_id
gen time = mdy(time_id - int(time_id/100) * 100,1 ,int(time_id/100) )
drop time_id
gen time_id = mofd(time)
format %tmNN/CCYY time_id
tab time_id
drop time 

* Elimina quem tem registros em diferentes municipios no mesmo mes.  -> 0 drops
duplicates tag id time_id, gen(dup)
tab dup
drop if dup == 1
drop dup

* Elimina quem muda de municipio - > 0 drops
sort id time_id
bysort id: gen first_muni_cd = muni_cd[1]
bysort id: gen change_muni = sum(muni_cd != first_muni_cd)
bysort id : egen temp = max(change_muni)
keep if temp == 0
drop first_muni_cd change_muni temp

* Cria painel balanceado
sort id muni_cd time_id
tsset id time_id
tsfill, full

order id muni_cd time_id
* Fill muni_cd
sort id muni_cd time_id // make all the muni_cd that are missing to go up. 
	bysort id (muni_cd): replace muni_cd = muni_cd[_n-1] if missing(muni_cd)
	sort id muni_cd time_id
	
*Fill other variables
foreach var of varlist value_sent-trans_self {
	replace `var'  = 0 if `var'  == .
	gen log_`var' = log(`var'+1)
}

* after_first_pix_rec, after_first_pix_sent
bysort id : egen temp = min(time_id) if trans_rec + trans_self> 0
bysort id : egen date_first_pix_rec = max(temp) 
drop temp 

bysort id : egen temp = min(time_id) if trans_sent + trans_self> 0 
bysort id : egen date_first_pix_sent = max(temp) 
drop temp 

		* This flags if the date time_id is before or after the first pix transaction
gen after_first_pix_rec = time_id >= date_first_pix_rec
gen after_first_pix_sent = time_id >= date_first_pix_sent

drop date_first_pix_rec date_first_pix_sent 
* I dropped the date so I can save on memory.

	* receiver, sender, user
gen sender = 0
replace sender = 1 if trans_sent + trans_self > 0
gen receiver = 0
replace receiver = 1 if trans_rec + trans_self> 0
gen user = 0
replace user = 1 if sender > 0 | receiver > 0

*Merge with flood
merge m:1 muni_cd time_id using "$dta\flood_monthly_2020_2022.dta", keep(3) keepusing(date_flood)
* Add only date_flood variable to save on memory
cap drop _merge

save "$dta\Pix_individuo_sample_flood.dta", replace

use "$dta\Pix_individuo_sample_flood.dta", replace

tempfile holding
save `holding'

keep id
duplicates drop

set seed 1234
sample 25

merge 1:m id using `holding', assert(match using) keep(match) nogenerate

sort id time_id
*format %tm date_flood
save "$dta\Pix_individuo_sample_flood25.dta", replace





log close