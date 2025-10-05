******************************************
*monta_base_pix_individuo.do
* Input: 
*   1) "$dta\Pix_individuo.dta"
*   2) "$dta\flood_monthly_2020_2022.dta"

* Output:
*   1) "$dta\Pix_individuo_cleaned`i'.dta"
*   2) "$dta\Pix_individuo_cleaned`i'_sample1.dta"
*	3) i == 1 and 2 (PF and PJ)

* Variables: id time_id muni_cd value_rec trans_rec value_sent trans_sent after_first_pix_sent after_first_pix_rec sender receiver user date_flood
* 				I create log_trans_rec, log_trans_sent, log_value_sent, log_value_rec in R later to save space.

* The goal: To clean, fill, and add date_flood and new treated variables to Pix_individuo.dta and prepare it for flood_SA_individual_v1.R and flood_SA_individual_v1_PJ.R

* To do: The sample individual is out of a sample of people that eventually use pix. 
*			Needs to add the people that never use pix. I think Jose was working on that
*			The ideal is to gather every CPF in the country, take a 1% sample 
*			and get all pix from the selected 1%

*****************************************

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
log using "$log\monta_base_pix_individuo.log", replace 

/*
*Fake data
global dta "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta"
global output "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output"
use "$dta\flood_pix_fake.dta", replace
*/

* Monthly at the individual level. 
* Lets get the data ready to run a SA on R. 

* Quem nunca fez pix nao esta aqui. Seria bom adicionar os CPFs faltando. 
* Tem uma base com todos os CPFs?	

* tipo_pessoa == 1 PF, tipo_pessoa == 2 PJ
* To run both: forval i = 1/2{
forval i = 1/2{
	use "$dta\Pix_individuo.dta" if tipo_pessoa == `i',replace 
	drop tipo_pessoa
	* Variables: value_sent trans_sent value_rec trans_rec id tipo_pessoa time_id muni_cd
	
	* FORMAT time_id
	* No Pix_PF_adoption.do nos fizemos isso 
	gen time = mdy(time_id - int(time_id/100) * 100,1 ,int(time_id/100) )
	drop time_id
	gen time_id = mofd(time)
	format %tmNN/CCYY time_id
	tab time_id
	drop time 

	* Elimina quem tem registros em diferentes municipios no mesmo mes. 
	duplicates tag id time_id, gen(dup)
	tab dup
	drop if dup >= 1
	drop dup

	* Elimina quem muda de municipio - > Apenas 87 em 38 milhoes 
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
	
	replace trans_rec  = 0 if trans_rec  == .
	replace trans_sent  = 0 if trans_sent  == .
	replace value_sent  = 0 if value_sent  == .
	replace value_rec  = 0 if value_rec  == .

	sort id muni_cd time_id // make all the muni_cd that are missing to go up. 
	bysort id (muni_cd): replace muni_cd = muni_cd[_n-1] if missing(muni_cd)
	sort id muni_cd time_id
	
	*Create some new variables:

		* after_first_pix_rec, date_first_pix_rec, after_first_pix_sent, date_first_pix_sent
			* Identifying time of first pix of the person
			* first pix has the date of the first pix o the that person
	bysort id : egen temp = min(time_id) if trans_rec > 0
	bysort id : egen date_first_pix_rec = max(temp) 
	drop temp 

	bysort id : egen temp = min(time_id) if trans_sent > 0 
	bysort id : egen date_first_pix_sent = max(temp) 
	drop temp 

			* This flags if the date time_id is before or after the first pix transaction
	gen after_first_pix_rec = time_id >= date_first_pix_rec
	gen after_first_pix_sent = time_id >= date_first_pix_sent
	
	drop date_first_pix_rec date_first_pix_sent 
	* I dropped the date so I can save on memory.
	
		* receiver, sender, user
	gen sender = 0
	replace sender = 1 if trans_sent > 0
	gen receiver = 0
	replace receiver = 1 if trans_rec > 0
	gen user = 0
	replace user = 1 if trans_rec > 0 | trans_sent > 0

		* log trans_rec trans_sent value_rec value_sent
	/*
	foreach var of varlist trans_rec trans_sent value_rec value_sent {
		cap drop log_`var'
		gen log_`var'=log(`var' + 1)
	}
	*/
	* PS: I dropped the logs so I can save on memory. We might need to add it back

	* Summarize and Save
	summ *

	*Check when date is missing
	
	* Label 
	cap label var id "ID"
	cap label var trans_rec "Transactions Received"
	cap label var trans_sent "Transactions Sent"
	cap label var value_sent "Value Sent"
	cap label var value_rec "Value Received"
	
	* after_first_pix_rec, date_first_pix_rec, after_first_pix_sent, date_first_pix_sent
	cap label var after_first_pix_sent "Sender Adoption"
	cap label var after_first_pix_rec "Receiver Adoption"
	
	* receiver, sender, user
	cap label variable sender "Senders"
	cap label variable receiver "Receivers"
	cap label variable user "Users"
	
	* log_ trans_rec trans_sent value_rec value_sent
	cap label var log_trans_rec "Log Transactions Received"
	cap label var log_trans_sent "Log Transactions Sent"
	cap label var log_value_sent "Log Value Sent"
	cap label var log_value_rec "Log Value Received"
	
	cap label var muni_cd "Municipality"
	cap label var time_id "Month"
	cap label var flood "Flood"
	cap label var id_municipio "IBGE Municipality Code"
	
	cap drop id_municipio // Need to save space
	
	*Merge with flood
	merge m:1 muni_cd time_id using "$dta\flood_monthly_2020_2022.dta", keep(3) keepusing(date_flood)
	* Add only date_flood variable to save on memory
	cap drop _merge
	
	* Limit to after Pix
	keep if time_id >= ym(2020,11) & time_id <= ym(2022,12) 
	
	*format %tm date_flood
	* Save
	save "$dta\Pix_individuo_cleaned`i'.dta",replace 
	
	tempfile holding
	save `holding'

	keep id
	duplicates drop

	set seed 1234
	sample 1

	merge 1:m id using `holding', assert(match using) keep(match) nogenerate
	
	sort id time_id
	*format %tm date_flood
	save "$dta\Pix_individuo_cleaned`i'_sample1.dta", replace
}



log close