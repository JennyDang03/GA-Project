*flood_SA_v3

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
log using "$log\flood_SA_v3.log", replace 

/*
*Fake data
global dta "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta"
global output "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output"
use "$dta\flood_pix_fake.dta", replace
*/

* Monthly at the individual level. 
* Lets get the data ready to run a SA on R. 

use "$dta\natural_disasters_monthly_filled_flood.dta", clear
merge m:1 id_municipio using "$dta\municipios2.dta"
tab id_municipio if _merge == 1
keep if _merge == 3
drop _merge
sort id_municipio id_municipio_bcb date
by id_municipio id_municipio_bcb date: gen dup = cond(_N==1,0,_n)
tab dup // No duplicates
drop if dup>1
keep id_municipio date number_disasters id_municipio_bcb
*put name conventions!!! 
rename id_municipio_bcb muni_cd
rename date time_id
rename number_disasters flood 
* Limit to after Pix
keep if time_id >= ym(2020,11) & time_id <= ym(2022,12) 
sort muni_cd time_id

* Label 
cap label var muni_cd "Municipality"
cap label var time_id "Month"
cap label var flood "Flood"
cap label var id_municipio "IBGE Municipality Code"

* Create variables: date_flood, after_flood
gen after_flood = 0
bysort muni_cd : egen temp = min(time_id) if flood > 0
bysort muni_cd : egen date_flood = max(temp) 
drop temp
format %tw date_flood
replace after_flood = 1 if time_id >= date_flood

save "$dta\flood_monthly_2020_2022.dta", replace


* Quem nunca fez pix nao esta aqui. Seria bom adicionar os CPFs faltando. 
* Tem uma base com todos os CPFs?	

* tipo_pessoa == 1 PF, tipo_pessoa == 2 PJ
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
	drop if dup == 1
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
	
	*drop date_first_pix_rec date_first_pix_sent 
	* PS: I dropped the date so I can save on memory. We might need to add it back so we can do IV
	
		* receiver, sender, user
	gen sender = 0
	replace sender = 1 if trans_sent > 0
	gen receiver = 0
	replace receiver = 1 if trans_rec > 0
	gen user = 0
	replace user = 1 if trans_rec > 0 | trans_sent > 0

		* log trans_rec trans_sent value_rec value_sent
	
	foreach var of varlist trans_rec trans_sent value_rec value_sent {
		cap drop log_`var'
		gen log_`var'=log(`var' + 1)
	}
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
	
	* Limit to after Pix
	keep if time_id >= ym(2020,11) & time_id <= ym(2022,12) 

	* Save
	save "$dta\Pix_individuo_cleaned`i'.dta",replace 
	
	tempfile holding
	save `holding'

	keep id
	duplicates drop

	set seed 1234
	sample 10

	merge 1:m id using `holding', assert(match using) keep(match) nogenerate

	save "$dta\Pix_individuo_cleaned`i'_sample.dta", replace
	
}

********************************************************************************
* Weekly at the Municipal level. 
********************************************************************************

* After Pix
* Weekly at the Municipal level. 
* Lets get the data ready to run a SA on R. 

use "$dta\natural_disasters_weekly_filled_flood.dta", clear
merge m:1 id_municipio using "$dta\municipios2.dta"
tab id_municipio if _merge == 1
keep if _merge == 3
drop _merge
sort id_municipio id_municipio_bcb week
by id_municipio id_municipio_bcb week: gen dup = cond(_N==1,0,_n)
tab dup // No duplicates
drop if dup>1
keep id_municipio week number_disasters id_municipio_bcb
*put name conventions!!! 
rename id_municipio_bcb muni_cd
rename number_disasters flood 
* Limit to after Pix
keep if week >= wofd(mdy(11, 16, 2020)) & week <= wofd(mdy(12, 31, 2022))
sort muni_cd week

* Create variables: date_flood, after_flood
gen after_flood = 0
bysort muni_cd : egen temp = min(week) if flood > 0
bysort muni_cd : egen date_flood = max(temp) 
drop temp
format %tw date_flood
replace after_flood = 1 if week >= date_flood

save "$dta\flood_weekly_2020_2022.dta", replace


* After Pix
use "$dta\Base_week_muni_fake.dta", replace

keep if week >= wofd(mdy(11, 16, 2020)) & week <= wofd(mdy(12, 31, 2022))

keep if week < yw(2022,27) // note that after that everything is 0 for pix


* Cria painel balanceado
sort muni_cd week
tsset muni_cd week
tsfill, full // Not necessary because it is strongly balanced

sum *

*Create some new variables:
	
	* Log valor_cartao_credito - qtd_boleto 
foreach var of varlist valor_cartao_credito - n_cli_rec_pj_intra {
	cap drop log_`var'
	gen log_`var'=log(`var' + 1)
}

* Label them
* Best variables: 
* valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito 
* valor_TED_intra qtd_TED_intra qtd_cli_TED_rec_PJ qtd_cli_TED_rec_PF qtd_cli_TED_pag_PJ qtd_cli_TED_pag_PF 
* valor_boleto qtd_boleto qtd_cli_pag_pf_boleto qtd_cli_pag_pj_boleto qtd_cli_rec_pj_boleto 
* valor_PIX_inflow qtd_PIX_inflow valor_PIX_outflow qtd_PIX_outflow valor_PIX_intra qtd_PIX_intra n_cli_pag_pf_intra n_cli_rec_pf_intra n_cli_pag_pj_intra n_cli_rec_pj_intra


*****

merge m:1 muni_cd week using "$dta\flood_weekly_2020_2022.dta"
keep if _merge == 3
drop _merge

* Save
save "$dta\flood_pix_weekly_fake.dta", replace


* qtd_PIX_intra  qtd_PIX_inflow  qtd_PIX_outflow
* valor_PIX_intra  valor_PIX_inflow  valor_PIX_outflow
* n_cli_pag_pf n_cli_rec_pf n_cli_pag_pj n_cli_rec_pj

* qtd_TED_intra qtd_boleto
* valor_boleto valor_TED_intra valor_cartao_debito valor_cartao_credito 


*use "$dta\flood_weekly_2020_2022.dta", replace

* Merge to Pix Data


* qtd_PIX_intra  qtd_PIX_inflow  qtd_PIX_outflow
* valor_PIX_intra  valor_PIX_inflow  valor_PIX_outflow
* n_cli_pag_pf n_cli_rec_pf n_cli_pag_pj n_cli_rec_pj

* qtd_TED_intra qtd_boleto
* valor_boleto valor_TED_intra valor_cartao_debito valor_cartao_credito 


* valor_inflow qtd_inflow n_cli_rec_pf_inflow n_cli_rec_pj_inflow valor_outflow qtd_outflow n_cli_pag_pf_outflow n_cli_pag_pj_outflow valor_intra qtd_intra n_cli_pag_pf_intra n_cli_rec_pf_intra n_cli_pag_pj_intra n_cli_rec_pj_intra

/*
gen n_cli_inflow    = n_cli_rec_pf_inflow + n_cli_rec_pj_inflow
gen n_cli_outflow   = n_cli_pag_pf_outflow + n_cli_pag_pj_outflow
gen n_cli_pag_intra = n_cli_pag_pf_intra + n_cli_pag_pj_intra 
gen n_cli_rec_intra = n_cli_rec_pf_intra + n_cli_rec_pj_intra
gen n_receivers = n_cli_inflow + n_cli_rec_intra
gen n_senders = n_cli_outflow + n_cli_pag_intra

		label var qtd_inflow "Log Inflow of Pix"
		label var qtd_outflow "Log Outflow of Pix"
		label var qtd_intra "Log Pix transactions within the municipality"
		label var n_cli_inflow "Log number of receivers of inflow"
		label var n_cli_outflow "Log number of senders of outflow"
		label var n_cli_pag_intra "Log number of senders of within the municipality"
		label var n_cli_rec_intra "Log number of receivers of within the municipality"
		label var n_receivers "Log number of receivers"
		label var n_senders "Log number of senders"
		
		*qtd_outflow qtd_intra n_receivers n_senders 
*/

*Possible Variables
/*
use "$dta\\Base_week_muni_flood.dta" , clear


cap drop l`var'
	gen l`var'=log(`var' + 1)

* qtd_PIX_intra  qtd_PIX_inflow  qtd_PIX_outflow
* valor_PIX_intra  valor_PIX_inflow  valor_PIX_outflow
* n_cli_pag_pf n_cli_rec_pf n_cli_pag_pj n_cli_rec_pj

* qtd_TED_intra qtd_boleto
* valor_boleto valor_TED_intra valor_cartao_debito valor_cartao_credito 


* valor_inflow qtd_inflow n_cli_rec_pf_inflow n_cli_rec_pj_inflow valor_outflow qtd_outflow n_cli_pag_pf_outflow n_cli_pag_pj_outflow valor_intra qtd_intra n_cli_pag_pf_intra n_cli_rec_pf_intra n_cli_pag_pj_intra n_cli_rec_pj_intra

gen n_cli_inflow    = n_cli_rec_pf_inflow + n_cli_rec_pj_inflow
gen n_cli_outflow   = n_cli_pag_pf_outflow + n_cli_pag_pj_outflow
gen n_cli_pag_intra = n_cli_pag_pf_intra + n_cli_pag_pj_intra 
gen n_cli_rec_intra = n_cli_rec_pf_intra + n_cli_rec_pj_intra
gen n_receivers = n_cli_inflow + n_cli_rec_intra
gen n_senders = n_cli_outflow + n_cli_pag_intra

		label var qtd_inflow "Log Inflow of Pix"
		label var qtd_outflow "Log Outflow of Pix"
		label var qtd_intra "Log Pix transactions within the municipality"
		label var n_cli_inflow "Log number of receivers of inflow"
		label var n_cli_outflow "Log number of senders of outflow"
		label var n_cli_pag_intra "Log number of senders of within the municipality"
		label var n_cli_rec_intra "Log number of receivers of within the municipality"
		label var n_receivers "Log number of receivers"
		label var n_senders "Log number of senders"
		
		*qtd_outflow qtd_intra n_receivers n_senders 
*/



/*
*Fake data
global dta "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta"
global output "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output"
use "$dta\flood_pix_fake.dta", replace
*/
