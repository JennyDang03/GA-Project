*****
* Pix Adoption
*****
* Esse codigo importa dados ja limpos de PIX agrupados por individuo (PF ou PJ)
* Os dados de input são gerados pelo Importa_Pix_individuo.do
* Input: 
*   1) "$dta\Pix_individuo.dta"

* Output: "$dta\Pix_individuo_adoption.dta"


clear all
set more off, permanently 

set emptycells drop

global log "D:\PIX_Matheus\Stata\log"
global dta "D:\PIX_Matheus\Stata\dta"
global output "D:\PIX_Matheus\Output"
global origdata "D:\PIX_Matheus\DadosOriginais"

* ADO 
adopath ++ "D:\ADO"

capture log close
log using "$log\Pix_individuo_adoption.log", replace 

use "$dta\Pix_individuo.dta", replace 

*For tests:
use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\pix\sample_pix_for_tests.dta", replace

*keep variables that matter:
keep id muni_cd time_id tipo_pessoa trans_rec trans_sent

* Create Pix Adoption 

gen dummy_rec = 0
gen dummy_sent = 0
replace dummy_rec = 1 if trans_rec > 0 & trans_rec <.
replace dummy_sent = 1 if trans_sent > 0 & trans_sent <.

* Dummy for first time receiving or sending
by id tipo_pessoa muni_cd (time_id), sort: gen noccur_rec = sum(dummy_rec)
by id tipo_pessoa muni_cd: gen byte first_rec = noccur_rec == 1  & noccur_rec[_n - 1] != noccur_rec

by id tipo_pessoa muni_cd (time_id), sort: gen noccur_sent = sum(dummy_sent)
by id tipo_pessoa muni_cd: gen byte first_sent = noccur_sent == 1  & noccur_sent[_n - 1] != noccur_sent

*Dummy for first time that individual learned to do both
gen first = first_sent + first_rec
gen ntime_id = -time_id

by id tipo_pessoa muni_cd (ntime_id), sort: gen noccur = sum(first)
by id tipo_pessoa muni_cd: gen byte adoption_event = noccur == 1  & noccur[_n - 1] != noccur

*Save only Adoption & Fill panel data
sort id tipo_pessoa muni_cd time_id
keep id tipo_pessoa muni_cd time_id adoption_event


*Commenting out this section because of RAM
/*

* Does id and time_id fully identifies each row?
isid id time_id
*If that is not the case, substitute id for panelid -> egen panelid = group(id tipo_pessoa muni_cd), label
*And run -> duplicates report id time_id

tsset id time_id
tsfill, full

replace adoption_event = 0 if adoption_event == .

*fill the gaps & make variable 1 for every month after adoption_event. 
sort id time_id
by id: carryforward muni_cd, replace
by id: carryforward tipo_pessoa, replace

gsort id - time_id
by id: carryforward muni_cd, replace
by id: carryforward tipo_pessoa, replace

gen adoption = 1 if adoption_event == 1
sort id time_id
by id: carryforward adoption, replace
replace adoption = 0 if adoption == .

gen adoption_date = time_id if adoption_event == 1
sort id time_id
by id: carryforward adoption_date, replace
gsort id - time_id
by id: carryforward adoption_date, replace
sort id time_id

drop adoption_event 
*/

save "$dta\Pix_individuo_adoption.dta", replace 
log close