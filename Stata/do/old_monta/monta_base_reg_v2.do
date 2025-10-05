*monta_base_reg

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
log using "$log\monta_base_reg_v2.log", replace 

/*
*Fake data
global dta "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta"
global output "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output"

use "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/pix/fake_panel_data.dta", clear
set seed 1234
gen random=runiform()
bysort id: gen grupo=cond(random[1]<=1/3,1,cond(random[1]>2/3,3,2))
drop random
set seed 1235
gen random=runiform()
cap drop muni_cd
bysort id: gen muni_cd=cond(random[1]<=1/5,1100015,cond(random[1]>2/5,1100023,cond(random[1]>3/5,1100031,cond(random[1]>4/5,1100049,1100056))))
drop random

gen mun_cd = muni_cd
gen after_first_pix_sent = after_adoption
gen after_first_pix_rec = after_adoption
gen after_first_pix = after_adoption

gen dist_bank = expected_distance

*/

use "$dta\Pix_PF_adoption.dta", clear 

*-------------------------------------------------------------------------------
* PART 1: Creating new variables and preparing dataset 
*-------------------------------------------------------------------------------
*merge m:1 id using "\\sbcdf176\PIX_Matheus$\Stata\dta\cpf_bolsa_familia.dta", keep(1 3) nogenerate

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
gen dist_event = time_id - mofd(mdy(5, 1, 2021))  // MAIO/21 é o tempo zero
egen id_dist=group(dist_event)
format %tmNN/CCYY time_id
labmask id_dist, values(time_id)

*-----------------------------------------------


*cap drop log_trans_sent
*gen log_trans_sent = log(trans_sent) 
* trans_sent sometimes is 0

*-----------------------------------------------

cap drop dist_expec
gen dist_expec = dist_caixa - expected_distance
cap drop dist_expec2
gen dist_expec2 = dist_expec*abs(dist_expec)
cap drop dist_expec3
gen dist_expec3 = dist_expec^3
cap drop dist_caixa2
gen dist_caixa2 = dist_caixa^2
cap drop dist_caixa_bank
gen dist_caixa_bank = dist_caixa - dist_bank
cap drop dist_caixa_bank2
gen dist_caixa_bank2 = dist_caixa_bank*abs(dist_caixa_bank)
cap drop dist_caixa_bank3
gen dist_caixa_bank3 = dist_caixa_bank^3

*-----------------------------------------------
* HOW WILL after_first_pix_rec change when date_first_pix_rec is missing? after_pix will be 0
* Must make sure date_first_pix_rec is the same for all id. 
sort id time_id
bysort id: carryforward date_first_pix_rec, replace
gsort id - time_id
bysort id: carryforward date_first_pix_rec, replace

sort id time_id
bysort id: carryforward date_first_pix_sent
gsort id - time_id
bysort id: carryforward date_first_pix_sent

cap drop after_first_pix_rec
gen after_first_pix_rec = time_id >= date_first_pix_rec
cap drop after_first_pix_sent
gen after_first_pix_sent = time_id >= date_first_pix_sent
cap drop after_first_pix
gen after_first_pix = min(after_first_pix_rec, after_first_pix_sent)

*** We will, sometimes, condition the regressions on having send no Pix before april.  
gen temp = 0
replace temp = 1 if after_first_pix_sent == 1 & time_id == ym(2021,4)
bysort id: egen previous_takers_sent = max(temp)
drop temp

gen temp = 0
replace temp = 1 if after_first_pix_rec == 1 & time_id == ym(2021,4)
bysort id: egen previous_takers_rec = max(temp)
drop temp

gen temp = 0
replace temp = 1 if after_first_pix == 1 & time_id == ym(2021,4)
bysort id: egen previous_takers = max(temp)
drop temp

gen all0 = 0 // 0 For every value, previous_takers == 0 when they didnt adopt in april 2021.

*-----------------------------------------------

gen after_adoption = 0
replace after_adoption = 1 if after_first_pix_rec == 1 & after_first_pix_sent == 1
bysort id : egen temp = min(time_id) if after_adoption > 0
bysort id : egen date_adoption = max(temp) 
drop temp
format %tmNN/CCYY date_adoption
*-----------------------------------------------
* Value per transaction
*gen avg_ticket_sent = 0
*replace avg_ticket_sent = value_sent/trans_sent if trans_sent > 0

gen sender = 0
replace sender = 1 if trans_sent > 0
gen receiver = 0
replace receiver = 1 if trans_rec > 0
gen user = 0
replace user = 1 if trans_rec > 0 | trans_sent > 0
*-----------------------------------------------


gen treat_exp = 0
replace treat_exp = 1 if dist_caixa > expected_distance
label var treat_exp "Dist Caixa > Exp Dist"

gen treat_bank = 0
replace treat_bank = 1 if dist_caixa > dist_bank
label var treat_bank "Dist Caixa > Dist Bank"

*-----------------------------------------------
cap label var after_adoption "Adoption"
cap label var after_first_pix_sent "Sender Adoption"
cap label var after_first_pix_rec "Receiver Adoption"
cap label var expected_distance "Expected Distance"
cap label var dist_bank "Dist to Bank"
cap label var dist_caixa "Dist to Caixa"
cap label var id "ID"
cap label var time_id "Month"
cap label var trans_rec "Transactions Received"
cap label var trans_sent "Transactions Sent"
cap label var value_sent "Value Sent"
cap label var value_rec "Value Received"
cap label var log_trans_sent "Log Transactions Sent"
cap label variable dist_expec "Recentered Dist"
cap label variable dist_expec2 "Recentered Dist^2"
cap label variable dist_expec3 "Recentered Dist^3"
cap label variable dist_caixa2  "Dist to Caixa^2"
cap label variable dist_caixa3  "Dist to Caixa^3"
cap label variable dist_caixa_bank "DistCaixa-DistBank"
cap label variable dist_caixa_bank2 "(DistCaixa-DistBank)^2"
cap label variable dist_caixa_bank3 "(DistCaixa-DistBank)^3"
cap label variable previous_takers_rec "Previous Receivers"
cap label variable previous_takers_sent "Previous Senders"
cap label variable previous_takers "Previous Users"
cap label variable sender "Senders"
cap label variable receiver "Receivers"
cap label variable user "Users"

* ------------------------------------------------------------------------------

save "$dta\Pix_PF_adoption_new_variables.dta", replace
*save "$dta\fake_panel_data_new_variables.dta", replace


log close