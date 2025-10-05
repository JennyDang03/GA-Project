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
log using "$log\monta_base_reg_v1.log", replace 

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
gen dist_event = time_id- mofd(mdy(5, 1, 2021))  // MAIO/21 é o tempo zero
egen id_dist=group(dist_event)
format %tmNN/CCYY time_id
labmask id_dist, values(time_id)

*-----------------------------------------------

gen after_adoption = 0
replace after_adoption = 1 if after_first_pix_rec == 1 & after_first_pix_sent == 1
bysort id : egen temp = min(time_id) if after_adoption > 0
bysort id : egen date_adoption = max(temp) 
drop temp
format %tmNN/CCYY date_adoption

*-----------------------------------------------

*cap drop log_trans_sent
*gen log_trans_sent = log(trans_sent) 
* trans_sent sometimes is 0

*-----------------------------------------------
cap label var after_adoption "Adoption"
cap label var after_first_pix_sent "Sender"
cap label var after_first_pix_rec "Receiver"
cap label var expected_distance "Expected Distance"
cap label var dist_bank "Distance to Bank"
cap label var dist_caixa "Distance to Caixa"
cap label var id "ID"
cap label var time_id "Month"
cap label var trans_rec "Transactions Received"
cap label var trans_sent "Transactions Sent"
cap label var value_sent "Value Received"
cap label var value_rec "Value Sent"
cap label var log_trans_sent "Log Transactions Sent"

save "$dta\Pix_PF_adoption_new_variables.dta", replace

*save "$dta\fake_panel_data_new_variables.dta", replace
/*
* Bring back transactions and values
*trans_rec trans_sent value_sent value_rec

* Do logit
* Do triple difference - with people not on covid relief. 
*/
log close