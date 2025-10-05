* Incomplete code with the idea of aggregating by individuo



* Monta_Base_CCS_Muni_flood

use "D:\PIX_Matheus\Stata\dta\CCS_muni_banco_PF.dta"






 





* Individuo

* What is the dta with only CCS??

use "$dta\Pix_bank_account.dta", clear
*************
* After PIX
*************
keep id muni_cd time_id value_rec trans_rec value_sent trans_sent after_first_pix_rec after_first_pix_sent after_first_pix after_first_pix after_adoption sender receiver user n_account_new n_account_stock

order id muni_cd time_id 

*check for duplicates
duplicates tag id time_id, gen(dup)
tab dup
drop if dup == 1
drop dup

*check if need tsfill - Foi feito no outro dta (Monta_base_Muni_Banco_self) mas parece que precisa
sort id time_id
tsset id time_id
tsfill, full

* create some new variables
* Idea: Aggregate by muni_cd and see the adoption of the city


* merge with flood
merge m:1 muni_cd time_id using "$dta\flood_monthly_2020_2022.dta", keep(3) keepusing(date_flood)
drop _merge



*************
* Before PIX
*************
