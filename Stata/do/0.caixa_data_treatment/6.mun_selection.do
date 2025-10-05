*Mun selection
* Our goal is to compare two types of municipality
* type one is strictly closer to caixa. That means it is closer to caixa and any other bank is far away. 
*type two is strictly closer to ONE bank and any other, and that one bank is not caixa. 


use "$path\population\dta\population2021.dta", replace
merge 1:1 id_municipio using "C:\Users\mathe\Dropbox\RESEARCH\Municipios\dta\municipios2.dta"
keep if _merge == 3
drop if id_municipio_bcb == 36263 // there seems to be a problem with it 
keep id_municipio populacao id_municipio_bcb
sort populacao
gen ranking = _n

sum populacao, detail
display  38297.6*5570
* 213 mi
sum populacao if ranking <= 5000
display 14358.06*5000
*72 mi

save "$path\population\dta\population2021_bcb_unique.dta", replace

use "$path\population\dta\population2021_bcb_unique.dta", replace
keep if ranking <= 5000
save "$path\pix\pix-event-study\Stata\dta\ibge\bottom_5000_mun.dta", replace


use "$path\population\dta\population2021_bcb_unique.dta", replace
keep if ranking > 5000
save "$path\pix\pix-event-study\Stata\dta\ibge\top_500_mun.dta", replace








use "$path\pix\pix-event-study\Stata\dta\Base_week_muni_sem_qtd_menor3\Base_week_muni_sem_qtd_menor3.dta", replace
rename muni_cd id_municipio_bcb
merge m:1 id_municipio_bcb using "$path\population\dta\population2021_bcb_unique.dta"
keep if _merge == 3
drop _merge

  
sum qtd_PIX_inflow if ranking <= 5000
sum qtd_PIX_outflow if ranking <= 5000
sum qtd_PIX_intra if ranking <= 5000
display 372000*8000
3,000,000,000

sum qtd_PIX_inflow if ranking > 5000
sum qtd_PIX_outflow if ranking > 5000
sum qtd_PIX_intra if ranking > 5000
display 42450*280000
12 billion

** Lets use weekly data now

**** Restricting the Municipalities

* Do a proper Diff diff - Synthetic control or CEM

* 

**** Not restricting


	clear
	clear mata
	clear matrix
	

	foreach path in D:\Dropbox C:\Dropbox C:\Users\mathe\Dropbox\RESEARCH {
		capture cd `path'
		if _rc == 0 macro def path `path'
	}	


********************************************************************************
* MUN SELECTION
********************************************************************************
use "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_2020_estban_closest_caixa_bank.dta", replace
keep id_municipio quantity_bank
rename id_municipio closest_bank_id_mun 
rename quantity_bank closest_bank_quantity_bank
save "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\temp_closest_caixa_bank.dta", replace

use "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_2020_estban_closest_caixa_bank.dta", replace

drop if quantity_bank > 1
merge m:1 closest_bank_id_mun using "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\temp_closest_caixa_bank.dta"
drop if _merge == 2
drop _merge

replace closest_caixa_d = 0 if closest_caixa_d == .
replace closest_bank_quantity_bank = 1 if closest_bank_quantity_bank == .
drop if closest_bank_quantity_bank>1

* Mun strictly closer to caixa
gen caixa_treatment = 1 if closest_caixa_d == closest_bank_d
replace caixa_treatment = 0 if caixa_treatment == . 
gen treatment = 1 if closest_bank_d < closest_caixa_d
replace treatment = 0 if treatment == . 
tab caixa_treatment
sum populacao if caixa_treatment == 1
sum populacao if caixa_treatment == 0
tab caixa
tab bank 

save "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_selection_closest_caixa_bank.dta", replace
use "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_selection_closest_caixa_bank.dta", replace
keep id_municipio id_municipio_bcb quantity_bank closest_bank_d closest_caixa_d populacao caixa_treatment treatment
save "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_selection.dta", replace


use "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_2020_estban_closest_caixa_bank.dta", replace
merge m:1 closest_bank_id_mun using "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\temp_closest_caixa_bank.dta"
drop if _merge == 2
drop _merge
replace closest_caixa_d = 0 if closest_caixa_d == .
replace closest_bank_quantity_bank = 1 if closest_bank_quantity_bank == .
gen caixa_treatment = 1 if closest_caixa_d == closest_bank_d
replace caixa_treatment = 0 if caixa_treatment == . 
gen treatment = 1 if closest_bank_d < closest_caixa_d
replace treatment = 0 if treatment == . 
save "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\all_mun_closest_caixa_bank.dta", replace

use "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\all_mun_closest_caixa_bank.dta", replace
keep id_municipio id_municipio_bcb quantity_bank closest_bank_d closest_caixa_d populacao caixa_treatment treatment
save "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\all_mun.dta", replace


********************************************************************************

use "$path\pix\pix-event-study\Stata\dta\Base_week_muni_sem_qtd_menor3\Base_week_muni_sem_qtd_menor3.dta", replace
rename muni_cd id_municipio_bcb
merge m:1 id_municipio_bcb using "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_selection.dta"
keep if _merge ==3
drop _merge

sort id_municipio week
gen week2 = week
order id_municipio week week2
gen D = 0
replace D = 1 if week > 3189

drop if week >= 3241

gen transactions_pix = qtd_PIX_inflow + qtd_PIX_outflow + qtd_PIX_intra
gen trans_capita = transactions_pix / populacao
gen log_transactions_pix = log(transactions_pix)

gen value_pix = valor_PIX_inflow + valor_PIX_outflow + valor_PIX_intra
gen value_capita = value_pix / populacao
gen log_value_pix = log(value_pix)


save "$path\pix\pix-event-study\Stata\dta\Base_week_muni_sem_qtd_menor3\regression_ready.dta", replace
*export delimited using "$path\pix\pix-event-study\CSV\regression_ready_pix_ted_boleto.csv", replace





********************************************************************************
use "$path\pix\pix-event-study\Stata\dta\Base_week_muni_sem_qtd_menor3\regression_ready.dta", replace

keep week transactions_pix value_pix treatment
collapse (sum) transactions_pix value_pix, by(week treatment)
gen log_transactions_pix = log(transactions_pix)
gen log_value_pix = log(value_pix)
keep week log_transactions_pix log_value_pix treatment
 
reshape wide log_transactions_pix log_value_pix, i(week) j(treatment)



gen week2 = week 
drop if week < 3167

*At 3189 15.458197 Treatment - 12.52942 Control
gen log_transactions_difference = log_transactions_pix1 - log_transactions_pix0 

line log_transactions_difference week, ///
	lc(black blue green red purple) /// 
	title("Log Transactions: Treatment - Control") ///
	legend(order(1 "Treatment - Control")) ///
	ytitle("Log Transactions") xtitle("Weeks") /// 
	xline(3189) ///
	xmlabel(3189 "Shock", angle(90))

line log_transactions_pix0 log_transactions_pix1 week, ///
	lc(black blue green red purple) /// 
	title("Log Transactions over Time") ///
	legend(order(1 "Control" 2 "Treatment")) ///
	ytitle("Log Transactions") xtitle("Weeks") /// 
	xline(3189) ///
	xmlabel(3189 "Shock", angle(90))

	
gen log_value_difference = log_value_pix1 - log_value_pix0 

	
line log_value_difference week, ///
	lc(black blue green red purple) /// 
	title("Log Value: Treatment - Control") ///
	legend(order(1 "Treatment - Control")) ///
	ytitle("Log Transactions") xtitle("Weeks") /// 
	xline(3189) ///
	xmlabel(3189 "Shock", angle(90))

line log_value_pix0 log_value_pix1 week, ///
	lc(black blue green red purple) /// 
	title("Log Value over Time") ///
	legend(order(1 "Control" 2 "Treatment")) ///
	ytitle("Log Value") xtitle("Weeks") /// 
	xline(3189) ///
	xmlabel(3189 "Shock", angle(90))
	
*graph export "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output\graphs\bank_presence.png", as(png) name("Graph") replace


********************************************************************************











**** ALL MUN

use "$path\pix\pix-event-study\Stata\dta\Base_week_muni_sem_qtd_menor3\Base_week_muni_sem_qtd_menor3.dta", replace
rename muni_cd id_municipio_bcb
merge m:1 id_municipio_bcb using "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\all_mun.dta"
keep if _merge ==3
drop _merge

sort id_municipio week
gen week2 = week
order id_municipio week week2
gen D = 0
replace D = 1 if week > 3189

drop if week >= 3241

gen transactions_pix = qtd_PIX_inflow + qtd_PIX_outflow + qtd_PIX_intra
gen trans_capita = transactions_pix / populacao
gen log_transactions_pix = log(transactions_pix)

gen value_pix = valor_PIX_inflow + valor_PIX_outflow + valor_PIX_intra
gen value_capita = value_pix / populacao
gen log_value_pix = log(value_pix)


save "$path\pix\pix-event-study\Stata\dta\Base_week_muni_sem_qtd_menor3\regression_ready_all_mun.dta", replace
export delimited using "$path\pix\pix-event-study\CSV\regression_ready_pix_ted_boleto_all_mun.csv", replace


********************************************************************************
use "$path\pix\pix-event-study\Stata\dta\Base_week_muni_sem_qtd_menor3\regression_ready_all_mun.dta", replace

keep week transactions_pix value_pix treatment
collapse (sum) transactions_pix value_pix, by(week treatment)
gen log_transactions_pix = log(transactions_pix)
gen log_value_pix = log(value_pix)
keep week log_transactions_pix log_value_pix treatment
 
reshape wide log_transactions_pix log_value_pix, i(week) j(treatment)



gen week2 = week 
drop if week < 3167

*At 3189 15.458197 Treatment - 12.52942 Control
cap drop log_transactions_difference
gen log_transactions_difference = log_transactions_pix1 - log_transactions_pix0 

line log_transactions_difference week, ///
	lc(black blue green red purple) /// 
	title("Log Transactions: Treatment - Control") ///
	legend(order(1 "Treatment - Control")) ///
	ytitle("Log Transactions") xtitle("Weeks") /// 
	xline(3189) ///
	xmlabel(3189 "Shock", angle(90))

line log_transactions_pix0 log_transactions_pix1 week, ///
	lc(black blue green red purple) /// 
	title("Log Transactions over Time") ///
	legend(order(1 "Control" 2 "Treatment")) ///
	ytitle("Log Transactions") xtitle("Weeks") /// 
	xline(3189) ///
	xmlabel(3189 "Shock", angle(90))

gen log_value_difference = log_value_pix1 - log_value_pix0 

	
line log_value_difference week, ///
	lc(black blue green red purple) /// 
	title("Log Value: Treatment - Control") ///
	legend(order(1 "Treatment - Control")) ///
	ytitle("Log Transactions") xtitle("Weeks") /// 
	xline(3189) ///
	xmlabel(3189 "Shock", angle(90))

line log_value_pix0 log_value_pix1 week, ///
	lc(black blue green red purple) /// 
	title("Log Value over Time") ///
	legend(order(1 "Control" 2 "Treatment")) ///
	ytitle("Log Value") xtitle("Weeks") /// 
	xline(3189) ///
	xmlabel(3189 "Shock", angle(90))	
graph export "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output\graphs\bank_presence.png", as(png) name("Graph") replace


********************************************************************************





















*****************************************************************
use "$path\pix\pix-event-study\Stata\dta\Base_week_muni_sem_qtd_menor3\regression_ready.dta", replace

use "$path\pix\pix-event-study\CSV\regression_ready_pix_ted_boleto.csv", clear
reg log_transactions_pix D##caixa_treatment i.week i.id_municipio

****
*Correct this code later
collapse (mean) log_transactions_pix log_value_pix, by(week caixa_treatment)
reshape wide log_transactions_pix log_value_pix, i(week) j(caixa_treatment)

line log_transactions_pix0 log_transactions_pix1 week
line log_value_pix0 log_value_pix1 week
****

******************************************************************


* How can I add controls that dont change over time? Are they all absorbed by the fixed effect on the municipality

*populacao
*i.week 

*i.id_municipio

*closest_caixa_d closest_bank_d








* Drop some observations to do i.id_municipio
tempfile holding
save `holding'

keep id_municipio
duplicates drop

set seed 1234
sample 500, count

merge 1:m id_municipio using `holding'

keep if caixa_treatment == 1 | _merge == 3
drop _merge



 




reg quant_capita D##caixa_treatment populacao i.date i.id_municipio closest_bank_d


****
*Correct this code later
collapse (mean) quantity value, by(date caixa_treatment)
reshape wide quantity value, i(date) j(caixa_treatment)

line quantity0 quantity1 date
line value0 value1 date
****

*****
** diff and diff controlling for population - STATA 17

didregress (quantity populacao) (caixa_treatment), group(id_municipio) time(date)
estat trendplots
estat ptrends
estat granger

*****
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	













*Mun selection
* Our goal is to compare two types of municipality
* type one is strictly closer to caixa. That means it is closer to caixa and any other bank is far away. 
*type two is strictly closer to ONE bank and any other, and that one bank is not caixa. 


	clear
	clear mata
	clear matrix
	

	foreach path in D:\Dropbox C:\Dropbox C:\Users\mathe\Dropbox\RESEARCH {
		capture cd `path'
		if _rc == 0 macro def path `path'
	}	



use "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_2020_estban_closest_caixa_bank.dta", replace
keep id_municipio quantity_bank
rename id_municipio closest_bank_id_mun 
rename quantity_bank closest_bank_quantity_bank
save "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\temp_closest_caixa_bank.dta", replace



use "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_2020_estban_closest_caixa_bank.dta", replace

drop if quantity_bank > 1
merge m:1 closest_bank_id_mun using "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\temp_closest_caixa_bank.dta"
drop if _merge == 2
drop _merge

replace closest_caixa_d = 0 if closest_caixa_d == .
replace closest_bank_quantity_bank = 1 if closest_bank_quantity_bank == .
drop if closest_bank_quantity_bank>1

* Mun strictly closer to caixa
gen caixa_treatment = 1 if closest_caixa_d == closest_bank_d
replace caixa_treatment = 0 if caixa_treatment == . 
tab caixa_treatment
sum populacao if caixa_treatment == 1
sum populacao if caixa_treatment == 0
tab caixa
tab bank 

save "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_selection_closest_caixa_bank.dta", replace
use "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_selection_closest_caixa_bank.dta", replace
keep id_municipio caixa_treatment populacao closest_bank_d closest_caixa_d
save "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_selection.dta", replace


use "$path\pix\dta\Pix\transactions_month.dta", replace 
merge m:1 id_municipio using "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_selection.dta"
keep if _merge ==3
drop _merge

gen date2 = date
gen D = 0
replace D = 1 if date > 735

* 2021m8 seemed to be incomplete
drop if date == 739

* Drop some observations to do i.id_municipio
tempfile holding
save `holding'

keep id_municipio
duplicates drop

set seed 1234
sample 500, count

merge 1:m id_municipio using `holding'

keep if caixa_treatment == 1 | _merge == 3
drop _merge

gen quant_capita = quantity / populacao

reg quant_capita D##caixa_treatment populacao i.date closest_caixa_d closest_bank_d

reg quant_capita D##caixa_treatment populacao i.date i.id_municipio closest_bank_d


****
*Correct this code later
collapse (mean) quantity value, by(date caixa_treatment)
reshape wide quantity value, i(date) j(caixa_treatment)

line quantity0 quantity1 date
line value0 value1 date
****

*****
** diff and diff controlling for population - STATA 17

didregress (quantity populacao) (caixa_treatment), group(id_municipio) time(date)
estat trendplots
estat ptrends
estat granger

*****


******************************************
*LETS DO THE OTHER DIFF DIFF, BASED ON DISTANCE TO CAIXA, CONTROLLED BY DISTANCE TO THE NEAREST.
******************************************




*Mun selection
* Doing this strategy seems to show that being closer to caixa is good, and not bad
* this may happens because closer to caixa are only a few municipalities with a greater population average. 

* LETS DO MUNICIPALITIES CLOSER TO CAIXA VS CLOSER TO OTHER BANKS










* Our goal is to compare two types of municipality
* type one is strictly closer to caixa. That means it is closer to caixa and any other bank is far away. 
*type two is strictly closer to ONE bank and any other, and that one bank is not caixa. 


	clear
	clear mata
	clear matrix
	

	foreach path in D:\Dropbox C:\Dropbox C:\Users\mathe\Dropbox\RESEARCH {
		capture cd `path'
		if _rc == 0 macro def path `path'
	}	

use "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_2020_estban_closest_caixa_bank.dta", replace








