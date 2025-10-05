* Bank Rates


********************************************************************************
* Add Poupanca vs CDI
********************************************************************************


import delimited "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\CSV\bank_rates\Poupanca e Taxa Referencial - STI-20230210171838247.csv", varnames(1) clear 

drop if date == "Source"

* Change date
gen date2 = date(date, "DMY")
format date2 %d
drop date
rename date2 date
gen enddate2 = date(enddate, "DMY")
format enddate2 %d
drop enddate
rename enddate2 enddate

ds, has(type string) 
foreach v in `r(varlist)' { 
    replace `v' = trim(`v') 
	replace `v' = subinstr(`v', "-", "",.)
	destring `v', replace
} 
cap drop poupanca
gen poupanca = 100*((1+savingdepositsafter05042012retur/100)^(252/(enddate - date))-1)

save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\bank_rates\poupanca.dta", replace


import delimited "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\CSV\bank_rates\Selic CDI and Savings deposits - STI-20230210172043856.csv", varnames(1) clear 

drop if date == "Source"

* Change date
gen date2 = date(date, "DMY")
format date2 %d
drop date
rename date2 date

ds, has(type string) 
foreach v in `r(varlist)' { 
    replace `v' = trim(`v') 
	*replace `v' = subinstr(`v', "-", "",.)
	replace `v' = subinstr(`v', ",", "",.)
	replace `v' = "" if `v' == "-"
	destring `v', replace
} 
save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\bank_rates\cdi.dta", replace

import delimited "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\CSV\bank_rates\Federal securities debt - STI-20230210172148997.csv", varnames(1) clear 

drop if date == "Source"

* Change date
gen date2 = date(date, "DMY")
format date2 %d
drop date
rename date2 date

ds, has(type string) 
foreach v in `r(varlist)' { 
    replace `v' = trim(`v') 
	*replace `v' = subinstr(`v', "-", "",.)
	replace `v' = subinstr(`v', ",", "",.)
	replace `v' = "" if `v' == "-"
	destring `v', replace
} 

save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\bank_rates\fed_debt.dta", replace

*******************************************************************************


use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\bank_rates\poupanca.dta", replace

sort date
quietly by date:  gen dup = cond(_N==1,0,_n)
tab dup
tab date if dup > 0
drop if dup>0 & savingdepositsafter05042012retur == .
tab date if dup > 0
drop dup
merge 1:1 date using "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\bank_rates\cdi.dta"
drop _m

keep date poupanca interestratecdiinannualtermsbasi
rename interestratecdiinannualtermsbasi cdi
 
gen date2 = date
keep if date >= 21915


line poupanca cdi date, ///
	title("Deposit Return") ///
	legend(order(1 "Traditional Banks (Poupança)" 2 "Fintechs (CDI)")) ///
	ytitle("Annual Return (%)") xtitle("") 
	*lc(black blue green red purple) ///
	*xline(3189) ///
	*xmlabel(3189 "Shock", angle(90))
graph export "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output\graphs\Deposit Return.png", as(png) name("Graph") replace



