* Create instrument

use "C:\Users\mathe\Dropbox\RESEARCH\Municipios\dta\mun_2020_estban_closest_caixa_bank.dta", replace

* Substitute . for 0
replace closest_bank_d = 0 if closest_bank_d == .
replace closest_caixa_d = 0 if closest_caixa_d == .

* create instrument: Is your closest bank a caixa? 0 or 1
* if caixa dist = closest distacia 

gen closest_caixa = 1 if closest_bank_d == closest_caixa_d
replace closest_caixa = 0 if closest_caixa == .

save "C:\Users\mathe\Dropbox\RESEARCH\Municipios\dta\mun_2020_estban_closest_caixa_bank2.dta", replace

