* Data with Address

********************************************************************************
* 1. Clean Data
********************************************************************************


* 1.1 Bank Branch - Agencias
import excel "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\CSV\bank_locations\202104AGENCIAS.xlsx", sheet("Plan1") cellrange(A10:R19332) firstrow case(lower) clear
drop if sequencialdocnpj == .

cap drop bank
gen bank = "Other"
replace bank = "Bradesco" if cnpj == "60746948"
replace bank = "BB" if cnpj == "00.000.000"
replace bank = "Santander" if cnpj == "90400888"
replace bank = "Caixa" if cnpj == "00.360.305"
replace bank = "Itau" if cnpj == "60701190"

* Change cnpj to number
replace cnpj = subinstr(cnpj, ".", "",.)
destring cnpj, gen(cnpj_number)

* Change cep to number
replace cep = subinstr(cep, "-", "",.)
gen cep5 = substr(cep,1,5)
destring cep, gen(cep_number)
destring cep5, replace

* Change datainício to start_date
gen start_date = date(datainício, "DMY")
format start_date %d
drop datainício

* Branch ID - possible to merge with ESTBAN
gen branch_id = real(string(sequencialdocnpj) + string(dvdocnpj))

/*
use "C:\Users\mathe\Dropbox\RESEARCH\ESTBAN\dta\0-estban_raw_AG.dta"
keep if year >= 2019
keep if month == 4
replace agencia = subinstr(agencia, "'", "",.)
destring agencia, replace
rename agencia branch_id
gen cnpj_number = cnpj

merge m:1 cnpj_number branch_id using "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\bank_address.dta", force
*/

* merge with id_mun 
sort município uf
rename município mun_name
rename uf state

foreach var of varlist mun_name{
gen Z=lower(`var')
drop `var'
rename Z `var'
}

ds, has(type string) 
foreach v in `r(varlist)' { 
    replace `v' = trim(`v') 
} 

*Generate ID
duplicates report
gen location_id = _n
gen type = "branch"

save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\bank_address.dta", replace
*use  "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\bank_address.dta", replace

***

use "C:\Users\mathe\Dropbox\RESEARCH\Municipios\dta\municipios2.dta", clear
keep id_municipio id_municipio_bcb nome sigla_uf id_uf
rename sigla_uf state
gen mun_name = nome
rename nome mun_name_original 
rename id_uf state_id

foreach var of varlist mun_name{
gen Z=lower(`var')
drop `var'
rename Z `var'
}
replace mun_name = subinstr(mun_name, "á", "a",.)
replace mun_name = subinstr(mun_name, "é", "e",.)
replace mun_name = subinstr(mun_name, "í", "i",.)
replace mun_name = subinstr(mun_name, "ó", "o",.)
replace mun_name = subinstr(mun_name, "ú", "u",.)
replace mun_name = subinstr(mun_name, "ã", "a",.)
replace mun_name = subinstr(mun_name, "õ", "o",.)
replace mun_name = subinstr(mun_name, "â", "a",.)
replace mun_name = subinstr(mun_name, "ê", "e",.)
replace mun_name = subinstr(mun_name, "î", "i",.)
replace mun_name = subinstr(mun_name, "ô", "o",.)
replace mun_name = subinstr(mun_name, "û", "u",.)

replace mun_name = subinstr(mun_name, "Á", "a",.)
replace mun_name = subinstr(mun_name, "É", "e",.)
replace mun_name = subinstr(mun_name, "Â", "a",.)
replace mun_name = subinstr(mun_name, "Ó", "o",.)

replace mun_name = subinstr(mun_name, "ç", "c",.)

replace mun_name = trim(mun_name)

save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_ids.dta", replace

***

use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_ids.dta", replace

merge 1:m mun_name state using "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\bank_address.dta", force

keep if _m == 3
drop _m

save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\bank_address_mun.dta", replace


* 1.2 Lotery Shops - Lotericas
import excel "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\CSV\bank_locations\202104CORRESPONDENTES.xlsx", sheet("desig_REL2143545(1)") cellrange(A7:Z444767) firstrow case(lower) clear

rename serviçosprestados function1
rename q function2
rename r function3
rename s function4
rename t function5
rename u function6
rename v function8
drop w x y z 

drop if cnpjdocorrespondente == . 

cap drop bank
gen bank = "Other"
replace bank = "Bradesco" if cnpjdacontratante == "60746948"
replace bank = "BB" if cnpjdacontratante == "0"
replace bank = "Santander" if cnpjdacontratante == "90400888"
replace bank = "Caixa" if cnpjdacontratante == "360305"
replace bank = "Itau" if cnpjdacontratante == "60701190"

* Change variables to number
destring cnpjdacontratante, replace
destring sufi, replace
destring dv, replace
gen nºdeordemdainstalação_original = nºdeordemdainstalação
replace nºdeordemdainstalação = subinstr(nºdeordemdainstalação, "I", "",.)
replace nºdeordemdainstalação = subinstr(nºdeordemdainstalação, "P", "",.)
replace nºdeordemdainstalação = trim(nºdeordemdainstalação)
destring nºdeordemdainstalação, replace

replace cep = trim(cep)
replace cep = subinstr(cep, "-", "",.)
replace cep = subinstr(cep, ".", "",.)
cap drop cep5
gen cep5 = substr(cep,1,5)
*tab bank if cep == ""
destring cep, gen(cep_number) force
destring cep5, replace force 

* FunctionsX variables
foreach var of varlist function*{
	* Seems that some CEPs are on functions, very few
	replace `var' = trim(`var')
	replace `var' = "1" if `var' == "X"
	replace `var' = "0" if `var' == ""
	keep if `var' == "1" | `var' == "0"
	destring `var', replace
}

/*
*Inc. I - recepção e encaminhamento de propostas de abertura de contas de depósitos à vista, a prazo e de poupança mantidas pela instituição contratante   
                                     
*Inc. II - realização de recebimentos, pagamentos e transferências eletrônicas visando à movimentação de contas de depósitos de titularidade de clientes mantidas pela instituição contratante
                                       
*Inc. III - recebimentos e pagamentos de qualquer natureza, e outras atividades decorrentes da execução de contratos e convênios de prestação de serviços mantidos pela instituição contratante com terceiros
                        
*Inc. IV - execução ativa e passiva de ordens de pagamento cursadas por intermédio da instituição contratante por solicitação de clientes e usuários   
                                                                                 
*Inc. V - recepção e encaminhamento de propostas de operações de crédito e de arrendamento mercantil concedidas pela instituição contratante, bem como outros serviços prestados para o acompanhamento da operação

*Inc. VI - recebimentos e pagamentos relacionados a letras de câmbio de aceite da instituição contratante

*Inc. VIII - recepção e encaminhamento de propostas de fornecimento de cartões de crédito de responsabilidade da instituição contratante                                              
*/

** Trim
foreach var of varlist nomedacontratante nomedocorrespondente tipodeinstalação municípiodainstalação uf endereço numeroend complemento numeroend bairro cep nºdeordemdainstalação_original{
	replace `var' = trim(`var')
}

* merge with id_mun 
sort municípiodainstalação uf
rename municípiodainstalação mun_name
rename uf state

foreach var of varlist mun_name{
	gen Z=lower(`var')
	drop `var'
	rename Z `var'
}

ds, has(type string) 
foreach v in `r(varlist)' { 
    replace `v' = trim(`v') 
} 

*Generate ID
duplicates report
duplicates drop
gen location_id = _n
gen type = "lotericas"

save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\lotery_address.dta", replace

*use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\lotery_address.dta", replace
***
use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_ids.dta", replace

merge 1:m mun_name state using "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\lotery_address.dta", force

keep if _m == 3
drop _m

save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\lotery_address_mun.dta", replace

* 1.3 ATMs - Caixas eletronicos

import excel "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\CSV\bank_locations\202104PAE.xlsx", sheet("Plan1") cellrange(A10:P27403) firstrow case(lower) clear

ds, has(type string) 
foreach v in `r(varlist)' { 
    replace `v' = trim(`v') 
} 

cap drop bank
gen bank = "Other"
replace bank = "Bradesco" if cnpj == "60746948"
replace bank = "BB" if cnpj == "00.000.000"
replace bank = "Santander" if cnpj == "90400888"
replace bank = "Caixa" if cnpj == "00.360.305"
replace bank = "Itau" if cnpj == "60701190"

*7679404 - BANCO TOPÁZIO S.A.
* Who is Banco24horas????????????????????????????????

* Delete miami, NY and last two rows
drop if uf == ""
* Change variables to number

* Cep
replace cep = subinstr(cep, "-", "",.)
replace cep = subinstr(cep, ".", "",.)
cap drop cep5
gen cep5 = substr(cep,1,5)
destring cep, gen(cep_number) force
destring cep5, replace force 
tab municipio if cep == ""

* cnpj
replace cnpj = subinstr(cnpj, ".", "",.)
destring cnpj, replace

** Mun_name
rename municipio mun_name
rename uf state

foreach var of varlist mun_name{
gen Z=lower(`var')
drop `var'
rename Z `var'
}


*Generate ID
duplicates report
duplicates drop
gen location_id = _n
gen type = "atm"

save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\atm_address.dta", replace

use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_ids.dta", replace

merge 1:m mun_name state using "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\atm_address.dta", force

keep if _m == 3
drop _m

save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\atm_address_mun.dta", replace

* 1.4 postos - Postos
import excel "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\CSV\bank_locations\202104POSTOS.xlsx", sheet("Plan1") cellrange(A10:Q18980) firstrow case(lower) clear

ds, has(type string) 
foreach v in `r(varlist)' { 
    replace `v' = trim(`v') 
} 

cap drop bank
gen bank = "Other"
replace bank = "Bradesco" if cnpj == "60746948"
replace bank = "BB" if cnpj == "00.000.000"
replace bank = "Santander" if cnpj == "90400888"
replace bank = "Caixa" if cnpj == "00.360.305"
replace bank = "Itau" if cnpj == "60701190"
* Who is Banco24horas????????????????????????????????

tab bank 
* Delete last two rows
drop if uf == ""

* cnpj
replace cnpj = subinstr(cnpj, ".", "",.)
destring cnpj, replace

* Cep
replace cep = subinstr(cep, "-", "",.)
replace cep = subinstr(cep, ".", "",.)
cap drop cep5
gen cep5 = substr(cep,1,5)
destring cep, gen(cep_number) force
destring cep5, replace force 
tab municipio if cep == ""


** Mun_name
rename municipio mun_name
rename uf state

foreach var of varlist mun_name{
gen Z=lower(`var')
drop `var'
rename Z `var'
}


*Generate ID
duplicates report
duplicates drop
gen location_id = _n
gen type = "postos"

save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\postos_address.dta", replace

use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_ids.dta", replace

merge 1:m mun_name state using "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\postos_address.dta", force

keep if _m == 3
drop _m

save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\postos_address_mun.dta", replace


* 1.5 filiais - filiais

import excel "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\CSV\bank_locations\202104FILIAISCONS.xlsx", sheet("Plan1") cellrange(A10:M352) firstrow case(lower) clear


ds, has(type string) 
foreach v in `r(varlist)' { 
    replace `v' = trim(`v') 
} 

cap drop bank
gen bank = "Other"
replace bank = "Bradesco" if cnpj == "60746948"
replace bank = "BB" if cnpj == "00.000.000"
replace bank = "Santander" if cnpj == "90400888"
replace bank = "Caixa" if cnpj == "00.360.305"
replace bank = "Itau" if cnpj == "60701190"

**** No big bank

tab bank 
* Delete last 6 rows
drop if uf == ""

* cnpj
replace cnpj = subinstr(cnpj, ".", "",.)
destring cnpj, replace

* Cep
replace cep = subinstr(cep, "-", "",.)
replace cep = subinstr(cep, ".", "",.)
cap drop cep5
gen cep5 = substr(cep,1,5)
destring cep, gen(cep_number) force
destring cep5, replace force 
tab municipio if cep == ""

* Change datainício to start_date
gen start_date = date(datainicio, "DMY")
format start_date %d
drop datainicio


** Mun_name
rename municipio mun_name
rename uf state

foreach var of varlist mun_name{
gen Z=lower(`var')
drop `var'
rename Z `var'
}

*Generate ID
duplicates report
duplicates drop
gen location_id = _n
gen type = "filiais"


save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\filiais_address.dta", replace

use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_ids.dta", replace

merge 1:m mun_name state using "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\filiais_address.dta", force

keep if _m == 3
drop _m

save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\filiais_address_mun.dta", replace




********************************************************************************
* 2.0 - Presence of Banks -> Merging
* Now lets use this information
********************************************************************************


* 2.1 - Agencias

use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\bank_address_mun.dta", replace
keep id_municipio id_municipio_bcb bank
tab bank // This data is used in a table in the paper
gen branches = 1
collapse (sum) branches, by(id_municipio id_municipio_bcb bank)
tab bank // This data is used in a table in the paper
** Make tables automatic
by id_municipio, sort: gen nvals = _n == 1
count if nvals // This data is used in a table in the paper
reshape wide branches, i(id_municipio id_municipio_bcb) j(bank) string
foreach var of varlist branches*{
	replace `var' = 0 if `var' == .
}
save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\bank_presence.dta", replace
*use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\bank_presence.dta", replace



* 2.2 - Lotericas 

***** Lotericas can be Caixa and BB


use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\lotery_address_mun.dta", replace
tab bank // This data is used in a table in the paper
foreach var of varlist function*{
	tab bank if `var' == 1
}
* function6 is not the answer
* function5 is the most popular
* function4 looks like it is the answer
*Function 3 seems to not be enough - evidence from Fortaleza R. Porto Alegre, 1775 - Autran Nunes
* Lotericas have everything but function 6 

/*
*Inc. I - recepção e encaminhamento de propostas de abertura de contas de depósitos à vista, a prazo e de poupança mantidas pela instituição contratante   
                                     
*Inc. II - realização de recebimentos, pagamentos e transferências eletrônicas visando à movimentação de contas de depósitos de titularidade de clientes mantidas pela instituição contratante
                                       
*Inc. III - recebimentos e pagamentos de qualquer natureza, e outras atividades decorrentes da execução de contratos e convênios de prestação de serviços mantidos pela instituição contratante com terceiros
                        
*Inc. IV - execução ativa e passiva de ordens de pagamento cursadas por intermédio da instituição contratante por solicitação de clientes e usuários   
                                                                                 
*Inc. V - recepção e encaminhamento de propostas de operações de crédito e de arrendamento mercantil concedidas pela instituição contratante, bem como outros serviços prestados para o acompanhamento da operação

*Inc. VI - recebimentos e pagamentos relacionados a letras de câmbio de aceite da instituição contratante

*Inc. VIII - recepção e encaminhamento de propostas de fornecimento de cartões de crédito de responsabilidade da instituição contratante       
*/

keep if function1 == 1 
tab bank
keep if function2 == 1 | function3 == 1 // not sure if correct 


tab bank // This data is used in a table in the paper


keep if function2 == 1 
tab bank // This data is used in a table in the paper


keep id_municipio id_municipio_bcb bank
gen correspondent = 1
collapse (sum) correspondent, by(id_municipio id_municipio_bcb bank)
tab bank // This data is used in a table in the paper
by id_municipio, sort: gen nvals = _n == 1
count if nvals // This data is used in a table in the paper
reshape wide correspondent, i(id_municipio id_municipio_bcb) j(bank) string
save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\correspondent_presence.dta", replace






********************************************************************************
* Diff and Diff graphs
********************************************************************************


* Selected Mun
********************************************************************************
use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\Base_week_muni_sem_qtd_menor3\regression_ready.dta", replace

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


line log_transactions_pix0 log_transactions_pix1 week, ///
	lc(black blue green red purple) /// 
	title("Log Transactions over Time - Selected Municipalities") ///
	legend(order(1 "Control" 2 "Treatment")) ///
	ytitle("Log Transactions") xtitle("Weeks") /// 
	xline(3189) ///
	xmlabel(3189 "Shock", angle(90))
	
graph export "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output\graphs\treat_control_selected_trans.png", as(png) name("Graph") replace





line log_transactions_difference week, ///
	lc(black blue green red purple) /// 
	title("Log Transactions: Treatment - Control") ///
	legend(order(1 "Treatment - Control")) ///
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

*All Mun
********************************************************************************
use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\Base_week_muni_sem_qtd_menor3\regression_ready_all_mun.dta", replace

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

line log_transactions_pix0 log_transactions_pix1 week, ///
	lc(black blue green red purple) /// 
	title("Log Transactions over Time - All Municipalities") ///
	legend(order(1 "Control" 2 "Treatment")) ///
	ytitle("Log Transactions") xtitle("Weeks") /// 
	xline(3189) ///
	xmlabel(3189 "Shock", angle(90))
graph export "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output\graphs\treat_control_all_trans.png", as(png) name("Graph") replace

line log_transactions_difference week, ///
	lc(black blue green red purple) /// 
	title("Log Transactions: Treatment - Control") ///
	legend(order(1 "Treatment - Control")) ///
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









