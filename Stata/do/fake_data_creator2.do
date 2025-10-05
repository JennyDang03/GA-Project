* This is the fake data creator
* I hope to use this code to alter data from the Central Bank to be allowed to work in my computer




*do we have data on owner of businesses?
* CSV do not work on Central Bank computer. Need to transform Dados_PF in dta first. 


clear all
set more off, permanently 
*set matsize 2000
set emptycells drop


*global log "D:\PIX_Matheus\Stata\log"
*global dta "D:\PIX_Matheus\Stata\dta"
*global dta_sample "D:\PIX_Matheus\Stata\dta_sample"
*global output "D:\PIX_Matheus\Output"
*global origdata "D:\PIX_Matheus\DadosOriginais"

global log "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\log"
global dta "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta"
global dta_sample "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta_sample"
global output "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output"
global origdata "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\DadosOriginais"



* ADO 
*adopath ++ "D:\ADO"

* -----------------------------------------------
* Fake pix transactions

use "$dta\pix\dados_originais_CE.dta", clear

keep pes_nu_cpf_cnpj_pagador pes_nu_cpf_cnpj_recebedor valor tipo_rec tipo_pag dia_stata 
rename pes_nu_cpf_cnpj_pagador id_sender
rename pes_nu_cpf_cnpj_recebedor id_receiver
rename valor value
rename tipo_rec type_receiver
rename tipo_pag type_sender
rename dia_stata date

gen week = wofd(date)
format %tw week
drop date

order id_sender type_sender id_receiver type_receiver value week

sample 0.01
* create random value
gen value_rand = floor((((1 + (uniform() - 0.5)/5) * value)*100))/100

drop value
rename value_rand value

save "$dta_sample\pix\dados_originais_CE_sample.dta", replace

*---------------------------------------
* Create altitude data
* it is missing the address data

use "$dta_sample\pix\dados_originais_CE_sample.dta", replace
keep id_sender
rename id_sender id
save "$dta_sample\receita_federal\temp_id_sample1.dta", replace

use "$dta_sample\pix\dados_originais_CE_sample.dta", replace
keep id_receiver
rename id_receiver id
save "$dta_sample\receita_federal\temp_id_sample2.dta", replace

use "$dta_sample\receita_federal\temp_id_sample2.dta", replace
append using "$dta_sample\receita_federal\temp_id_sample1.dta"
sort id
quietly by id: gen dup = cond(_N==1,0,_n)
drop if dup >1
drop dup
save "$dta_sample\receita_federal\id_sample.dta", replace

use "$dta_sample\ibge\ibge_bcb.dta", replace
gen rand = runiform()
sort rand
gen rand_id = _n
save "$dta_sample\ibge\temp_ibge_bcb_rand_id.dta", replace
*5534

use "$dta_sample\receita_federal\id_sample.dta", replace
gen rand = runiform()
sort rand 
gen rand_id1 = _n
gen rand_id = mod(rand_id1,5534)+1
merge m:1 rand_id using "$dta_sample\ibge\temp_ibge_bcb_rand_id.dta"
drop rand rand_id rand_id1 _merge

gen rand = runiform()
sort rand 
gen rand_id1 = _n
gen rand_id = mod(rand_id1,3)+1
gen low_altitude = 0
gen middle_altitude = 0
gen high_altitude = 0
replace low_altitude = 1 if rand_id == 1
replace middle_altitude = 1 if rand_id == 2
replace high_altitude = 1 if rand_id == 3
drop rand rand_id rand_id1

save "$dta_sample\receita_federal\id_altitude_sample.dta", replace
* it is missing the address data
*--------------------------------------------------
* Address data
use "$dta_sample\receita_federal\id_altitude_sample.dta", replace
* I have a 10k address sample on my computer. merge then do households. 
* I need type to make household. 


save "$dta_sample\receita_federal\id_address_sample.dta", replace
*--------------------------------------------------
*database on type


use "$dta_sample\pix\dados_originais_CE_sample.dta", replace
keep id_sender type_sender
rename id_sender id
rename type_sender type
save "$dta_sample\receita_federal\temp_id_sample1.dta", replace

use "$dta_sample\pix\dados_originais_CE_sample.dta", replace
keep id_receiver type_receiver
rename id_receiver id
rename type_receiver type
save "$dta_sample\receita_federal\temp_id_sample2.dta", replace

use "$dta_sample\receita_federal\temp_id_sample2.dta", replace
append using "$dta_sample\receita_federal\temp_id_sample1.dta"
sort id
quietly by id: gen dup = cond(_N==1,0,_n)
drop if dup >1
drop dup
save "$dta_sample\receita_federal\id_type_sample.dta", replace


*--------------------------------------------------

* Merge altitue and pix
use "$dta_sample\receita_federal\id_altitude_sample.dta", replace
foreach var in *{
    rename `var' `var'_sender
}
save "$dta_sample\receita_federal\temp_id_altitude_sample_sender.dta", replace

use "$dta_sample\receita_federal\id_altitude_sample.dta", replace
foreach var in *{
    rename `var' `var'_receiver
}
save "$dta_sample\receita_federal\temp_id_altitude_sample_receiver.dta", replace

use "$dta_sample\pix\dados_originais_CE_sample.dta", replace
merge m:1 id_sender using "$dta_sample\receita_federal\temp_id_altitude_sample_sender.dta"
keep if _merge == 3
drop _merge
merge m:1 id_receiver using "$dta_sample\receita_federal\temp_id_altitude_sample_receiver.dta"
keep if _merge == 3
drop _merge

save "$dta_sample\pix\dados_originais_CE_with_altitude_sample.dta", replace

* -------------------------------------------------------
* create TED and Boleto similar to pix

use "$dta_sample\pix\dados_originais_CE_with_altitude_sample.dta", replace
gen rand = runiform()
sort rand 
drop if rand < 0.5
save "$dta_sample\ted\dados_originais_CE_with_altitude_sample.dta", replace
use "$dta_sample\pix\dados_originais_CE_with_altitude_sample.dta", replace
gen rand = runiform()
sort rand 
drop if rand > 0.5
save "$dta_sample\boleto\dados_originais_CE_with_altitude_sample.dta", replace

*----------------------------------------
* Create data from the Receita with date of birth, gender, date of death. 
use "$dta_sample\receita_federal\id_sample.dta", replace
gen rand = runiform()
gen male = 1 if rand > 0.5
replace male = 0 if male ==.
sort rand
gen rand_id = _n
gen rand_id1 = mod(rand_id,60)
gen birth_year = 2010-rand_id1
drop rand rand_id rand_id1
gen rand = runiform()
sort rand
gen rand_id = _n
gen birth_month = mod(rand_id,12)+1
drop rand rand_id 
gen rand = runiform()
sort rand
gen rand_id = _n
gen birth_day = mod(rand_id,28)+1
drop rand rand_id 
gen death_year = .
save "$dta_sample\receita_federal\id_birth_gender_sample.dta", replace

* -----------------------------------------------
* create data with who got auxilio or bolsa familia or nothing. 
use "$dta_sample\receita_federal\id_sample.dta", replace
gen rand = runiform()
sort rand
gen rand_id = _n
gen rand_id1 = mod(rand_id,3)+1
gen no_assistance = 0
gen covid_relief = 0
gen bolsa_familia = 0
replace no_assistance = 1 if rand_id1 == 1
replace covid_relief = 1 if rand_id1 == 2
replace bolsa_familia = 1 if rand_id1 == 3
drop rand rand_id rand_id1
save "$dta_sample\receita_federal\id_covid_relief_sample.dta", replace


**** NEEDS TO BE PEOPLE
*use "$dta_sample\receita_federal\id_type_sample.dta", replace
*keep if type == 1


* -----------------------------------------------
* database with opening and closing of bank accounts 
*(id, if, date_opening, date_closing)
use "$dta_sample\receita_federal\id_sample.dta", replace
gen rand = runiform()
sort rand
gen rand_id = _n
gen rand_id1 = mod(rand_id,3)+1
keep if rand_id1 == 1
drop rand rand_id rand_id1 
save "$dta_sample\receita_federal\temp_id_sample_1.dta", replace
use "$dta_sample\receita_federal\id_sample.dta", replace
gen rand = runiform()
sort rand
gen rand_id = _n
gen rand_id1 = mod(rand_id,3)+1
keep if rand_id1 == 2
drop rand rand_id rand_id1 
save "$dta_sample\receita_federal\temp_id_sample_2.dta", replace
use "$dta_sample\receita_federal\id_sample.dta", replace
gen rand = runiform()
sort rand
gen rand_id = _n
gen rand_id1 = mod(rand_id,3)+1
keep if rand_id1 == 3
drop rand rand_id rand_id1 
save "$dta_sample\receita_federal\temp_id_sample_3.dta", replace

use "$dta_sample\receita_federal\temp_id_sample_1.dta", replace
append using "$dta_sample\receita_federal\temp_id_sample_2.dta"
append using "$dta_sample\receita_federal\temp_id_sample_2.dta"
append using "$dta_sample\receita_federal\temp_id_sample_3.dta"
append using "$dta_sample\receita_federal\temp_id_sample_3.dta"
append using "$dta_sample\receita_federal\temp_id_sample_3.dta"

gen rand = runiform()
sort rand
gen rand_id = _n
gen bank_id = mod(rand_id,10)+1
drop rand rand_id


gen rand = runiform()
sort rand
gen rand_id = _n
gen week_opening = 3223 - mod(rand_id,500) 
format %tw week_opening
drop rand rand_id

gen week_closing = .
gen rand = runiform()
sort rand
gen rand_id = _n
replace week_closing = week_opening + 50 if mod(rand_id,50) == 0
drop rand rand_id
format %tw week_closing
save "$dta_sample\ccs - bank opening\bank_opening.dta", replace

* -----------------------------------------------
* Base uso de credito

* (id, month, bank_id, limite_credito_pessoal, limite_capital_de_giro, juros_credito_pessoal, juros_capital_de_giro,  default_30_days, new_loan, amount_owning_credito_pessoal, amount_owning, comprometimento de renda?)
* rating,
* (id, income, rating, amount_owning,)
* credit_card_use and expansion of limits, 
* endereco - estao tentando geo localizacao



* -----------------------------------------------

* Base do value of transactions in a day at a merchant. Not debit, right?
*(id, value, day)

use "$dta_sample\receita_federal\id_type_sample.dta", replace
keep if type == 2

* Need to fill for each date
gen week = 3223 - 500
append using "$dta_sample\receita_federal\id_type_sample.dta"
keep if type == 2
keep if id != .
replace week = 3223 if week == .
drop type

tsset id week
tsfill

* need to create transaction values
gen rand = runiform()
replace rand = 0.01 if rand < 0.01
gen value = floor(10000/rand)/100
drop rand

format %tw week

save "$dta_sample\ccs - credit card transactions\id_values_day.dta", replace































* -----------------------------------------------
* Rais, see how they clean RAIS


* -----------------------------------------------
* ESTBAN, desastres, dados municipais. I have those

* No need
* -----------------------------------------------

	* Create collapses for individuals, week
use "$dta_sample\pix\dados_originais_CE_sample.dta", replace


***** Do Total transactions -> Then separate (inflo, outflow, intra)*(high, normal, low altitude)*(type 1 -people-, type 2 business)
*id_sender
rename id_sender id
drop if id == id_receiver

gen transactions = 1 if id !=0
collapse (sum) value transactions, by(week id)
*Create first time sender 
sort id	week
by id: gen first_time_sent = 1 if _n == 1
replace first_time_sent = 0 if first_time_sent == .
rename transactions transactions_sent
rename value value_sent 

* fill with zeros
tsset id week
tsfill, full
foreach var in value_sent transactions_sent first_time_sent{
	replace `var' = 0 if `var' == .
}

* save
save "$dta_sample\pix\temp_dados_originais_CE_sender_sample.dta", replace

* do it again with receiver
use "$dta_sample\pix\dados_originais_CE_sample.dta", replace
rename id_receiver id
drop if id == id_sender

gen transactions = 1 if id !=0
collapse (sum) value transactions, by(week id)
*Create first time receiver 
sort id	week
by id: gen first_time_received = 1 if _n == 1
replace first_time_received = 0 if first_time_received == .
rename transactions transactions_received
rename value value_received 

* fill with zeros
tsset id week
tsfill, full
foreach var in value_received transactions_received first_time_received{
	replace `var' = 0 if `var' == .
}

* save
save "$dta_sample\pix\temp_dados_originais_CE_receiver_sample.dta", replace


* merge
use "$dta_sample\pix\temp_dados_originais_CE_sender_sample.dta", replace
merge 1:1 id week using "$dta_sample\pix\temp_dados_originais_CE_receiver_sample.dta"

foreach var in value_sent transactions_sent first_time_sent value_received transactions_received first_time_received{
	replace `var' = 0 if `var' == .
}
drop _merge
*save
save "$dta_sample\pix\dados_originais_CE_id_collapse_sample.dta", replace


***** Do Total transactions -> Then separate (inflow, outflow, intra)*(high, normal, low altitude)*(type 1 -people-, type 2 business)

		
*------------------------------------------------------------------
	
	* Create collapses for municipalities, week
	
*------------------------------------------------------------------

use "$dta_sample\boleto\dados_originais_CE_with_altitude_sample.dta", replace
drop rand
***** Do Total transactions -> Then separate (inflow, outflow, intra)*(type 1 -people-, type 2 business)* (sender altitude dummy

drop if id_sender == id_receiver
gen transactions = 1

sort id_receiver week
by id_receiver: gen first_time_received = 1 if _n == 1
replace first_time_received = 0 if first_time_received == .
sort id_sender week
by id_sender: gen first_time_sent = 1 if _n == 1
replace first_time_sent = 0 if first_time_received == .


*focus on a collapse that can be done after the collapse for individuals. 

* Put dummy for intra, outside the municipality
*do it twice, one collapsing mun_receiver, another mun_sender

collapse (sum) value  transactions first_time_received first_time_sent, by(week type_sender type_receiver id_municipio_sender id_municipio_receiver low_altitude_sender middle_altitude_sender high_altitude_sender low_altitude_receiver middle_altitude_receiver high_altitude_receiver)
	
	
	
	
	
	* pix pra fora, pix pra dentro, pix intra municipal
	
	
	
	
	* pix pra si mesmo
	
 	
* -------------------------------------------------


















	
	* Filter for the State
	use  "$dta\ibge_bcb.dta", replace
	keep if sigla_uf=="`state'"
	save  "$dta\ibge_bcb_`state'.dta", replace
	
	*** Pix de Dentro para Fora
	use "$dta\dados_originais_`state'.dta",replace
	gen qtd = 1 
	drop if  mun_pag == mun_rec
	collapse (sum) valor qtd, by(week mun_pag)
	**************
	*filling up with 0s
	drop if mun_pag == -3
	xtset mun_pag week
	tsfill, full
	replace valor=0 if valor == . 
	replace qtd=0 if qtd == . 
	rename mun_pag id_municipio_bcb
	**************
	**************
	*excluding transfers that the payers are not from the state
	 merge m:1 id_municipio_bcb using "$dta\ibge_bcb_`state'.dta", keep(3) nogen
	drop sigla_uf
	**************
	save "$dta\dados_originais_`state'_dentro_fora.dta",replace
	
	*** Pix de Dentro para Dentro 
	use "$dta\dados_originais_`state'.dta",replace
	gen qtd = 1 
	keep if  mun_pag == mun_rec
	collapse (sum) valor qtd, by(week mun_pag)
	**************
	*filling up with 0s
	drop if mun_pag == -3
	xtset mun_pag week
	tsfill, full
	replace valor=0 if valor == . 
	replace qtd=0 if qtd == . 
	rename mun_pag id_municipio_bcb
	**************
	**************
	*excluding transfers that the receivers are not from the state
	merge m:1 id_municipio_bcb using "$dta\ibge_bcb_`state'.dta", keep(3) nogen
	drop sigla_uf
	**************
	save "$dta\dados_originais_`state'_dentro_dentro.dta",replace

	
	
	*** Pix de Fora para dentro
	use "$dta\dados_originais_`state'.dta",replace
	gen qtd = 1 
	drop if  mun_pag == mun_rec
	collapse (sum) valor qtd, by(week mun_rec)
	**************
	*filling up with 0s
	drop if mun_rec == -3
	xtset mun_rec week
	tsfill, full
	replace valor=0 if valor == . 
	replace qtd=0 if qtd == . 
	rename mun_rec id_municipio_bcb
	**************
	**************
	*excluding transfers that are not from CE
	merge m:1 id_municipio_bcb using "$dta\ibge_bcb_`state'.dta", keep(3) nogen
	drop sigla_uf
	**************
	* Percentage of the population
	*Percentage of the population that already had Pix. 
	save "$dta\dados_originais_`state'_fora_dentro.dta",replace

	*** Primeiro Pix recebido!
	use "$dta\dados_originais_`state'.dta",replace
	sort pes_nu_cpf_cnpj_recebedor dia_stata
	by pes_nu_cpf_cnpj_recebedor: gen first_pix_received=_n
	keep if first_pix_received==1

	collapse (sum) first_pix_received valor, by(week mun_rec)
	sort mun_rec week
	**************
	*filling up with 0s
	drop if mun_rec == -3
	xtset mun_rec week
	tsfill, full
	replace valor=0 if valor == . 
	replace first_pix_received=0 if first_pix_received == . 
	rename mun_rec id_municipio_bcb
	**************
	**************
	*excluding transfers that the receivers are not from the state
	merge m:1 id_municipio_bcb using "$dta\ibge_bcb_`state'.dta", keep(3) nogen
	drop sigla_uf
	**************
	save "$dta\dados_originais_`state'_first_pix_received.dta",replace

	
	
	
	*** Primeiro Pix mandado!
	use "$dta\dados_originais_`state'.dta",replace

	sort pes_nu_cpf_cnpj_pagador dia_stata
	by pes_nu_cpf_cnpj_pagador: gen first_pix_paid=_n
	keep if first_pix_paid==1

	collapse (sum) first_pix_paid valor, by(week mun_pag)
	sort mun_pag week
	**************
	*filling up with 0s
	drop if mun_pag == -3
	xtset mun_pag week
	tsfill, full
	replace valor=0 if valor == . 
	replace first_pix_paid=0 if first_pix_paid == . 
	rename mun_pag id_municipio_bcb
	**************
	**************
	*excluding transfers that are not from CE
	merge m:1 id_municipio_bcb using "$dta\ibge_bcb_`state'.dta", keep(3) nogen
	drop sigla_uf
	**************
	save "$dta\dados_originais_`state'_first_pix_paid.dta",replace


	log close
}