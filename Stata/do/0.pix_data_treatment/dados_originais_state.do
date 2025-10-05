* Esse codigo trabalha com dados individuais de PIX de cada estado
* Nesse caso so temos o do Ceara
* Input: "$origdata\PIX_MUNI_`state'_`year'`m'_`i'_`j'.csv"
* Output: "$dta\dados_originais_`state'.dta"

* Depois eu faço um tratamento nos dados originais. 
*





clear all
set more off, permanently 
set matsize 2000
set emptycells drop


global log "D:\PIX_Matheus\Stata\log"
global dta "D:\PIX_Matheus\Stata\dta"
global output "D:\PIX_Matheus\Output"
global origdata "D:\PIX_Matheus\DadosOriginais"

* ADO 
adopath ++ "D:\ADO"

foreach state in CE {
    capture log close
    log using "$log\dados_originais_`state'.log", replace 
	* i is payer, j is receiver
	* 1 is individual, 2 is firms
	forvalues i = 1/2{
		forvalues j=1/2{
			forvalues year = 2020/2021 {
				local meses  01 02 03 04 05 06 07 08 09 10 11 12
				foreach m of local meses {
					if  ~(`year '==2022 & `m'>=7) & ~(`year'==2020 & `m'<=10) {
						dis `year' `m'
						import delimited "$origdata\PIX_MUNI_`state'_`year'`m'_`i'_`j'.csv", clear
						if  ~(`year'==2020 & `m'== 11 & `i'==1 & `j'== 1) {
								append using "$dta\dados_originais_`state'.dta"
						}	
						save "$dta\dados_originais_`state'.dta",replace 
					}
				}
			}
		}
		}
	*change date
	rename laf_dt_liquidacao dia
	replace dia = subinstr(dia,"-","",.)
	gen dia_stata = daily(dia,"YMD")
	format %td dia_stata

	gen week = wofd(dia_stata)
	format %tw week

	save "$dta\dados_originais_`state'.dta",replace
	*****Random sample
	sample 5
	save "$dta\dados_originais_`state'_sample.dta",replace
	
	
	
	
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

******************************************************************************














 

**** NEED POPULATION DATA

*** Unique Pix receivers % of population !
use "$dta\dados_originais_CE_first_pix_received.dta",replace

bysort mun_rec (week): gen total_unique_pix=sum(first_pix_received)

gen pix_percentage = total_unique_pix/population
gen pop_without_pix = population-total_unique_pix
gen adoption = first_pix_received/pop_without_pix[_n-1] ///

*save "$dta\dados_originais_CE_first_pix_received.dta",replace







*** Pix de Fora para dentro
use "$dta\dados_originais_CE_fora_dentro.dta",replace


* Percentage of the population

*Percentage of the population that already had Pix. 



*** First Pix received as a % of the population remaining without pix !
use "$dta\dados_originais_CE.dta",replace







******************************************
******
** I need to fill in all the 0s that are not counted inside the collapse

******************************************


