
*****
* Pix_PF_adoption_dist_sample
*****
* Esse codigo importa dados ja limpos de PIX e junta com dados do endereço e distancias 
* Possui somente de quem:
*     -  teve aux emergencial em abril 21 e 
*     -  está nos 5000 menores municipios 
*	  -  está no sample de 700k endereços aleatoriamente selecionados
* Os dados de input são gerados pelo Pix_PF_adoption.do
* Input:  "$dta\Pix_PF_adoption.dta"
* Output: "$dta\Pix_PF_adoption_dist_sample.dta"

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
log using "$log\Pix_PF_adoption_dist_sample.log", replace 

* Load Data
use "$dta\Pix_PF_adoption.dta", replace 

* Merge with address data. 
merge m:1 index using "$dta\dist_caixa\dist_caixa_multiprocess7.dta"
* Only random addresses
keep if _merge == 3
drop _merge

* Create Pix Adoption - Dummy for first time that individual learned to do both
gen after_adoption = 1 if after_first_pix_rec == 1 & after_first_pix_sent == 1

bysort id : egen temp = min(time_id) if after_adoption > 0
bysort id : egen date_adoption = max(temp) 
drop temp

format %tmNN/CCYY date_adoption

*Add confidence level - Maybe I will add it in R.
merge m:1 index using "$dta\aux_address\aux_address_partial_results7_super_cleaned.dta", keepusing(confidence) 
keep if _merge == 3
drop _merge

* I can add after_event but it is added on R.

save "$dta\Pix_PF_adoption_dist_sample.dta", replace 

log close