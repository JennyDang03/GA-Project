*****************************************
*monta_base_pix_self_individuo.do
* Input: 
*   1) "$dta\Pix_individuo_PF_self.dta"
*   2) "$dta\Pix_individuo_PJ_self.dta"
*	3) "$dta\flood_monthly_2020_2022.dta"

* Output:
*   1) "$dta\Pix_individuo_PF_self_cleaned.dta"
*   2) "$dta\Pix_individuo_PJ_self_cleaned.dta"
*	3) "$dta\Pix_individuo_PF_self_cleaned_sample1.dta"
*	4) "$dta\Pix_individuo_PJ_self_cleaned_sample1.dta"

* Variables: id muni_cd time_id value_self trans_self after_first_pix_self user date_flood

* The goal: To clean, fill, and add date_flood and new treated variables to Pix_individuo_PF_self.dta and Pix_individuo_PJ_self.dta, and prepare it for flood_SA_individual_self_v1.R

* To do: The sample individual is out of a sample of people that eventually use pix. 
*			Needs to add the people that never use pix. I think Jose was working on that
*			The ideal is to gather every CPF in the country, take a 1% sample 
*			and get all pix from the selected 1%

*****************************************

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
log using "$log\monta_base_pix_self_individuo.log", replace 


forval i = 1/2{
	if `i' == 1 {
		use "$dta\Pix_individuo_PF_self.dta", replace 
	}
	else if `i' == 2 {
		use "$dta\Pix_individuo_PJ_self.dta", replace
	}
	
	* For tipo_pessoa1 Number of unique values of id is  19793415
	* For tipo_pessoa1 Number of records is  190277792
	drop tipo_pessoa
	* Variables: id value_self trans_self muni_cd time_id
	
	
	* fix time_id
	replace time_id = ym(floor(time_id/100),mod(time_id,100))
	format time_id %tm
	sort id time_id
	
	* Elimina quem tem registros em diferentes municipios no mesmo mes. 
	duplicates tag id time_id, gen(dup)
	tab dup
	drop if dup == 1
	drop dup
	* Nao tem duplicates quando tipo_pessoa == 1

	* Elimina quem muda de municipio - > Apenas 87 em 38 milhoes 
	sort id time_id
	bysort id: gen first_muni_cd = muni_cd[1]
	bysort id: gen change_muni = sum(muni_cd != first_muni_cd)
	bysort id : egen temp = max(change_muni)
	keep if temp == 0
	drop first_muni_cd change_muni temp
	* Nao tem duplicates quando tipo_pessoa == 1
	
	* Cria painel balanceado
	sort id muni_cd time_id
	tsset id time_id
	tsfill, full
	
	replace trans_self  = 0 if trans_self  == .
	replace value_self  = 0 if value_self  == .
	
	sort id muni_cd time_id // make all the muni_cd that are missing to go up. 
	bysort id (muni_cd): replace muni_cd = muni_cd[_n-1] if missing(muni_cd)
	sort id muni_cd time_id
	
	*Create some new variables:

		* after_first_pix_self, date_first_pix_self
			* Identifying time of first pix of the person
			* first pix has the date of the first pix o the that person
	bysort id : egen temp = min(time_id) if trans_self > 0
	bysort id : egen date_first_pix_self = max(temp) 
	drop temp 

			* This flags if the date time_id is before or after the first pix transaction
	gen after_first_pix_self = time_id >= date_first_pix_self
	drop date_first_pix_self 
	
	gen user = 0
	replace user = 1 if trans_self > 0
	
	*merge with flood
	merge m:1 muni_cd time_id using "$dta\flood_monthly_2020_2022.dta", keep(3) keepusing(date_flood)
	* Add only date_flood variable to save on memory
	cap drop _merge
	
	* Limit to after Pix
	keep if time_id >= ym(2020,11) & time_id <= ym(2022,12) 
	
	*save
	if `i' == 1 {
		save "$dta\Pix_individuo_PF_self_cleaned.dta", replace 
	}
	else if `i' == 2 {
		save "$dta\Pix_individuo_PJ_self_cleaned.dta", replace
	}
	
	tempfile holding
	save `holding'

	keep id
	duplicates drop

	set seed 1234
	sample 1

	merge 1:m id using `holding', assert(match using) keep(match) nogenerate
	
	sort id time_id
	if `i' == 1 {
		save "$dta\Pix_individuo_PF_self_cleaned_sample1.dta", replace 
	}
	else if `i' == 2 {
		save "$dta\Pix_individuo_PJ_self_cleaned_sample1.dta", replace
	}
}

*Run in R



log close