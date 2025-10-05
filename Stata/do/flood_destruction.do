*flood_destruction


clear all
set more off, permanently 

set emptycells drop

global path "\\sbcdf176\PIX_Matheus$\"
global path "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\"


global log "$path\log"
global dta "$path\Stata\dta"
global output "$path\Output"
global origdata "$path\DadosOriginais"

* ADO 
adopath ++ "D:\ADO"
adopath ++ "//sbcdf060/depep01$/ADO"
adopath ++ "//sbcdf060/depep01$/ado-776e"


********************************************************
use "$dta\natural_disasters_monthly_filled_flood.dta", replace 
gen ano = year(dofm(date))
gen mes = month(dofm(date))
collapse (sum) number_disasters, by(id_municipio ano)
gen flood = 0
replace flood = 1 if number_disasters>0 
replace flood = . if missing(number_disasters)
save "$dta\natural_disasters_yearly_filled_flood.dta", replace 

use "$dta\natural_disasters_yearly_filled_flood.dta", replace 
gen after_flood = 0
bysort id_municipio : egen temp = min(ano) if flood > 0
bysort id_municipio : egen date_flood = max(temp) 
drop temp
format %tm date_flood
replace after_flood = 1 if ano >= date_flood
save "$dta\annualflood_2013_2022.dta", replace 


********************************************************
use "$dta\Importa_pib_mun.dta", replace 
destring id_municipio, replace
merge 1:1 id_municipio ano using "$dta\natural_disasters_yearly_filled_flood.dta"
keep if _merge == 3
drop _merge

merge m:1 id_municipio using "$dta\municipios2.dta"
tab id_municipio if _merge == 1
keep if _merge == 3
drop _merge

keep id_municipio ano pib impostos_liquidos va va_agropecuaria va_industria va_servicos va_adespss number_disasters flood id_municipio_bcb
rename id_municipio_bcb muni_cd

* Create variables: date_flood, after_flood
gen after_flood = 0
bysort muni_cd : egen temp = min(ano) if flood > 0
bysort muni_cd : egen date_flood = max(temp) 
drop temp
format %tm date_flood
replace after_flood = 1 if ano >= date_flood


save "$dta\flood_pib_mun.dta", replace 

