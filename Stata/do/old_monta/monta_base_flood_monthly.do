******************************************
*monta_base_flood_monthly.do

* Input: 
*   1) "$dta\natural_disasters_monthly_filled_flood.dta" // Esse é gerado pela pasta C:\Users\mathe\Dropbox\RESEARCH\Natural Disasters, and running \code\0.clean_nat_disaster.do
*   2) "$dta\municipios2.dta" // Esse é gerado pela pasta C:\Users\mathe\Dropbox\RESEARCH\Municipios\dta, codigo 0.clean_municipios.do

* Output: 
*	1) "$dta\flood_monthly_2020_2022.dta"
*	2) "$dta\flood_monthly_2019_2020.dta"

* Variables: id_municipio time_id flood muni_cd after_flood date_flood

* The goal: Esse do file cria as variaveis muni_cd after_flood date_flood
* 			So faz sentido usar esse dta se vc quer fazer um event study com essas datas Pre e Pos Pix. 
* 			Pre pix: keep if time_id >= ym(2018,1) & time_id < ym(2020,11) 
* 			Pos Pix: keep if time_id >= ym(2020,11) & time_id <= ym(2022,12)
* 			Ao fazer merge, a unica variavel que importa é o date_flood para um event study

*To do: 

******************************************
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

/*
global dta "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta"
global log "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\log"
global output "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output"

*/

capture log close
log using "$log\monta_base_flood_monthly.log", replace 

****** 
* After PIX
******

use "$dta\natural_disasters_monthly_filled_flood.dta", clear
merge m:1 id_municipio using "$dta\municipios2.dta"
tab id_municipio if _merge == 1
keep if _merge == 3
drop _merge
sort id_municipio id_municipio_bcb date
by id_municipio id_municipio_bcb date: gen dup = cond(_N==1,0,_n)
tab dup // No duplicates
drop if dup>1
keep id_municipio date number_disasters id_municipio_bcb id_regiao_imediata id_regiao_intermediaria capital_uf id_uf nome_regiao
*put name conventions!!! 
rename id_municipio_bcb muni_cd
rename date time_id
*rename number_disasters flood 
********************************************************************************
* Should be 
gen flood = 0
replace flood = 1 if number_disasters>0 
replace flood = . if missing(number_disasters)
********************************************************************************

* Limit to after Pix
keep if time_id >= ym(2020,11) & time_id <= ym(2022,12) 
sort muni_cd time_id

* Label 
cap label var muni_cd "Municipality"
cap label var time_id "Month"
cap label var flood "Flood"
cap label var id_municipio "IBGE Municipality Code"

* Create variables: date_flood, after_flood
gen after_flood = 0
bysort muni_cd : egen temp = min(time_id) if flood > 0
bysort muni_cd : egen date_flood = max(temp) 
drop temp
format %tm date_flood
replace after_flood = 1 if time_id >= date_flood

save "$dta\flood_monthly_2020_2022.dta", replace

****** 
* After PIX - 2023
******
use "$dta\natural_disasters_monthly_filled_flood.dta", clear
merge m:1 id_municipio using "$dta\municipios2.dta"
tab id_municipio if _merge == 1
keep if _merge == 3
drop _merge
sort id_municipio id_municipio_bcb date
by id_municipio id_municipio_bcb date: gen dup = cond(_N==1,0,_n)
tab dup // No duplicates
drop if dup>1
keep id_municipio date number_disasters id_municipio_bcb id_regiao_imediata id_regiao_intermediaria capital_uf id_uf nome_regiao
*put name conventions!!! 
rename id_municipio_bcb muni_cd
rename date time_id
*rename number_disasters flood 
********************************************************************************
* Should be 
gen flood = 0
replace flood = 1 if number_disasters>0 
replace flood = . if missing(number_disasters)
********************************************************************************

* Limit to after Pix
keep if time_id >= ym(2020,11) & time_id <= ym(2023,12) 
sort muni_cd time_id

* Label 
cap label var muni_cd "Municipality"
cap label var time_id "Month"
cap label var flood "Flood"
cap label var id_municipio "IBGE Municipality Code"

* Create variables: date_flood, after_flood
gen after_flood = 0
bysort muni_cd : egen temp = min(time_id) if flood > 0
bysort muni_cd : egen date_flood = max(temp) 
drop temp
format %tm date_flood
replace after_flood = 1 if time_id >= date_flood

save "$dta\flood_monthly_2020_2023.dta", replace

****** 
* After PIX - COVID
******
use "$dta\natural_disasters_monthly_filled_flood.dta", clear
merge m:1 id_municipio using "$dta\municipios2.dta"
tab id_municipio if _merge == 1
keep if _merge == 3
drop _merge
sort id_municipio id_municipio_bcb date
by id_municipio id_municipio_bcb date: gen dup = cond(_N==1,0,_n)
tab dup // No duplicates
drop if dup>1
keep id_municipio date number_disasters id_municipio_bcb id_regiao_imediata id_regiao_intermediaria capital_uf id_uf nome_regiao
*put name conventions!!! 
rename id_municipio_bcb muni_cd
rename date time_id
*rename number_disasters flood 
********************************************************************************
* Should be 
gen flood = 0
replace flood = 1 if number_disasters>0 
replace flood = . if missing(number_disasters)
********************************************************************************

* Limit to after Pix
keep if time_id >= ym(2020,11) & time_id <= ym(2021,6) 
sort muni_cd time_id

* Label 
cap label var muni_cd "Municipality"
cap label var time_id "Month"
cap label var flood "Flood"
cap label var id_municipio "IBGE Municipality Code"

* Create variables: date_flood, after_flood
gen after_flood = 0
bysort muni_cd : egen temp = min(time_id) if flood > 0
bysort muni_cd : egen date_flood = max(temp) 
drop temp
format %tm date_flood
replace after_flood = 1 if time_id >= date_flood

save "$dta\flood_monthly_202011_202106.dta", replace






****** 
* Before PIX
******


use "$dta\natural_disasters_monthly_filled_flood.dta", clear
merge m:1 id_municipio using "$dta\municipios2.dta"
tab id_municipio if _merge == 1
keep if _merge == 3
drop _merge
sort id_municipio id_municipio_bcb date
by id_municipio id_municipio_bcb date: gen dup = cond(_N==1,0,_n)
tab dup // No duplicates
drop if dup>1
keep id_municipio date number_disasters id_municipio_bcb id_regiao_imediata id_regiao_intermediaria capital_uf id_uf nome_regiao
*put name conventions!!! 
rename id_municipio_bcb muni_cd
rename date time_id
*rename number_disasters flood 
********************************************************************************
* Should be 
gen flood = 0
replace flood = 1 if number_disasters>0 
replace flood = . if missing(number_disasters)
********************************************************************************

* Limit to before Pix
keep if time_id >= ym(2019,1) & time_id < ym(2020,11) 
sort muni_cd time_id

* Label 
cap label var muni_cd "Municipality"
cap label var time_id "Month"
cap label var flood "Flood"
cap label var id_municipio "IBGE Municipality Code"

* Create variables: date_flood, after_flood
gen after_flood = 0
bysort muni_cd : egen temp = min(time_id) if flood > 0
bysort muni_cd : egen date_flood = max(temp) 
drop temp
format %tm date_flood
replace after_flood = 1 if time_id >= date_flood

save "$dta\flood_monthly_2019_2020.dta", replace


use "$dta\natural_disasters_monthly_filled_flood.dta", clear
merge m:1 id_municipio using "$dta\municipios2.dta"
tab id_municipio if _merge == 1
keep if _merge == 3
drop _merge
sort id_municipio id_municipio_bcb date
by id_municipio id_municipio_bcb date: gen dup = cond(_N==1,0,_n)
tab dup // No duplicates
drop if dup>1
keep id_municipio date number_disasters id_municipio_bcb id_regiao_imediata id_regiao_intermediaria capital_uf id_uf nome_regiao
*put name conventions!!! 
rename id_municipio_bcb muni_cd
rename date time_id
*rename number_disasters flood 
********************************************************************************
* Should be 
gen flood = 0
replace flood = 1 if number_disasters>0 
replace flood = . if missing(number_disasters)
********************************************************************************

* Limit to before Pix
keep if time_id >= ym(2018,1) & time_id < ym(2020,11) 
sort muni_cd time_id

* Label 
cap label var muni_cd "Municipality"
cap label var time_id "Month"
cap label var flood "Flood"
cap label var id_municipio "IBGE Municipality Code"

* Create variables: date_flood, after_flood
gen after_flood = 0
bysort muni_cd : egen temp = min(time_id) if flood > 0
bysort muni_cd : egen date_flood = max(temp) 
drop temp
format %tm date_flood
replace after_flood = 1 if time_id >= date_flood

save "$dta\flood_monthly_2018_2020.dta", replace



****** 
* Before PIX - COVID
******

use "$dta\natural_disasters_monthly_filled_flood.dta", clear
merge m:1 id_municipio using "$dta\municipios2.dta"
tab id_municipio if _merge == 1
keep if _merge == 3
drop _merge
sort id_municipio id_municipio_bcb date
by id_municipio id_municipio_bcb date: gen dup = cond(_N==1,0,_n)
tab dup // No duplicates
drop if dup>1
keep id_municipio date number_disasters id_municipio_bcb id_regiao_imediata id_regiao_intermediaria capital_uf id_uf nome_regiao
*put name conventions!!! 
rename id_municipio_bcb muni_cd
rename date time_id
*rename number_disasters flood 
********************************************************************************
* Should be 
gen flood = 0
replace flood = 1 if number_disasters>0 
replace flood = . if missing(number_disasters)
********************************************************************************


* Limit to before Pix
keep if time_id >= ym(2020,3) & time_id < ym(2020,11) 
sort muni_cd time_id

* Label 
cap label var muni_cd "Municipality"
cap label var time_id "Month"
cap label var flood "Flood"
cap label var id_municipio "IBGE Municipality Code"

* Create variables: date_flood, after_flood
gen after_flood = 0
bysort muni_cd : egen temp = min(time_id) if flood > 0
bysort muni_cd : egen date_flood = max(temp) 
drop temp
format %tm date_flood
replace after_flood = 1 if time_id >= date_flood

save "$dta\flood_monthly_202003_202010.dta", replace





use "$dta\flood_monthly_2018_2020.dta", replace
keep muni_cd id_regiao_imediata id_regiao_intermediaria capital_uf id_uf nome_regiao
sort muni_cd id_regiao_imediata id_regiao_intermediaria capital_uf id_uf nome_regiao
by muni_cd id_regiao_imediata id_regiao_intermediaria capital_uf id_uf nome_regiao: gen dup = cond(_N==1,0,_n)
tab dup // No duplicates
drop if dup>1
drop dup
save "$dta\regions_brazil.dta", replace

log close