******************************************
*monta_base_flood_weekly.do

* Input: 
*   1) "$dta\natural_disasters_weekly_filled_flood.dta" // Esse é gerado pela pasta C:\Users\mathe\Dropbox\RESEARCH\Natural Disasters, and running \code\0.clean_nat_disaster.do
*   2) "$dta\municipios2.dta" // Esse é gerado pela pasta C:\Users\mathe\Dropbox\RESEARCH\Municipios\dta, codigo 0.clean_municipios.do

* Output: 
*	1) "$dta\flood_weekly_2020_2022.dta" 
*	2) "$dta\flood_weekly_2019_2020.dta"
*	3) "$dta\flood_weekly_2018_2020.dta"

* Variables: id_municipio time_id flood muni_cd after_flood date_flood

* The goal: Esse do file cria as variaveis muni_cd after_flood date_flood
* 			So faz sentido usar esse dta se vc quer fazer um event study com essas datas Pre e Pos Pix. 
* 			Pre pix: keep if keep if week >= wofd(mdy(11, 16, 2020)) & week <= wofd(mdy(12, 31, 2022)) 
* 			Pos Pix: keep if week >= wofd(mdy(1, 1, 2018)) & week < wofd(mdy(11, 16, 2020))
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

use "$dta\natural_disasters_weekly_filled_flood.dta", clear
merge m:1 id_municipio using "$dta\municipios2.dta"
tab id_municipio if _merge == 1
keep if _merge == 3
drop _merge
sort id_municipio id_municipio_bcb week
by id_municipio id_municipio_bcb week: gen dup = cond(_N==1,0,_n)
tab dup // No duplicates
drop if dup>1
keep id_municipio week number_disasters id_municipio_bcb id_regiao_imediata id_regiao_intermediaria capital_uf id_uf nome_regiao
*put name conventions!!! 
rename id_municipio_bcb muni_cd
*rename number_disasters flood 
********************************************************************************
* Should be 
gen flood = 0
replace flood = 1 if number_disasters>0 
replace flood = . if missing(number_disasters)
********************************************************************************

* Limit to after Pix
keep if week >= wofd(mdy(11, 16, 2020)) & week <= wofd(mdy(12, 31, 2022))
sort muni_cd week

* Create variables: date_flood, after_flood
gen after_flood = 0
bysort muni_cd : egen temp = min(week) if flood > 0
bysort muni_cd : egen date_flood = max(temp) 
drop temp
format %tw date_flood
replace after_flood = 1 if week >= date_flood

save "$dta\flood_weekly_2020_2022.dta", replace


use "$dta\natural_disasters_weekly_filled_flood.dta", clear
merge m:1 id_municipio using "$dta\municipios2.dta"
tab id_municipio if _merge == 1
keep if _merge == 3
drop _merge
sort id_municipio id_municipio_bcb week
by id_municipio id_municipio_bcb week: gen dup = cond(_N==1,0,_n)
tab dup // No duplicates
drop if dup>1
keep id_municipio week number_disasters id_municipio_bcb id_regiao_imediata id_regiao_intermediaria capital_uf id_uf nome_regiao
*put name conventions!!! 
rename id_municipio_bcb muni_cd
*rename number_disasters flood 
********************************************************************************
* Should be 
gen flood = 0
replace flood = 1 if number_disasters>0 
replace flood = . if missing(number_disasters)
********************************************************************************

* Limit to after Pix
keep if week >= wofd(mdy(11, 16, 2020)) & week <= wofd(mdy(12, 31, 2023))
sort muni_cd week

* Create variables: date_flood, after_flood
gen after_flood = 0
bysort muni_cd : egen temp = min(week) if flood > 0
bysort muni_cd : egen date_flood = max(temp) 
drop temp
format %tw date_flood
replace after_flood = 1 if week >= date_flood

save "$dta\flood_weekly_2020_2023.dta", replace


****** 
* After PIX - COVID
******

use "$dta\natural_disasters_weekly_filled_flood.dta", clear
merge m:1 id_municipio using "$dta\municipios2.dta"
tab id_municipio if _merge == 1
keep if _merge == 3
drop _merge
sort id_municipio id_municipio_bcb week
by id_municipio id_municipio_bcb week: gen dup = cond(_N==1,0,_n)
tab dup // No duplicates
drop if dup>1
keep id_municipio week number_disasters id_municipio_bcb id_regiao_imediata id_regiao_intermediaria capital_uf id_uf nome_regiao
*put name conventions!!! 
rename id_municipio_bcb muni_cd
*rename number_disasters flood 
********************************************************************************
* Should be 
gen flood = 0
replace flood = 1 if number_disasters>0 
replace flood = . if missing(number_disasters)
********************************************************************************

* Limit to after Pix
keep if week >= wofd(mdy(11, 16, 2020)) & week < wofd(mdy(07, 31, 2021)) // 37 weeks
sort muni_cd week

* Create variables: date_flood, after_flood
gen after_flood = 0
bysort muni_cd : egen temp = min(week) if flood > 0
bysort muni_cd : egen date_flood = max(temp) 
drop temp
format %tw date_flood
replace after_flood = 1 if week >= date_flood

save "$dta\flood_weekly_202011_202106.dta", replace




****** 
* Before PIX
******

use "$dta\natural_disasters_weekly_filled_flood.dta", clear
merge m:1 id_municipio using "$dta\municipios2.dta"
tab id_municipio if _merge == 1
keep if _merge == 3
drop _merge
sort id_municipio id_municipio_bcb week
by id_municipio id_municipio_bcb week: gen dup = cond(_N==1,0,_n)
tab dup // No duplicates
drop if dup>1
keep id_municipio week number_disasters id_municipio_bcb id_regiao_imediata id_regiao_intermediaria capital_uf id_uf nome_regiao
*put name conventions!!! 
rename id_municipio_bcb muni_cd
*rename number_disasters flood 
********************************************************************************
* Should be 
gen flood = 0
replace flood = 1 if number_disasters>0 
replace flood = . if missing(number_disasters)
********************************************************************************
* Limit to after Pix
keep if week >= wofd(mdy(1, 1, 2019)) & week < wofd(mdy(11, 16, 2020))
sort muni_cd week

* Create variables: date_flood, after_flood
gen after_flood = 0
bysort muni_cd : egen temp = min(week) if flood > 0
bysort muni_cd : egen date_flood = max(temp) 
drop temp
format %tw date_flood
replace after_flood = 1 if week >= date_flood

save "$dta\flood_weekly_2019_2020.dta", replace

use "$dta\natural_disasters_weekly_filled_flood.dta", clear
merge m:1 id_municipio using "$dta\municipios2.dta"
tab id_municipio if _merge == 1
keep if _merge == 3
drop _merge
sort id_municipio id_municipio_bcb week
by id_municipio id_municipio_bcb week: gen dup = cond(_N==1,0,_n)
tab dup // No duplicates
drop if dup>1
keep id_municipio week number_disasters id_municipio_bcb id_regiao_imediata id_regiao_intermediaria capital_uf id_uf nome_regiao
*put name conventions!!! 
rename id_municipio_bcb muni_cd
*rename number_disasters flood 
********************************************************************************
* Should be 
gen flood = 0
replace flood = 1 if number_disasters>0 
replace flood = . if missing(number_disasters)
********************************************************************************
* Limit to after Pix
keep if week >= wofd(mdy(1, 1, 2018)) & week < wofd(mdy(11, 16, 2020))
sort muni_cd week

* Create variables: date_flood, after_flood
gen after_flood = 0
bysort muni_cd : egen temp = min(week) if flood > 0
bysort muni_cd : egen date_flood = max(temp) 
drop temp
format %tw date_flood
replace after_flood = 1 if week >= date_flood

save "$dta\flood_weekly_2018_2020.dta", replace

****** 
* Before PIX - COVID
******

use "$dta\natural_disasters_weekly_filled_flood.dta", clear
merge m:1 id_municipio using "$dta\municipios2.dta"
tab id_municipio if _merge == 1
keep if _merge == 3
drop _merge
sort id_municipio id_municipio_bcb week
by id_municipio id_municipio_bcb week: gen dup = cond(_N==1,0,_n)
tab dup // No duplicates
drop if dup>1
keep id_municipio week number_disasters id_municipio_bcb id_regiao_imediata id_regiao_intermediaria capital_uf id_uf nome_regiao
*put name conventions!!! 
rename id_municipio_bcb muni_cd
*rename number_disasters flood 
********************************************************************************
* Should be 
gen flood = 0
replace flood = 1 if number_disasters>0 
replace flood = . if missing(number_disasters)
********************************************************************************
* Limit to after Pix
keep if week >= wofd(mdy(3, 1, 2020)) & week < wofd(mdy(11, 16, 2020))  // 37 weeks
sort muni_cd week

* Create variables: date_flood, after_flood
gen after_flood = 0
bysort muni_cd : egen temp = min(week) if flood > 0
bysort muni_cd : egen date_flood = max(temp) 
drop temp
format %tw date_flood
replace after_flood = 1 if week >= date_flood

save "$dta\flood_weekly_202003_202010.dta", replace







log close
