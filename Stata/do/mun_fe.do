*mun_fe

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
* Flood Risk
use "$dta\natural_disasters_monthly_filled_flood.dta", replace 
keep if date <= ym(2017,12)
collapse (sum) number_disasters, by(id_municipio)
gen flood_risk = 0
replace flood_risk = 1 if number_disasters>0 
replace flood_risk = . if missing(number_disasters)
keep id_municipio flood_risk
tab flood_risk

merge m:1 id_municipio using "$dta\municipios2.dta"
tab id_municipio if _merge == 1
keep if _merge == 3
drop _merge

keep id_municipio flood_risk id_municipio_bcb id_regiao_imediata id_regiao_intermediaria capital_uf id_uf nome_regiao
rename id_municipio_bcb muni_cd
encode nome_regiao, gen(nome_regiao_code)
save "$dta\mun_fe.dta",replace

use "C:\Users\mathe\Dropbox\RESEARCH\Natural Disasters\dta\nat_dis_monthly_1991_2022.dta", replace
* Delete some dates
keep if time_id <= ym(2017,12)
**
collapse (sum) flood_n, by(id_municipio)
gen flood_risk2 = 0
replace flood_risk2 = 1 if flood_n>0
replace flood_risk2 = . if missing(flood_n)
tab flood_risk2
keep id_municipio flood_risk2

merge 1:1 id_municipio using "$dta\mun_fe.dta"
keep if _merge == 3
drop _merge
save "$dta\mun_fe.dta",replace

* Put nat_dis_monthly_1991_2022.dta together with natural_disasters_monthly_filled_flood.dta
use "$dta\nat_dis_monthly_1991_2022.dta", replace
* Delete some dates
keep if time_id <= ym(2012,12)
keep time_id flood_n id_municipio
rename time_id date
rename flood_n number_disasters
append using "$dta\natural_disasters_monthly_filled_flood.dta"
rename date time_id
rename number_disasters flood_n
save "$dta\natural_disasters_monthly_filled_flood_1991.dta", replace


* flood_risk5
use "$dta\natural_disasters_monthly_filled_flood_1991.dta", replace
* Delete some dates
keep if time_id <= ym(2017,12)
collapse (sum) flood_n, by(id_municipio)
gen flood_risk5 = 0 
replace flood_risk5 = 1 if flood_n == 1 
replace flood_risk5 = 2 if flood_n == 2 
replace flood_risk5 = 3 if flood_n == 3 | flood_n == 4  | flood_n == 5 
replace flood_risk5 = 4 if flood_n > 5 
replace flood_risk5 = . if missing(flood_n)
tab flood_risk5
tab flood_n
keep id_municipio flood_risk5
merge 1:1 id_municipio using "$dta\mun_fe.dta"
keep if _merge == 3
drop _merge
save "$dta\mun_fe.dta",replace

* flood_risk4
use "$dta\natural_disasters_monthly_filled_flood_1991.dta", replace
* Delete some dates
keep if time_id <= ym(2017,12)
collapse (sum) flood_n, by(id_municipio)
tab flood_n
gen flood_risk4 = 0 
replace flood_risk4 = 1 if flood_n == 1 
replace flood_risk4 = 2 if flood_n == 2 | flood_n == 3 
replace flood_risk4 = 3 if flood_n >= 4 
replace flood_risk4 = . if missing(flood_n)
tab flood_risk4
tab flood_n
keep id_municipio flood_risk4
merge 1:1 id_municipio using "$dta\mun_fe.dta"
keep if _merge == 3
drop _merge
save "$dta\mun_fe.dta",replace


* flood_risk3
* number of floods from 1991-2017

* flood_risk4
	* Weighted average of the flood risk. Higher risk if flood happened recent. 
	
* flood_risk_time
	* Last 5 years, how many floods did we have?
	
	* Last 10 years, how many floods did we have?
	
	* Last 15 years, how many floods did we have?
	
	* Weighted average of the flood risk. Higher risk if flood happened recent. 
	
	* number of floods from 1991-2017 by season/month/week





* PIB quartiles
use "$dta\Importa_pib_mun.dta", replace 
destring id_municipio, replace
keep if ano == 2017
rename pib pib_2017 
keep id_municipio pib_2017
merge 1:1 id_municipio using "$dta\mun_fe.dta"
keep if _merge == 3
drop _merge
xtile pib_2017_quart = pib_2017, nq(4)
save "$dta\mun_fe.dta",replace

* Rural vs Urban
import excel "$path\CSV\rural - urban\Tipologia_municipal_rural_urbano.xlsx", sheet("Tipologia_munic_rural_urbano") firstrow case(lower) clear
rename cd_gcmun id_municipio
encode tipo, gen(rural_urban)
keep id_municipio rural_urban
merge 1:1 id_municipio using "$dta\mun_fe.dta"
keep if _merge == 3
drop _merge
save "$dta\mun_fe.dta",replace


* Population 2022, Population 2010, Populacao 2017 - Quartiles
use "$dta/Importa_pop_mun.dta", replace
drop if id_municipio == "NA"
destring id_municipio,replace
keep if ano == 2010 | ano == 2022
drop sigla_uf
rename populacao pop
reshape wide pop, i(id_municipio) j(ano) 
xtile pop2010_quart = pop2010, nq(4)
xtile pop2022_quart = pop2022, nq(4)
merge 1:1 id_municipio using "$dta\mun_fe.dta"
keep if _merge == 3
drop _merge
save "$dta\mun_fe.dta",replace



************************************************************************

* Covid is control (changes with time)
 use "C:\Users\mathe\Dropbox\RESEARCH\Covid_Estban\dta\1.covid_cases_monthly.dta", replace



* 3g is control (changes with time)


* 3g 2017? - Quartiles





