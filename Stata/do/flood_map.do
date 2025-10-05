




clear all
set more off, permanently 

set emptycells drop

global path "\\sbcdf176\PIX_Matheus$\"
global path "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\"
global path "C:\Users\mathe\Dropbox\"


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
* Need a better thing than municipios2
merge m:1 id_municipio using "$dta\municipios2.dta"
tab id_municipio if _merge == 1
keep if _merge == 3
drop _merge

keep id_municipio date number_disasters id_municipio_bcb nome_uf sigla_uf

*collapse to total floods
collapse (sum) number_disasters, by(nome_uf sigla_uf)

* we could use nome_uf with NAME_1
gen HASC_1 = "BR." + sigla_uf
merge m:1 HASC_1 using "$dta\shapefiles\bra_adm1\Brazil_state.dta"

*spshape2dta "$dta\shapefiles\bra_adm1\BRA_adm1"
*grmap, activate

*grmap number_disasters

spmap number_disasters using "$dta\shapefiles\bra_adm1\C-Brazil_state.dta", id(_ID) fcolor(navy*0 navy*0.5 navy*0.7 navy*1.0) ocolor(black) ///
title("Number of Disasters by Location") note("Your notes here")

graph export "$output\floods_by_state.jpg", as(jpg) name("Graph") quality(100) replace


********************************************************


use "$dta\natural_disasters_monthly_filled_flood.dta", replace 
*collapse to total floods
collapse (sum) number_disasters, by(id_municipio)

tostring id_municipio, gen(CODIGO_MUN) 

merge m:1 CODIGO_MUN using "$dta\shapefiles\Brazil-municipios\Brazil_mun.dta"
tab id_municipio if _merge == 1
tab NOME_1 if _merge == 2

drop if _merge == 1
replace number_disasters = 0 if _merge == 2
drop _merge

spmap number_disasters using "$dta\shapefiles\Brazil-municipios\C-Brazil_mun.dta", ///
	clmethod(custom) clbreaks(0 0.99 1.99 2.99 3.99 4.99 12) ///
	id(_ID) fcolor(navy*0 navy*0.4 navy*0.5 navy*0.6 navy*0.7 navy*0.8) ocolor(black ..) ///
	osize(0.001 ..) ///
	ndfcolor(navy*0) ///
	legend(symy(*1.1) symx(*1.1) size(*1.1) position(4)) legorder(hilo) ///
	legend(label(2 "0") label(3 "1" ) label(4 "2" )  label(5 "3" )  label(6 "4") label(7 "5-12"))
	*title("Number of Floods") ///
	*note("Your notes here")
*fcolor(Blues2)

graph export "$output\floods_by_mun.png", as(png) name("Graph") replace


use "$path\RESEARCH\Natural Disasters\dta\nat_dis_monthly_1991_2022.dta", replace 

*collapse to total floods
collapse (sum) drought_n flood_n virus_n others_n, by(id_municipio)

tostring id_municipio, gen(CODIGO_MUN) 

merge m:1 CODIGO_MUN using "$dta\shapefiles\Brazil-municipios\Brazil_mun.dta"
tab id_municipio if _merge == 1
tab NOME_1 if _merge == 2

drop if _merge == 1
replace drought_n = 0 if _merge == 2 
replace flood_n = 0 if _merge == 2 
replace virus_n = 0 if _merge == 2 
replace others_n = 0 if _merge == 2 
drop _merge

spmap flood_n using "$dta\shapefiles\Brazil-municipios\C-Brazil_mun.dta", ///
	clmethod(custom) clbreaks(0 0.99 1.99 2.99 3.99 4.99 10.99 20.99 89) ///
	id(_ID) fcolor(navy*0 navy*0.4 navy*0.5 navy*0.6 navy*0.7 navy*0.8 navy*0.9 navy*1.0) ocolor(black ..) ///
	osize(0.001 ..) ///
	ndfcolor(navy*0) ///
	legend(symy(*1.1) symx(*1.1) size(*1.1) position(4)) legorder(hilo) ///
	legend(label(2 "0") label(3 "1" ) label(4 "2" )  label(5 "3" )  label(6 "4") label(7 "5-10") label(8 "11-20") label(9 "21-89"))	
	
	*legstyle(2) ///
	*legtitle("oi") 
	*title("Number of Floods") ///
	*note("Your notes here")

* PUT legend on the right side. 
* Fix scale so that they match 
	
graph export "$output\floods_by_mun_1991_2022.png", as(png) name("Graph") replace


spmap drought_n using "$dta\shapefiles\Brazil-municipios\C-Brazil_mun.dta", ///
	clmethod(custom) clbreaks(0 0.99 1.99 2.99 3.99 4.99 10.99 20.99 89) ///
	id(_ID) fcolor(Reds2) ocolor(black ..) ///
	osize(0.001 ..) ///
	ndfcolor(navy*0) ///
	legend(symy(*1.1) symx(*1.1) size(*1.1) position(4)) legorder(hilo) ///
	legend(label(2 "0") label(3 "1" ) label(4 "2" )  label(5 "3" )  label(6 "4") label(7 "5-10") label(8 "11-20") label(9 "21-89"))	

graph export "$output\droughts_by_mun_1991_2022.png", as(png) name("Graph") replace



spmap others_n using "$dta\shapefiles\Brazil-municipios\C-Brazil_mun.dta", ///
	clmethod(custom) clbreaks(0 0.99 1.99 2.99 3.99 4.99 10.99 20.99 89) ///
	id(_ID) fcolor(navy*0 navy*0.4 navy*0.5 navy*0.6 navy*0.7 navy*0.8 navy*0.9 navy*1.0) ocolor(black ..) ///
	osize(0.001 ..) ///
	ndfcolor(navy*0) ///
	legend(symy(*1.1) symx(*1.1) size(*1.1) position(4)) legorder(hilo) ///
	legend(label(2 "0") label(3 "1" ) label(4 "2" )  label(5 "3" )  label(6 "4") label(7 "5-10") label(8 "11-20") label(9 "21-89"))	
	
spmap virus_n using "$dta\shapefiles\Brazil-municipios\C-Brazil_mun.dta", ///
	clmethod(custom) clbreaks(0 0.99 1.99 2.99 3.99 4.99 10.99 20.99 89) ///
	id(_ID) fcolor(navy*0 navy*0.4 navy*0.5 navy*0.6 navy*0.7 navy*0.8 navy*0.9 navy*1.0) ocolor(black ..) ///
	osize(0.001 ..) ///
	ndfcolor(navy*0) ///
	legend(symy(*1.1) symx(*1.1) size(*1.1) position(4)) legorder(hilo) ///
	legend(label(2 "0") label(3 "1" ) label(4 "2" )  label(5 "3" )  label(6 "4") label(7 "5-10") label(8 "11-20") label(9 "21-89"))	
	


******************************************************

* Make line Graph

use "$dta\natural_disasters_monthly_filled_flood.dta", replace 

*collapse to total floods
collapse (sum) number_disasters, by(date)
line number_disasters date

gen date2 = dofm(date)
gen month = month(date2)
collapse (sum) number_disasters, by(month)

twoway bar number_disasters month, ///
	xlabel(1 "Jan" 2 "Feb" 3 "Mar" 4 "Apr" 5 "May" 6 "Jun" 7 "Jul" 8 "Aug" 9 "Sep" 10 "Oct" 11 "Nov" 12 "Dec") ///
	xtitle("") ytitle("Number of Floods") ///
	title("Seasonality of Floods") ///
	graphregion(color(white))

* Save Graph
graph export "$output\seasonality_floods_2013_2022.png", as(png) name("Graph") replace



use "$path\RESEARCH\Natural Disasters\dta\nat_dis_monthly_1991_2022.dta", replace 
*collapse to total floods
collapse (sum) drought_n flood_n virus_n others_n, by(time_id)
line flood_n time_id

gen date2 = dofm(time_id)
gen month = month(date2)
collapse (sum) drought_n flood_n virus_n others_n, by(month)

twoway bar flood_n month, ///
	xlabel(1 "Jan" 2 "Feb" 3 "Mar" 4 "Apr" 5 "May" 6 "Jun" 7 "Jul" 8 "Aug" 9 "Sep" 10 "Oct" 11 "Nov" 12 "Dec") ///
	xtitle("") ytitle("Number of Floods") ///
	title("Seasonality of Floods") ///
	graphregion(color(white))

* Save Graph
graph export "$output\seasonality_floods_1991_2022.png", as(png) name("Graph") replace



************************************************
* How destructive they are. 




************************************************
*Histogram
use "$dta\natural_disasters_monthly_filled_flood.dta", replace
gen flood = 0
replace flood = 1 if number_disasters>0
replace flood = . if missing(number_disasters)
collapse (sum) number_disasters flood, by(id_municipio)
tab flood
histogram flood, discrete ///
	xtitle("Number of Floods") ytitle("Frequency") ///
	title("2013-2022") ///
	graphregion(color(white)) fcolor(navy*0.8) lcolor(navy)
graph export "$output\histogram_2013_2022.png", as(png) name("Graph") replace

use "$dta\natural_disasters_monthly_filled_flood.dta", replace 
keep if date >= ym(2020,11)
tab number_disasters
* Many times, we have two disasters in one calendar month, lets not count that
gen flood = 0
replace flood = 1 if number_disasters>0 
replace flood = . if missing(number_disasters)
collapse (sum) number_disasters flood, by(id_municipio)
tab flood
histogram flood, discrete ///
	xtitle("Number of Floods") ytitle("Frequency") ///
	title("2020-2022") ///
	graphregion(color(white)) fcolor(navy*0.8) lcolor(navy)
graph export "$output\histogram_2020_2022.png", as(png) name("Graph") replace
tab number_disasters
*histogram number_disasters, discrete

use "$dta\natural_disasters_monthly_filled_flood.dta", replace 
keep if date < ym(2020,11) & date >= ym(2018,1) 
tab number_disasters
* Many times, we have two disasters in one calendar month, lets not count that
gen flood = 0
replace flood = 1 if number_disasters>0
replace flood = . if missing(number_disasters)
collapse (sum) number_disasters flood, by(id_municipio)
tab flood
histogram flood, discrete ///
	xtitle("Number of Floods") ytitle("Frequency") ///
	title("2018-2020") ///
	graphregion(color(white)) fcolor(navy*0.8) lcolor(navy)
graph export "$output\histogram_2018_2020.png", as(png) name("Graph") replace
tab number_disasters
*histogram number_disasters, discrete




use "$path\RESEARCH\Natural Disasters\dta\nat_dis_monthly_1991_2022.dta", replace

collapse (sum) drought_n flood_n virus_n others_n, by(id_municipio)
tab flood_n
gen flood = flood_n
replace flood = 10 if flood_n >=10
histogram flood_n, discrete ///
	xtitle("Number of Floods") ytitle("Frequency") ///
	title("1991-2022") ///
	graphregion(color(white)) fcolor(navy*0.8) lcolor(navy)
graph export "$output\histogram_1991_2022.png", as(png) name("Graph") replace


replace flood = flood_n
replace flood = 13 if flood_n >=13
histogram flood, discrete ///
	xtitle("Number of Floods") ytitle("Frequency") ///
	title("1991-2022") ///
	graphregion(color(white)) fcolor(navy*0.8) lcolor(navy)
graph export "$output\histogram_1991_2022_v2.png", as(png) name("Graph") replace




***

use "$dta\natural_disasters_monthly_filled_drought.dta", replace
gen drought = 0
replace drought = 1 if number_disasters>0
collapse (sum) number_disasters drought, by(id_municipio)
tab drought
histogram drought, discrete ///
	xtitle("Number of Droughts") ytitle("Frequency") ///
	title("2013-2022") ///
	graphregion(color(white)) fcolor(navy*0.8) lcolor(navy)
graph export "$output\histogram_2013_2022_drought.png", as(png) name("Graph") replace




*******************************


* Line Graphs


use "$dta\natural_disasters_monthly_filled_flood.dta", replace 
gen flood = 0
replace flood = 1 if number_disasters>0
replace flood = . if missing(number_disasters)
*collapse to total floods
collapse (sum) number_disasters flood, by(date)
line number_disasters date
line flood date


* Line Graphs - year

use "$path\RESEARCH\Natural Disasters\dta\nat_dis_monthly_1991_2022.dta", replace 
*collapse to total floods
gen date2 = dofm(time_id)
gen year = year(date2)
collapse (sum) drought_n flood_n virus_n others_n, by(year)
twoway line flood_n year, ///
	xtitle("Year") ytitle("Number of Floods") ///
    title("Number of Floods Over Time") ///
    graphregion(color(white)) 

* Save Graph
graph export "$output\yearly_floods_1991_2022.png", as(png) name("Graph") replace


use "$dta\natural_disasters_monthly_filled_flood.dta", replace 
*collapse to total floods
gen date2 = dofm(date)
gen year = year(date2)
collapse (sum) number_disasters, by(year)
twoway line number_disasters year, ///
	xtitle("Year") ytitle("Number of Floods") ///
    title("Number of Floods Over Time") ///
    graphregion(color(white)) 

* Save Graph
graph export "$output\yearly_floods_2013_2022.png", as(png) name("Graph") replace
