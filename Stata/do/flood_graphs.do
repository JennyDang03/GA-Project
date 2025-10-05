
clear all
set more off, permanently 
set emptycells drop

global path "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\"
global path2 "C:\Users\mathe\Dropbox\"
global log "$path\log"
global dta "$path\Stata\dta"
global output "$path\Output"
global origdata "$path\DadosOriginais"

* From flood_map
use "$path2\RESEARCH\Natural Disasters\dta\nat_dis_monthly_1991_2022.dta", replace 

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
	legend(label(2 "0") label(3 "1" ) label(4 "2" )  label(5 "3" )  label(6 "4") label(7 "5-10") label(8 "11-20") label(9 "21-89"))	///
	plotregion(color("250 250 250"))
	* 250 250 250 is to match metropolis background color. 
	
graph export "$output\floods_by_mun_1991_2022(2).png", as(png) name("Graph") replace

spmap flood_n using "$dta\shapefiles\Brazil-municipios\C-Brazil_mun.dta", ///
	clmethod(custom) clbreaks(0 0.99 1.99 2.99 3.99 4.99 10.99 20.99 89) ///
	id(_ID) fcolor(navy*0 navy*0.4 navy*0.5 navy*0.6 navy*0.7 navy*0.8 navy*0.9 navy*1.0) ocolor(black ..) ///
	osize(0.001 ..) ///
	ndfcolor(navy*0) ///
	legend(symy(*1.1) symx(*1.1) size(*1.1) position(4)) legorder(hilo) ///
	legend(label(2 "0") label(3 "1" ) label(4 "2" )  label(5 "3" )  label(6 "4") label(7 "5-10") label(8 "11-20") label(9 "21-89"))	///
	plotregion(color(white))
	* 250 250 250 is to match metropolis background color. 
	
graph export "$output\floods_by_mun_1991_2022.png", as(png) name("Graph") replace


******************************************************

use "$dta\natural_disasters_monthly_filled_flood.dta", replace 

keep if date < ym(2023,1)
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
	graphregion(color("250 250 250")) ///
    plotregion(style(none)) 

* Save Graph
graph export "$output\seasonality_floods_2013_2022(2).png", as(png) name("Graph") replace

twoway bar number_disasters month, ///
	xlabel(1 "Jan" 2 "Feb" 3 "Mar" 4 "Apr" 5 "May" 6 "Jun" 7 "Jul" 8 "Aug" 9 "Sep" 10 "Oct" 11 "Nov" 12 "Dec") ///
	xtitle("") ytitle("Number of Floods") ///
	title("Seasonality of Floods") ///
	graphregion(color(white)) ///
    plotregion(style(none)) 

* Save Graph
graph export "$output\seasonality_floods_2013_2022.png", as(png) name("Graph") replace

************************************************
use "$path\RESEARCH\Natural Disasters\dta\nat_dis_monthly_1991_2022.dta", replace 
rename time_id date
rename flood_n number_disasters
keep date number_disasters id_municipio
*keep if date < ym(2013,01)
*append using "$dta\natural_disasters_monthly_filled_flood.dta"
*keep if date < ym(2023,01)
collapse (sum) number_disasters, by(date)
line number_disasters date

gen date2 = dofm(date)
gen month = month(date2)
collapse (sum) number_disasters, by(month)

twoway bar number_disasters month, ///
	xlabel(1 "Jan" 2 "Feb" 3 "Mar" 4 "Apr" 5 "May" 6 "Jun" 7 "Jul" 8 "Aug" 9 "Sep" 10 "Oct" 11 "Nov" 12 "Dec") ///
	xtitle("") ytitle("Number of Floods") ///
	title("Seasonality of Floods") ///
	graphregion(color("250 250 250")) ///
    plotregion(style(none)) 
* Save Graph
graph export "$output\seasonality_floods_1991_2022(2).png", as(png) name("Graph") replace

************************************************
*Histogram
use "$dta\natural_disasters_monthly_filled_flood.dta", replace
keep if date < ym(2023,01)
gen flood = 0
replace flood = 1 if number_disasters>0
replace flood = . if missing(number_disasters)
collapse (sum) number_disasters flood, by(id_municipio)
tab flood
histogram flood, discrete ///
	xtitle("Number of Floods") ytitle("Frequency") ///
	title("2013-2022") ///
	graphregion(color("250 250 250")) fcolor(navy*0.8) lcolor(navy) ///
    plotregion(style(none)) 
graph export "$output\histogram_2013_2022.png", as(png) name("Graph") replace

use "$path\RESEARCH\Natural Disasters\dta\nat_dis_monthly_1991_2022.dta", replace
rename time_id date
rename flood_n number_disasters
keep date number_disasters id_municipio
*keep if date < ym(2013,01)
*append using "$dta\natural_disasters_monthly_filled_flood.dta"
*keep if date < ym(2023,01)
gen flood = 0
replace flood = 1 if number_disasters>0
replace flood = . if missing(number_disasters)
collapse (sum) number_disasters flood, by(id_municipio)
tab flood

histogram flood, discrete ///
	xtitle("Number of Floods") ytitle("Frequency") ///
	title("1991-2022") ///
	graphregion(color("250 250 250")) fcolor(navy*0.8) lcolor(navy) ///
    plotregion(style(none)) 

graph export "$output\histogram_1991_2022(2).png", as(png) name("Graph") replace

histogram flood, discrete ///
	xtitle("Number of Floods") ytitle("Frequency") ///
	title("1991-2022") ///
	graphregion(color(white)) fcolor(navy*0.8) lcolor(navy) ///
    plotregion(style(none)) 

graph export "$output\histogram_1991_2022.png", as(png) name("Graph") replace


gen flood_n = flood
replace flood_n = 13 if flood >=13
histogram flood_n, discrete ///
	xtitle("Number of Floods") ytitle("Frequency") ///
	title("1991-2022") ///
	graphregion(color("250 250 250")) fcolor(navy*0.8) lcolor(navy) ///
    plotregion(style(none)) 
graph export "$output\histogram_1991_2022_v2(2).png", as(png) name("Graph") replace





* flood_risk5


use "$path\RESEARCH\Natural Disasters\dta\nat_dis_monthly_1991_2022.dta", replace
keep if time_id <= ym(2018,12)
rename time_id date
rename flood_n number_disasters
keep date number_disasters id_municipio
*keep if date < ym(2013,01)
*append using "$dta\natural_disasters_monthly_filled_flood.dta"
*keep if date < ym(2023,01)
gen flood = 0
replace flood = 1 if number_disasters>0
replace flood = . if missing(number_disasters)
collapse (sum) number_disasters flood, by(id_municipio)
tab flood

histogram flood, discrete ///
	xtitle("Number of Floods") ytitle("Frequency") ///
	title("1991-2018") ///
	graphregion(color("250 250 250")) fcolor(navy*0.8) lcolor(navy) ///
    plotregion(style(none)) 

graph export "$output\histogram_1991_2018(2).png", as(png) name("Graph") replace

histogram flood, discrete ///
	xtitle("Number of Floods") ytitle("Frequency") ///
	title("1991-2018") ///
	graphregion(color(white)) fcolor(navy*0.8) lcolor(navy) ///
    plotregion(style(none)) 

graph export "$output\histogram_1991_2018(2).png", as(png) name("Graph") replace

*****************************************
* Line Graphs - year

use "$path\RESEARCH\Natural Disasters\dta\nat_dis_monthly_1991_2022.dta", replace 
*collapse to total floods
gen date2 = dofm(time_id)
gen year = year(date2)
collapse (sum) drought_n flood_n virus_n others_n, by(year)
twoway line flood_n year, ///
	xtitle("Year") ytitle("Number of Floods") ///
    title("Number of Floods Over Time") ///
    graphregion(color("250 250 250"))  ///
    plotregion(style(none)) 

* Save Graph
graph export "$output\yearly_floods_1991_2022.png", as(png) name("Graph") replace


