* week_to_month.dta


* Transform Week in Month



clear all
set more off, permanently 

set emptycells drop

global log "\\sbcdf176\PIX_Matheus$\Stata\log"
global dta "\\sbcdf176\PIX_Matheus$\Stata\dta"
global output "\\sbcdf176\PIX_Matheus$\Output"
global origdata "\\sbcdf176\PIX_Matheus$\DadosOriginais"
global do "\\sbcdf176\PIX_Matheus$\Stata\do"

/*
*Fake data
global dta "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta"
global output "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output"
use "$dta\flood_pix_weekly_fake.dta", replace

*/
use "$dta\Base_week_muni_flood.dta.dta", replace

* Based on https://journals.sagepub.com/doi/pdf/10.1177/1536867X1201200316

gen date = dofw(week)
format date %td

gen time_id = mofd(date)
format time_id %tm

gen length=min(day(date),7)
local N = _N
expand 2 if length < 7
replace time_id = time_id - 1 if _n > `N'
replace length = 7 - length if _n > `N'

collapse (mean) valor_cartao_credito - date_flood (count) days = length [fw=length], by(time_id muni_cd)

* Now, date flood is in Weeks instead of months. 
drop date_flood
merge m:1 muni_cd time_id using "$dta\flood_monthly_2020_2022.dta", keep(3) keepusing(date_flood)
drop _merge 

order muni_cd time_id
sort muni_cd time_id

save "$dta\Base_month_muni_flood.dta", replace
*save "$dta\flood_pix_monthly_fake.dta", replace


