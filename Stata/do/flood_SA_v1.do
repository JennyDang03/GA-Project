*flood_SA_v1.do

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
log using "$log\flood_SA_v1.log", replace 

use "$dta\danos_informados_monthly_filled_flood.dta", clear
merge m:1 id_municipio using "$dta\municipios2.dta"
keep if _merge == 3
drop _merge

keep id_municipio_bcb date number_disasters
rename id_municipio_bcb muni_cd
rename date time_id
rename number_disasters flood 
keep if time_id >= ym(2020,11) & time_id <= ym(2022,12) 
sort muni_cd time_id

* Create Event Study variables -----------------------
gen after_flood = 0
bysort muni_cd : egen temp = min(time_id) if flood > 0
bysort muni_cd : egen date_flood = max(temp) 
drop temp
format %tmNN/CCYY date_flood
replace after_flood = 1 if time_id >= date_flood

merge 1:m muni_cd time_id using "$dta\Pix_PF_adoption_new_variables.dta" // I saved a new version with the new variables we created so we dont need to create again. 

*drop if _merge == 1
keep if _merge == 3
drop _merge
sort muni_cd id time_id

*Replace Flood variables with 0 that are missing

save "$dta\flood_pix.dta", replace

use "$dta\flood_pix.dta", replace
/*
*Fake data
global dta "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta"
global output "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output"
use "$dta\flood_pix_fake.dta", replace
*/

g time_to_treat = time_id - date_flood
replace time_to_treat = 0 if missing(date_flood)
* JOSE: vc pode checar se date_first_pix_sent está missing para nao tratados?
g treat = !missing(date_flood)
g never_treat = missing(date_flood)

* Sun/Abraham suggest to not include the extreme leads/lags that have smaller number of cases.
* Create relative-time indicators for treated groups by hand
* ignore distant leads and lags due to lack of observations
* (note this assumes any effects outside these leads/lags is 0)
tab time_to_treat
forvalues t = -12(1)12 {
	if `t' < -1 {
		local tname = abs(`t')
		g g_m`tname' = time_to_treat == `t'
	}
	else if `t' >= 0 {
		g g_`t' = time_to_treat == `t'
	}
}

global list_of_y after_first_pix_rec receiver trans_rec value_rec
*global list_of_treat expected_distance dist_bank
global list_of_types all // cad extracad non_bf bf all

/*
merge 1:m muni_cd time_id using "$dta\fake_panel_data_new_variables.dta"
drop if _merge == 1
*/

foreach var1 of varlist * { 
	local l`var1' : variable label `var1'
	if `"`l`var1''"' == "" {
		local l`var1' "`var1'"
		}
}
local version "v1_Stata"
foreach type of global list_of_types {
	foreach y of global list_of_y {
		
		eventstudyinteract `y' g_* if `type' == 1, cohort(date_flood) control_cohort(never_treat) absorb(i.id i.time_id) vce(cluster muni_cd) // covariates()

		*https://www.princeton.edu/~otorres/DID101.pdf
		matrix C = e(b_iw)
		mata st_matrix("A",sqrt(diagonal(st_matrix("e(V_iw)"))))
		matrix C = C \ A'
		matrix list C
		coefplot matrix(C[1]), se(C[2]) title("Event Study: Floods on `l`y''") subtitle(`"`reg_equation'"', size(vsmall)) order(g_m12 g_m11 g_m10 g_m9 g_m8 g_m7 g_m6 g_m5 g_m4 g_m3 g_m2 g_0 g_1 g_2 g_3 g_4 g_5 g_6 g_7 g_8 g_9 g_10 g_11 g_12) rename(g_m* = "-" g_* = "") vertical yline(0, lcolor(black) lwidth(thin)) xline(11.5, lcolor(black) lwidth(thin) lpattern(dash)) levels(90) graphregion(color(white)) xtitle("Months") ytitle("`l`y''") note("The effect of a flood on `l`y'' for `l`type'' following Sun and Abraham (2020)", size(vsmall))
		graph export "$output\flood_`y'_`type'_`version'.png", replace
	}
}


log close