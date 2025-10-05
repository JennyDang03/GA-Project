

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
log using "$log\Reg_Ini.log", replace 

use "$dta\Pix_PF_adoption.dta", clear 

drop if confidence <= 8
drop if aux_emerg_jan_mar21 == 1

* Tira Nov/2020 e Dez/2020 e vai até ...
keep if time_id>731 & time_id<=747

cap drop after_event 
gen after_event  =  time_id >= mofd(mdy(5, 1, 2021))
gen dist_event = time_id- mofd(mdy(5, 1, 2021))  // MAIO/21 é o tempo zero
egen id_dist=group(dist_event)
labmask id_dist, values(dist_event)

gen prop_dist = dist_caixa / expected_distance
*gen prop_dist = dist_caixa / dist_otherbank

gen diff_dist = dist_caixa - expected_distance


*reghdfe after_first_pix_sent c.prop_dist#1.after_event , absorb(id time_id) vce(robust) base

*reghdfe after_first_pix_rec c.prop_dist#1.after_event , absorb(id time_id) vce(robust) base

est clear 
eststo SENT:reghdfe after_first_pix_sent ib5.id_dist#c.prop_dist , absorb(id time_id) vce(robust) base

local fazlabel " "
display "`fazlabel'" 
local aspas = `""'
levelsof id_dist, local(levels)
foreach l of local levels {
	*local mylist "`mylist' `l'.id_dist"
	local m = `l'-5
	*local fazlabel "`fazlabel' 	`l'.id_dist#c.dist_caixa =  `aspas' `l'  `aspas' "	
	local fazlabel "`fazlabel' 	`l'.id_dist#c.prop_dist =  `aspas' `m'  `aspas' "	
	
}
display "`fazlabel'" 
 
coefplot SENT, vertical yline(0, lcolor(black) lwidth(thin)) xline(5, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) omitted baselevels  xtitle("Months from jan/21") xlab(, labsize(vsmall)) drop(_cons *expected_distance)  headings(`fazlabel', labsize(vsmall) ) rename(*.id_dist#c.prop_dist = "") 

graph export "$output\SENT_PIX_Prop_Dist.png", replace
	

log close 