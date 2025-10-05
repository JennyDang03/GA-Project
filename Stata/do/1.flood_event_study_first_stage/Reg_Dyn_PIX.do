
global log "\\sbcdf060\depepmetas$\usuarios\Jose Renato\Projetos\PIX_Matheus\"
global dta "\\sbcdf060\depepmetas$\usuarios\Jose Renato\Projetos\PIX_Matheus\"
global output "\\sbcdf060\depepmetas$\usuarios\Jose Renato\Projetos\PIX_Matheus\"


* ADO
 adopath ++ "//sbcdf060/depep01$/ADO"
 adopath ++ "//sbcdf060/depep01$/ado-776e"

capture log close
log using "$log\Reg_Dyn_Pix.log", replace 

use "$dta\\Base_week_muni_flood.dta" , clear

global window=12  // em semanas
global FE muni_cd week c.pc_moradores_3g#week
*global FE muni_cd week 
*global FE muni_cd week pc_moradores_3g


********** POST PIX **********
keep if week >= wofd(mdy(12, 1, 2020)) & week <= wofd(mdy(4, 30, 2022))
*drop if moradores >= 100000

keep if  (date_w_disaster_mun >=  wofd(mdy(2, 1, 2021)) & date_w_disaster_mun <=  wofd(mdy(12, 31, 2021)) ) |  date_w_disaster_mun == .

cap drop dist_disaster_mun id_dist_disaster_mun

gen dist_disaster_mun=week-date_w_disaster_mun if abs(week-date_w)<=${window}&missing(date_w)==0
replace dist_disaster_mun=-1 if missing(date_w_disaster_mun) & cem_weights>0
egen id_dist_disaster_mun=group(dist_disaster_mun)
labmask id_dist_disaster_mun, values(dist_disaster_mun)


* PIX quantidade de transações
foreach var in  qtd_PIX_intra  qtd_PIX_inflow  qtd_PIX_outflow{
	cap drop l`var'
	gen l`var'=log(`var' + 1)
	eststo QTD:reghdfe l`var' ib${window}.id_dist_disaster_mun [aweight=cem_weights] if cem_weights>0&(abs(week-date_w)<=${window}|missing(date_w)), absorb($FE) vce(cluster muni_cd) base 

	coefplot QTD, vertical yline(0, lcolor(black) lwidth(thin)) xline(12, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) omitted baselevels ytitle("`var'")  xtitle("Weeks after the disaster")  drop(_cons) 
	
	graph save Graph "$output\QTD_PIX_`var'.gph", replace
	graph export "$output\QTD_PIX_`var'.png", replace

}

* PIX Valores
foreach var in  valor_PIX_intra  valor_PIX_inflow  valor_PIX_outflow {
	cap drop l`var'
	gen l`var'=log(`var' + 1)
	eststo VOLUME: reghdfe l`var' ib${window}.id_dist_disaster_mun [aweight=cem_weights] if cem_weights>0&(abs(week-date_w)<=${window}|missing(date_w)),absorb($FE) vce(cluster muni_cd) base 
	
	
	* 	coefplot VOLUME,  vertical drop(_cons) omit yline(0) xline(11.5,lcolor(gs0) lpattern(dash)) levels(90)
		coefplot VOLUME, vertical yline(0, lcolor(black) lwidth(thin)) xline(12, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) omitted baselevels  xtitle("Weeks after the disaster") ytitle("`var'") drop(_cons) 
	
	graph save Graph "$output\VOL_PIX_`var'.gph", replace
	graph export "$output\VOL_PIX_`var'.png", replace

}


* PIX quantidades de clientes 
foreach var in  n_cli_pag_pf n_cli_rec_pf n_cli_pag_pj n_cli_rec_pj  {
	cap drop l`var'
	gen l`var'=log(`var'+1)
	eststo QTD:reghdfe l`var' ib${window}.id_dist_disaster_mun [aweight=cem_weights] if cem_weights>0&(abs(week-date_w)<=${window}|missing(date_w)), absorb($FE)  vce(cluster muni_cd) base 

	coefplot QTD, vertical yline(0, lcolor(black) lwidth(thin)) xline(12, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) omitted baselevels  xtitle("Weeks after the disaster") ytitle("`var'") drop(_cons) 

	graph save Graph "$output\QTD_CLI_PIX`var'.gph", replace
	graph export "$output\QTD_CLI_PIX`var'.png", replace

}



log close

