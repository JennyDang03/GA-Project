* Cria graficos de event studies de estados e do Brasil
* Input:
* - "$dta\disaster_weekly_`disaster'.dta"
* - "$dta\cem_`state'_`disaster'.dta"
* - "$dta\PIX_week_muni_clean.dta"
* Output
* - Graficos 
*



clear all
set more off, permanently 
set matsize 2000
set emptycells drop


global log "D:\PIX_Matheus\Stata\log"
global dta "D:\PIX_Matheus\Stata\dta"
global output "D:/PIX_Matheus/Output"
global origdata "D:\PIX_Matheus\DadosOriginais"

* ADO - Problem Here
adopath ++ "D:\ado"


capture log close
log using "$log\Reg_Dyn_Pix.log", replace 



foreach state in CE {
	foreach disaster in  flood drought {
		use "$dta\disaster_weekly_`disaster'.dta", replace
		keep if week >= wofd(mdy(11, 1, 2020)) & week <= wofd(mdy(12, 31, 2021))
		merge m:1 id_municipio id_municipio_bcb using "$dta\cem_`state'_`disaster'.dta"
		keep if _merge == 3
		drop _merge
		foreach transaction_type in fora_dentro dentro_fora ///
				dentro_dentro first_pix_received first_pix_paid {		
			merge 1:1 id_municipio_bcb week using "$dta\dados_originais_`state'_`transaction_type'.dta"
			global window=12  // em semanas
			global FE week //id_municipio week c.pc_moradores_3g#week

			keep if  (date_w_disaster_mun >=  wofd(mdy(2, 1, 2021)) & date_w_disaster_mun <=  wofd(mdy(12, 31, 2021)) ) |  date_w_disaster_mun == .

			cap drop dist_disaster_mun id_dist_disaster_mun
			gen dist_disaster_mun=week-date_w_disaster_mun if abs(week-date_w)<=${window}&missing(date_w)==0
			replace dist_disaster_mun=-1 if missing(date_w_disaster_mun) & cem_weights>0
			egen id_dist_disaster_mun=group(dist_disaster_mun)
			labmask id_dist_disaster_mun, values(dist_disaster_mun)

			* PIX quantidade de transações
			foreach var in valor qtd{
				cap drop l`var'
				gen l`var'=log(`var' + 1)
				eststo QTD:reghdfe l`var' ib${window}.id_dist_disaster_mun, ab sorb($FE) vce(cluster id_municipio) base 

				*eststo QTD:reghdfe l`var' ib${window}.id_dist_disaster_mun [aweight=cem_weights] if cem_weights>0&(abs(week-date_w)<=${window}|missing(date_w)), absorb($FE) vce(cluster id_municipio) base 

				coefplot QTD, vertical yline(0, lcolor(black) lwidth(thin)) xline(12, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) omitted baselevels ytitle("`var'")  xtitle("Weeks after the disaster")  drop(_cons) 
				
				graph save Graph "$output\graph_`state'_`transaction_type'_`var'_`disaster'.gph", replace
				graph export "$output\graph_`state'_`transaction_type'_`var'_`disaster'.png", replace

			}
		}
	}
}



******************************************************************************
*BRAZIL
******************************************************************************


use "$dta\PIX_week_muni.dta", replace
drop if muni_cd == -3
sort muni_cd week
xtset muni_cd week
tsfill, full
foreach var in valor_inflow qtd_inflow n_cli_rec_pf_inflow n_cli_rec_pj_inflow valor_outflow qtd_outflow n_cli_pag_pf_outflow n_cli_pag_pj_outflow valor_intra qtd_intra n_cli_pag_pf_intra n_cli_rec_pf_intra n_cli_pag_pj_intra n_cli_rec_pj_intra {
    replace `var'=0 if `var' == . 
}
rename muni_cd id_municipio_bcb
save "$dta\PIX_week_muni_clean.dta", replace


foreach disaster in  flood drought {
		use "$dta\disaster_weekly_`disaster'.dta", replace
		keep if week >= wofd(mdy(12, 1, 2020)) & week <= wofd(mdy(12, 31, 2021))
		merge m:1 id_municipio id_municipio_bcb using "$dta\cem_brasil_`disaster'.dta"
		keep if _merge == 3
		drop _merge
	
		merge 1:1 id_municipio_bcb week using "$dta\PIX_week_muni_clean.dta"
		global window=12  // em semanas
		global FE week id_municipio // c.pc_moradores_3g#week

		keep if  (date_w_disaster_mun >=  wofd(mdy(2, 1, 2021)) & date_w_disaster_mun <=  wofd(mdy(12, 31, 2021)) ) |  date_w_disaster_mun == .

		cap drop dist_disaster_mun id_dist_disaster_mun
		gen dist_disaster_mun=week-date_w_disaster_mun if abs(week-date_w)<=${window}&missing(date_w)==0
		replace dist_disaster_mun=-1 if missing(date_w_disaster_mun) & cem_weights>0
		egen id_dist_disaster_mun=group(dist_disaster_mun)
		labmask id_dist_disaster_mun, values(dist_disaster_mun)
		
		cap drop n_cli_inflow n_cli_outflow n_cli_pag_intra n_cli_rec_intra
		gen n_cli_inflow    = n_cli_rec_pf_inflow + n_cli_rec_pj_inflow
		gen n_cli_outflow   = n_cli_pag_pf_outflow + n_cli_pag_pj_outflow
		gen n_cli_pag_intra = n_cli_pag_pf_intra + n_cli_pag_pj_intra 
		gen n_cli_rec_intra = n_cli_rec_pf_intra + n_cli_rec_pj_intra
		gen n_receivers = n_cli_inflow + n_cli_rec_intra
		gen n_senders = n_cli_outflow + n_cli_pag_intra
		label var qtd_inflow "Log Inflow of Pix"
		label var qtd_outflow "Log Outflow of Pix"
		label var qtd_intra "Log Pix transactions within the municipality"
		label var n_cli_inflow "Log number of receivers of inflow"
		label var n_cli_outflow "Log number of senders of outflow"
		label var n_cli_pag_intra "Log number of senders of within the municipality"
		label var n_cli_rec_intra "Log number of receivers of within the municipality"
		label var n_receivers "Log number of receivers"
		label var n_senders "Log number of senders"
		* PIX quantidade de transações
		foreach var in qtd_inflow qtd_outflow qtd_intra n_receivers n_senders {
			cap drop l`var'
			gen l`var'=log(`var' + 1)
			*eststo QTD:reghdfe l`var' ib${window}.id_dist_disaster_mun, ab sorb($FE) vce(cluster id_municipio) base 
			eststo QTD:reghdfe l`var' ib${window}.id_dist_disaster_mun [aweight=cem_weights] if cem_weights>0&(abs(week-date_w)<=${window}|missing(date_w)), absorb($FE) vce(cluster id_municipio) base 
 
			coefplot QTD, vertical yline(0, lcolor(black) lwidth(thin)) xline(12, lcolor(black) lwidth(thin) lpattern(dash)) levels(90)  graphregion(color(white)) omitted baselevels ytitle("`: variable label `var''") xtitle("Weeks after the `disaster'")  drop(_cons) 
			*
			*Graph
			graph save "Graph" "$output/graph_brasil_`var'_`disaster'", replace
			graph export "$output\graph_brasil_`var'_`disaster'.png", replace

		}
	}
	
	
	***************************************************************************
	
	
	
	
	
	
	importa pix muni















*** Pix de Dentro para Fora
**** !!!!!!!!!!! Seems to have something wron since it was supposed to be only 184 municipaliteis

use "$dta\dados_originais_`state'_dentro_fora.dta",replace

*Merge with floods
merge m:m id_municipio_bcb week using "$dta\disaster_weekly_flood.dta"
keep if week >= wofd(mdy(11, 1, 2020)) & week <= wofd(mdy(1, 1, 2022))
tab _merge
keep if _merge == 3
drop _merge
sort id_municipio week
order id_municipio


























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




*** Pix de Dentro para Fora
**** !!!!!!!!!!! Seems to have something wron since it was supposed to be only 184 municipaliteis

use "$dta\dados_originais_`state'_dentro_fora.dta",replace

*Merge with floods
merge m:m id_municipio_bcb week using "$dta\disaster_weekly_flood.dta"
keep if week >= wofd(mdy(11, 1, 2020)) & week <= wofd(mdy(1, 1, 2022))
tab _merge
keep if _merge == 3
drop _merge
sort id_municipio week
order id_municipio
*** Pix de Dentro para Dentro 

use "$dta\dados_originais_`state'_dentro_dentro.dta",replace

*** Pix de Fora para dentro
**** !!!!!!!!!!! Seems to have something wron since it was supposed to be only 184 municipaliteis

use "$dta\dados_originais_`state'_fora_dentro.dta",replace

*** Primeiro Pix recebido!

use "$dta\dados_originais_`state'_first_pix_received.dta",replace
*** Primeiro Pix mandado!

use "$dta\dados_originais_`state'_first_pix_paid.dta",replace




log close

