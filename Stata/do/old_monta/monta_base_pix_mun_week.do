
* Os dados de input são gerados pelo "$do\Importa_Pix_individuo.do" e por ______________ flood monthly

******************************************
*monta_base_pix_mun_week.do
* Input: 
*   1) "$dta\Base_week_muni.dta"
*   2) "$dta\flood_weekly_2020_2022.dta"
*	3) "$dta\flood_weekly_2019_2020.dta"

* Output:
*   1) "$dta\Base_week_muni_flood.dta"
*   2) "$dta\Base_week_muni_flood_beforePIX.dta"

* Variables: muni_cd week valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito valor_TED_intra qtd_TED_intra qtd_cli_TED_rec_PJ qtd_cli_TED_rec_PF qtd_cli_TED_pag_PJ qtd_cli_TED_pag_PF valor_boleto_deb valor_boleto_dinheiro qtd_boleto_deb qtd_boleto_dinheiro valor_boleto valor_boleto_eletronico valor_boleto_ATM valor_boleto_age valor_boleto_corban qtd_boleto qtd_boleto_eletronico qtd_boleto_ATM qtd_boleto_age qtd_boleto_corban qtd_cli_pag_pf_boleto qtd_cli_pag_pj_boleto qtd_cli_rec_pj_boleto valor_PIX_inflow qtd_PIX_inflow n_cli_rec_pf_inflow n_cli_rec_pj_inflow valor_PIX_outflow qtd_PIX_outflow n_cli_pag_pf_outflow n_cli_pag_pj_outflow valor_PIX_intra qtd_PIX_intra n_cli_pag_pf_intra n_cli_rec_pf_intra n_cli_pag_pj_intra n_cli_rec_pj_intra MUN_CD_IBGE codmun_ibge MUN_NU_LATITUDE MUN_NU_LONGITUDE post_pix
* + Log variants: log_var

* The goal: prepare Base_week_muni to receive flood variables.
* 			Then run flood_SA_muni_v1.R

* To do: 

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

capture log close
log using "$log\monta_base_pix_mun_week.log", replace 


********************************************************************************
* Weekly at the Municipal level. 
********************************************************************************
************
* After Pix
************
*use "$dta\Base_week_muni_fake.dta", replace
use "$dta\Base_week_muni.dta", replace

keep if week >= wofd(mdy(11, 16, 2020)) & week <= wofd(mdy(12, 31, 2022))

keep if week <= yw(2022,26) // note that after that everything is 0 for pix

* Cria painel balanceado
sort muni_cd week
tsset muni_cd week
tsfill, full // Not necessary because it is strongly balanced
sum *

*If not strogly balanced, you need to substitute missing by zeros

*Create some new variables:
	* Log valor_cartao_credito - qtd_boleto 
foreach var of varlist valor_cartao_credito - n_cli_rec_pj_intra {
	cap drop log_`var'
	gen log_`var'=log(`var' + 1)
}

* Label them
* Best variables: 
* valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito 
* valor_TED_intra qtd_TED_intra qtd_cli_TED_rec_PJ qtd_cli_TED_rec_PF qtd_cli_TED_pag_PJ qtd_cli_TED_pag_PF 
* valor_boleto qtd_boleto qtd_cli_pag_pf_boleto qtd_cli_pag_pj_boleto qtd_cli_rec_pj_boleto 
* valor_PIX_inflow qtd_PIX_inflow valor_PIX_outflow qtd_PIX_outflow valor_PIX_intra qtd_PIX_intra n_cli_pag_pf_intra n_cli_rec_pf_intra n_cli_pag_pj_intra n_cli_rec_pj_intra
*****

merge m:1 muni_cd week using "$dta\flood_weekly_2020_2022.dta", keep(3) keepusing(date_flood)
drop _merge

* Save
*save "$dta\flood_pix_weekly_fake.dta", replace
save "$dta\Base_week_muni_flood.dta", replace

************
* Before Pix
************
*use "$dta\Base_week_muni_fake.dta", replace
use "$dta\Base_week_muni.dta", replace

keep if week < wofd(mdy(11, 16, 2020)) & week >= wofd(mdy(1, 1, 2018))

* Cria painel balanceado
sort muni_cd week
tsset muni_cd week
tsfill, full // Not necessary because it is strongly balanced
sum *

*Create some new variables:
	* Log valor_cartao_credito - qtd_boleto 
foreach var of varlist valor_cartao_credito - n_cli_rec_pj_intra {
	cap drop log_`var'
	gen log_`var'=log(`var' + 1)
}

* Label them
* Best variables: 
* valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito 
* valor_TED_intra qtd_TED_intra qtd_cli_TED_rec_PJ qtd_cli_TED_rec_PF qtd_cli_TED_pag_PJ qtd_cli_TED_pag_PF 
* valor_boleto qtd_boleto qtd_cli_pag_pf_boleto qtd_cli_pag_pj_boleto qtd_cli_rec_pj_boleto 
* valor_PIX_inflow qtd_PIX_inflow valor_PIX_outflow qtd_PIX_outflow valor_PIX_intra qtd_PIX_intra n_cli_pag_pf_intra n_cli_rec_pf_intra n_cli_pag_pj_intra n_cli_rec_pj_intra
*****

merge m:1 muni_cd week using "$dta\flood_weekly_2018_2020.dta", keep(3) keepusing(date_flood)
drop _merge

* Save
*save "$dta\flood_pix_weekly_fake.dta", replace
save "$dta\Base_week_muni_flood_beforePIX.dta", replace



log close