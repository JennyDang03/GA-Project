******************************************
* Os dados de input são gerados pelo CartaoMuniDia.R
*Importa_Cartao_Muni.do
* Input: 
*   1) "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\CartaoDebitoMuni_`year'.csv"
*   2) "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\CartaoCreditoMuni_`year'.csv"
*	3) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\cod_mun.dta"

* Output:
*   1) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\Cartao_week_muni.dta" (week x muni)
*   2) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\Cartao_week_muni_TP.dta" (week x muni x tipo pessoa)

* Variables: muni_cd week valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito
*				tipo_pessoa

* The goal: * Importa dados de cartão de debito e credito por municipio, tipo de pessoa e dia
* Agrupa por semana e grava

* To do:
 
******************************************
	
* Paths and clears 
clear all
set more off, permanently 
set matsize 2000
set emptycells drop

global log "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\log\"
global dta "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\"
global output "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Output\"
global origdata "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\OrigData\"

* ADO
 adopath ++ "//sbcdf060/depep01$/ADO"
 adopath ++ "//sbcdf060/depep01$/ado-776e"

capture log close
log using "$log\Importa_Cartao_Muni_Dia.log", replace 

************* CARTAO DE DEBTIO ***************

forvalues year = 2018/2022{
	
	import delimited "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\CartaoDebitoMuni_`year'.csv", clear 
	unique deb_cd_tp_pessoa_pontovenda deb_mun_ibge_cd_pontovenda deb_dt_dt_pgto
	replace deb_dt_dt_pgto = subinstr(deb_dt_dt_pgto,"-","",.)
	gen dia = daily(deb_dt_dt_pgto,"YMD")
	format %td dia
	ren deb_mun_ibge_cd_pontovenda id_munic_7 
	tostring id_munic_7, replace
	merge m:1 id_munic_7 using "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\cod_mun.dta", nogenerate keep(3)
	keep deb_cd_tp_pessoa_pontovenda valor dia id_bcb qtd_ponto_venda
	if  ~(`year'==2018) {
			append using temp
		}	
	save temp, replace 
	
}

ren id_bcb muni_cd // codigo municipio do BCB
ren deb_cd_tp_pessoa_pontovenda tipo_pessoa
ren valor valor_cartao_debito
ren qtd_ponto_venda qtd_cli_cartao_debito

gen week = wofd(dia)
format %tw week

collapse (sum) valor_cartao_debito (mean) qtd_cli_cartao_debito, by(week muni_cd tipo_pessoa)

unique muni_cd tipo_pessoa week
save "$dta\Cartao_week_muni_TP", replace 
erase temp.dta

************* CARTAO DE CREDITO ***************
forvalues year = 2018/2022{
	
	import delimited "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\CartaoCreditoMuni_`year'.csv", clear 
	unique cre_cd_tp_pessoa_pontovenda cre_mun_ibge_cd_pontovenda cre_dt_dtpgto
	replace cre_dt_dtpgto = subinstr(cre_dt_dtpgto,"-","",.)
	gen dia = daily(cre_dt_dtpgto,"YMD")
	format %td dia
	ren cre_mun_ibge_cd_pontovenda id_munic_7
	tostring id_munic_7, replace
	merge m:1 id_munic_7 using "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\cod_mun.dta", nogenerate keep(3)
	keep cre_cd_tp_pessoa_pontovenda valor dia id_bcb qtd_ponto_venda
	if  ~(`year'==2018) {
			append using temp
		}	
	save temp, replace 
	
}

ren id_bcb muni_cd // codigo municipio do BCB
ren cre_cd_tp_pessoa_pontovenda tipo_pessoa
ren valor valor_cartao_credito
ren qtd_ponto_venda qtd_cli_cartao_credito


gen week = wofd(dia)
format %tw week

collapse (sum) valor_cartao_credito (mean) qtd_cli_cartao_credito, by(week muni_cd tipo_pessoa)
merge 1:1 muni_cd week tipo_pessoa using "$dta\Cartao_week_muni_TP", nogenerate  

replace qtd_cli_cartao_debito= 0 if qtd_cli_cartao_debito== .
replace qtd_cli_cartao_credito= 0 if qtd_cli_cartao_credito== .
replace valor_cartao_credito= 0 if valor_cartao_credito== .
replace valor_cartao_debito= 0 if valor_cartao_debito== .

destring muni_cd, replace force
drop if muni_cd == .
save "$dta\Cartao_week_muni_TP", replace 
erase temp.dta

***********************************
* Algumas estatisticas *
sum *
codebook
tab week

preserve 
	* maior parte é de PJ
	collapse (sum) valor_cartao_credito valor_cartao_debito, by(tipo_pessoa)
	list
restore 
***********************************

* Agrupa informação dono da maquininha é PF ou PJ
collapse (sum) valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito, by(muni_cd week)
save "$dta\Cartao_week_muni", replace 


log close


