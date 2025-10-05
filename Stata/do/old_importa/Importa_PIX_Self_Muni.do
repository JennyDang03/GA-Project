******************************************
* Importa_PIX_Self_Muni.do
* Input: 
*   1) "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\PIX_Muni_Self`year'`m'.csv"

* Output:
*   1) "$dta\PIX_week_muni_self.dta"

* Variables: mun_cd week valor_self_pf valor_self_pj qtd_self_pf qtd_self_pj n_cli_self_pf n_cli_self_pj

* The goal: Importa dados de PIX para si mesmo por Municipio por dia
* 			Agrupa por semana e grava
* 			Repete para municipio do recebedor, do pagador, inflow, outlow e intra-municipio 
* 			importa arquivos mensais csv vindo do Teradata 
* 			Arquivos csv gerados pelos código em R: PIXMuniAggreg.R
* 			Faz conversão de tipos de dados
* 			Agrupa por semana
* 			salva em arquivo dta

* To do: In this file it is possible to aggregate everything by month!

******************************************

clear all
set more off, permanently 
set matsize 2000
set emptycells drop

global log "\\sbcdf176\PIX_Matheus$\Stata\log"
global dta "\\sbcdf176\PIX_Matheus$\Stata\dta\"
global output "\\sbcdf176\PIX_Matheus$\Output"
global origdata "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\OrigData\"


* ADO
 adopath ++ "//sbcdf060/depep01$/ADO"
 adopath ++ "//sbcdf060/depep01$/ado-776e"

capture log close
log using "$log\Importa_PIX_Self_Muni.log", replace 

import delimited "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\PIX_Muni_Self202011.csv", clear

*** STEP 1 : PIX INTRA MUNICIPIO
cap drop temp

forvalues year = 2020/2022{
	local meses  01 02 03 04 05 06 07 08 09 10 11 12
	foreach m of local meses {
		if  ~(`year'==2020 & `m'<=11) {
			dis `year' `m'
			import delimited "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\PIX_Muni_Self`year'`m'.csv", encoding(ISO-8859-2) clear 
			if  ~(`year'==2020 & `m'==12) {
				append using temp
			}	
		save temp, replace 
		}
	}  // fim loop dos meses
} // Fim loop dos anos 

replace dia = subinstr(dia,"-","",.)
gen dia_stata = daily(dia,"YMD")
format %td dia_stata

gen week = wofd(dia_stata)
format %tw week

collapse (sum) valor_self qtd_self n_cli_self, by(week mun_cd tipo_pessoa)

* Transforma em média diária 
replace n_cli_self = n_cli_self / 7

gen valor_self_pf = valor_self if tipo_pessoa == 1
gen valor_self_pj = valor_self if tipo_pessoa == 2
gen qtd_self_pf = qtd_self if tipo_pessoa == 1
gen qtd_self_pj = qtd_self if tipo_pessoa == 2
gen n_cli_self_pf = n_cli_self if tipo_pessoa == 1
gen n_cli_self_pj = n_cli_self if tipo_pessoa == 2

collapse (sum) valor_self_pf valor_self_pj qtd_self_pf qtd_self_pj  n_cli_self_pf n_cli_self_pj, by(week mun_cd)


save "$dta\PIX_week_muni_self.dta", replace 

log close











