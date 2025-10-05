
* Importa dados do PIX por Municipio por dia, considerando outflow do muni,
* inflow do muni e intra-municipio 
* Agrupa por semana e grava
* Input: 
*    - PIX_INTRAMUNIyearmonth.csv
*    - PIX_MUNI_Outflowyearmonth.csv
*    - PIX_MUNI_Outflowyearmonth.csv
* Output: PIX_week_muni.dta
* Código:
	* Repete para municipio do recebedor, do pagador, inflow, outlow e intra-municipio 
		* importa arquivos mensais csv vindo do Teradata 
			* Arquivos csv gerados pelos código em R: PIXMuniAggreg.R
		* Faz conversão de tipos de dados
		* Agrupa por semana
		* salva em arquivo dta
	
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
log using "$log\Importa_PIX_Muni.log", replace 

*** STEP 1 : PIX INTRA MUNICIPIO
cap drop temp

forvalues year = 2020/2022{
	local meses  01 02 03 04 05 06 07 08 09 10 11 12
	foreach m of local meses {
		if  ~(`year'==2022 & `m'>=7) & ~(`year'==2020 & `m'<=11) {
			dis `year' `m'
			import delimited "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\PIX_INTRAMUNI`year'`m'.csv", encoding(ISO-8859-2) clear 
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

collapse (sum) valor qtd n_cli_pag_pf n_cli_rec_pf n_cli_pag_pj n_cli_rec_pj, by(week mun_pag)

* Transforma em média diária 
replace n_cli_pag_pf = n_cli_pag_pf / 7
replace n_cli_rec_pf = n_cli_rec_pf / 7
replace n_cli_pag_pj = n_cli_pag_pj / 7
replace n_cli_rec_pj = n_cli_rec_pj / 7

rename mun_pag muni_cd
rename valor valor_intra
rename qtd qtd_intra
ren n_cli_pag_pf n_cli_pag_pf_intra
ren n_cli_rec_pf n_cli_rec_pf_intra
ren n_cli_pag_pj n_cli_pag_pj_intra
ren n_cli_rec_pj n_cli_rec_pj_intra 

*merge 1:1 muni_cd week using "$dta\PIX_week_muni", nogenerate  
save "$dta\PIX_week_muni", replace 
erase temp.dta

*** STEP 2 : PIX OUTFLOW - DE DENTRO MUNICIPIO PARA FORA
cap drop temp

forvalues year = 2020/2022{
	local meses  01 02 03 04 05 06 07 08 09 10 11 12
	foreach m of local meses {
		if  ~(`year'==2022 & `m'>=7) & ~(`year'==2020 & `m'<=11) {
			import delimited "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\PIX_MUNI_Outflow`year'`m'.csv", encoding(ISO-8859-2) clear 
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

collapse (sum) valor qtd n_cli_*, by(week mun_pag)

* Transforma em média diária 
replace n_cli_pag_pf = n_cli_pag_pf / 7
replace n_cli_pag_pj = n_cli_pag_pj / 7

ren n_cli_pag_pf n_cli_pag_pf_outflow
ren n_cli_pag_pj n_cli_pag_pj_outflow
rename mun_pag muni_cd
rename valor valor_outflow
rename qtd qtd_outflow

merge 1:1 muni_cd week using "$dta\PIX_week_muni", nogenerate  
save "$dta\PIX_week_muni", replace 
erase temp.dta


*** STEP 3 : PIX INFLOW - DE FORA DO MUNICIPIO PARA DENTRO
cap drop temp

forvalues year = 2020/2022{
	local meses  01 02 03 04 05 06 07 08 09 10 11 12
	foreach m of local meses {
		if  ~(`year'==2022 & `m'>=7) & ~(`year'==2020 & `m'<=11) {
			import delimited "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\PIX_MUNI_Inflow`year'`m'.csv", encoding(ISO-8859-2) clear 
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

collapse (sum) valor qtd n_cli_*, by(week mun_rec)

* Transforma em média diária 
replace n_cli_rec_pf = n_cli_rec_pf / 7
replace n_cli_rec_pj = n_cli_rec_pj / 7

ren n_cli_rec_pj n_cli_rec_pj_inflow 
ren n_cli_rec_pf n_cli_rec_pf_inflow
rename mun_rec muni_cd
rename valor valor_inflow
rename qtd qtd_inflow

merge 1:1 muni_cd week using "$dta\PIX_week_muni", nogenerate  
save "$dta\PIX_week_muni", replace 
erase temp.dta

***********************************
codebook

sum *

tab week

log close
