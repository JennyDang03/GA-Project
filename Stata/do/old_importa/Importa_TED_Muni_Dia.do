******************************************
* Os dados de input são gerados pelo TED por Muni x dia.R
* Importa_TED_Muni_Dia.do
* Input: 
*   1) "\\sbcdf060\depep$\DEPEPCOPEF\TEDSlicer\results\TED_muni_dia_intra_`year'.csv"
*	2) "\\sbcdf060\depep$\DEPEPCOPEF\TEDSlicer\results\TED_muni_dia_intra_SITRAF_`year'.csv"
*	3) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\municipios.dta"

* Output:
*   1) "$dta\TED_week_intra_muni"
*	2) "$dta\TED_week_muni"

* Variables: week muni_cd tipo_pessoa_rec tipo_pessoa_pag valor_TED_intra  qtd_TED_intra
*			week muni_cd valor_TED_intra  qtd_TED_intra

* The goal: * Importa dados de TEDs por municipio recebedor/pagador, tipo de pessoa e dia
* Agrupa por semana e grava dta por muni x week x tipo pessoa  (sem intramuni)
* Agrupa para salvar por muni x week (com intramuni)
* Para o intra-municipio, por municipio x week x tipo pessoa recebedora e pagadora

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
log using "$log\Importa_TED_Muni_Dia.log", replace 

******************
*** INTRA MUNI ***
******************

************* TED - STR - Por INTRA-MUNI ***************
forvalues year = 2018/2022{
	
	import delimited "\\sbcdf060\depep$\DEPEPCOPEF\TEDSlicer\results\TED_muni_dia_intra_`year'.csv", encoding(ISO-8859-2) clear
	keep if (tipo_pessoa_pag == "J"| tipo_pessoa_pag == "F")  & (tipo_pessoa_rec == "J"| tipo_pessoa_rec == "F")	
	replace dia = substr(dia,1,10)
	replace dia = subinstr(dia,"-","",.)
	gen dia_stata = daily(dia,"YMD")
	format %td dia_stata
	drop dia
	rename totalpayment valor_TED_intra
	rename quantitypayment qtd_TED_intra
	if  ~(`year'==2018) {
			append using temp
		}	
	save temp, replace 
	
}

gen week = wofd(dia_stata)
format %tw week

tab tipo_pessoa_rec tipo_pessoa_pag
***** Agrega por Semana ****
collapse (sum) valor_TED_intra  qtd_TED_intra, by(week muni_cd tipo_pessoa_rec tipo_pessoa_pag)
gen STR = 1
save "$dta\TED_week_intra_muni", replace 

************* TED - SITRAF - Por INTRA-MUNI ***************

forvalues year = 2018/2022{
	
	import delimited "\\sbcdf060\depep$\DEPEPCOPEF\TEDSlicer\results\TED_muni_dia_intra_SITRAF_`year'.csv", encoding(ISO-8859-2) clear
	keep if (tipo_pessoa_pag == "J"| tipo_pessoa_pag == "F")  & (tipo_pessoa_rec == "J"| tipo_pessoa_rec == "F")	
	replace dia = substr(dia,1,10)
	replace dia = subinstr(dia,"-","",.)
	gen dia_stata = daily(dia,"YMD")
	format %td dia_stata
	drop dia
	rename totalpayment valor_TED_intra
	rename quantitypayment qtd_TED_intra
	if  ~(`year'==2018) {
			append using temp
		}	
	save temp, replace 
	
}

gen week = wofd(dia_stata)
format %tw week

tab tipo_pessoa_rec tipo_pessoa_pag
***** Agrega por Semana ****
collapse (sum) valor_TED_intra  qtd_TED_intra, by(week muni_cd tipo_pessoa_rec tipo_pessoa_pag)
gen SITRAF = 1
merge 1:1 week muni_cd tipo_pessoa_rec tipo_pessoa_pag using "$dta\TED_week_intra_muni", nogenerate  

***** Troca codigo do municipio ******
ren muni_cd MUN_CD
drop if MUN_CD == 0
merge m:1 MUN_CD using "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\municipios.dta"
keep if _merge == 1 | _merge == 3
drop _merge
drop MUN_CD SPA_CD PAI_CD MUN_NM MUN_NM_NAO_FORMATADO MUN_CD_UNICAD MUN_IB_MUNICIPIO_BRASIL MUN_CD_RFB MUN_CD_IBGE MUN_CD_ECT MUN_DT_CRIACAO MUN_DT_INSTALACAO MUN_DT_EXTINCAO MUN_IB_VIGENTE MUN_DH_ATUALIZACAO MUN_NU_LATITUDE MUN_NU_LONGITUDE MUN_IB_CAPITAL_SUB_BRASIL
ren  MUN_CD_CADMU muni_cd
*************************************

* Dados separados de SITRAF E STR
save "$dta\TED_week_intra_muni", replace 

*** Agrega SITRAF E STR e cria arquivo por week x Municipio
collapse (sum) valor_TED_intra  qtd_TED_intra, by(week muni_cd)
save "$dta\TED_week_muni", replace 

log close

