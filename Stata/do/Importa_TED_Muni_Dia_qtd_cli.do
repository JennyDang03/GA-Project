******************************************
* ERROR HERE. if one of them are missing, the sum is missing. 
*** Força zero nos nulos - before sum
* Also, there are no results for PF because the sql is wrong. 


* Os dados de input são gerados pelo TED por Muni x dia.R
*Importa_TED_Muni_Dia_qtd_cli.do
* Input: 
*   1) "\\sbcdf060\depep$\DEPEPCOPEF\TEDSlicer\results\TED_muni_dia_QTDCLI_PAG_STR`year'.csv"
*	2) "\\sbcdf060\depep$\DEPEPCOPEF\TEDSlicer\results\TED_muni_dia_QTDCLI_REC_STR`year'.csv"
*	3) "\\sbcdf060\depep$\DEPEPCOPEF\TEDSlicer\results\TED_muni_dia_QTDCLI_PAG_SITRAF`year'.csv"
*	4) "\\sbcdf060\depep$\DEPEPCOPEF\TEDSlicer\results\TED_muni_dia_QTDCLI_REC_SITRAF`year'.csv"
*	5) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\municipios.dta"

* Output:
*   1) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\TED_week_muni_QTD_CLI"

* Variables: week muni_cd qtd_cli_TED_rec_PJ qtd_cli_TED_rec_PF qtd_cli_TED_pag_PJ qtd_cli_TED_pag_PF

* The goal: Repete para cada ano e para recebedor e pagador, importa arquivos mensais csv vindo do Teradata, Arquivos csv gerados pelos código em R: TED por Muni x dia.R, Faz conversão de tipos de dados, Agrupa por semana, salva em arquivo dta, junto pagador e recebedor, e depois salva 

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
log using "$log\Importa_TED_Muni_Dia_qtd_cli.log", replace 

******************* STR **************************
************* TED - STR - PAGADOR ***************
forvalues year = 2018/2022{
	
	import delimited "\\sbcdf060\depep$\DEPEPCOPEF\TEDSlicer\results\TED_muni_dia_QTDCLI_PAG_STR`year'.csv", encoding(ISO-8859-2) clear
	keep if (tipo_pessoa_pag == "J"| tipo_pessoa_pag == "F") 
	replace dia = substr(dia,1,10)
	replace dia = subinstr(dia,"-","",.)
	gen dia_stata = daily(dia,"YMD")
	format %td dia_stata
	drop dia
	rename qtd_cli_pag qtd_cli_TED_pag
	if  ~(`year'==2018) {
			append using temp
		}	
	save temp, replace 
	
}

gen week = wofd(dia_stata)
format %tw week

ren tipo_pessoa_pag tipo_pessoa
***** Agrega por Semana ****
collapse (mean) qtd_cli_TED_pag  , by(week muni_cd tipo_pessoa)
gen qtd_cli_TED_pag_PF = qtd_cli_TED_pag if tipo_pessoa == "F"
gen qtd_cli_TED_pag_PJ = qtd_cli_TED_pag if tipo_pessoa == "J"
drop qtd_cli_TED_pag

collapse (mean) qtd_cli_TED_pag_PJ  qtd_cli_TED_pag_PF , by(week muni_cd)
rename qtd_cli_TED_pag_PJ qtd_cli_TED_pag_PJ_STR
rename qtd_cli_TED_pag_PF qtd_cli_TED_pag_PF_STR
save "$dta\TED_week_muni_QTD_CLI", replace 

******* TED - STR - RECEBEDOR **************
forvalues year = 2018/2022{
	
	import delimited "\\sbcdf060\depep$\DEPEPCOPEF\TEDSlicer\results\TED_muni_dia_QTDCLI_REC_STR`year'.csv", encoding(ISO-8859-2) clear
	keep if (tipo_pessoa_rec == "J"| tipo_pessoa_rec == "F") 
	replace dia = substr(dia,1,10)
	replace dia = subinstr(dia,"-","",.)
	gen dia_stata = daily(dia,"YMD")
	format %td dia_stata
	drop dia
	rename qtd_cli_rec qtd_cli_TED_rec
	if  ~(`year'==2018) {
			append using temp
		}	
	save temp, replace 
	
}

gen week = wofd(dia_stata)
format %tw week

ren tipo_pessoa_rec tipo_pessoa
***** Agrega por Semana ****
collapse (mean) qtd_cli_TED_rec  , by(week muni_cd tipo_pessoa)
gen qtd_cli_TED_rec_PF = qtd_cli_TED_rec if tipo_pessoa == "F"
gen qtd_cli_TED_rec_PJ = qtd_cli_TED_rec if tipo_pessoa == "J"
drop qtd_cli_TED_rec

collapse (mean) qtd_cli_TED_rec_PJ  qtd_cli_TED_rec_PF , by(week muni_cd)
rename qtd_cli_TED_rec_PJ qtd_cli_TED_rec_PJ_STR
rename qtd_cli_TED_rec_PF qtd_cli_TED_rec_PF_STR

* Faz merge com pagadores 
merge 1:1 week muni_cd using "$dta\TED_week_muni_QTD_CLI", nogenerate  
save "$dta\TED_week_muni_QTD_CLI", replace 

******************* SITRAF **************************
************* TED - SITRAF - PAGADOR ***************
forvalues year = 2018/2022{
	
	import delimited "\\sbcdf060\depep$\DEPEPCOPEF\TEDSlicer\results\TED_muni_dia_QTDCLI_PAG_SITRAF`year'.csv", encoding(ISO-8859-2) clear
	keep if (tipo_pessoa_pag == "J"| tipo_pessoa_pag == "F") 
	replace dia = substr(dia,1,10)
	replace dia = subinstr(dia,"-","",.)
	gen dia_stata = daily(dia,"YMD")
	format %td dia_stata
	drop dia
	rename qtd_cli_pag qtd_cli_TED_pag
	if  ~(`year'==2018) {
			append using temp
		}	
	save temp, replace 
	
}

gen week = wofd(dia_stata)
format %tw week

ren tipo_pessoa_pag tipo_pessoa
***** Agrega por Semana ****
collapse (mean) qtd_cli_TED_pag  , by(week muni_cd tipo_pessoa)
gen qtd_cli_TED_pag_PF = qtd_cli_TED_pag if tipo_pessoa == "F"
gen qtd_cli_TED_pag_PJ = qtd_cli_TED_pag if tipo_pessoa == "J"
drop qtd_cli_TED_pag

collapse (mean) qtd_cli_TED_pag_PJ  qtd_cli_TED_pag_PF , by(week muni_cd)
rename qtd_cli_TED_pag_PJ qtd_cli_TED_pag_PJ_SITRAF
rename qtd_cli_TED_pag_PF qtd_cli_TED_pag_PF_SITRAF

merge 1:1 week muni_cd using "$dta\TED_week_muni_QTD_CLI", nogenerate  
save "$dta\TED_week_muni_QTD_CLI", replace 

******* TED - SITRAF - RECEBEDOR **************
forvalues year = 2018/2022{
	
	import delimited "\\sbcdf060\depep$\DEPEPCOPEF\TEDSlicer\results\TED_muni_dia_QTDCLI_REC_SITRAF`year'.csv", encoding(ISO-8859-2) clear
	keep if (tipo_pessoa_rec == "J"| tipo_pessoa_rec == "F") 
	replace dia = substr(dia,1,10)
	replace dia = subinstr(dia,"-","",.)
	gen dia_stata = daily(dia,"YMD")
	format %td dia_stata
	drop dia
	rename qtd_cli_rec qtd_cli_TED_rec
	if  ~(`year'==2018) {
			append using temp
		}	
	save temp, replace 
	
}

gen week = wofd(dia_stata)
format %tw week

ren tipo_pessoa_rec tipo_pessoa
***** Agrega por Semana ****
collapse (mean) qtd_cli_TED_rec  , by(week muni_cd tipo_pessoa)
gen qtd_cli_TED_rec_PF = qtd_cli_TED_rec if tipo_pessoa == "F"
gen qtd_cli_TED_rec_PJ = qtd_cli_TED_rec if tipo_pessoa == "J"
drop qtd_cli_TED_rec

collapse (mean) qtd_cli_TED_rec_PJ  qtd_cli_TED_rec_PF , by(week muni_cd)
rename qtd_cli_TED_rec_PJ qtd_cli_TED_rec_PJ_SITRAF
rename qtd_cli_TED_rec_PF qtd_cli_TED_rec_PF_SITRAF

* Faz merge com pagadores 
merge 1:1 week muni_cd using "$dta\TED_week_muni_QTD_CLI", nogenerate  

***** Troca codigo do municipio ******
ren muni_cd MUN_CD
drop if MUN_CD == 0
merge m:1 MUN_CD using "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\municipios.dta"
keep if _merge == 1 | _merge == 3
drop _merge
drop MUN_CD SPA_CD PAI_CD MUN_NM MUN_NM_NAO_FORMATADO MUN_CD_UNICAD MUN_IB_MUNICIPIO_BRASIL MUN_CD_RFB MUN_CD_IBGE MUN_CD_ECT MUN_DT_CRIACAO MUN_DT_INSTALACAO MUN_DT_EXTINCAO MUN_IB_VIGENTE MUN_DH_ATUALIZACAO MUN_NU_LATITUDE MUN_NU_LONGITUDE MUN_IB_CAPITAL_SUB_BRASIL
ren  MUN_CD_CADMU muni_cd
*************************************

* ERROR HERE. if one of them are missing, the sum is missing. 
*** Força zero nos nulos - before sum
foreach var in  qtd_cli_TED_rec_PJ_SITRAF qtd_cli_TED_rec_PF_SITRAF qtd_cli_TED_pag_PJ_SITRAF qtd_cli_TED_pag_PF_SITRAF qtd_cli_TED_rec_PJ_STR qtd_cli_TED_rec_PF_STR qtd_cli_TED_pag_PJ_STR qtd_cli_TED_pag_PF_STR {
	replace `var'  = 0 if  `var'  == .
}

gen qtd_cli_TED_rec_PJ = (qtd_cli_TED_rec_PJ_SITRAF + qtd_cli_TED_rec_PJ_STR) / 2
gen qtd_cli_TED_rec_PF = (qtd_cli_TED_rec_PF_SITRAF + qtd_cli_TED_rec_PF_STR) / 2 

gen qtd_cli_TED_pag_PJ = (qtd_cli_TED_pag_PJ_SITRAF + qtd_cli_TED_pag_PJ_STR) / 2
gen qtd_cli_TED_pag_PF = (qtd_cli_TED_pag_PF_SITRAF + qtd_cli_TED_pag_PF_STR) / 2 

drop qtd_cli_TED_rec_PJ_SITRAF qtd_cli_TED_rec_PF_SITRAF qtd_cli_TED_pag_PJ_SITRAF qtd_cli_TED_pag_PF_SITRAF qtd_cli_TED_rec_PJ_STR qtd_cli_TED_rec_PF_STR qtd_cli_TED_pag_PJ_STR qtd_cli_TED_pag_PF_STR

*** Força zero nos nulos 
foreach var in  qtd_cli_TED_rec_PJ qtd_cli_TED_rec_PF qtd_cli_TED_pag_PJ qtd_cli_TED_pag_PF  {

	replace `var'  = 0 if  `var'  == .

}

save "$dta\TED_week_muni_QTD_CLI", replace 

log close
