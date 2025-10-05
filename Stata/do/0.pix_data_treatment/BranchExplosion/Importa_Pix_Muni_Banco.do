
* Importa dados do PIX por Municipio x Banco x Tipo Pessoa x dia, considerando pagador e recebedor 
* Agrupa por semana
* Grava dados do PIX por Municipio x Banco x Tipo Pessoa x semana
* Input: 
*    - PIX_MUNI_IF_yearmonth.csv
*    - PIX_MUNI_IF_REC_yearmonth.csv
*    - PIX_INTRA_IF_PAG_yearmonth.csv
*    - PIX_INTRA_IF_REC_yearmonth.csv
* Output:
*     PIX_week_muni_banco.dta
* Código:
	* Repete para municipio do recebedor e do pagador, geral e intra municipio 
		* importa arquivos mensais csv vindo do Teradata 
			* Arquivos csv gerados pelos código em R: PIXMuniBanco.R
		* Faz conversão de tipos de dados
		* Agrupa por semana
		* salva em arquivo dta (PIX_week_muni_banco.dta)
	* Troca codigo do municipio
	
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
log using "$log\Importa_PIX_Muni_Banco2.log", replace 


*** STEP 1 : PIX POR MUNICIPIO x BANCO x TIPO DE PESSOA DO PAGADOR
cap drop temp

forvalues year = 2020/2022{
	local meses  01 02 03 04 05 06 07 08 09 10 11 12
	foreach m of local meses {
	    if  ~(`year'==2022 & `m'>=7) & ~(`year'==2020 & `m'<=11) {
			dis `year' `m'
			import delimited "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\PIX_MUNI_IF_`year'`m'.csv", encoding(ISO-8859-2) clear 
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

collapse (sum) valor qtd, by(week mun_pag if_pag tipo_pag)

rename mun_pag muni_cd
rename valor valor_pag
rename qtd qtd_pag
rename if_pag IF
rename tipo_pag tipo

save "$dta\PIX_week_muni_banco", replace 
erase temp.dta

*** STEP 2 : PIX POR MUNICIPIO x BANCO x TIPO DE PESSOA DO RECEBEDOR
cap drop temp

forvalues year = 2020/2022{
	local meses  01 02 03 04 05 06 07 08 09 10 11 12
	foreach m of local meses {
	    if  ~(`year'==2022 & `m'>=7) & ~(`year'==2020 & `m'<=11) {
			dis `year' `m'
		import delimited "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\PIX_MUNI_IF_REC_`year'`m'.csv", encoding(ISO-8859-2) clear 
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

collapse (sum) valor qtd, by(week mun_rec if_rec tipo_rec)

rename mun_rec muni_cd
rename valor valor_rec
rename qtd qtd_rec
rename if_rec IF
rename tipo_rec tipo

merge 1:1 muni_cd IF tipo week  using "$dta\PIX_week_muni_banco", nogenerate  
save "$dta\PIX_week_muni_banco", replace 
erase temp.dta

**************************************************************************
*** STEP 3 : PIX INTRA MUNICIPIO x BANCO x TIPO DE PESSOA DO PAGADOR 
**************************************************************************
cap drop temp

forvalues year = 2020/2022{
	local meses  01 02 03 04 05 06 07 08 09 10 11 12
	foreach m of local meses {
	    if  ~(`year'==2022 & `m'>=7) & ~(`year'==2020 & `m'<=11) {
			dis `year' `m'
		import delimited "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\PIX_INTRA_IF_PAG`year'`m'.csv", encoding(ISO-8859-2) clear 
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

collapse (sum) valor qtd, by(week mun_pag if_pag tipo_pag)

rename mun_pag muni_cd
rename valor valor_intra_pag
rename qtd qtd_intra_pag
rename if_pag IF
rename tipo_pag tipo

merge 1:1 muni_cd IF tipo week  using "$dta\PIX_week_muni_banco", nogenerate  
save "$dta\PIX_week_muni_banco", replace 
erase temp.dta


**************************************************************************
*** STEP 4 : PIX INTRA MUNICIPIO x BANCO x TIPO DE PESSOA DO RECEBEDOR
**************************************************************************
cap drop temp

forvalues year = 2020/2022{
	local meses  01 02 03 04 05 06 07 08 09 10 11 12
	foreach m of local meses {
	    if  ~(`year'==2022 & `m'>=7) & ~(`year'==2020 & `m'<=11) {
			dis `year' `m'
		import delimited "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\PIX_INTRA_IF_REC`year'`m'.csv", encoding(ISO-8859-2) clear 
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

collapse (sum) valor qtd, by(week mun_rec if_rec tipo_rec)

rename mun_rec muni_cd
rename valor valor_intra_rec
rename qtd qtd_intra_rec
rename if_rec IF
rename tipo_rec tipo

merge 1:1 muni_cd IF tipo week  using "$dta\PIX_week_muni_banco", nogenerate  
save "$dta\PIX_week_muni_banco", replace 
erase temp.dta


***********************************
codebook

sum *

tab week
***********************************

log close
