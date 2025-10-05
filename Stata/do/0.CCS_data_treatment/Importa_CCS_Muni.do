
* Importa dados do estoque de contas abertas CCS
* por Municipio x Banco x Tipo Pessoa x semana
* Agrupa por semana x municipio
* Grava dados do CCS por Municipio x semana
* Input: 
*    - CCS_Muni_IF_PJ_estoquei.csv
* Output:
*     CCS_estoque_week_muni.dta
* Código:
		* importa arquivos mensais csv vindo do Teradata 
			* Arquivos csv gerados pelos código em R: CCS_Muni_IF_estoque.R
		* Faz conversão de tipos de dados
		* Agrupa por muni x semana
		* salva em arquivo dta (CCS_estoque_week_muni.dta)
	
* Paths and clears 
clear all
set more off, permanently 
set matsize 2000
set emptycells drop

global log "D:\PIX_Matheus\Stata\log"
global dta "D:\PIX_Matheus\Stata\dta"
global output "D:\PIX_Matheus\Output"

* ADO
 adopath ++ "//sbcdf060/depep01$/ADO"
 adopath ++ "//sbcdf060/depep01$/ado-776e"

capture log close
log using "$log\Importa_CCS_Muni.log", replace 

*** STEP 1 : Importa Estoque de contas abertas de PF por MUNICIPIO x BANCO x SEMANA
cap drop temp

*ini_date = (date("01/01/2018","DMY")
* i = 1 é o dia 1/1/2018, e depois vai andando de 7 em 7 dias

forvalues i = 1/239{
			dis `i'
			import delimited "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\CCS_Muni_IF_PF_estoque`i'.csv", encoding(ISO-8859-2) clear 
			gen dia = 21185 + (`i' - 1) * 7
			if  ~(`i'==1) {
				append using "D:\TEMPSTATA\temp.dta"
			}	

			save "D:\TEMPSTATA\temp.dta", replace 
} // Fim loop da variável i

format %td dia
gen week = wofd(dia)
format %tw week

rename instituicao IF
ren qtd qtd_contas_PF

drop if muni_cd == .
drop if muni_cd <0

egen id_mun_bank =group(IF muni_cd)
duplicates drop id_mun_bank week, force
drop dia id_mun_bank

replace qtd_contas_PF = 0 if qtd_contas_PF == .

collapse (sum) qtd_contas_PF , by(muni_cd week)

save "$dta\CCS_estoque_week_muni", replace 
*erase temp.dta

***********************************
codebook

sum *

tab week
***********************************

log close
