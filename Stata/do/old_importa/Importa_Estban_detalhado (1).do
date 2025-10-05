* Importa dados do Estban com mais detalhes que os públicos  
* Input:  ESTBAN_DETA.csv
* Output: Estban_detalhado.dta
	
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
log using "$log\Importa_Estban_detalhado.log", replace 

*** Pega dados de 2018 a 2021
cap drop temp
import delimited "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\ESTBAN_DETA.csv", encoding(ISO-8859-2) clear

ren mun_cd_ibge codmun_ibge 
ren mun_cd_cadmu muni_cd

collapse (sum) sdag_vlr_sdo, by(cnpj data_base conta_id codmun_ibge muni_cd)
reshape wide sdag_vlr_sdo, i(cnpj data_base codmun_ibge muni_cd) j(conta_id)

foreach var of varlist sdag_vlr_sdo*{
	replace `var'=0 if missing(`var')
}

ren sdag_vlr_sdo111 caixa
ren sdag_vlr_sdo160 lending
ren sdag_vlr_sdo169 imobiliario
ren sdag_vlr_sdo411 dep_vista_PF
ren sdag_vlr_sdo412 dep_vista_PJ
ren sdag_vlr_sdo420 poupanca
ren sdag_vlr_sdo432 dep_prazo

tostring codmun_ibge, replace 

save "$dta\Estban_detalhado.dta", replace 

***********************************
codebook
sum *

log close


