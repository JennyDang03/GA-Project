* So da uma ajeitada na Base_CCS_aberturas_muni_banco.dta e  "D:\PIX_Matheus\Stata\dta\Base_CCS_estoque_muni_banco.dta"
* Limpa e une as duas. 
* Input:
* - Base_CCS_aberturas_muni_banco.dta
* - "D:\PIX_Matheus\Stata\dta\Base_CCS_estoque_muni_banco.dta"
* Outputs:
* - "D:\PIX_Matheus\Stata\dta\Base_CCS_muni_banco_clean.dta"




clear all
set more off, permanently 
set matsize 2000
set emptycells drop


global log "D:\PIX_Matheus\Stata\log"
global dta "D:\PIX_Matheus\Stata\dta"
global output "D:\PIX_Matheus\Output"
global origdata "D:\PIX_Matheus\DadosOriginais"

* ADO 
adopath ++ "D:\ADO"

use "D:\PIX_Matheus\Stata\dta\Base_CCS_aberturas_muni_banco.dta", replace
drop pib_2019-post_robbery_bank
drop controle - cong_id
drop id_mun_bank
drop if week < 3068 | week > 3249
order IF muni_cd
sort IF muni_cd week
save "D:\PIX_Matheus\Stata\dta\Base_CCS_aberturas_muni_banco_clean.dta", replace
*******************************************
use "D:\PIX_Matheus\Stata\dta\Base_CCS_estoque_muni_banco.dta"
drop pib_2019-post_robbery_bank
drop controle - cong_id
drop MUN_CD_IBGE
drop id_mun_bank
drop if week < 3068 | week > 3249
order IF muni_cd week
sort IF muni_cd week
save "D:\PIX_Matheus\Stata\dta\Base_CCS_estoque_muni_banco_clean.dta", replace
********************************************
merge 1:1 IF muni_cd week using "D:\PIX_Matheus\Stata\dta\Base_CCS_aberturas_muni_banco_clean.dta"
save "D:\PIX_Matheus\Stata\dta\Base_CCS_muni_banco_clean.dta", replace