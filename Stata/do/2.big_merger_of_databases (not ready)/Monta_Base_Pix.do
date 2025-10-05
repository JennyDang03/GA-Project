****** Monta base juntando os vários dtas ******
* 1) Abre arquivo Pix por transacao 
* 2) Adiciona info de cadastro das IF/IP
* 3) adiciona dados do municipio
* Etc....

clear all
set more off, permanently 
set matsize 2000
set emptycells drop

global log "D:\PIX_Matheus\Stata\log"
global dta "D:\PIX_Matheus\Stata\dta"
global output "D:\PIX_Matheus\Output"
global origdata "D:\PIX_Matheus\DadosOriginais"

* ADO
adopath ++ "D:\ado"

capture log close
log using "$log\Monta_Base_Pix.log", replace 

*** 1) Abre arquivo Pix por transacao ***
*use "$dta\dados_originais_ceara.dta", clear
use "$dta\dados_originais_ceara_sample.dta", clear

*************************************************
*** 2) Adiciona info de cadastro das IF/IP ******
*************************************************
*** 2.1) IF ou IP recebedora
rename if_rec cnpj8_if
merge m:1 cnpj8_if using "$dta\Cadastro_IF.dta", nogenerate keep(3)
rename cnpj8_if if_rec
rename number_branches if_rec_n_branches
rename controle if_rec_controle
rename macroseg_if if_rec_macroseg_if
rename belong_cong if_rec_belong_cong
drop porte_cong_prud macroseg_cong_prud cong_id
*** 2.2) IF ou IP pagadora
rename if_pag cnpj8_if
merge m:1 cnpj8_if using "$dta\Cadastro_IF.dta", nogenerate keep(3)
rename cnpj8_if if_pag
rename number_branches if_pag_n_branches
rename controle if_pag_controle
rename macroseg_if if_pag_macroseg_if
rename belong_cong if_pag_belong_cong
drop porte_cong_prud macroseg_cong_prud cong_id

*** 3) adiciona dados do municipio


