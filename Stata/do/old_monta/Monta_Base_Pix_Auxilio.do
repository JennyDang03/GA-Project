* Esse codigo junta base com uso de Pix para os 5000 menores municipios com 
* com o índice do endereço, e filtra para apenas usuarios do auxilio emergencial de abril-2021 
* Os dados de input são gerados por Importa_Pix_individuo.do, Gera_id_addres_index_match.do
* Input: 
*   1) Pix_individuo.dta    (todo mundo dos 5000 menores municipios) 
*   2) id_index_address.dta (possui apenas usuarios do auxilio emergencial abril-2021)

* Outputs:
*		id_aux_abril21_sem_pix.dta
* 	 	Pix_individuo_aux_abr21.dta"

clear all
set more off, permanently 

set emptycells drop

global log "\\sbcdf176\PIX_Matheus$\Stata\log"
global dta "\\sbcdf176\PIX_Matheus$\Stata\dta"
global output "\\sbcdf176\PIX_Matheus$\Output"
global origdata "\\sbcdf176\PIX_Matheus$\DadosOriginais"

* ADO 
adopath ++ "D:\ADO"
 adopath ++ "//sbcdf060/depep01$/ADO"
 adopath ++ "//sbcdf060/depep01$/ado-776e"

capture log close
log using "$log\Monta_Base_Pix_Auxilio.log", replace 

* Abre base do uso do Pix somente para PF
use "$dta\Pix_individuo.dta" if tipo_pessoa == 1
* Pega os dados do índice de endereços
merge m:1 id using "$dta\id_index_address.dta"
* elimina quem não está na base dos endereços, i.e., quem não tem auxilio em 04-2021 
drop if _merge == 1

* Cria base de quem recebeu auxilio mas não fez Pix 
preserve
	keep if _merge == 2 // aprox 263k CPFs
	drop  tipo_pessoa muni_cd time_id value_rec trans_rec value_sent trans_sent _merge index0
	save "$dta\id_aux_abril21_sem_pix.dta", replace
restore 

* Esse codigo nao esta adicionando muita gente de municipios grandes que nunca fizeram pix? A gente so usa os 5000 menores municipios. 

* elimina quem recebeu auxilio mas não fez Pix 
drop if _merge == 2
drop _merge

* Salva base de uso do Pix para quem recebeu auxilio emergencial em 04-2021 e mora nos 5k menores municipios 
save "$dta\Pix_individuo_aux_abr21.dta", replace

log close 

