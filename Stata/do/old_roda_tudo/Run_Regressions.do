
*****
* Run_Regressions
*****
* Esse codigo importa dados ja limpos de PIX e execulta regressoes 
* Possui somente de quem:
*     -  teve aux emergencial em abril 21 e 
*     -  está nos 5000 menores municipios 
*	  -  está no sample de 700k endereços aleatoriamente selecionados
* Os dados de input são gerados pelo Monta_Base_Pix_auxilio.do
* Input:  "$dta\Pix_individuo_aux_abr21.dta"
* Output: "$dta\Pix_PF_adoption.dta"
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
log using "$log\Run_Regressions.log", replace 

use "$dta\Pix_PF_adoption_dist_sample.dta", replace 


* Run regressions

*Equation: after_adoption_{i,t} = alpha_{i} + alpha_{t} + beta_0 dist_caixa_{i} + beta_1 expected_distance_{i} + beta_2 after_event_{t}
*								  + beta_3 (dist_caixa_{i} * after_event_{t}) + beta_4 (expected_distance_{i} * after_event_{t})


		  
		  
*Equation: after_adoption_{i,t} = alpha_{i} + alpha_{t} + beta_0 dist_caixa_{i} + beta_1 expected_distance_{i} + beta_2 after_event_{t}
*								  + beta_3 (dist_caixa_{i} * after_event_{t}) + beta_4 (expected_distance_{i} * after_event_{t})



* Export tables






log close