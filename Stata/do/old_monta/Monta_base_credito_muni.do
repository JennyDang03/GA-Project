******************************************
* Monta_base_credito_muni.do
* Input: 
*   1) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\Credito_muni_mes_PF.dta"
*   2) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\Credito_muni_mes_PJ.dta"

* Output:
*   1) "$dta\Base_credito_muni.dta"

* Variables: ano_mes codmun_ibge
*			 qtd_cli_total qtd_cli_total_PF qtd_cli_total_PJ
*			 vol_credito_total vol_credito_total_PF vol_credito_total_PJ
* 			 vol_emprestimo_pessoal qtd_cli_emp_pessoal
* 			 vol_cartao qtd_cli_cartao
* 			 + other variables not so important

* The goal: It just merges together Credito_muni_mes_PF with Credito_muni_mes_PJ.

* To do: 

******************************************

clear all
set more off, permanently 
set matsize 2000
set emptycells drop

global log "\\sbcdf176\PIX_Matheus$\Stata\log"
global dta "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\"
global output "\\sbcdf176\PIX_Matheus$\Output"
global origdata "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\OrigData\"


* ADO
 adopath ++ "//sbcdf060/depep01$/ADO"
 adopath ++ "//sbcdf060/depep01$/ado-776e"

capture log close
log using "$log\Monta_base_credito.log", replace 

*** Começa carregando base do SCR
use "$dta\Credito_muni_mes_PF", clear
* Adiciona PJ
merge 1:1 ano_mes codmun_ibge using  "$dta\Credito_muni_mes_PJ.dta", keep(3) nogenerate

gen qtd_cli_total  = qtd_cli_total_PF + qtd_cli_total_PJ
gen vol_credito_total = vol_credito_total_PF + vol_credito_total_PJ

gen post_pix=date_ym>ym(2020,11)
*gen log_pib_2019 = ln(pib_2019)

save "\\sbcdf176\PIX_Matheus$\Stata\dta\Base_credito_muni.dta", replace 

sum *

log close




