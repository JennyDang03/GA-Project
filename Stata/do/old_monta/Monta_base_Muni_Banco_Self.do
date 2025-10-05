******************************************
*Monta_base_Muni_Banco_self.do
* Input: 
*   1) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\PIX_week_muni_banco_self.dta"
*   2) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\Cadastro_IF.dta"

* Output:
*   1) "$dta\Base_muni_banco_self.dta"


* Variables: id_mun_bank_tipo IF muni_cd tipo week tipo_inst big_bank public digbank belong_cong macroseg_if_txt controle number_branches
*			valor_self_pag qtd_self_pag valor_self_rec qtd_self_rec 
			
* The goal: prepare PIX_week_muni_banco_self for a future analysis

* To do: it would need TED, Boleto for us to do a Before Pix analysis. 

******************************************

* Paths and clears 
clear all
set more off, permanently 
set matsize 2000
set emptycells drop

global log "\\sbcdf176\PIX_Matheus$\Stata\log"
global dta "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\"
global output "\\sbcdf176\PIX_Matheus$\Output"
global ori

* ADO
 adopath ++ "//sbcdf060/depep01$/ADO"
 adopath ++ "//sbcdf060/depep01$/ado-776e"

capture log close
log using "$log\Monta_base_Muni_Banco_self.log", replace 

use "$dta\PIX_week_muni_banco_self", clear

drop if muni_cd == .
drop if tipo == .

* Agrega por if_rec e depois por if_pag

*** AGREGA IF RECEBEDORA ***
collapse (sum) valor qtd, by(muni_cd tipo week if_rec)

ren if_rec IF 
ren valor valor_self_rec
ren qtd qtd_self_rec 

save "\\sbcdf176\PIX_Matheus$\Stata\dta\Base_muni_banco_self.dta", replace 

use "$dta\PIX_week_muni_banco_self", clear

drop if muni_cd == .
drop if tipo == .

collapse (sum) valor qtd, by(muni_cd tipo week if_pag)

ren if_pag IF 
ren valor valor_self_pag
ren qtd qtd_self_pag 

merge 1:1 muni_cd IF tipo week using "\\sbcdf176\PIX_Matheus$\Stata\dta\Base_muni_banco_self.dta"


**** Preenche painel para ficar balanceado
egen id_mun_bank_tipo =group(IF muni_cd tipo)
xtset id_mun_bank_tipo week, weekly
tsfill, full

egen muni_temp = min(muni_cd), by(id_mun_bank_tipo) // why max? I switched to min
egen IF_temp = min(IF), by(id_mun_bank_tipo) // why max? I switched to min
egen tipo_temp = min(tipo), by(id_mun_bank_tipo)

replace muni_cd = muni_temp if muni_cd == .
replace IF = IF_temp if if_rec == .
replace tipo = tipo_temp if tipo == .

drop muni_temp IF_temp tipo_temp

**********************************************
*****  Pega informações de cadastro das IF ***
**********************************************
rename IF cnpj8_if
merge m:1 cnpj8_if using "$dta\Cadastro_IF.dta"
keep if _merge == 3
drop _merge
rename cnpj8_if IF

tab macroseg_if
unique IF
drop segmento_unicad porte_cong_prud segmento_if macroseg_cong_prud cong_id

label drop cong_id
label drop _merge
label define type_inst_label 1 "Public Bank" 2 "Large Private Banks" 4 "Credit Unions" 5 "Digital Institutions" 6 "Others"
label values tipo_inst type_inst_label
tab tipo_inst

**** Coloca zero nos missings
gen post_pix=week>=wofd(date("16/11/2020","DMY"))

foreach var in valor qtd {
	replace `var'=0 if missing(`var')&post_pix==1
}

// tipo_inst = 1   bancos comerciais Publicos - Federais ou estaduais 
// tipo_inst = 2   bancos comerciais grandes Privados
// tipo_inst = 4   cooperativas de credito
// tipo_inst = 5   bancos digitais ou IPs 
// tipo_inst = 6   o resto: b1 não-grande e não-digital, n1, b2, etc


save "\\sbcdf176\PIX_Matheus$\Stata\dta\Base_muni_banco_self.dta", replace 


log close

