* Esse codigo importa as aberturas de contas e fechamentos de contas 
* daqueles que receberam auxilio emergencial 
* Os dados de input são gerados pelo CCS_aux_emerg.R
* Input:   $origdata\CCS_aux_emerg_estoque.csv
* Output: "$dta\CCS_estoque_aux_emerg.dta"

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
log using "$log\CCS_aux_emerg_estoque.log", replace 

import delimited "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/CCS_aux_emerg_estoque.csv", clear

unique id

replace rel_dt_inicio = substr(rel_dt_inicio,1,7)
cap drop m_opening_date
gen m_opening_date = monthly(rel_dt_inicio,"YM")
format %tm m_opening_date

replace rel_dt_fim = substr(rel_dt_fim,1,7)
cap drop m_closing_date
gen m_closing_date = monthly(rel_dt_fim,"YM")
format %tm m_closing_date

local start=ym(2019,1)
local end=ym(2022,6)
forvalues m = `start'/`end' {
    preserve
		* Create variable accounts
		gen n_account_new = (m_opening_date == `m')
		gen n_account_stock = 1 if m_opening_date <= `m' & (m_closing_date >= `m' | m_closing_date == .)
		replace n_account_stock = 0 if n_account_stock == .
		collapse (sum) n_account_new n_account_stock, by(id) // Counts the non missing accounts
		gen time_id = `m'
		save "$dta\temp_accounts_`m'.dta", replace
	restore
}

local start=ym(2019,1)
local end=ym(2022,6)
forvalues m = `start'/`end' {	
    	
		if `m' == `start' {
			use "$dta\temp_accounts_`m'.dta", clear 
		}
		else {		
			append using "$dta\temp_accounts_`m'.dta"
		}

}

label var n_account_new "Accounts opened on that month"
label var n_account_stock "Stock of accounts"
sort id time_id
format %tmNN/CCYY time_id

save "$dta\CCS_aux_emerg.dta", replace

log close

