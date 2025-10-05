*sample creator

* Create descriptions of real files

clear all
set more off, permanently 
set matsize 2000
set emptycells drop


global log "D:\PIX_Matheus\Stata\log"
global dta "D:\PIX_Matheus\Stata\dta"
global output "D:\PIX_Matheus\Output"
global origdata "D:\PIX_Matheus\DadosOriginais"

* ADO
adopath ++ "D:\ADO\"

local files: dir "$dta" files "*.dta"
foreach f of local files {
	capture log close
	log using "$log\description_`f'.log", replace
	display "`f'"
	use `"$dta\\`f'"', clear
	describe
	codebook
	inspect
	sum *, detail

	foreach var of varlist _all{
		cap noisily tab `var'
	}
	
	log close
}