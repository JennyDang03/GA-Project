* temp_do_get_sample

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

/*
use "$dta\Base_week_muni_fake.dta", replace
rename muni_cd id
rename week time_id 
*/

use "$dta\Pix_individuo_cleaned1.dta", replace

tempfile holding
save `holding'

keep id
duplicates drop

set seed 1234
sample 10

merge 1:m id using `holding', assert(match using) keep(match) nogenerate

save "$dta\Pix_individuo_cleaned1_sample.dta", replace
