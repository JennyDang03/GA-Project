* temp_do_add_date_flood

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


use "$dta\Pix_individuo_cleaned1.dta", replace

cap drop flood
cap drop id_municipio
merge m:1 muni_cd time_id using "$dta\flood_monthly_2020_2022.dta", keep(3) keepusing(date_flood)

save "$dta\Pix_individuo_cleaned1.dta", replace
