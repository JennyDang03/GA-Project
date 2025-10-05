* Radiance
* Run first maps_nightlights.py 


clear all
set more off, permanently 

set emptycells drop

global log "\\sbcdf176\PIX_Matheus$\Stata\log"
global dta "\\sbcdf176\PIX_Matheus$\Stata\dta"
global output "\\sbcdf176\PIX_Matheus$\Output"
global origdata "\\sbcdf176\PIX_Matheus$\DadosOriginais"

*global dta "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta"
*global output "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output"
* ADO 
adopath ++ "D:\ADO"
adopath ++ "//sbcdf060/depep01$/ADO"
adopath ++ "//sbcdf060/depep01$/ado-776e"

capture log close
log using "$log\radiance_balance_test.log", replace 

use "$dta\radiance.dta",clear
drop index


*create municipality id (number) for mun_name
encode mun_name, gen(mun_id)
xtset mun_id
xtreg radiance caixa, fe robust


*estout , replace cells(b(star fmt(3)) se(fmt(3))) style(tex) label title("Regression Results") nonum


log close