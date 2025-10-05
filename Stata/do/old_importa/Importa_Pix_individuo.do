******************************************
* Baixar Dados com 
	* Pix_mes_por_ind_rec.sql 
	* Pix_mes_por_ind_pag.sql
* Depois, rodar o programa R 
	* Pix_por_Ind_Mes.R

*Importa_Pix_individuo.do
* Input: 
*   1) "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\PIX_MES_PAG_`year'`m'.csv"
*   2) "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\PIX_MES_REC_`year'`m'.csv"

* Output:
*   1) "$dta\Pix_individuo.dta"

* Variables: id tipo_pessoa time_id muni_cd value_rec trans_rec value_sent trans_sent

* The goal: Aggregate the monthly pix transactions for every person and firm. 

* To do: Note the other form of doing this, Importa_Pix_individuo_sample.do

******************************************
clear all
set more off, permanently 

set emptycells drop

global log "\\sbcdf176\PIX_Matheus$\Stata\log"
global dta "\\sbcdf176\PIX_Matheus$\Stata\dta"
global output "\\sbcdf176\PIX_Matheus$\Output"
global origdata "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results"

* ADO 
adopath ++ "D:\ADO"
 adopath ++ "//sbcdf060/depep01$/ADO"
 adopath ++ "//sbcdf060/depep01$/ado-776e"

capture log close
log using "$log\Importa_Pix_individuo.log", replace 

* 1) Pagamentos 

forvalues year = 2020/2022 {
	local meses  01 02 03 04 05 06 07 08 09 10 11 12
	foreach m of local meses {
		if  ~(`year'==2020 & `m'<=10) {
			dis `year' `m'
			import delimited "$origdata\PIX_MES_PAG_`year'`m'.csv", clear
			gen time_id = `year'`m'
			save "$dta\temp_pag_`year'`m'.dta",replace 
		}
	}
}

use "$dta\temp_pag_202011.dta", clear 
forvalues year = 2020/2022 {
	local meses  01 02 03 04 05 06 07 08 09 10 11 12
	foreach m of local meses {
		if  ~(`year'==2020 & `m'<=11) {
			dis `year' `m'
			append using "$dta\temp_pag_`year'`m'.dta"
			erase  "$dta\temp_pag_`year'`m'.dta"
		}
	}
}

compress
drop if id_municipio_bcb<=0
drop if id_municipio_bcb == .
ren id_municipio_bcb muni_cd
drop if id == .
drop if tipo_pessoa == .


* Agrupa CNPJ14 para CNPJ8
collapse (sum) value_sent trans_sent, by(id tipo_pessoa time_id muni_cd)

save "$dta\temp_pag.dta",replace 

* 2) Recebimentos 

forvalues year = 2020/2022 {
	local meses  01 02 03 04 05 06 07 08 09 10 11 12
	foreach m of local meses {
		if  ~(`year'==2020 & `m'<=10) {
			dis `year' `m'
			import delimited "$origdata\PIX_MES_REC_`year'`m'.csv", clear
			gen time_id = `year'`m'
			save "$dta\temp_rec_`year'`m'.dta",replace 
		}
	}
}

use "$dta\temp_rec_202011.dta", clear 
forvalues year = 2020/2022 {
	local meses  01 02 03 04 05 06 07 08 09 10 11 12
	foreach m of local meses {
		if  ~(`year'==2020 & `m'<=11) {
			dis `year' `m'
			append using "$dta\temp_rec_`year'`m'.dta"
			erase  "$dta\temp_rec_`year'`m'.dta"
		}
	}
}

compress
drop if id_municipio_bcb<=0
drop if id_municipio_bcb == .
ren id_municipio_bcb muni_cd
drop if id == .
drop if tipo_pessoa == .

* Agrupa CNPJ14 para CNPJ8
collapse (sum) value_rec trans_rec, by(id tipo_pessoa time_id muni_cd)

save "$dta\temp_rec.dta",replace 

* 3) Faz o merge de pagamentos e recebimentos

merge 1:1 id tipo_pessoa time_id muni_cd using "$dta\temp_pag.dta", nogen 

replace value_rec = 0 if value_rec == .
replace trans_rec = 0 if trans_rec == .
replace value_sent = 0 if value_sent == .
replace trans_sent = 0 if trans_sent == .

save "$dta\Pix_individuo.dta",replace 

log close