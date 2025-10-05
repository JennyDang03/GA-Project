*****************************************
* Os dados de input são gerados pelo Pix_por_Ind_Mes.R, SQL PIX_mes_individuo_self.sql

*Importa_Pix_individuo_self.do
* Input: 
*   1) "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\PIX_MES_SELF_`year'`m'.csv"

* Output:
*   1) "$dta\Pix_individuo_PJ_self.dta"
*   1) "$dta\Pix_individuo_PF_self.dta"

* Variables: id tipo_pessoa muni_cd time_id value_self trans_self

* The goal: Aggregate the monthly pix transactions for every person and firm to themselves. 

* To do: Note the other form of doing this, Importa_Pix_individuo_sample.do

*****************************************
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
log using "$log\Importa_Pix_individuo_self.log", replace 

* Importa do CSV para arquivos dta temporários
forvalues year = 2020/2022 {
	local meses  01 02 03 04 05 06 07 08 09 10 11 12
	foreach m of local meses {
		if  ~(`year'==2020 & `m'<=10) {
			dis `year' `m'
			import delimited "$origdata\PIX_MES_SELF_`year'`m'.csv", clear
			gen time_id = `year'`m'
			save "$dta\temp_self_`year'`m'.dta",replace 
		}
	}
}

* Junta arquivos temporários num arquivo só
clear
forvalues year = 2020/2022 {
	local meses  01 02 03 04 05 06 07 08 09 10 11 12
	foreach m of local meses {
		if  ~(`year'==2020 & `m'<=11) {
			dis `year' `m'
			append using "$dta\temp_self_`year'`m'.dta"
			erase  "$dta\temp_self_`year'`m'.dta"
		}
	}
}

compress
drop if id_municipio_bcb<=0
drop if id_municipio_bcb == .
ren id_municipio_bcb muni_cd
drop if id == .
drop if tipo_pessoa == .

preserve 
	keep if tipo_pessoa == 2
	* Agrupa CNPJ14 para CNPJ8
	collapse (sum) value_self trans_self , by(id tipo_pessoa time_id muni_cd)
	save "$dta\Pix_individuo_PJ_self.dta",replace 

restore 

keep if tipo_pessoa == 1
save "$dta\Pix_individuo_PF_self.dta",replace 

log close