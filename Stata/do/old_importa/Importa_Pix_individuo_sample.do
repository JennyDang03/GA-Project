******************************************
* Os dados de input são gerados pelo Pix_por_Ind_Mes_Sample.R

*Importa_Pix_individuo_sample.do
* Input: 
*   1) "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\Pix_mes_ind_self_sample`year'`m'.csv.csv"
*   2) "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\Pix_mes_ind_rec_sample`year'`m'.csv"
*   3) "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\Pix_mes_ind_pag_sample`year'`m'.csv"

* Output:
*   1) "$dta\Pix_individuo_sample.dta"

* Variables: id value_sent trans_sent muni_cd time_id value_rec trans_rec value_self trans_self

* The goal: To get put together transactions sent, received and self for a sample of individuals - 2 million of CPFs. This code plus a future code cleaning the data  plus a code doing it for PJ will substitute: Importa_Pix_individuo.do, monta_base_pix_individuo.do, Importa_Pix_individuo_self, and monta_base_pix_self_individuo.do

* To do: I am not sure if this includes people that never used pix. Also, this does not include firms. We need to do tsfill, clean, and add flood. 

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
log using "$log\Importa_Pix_individuo_sample.log", replace 

* Executa a importacao nessa ordem:
* 1) Self
* 2) Recebidos
* 3) Enviados

*****************
**** 1) SELF ****
*****************
* Importa do CSV para arquivos dta temporários
forvalues year = 2020/2022 {
	local meses  01 02 03 04 05 06 07 08 09 10 11 12
	foreach m of local meses {
		if  ~(`year'==2020 & `m'<=10) {
			dis `year' `m'
			import delimited "$origdata\Pix_mes_ind_self_sample`year'`m'.csv", clear
			gen time_id = `year'`m'
			save "$dta\temp_`year'`m'.dta",replace 
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
			append using "$dta\temp_`year'`m'.dta"
			erase  "$dta\temp_`year'`m'.dta"
		}
	}
}

compress
drop if muni_cd<=0
drop if muni_cd == .
drop if id == .

save "$dta\Pix_individuo_sample.dta",replace 

**********************
**** 2) RECEBIDOS ****
**********************

* Importa do CSV para arquivos dta temporários
forvalues year = 2020/2022 {
	local meses  01 02 03 04 05 06 07 08 09 10 11 12
	foreach m of local meses {
		if  ~(`year'==2020 & `m'<=10) {
			dis `year' `m'
			import delimited "$origdata\Pix_mes_ind_rec_sample`year'`m'.csv", clear
			gen time_id = `year'`m'
			save "$dta\temp_`year'`m'.dta",replace 
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
			append using "$dta\temp_`year'`m'.dta"
			erase  "$dta\temp_`year'`m'.dta"
		}
	}
}

compress
drop if muni_cd<=0
drop if muni_cd == .
drop if id == .

merge 1:1 id muni_cd time_id using "$dta\Pix_individuo_sample.dta", nogenerate

save "$dta\Pix_individuo_sample.dta",replace 

**********************
**** 3) enviados  ****
**********************

* Importa do CSV para arquivos dta temporários
forvalues year = 2020/2022 {
	local meses  01 02 03 04 05 06 07 08 09 10 11 12
	foreach m of local meses {
		if  ~(`year'==2020 & `m'<=10) {
			dis `year' `m'
			import delimited "$origdata\Pix_mes_ind_pag_sample`year'`m'.csv", clear
			gen time_id = `year'`m'
			save "$dta\temp_`year'`m'.dta",replace 
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
			append using "$dta\temp_`year'`m'.dta"
			erase  "$dta\temp_`year'`m'.dta"
		}
	}
}

compress
drop if muni_cd<=0
drop if muni_cd == .
drop if id == .

merge 1:1 id muni_cd time_id using "$dta\Pix_individuo_sample.dta", nogenerate

save "$dta\Pix_individuo_sample.dta",replace 

log close