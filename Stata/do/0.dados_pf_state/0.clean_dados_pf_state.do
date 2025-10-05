*dados_pf_state

clear all
set more off, permanently 
set matsize 2000
set emptycells drop


global log "D:\PIX_Matheus\Stata\log"
global dta "D:\PIX_Matheus\Stata\dta"
global dta_sample "D:\PIX_Matheus\Stata\dta_sample"
global output "D:\PIX_Matheus\Output"
global origdata "D:\PIX_Matheus\DadosOriginais"

* ADO 
adopath ++ "D:\ADO"


import delimited "$origdata\dados_pf_state\Dados_PF_CE.csv"
