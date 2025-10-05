******************************************
* Monta_base_Muni.do
* Input: 
*   1) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\Cartao_week_muni"
*   2) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\TED_week_muni"
*	3) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\TED_week_muni_QTD_CLI"
*	4) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\Boleto_week_muni"
*	5) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\Boleto_week_muni_qtd_cli"
*	6) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\PIX_week_muni"
*	7) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\municipios"

* Output:
*   1) "$dta\Base_week_muni.dta"

* Variables: muni_cd week valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito valor_TED_intra qtd_TED_intra qtd_cli_TED_rec_PJ qtd_cli_TED_rec_PF qtd_cli_TED_pag_PJ qtd_cli_TED_pag_PF valor_boleto_deb valor_boleto_dinheiro qtd_boleto_deb qtd_boleto_dinheiro valor_boleto valor_boleto_eletronico valor_boleto_ATM valor_boleto_age valor_boleto_corban qtd_boleto qtd_boleto_eletronico qtd_boleto_ATM qtd_boleto_age qtd_boleto_corban qtd_cli_pag_pf_boleto qtd_cli_pag_pj_boleto qtd_cli_rec_pj_boleto valor_PIX_inflow qtd_PIX_inflow n_cli_rec_pf_inflow n_cli_rec_pj_inflow valor_PIX_outflow qtd_PIX_outflow n_cli_pag_pf_outflow n_cli_pag_pj_outflow valor_PIX_intra qtd_PIX_intra n_cli_pag_pf_intra n_cli_rec_pf_intra n_cli_pag_pj_intra n_cli_rec_pj_intra MUN_CD_IBGE codmun_ibge MUN_NU_LATITUDE MUN_NU_LONGITUDE post_pix

* The goal: Cria base com informacoes sobre Pix, boleto, Ted, e Cartao.

* To do: Update data, maybe get some aggregation to the month level. 

******************************************
* Paths and clears 
clear all
set more off, permanently 
set matsize 2000
set emptycells drop

global log "\\sbcdf176\PIX_Matheus$\Stata\log"
global dta "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\"
global output "\\sbcdf176\PIX_Matheus$\Output"
global origdata "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\OrigData\"

* ADO
 adopath ++ "//sbcdf060/depep01$/ADO"
 adopath ++ "//sbcdf060/depep01$/ado-776e"

capture log close
log using "$log\Monta_base_Muni.log", replace 

*** Começa carregando base do cartão
use "$dta\Cartao_week_muni", clear

*** Adiciona a base de TED
merge m:1 muni_cd week using "$dta\TED_week_muni.dta"
drop _merge
merge m:1 muni_cd week using "$dta\TED_week_muni_QTD_CLI.dta"
drop _merge

*** Adiciona a base de Boletos
merge m:1 muni_cd week using "$dta\Boleto_week_muni.dta"
drop _merge

merge m:1 muni_cd week using "$dta\Boleto_week_muni_qtd_cli.dta"
drop _merge

*** Adiciona a base de PIX
merge m:1 muni_cd week using "$dta\PIX_week_muni.dta"
drop _merge

* renomeia variaveis do PIX para não confundir 
rename valor_intra valor_PIX_intra
rename qtd_intra qtd_PIX_intra
rename valor_inflow valor_PIX_inflow
rename qtd_inflow qtd_PIX_inflow
rename valor_outflow valor_PIX_outflow
rename qtd_outflow qtd_PIX_outflow

*** Força zero nos nulos 
foreach var in  valor_TED_intra qtd_TED_intra valor_cartao_credito valor_cartao_debito   {

	replace `var'  = 0 if  `var'  == .

}

drop if muni_cd == . | muni_cd <=0

*pega cod muni IBGE
rename muni_cd MUN_CD_CADMU
merge m:1 MUN_CD_CADMU using "$dta\municipios.dta", nogenerate keep(1 3)
rename  MUN_CD_CADMU muni_cd
tostring MUN_CD_IBGE, gen(codmun_ibge)
drop MUN_CD SPA_CD PAI_CD MUN_NM MUN_NM_NAO_FORMATADO MUN_CD_UNICAD MUN_IB_MUNICIPIO_BRASIL MUN_CD_RFB  MUN_CD_ECT MUN_DT_CRIACAO MUN_DT_INSTALACAO MUN_DT_EXTINCAO MUN_IB_VIGENTE MUN_DH_ATUALIZACAO   MUN_IB_CAPITAL_SUB_BRASIL

**** Coloca zero nos missings do PIX 
gen post_pix=week>=wofd(date("16/11/2020","DMY"))

foreach var in valor_PIX_intra qtd_PIX_intra valor_PIX_inflow qtd_PIX_inflow valor_PIX_outflow qtd_PIX_outflow n_cli_pag_pf_intra n_cli_rec_pf_intra n_cli_pag_pj_intra n_cli_rec_pj_intra n_cli_rec_pj_inflow n_cli_rec_pf_inflow n_cli_pag_pf_outflow n_cli_pag_pj_outflow{
	replace `var'=0 if missing(`var')&post_pix==1
}

*drop id_munic_6 id_tse id_rf id_bcb id_judicial_district id_health_region id_state
cap drop _merge 
*drop sigla município cod_mun_bcb municipio categoria_local categoria_roubo

save "\\sbcdf176\PIX_Matheus$\Stata\dta\Base_week_muni", replace 

sum *

log close




