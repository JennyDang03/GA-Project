* Monta base juntando dados do PIX e cartões de credito por município x semana
* Input: 
*    - PIX_week_muni.dta
*    - Cartao_week_muni.dta
*    - TED_week_muni.dta
*    - Boleto_week_muni.dta
*    - flood_cem.dta
* Output: Base_week_muni_flood.dta

* Paths and clears 
clear all
set more off, permanently 
set matsize 2000
set emptycells drop

global log "\\sbcdf060\depepmetas$\usuarios\Jose Renato\Projetos\PIX_Matheus\"
global dta "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\"
global output "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Output\"
global origdata "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\OrigData\"

* ADO
 adopath ++ "D:\ADO"

capture log close
log using "$log\Monta_base_Muni.log", replace 

*** Começa carregando base do cartão
use "$dta\Cartao_week_muni", clear

*** Adiciona a base de TED
merge m:1 muni_cd week using "$dta\TED_week_muni.dta"
drop _merge

*** Adiciona a base de Boletos
merge m:1 muni_cd week using "$dta\Boleto_week_muni.dta"
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
foreach var in  qtd_TED_intra qtd_TED_REC qtd_TED_PAG valor_cartao_credito valor_cartao_debito qtd_TED_REC  valor_TED_REC qtd_TED_PAG  valor_TED_PAG {

	replace `var'  = 0 if  `var'  == .

}

drop if muni_cd == . | muni_cd <=0

*pega cod muni IBGE
rename muni_cd MUN_CD_CADMU
merge m:1 MUN_CD_CADMU using "$dta\municipios.dta", nogenerate keep(1 3)
rename  MUN_CD_CADMU muni_cd
tostring MUN_CD_IBGE, gen(codmun_ibge)
drop MUN_CD SPA_CD PAI_CD MUN_NM MUN_NM_NAO_FORMATADO MUN_CD_UNICAD MUN_IB_MUNICIPIO_BRASIL MUN_CD_RFB  MUN_CD_ECT MUN_DT_CRIACAO MUN_DT_INSTALACAO MUN_DT_EXTINCAO MUN_IB_VIGENTE MUN_DH_ATUALIZACAO MUN_NU_LATITUDE MUN_NU_LONGITUDE MUN_IB_CAPITAL_SUB_BRASIL

merge m:1 codmun_ibge using "\\sbcdf060\depepmetas$\usuarios\Jose Renato\Projetos\PIX_Matheus\flood_cem.dta", nogenerate keep(1 3)


save "\\sbcdf060\depepmetas$\usuarios\Jose Renato\Projetos\PIX_Matheus\Base_week_muni_flood", replace 

sum *

log close
