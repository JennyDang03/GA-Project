* Monta base juntando dados do PIB, roubos, internet 3g, etc
* Input: 
*    - XXXXXXXXXXXXXXXXXX.dta
*    - pib_2019.dta
*    - cobertura de internet 3g 
* Output:
*  mun_weights.dta


global log "\\sbcdf060\depepmetas$\usuarios\Jose Renato\Projetos\PIX_Matheus\"
global dta "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\"
global output "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Output\"
global origdata "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\OrigData\"

* ADO
 adopath ++ "D:\ADO"

clear all

capture log close
log using "$log\Monta_CEM.log", replace

use "${dta}\pib_2019.dta", clear

* Municipality 3g data
merge m:1 codmun_ibge using "${dta}\3g_coverage.dta", keep(1 3) nogen
destring pc_area_3g pc_moradores_3g pc_domicilios_3g  area moradores domicilios, replace force

duplicates drop codmun_ibge, force
gen id_municipio = real(codmun_ibge)

cap erase temp.dta
save temp

use "\\sbcdf060\depepmetas$\usuarios\Jose Renato\Projetos\PIX_Matheus\disaster_weekly_flood.dta", clear

merge m:1 id_municipio using temp
unique id_municipio_bcb if _merge == 2
keep if _merge == 3
drop _merge
drop Município UF
 
sum  pib_2019 pc_area_3g pc_moradores_3g pc_domicilios_3g area moradores domicilios

gen date_w_disaster = week if number_disasters >= 1 
bys codmun_ibge: egen date_w_disaster_mun=min(date_w_disaster)
gen disaster_flag = date_w_disaster_mun ~= .
drop week date_w_disaster

collapse (min)  pib_2019 pc_area_3g pc_moradores_3g pc_domicilios_3g area moradores domicilios date_w_disaster_mun disaster_flag,by(id_municipio id_municipio_bcb sigla codmun_ibge município)

cem pc_area_3g pc_moradores_3g pc_domicilios_3g area moradores  domicilios  pib_2019 , tr(disaster_flag)

save "\\sbcdf060\depepmetas$\usuarios\Jose Renato\Projetos\PIX_Matheus\flood_cem.dta", replace

log close


