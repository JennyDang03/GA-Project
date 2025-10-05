*****
* Pix Adoption PF
*****
* Esse codigo importa dados ja limpos de PIX agrupados por individuo 
* Possui somente de quem:
*     -  teve aux emergencial em abril 21 e 
*     -  está nos 5000 menores municipios 
* Os dados de input são gerados pelo Monta_Base_Pix_auxilio.do
* Então gera as variaveis after_first_pix_XXX e date_first_pix_XXX
* Onde XXX pode ser rec ou sent
* **********
* Input:  Pix_individuo_aux_abr21.dta
*         id_aux_abril21_sem_pix.dta 
*         dist_caixa_multiprocess7.dta
* Output: Pix_PF_adoption.dta"
************

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

capture log close
log using "$log\Pix_PF_adoption.log", replace 

* carrega arquivo somente de quem teve aux emergencial em abril 21 e está nos 5000 menores municipios 
use "$dta\Pix_individuo_aux_abr21.dta", replace 

gen time = mdy(time_id - int(time_id/100) * 100,1 ,int(time_id/100) )
drop time_id
gen time_id = mofd( time)
format %tmNN/CCYY time_id
tab time_id
drop time 

* Elimina quem tem registros em diferentes municipios. 
duplicates tag id time_id, gen(dup)
tab dup
drop if dup == 1
drop dup

* elimina variaveis nao usadas -------------------------------------------------
* drop value_sent value_rec index0 
drop index0

* Cria painel balanceado
sort id muni_cd time_id
tsset id time_id
tsfill, full

replace trans_rec = 0 if trans_rec == .
replace trans_sent = 0 if trans_sent == .

* what about value_sent value_rec? We end up substituting later. 

* preenche tambem index 
bysort id: egen indexT = max(index) 
replace index = indexT if index == .

* Create Pix Adoption 
*gen dummy_rec = 0
*gen dummy_sent = 0
*replace dummy_rec = 1 if trans_rec > 0
*replace dummy_sent = 1 if trans_sent > 0

* Identifying time of first pix of the person
* first pix has the date of the first pix o the that person
bysort id : egen temp = min(time_id) if trans_rec > 0
bysort id : egen date_first_pix_rec = max(temp) 
drop temp 

bysort id : egen temp = min(time_id) if trans_sent > 0 
bysort id : egen date_first_pix_sent = max(temp) 
drop temp 

format %tmNN/CCYY date_first_pix_rec
format %tmNN/CCYY date_first_pix_sent

* This flags if the date time_id is before or after the first pix transaction
gen after_first_pix_rec = time_id >= date_first_pix_rec
gen after_first_pix_sent = time_id >= date_first_pix_sent
************************** - This seems wrong - maybe it should be >= ????


*Save only Adoption ------------------------------------------------------------
keep id index time_id after_first_pix_rec after_first_pix_sent ///
	date_first_pix_rec date_first_pix_sent muni_cd ///
	trans_rec trans_sent value_sent value_rec

* Temos que adicionar os que nunca usaram Pix.  São cerca de 260k
* Esses id's estão no arquivo "$dta\id_aux_abril21_sem_pix.dta"

merge m:1 id using "$dta\id_aux_abril21_sem_pix.dta"
replace time_id = 730 if _merge == 2
replace after_first_pix_sent = 0 if _merge == 2
replace after_first_pix_rec = 0 if _merge == 2
drop _merge 

sort id time_id
tsset id time_id
tsfill, full

replace after_first_pix_sent = 0 if after_first_pix_sent == .
replace after_first_pix_rec  = 0 if after_first_pix_rec  == .
replace trans_rec  = 0 if trans_rec  == .
replace trans_sent  = 0 if trans_sent  == .
replace value_sent  = 0 if value_sent  == .
replace value_rec  = 0 if value_rec  == .

* Adiciona dados da localizacao e distancias
* por enquanto, somente uma amostra 
* Aqui temos somente os que receberam auxilio emergencial?
*merge m:1 index using "$dta\aux_address_partial_results7_super_cleaned.dta"
merge m:1 index using "$dta\dist_caixa_multiprocess7.dta"

keep if _merge == 3
drop _merge 
sum *
unique index
unique id

*gen after_event  =  time_id >= mofd(mdy(4, 30, 2021))

gen after_first_pix = min(after_first_pix_rec, after_first_pix_sent)

* Adiciona lista dos que receberam parcelas atrasadas do auxilio emergencial em jan-mar 2021
merge m:1 id using "$dta\id_mes_auxilio_jan_mar_21.dta", keep(1 3)
gen aux_emerg_jan_mar21 = (_merge == 3)
drop _merge

* Adiciona status de BF, extracad, cad
* Filtro para deixar somente os que receberam auxilio emergencial fica onde?
merge m:1 id using "\\sbcdf176\PIX_Matheus$\Stata\dta\cpf_bolsa_familia.dta", keep(1 3) nogenerate

save "$dta\Pix_PF_adoption.dta", replace 

log close

