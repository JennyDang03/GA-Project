* Work with Estban data

** add more estban months


******* Use data from April 30th 2021, not before or after


use "C:\Users\mathe\Dropbox\RESEARCH\ESTBAN\dta\1-estban_AG.dta", clear
cap drop bank
gen bank = "Other"
replace bank = "Bradesco" if nome_instituicao == "BCO BRADESCO S.A."
replace bank = "BB" if nome_instituicao == "BCO DO BRASIL S.A."
replace bank = "Santander" if nome_instituicao == "BCO SANTANDER (BRASIL) S.A."
replace bank = "Caixa" if nome_instituicao == "CAIXA ECONOMICA FEDERAL"
replace bank = "Itau" if nome_instituicao == "ITAÚ UNIBANCO S.A."

gen date=ym(year,month)
format date %tm
bysort date cod_munic bank: gen presence = _n == 1
gen branches_number = 1
save "C:\Users\mathe\Dropbox\RESEARCH\ESTBAN\dta\1.1-estban_AG.dta", replace




use "C:\Users\mathe\Dropbox\RESEARCH\ESTBAN\dta\1.1-estban_AG.dta", clear
*Number of branches and presence in municipalities
collapse (sum) presence assets loans_total deposits total_ativo_c399y branches_number, by(date bank nome_instituicao)
save "C:\Users\mathe\Dropbox\RESEARCH\ESTBAN\dta\1.1-estban_AG_collapse.dta", replace
use "C:\Users\mathe\Dropbox\RESEARCH\ESTBAN\dta\1.1-estban_AG_collapse.dta", replace

/*
generate nome_instituicao1 = subinstr(nome_instituicao," ","_",.)
generate nome_instituicao2 = substr(nome_instituicao1,1,230)
generate nome_instituicao3 = subinstr(nome_instituicao2,"[^a-zA-Z0-9_]","",.)
generate nome_instituicao4 = subinstr(nome_instituicao3,"Ç","C",.)
generate nome_instituicao5 = subinstr(nome_instituicao4,"Õ","O",.)
generate nome_instituicao6 = subinstr(nome_instituicao5,"Ã","A",.)
generate nome_instituicao7 = subinstr(nome_instituicao6,"Ú","U",.)
generate nome_instituicao8 = subinstr(nome_instituicao7,"-","_",.)
generate nome_instituicao9 = subinstr(nome_instituicao8,".","",.)
generate nome_instituicao10 = subinstr(nome_instituicao9,"/","",.)
*rename nome_instituicao10 bank_name

keep presence assets loans_total deposits total_ativo_c399y branches_number date nome_instituicao10
reshape wide presence assets loans_total deposits total_ativo_c399y branches_number, i(date) j(nome_instituicao10) string
*missings dropvars, force


*/

rename branches_number branches
rename total_ativo_c399y cash
rename loans_total loans

collapse (sum) presence assets loans deposits cash branches, by(date bank)
reshape wide branches presence assets loans deposits cash, i(date) j(bank) string
*missings dropvars, force
gen date2 = date
keep if date >= 708
* por nomes nessas xlines
line branches* date, xline(730) xline(722) xline(735)

line presence* date, xline(730) xline(722) xline(735)

line assets* date, xline(730) xline(722) xline(735)

line loans* date, xline(730) xline(722) xline(735)

line deposits* date, xline(730) xline(722) xline(735)

line cash* date, xline(730) xline(722) xline(735)

************************
*Making Beautiful graphs with ChatGPT help
*format date %td MMM, YYYY
line presenceBB presenceBradesco presenceCaixa presenceItau presenceOther date, ///
	lc(black blue green red purple) /// 
	title("Bank Presence over Time") ///
	legend(order(1 "BB" 2 "Bradesco" 3 "Caixa" 4 "Itau" 5 "Other")) ///
	ytitle("Municipalities with bank branches") xtitle("Month") /// 
	xline(722 730 735) ///
	xmlabel(722 "Covid" 730 "Pix Launch" 735 "Shock", angle(90))
graph export "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output\graphs\bank_presence.png", as(png) name("Graph") replace

*graph save "Graph" "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output\graphs\bank_presence.gph"

line branchesBB branchesBradesco branchesCaixa branchesItau branchesOther date, ///
	lc(black blue green red purple) /// 
	title("Number of Branches over Time") ///
	legend(order(1 "BB" 2 "Bradesco" 3 "Caixa" 4 "Itau" 5 "Other")) ///
	ytitle("Number of Branches") xtitle("Month") /// 
	xline(722 730 735) ///
	xmlabel(722 "Covid" 730 "Pix Launch" 735 "Shock", angle(90))
graph export "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output\graphs\branches_number.png", as(png) name("Graph") replace

*graph save "Graph" "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output\graphs\branches_number.gph"

*******************************************************************************


	
/******************************************************************************

*		Adapted code from 1.municipality_estban on 
			C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\do\0.caixa_data_treatment\codes from previous work on Municipio and ESTBAN

*       Last revisor: 	MS		

https://www3.bcb.gov.br/aplica/cosif
				
*******************************************************************************/
		
	clear
	clear mata
	clear matrix
	

	foreach path in D:\Dropbox C:\Dropbox C:\Users\mathe\Dropbox\RESEARCH {
		capture cd `path'
		if _rc == 0 macro def path `path'
	}

	
use "C:\Users\mathe\Dropbox\RESEARCH\ESTBAN\dta\1-estban_AG.dta", clear
keep if year == 2021 & month == 4
keep codmun cod_munic nome_instituicao 
*total_ativo_c399y deposits loans_total assets

rename codmun id_municipio_bcb
rename cod_munic id_municipio

gen bank = "Other"
replace bank = "Bradesco" if nome_instituicao == "BCO BRADESCO S.A."
replace bank = "BB" if nome_instituicao == "BCO DO BRASIL S.A."
replace bank = "Santander" if nome_instituicao == "BCO SANTANDER (BRASIL) S.A."
replace bank = "Caixa" if nome_instituicao == "CAIXA ECONOMICA FEDERAL"
replace bank = "Itau" if nome_instituicao == "ITAÚ UNIBANCO S.A."
drop nome_instituicao

gen branches_ = 1
collapse (sum) branches_, by(id_municipio_bcb id_municipio bank)
gen presence_ = 1
reshape wide branches_ presence_, i(id_municipio_bcb id_municipio) j(bank) string
foreach x of varlist *{
	replace `x' = 0 if missing(`x')
}
save "$path\ESTBAN\dta\2-estban_AG_04_2021.dta", replace

use "$path\ESTBAN\dta\2-estban_AG_04_2021.dta", replace

merge m:1 id_municipio_bcb using "$path\Municipios\dta\municipios2.dta", keepus(id_municipio_bcb id_municipio)
drop if _merge == 1
foreach x of varlist *{
	replace `x' = 0 if missing(`x')
}
drop _merge

	/*
	*drop if there are two id_municipio_bcb or two id_municipio
	sort id_municipio
    quietly by id_municipio:  gen dup = cond(_N==1,0,_n)
	tab dup
	drop dup
	* Brasilia always gives problems
	*drop if id_municipio == 5300108
	*/
	
* compare cities with caixa to cities without caixa but other bank
merge 1:1 id_municipio using "$path\population\dta\population2021.dta", keepus(id_municipio populacao)
keep if _merge == 3
drop _merge
	
save "$path\ESTBAN\dta\3-estban_AG_04_2021.dta", replace


/*
****************************************
************* Look for 2021?
************* Code from 2.clean_br_geobr_mapas
****************************************

* interpret multipoligon
shp2dta using "$path\Municipios\raw\from_ipeaGIT_geobr\mun_2020", database("$path\Municipios\dta\mun_2020_database.dta") coordinates("$path\Municipios\dta\mun_2020_coordinates.dta") gencentroids(stub) genid(newvarname) replace

use "$path\Municipios\dta\mun_2020_database.dta", replace
*rename CODIGO_MUN id_municipio
rename code_mn id_municipio
order id_municipio
*destring id_municipio, replace
*sort id_municipio
rename newvarname _ID
save "$path\Municipios\dta\mun_2020_database1.dta", replace

use "$path\Municipios\dta\mun_2020_coordinates.dta", replace
merge m:1 _ID using "$path\Municipios\dta\mun_2020_database1.dta", keepusing(id_municipio)
drop _merge _ID
order id_municipio
save "$path\Municipios\dta\mun_2020_coordinates1.dta", replace


use "$path\Municipios\dta\mun_2020_database1.dta", replace
drop _ID name_mn cod_stt abbrv_s nam_stt cod_rgn nam_rgn
rename x_stub x_center
rename y_stub y_center
save "$path\Municipios\dta\mun_2020_database2.dta", replace
*/

use "$path\Municipios\dta\mun_2020_database2.dta", replace

* merge with ESTBAN
merge 1:1 id_municipio using "$path\ESTBAN\dta\3-estban_AG_04_2021.dta"
keep if _m == 3
drop _m

save "$path\Municipios\dta\mun_2021_estban.dta", replace

* do algorithm to find smallest distance between two coordinates

foreach x in BB Bradesco Caixa Itau Santander Other{
	use "$path\Municipios\dta\mun_2021_estban.dta", replace
	cd "$path\Municipios\temp"
	* save type1 and type2 observation separately
	*tempfile main bank1
	save "main.dta", replace
	save "`x'1.dta", replace
	use "`x'1.dta", replace
	keep if presence_`x' == 1
	rename * *1
	gen id1 = _n
	save "`x'1.dta", replace
	use "main.dta", replace
	keep if presence_`x' == 0
	gen id0 = _n

	* form all pairwise combinations and calculate distance
	cross using "`x'1.dta"
	geodist x_center y_center x_center1 y_center1, gen(d_`x')
	sort id0 d_`x'

	*Drop todas exceto a menor
	bysort id0: gen distance_rank = _n
	keep if distance_rank ==1
	drop id0 id1 distance_rank

	* Name things correctly
	
	rename id_municipio1 closest_`x'_mun
	*rename x_center1 closest_`x'_x
	*rename y_center1 closest_`x'_y
	*rename populacao1 closest_`x'_pop
	rename d closest_`x'_d
	drop *1
	*save
	save "mun_2021_estban_closest_`x'.dta", replace
}

*****
* Merge
*****
use "$path\Municipios\dta\mun_2021_estban.dta", replace
foreach x in BB Bradesco Caixa Itau Santander Other{
	merge 1:1 id_municipio using "$path\Municipios\temp\mun_2021_estban_closest_`x'.dta"
	drop _merge
}
* replace distance when it is missing
foreach x of varlist closest_*_d{
	replace `x' = 0 if missing(`x')
}
*Variable creation
egen double closest_bank_d = rowmin(closest_BB_d closest_Bradesco_d closest_Caixa_d closest_Itau_d closest_Santander_d closest_Other_d)
egen expected_distance = rowmean(closest_BB_d closest_Bradesco_d closest_Caixa_d closest_Itau_d closest_Santander_d)
gen centered_closest_Caixa_d = closest_Caixa_d - expected_distance

gen closest_bank_mun = .
foreach x in BB Bradesco Caixa Itau Santander Other{
	replace closest_bank_mun = closest_`x'_mun if closest_bank_d == closest_`x'_d
}
replace closest_bank_mun = id_municipio if missing(closest_bank_mun)

gen branches = branches_BB + branches_Bradesco + branches_Caixa + branches_Itau + branches_Other + branches_Santander

** Save
save "$path\Municipios\dta\mun_2021_estban_closest_bank.dta", replace
save "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_2021_estban_closest_bank.dta", replace



******************************************************************************** 
/*
Code based on 6. Mun_selection

*Mun selection
* Our goal is to compare two types of municipality
* type one is strictly closer to caixa. That means it is closer to caixa and any other bank is far away. 
*type two is strictly closer to ONE bank and any other, and that one bank is not caixa. 


*/


******************************************
* 1. First lets do the code for the Borusyak & Hull paper

*the regressions must be done in R and must probably be done in KLC super computer

******************************************
use "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_2021_estban_closest_bank.dta", replace
keep id_municipio branches
rename id_municipio closest_bank_mun 
rename branches closest_bank_branches
save "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\temp_closest_caixa_bank2.dta", replace


use "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_2021_estban_closest_bank.dta", replace
merge m:1 closest_bank_mun using "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\temp_closest_caixa_bank2.dta"
drop if _merge == 2
drop _merge
gen treatment = 1 if closest_bank_d < closest_Caixa_d
replace treatment = 0 if treatment == . 
save "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\all_mun_closest_bank.dta", replace

use "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\all_mun_closest_bank.dta", replace
keep id_municipio id_municipio_bcb populacao closest_BB_d closest_Bradesco_d closest_Caixa_d closest_Itau_d closest_Santander_d closest_Other_d closest_bank_d expected_distance centered_closest_Caixa_d closest_bank_mun branches closest_bank_branches treatment
save "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\all_mun.dta", replace

use "$path\pix\pix-event-study\Stata\dta\Base_week_muni_sem_qtd_menor3\Base_week_muni_sem_qtd_menor3.dta", replace
rename muni_cd id_municipio_bcb
merge m:1 id_municipio_bcb using "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\all_mun.dta"
keep if _merge ==3
drop _merge

sort id_municipio week
gen week2 = week
order id_municipio week week2
gen D = 0
replace D = 1 if week > 3189
drop if week >= 3241 //3241 or 3224
gen transactions_pix = qtd_PIX_inflow + qtd_PIX_outflow + qtd_PIX_intra
gen log_transactions_pix = log(transactions_pix)
gen value_pix = valor_PIX_inflow + valor_PIX_outflow + valor_PIX_intra
gen log_value_pix = log(value_pix)

save "$path\pix\pix-event-study\Stata\dta\Base_week_muni_sem_qtd_menor3\regression_ready_all_mun.dta", replace
use "$path\pix\pix-event-study\Stata\dta\Base_week_muni_sem_qtd_menor3\regression_ready_all_mun.dta", replace
label var expected_distance "Average distance of the 5 big banks"
egen expected_distance2 = rowmean(closest_BB_d closest_Bradesco_d closest_Itau_d closest_Santander_d)
gen centered_closest_Caixa_d2 = closest_Caixa_d - expected_distance2
label var expected_distance2 "Average distance of the 4 big banks, excluding Caixa"
label var centered_closest_Caixa_d2 "Dist_Caixa - Average distance of the 4 other banks"
save "$path\pix\pix-event-study\Stata\dta\Base_week_muni_sem_qtd_menor3\regression_ready_all_mun.dta", replace
export delimited using "$path\pix\pix-event-study\CSV\regression_ready_all_mun.csv", replace
********************************************************************************

use "$path\pix\pix-event-study\Stata\dta\Base_week_muni_sem_qtd_menor3\regression_ready_all_mun.dta", replace

reg log_transactions_pix i.week  D##c.closest_Caixa_d D##c.closest_bank_d D##c.expected_distance // i.id_municipio


populacao closest_bank_d closest_Caixa_d expected_distance centered_closest_Caixa_d branches closest_bank_branches treatment D



use "$path\pix\pix-event-study\Stata\dta\Base_week_muni_sem_qtd_menor3\regression_ready.dta", replace

use "$path\pix\pix-event-study\CSV\regression_ready_pix_ted_boleto.csv", clear
reg log_transactions_pix D##treatment i.week i.id_municipio

****
*Correct this code later
collapse (mean) log_transactions_pix log_value_pix, by(week caixa_treatment)
reshape wide log_transactions_pix log_value_pix, i(week) j(caixa_treatment)

line log_transactions_pix0 log_transactions_pix1 week
line log_value_pix0 log_value_pix1 week
****






*Mun selection
* Our goal is to compare two types of municipality
* type one is strictly closer to caixa. That means it is closer to caixa and any other bank is far away. 
*type two is strictly closer to ONE bank and any other, and that one bank is not caixa. 


use "$path\population\dta\population2021.dta", replace
merge 1:1 id_municipio using "C:\Users\mathe\Dropbox\RESEARCH\Municipios\dta\municipios2.dta"
keep if _merge == 3
drop if id_municipio_bcb == 36263 // there seems to be a problem with it 
keep id_municipio populacao id_municipio_bcb
sort populacao
gen ranking = _n

sum populacao, detail
display  38297.6*5570
* 213 mi
sum populacao if ranking <= 5000
display 14358.06*5000
*72 mi

save "$path\population\dta\population2021_bcb_unique.dta", replace

use "$path\population\dta\population2021_bcb_unique.dta", replace
keep if ranking <= 5000
save "$path\pix\pix-event-study\Stata\dta\ibge\bottom_5000_mun.dta", replace


use "$path\population\dta\population2021_bcb_unique.dta", replace
keep if ranking > 5000
save "$path\pix\pix-event-study\Stata\dta\ibge\top_500_mun.dta", replace








use "$path\pix\pix-event-study\Stata\dta\Base_week_muni_sem_qtd_menor3\Base_week_muni_sem_qtd_menor3.dta", replace
rename muni_cd id_municipio_bcb
merge m:1 id_municipio_bcb using "$path\population\dta\population2021_bcb_unique.dta"
keep if _merge == 3
drop _merge

  
sum qtd_PIX_inflow if ranking <= 5000
sum qtd_PIX_outflow if ranking <= 5000
sum qtd_PIX_intra if ranking <= 5000
display 372000*8000
3,000,000,000

sum qtd_PIX_inflow if ranking > 5000
sum qtd_PIX_outflow if ranking > 5000
sum qtd_PIX_intra if ranking > 5000
display 42450*280000
12 billion

** Lets use weekly data now

**** Restricting the Municipalities

* Do a proper Diff diff - Synthetic control or CEM

* 

**** Not restricting


	clear
	clear mata
	clear matrix
	

	foreach path in D:\Dropbox C:\Dropbox C:\Users\mathe\Dropbox\RESEARCH {
		capture cd `path'
		if _rc == 0 macro def path `path'
	}	


********************************************************************************
* MUN SELECTION
********************************************************************************
use "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_2020_estban_closest_caixa_bank.dta", replace
keep id_municipio quantity_bank
rename id_municipio closest_bank_id_mun 
rename quantity_bank closest_bank_quantity_bank
save "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\temp_closest_caixa_bank.dta", replace

**********
use "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_2020_estban_closest_caixa_bank.dta", replace

drop if quantity_bank > 1
merge m:1 closest_bank_id_mun using "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\temp_closest_caixa_bank.dta"
drop if _merge == 2
drop _merge

replace closest_caixa_d = 0 if closest_caixa_d == .
replace closest_bank_quantity_bank = 1 if closest_bank_quantity_bank == .
drop if closest_bank_quantity_bank>1

* Mun strictly closer to caixa
gen caixa_treatment = 1 if closest_caixa_d == closest_bank_d
replace caixa_treatment = 0 if caixa_treatment == . 
gen treatment = 1 if closest_bank_d < closest_caixa_d
replace treatment = 0 if treatment == . 
tab caixa_treatment
sum populacao if caixa_treatment == 1
sum populacao if caixa_treatment == 0
tab caixa
tab bank 

save "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_selection_closest_caixa_bank.dta", replace
use "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_selection_closest_caixa_bank.dta", replace
keep id_municipio id_municipio_bcb quantity_bank closest_bank_d closest_caixa_d populacao caixa_treatment treatment
save "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_selection.dta", replace

**********
use "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_2020_estban_closest_caixa_bank.dta", replace
merge m:1 closest_bank_id_mun using "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\temp_closest_caixa_bank.dta"
drop if _merge == 2
drop _merge
replace closest_caixa_d = 0 if closest_caixa_d == .
replace closest_bank_quantity_bank = 1 if closest_bank_quantity_bank == .
gen caixa_treatment = 1 if closest_caixa_d == closest_bank_d
replace caixa_treatment = 0 if caixa_treatment == . 
gen treatment = 1 if closest_bank_d < closest_caixa_d
replace treatment = 0 if treatment == . 
save "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\all_mun_closest_caixa_bank.dta", replace

use "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\all_mun_closest_caixa_bank.dta", replace
keep id_municipio id_municipio_bcb quantity_bank closest_bank_d closest_caixa_d populacao caixa_treatment treatment
save "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\all_mun.dta", replace


********************************************************************************

use "$path\pix\pix-event-study\Stata\dta\Base_week_muni_sem_qtd_menor3\Base_week_muni_sem_qtd_menor3.dta", replace
rename muni_cd id_municipio_bcb
merge m:1 id_municipio_bcb using "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_selection.dta"
keep if _merge ==3
drop _merge

sort id_municipio week
gen week2 = week
order id_municipio week week2
gen D = 0
replace D = 1 if week > 3189

drop if week >= 3241

gen transactions_pix = qtd_PIX_inflow + qtd_PIX_outflow + qtd_PIX_intra
gen trans_capita = transactions_pix / populacao
gen log_transactions_pix = log(transactions_pix)

gen value_pix = valor_PIX_inflow + valor_PIX_outflow + valor_PIX_intra
gen value_capita = value_pix / populacao
gen log_value_pix = log(value_pix)


save "$path\pix\pix-event-study\Stata\dta\Base_week_muni_sem_qtd_menor3\regression_ready.dta", replace
*export delimited using "$path\pix\pix-event-study\CSV\regression_ready_pix_ted_boleto.csv", replace

**** ALL MUN

use "$path\pix\pix-event-study\Stata\dta\Base_week_muni_sem_qtd_menor3\Base_week_muni_sem_qtd_menor3.dta", replace
rename muni_cd id_municipio_bcb
merge m:1 id_municipio_bcb using "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\all_mun.dta"
keep if _merge ==3
drop _merge

sort id_municipio week
gen week2 = week
order id_municipio week week2
gen D = 0
replace D = 1 if week > 3189

drop if week >= 3241

gen transactions_pix = qtd_PIX_inflow + qtd_PIX_outflow + qtd_PIX_intra
gen trans_capita = transactions_pix / populacao
gen log_transactions_pix = log(transactions_pix)

gen value_pix = valor_PIX_inflow + valor_PIX_outflow + valor_PIX_intra
gen value_capita = value_pix / populacao
gen log_value_pix = log(value_pix)


save "$path\pix\pix-event-study\Stata\dta\Base_week_muni_sem_qtd_menor3\regression_ready_all_mun.dta", replace
export delimited using "$path\pix\pix-event-study\CSV\regression_ready_pix_ted_boleto_all_mun.csv", replace





*****************************************************************
use "$path\pix\pix-event-study\Stata\dta\Base_week_muni_sem_qtd_menor3\regression_ready.dta", replace

use "$path\pix\pix-event-study\CSV\regression_ready_pix_ted_boleto.csv", clear
reg log_transactions_pix D##caixa_treatment i.week i.id_municipio

****
*Correct this code later
collapse (mean) log_transactions_pix log_value_pix, by(week caixa_treatment)
reshape wide log_transactions_pix log_value_pix, i(week) j(caixa_treatment)

line log_transactions_pix0 log_transactions_pix1 week
line log_value_pix0 log_value_pix1 week
****

******************************************************************


* How can I add controls that dont change over time? Are they all absorbed by the fixed effect on the municipality

*populacao
*i.week 

*i.id_municipio

*closest_caixa_d closest_bank_d








* Drop some observations to do i.id_municipio
tempfile holding
save `holding'

keep id_municipio
duplicates drop

set seed 1234
sample 500, count

merge 1:m id_municipio using `holding'

keep if caixa_treatment == 1 | _merge == 3
drop _merge



 




reg quant_capita D##caixa_treatment populacao i.date i.id_municipio closest_bank_d


****
*Correct this code later
collapse (mean) quantity value, by(date caixa_treatment)
reshape wide quantity value, i(date) j(caixa_treatment)

line quantity0 quantity1 date
line value0 value1 date
****

*****
** diff and diff controlling for population - STATA 17

didregress (quantity populacao) (caixa_treatment), group(id_municipio) time(date)
estat trendplots
estat ptrends
estat granger

*****
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	













*Mun selection
* Our goal is to compare two types of municipality
* type one is strictly closer to caixa. That means it is closer to caixa and any other bank is far away. 
*type two is strictly closer to ONE bank and any other, and that one bank is not caixa. 


	clear
	clear mata
	clear matrix
	

	foreach path in D:\Dropbox C:\Dropbox C:\Users\mathe\Dropbox\RESEARCH {
		capture cd `path'
		if _rc == 0 macro def path `path'
	}	



use "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_2020_estban_closest_caixa_bank.dta", replace
keep id_municipio quantity_bank
rename id_municipio closest_bank_id_mun 
rename quantity_bank closest_bank_quantity_bank
save "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\temp_closest_caixa_bank.dta", replace



use "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_2020_estban_closest_caixa_bank.dta", replace

drop if quantity_bank > 1
merge m:1 closest_bank_id_mun using "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\temp_closest_caixa_bank.dta"
drop if _merge == 2
drop _merge

replace closest_caixa_d = 0 if closest_caixa_d == .
replace closest_bank_quantity_bank = 1 if closest_bank_quantity_bank == .
drop if closest_bank_quantity_bank>1

* Mun strictly closer to caixa
gen caixa_treatment = 1 if closest_caixa_d == closest_bank_d
replace caixa_treatment = 0 if caixa_treatment == . 
tab caixa_treatment
sum populacao if caixa_treatment == 1
sum populacao if caixa_treatment == 0
tab caixa
tab bank 

save "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_selection_closest_caixa_bank.dta", replace
use "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_selection_closest_caixa_bank.dta", replace
keep id_municipio caixa_treatment populacao closest_bank_d closest_caixa_d
save "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_selection.dta", replace


use "$path\pix\dta\Pix\transactions_month.dta", replace 
merge m:1 id_municipio using "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_selection.dta"
keep if _merge ==3
drop _merge

gen date2 = date
gen D = 0
replace D = 1 if date > 735

* 2021m8 seemed to be incomplete
drop if date == 739

* Drop some observations to do i.id_municipio
tempfile holding
save `holding'

keep id_municipio
duplicates drop

set seed 1234
sample 500, count

merge 1:m id_municipio using `holding'

keep if caixa_treatment == 1 | _merge == 3
drop _merge

gen quant_capita = quantity / populacao

reg quant_capita D##caixa_treatment populacao i.date closest_caixa_d closest_bank_d

reg quant_capita D##caixa_treatment populacao i.date i.id_municipio closest_bank_d


****
*Correct this code later
collapse (mean) quantity value, by(date caixa_treatment)
reshape wide quantity value, i(date) j(caixa_treatment)

line quantity0 quantity1 date
line value0 value1 date
****

*****
** diff and diff controlling for population - STATA 17

didregress (quantity populacao) (caixa_treatment), group(id_municipio) time(date)
estat trendplots
estat ptrends
estat granger

*****


******************************************
*LETS DO THE OTHER DIFF DIFF, BASED ON DISTANCE TO CAIXA, CONTROLLED BY DISTANCE TO THE NEAREST.
******************************************




*Mun selection
* Doing this strategy seems to show that being closer to caixa is good, and not bad
* this may happens because closer to caixa are only a few municipalities with a greater population average. 

* LETS DO MUNICIPALITIES CLOSER TO CAIXA VS CLOSER TO OTHER BANKS










* Our goal is to compare two types of municipality
* type one is strictly closer to caixa. That means it is closer to caixa and any other bank is far away. 
*type two is strictly closer to ONE bank and any other, and that one bank is not caixa. 


	clear
	clear mata
	clear matrix
	

	foreach path in D:\Dropbox C:\Dropbox C:\Users\mathe\Dropbox\RESEARCH {
		capture cd `path'
		if _rc == 0 macro def path `path'
	}	

use "$path\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\mun_2020_estban_closest_caixa_bank.dta", replace



































