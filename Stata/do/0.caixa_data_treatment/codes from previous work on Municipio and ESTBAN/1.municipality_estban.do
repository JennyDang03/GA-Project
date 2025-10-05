
* ESTBAN, type of bank, each municipality
	
	
	
/******************************************************************************

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
		
	cd "$path\ESTBAN\dta"
	use "1-estban_AG.dta", replace
	keep codmun cod_munic year month uf municipio cnpj nome_instituicao
	keep if year == 2020 & month == 12
	tab nome_instituicao
	gen caixa = 0
	replace caixa = 1 if nome_instituicao == "CAIXA ECONOMICA FEDERAL"
	gen bank = 1
	rename codmun id_municipio_bcb
	rename cod_munic id_municipio
	gen quantity_bank = 1
	
	collapse (max) caixa bank (sum) quantity_bank, by(id_municipio_bcb id_municipio)
	save "$path\ESTBAN\dta\2-estban_AG_12_2020.dta", replace
	
	use "$path\ESTBAN\dta\2-estban_AG_12_2020.dta", replace
	
	merge 1:1 id_municipio_bcb using "$path\Municipios\dta\municipios2.dta", keepus(id_municipio_bcb id_municipio)
	replace bank = 0 if bank == .
	replace caixa = 0 if caixa == .
	replace quantity_bank = 0 if quantity_bank == .
	drop _merge
	
	*drop if there are two id_municipio_bcb or two id_municipio
	sort id_municipio
    quietly by id_municipio:  gen dup = cond(_N==1,0,_n)
	tab dup
	drop dup
	* Brasilia always gives problems
	drop if id_municipio == 5300108
	
	
	* compare cities with caixa to cities without caixa but other bank
	merge 1:1 id_municipio using "$path\population\dta\population2020.dta", keepus( id_municipio populacao)
	keep if _merge == 3
	drop _merge
	
	save "$path\ESTBAN\dta\3-estban_AG_12_2020.dta", replace
	
	
	use "$path\ESTBAN\dta\3-estban_AG_12_2020.dta", replace
	
	regress populacao caixa quantity_bank
	keep if quantity_bank ==1
	regress populacao caixa
	
	sum populacao if caixa == 1 & quantity_bank ==1
	sum populacao if caixa == 0 & quantity_bank ==1
	
	
	
	
	
	
