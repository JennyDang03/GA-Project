	clear
	clear mata
	clear matrix
	

	foreach path in D:\Dropbox C:\Dropbox C:\Users\mathe\Dropbox\RESEARCH {
		capture cd `path'
		if _rc == 0 macro def path `path'
	}	
	
	use "$path\Municipios\dta\municipios.dta", replace
	
	replace id_municipio_bcb = "" if id_municipio_bcb == "NA" 
	destring id_*, replace
	destring ddd, replace
	
	save "$path\Municipios\dta\municipios1.dta", replace
	
	use "$path\Municipios\dta\municipios1.dta", replace
	drop if id_municipio_bcb == .
	save "$path\Municipios\dta\municipios2.dta", replace
	
	
	
	import excel "C:\Users\mathe\Dropbox\RESEARCH\Municipios\raw\Tipologia_municipal_rural_urbano.xlsx", sheet("Tipologia_munic_rural_urbano") firstrow clear
	
	rename CD_GCMUN id_municipio
	rename TIPO tipo
	keep id_municipio tipo
	
	save "$path\Municipios\dta\rural_urban.dta", replace