	clear
	clear mata
	clear matrix
	

	foreach path in D:\Dropbox C:\Dropbox C:\Users\mathe\Dropbox\RESEARCH {
		capture cd `path'
		if _rc == 0 macro def path `path'
	}	
	

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

