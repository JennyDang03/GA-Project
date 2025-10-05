clear
	clear mata
	clear matrix
	

	foreach path in D:\Dropbox C:\Dropbox C:\Users\mathe\Dropbox\RESEARCH {
		capture cd `path'
		if _rc == 0 macro def path `path'
	}	
	
use "$path\Municipios\dta\mun_2020_database2.dta", replace

* merge with ESTBAN
merge 1:1 id_municipio using "$path\ESTBAN\dta\3-estban_AG_12_2020.dta"
keep if _m == 3
drop _m

save "$path\Municipios\dta\mun_2020_estban.dta", replace

* do algorithm to find smallest distance between two coordinates
use "$path\Municipios\dta\mun_2020_estban.dta", replace

**TEST
*generate random = runiform()
*sort random
*generate insample = _n <= 100 
*drop if insample == 0 
*drop random insample 

cd "$path\Municipios\temp"

* save type1 and type2 observation separately
*tempfile main bank1
save "main.dta", replace
save "bank1.dta", replace
use "bank1.dta", replace
keep if bank == 1
rename * *1
gen id1 = _n
save "bank1.dta", replace
use "main.dta", replace
keep if bank == 0
gen id0 = _n

* form all pairwise combinations and calculate distance
cross using "bank1.dta"
geodist x_center y_center x_center1 y_center1, gen(d)
sort id0 d

*Drop todas exceto a menor
bysort id0: gen distance_rank = _n
keep if distance_rank ==1
drop id0 id1 distance_rank

* Name things correctly
drop caixa1 bank1 id_municipio_bcb1 quantity_bank1
rename id_municipio1 closest_bank_id_mun
rename x_center1 closest_bank_x
rename y_center1 closest_bank_y
rename populacao1 closest_bank_pop
rename d closest_bank_d

*save
save "$path\Municipios\dta\mun_2020_estban_closest_bank.dta", replace


***
* do it again on Caixa
***
use "$path\Municipios\dta\mun_2020_estban.dta", replace


cd "$path\Municipios\temp"

* save type1 and type2 observation separately
*tempfile main bank1
save "main.dta", replace
save "caixa1.dta", replace
use "caixa1.dta", replace
keep if caixa == 1
rename * *1
gen id1 = _n
save "caixa1.dta", replace
use "main.dta", replace
keep if caixa == 0
gen id0 = _n

* form all pairwise combinations and calculate distance
cross using "caixa1.dta"
geodist x_center y_center x_center1 y_center1, gen(d)
sort id0 d

*Drop todas exceto a menor
bysort id0: gen distance_rank = _n
keep if distance_rank ==1
drop id0 id1 distance_rank

* Name things correctly
drop caixa1 bank1 id_municipio_bcb1 quantity_bank1
rename id_municipio1 closest_caixa_id_mun
rename x_center1 closest_caixa_x
rename y_center1 closest_caixa_y
rename populacao1 closest_caixa_pop
rename d closest_caixa_d

*save
save "$path\Municipios\dta\mun_2020_estban_closest_caixa.dta", replace

*****
* Merge
*****
use "$path\Municipios\dta\mun_2020_estban.dta", replace

merge 1:1 id_municipio using "$path\Municipios\dta\mun_2020_estban_closest_bank.dta"
replace closest_bank_d = 0 if closest_bank_d == .
drop _merge
merge 1:1 id_municipio using "$path\Municipios\dta\mun_2020_estban_closest_caixa.dta"
drop _merge

save "$path\Municipios\dta\mun_2020_estban_closest_caixa_bank.dta", replace





**********************************************************
*https://janstuhler.files.wordpress.com/2013/06/slidesuc3m.pdf
*geocode3
traveltime3, start(p0) end(p1)
*p1 needs to be x_center,y_center as string





* Shortest distance
local n = _N
forval i = 1/`n' {
		forval j = 1/`n' {
		if  (`i' != `j') & (observation_type[`i']==1) &
(observation_type[`j']==2) {
		local d  = (latitude[`i'] - latitude[`j'])^2 + (longitude[`i'] -
longitude[`j'])^2
		replace bank_2010_1_`j'=`d' in `i'
		if `d' < bank_1_dist_1[`i'] {
								replace bank_1_dist_1 = `d' in `i'
								replace bank_1_id_1 = `j' in `i'
						}
		}
}
}





* save type1 and type2 observation separately
tempfile main type2
save "`main'"
keep if otype == 2
rename * *2
gen id2 = _n
save "`type2'"
use "`main'"
keep if otype == 1
gen id1 = _n

* form all pairwise combinations and calculate distance
cross using "`type2'"
geodist lat lon lat2 lon2, gen(d)
sort id1 d















