* Dist to Caixa and Expected Dist to Caixa


use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\address_normalized_cleaned_url_google2.dta", replace
keep latitude longitude bank
rename * *1
cd "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\temp"
save "allbanks1.dta", replace

use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\aux_address\aux_address_partial_results7_super_cleaned.dta",replace
keep if index == 10
keep latitude longitude index
cross using "allbanks1.dta"
geodist latitude longitude latitude1 longitude1, gen(d_allbanks)
sort index d_allbanks
bysort index: gen distance_rank = _n
merge m:1 distance_rank using "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/dist_caixa/distance_rank_weight.dta"
drop _merge
gen weighted_distance = weight * d_allbanks
gen dist_caixa = 24.978804
gen expected_dist = 18.609229
gen dist_bank = .18508223
*Calculate closest caixa & expected distance to caixa



keep if distance_rank ==1
drop id2 id1 distance_rank
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


