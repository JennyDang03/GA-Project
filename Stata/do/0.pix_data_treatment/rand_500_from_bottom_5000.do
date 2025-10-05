*random 500 municipalities out of the bottom 5000

forvalues i = 1(1)10{
    use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ibge\bottom_5000_mun.dta", replace
	set seed 1234
	cap drop rand
	gen rand = runiform()
	sort rand
	gen rand_ranking = _n
	
	* gen the csv with 500 random municipalities
	keep if rand_ranking > 500*(`i' - 1) & rand_ranking <= 500 * `i'
	keep id_municipio_bcb rand_ranking
	gen i = 1
	reshape wide id_municipio_bcb, i(i) j(rand_ranking)
	drop i
	export delimited using "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ibge\rand_500_from_bottom_5000_`i'.csv", delimiter(",") novarnames replace
}

use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ibge\top_500_mun.dta", replace
keep id_municipio_bcb ranking
gen i = 1
reshape wide id_municipio_bcb, i(i) j(ranking)
drop i
export delimited using "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ibge\top500.csv", delimiter(",") novarnames replace