
*import address_normalized_cleaned_url.csv
*this is the original address transformed to url by Python: make_url.py
import delimited "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\CSV\geocoding\address_normalized_cleaned_url.csv", clear
rename v1 unique_location_id
rename v2 encoded_address

merge 1:1 unique_location_id using "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\address_normalized_cleaned.dta"
drop location_id str_location_id type bank _merge

merge 1:1 unique_location_id using "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\address_normalized.dta"
drop _merge
save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\address_normalized_cleaned_url.dta", replace

* Clean Address data

import delimited "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\CSV\geocoding\address_normalized_cleaned_url_latlong_google.csv", encoding(UTF-8) clear
drop v1
rename input_string encoded_address
rename type type_google
duplicates report
duplicates drop
merge 1:m encoded_address using "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\address_normalized_cleaned_url.dta"
drop _merge

save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\address_normalized_cleaned_url_google.dta", replace

use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\address_normalized_cleaned_url_google.dta", replace
keep latitude longitude accuracy bank type mun_name state unique_location_id
save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\address_normalized_cleaned_url_google2.dta", replace


**************** Now People's Addresses



*import delimited "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\CSV\aux_address\30k_files_latlong\aux_address0.csv", encoding(UTF-8) clear

forvalues i = 0/22 {
	import delimited "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\CSV\aux_address\30k_files_latlong\aux_address`i'.csv", encoding(UTF-8) clear
	rename v1 index
	rename v2 latitude
	rename v3 longitude
	rename v4 confidence
	rename v5 formatted 
	save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\aux_address\30k_files_latlong\aux_address`i'.dta",replace
}
use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\aux_address\30k_files_latlong\aux_address0.dta",replace

forvalues i = 1/22  {
	append using "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\aux_address\30k_files_latlong\aux_address`i'.dta"
}

save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\aux_address\aux_address_partial_results.dta",replace

use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\aux_address\aux_address_partial_results.dta",replace
* Clean 
tab confidence
keep if confidence >=7
save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\aux_address\aux_address_partial_results7.dta",replace

use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\aux_address\aux_address_partial_results7.dta",replace
*merge it back
merge 1:1 index using "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\aux_address\aux_address_cleaned.dta"
keep if _merge == 3
drop _merge
order index index0 latitude longitude confidence formatted address

save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\aux_address\aux_address_partial_results7_cleaned.dta",replace

use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\aux_address\aux_address_partial_results7_cleaned.dta",replace

keep index index0 latitude longitude confidence mun_cd

save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\aux_address\aux_address_partial_results7_super_cleaned.dta",replace

