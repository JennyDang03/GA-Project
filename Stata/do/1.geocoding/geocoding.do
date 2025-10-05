******** GEOCODING

* Trim function seems outdated -> strtrim, stritrim
/*
ds, has(type string) 
foreach v in `r(varlist)' { 
    replace `v' = strtrim(`v') 
	replace `v' = stritrim(`v')
} 
*/


use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\bank_address.dta", replace
tab bank

keep cnpj_number branch_id endereço número ///
	complemento bairro cep state mun_name bank location_id type
order bank type endereço número complemento bairro cep  ///
	mun_name state

rename endereço endereco
rename número numero

save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\bank_address_normalized.dta", replace

use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\lotery_address.dta", replace

keep if function4 == 1 
tab bank

rename cnpjdacontratante cnpj_number
rename cnpjdocorrespondente cnpj_loterica 
rename endereço endereco
rename numeroend numero

keep cnpj_number cnpj_loterica ///
	cnpj_loterica endereco numero ///
	complemento bairro cep state mun_name bank type location_id

order bank type endereco numero complemento bairro cep  ///
	mun_name state

save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\lotery_address_normalized.dta", replace

use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\atm_address.dta", replace
tab bank

rename cnpj cnpj_number
rename endereço endereco
rename número numero

keep cnpj_number  ///
	endereco numero ///
	complemento bairro cep state mun_name bank type location_id

order bank type endereco numero complemento bairro cep  ///
	mun_name state

save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\atm_address_normalized.dta", replace


use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\postos_address.dta", replace
tab bank

* Bradesco Posto de atendimento is used only for employees of companies - https://banco.bradesco/html/pessoajuridica/solucoes-integradas/outros/recursos-humanos/posto-de-atendimento-bancario.shtm

*use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\filiais_address.dta", replace
*tab bank

*************************************************************************
* Append

use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\bank_address_normalized.dta", replace

append using "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\lotery_address_normalized.dta"

append using "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\atm_address_normalized.dta"

save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\address_normalized.dta",replace

use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\address_normalized.dta",replace
cap drop str_location_id
tostring location_id, gen(str_location_id)
gen unique_location_id = type + bank + str_location_id
save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\address_normalized.dta",replace


********************************************************************************
use "C:\Users\mathe\Dropbox\RESEARCH\Municipios\dta\municipios2.dta", replace
keep sigla_uf nome_uf
duplicates drop
rename sigla_uf state
rename nome_uf state_name
merge 1:m state using "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\address_normalized.dta"

* Delete complemento - does not help geocoding
keep location_id type bank endereco numero bairro cep mun_name state_name 
order location_id type bank endereco numero bairro cep mun_name state_name
gen country = "Brasil"

******** Endereco Cleaning
replace endereco = " " + endereco 

* There are commas in the Address and no space
replace endereco = subinstr(endereco,",",", ",.)

* After all "." there is no space
replace endereco = subinstr(endereco,".",". ",.)

* if the first few letters are ROD, AV or R
* ROD. rodovia
replace endereco = subinstr(endereco," ROD. ","RODOVIA ",.)
replace endereco = subinstr(endereco," ROD ","RODOVIA ",.)

* AV. or AV becomes avenida
replace endereco = subinstr(endereco," AV. ","AVENIDA ",.)
replace endereco = subinstr(endereco," AV ","AVENIDA ",.)

* R or R. becomes Rua
replace endereco = subinstr(endereco," R. ","RUA ",.)
replace endereco = subinstr(endereco," R ","RUA ",.)

* Exclude S/N SN SN. ", S/N" 
replace endereco = subinstr(endereco," SN."," ",.)
replace endereco = subinstr(endereco," S/N."," ",.)
replace endereco = subinstr(endereco," SN"," ",.)
replace endereco = subinstr(endereco," S/N"," ",.)

* Change - for , 
replace endereco = subinstr(endereco," - ",", ",.)
replace endereco = subinstr(endereco,"-"," ",.)

* NUM - exclude
replace endereco = subinstr(endereco," NUM "," ",.)
replace endereco = subinstr(endereco,"Nº"," ",.)
replace endereco = subinstr(endereco," NO. "," ",.)

******** Numero Cleaning
* SN
replace numero = subinstr(numero,"SN."," ",.)
replace numero = subinstr(numero,"S/N."," ",.)
replace numero = subinstr(numero,"SN"," ",.)
replace numero = subinstr(numero,"S/Nº"," ",.)
replace numero = subinstr(numero,"S/N"," ",.)
replace numero = subinstr(numero,"Nº"," ",.)

* / and - 
replace numero = subinstr(numero,"/"," ",.)
replace numero = subinstr(numero,"-"," ",.)

******** Cep Cleaning
* CEP = 0 -> take them out
replace cep = strtrim(cep) 
replace cep = stritrim(cep)
replace cep= "" if cep == "0"

*********************************** Creating Address
gen address = endereco + ", " + numero + ", " + bairro + ", " +  mun_name + ", " + state_name + ", " + country + ", " + cep

replace address = strtrim(address) 
replace address = stritrim(address)
replace address = subinstr(address,", ,",", ",.)
replace address = subinstr(address,",,",",",.)
replace address = subinstr(address,",  ,",", ",.)
replace address = strtrim(address) 
replace address = stritrim(address)
replace address = subinstr(address,", ,",", ",.)
replace address = strtrim(address) 
replace address = stritrim(address)
replace address = subinstr(address," ,",",",.)
replace address = strtrim(address) 
replace address = stritrim(address)
* Take out the last comma
replace address = address + "  "
replace address = subinstr(address,",  ","",.)
replace address = strtrim(address) 
replace address = stritrim(address)

keep location_id type bank address

tostring location_id, gen(str_location_id)
gen unique_location_id = type + bank + str_location_id

save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\ESTBAN-CAIXA\address_normalized_cleaned.dta", replace

* Go to make_url.py to create address_normalized_cleaned_url.csv

*gen url_address = urlencode(address)

/*
ds, has(type string) 
foreach v in `r(varlist)' { 
    replace `v' = strtrim(`v') 
	replace `v' = stritrim(`v')
} 
*/

* Open a Python session in Stata
/*
python:


pip install numpy
from sfi import Data
import urllib.parse
  
address = np.array(Data.get("address"))
encoded_address = urllib.parse.quote(address)

Data.addVarByte('encoded_address')
Data.store('encoded_address', None, encoded_address)
end
*/

/*
import urllib.parse
import pandas as pd

df = pd.read_stata("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta/ESTBAN-CAIXA/address_normalized_cleaned.dta")
df.head(10)
df["encoded_address"] = df["address"].apply(urllib.parse.quote)

header = ["unique_location_id", "encoded_address"]
df.to_csv("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/CSV/geocoding/address_normalized_cleaned_url.csv",header=False,index=False, columns = header)
*/

********************************************************************************
* Aux Address Cleaning
*import delimited "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\CSV\aux_address\Todos_enderecos_aux_emerg_amostra_muni_index.csv", clear
* Has only 3.3 million addresses
*There is something wrong with this file
* The R read the CSV wrong

import delimited "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\CSV\aux_address\Todos_enderecos_aux_emerg_amostra_muni.csv", clear
* Has 10.8 million addresses
gen index0 = _n

* Error reading the CSV
replace mun_cd = 56119 in 2050459
replace cd_cep = "58238000" in 2050459
drop if _n == 2050460

gen index = _n
 
save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\aux_address\aux_address.dta", replace

* Create mun_name and state_name
use "C:\Users\mathe\Dropbox\RESEARCH\Municipios\dta\municipios2.dta", replace
keep id_municipio_bcb nome nome_uf
rename id_municipio_bcb mun_cd
merge 1:m mun_cd using "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\aux_address\aux_address.dta"

keep if _merge == 3
drop _merge

* Cleaning of variables
ds, has(type string) 
foreach v in `r(varlist)' { 
    replace `v' = strtrim(`v') 
	replace `v' = stritrim(`v')
} 

******** Numero Cleaning
* SN
gen numero = peg_an_numero
replace numero = subinstr(numero,".","",.)
replace numero = subinstr(numero,"Sno","",.)
replace numero = subinstr(numero,"Sn","",.)
replace numero = subinstr(numero,"S no","",.)
replace numero = subinstr(numero,"S n","",.)
replace numero = subinstr(numero,"SNo","",.)
replace numero = subinstr(numero,"SN","",.)
replace numero = subinstr(numero,"S No","",.)
replace numero = subinstr(numero,"S N","",.)
replace numero = subinstr(numero,"S/N","",.)
replace numero = subinstr(numero,"N ","",.)
replace numero = subinstr(numero,"º"," ",.)
*First few zeros
quietly count if usubstr(numero, 1, 1) == "0" 
		while r(N) { 
			replace numero = usubstr(numero, 2, .) if usubstr(numero, 1, 1) == "0" 	
			count if usubstr(numero, 1, 1) == "0" 
		}

rename nome mun_name
rename nome_uf state_name

* create address

gen address = peg_ds_logradouro + ", " + numero + ", " + mun_name + ", " + state_name + ", " + cd_cep + ", " + "Brasil" 

replace address = strtrim(address) 
replace address = stritrim(address)
replace address = subinstr(address,", ,",", ",.)
replace address = subinstr(address,",,",",",.)
replace address = subinstr(address,",  ,",", ",.)
replace address = strtrim(address) 
replace address = stritrim(address)
replace address = subinstr(address,", ,",", ",.)
replace address = strtrim(address) 
replace address = stritrim(address)
replace address = subinstr(address," ,",",",.)

save "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\aux_address\aux_address_cleaned.dta", replace

* Then Go to python make_url.py to encode url and split csv into multiple samples. 




