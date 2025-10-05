use "C:\Users\mathe\Dropbox\RESEARCH\Pix vs CBDC project\data\data_main\estban.dta", replace

gen top5 = 0
replace top5 = 1 if (cnpj == 360305 | cnpj == 60701190 | cnpj == 90400888 | cnpj == 60746948 |cnpj == 0)

gen branches = 1

collapse (sum) encaixe-captaceos_mercado branches, by(top5 date)

keep date top5 deposits branches

reshape wide deposits branches, i(date) j(top5)
 
gen deposits_share = deposits1/(deposits1+deposits0)
gen branches_share = branches1/(branches1+branches0)
 


line deposits_share date
line branches_share date


foreach var of varlist encaixe-captaceos_mercado branches {
	
}
