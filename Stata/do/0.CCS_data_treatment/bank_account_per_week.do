*****
* Work with ccs - number of bank accounts
*****





use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\CCS_estoque_week_muni\CCS_estoque_week_muni.dta", replace

collapse (sum) qtd_contas_PF, by(week)

replace qtd_contas_PF= qtd_contas_PF/214000000
gen week2 = week

line qtd_contas_PF week, ///
	title("Bank accounts over time") ///
	ytitle("Bank accounts per person") xtitle("Weeks") ///
	xline(3134 3167 3189) ///
	xmlabel(3134 "Covid" 3167 "Pix Launch" 3189 "Shock", angle(90))
	*legend(order(1 "Traditional Banks (Poupança)" 2 "Fintechs (CDI)")) ///
	*lc(black blue green red purple) ///
	*xline(3189) ///
	*xmlabel(3189 "Shock", angle(90))
graph export "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output\graphs\bank_accounts.png", as(png) name("Graph") replace



use "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\Base_week_muni_sem_qtd_menor3\regression_ready_all_mun.dta", replace

keep transactions_pix value_pix log_transactions_pix log_value_pix id_municipio id_municipio_bcb week
rename id_municipio_bcb muni_cd

merge 1:1 muni_cd week using "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta\CCS_estoque_week_muni\CCS_estoque_week_muni.dta"
keep if _m==3
drop _m
drop if week < 3189

gen log_qtd_contas_PF = log(qtd_contas_PF)
sum transactions_pix
gen transactions_pix_SD = transactions_pix/`r(sd)'
gen transactions_pix_0000 =  transactions_pix/10000

reg log_qtd_contas_PF transactions_pix_SD
estout

reg log_qtd_contas_PF transactions_pix_0000