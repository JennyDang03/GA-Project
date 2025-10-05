	

/******************************************************************************

*       Last revisor: 	JP		

*		Import raw data from ESTBAN

*		- Oct 23 2015: I found out BCB updated ESTBAN data and added bank identifiers	   

* 		- Jan 26 2016: 	I found that some municipality names have some extra spaces which generate multiple obs 
						for the same bank and municipality. Therefore: I exclude from this collapse the "municipio" variable. 
						This will most likely generate different numbers in the final tables with respect to Draft December 2015
						
* 		- 2019: re-downloaded ESTBAN for Brazil Household Debt project. 
				Data has been updated from BCB and now contains branch level data
				
		- 2021:	However: notice that the new data has LESS variables than the previous version
				Some categories are merged: e.g. 141 now includes 141 and 142
				correspondences can be checked in column E of chapter 2 of https://www3.bcb.gov.br/aplica/cosif
				
				
*******************************************************************************/
		
	clear
	clear mata
	clear matrix
	

	foreach path in D:\Dropbox C:\Dropbox C:\Users\mathe\Dropbox {
		capture cd `path'
		if _rc == 0 macro def path `path'
	}
		
****** I stopped erasing the dta files for each month so You wont need to do this process several times. Another possibility is to just get raw and append the new data. 
	foreach year in 1988{
		forvalues x=7(1)9{
			cd "$path\ESTBAN\raw\"
			import delimited `year'0`x'_ESTBAN.CSV, delimiter(";") varnames(3) clear 
			gen month=`x'
			gen year=`year'
			order month year
			cd "$path\ESTBAN\bin\"
			compress
			save "`x'_`year'.dta", replace
		}
		}
	forvalues year=1989(1)2020{
		forvalues x=1(1)9{
			cd "$path\ESTBAN\raw\"
			import delimited `year'0`x'_ESTBAN.CSV, delimiter(";") varnames(3) clear 
			gen month=`x'
			gen year=`year'
			order month year
			cd "$path\ESTBAN\bin\"
			compress
			save "`x'_`year'.dta", replace
		}
		}
	forvalues year=1988(1)2020{
		forvalues x=10(1)12{
			cd "$path\ESTBAN\raw\"
			import delimited `year'`x'_ESTBAN.CSV, delimiter(";") varnames(3) clear 
			gen month=`x'
			gen year=`year'
			order month year
			cd "$path\ESTBAN\bin\"
			compress
			save "`x'_`year'.dta", replace
		}
		}	
		
		
		
		
		
	foreach year in 2021{
		forvalues x=1(1)9{
			cd "$path\ESTBAN\raw\"
			import delimited `year'0`x'_ESTBAN.CSV, delimiter(";") varnames(3) clear 
			gen month=`x'
			gen year=`year'
			order month year
			cd "$path\ESTBAN\bin\"
			compress
			save "`x'_`year'.dta", replace
		}
		}
	foreach year in 2021{
		forvalues x=10(1)12{
			cd "$path\ESTBAN\raw\"
			import delimited `year'`x'_ESTBAN.CSV, delimiter(";") varnames(3) clear 
			gen month=`x'
			gen year=`year'
			order month year
			cd "$path\ESTBAN\bin\"
			compress
			save "`x'_`year'.dta", replace
		}
		}	

****** I stopped erasing the dta files for each month so You wont need to do this process several times. Another possibility is to just get raw and append the new data. 
	
	* Append
		cd "$path\ESTBAN\bin\"
		use 7_1988.dta, clear
		
		forvalues y=8(1)12{
			append using `y'_1988.dta
			*erase `y'_1988.dta
		}
		forvalues x= 1989(1)2020{
			forvalues y=1(1)12{
				append using `y'_`x'.dta
				disp "`y'_`x'"
				*erase `y'_`x'.dta
				}
			}	
		foreach x in 2021{
			forvalues y=1(1)10{
				append using `y'_`x'.dta
				disp "`y'_`x'"
				*erase `y'_`x'.dta
				}
			}	
	
	cd "$path\ESTBAN\dta"
	save "0-estban_raw.dta", replace
	

	
	
	
	
	
	
	
	
	
	
import delimited 201903_ESTBAN_AG.CSV, delimiter(";") varnames(3) clear 
	
	
	*** Notice that in the new ESTBAN version some items are missing relative to what we used in the QJE paper. 
	* For example: depositos a vista, depositos personas fisicas, depositos personas juridicas are not reported
	* in this section of the code I compare the old estban extracted for the QJE paper with the most updated estban
	

	
	use "$path\ESTBAN\dta\0-estban_raw.dta", replace
	keep if month == 12
	collapse (sum) verbete*, by(year)
	describe, varlist
	macro define varlist `r(varlist)'
	foreach v in $varlist {
		format `v' %16.0f
		}
	

	use "$path\Agriculture_Finance\DATA\dta\0-estban_raw.dta", replace
	keep if month == 12
	format var %16.0f
	
	collapse (sum) verbete*, by(year)
	describe, varlist
	macro define varlist `r(varlist)'
	foreach v in $varlist {
		format `v' %16.0f
		}

	
	****************************************************************************

	use "$path\Agriculture_Finance\DATA\dta\0-estban_raw.dta", replace
	keep if month == 12
	drop month
	collapse (sum) agen_esperadas-verbete_899_total_do_passivo, by(codmun year)
	
	keep codmun year agen_esperadas verbete_432_depositos_a_prazo verbete_420_depositos_de_poup verbete_112_depositos_bancarios verbete_162_financiamentos
	save "$path\ESTBAN\bin\temp_estban_codmunyear_qje.dta", replace
	
	
	
	use "$path\ESTBAN\dta\0-estban_raw.dta", replace
	keep if month == 12
	drop month
	collapse (sum) agen_esperadas-verbete_899_total_do_passivo, by(codmun year)
	keep if year<=2014
	
	ren verbete_420_depositos_de_poupanc verbete_420_depositos_de_poup
	foreach var in agen_esperadas verbete_432_depositos_a_prazo verbete_420_depositos_de_poup verbete_112_depositos_bancarios verbete_162_financiamentos {
		ren `var' X`var'
		}
	keep codmun year X*
	
	save "$path\ESTBAN\bin\temp_estban_codmunyear_new.dta", replace
	
	use "$path\ESTBAN\bin\temp_estban_codmunyear_qje.dta", clear
	merge 1:1 codmun year using "$path\ESTBAN\bin\temp_estban_codmunyear_new.dta"
	
	pwcorr agen_esperadas Xagen_esperadas
	pwcorr verbete_432_depositos_a_prazo 		Xverbete_432_depositos_a_prazo
	pwcorr verbete_162_financiamentos 			Xverbete_162_financiamentos
	pwcorr verbete_420_depositos_de_poup 		Xverbete_420_depositos_de_poup
	pwcorr verbete_112_depositos_bancarios 		Xverbete_112_depositos_bancarios
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	* RESULT: 
	* The variables in the new and old ESTBAN are the same, but the new ESTBAN contains LESS variables. 
	* This implies we should use the old ESTBAN (QJE paper) to be on the safe side. 
	
	******** READ ABOVE BEFORE RUNNING CODE BELOW
	
	****************************************************************************
	cd "$path\ESTBAN\dta"
	use "0-estban_raw.dta", replace
		
	* Rename variables (same as in BCB data)    
	rename verbete_420_depositos_de_poupanc 	dpoup_c420y
	rename verbete_432_depositos_a_prazo		dprazo_c432y
	rename verbete_160_operacoes_de_credito		opcred_c160y
	rename verbete_163_fin_rurais_agricul_c		fr_a_ci_c163y
	rename verbete_165_fin_rurais_agricul_c		fr_a_com_c165y
	rename verbete_164_fin_rurais_pecuar_cu		fr_p_ci_c164y
	rename verbete_166_fin_rurais_pecuaria_		fr_p_com_c166y
	rename verbete_169_financiamentos_imobi		imob_c169y
	rename verbete_162_financiamentos 			fin_c162y
	rename verbete_171_outras_operacoes_de_		outras_op_cred_c171y
	rename verbete_172_outros_creditos			outros_cred_c172y
	rename verbete_161_empres_e_tit_descont		emp_tit_desc_c161y
	rename verbete_120_aplic_interfinanc_de		ail_c120y
	rename verbete_130_tit_e_val_mob_e_inst		tvmifd_c130y
	rename verbete_140_rel_interfinanc_e_in 	rii_c140y
	rename verbete_110_encaixe					disp_c110y
	rename verbete_180_arrendamento_mercant		arrend_c180y				
	rename verbete_184_prov_poper_arr_merca		prov_arrend_c184y
	rename verbete_190_outros_valores_e_ben		outros_val_c190y
	rename verbete_200_permanente				permanente_c200y
	rename verbete_399_total_do_ativo			total_ativo_c399y
	rename verbete_899_total_do_passivo			total_passivo_c899y
	
	* total deposits: 
	gen deposits = dpoup_c420y  + dprazo_c432y
		
	* main potential uses of capital: 
	* 1) credit operations
	gen loans_total 	= opcred_c160y
	gen loans_agri		= fr_a_ci_c163y +  fr_a_com_c165y + fr_p_ci_c164y + fr_p_com_c166y  /* agri and pecuaria */
	gen loans_noagri	= loans_total - loans_agri
	gen loans_house 	= imob_c169y
	gen loans_other		= outras_op_cred_c171y + outros_cred_c172y 
	gen loans_fin_emp	= fin_c162y + emp_tit_desc_c161y
	
	tabstat loans_total loans_agri loans_house loans_other loans_fin_emp, stat(sum) format(%16.0f) col(stat)
	tabstat loans_total loans_agri loans_house loans_other loans_fin_emp arrend_c180y, stat(sum) format(%16.0f) col(stat)

	* 2) disponibilidades: 							disp_c110y  (we can probably disregard this, negligeable amount)
	* 3) aplicacoes interfinanceiras de liquidez: 	ail_c120y	(short term funding for liquidity needs)
	* 4) titulos:									tvmifd_c130y
	* 5) relacoes interfin and interdep:			rii_c140y
	* 6) permanente  (not sure what this is)		permanente_c200y
	
	* gen total assets and total liabilities (excluding contas de compencacao and RII)
	gen assets 			=  disp_c110y + ail_c120y + tvmifd_c130y + opcred_c160y + arrend_c180y + prov_arrend_c184y + outros_val_c190y + permanente_c200y 

	gen liab 			= 	 dpoup_c420y + dprazo_c432y ///
						+ verbete_430_depositos_interifnan + verbete_440_rel_interfinanc_e_in ///
						+ verbete_460_obrig_por_emp_e_repa+ verbete_480_obrigacoes_por_receb+ verbete_490_cheques_administrati /// 
						+ verbete_610_patrimonio_liquido+ verbete_710_contas_de_resultado+ verbete_711_contas_credoras + verbete_712_contas_devedoras
	
	* how do assets compare to "total do ativo"?
	gen assets_share 	= assets / total_ativo_c399y
	gen liab_share 		= liab   / total_passivo_c899y
	
	* Merge with municipality identifier:
	* matching is not perfect here, these must be new municipalities created after 2015. tab year if _m == 1 gives all obs in 2015-2018 - MOJUI DOS CAMPOS was the city that was created. 
	
	merge m:1 codmun using "$path\ESTBAN\dta\Bridge-cod_munic_BCB_codmun.dta", keepus(cod_munic)
	drop if _m == 1
	drop _m
	
	order codmun cod_munic year month
	
	compress
	
	keep month year data_base uf codmun cod_munic municipio cnpj nome_inst agen_esp agen_proc  ///
		 deposits loans_* assets total_ativo_c399y 

	cd "$path\ESTBAN\dta"
	sort cod_munic year month nome_instituicao
	save "1-estban.dta", replace
	
	
	
	
	
	****************************************************************************
	cd 	"$path\ESTBAN\dta"
	use  "1-estban.dta", replace
	
	replace cod_munic = floor(cod_munic/10)

	ren cod_munic cod_munic6

	merge m:1 cod_munic using "$path\ESTBAN\dta\Bridge_uf_meso_micro_codmunic.dta"
	drop if _m == 2
	drop _m
	
	ren UF 						state_code
	ren Nome_UF					state_name
	ren Nome_Mesorregião		meso_name
	ren Nome_Microrregião		micro_name
	ren Município				muni_name
	ren total_ativo_c399y		ativo
	

	preserve
			
		collapse (sum) agen_esperadas-assets, by(year month uf state_code meso micro cod_munic)

		order cod_munic micro meso state_code uf year month
		
		ren agen_esperadas  branch_exp
		ren agen_proc		branch_data
		
		label var branch_exp  	"number of branches expected (ESTBAN)"
		label var branch_data  	"number of branches with data (ESTBAN)"
		label var deposits  	"total deposits in BRL (ESTBAN)"
		label var loans_total  	"total loans in BRL (ESTBAN)"
		label var loans_agri  	"total rural loans in BRL (ESTBAN)"
		label var loans_noagri  "total nonrural loans in BRL (ESTBAN)"
		label var loans_house  	"total mortgage loans in BRL (ESTBAN)"
		label var loans_other  	"total other loans in BRL (ESTBAN)"
		label var loans_fin_emp "total financiamentos/emprestamos loans in BRL (ESTBAN)"
		label var assets  		"total assets in BRL (ESTBAN)"
		label var ativo 		"total ativo in BRL (ESTBAN)"
		
		replace assets = . if assets <0
		
		/*
		gen kexp = (deposits - loans_total) / assets
		gen kexp2 = (deposits - loans_total) / ativo	
		winsor kexp, gen(kexp_p99) p(0.01)
		winsor kexp, gen(kexp_p95) p(0.05)
		hist kexp_p95 [w=assets]
		winsor kexp2, gen(kexp2_p95) p(0.05)
		hist kexp2_p95 [w=ativo]
		
		scatter kexp_p95 kexp2_p95 [aw=assets], ms(tiny)
		
		pwcorr kexp_p95 kexp2_p95, sig
		
		*/
		
		
		save  "$path\ESTBAN\dta\ESTBAN_codmunic_monthly.dta", replace
		
		*save  "$path\Brazil_Migration\data\estban\ESTBAN_codmunic_monthly.dta", replace  *************************************
		
	restore 
		
	
	* Merge with AMC and collapse at AMC level 

	preserve
		
		cd $path
		destring cod_munic, replace				
		merge m:1 cod_munic using "$path\ESTBAN\dta\Bridge_cod_munic_AMC.dta"
		keep if _merge == 3
		drop _m
		
		drop if AMC == .
			
		collapse (sum) agen_esperadas-assets, by(state_code state_name AMC year month)

		
		ren agen_esperadas  branch_exp
		ren agen_proc		branch_data
		
		label var branch_exp  	"number of branches expected (ESTBAN)"
		label var branch_data  	"number of branches with data (ESTBAN)"
		label var deposits  	"total deposits in BRL (ESTBAN)"
		label var loans_total  	"total loans in BRL (ESTBAN)"
		label var loans_agri  	"total rural loans in BRL (ESTBAN)"
		label var loans_noagri  "total nonrural loans in BRL (ESTBAN)"
		label var loans_house  	"total mortgage loans in BRL (ESTBAN)"
		label var loans_other  	"total other loans in BRL (ESTBAN)"
		label var loans_fin_emp "total financiamentos/emprestamos loans in BRL (ESTBAN)"
		label var assets  		"total assets in BRL (ESTBAN)"
		label var ativo 		"total ativo in BRL (ESTBAN)"
		
		save  "$path\ESTBAN\dta\ESTBAN_AMC_monthly.dta", replace
		
	restore 
	
	
	
	****************************************************************************
	****************************************************************************
	****************************************************************************
	****************************************************************************
	****************************************************************************
	****************************************************************************
	
	cd $path
	destring cod_munic, replace				
	merge m:1 cod_munic using "$path\ESTBAN\dta\Bridge_cod_munic_AMC.dta"
	keep if _merge == 3
	drop _m
	
	drop if AMC == .

	*preserve
			
		collapse (sum) agen_esperadas-assets, by(year month uf state_code meso micro cod_munic AMC)

		order AMC cod_munic micro meso state_code uf year month
		
		ren agen_esperadas  branch_exp
		ren agen_proc		branch_data
		
		label var branch_exp  	"number of branches expected (ESTBAN)"
		label var branch_data  	"number of branches with data (ESTBAN)"
		label var deposits  	"total deposits in BRL (ESTBAN)"
		label var loans_total  	"total loans in BRL (ESTBAN)"
		label var loans_agri  	"total rural loans in BRL (ESTBAN)"
		label var loans_noagri  "total nonrural loans in BRL (ESTBAN)"
		label var loans_house  	"total mortgage loans in BRL (ESTBAN)"
		label var loans_other  	"total other loans in BRL (ESTBAN)"
		label var loans_fin_emp "total financiamentos/emprestamos loans in BRL (ESTBAN)"
		label var assets  		"total assets in BRL (ESTBAN)"
		label var ativo 		"total ativo in BRL (ESTBAN)"
		
		replace assets = . if assets <0
		
		/*
		gen kexp = (deposits - loans_total) / assets
		gen kexp2 = (deposits - loans_total) / ativo	
		winsor kexp, gen(kexp_p99) p(0.01)
		winsor kexp, gen(kexp_p95) p(0.05)
		hist kexp_p95 [w=assets]
		winsor kexp2, gen(kexp2_p95) p(0.05)
		hist kexp2_p95 [w=ativo]
		
		scatter kexp_p95 kexp2_p95 [aw=assets], ms(tiny)
		
		pwcorr kexp_p95 kexp2_p95, sig
		
		*/
		
		sort  state_code AMC year month  
		save  "$path\ESTBAN\dta\ESTBAN_codmunic_AMC_monthly.dta", replace
		
	*restore 
	
	
	
	
	
	
	
	