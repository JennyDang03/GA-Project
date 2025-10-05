	
* ESTBAN data on each agency
	
	
	
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
			import delimited `year'0`x'_ESTBAN_AG.CSV, delimiter(";") varnames(3) clear 
			gen month=`x'
			gen year=`year'
			order month year
			cd "$path\ESTBAN\bin\"
			compress
			save "`x'_`year'_AG.dta", replace
		}
		}
	forvalues year=1989(1)2020{
		forvalues x=1(1)9{
			cd "$path\ESTBAN\raw\"
			import delimited `year'0`x'_ESTBAN_AG.CSV, delimiter(";") varnames(3) clear 
			gen month=`x'
			gen year=`year'
			order month year
			cd "$path\ESTBAN\bin\"
			compress
			save "`x'_`year'_AG.dta", replace
		}
		}
	forvalues year=1988(1)2020{
		forvalues x=10(1)12{
			cd "$path\ESTBAN\raw\"
			import delimited `year'`x'_ESTBAN_AG.CSV, delimiter(";") varnames(3) clear 
			gen month=`x'
			gen year=`year'
			order month year
			cd "$path\ESTBAN\bin\"
			compress
			save "`x'_`year'_AG.dta", replace
		}
		}	
		
		
		
		
		
	foreach year in 2021{
		forvalues x=1(1)9{
			cd "$path\ESTBAN\raw\"
			import delimited `year'0`x'_ESTBAN_AG.CSV, delimiter(";") varnames(3) clear 
			gen month=`x'
			gen year=`year'
			order month year
			cd "$path\ESTBAN\bin\"
			compress
			save "`x'_`year'_AG.dta", replace
		}
		}
	foreach year in 2021{
		foreach x in 10{
			cd "$path\ESTBAN\raw\"
			import delimited `year'`x'_ESTBAN_AG.CSV, delimiter(";") varnames(3) clear 
			gen month=`x'
			gen year=`year'
			order month year
			cd "$path\ESTBAN\bin\"
			compress
			save "`x'_`year'_AG.dta", replace
		}
		}	

****** I stopped erasing the dta files for each month so You wont need to do this process several times. Another possibility is to just get raw and append the new data. 
	
	* Append
		cd "$path\ESTBAN\bin\"
		use 7_1988_AG.dta, clear
		
		forvalues y=8(1)12{
			append using `y'_1988_AG.dta
			*erase `y'_1988_AG.dta
		}
		forvalues x= 1989(1)2020{
			forvalues y=1(1)12{
				append using `y'_`x'_AG.dta
				disp "`y'_`x'_AG"
				*erase `y'_`x'_AG.dta
				}
			}	
		foreach x in 2021{
			forvalues y=1(1)10{
				append using `y'_`x'_AG.dta
				disp "`y'_`x'_AG"
				*erase `y'_`x'_AG.dta
				}
			}	
	
	cd "$path\ESTBAN\dta"
	save "0-estban_raw_AG.dta", replace
	

	
	
	
	
	
	
	
	

	
	*** Notice that in the new ESTBAN version some items are missing relative to what we used in the QJE paper. 
	* For example: depositos a vista, depositos personas fisicas, depositos personas juridicas are not reported
	* in this section of the code I compare the old estban extracted for the QJE paper with the most updated estban
	

* I excluded this section since it is on Import_ESTBAN	
	
	
	* RESULT: 
	* The variables in the new and old ESTBAN are the same, but the new ESTBAN contains LESS variables. 
	* This implies we should use the old ESTBAN (QJE paper) to be on the safe side. 
	
	******** READ ABOVE BEFORE RUNNING CODE BELOW
	
	****************************************************************************
	cd "$path\ESTBAN\dta"
	use "0-estban_raw_AG.dta", replace
		
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
	
	keep month year data_base uf codmun cod_munic municipio cnpj nome_inst   ///
		 deposits loans_* assets total_ativo_c399y 
		 
		 *There is no agen_esp agen_proc

	cd "$path\ESTBAN\dta"
	sort cod_munic year month nome_instituicao
	save "1-estban_AG.dta", replace
	
	
	
*** The rest of the code on Import_ESTBAN only collapses the data in several forms
*** Since this is the uncollapsed version of the Import_ESTBAN, I excluded the rest of the code
	
	
	
	
	
	