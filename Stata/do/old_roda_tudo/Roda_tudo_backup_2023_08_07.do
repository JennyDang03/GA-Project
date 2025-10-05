*Roda_tudo


*** Arquivo para documentar a sequencia de do-files
clear all
set more off, permanently 

set emptycells drop

global log "\\sbcdf176\PIX_Matheus$\Stata\log"
global dta "\\sbcdf176\PIX_Matheus$\Stata\dta"
global output "\\sbcdf176\PIX_Matheus$\Output"
global origdata "\\sbcdf176\PIX_Matheus$\DadosOriginais"
global do "\\sbcdf176\PIX_Matheus$\Stata\do"

global dta_BranchExplosion "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta"
global teradata_results "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results"

* ADO 
adopath ++ "D:\ADO"
adopath ++ "//sbcdf060/depep01$/ADO"
adopath ++ "//sbcdf060/depep01$/ado-776e"

******************************************
*
*
*
* Summary
*
*
*
******************************************
* Inputs:

* Monta_base_Muni.do
*   1) "$dta_BranchExplosion\Cartao_week_muni"
*   2) "$dta_BranchExplosion\TED_week_muni"
*	3) "$dta_BranchExplosion\TED_week_muni_QTD_CLI"
*	4) "$dta_BranchExplosion\Boleto_week_muni"
*	5) "$dta_BranchExplosion\Boleto_week_muni_qtd_cli"
*	6) "$dta_BranchExplosion\PIX_week_muni"
*	7) "$dta_BranchExplosion\municipios"

* Importa_PIX_Self_Muni.do
*   1) "$teradata_results\PIX_Muni_Self`year'`m'.csv"

* Monta_base_credito.do
*   1) "dta_BranchExplosion\Credito_muni_mes_PF.dta"
*   2) "dta_BranchExplosion\Credito_muni_mes_PJ.dta"

*Monta_base_Muni_Banco.do
*   1) "$dta_BranchExplosion\PIX_week_muni_banco.dta"
*   2) "$dta_BranchExplosion\Cadastro_IF.dta"

*Monta_base_Muni_Banco_self.do
* Input: 
*   1) "$dta_BranchExplosion\PIX_week_muni_banco_self.dta"

*Importa_CCS_BcoMuni.do
* Input: 
*   1) "$teradata_results\CCS_Muni_IF_PF_estoque`year'`m'.csv"

* Eu acho que os dados de input são gerados pelo Pix_por_Ind_Mes_Sample.R
*Importa_Pix_individuo_sample.do
*   1) "$teradata_results\Pix_mes_ind_self_sample`year'`m'.csv.csv"
*   2) "$teradata_results\Pix_mes_ind_rec_sample`year'`m'.csv"
*   3) "$teradata_results\Pix_mes_ind_pag_sample`year'`m'.csv"








**** NOT RELEVANT ****

*   1) "$dta\natural_disasters_weekly_filled_flood.dta" // Esse é gerado pela pasta C:\Users\mathe\Dropbox\RESEARCH\Natural Disasters, and running \code\0.clean_nat_disaster.do
*   1) "$dta\natural_disasters_monthly_filled_flood.dta" // Esse é gerado pela pasta C:\Users\mathe\Dropbox\RESEARCH\Natural Disasters, and running \code\0.clean_nat_disaster.do
*   2) "$dta\municipios2.dta" // Esse é gerado pela pasta C:\Users\mathe\Dropbox\RESEARCH\Municipios\dta, codigo 0.clean_municipios.do

* Eu acho que os dados de input são gerados pelo Pix_mes_por_ind_rec.sql, Pix_mes_por_ind_pag.sql, Pix_por_Ind_Mes.R
*Importa_Pix_individuo.do
*   1) "$teradata_results\PIX_MES_PAG_`year'`m'.csv"
*   2) "$teradata_results\PIX_MES_REC_`year'`m'.csv"

* Eu acho que os dados de input são gerados pelo Pix_por_Ind_Mes.R, SQL PIX_mes_individuo_self.sql
*Importa_Pix_individuo_self.do
* Input: 
*   1) "$teradata_results\PIX_MES_SELF_`year'`m'.csv"
******************************************


*To do:

* Monta_base_Muni.do
* To do: Update data, maybe get some aggregation to the month level. 
* We need to create the database for self (TED and Boleto missing). 

* monta_base_pix_mun_week_self.do
* To do: it would need TED, Boleto for us to do a Before Pix analysis. 

* Monta_base_credito.do
* To do: it would need TED, Boleto for us to do a Before Pix analysis. 

*Monta_base_Muni_Banco_self.do
* To do: it would need TED, Boleto for us to do a Before Pix analysis. 

*Importa_CCS_BcoMuni.do
* To do: Need to download PJ

*Monta_base_Pix_individuo_sample_flood.do
* To do: Add PJ

*** ESTBAN detalhado 

*** Bank account for individuals

******************************************
*
*
*
* Flood
*
*
*
******************************************
*monta_base_flood_weekly.do

* Input: 
*   1) "$dta\natural_disasters_weekly_filled_flood.dta" // Esse é gerado pela pasta C:\Users\mathe\Dropbox\RESEARCH\Natural Disasters, and running \code\0.clean_nat_disaster.do
*   2) "$dta\municipios2.dta" // Esse é gerado pela pasta C:\Users\mathe\Dropbox\RESEARCH\Municipios\dta, codigo 0.clean_municipios.do

* Output: 
*	1) "$dta\flood_weekly_2020_2022.dta" 
*	2) "$dta\flood_weekly_2019_2020.dta"

* Variables: id_municipio time_id flood muni_cd after_flood date_flood

* The goal: Esse do file cria as variaveis muni_cd after_flood date_flood
* 			So faz sentido usar esse dta se vc quer fazer um event study com essas datas Pre e Pos Pix. 
* 			Pre pix: keep if keep if week >= wofd(mdy(11, 16, 2020)) & week <= wofd(mdy(12, 31, 2022)) 
* 			Pos Pix: keep if week >= wofd(mdy(1, 1, 2018)) & week < wofd(mdy(11, 16, 2020))
* 			Ao fazer merge, a unica variavel que importa é o date_flood para um event study

*To do: 

do "$do\monta_base_flood_weekly.do"
******************************************
*monta_base_flood_monthly.do

* Input: 
*   1) "$dta\natural_disasters_monthly_filled_flood.dta" // Esse é gerado pela pasta C:\Users\mathe\Dropbox\RESEARCH\Natural Disasters, and running \code\0.clean_nat_disaster.do
*   2) "$dta\municipios2.dta" // Esse é gerado pela pasta C:\Users\mathe\Dropbox\RESEARCH\Municipios\dta, codigo 0.clean_municipios.do

* Output: 
*	1) "$dta\flood_monthly_2020_2022.dta"
*	2) "$dta\flood_monthly_2019_2020.dta"

* Variables: id_municipio time_id flood muni_cd after_flood date_flood

* The goal: Esse do file cria as variaveis muni_cd after_flood date_flood
* 			So faz sentido usar esse dta se vc quer fazer um event study com essas datas Pre e Pos Pix. 
* 			Pre pix: keep if time_id >= ym(2018,1) & time_id < ym(2020,11) 
* 			Pos Pix: keep if time_id >= ym(2020,11) & time_id <= ym(2022,12)
* 			Ao fazer merge, a unica variavel que importa é o date_flood para um event study

*To do: 

do "$do\monta_base_flood_weekly.do"
******************************************
*
*
*
* Municipios
*
*
*
******************************************

*do "$do\Importa_PIX_muni.do" // Generates PIX_week_muni.dta

* Also look at Importa_PIX_Muni_Banco -> PIX_week_muni_banco.dta


******************************************
* Monta_base_Muni.do
* Input: 
*   1) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\Cartao_week_muni"
*   2) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\TED_week_muni"
*	3) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\TED_week_muni_QTD_CLI"
*	4) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\Boleto_week_muni"
*	5) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\Boleto_week_muni_qtd_cli"
*	6) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\PIX_week_muni"
*	7) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\municipios"

* Output:
*   1) "$dta\Base_week_muni.dta"

* Variables: muni_cd week valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito valor_TED_intra qtd_TED_intra qtd_cli_TED_rec_PJ qtd_cli_TED_rec_PF qtd_cli_TED_pag_PJ qtd_cli_TED_pag_PF valor_boleto_deb valor_boleto_dinheiro qtd_boleto_deb qtd_boleto_dinheiro valor_boleto valor_boleto_eletronico valor_boleto_ATM valor_boleto_age valor_boleto_corban qtd_boleto qtd_boleto_eletronico qtd_boleto_ATM qtd_boleto_age qtd_boleto_corban qtd_cli_pag_pf_boleto qtd_cli_pag_pj_boleto qtd_cli_rec_pj_boleto valor_PIX_inflow qtd_PIX_inflow n_cli_rec_pf_inflow n_cli_rec_pj_inflow valor_PIX_outflow qtd_PIX_outflow n_cli_pag_pf_outflow n_cli_pag_pj_outflow valor_PIX_intra qtd_PIX_intra n_cli_pag_pf_intra n_cli_rec_pf_intra n_cli_pag_pj_intra n_cli_rec_pj_intra MUN_CD_IBGE codmun_ibge MUN_NU_LATITUDE MUN_NU_LONGITUDE post_pix

* The goal: Cria base com informacoes sobre Pix, boleto, Ted, e Cartao.

* To do: Update data, maybe get some aggregation to the month level. I would prefer number of clients instead of number of clients intra, ...

do "$do\Monta_base_Muni.do"
******************************************
*monta_base_pix_mun_week.do
* Input: 
*   1) "$dta\Base_week_muni.dta"
*   2) "$dta\flood_weekly_2020_2022.dta"
*	3) "$dta\flood_weekly_2019_2020.dta"

* Output:
*   1) "$dta\Base_week_muni_flood.dta"
*   2) "$dta\Base_week_muni_flood_beforePIX.dta"

* Variables: muni_cd week valor_cartao_credito valor_cartao_debito qtd_cli_cartao_debito qtd_cli_cartao_credito valor_TED_intra qtd_TED_intra qtd_cli_TED_rec_PJ qtd_cli_TED_rec_PF qtd_cli_TED_pag_PJ qtd_cli_TED_pag_PF valor_boleto_deb valor_boleto_dinheiro qtd_boleto_deb qtd_boleto_dinheiro valor_boleto valor_boleto_eletronico valor_boleto_ATM valor_boleto_age valor_boleto_corban qtd_boleto qtd_boleto_eletronico qtd_boleto_ATM qtd_boleto_age qtd_boleto_corban qtd_cli_pag_pf_boleto qtd_cli_pag_pj_boleto qtd_cli_rec_pj_boleto valor_PIX_inflow qtd_PIX_inflow n_cli_rec_pf_inflow n_cli_rec_pj_inflow valor_PIX_outflow qtd_PIX_outflow n_cli_pag_pf_outflow n_cli_pag_pj_outflow valor_PIX_intra qtd_PIX_intra n_cli_pag_pf_intra n_cli_rec_pf_intra n_cli_pag_pj_intra n_cli_rec_pj_intra MUN_CD_IBGE codmun_ibge MUN_NU_LATITUDE MUN_NU_LONGITUDE post_pix
* + Log variants: log_var

* The goal: prepare Base_week_muni to receive flood variables.
* 			Then run flood_SA_muni_v1.R

* To do: 

do "$do\monta_base_pix_mun_week.do"
******************************************
* Importa_PIX_Self_Muni.do
* Input: 
*   1) "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\PIX_Muni_Self`year'`m'.csv"

* Output:
*   1) "$dta\PIX_week_muni_self.dta"

* Variables: mun_cd week valor_self_pf valor_self_pj qtd_self_pf qtd_self_pj n_cli_self_pf n_cli_self_pj

* The goal: Importa dados de PIX para si mesmo por Municipio por dia
* 			Agrupa por semana e grava
* 			Repete para municipio do recebedor, do pagador, inflow, outlow e intra-municipio 
* 			importa arquivos mensais csv vindo do Teradata 
* 			Arquivos csv gerados pelos código em R: PIXMuniAggreg.R
* 			Faz conversão de tipos de dados
* 			Agrupa por semana
* 			salva em arquivo dta

* To do: In this file it is possible to aggregate everything by month!

do "$do\Importa_PIX_Self_Muni.do"
******************************************
* monta_base_pix_mun_week_self.do
* Input: 
*   1) "$dta\PIX_week_muni_self.dta"
*   2) "$dta\flood_weekly_2020_2022.dta"
*	3) "$dta\flood_weekly_2019_2020.dta"

* Output:
*   1) "$dta\PIX_week_muni_self_flood.dta"
*   2) "$dta\PIX_week_muni_self_flood_sample10.dta"

* Variables: muni_cd week date_flood valor_self_pf valor_self_pj qtd_self_pf qtd_self_pj n_cli_self_pf n_cli_self_pj log_valor_self_pf log_valor_self_pj log_qtd_self_pf log_qtd_self_pj log_n_cli_self_pf log_n_cli_self_pj

* The goal: To add date_flood to PIX_week_muni_self.dta and prepare it for flood_SA_muni_self_v1.R

* To do: it would need TED, Boleto for us to do a Before Pix analysis.

do "$do\monta_base_pix_mun_week_self.do"
******************************************
* Monta_base_credito.do
* Input: 
*   1) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\Credito_muni_mes_PF.dta"
*   2) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\Credito_muni_mes_PJ.dta"

* Output:
*   1) "$dta\Base_credito_muni.dta"

* Variables: ano_mes codmun_ibge
*			 qtd_cli_total qtd_cli_total_PF qtd_cli_total_PJ
*			 vol_credito_total vol_credito_total_PF vol_credito_total_PJ
* 			 vol_emprestimo_pessoal qtd_cli_emp_pessoal
* 			 vol_cartao qtd_cli_cartao
* 			 + other variables not so important

* The goal: It just merges together Credito_muni_mes_PF with Credito_muni_mes_PJ.

* To do: it would need TED, Boleto for us to do a Before Pix analysis. 

do "$do\Monta_base_credito.do"
******************************************
* Monta_base_credito_flood.do
* Input: 
*   1) "$dta\Base_credito_muni.dta"
*   2) "$dta\flood_monthly_2020_2022.dta"
*	3) "$dta\flood_monthly_2019_2020.dta"
*	4) "$dta\municipios2.dta"

* Output:
*   1) "$dta\Base_credito_muni_flood.dta"
*   2) "$dta\Base_credito_muni_flood_beforePIX.dta"

* Variables: muni_cd time_id date_flood
*			 qtd_cli_total qtd_cli_total_PF qtd_cli_total_PJ
*			 vol_credito_total vol_credito_total_PF vol_credito_total_PJ
* 			 vol_emprestimo_pessoal qtd_cli_emp_pessoal
* 			 vol_cartao qtd_cli_cartao
* 			 + Log variations of it + other variables not so important

* The goal: To add date_flood to Base_credito_muni.dta and prepare it for flood_credito_muni_month.R

* To do: 

do "$do\Monta_base_credito_flood.do"
******************************************
*Monta_base_Muni_Banco.do
* Input: 
*   1) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\PIX_week_muni_banco.dta"
*   2) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\Cadastro_IF.dta"

* Output:
*   1) "$dta\Base_week_muni_banco.dta"

* Variables: IF muni_cd tipo week
*			n_cli_rec n_cli_pag valor_intra_rec qtd_intra_rec valor_intra_pag qtd_intra_pag valor_rec qtd_rec valor_pag qtd_pag
*			id_mun_bank_tipo number_branches controle macroseg_if_txt belong_cong digbank public big_bank tipo_inst post_pix
			
* The goal: prepare PIX_week_muni_banco for a future analysis

* To do: it would need TED, Boleto for us to do a Before Pix analysis. 
 
do "$do\Monta_base_Muni_Banco.do"
******************************************
*Monta_base_Muni_Banco_flood
* Input: 
*   1) "$dta\Base_week_muni_banco.dta"
*   2) "$dta\flood_weekly_2020_2022.dta"

* Output:
*   1) "$dta\Base_muni_banco_flood.dta"
*   2) "$dta\Base_muni_banco_flood_collapsed.dta"
*	3) "$dta\Base_muni_banco_flood_collapsed2.dta"
* Variables: id_mun_bank_tipo IF tipo muni_cd week date_flood
*			tipo muni_cd week date_flood bank_type valor_netflow qtd_netflow n_cli_rec n_cli_pag valor_rec qtd_rec valor_pag qtd_pag valor_ratio qtd_ratio log_valor_ratio log_qtd_ratio
* log_valor_totalflow log_qtd_totalflow valor_totalflow qtd_totalflow

* The goal: To add date_flood to Base_week_muni_banco.dta and prepare it for flood_flow_banco_muni.R

* To do: it would need TED, Boleto for us to do a Before Pix analysis. 

do "$do\Monta_base_Muni_Banco_flood.do"
******************************************
*Monta_base_Muni_Banco_self.do
* Input: 
*   1) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\PIX_week_muni_banco_self.dta"
*   2) "\\sbcdf060\depep$\DEPEPCOPEF\Projetos\BranchExplosion\Stata\dta\Cadastro_IF.dta"

* Output:
*   1) "$dta\Base_muni_banco_self.dta"


* Variables: id_mun_bank_tipo IF muni_cd tipo week tipo_inst big_bank public digbank belong_cong macroseg_if_txt controle number_branches
*			valor_self_pag qtd_self_pag valor_self_rec qtd_self_rec 
			
* The goal: prepare PIX_week_muni_banco_self for a future analysis

* To do: it would need TED, Boleto for us to do a Before Pix analysis. 

do "$do\Monta_base_Muni_Banco_self.do"
******************************************
*Monta_base_Muni_Banco_self_flood.do
* Input: 
*   1) "$dta\Base_muni_banco_self.dta"
*   2) "$dta\flood_weekly_2020_2022.dta"

* Output:
*   1) "$dta\Base_muni_banco_self_flood.dta"
*   2) "$dta\Base_muni_banco_self_flood_collapsed.dta"

* Variables: id_mun_bank_tipo IF tipo muni_cd week date_flood
*			tipo muni_cd week date_flood bank_type
*			valor_self_pag qtd_self_pag valor_self_rec qtd_self_rec valor_self_netflow qtd_self_netflow valor_self_ratio qtd_self_ratio log_valor_self_ratio log_qtd_self_ratio

* The goal: To add date_flood to Base_muni_banco_self_flood.dta and prepare it for flood_flow_banco_muni_self.R

* To do: it would need TED, Boleto for us to do a Before Pix analysis. 

do "$do\Monta_base_Muni_Banco_self_flood.do"
******************************************
*Importa_CCS_BcoMuni.do
* Input: 
*   1) "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\CCS_Muni_IF_PF_estoque`year'`m'.csv"

* Output:
*	1) "$dta\CCS_muni_banco_PF.dta"

* Variables: IF qtd muni_cd time_id

* The goal: to download and create a base with stock of bank accounts per Bank x Muni_cd x Month

* To do: Need to download PJ

do "$do\Importa_CCS_BcoMuni.do"
******************************************
*Monta_base_CCS_muni_banco_PF_flood.do
* Input: 
*   1) "$dta\CCS_muni_banco_PF.dta"
*   2) "$dta\flood_monthly_2020_2022.dta"
*	3) "$dta\flood_monthly_2019_2020.dta"
*	4) "$dta\Cadastro_IF.dta"

* Output:
*   1) "$dta\CCS_muni_banco_PF_flood.dta"
*   2) "$dta\CCS_muni_banco_PF_flood_collapsed.dta"
*	3) "$dta\CCS_muni_banco_PF_flood_beforePIX.dta"
*	4) "$dta\CCS_muni_banco_PF_flood_collapsed_beforePIX.dta"


* Variables: IF qtd muni_cd time_id id_mun_bank log_qtd date_flood tipo_inst bank_type

* The goal: To add date_flood to CCS_muni_banco_PF.dta and prepare it for flood_contas_bancarias_muni.R

* To do: Maybe we can create other types of collapse.

do "$do\Monta_base_CCS_muni_banco_PF_flood.do"
******************************************
*
*
*
* Individual
*
*
*
******************************************
* Os dados de input são gerados pelo Pix_por_Ind_Mes_Sample.R

*Importa_Pix_individuo_sample.do
* Input: 
*   1) "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\Pix_mes_ind_self_sample`year'`m'.csv.csv"
*   2) "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\Pix_mes_ind_rec_sample`year'`m'.csv"
*   3) "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\Pix_mes_ind_pag_sample`year'`m'.csv"

* Output:
*   1) "$dta\Pix_individuo_sample.dta"

* Variables: id value_sent trans_sent muni_cd time_id value_rec trans_rec value_self trans_self

* The goal: To get put together transactions sent, received and self for a sample of individuals - 2 million of CPFs. This code plus a future code cleaning the data  plus a code doing it for PJ will substitute: Importa_Pix_individuo.do, monta_base_pix_individuo.do, Importa_Pix_individuo_self, and monta_base_pix_self_individuo.do

* To do: I am not sure if this includes people that never used pix (YES). Also, this does not include firms. We need to do tsfill, clean, and add flood. 

do "$do\Importa_Pix_individuo_sample.do"

*Superior!!!!!!





******************************************
* Baixar Dados com 
	* Pix_mes_por_ind_rec.sql 
	* Pix_mes_por_ind_pag.sql
* Depois, rodar o programa R 
	* Pix_por_Ind_Mes.R

*Importa_Pix_individuo.do
* Input: 
*   1) "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\PIX_MES_PAG_`year'`m'.csv"
*   2) "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\PIX_MES_REC_`year'`m'.csv"

* Output:
*   1) "$dta\Pix_individuo.dta"

* Variables: id tipo_pessoa time_id muni_cd value_rec trans_rec value_sent trans_sent

* The goal: Aggregate the monthly pix transactions for every person and firm. 

* To do: Note the other form of doing this, Importa_Pix_individuo_sample.do

do "$do\Importa_Pix_individuo.do"
******************************************
*monta_base_pix_individuo.do
* Input: 
*   1) "$dta\Pix_individuo.dta"
*   2) "$dta\flood_monthly_2020_2022.dta"

* Output:
*   1) "$dta\Pix_individuo_cleaned`i'.dta"
*   2) "$dta\Pix_individuo_cleaned`i'_sample1.dta"
*	3) i == 1 and 2 (PF and PJ)

* Variables: id time_id muni_cd value_rec trans_rec value_sent trans_sent after_first_pix_sent after_first_pix_rec sender receiver user date_flood
* 				I create log_trans_rec, log_trans_sent, log_value_sent, log_value_rec in R later to save space.

* The goal: To clean, fill, and add date_flood and new treated variables to Pix_individuo.dta and prepare it for flood_SA_individual_v1.R and flood_SA_individual_v1_PJ.R

* To do: The sample individual is out of a sample of people that eventually use pix. 
*			Needs to add the people that never use pix. I think Jose was working on that
*			The ideal is to gather every CPF in the country, take a 1% sample 
*			and get all pix from the selected 1%

do "$do\monta_base_pix_individuo.do"
*****************************************
* Os dados de input são gerados pelo Pix_por_Ind_Mes.R, SQL PIX_mes_individuo_self.sql

*Importa_Pix_individuo_self.do
* Input: 
*   1) "\\sbcdf060\depep$\DEPEPCOPEF\Teradata slicer\results\PIX_MES_SELF_`year'`m'.csv"

* Output:
*   1) "$dta\Pix_individuo_PJ_self.dta"
*   1) "$dta\Pix_individuo_PF_self.dta"

* Variables: id tipo_pessoa muni_cd time_id value_self trans_self

* The goal: Aggregate the monthly pix transactions for every person and firm to themselves. 

* To do: Note the other form of doing this, Importa_Pix_individuo_sample.do

do "$do\Importa_Pix_individuo_self.do"
*****************************************
*monta_base_pix_self_individuo.do
* Input: 
*   1) "$dta\Pix_individuo_PF_self.dta"
*   2) "$dta\Pix_individuo_PJ_self.dta"
*	3) "$dta\flood_monthly_2020_2022.dta"

* Output:
*   1) "$dta\Pix_individuo_PF_self_cleaned.dta"
*   2) "$dta\Pix_individuo_PJ_self_cleaned.dta"
*	3) "$dta\Pix_individuo_PF_self_cleaned_sample1.dta"
*	4) "$dta\Pix_individuo_PJ_self_cleaned_sample1.dta"

* Variables: id muni_cd time_id value_self trans_self after_first_pix_self user date_flood

* The goal: To clean, fill, and add date_flood and new treated variables to Pix_individuo_PF_self.dta and Pix_individuo_PJ_self.dta, and prepare it for flood_SA_individual_self_v1.R

* To do: The sample individual is out of a sample of people that eventually use pix. 
*			Needs to add the people that never use pix. I think Jose was working on that
*			The ideal is to gather every CPF in the country, take a 1% sample 
*			and get all pix from the selected 1%

do "$do\monta_base_pix_self_individuo.do"
*****************************************


