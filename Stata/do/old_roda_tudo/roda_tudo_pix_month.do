*roda_tudo_pix_month

*** Arquivo para documentar a sequencia de do-files
global do "\\sbcdf176\PIX_Matheus$\Stata\do"

* Baixar Dados com 
	* Pix_mes_por_ind_rec.sql 
	* Pix_mes_por_ind_pag.sql
* Depois, rodar o programa R 
	* Pix_por_Ind_Mes.R
* Depois importar dados do PIX agrupados por individuo (PF ou PJ)
do "$do\Importa_Pix_individuo.do"

* Get Flood - do post pix and pre pix
do "$do\monta_base_flood_monthly.do"

* Create variables to Pix_individuo and add Flood
do "$do\monta_base_pix_individuo.do"

* Then, do a regression in R
* flood_SA_individual_v1.R
* flood_SA_individual_v1_PJ.R


* I can also aggregate PF by mun by month and do the same for PF
* This way I can get some info on by month by PF/PJ by mun


* Then we do the same sequence to Pix municipal level and weekly level
* "$do\roda_tudo_pix_week.do"

* Write later about Self -> muni and individuo