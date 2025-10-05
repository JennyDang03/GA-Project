*roda_tudo_pix_week

*** Arquivo para documentar a sequencia de do-files
global do "\\sbcdf176\PIX_Matheus$\Stata\do"

* Baixar Dados com SQL:
	*  
	* 
* Needs to do several steps to load Pix, credit card, debit card, boleto and ted
do "$do\Importa_PIX_muni.do" // Generates PIX_week_muni.dta
* Look as well at Importa_PIX_Muni_Banco


* Puts together these dtas
*    - PIX_week_muni.dta -> Importa_PIX_muni.do
*    - Cartao_week_muni.dta
*    - TED_week_muni.dta
*    - Boleto_week_muni.dta
*    - flood_cem.dta
do "$do\Monta_base_Muni.do"

* Get Flood - do post pix and pre pix
do "$do\monta_base_flood_weekly.do"


* Load Base and add Flood
do "$do\monta_base_pix_mun_week.do"

* Then, do a regression in R
* * flood_SA_muni_v1.r // Does before and after





* Write later about Self -> muni and individuo