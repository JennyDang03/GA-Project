*** Arquivo para documentar a sequencia de do-files
global do "\\sbcdf176\PIX_Matheus$\Stata\do"

* Inicialmente, rodar o programa R Pix_por_Ind_Mes.R
* Depois importar dados do PIX agrupados por individuo (PF ou PJ)
do "$do\Importa_Pix_individuo.do"

* Importa dados dos endereços do auxilio emergencial 
* e cria um dta 
do "$do\Importa_todos_end_aux_emerg.do"

* Esse codigo importa  os endereços do auximo emergencial de abril de 2021 com identificacao anonimizada
* e faz o match com os index de endereços
do "$do\Gera_id_addres_index_match.do"

* Esse codigo junta base com uso de Pix para os 5000 menores municipios com 
* com o índice do endereço, e filtra para apenas usuarios do auxilio emergencial de abril-2021 
do "$do\Monta_Base_Pix_Auxilio.do"

* Esse codigo importa dados ja limpos de PIX agrupados por individuo 
* Possui somente de quem:
*     -  teve aux emergencial em abril 21 e 
*     -  está nos 500 menores municipios 
* Então gera as variaveis after_first_pix_XXX e date_first_pix_XXX
* Onde XXX pode ser rec ou sent
* Tambem adiciona dados de localizacao e distancias
 
do "$do\Pix_PF_adoption.do"





