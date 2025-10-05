******************************************************************
*  Graphs -- Liang, Sampaio, Sarkisyan
******************************************************************



clear all
set more off
set scheme s1color


* Define the main path
local path_main "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\CSV\comparacao entre paises\"

* Read comparacao.xlsx
import excel using "`path_main'comparacao.xlsx", firstrow clear

* Change name from "United kingdom" to "UK"
rename (Unitedkingdom) (UK)
label var UK "UK"
twoway (line Brazil Years, lpattern("l")) (line Australia Years, lpattern("___#")) (line Chile Years, lpattern("_--_#")) (line Denmark Years, lpattern("_-_#")) (line India Years, lpattern("_-")) (line Mexico Years, lpattern("_.._#")) (line Nigeria Years, lpattern("_._#"))  (line Singapore Years, lpattern("_.")) (line Sweden Years, lpattern("__#")) (line UK Years, lpattern("_#")), xtitle("Years after launch") ytitle("Transactions per capita") graphregion(color(white)) xlab(0(1)10) yline(0 50 100 150 200, lcolor(gs14)) legend(rows(3))
	graph export "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output\graphs\comparacao_stata.pdf", replace


* Read cash_comparacao.xlsx
local path_main "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\CSV\comparacao entre paises\"
import excel using "`path_main'cash_comparacao.xlsx", firstrow clear

* Keep only records where Years >= 2018
*keep if Years >= 2018


* make graphs


twoway (line Brazil Years, lpattern("solid")) (line Colombia Years, lpattern("dotted")) (line Mexico Years, lpattern("dotdash")) (line Peru Years, lpattern("twodash")), ytitle("Cash Transactions") graphregion(color(white)) xlabel(2012(2)2022) ylabel(0.2 "20%" 0.4 "40%" 0.6 "60%" 0.8 "80%", angle(90) grid glcolor(gs14)) tline(`=2020', lcolor(black) lpattern("."))

* Save the graph
graph export "C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output\graphs\cash_comparacao.pdf", replace