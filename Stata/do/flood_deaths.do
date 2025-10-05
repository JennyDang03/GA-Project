*flood_deaths.dta

* Do balance tables

use "C:\Users\mathe\Dropbox\RESEARCH\Natural Disasters\dta\danos_informados_monthly_filled_flood.dta"

collapse (sum) dh*

sum dh_mortos, detail


use "C:\Users\mathe\Dropbox\RESEARCH\Natural Disasters\dta\danos_informados_monthly_filled_flood.dta"

collapse (sum) dh_mortos-pepr_serviçosr


