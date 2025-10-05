* Get Stock and Watson Cigarette data
import delimited "https://vincentarelbundock.github.io/Rdatasets/csv/AER/CigarettesSW.csv", clear

* Adjust everything for inflation
g rprice = price/cpi
g rincome = (income/population)/cpi
g tdiff = (taxs - tax)/cpi

* And take logs
g lpacks = ln(packs)
g lrincome = ln(rincome)
g lrprice = ln(rprice)

* The syntax for the regression is
* name_of_estimator dependent_variable controls (endogenous_variable = instruments)
* where name_of_estimator can be two stage least squares (2sls), 
* limited information maximum likelihood (liml, note that ivregress doesn't support k-class estimators), 
* or generalized method of moments (gmm)
* Here we can run two stage least squares
ivregress 2sls lpacks rincome (lrprice = tdiff)

* Or gmm. 
ivregress gmm lpacks rincome (lrprice = tdiff)