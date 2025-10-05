* https://docs.iza.org/dp13524.pdf
*bacondecomp

webuse set www.damianclarke.net/stata/
webuse bacon_example, clear
generate timeToTreat = year - _nfd

sort stfips year
list stfips year _nfd timeToTreat in 1/10, noobs sepby(stfips) abbreviate(11)

* Using reg
#delimit ;
	quietly
	eventdd asmrs pcinc asmrh cases i.year i.stfips, timevar(timeToTreat)
	method( , cluster(stfips)) graph_op(ytitle("Suicides per 1m women")
	xlabel(-20(5)25));
#delimit cr

matrix list e(leads)

* Now using reghdfe
#delimit ;
	eventdd asmrs pcinc asmrh cases, timevar(timeToTreat)
	method(hdfe, absorb(i.stfips i.year) cluster(stfips))
	graph_op(ytitle("Suicides per 1m women") xlabel(-20(5)25));
#delimit cr


estat leads
estat lags
estat eventdd

#delimit ;
	quietly
	eventdd asmrs pcinc asmrh cases i.year, timevar(timeToTreat) inrange leads(10)
	lags(10) method(fe, cluster(stfips)) graph_op(ytitle("Suicides per 1m women"));
#delimit cr

#delimit ;
	quietly
	eventdd asmrs pcinc asmrh cases i.year, timevar(timeToTreat) balanced
	method(fe, cluster(stfips)) graph_op(ytitle("Suicides per 1m women"));
#delimit cr

#delimit ;
	eventdd asmrs pcinc asmrh cases, timevar(timeToTreat) keepbal(stfips)
	leads(15) lags(10) method(hdfe, absorb(i.stfips i.year) cluster(stfips))
	graph_op(ytitle("Suicides per 1m women"));
#delimit cr

#delimit ;
	quietly
	eventdd asmrs pcinc asmrh cases i.year, timevar(timeToTreat) accum leads(15)
	lags(10) method(fe, cluster(stfips)) graph_op(ytitle("Suicides per 1m women"));
#delimit cr

#delimit ;
	quietly
	eventdd asmrs pcinc asmrh cases i.year, timevar(timeToTreat) accum leads(15)
	lags(10) noend method(fe, cluster(stfips))
	graph_op( ytitle("Suicides per 1m women"));
#delimit cr

#delimit ;
	quietly
	eventdd asmrs pcinc asmrh cases i.year, timevar(timeToTreat) ci(rarea)
	method(fe, cluster(stfips)) graph_op(ytitle("Suicides per 1m women")
	xlabel(-20(5)25));
#delimit cr