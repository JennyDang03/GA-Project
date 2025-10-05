* Regression equations

** With Distance to Caixa 

PIX_{m,t} = \alpha_m + \alpha_t + \beta (D_{t} \times ExtraDist_{m}) + \delta DistBank_{m}+\gamma Controls_{m,t}+\epsilon_{m,t}


PIX_{i,t} = \alpha_i + \alpha_t + 
\beta_{0} D_{t} + \beta_1 DistCaixa_{i} + \beta_2 DistBank_i + \beta_3 (D_{t} \times DistCaixa_{i}) + \beta_4 (D_{t} \times DistBank_i) + \gamma Controls_{i,t} + \epsilon_{i,t}







** With Dummy 

* DummyCaixa_ is closest_caixa

PIX_{m,t} = \alpha_m + \alpha_t + \beta_0 D_{t} + \beta_1 DummyCaixa_{m} + \beta_2 DistBank_{m} + \beta_3 (D_{t} \times DummyCaixa_{m}) + \beta_4 (D_{t} \times DistBank_{m}) + \gamma Controls_{m,t}+\epsilon_{m,t}

xtset id_municipio week
xi: regress PIX i.id_municipio i.week i.D*i.closest_caixa closest_bank_d








PIX_{i,t} = \alpha_i + \alpha_t + 
\beta_{0} D_{t} + \beta_1 DummyCaixa_{i} + \beta_2 DistBank_i + \beta_3 (D_{t} \times DummyCaixa_{i}) + \beta_4 (D_{t} \times DistBank_i) + \gamma Controls_{i,t} + \epsilon_{i,t}


xtset id week
xi: regress PIX i.id i.week i.D*i.closest_caixa closest_bank_d

* i.D*i.closest_caixa is three things:  i.D i.closest_caixa i.D*i.closest_caixa
* maybe I should add ", r" to the regression. One guy from princeton was using. I dont know why


xtset id week
xi: regress PIX i.id i.week i.D i.closest_caixa closest_bank_d i.D*i.closest_caixa i.D*closest_bank_d



