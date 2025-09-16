# Callaway santana example
# https://cran.r-project.org/web/packages/did/vignettes/did-basics.html
# Create the data:
set.seed(1814)
time.periods <- 4
sp$te.e <- 1:time.periods
dta <- build_sim_dataset(sp)
nrow(dta)
head(dta)

# estimate group-time average treatment effects using att_gt method
example_attgt <- att_gt(yname = "Y",
                        tname = "period",
                        idname = "id",
                        gname = "G", # this is when it was treated - first treat
                        xformla = ~X, # this is the covariates Time x Flood Risk, fixed effects.
                        data = dta
)

# summarize the results
summary(example_attgt)

#agg.simple <- aggte(example_attgt, type = "simple")
#summary(agg.simple) # Problem with Small Group Sizes

agg.es <- aggte(example_attgt, type = "dynamic")
summary(agg.es)
ggdid(agg.es)

#agg.gs <- aggte(example_attgt, type = "group")
#summary(agg.gs)
#ggdid(agg.gs)

# To create balanced graphs:
mw.dyn.balance <- aggte(mw.attgt, type = "dynamic", balance_e=1) # The 1 means that the results will be balanced for -2 and for t=1. 
summary(mw.dyn.balance)
ggdid(mw.dyn.balance, ylim = c(-.3,.3))


# To include not yet treated:
example_attgt_altcontrol <- att_gt(yname = "Y",
                                   tname = "period",
                                   idname = "id",
                                   gname = "G",
                                   xformla = ~X,
                                   data = dta,
                                   control_group = "notyettreated"          
)
summary(example_attgt_altcontrol)


