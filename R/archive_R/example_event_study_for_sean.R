

# Load data!
dat_flood <- read_dta(file.path(dta_path,"DATA.dta"))
# converts to data.table
setDT(dat_flood)

#Create Treat and Time to Treat
dat_flood[, treat := ifelse(is.na(date_flood), 0, 1)]
dat_flood[, time_to_treat := ifelse(treat==1, week - date_flood, 0)]

# Following Sun and Abraham, 
# We give never-treated units a fake "treatment" date far outside the relevant study period.
dat_flood[, time_id_treated := ifelse(treat==0, 10000, date_flood)]

# Set Limits
xlimit_low <- -12 
xlimit_up <- 24

# Question
################################################################################
# I need to limit the graph to -12 weeks before the flood and +24 weeks after.
# I can run the regression with no limits and then just cut the graph down. 
# However, that uses too much RAM. I remember you mentioned that you can set: 
# time_to_treat == -12 if time_to_treat<=-12
# In this case I am cutting the data in -16, and +32 and showing just -12, +24
xlimits <- seq(ceiling(xlimit_low*1.333),ceiling(xlimit_up*1.333),by=1)
dat_flood <- subset(dat_flood,time_to_treat %in% xlimits)
################################################################################

mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                   muni_cd + week,            ## FEs
                 cluster = ~muni_cd,                          ## Clustered SEs
                 data = dat)

mod_sa = feols(Y ~ sunab(time_id_treated, week) 
                         muni_cd + week,            ## FEs
                       cluster = ~muni_cd,
                       data = dat_subset)


png(file.path(output_path,paste0("Graph_name.png")), width = 640*4, height = 480*4, res = 200)
iplot(mod_sa, sep = 0.5, ref.line = -1,
      xlab = 'Week',
      main = main_title,
      ci_level = 0.90, xlim = c(xlimit_l,xlimit_u))
legend("bottomleft", col = c(1), pch = c(20), 
       legend = c("Ticker 1"), cex = 0.8)
dev.off()



# The regression formula takes the format
# dependent vairable ~ 
#    controls |
#    fixed.effects | 
#    (endogenous.variables ~ instruments) |
#    clusters.for.standard.errors
# So if need be it is straightforward to adjust this example to account for
# fixed effects and clustering.
# Note the 0 indicating no fixed effects

ivmodel2 <- felm(n_account_stock ~ 0 | id + time_id + muni_cd:time_id | 
                 (user?? ~ after_flood) | 
                 id, data = dat_iv)


