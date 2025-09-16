# IV example from:
# https://lost-stats.github.io/Model_Estimation/Research_Design/instrumental_variables.html

# If necessary, install both packages.
install.packages(c('AER','lfe'))
# Load AER
library(AER)

# Load the Cigarettes data from ivreg, following the example
data(CigarettesSW)
# We will be using cigarette taxes as an instrument for cigarette prices
# to evaluate the effect of cigarette price on log number of packs smoked
# With income per capita as a control

# Adjust everything for inflation
CigarettesSW$rprice <- CigarettesSW$price/CigarettesSW$cpi
CigarettesSW$rincome <- CigarettesSW$income/CigarettesSW$population/CigarettesSW$cpi
CigarettesSW$tdiff <- (CigarettesSW$taxs - CigarettesSW$tax)/CigarettesSW$cpi

# The regression formula takes the format
# dependent.variable ~ endogenous.variables + controls | instrumental.variables + controls
ivmodel <- ivreg(log(packs) ~ log(rprice) + log(rincome) | tdiff + log(rincome),
                 data = CigarettesSW)
summary(ivmodel)


# Now we will run the same model with lfe::felm
library(lfe)

# The regression formula takes the format
# dependent variable ~ 
#    controls |
#    fixed.effects | 
#    (endogenous.variables ~ instruments) |
#    clusters.for.standard.errors
# So if need be it is straightforward to adjust this example to account for
# fixed effects and clustering.
# Note the 0 indicating no fixed effects
ivmodel2 <- felm(log(packs) ~ log(rincome) | 0 | (log(rprice) ~ tdiff),
                 data = CigarettesSW)
summary(ivmodel2)

# felm can also use several k-class estimation methods; see help(felm) for the full list.
# Let's run it with a limited-information maximum likelihood estimator with 
# the fuller adjustment set to minimize squared error (4).
ivmodel3 <- felm(log(packs) ~ log(rincome) | 0 | (log(rprice) ~ tdiff),
                 data = CigarettesSW, kclass = 'liml', fuller = 4)
summary(ivmodel3)
