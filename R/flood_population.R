#flood_population.R

################################################################################
#flood_population.R
# Input: 
# Output: 
#         
# y: 


# The goal: 


# To do: 

################################################################################

options(download.file.method = "wininet")

#install.packages(c("data.table","fixest","haven","ggplot2"))

library(data.table) ## For some minor data wrangling
library(fixest)     ## NB: Requires version >=0.9.0
library(haven)
library(ggplot2)
library(dplyr)

rm(list = ls()) ## Clear workspace

setwd("//sbcdf176/Pix_Matheus$")

# Set file paths
log_path <- "//sbcdf176/PIX_Matheus$/Stata/log"
dta_path <- "//sbcdf176/PIX_Matheus$/Stata/dta"
output_path <- "//sbcdf176/PIX_Matheus$/Output"
origdata_path <- "//sbcdf176/PIX_Matheus$/DadosOriginais"
R_path <- "//sbcdf176/PIX_Matheus$/R"
#dta_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta"
#output_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output"
#log_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/log"

log_file <- file.path(log_path, "flood_estban.log")
#sink(log_file) ## redirect R output to log file

################################################################################
mun_fe <- read_dta(file.path(dta_path, "mun_fe.dta"))
flood <- read_dta(file.path(dta_path,"annualflood_2013_2022.dta"))
flood <- flood %>%
  group_by(id_municipio) %>%
  summarize(flood = max(flood))
table(flood$flood)

mun_fe <- merge(mun_fe,flood)

ggplot(mun_fe, aes(x = pop2010, y = pop2022, color = factor(flood))) +
  geom_point() +
  labs(title = "Scatter Plot of Population Data",
       x = "pop2010",
       y = "pop2022") +
  scale_color_manual(values = c("1" = "blue", "0" = "red"))

ggplot(mun_fe, aes(x = log(pop2010), y = log(pop2022), color = factor(flood))) +
  geom_point() +
  labs(title = "Scatter Plot of Population Data (Log Scale)",
       x = "log(pop2010)",
       y = "log(pop2022)") +
  scale_color_manual(values = c("1" = "blue", "0" = "red"))


model <- lm((pop2022 - pop2010) / pop2010 ~ factor(flood), data = mun_fe)
summary(model)


# Create a summary data frame
summary_data <- mun_fe %>%
  group_by(flood) %>%
  summarize(mean_growth = mean((pop2022 - pop2010) / pop2010),
            sd_growth = sd((pop2022 - pop2010) / pop2010),
            se_growth = sd_growth / sqrt(n()))

# Calculate confidence intervals (95%)
summary_data <- summary_data %>%
  mutate(
    ci_lower = mean_growth - 1.96 * se_growth,
    ci_upper = mean_growth + 1.96 * se_growth
  )

# Create a bar graph
ggplot(summary_data, aes(x = factor(flood), y = mean_growth)) +
  geom_bar(stat = "identity", position = "dodge", fill = "blue") +
  geom_errorbar(aes(ymin = ci_lower, ymax = ci_upper), width = 0.2, position = position_dodge(0.9)) +
  labs(title = "Population Growth Summary",
       x = "Flood",
       y = "Mean Growth") +
  scale_x_discrete(labels = c("0" = "No Flood", "1" = "Flood"))


