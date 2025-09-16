library(tidyverse)
library(lubridate)
library(tibble)
library(dplyr)
library(haven)

# Variables in the original file:
# id index time_id after_first_pix_rec after_first_pix_sent date_first_pix_rec date_first_pix_sent
# after_adoption, date_adoption
# I add after_event later. 

###################
# after_first_pix_rec after_first_pix_sent date_first_pix_rec date_first_pix_sent
##################

# Set seed for reproducibility
set.seed(123)

# Define number of individuals and time periods
n_individuals <- 10000
n_periods <- 26

# Create panel data frame
panel_data <- tibble(
  id = rep(1:n_individuals, each = n_periods),
  index = rep(1:n_individuals, each = n_periods),
  t = rep(1:n_periods, times = n_individuals),
  dist_caixa = rep(rnorm(n_individuals, mean = 10, sd = 2), each = n_periods),
  expected_distance = rep(rnorm(n_individuals, mean = 10, sd = 2), each = n_periods),
  date = as.Date("2020-11-01") %m+% months(t-1) # create date variable using the starting date "2020-11-01"
)
panel_data$time_id = panel_data$t + 729

panel_data <- panel_data %>%
  group_by(id) %>%
  mutate(
    adoption_date = sample(1:(2*n_periods), 1),
    after_adoption = ifelse(adoption_date <= t, 1, 0)
  ) %>%
  ungroup()

panel_data <- panel_data %>% 
  select(-adoption_date)

# Add after_may variable
panel_data$after_event <- ifelse(month(panel_data$date) >= 5 & year(panel_data$date) >= 2021, 1, 0)

panel_data <- panel_data %>% 
  select(-date)
panel_data <- panel_data %>% 
  select(-t)
# Export panel_data to Stata .dta format
write_dta(panel_data, "C:\\Users\\mathe\\Dropbox\\RESEARCH\\pix\\pix-event-study\\Stata\\dta\\pix\\fake_panel_data.dta")
