# Load necessary libraries
library(dplyr)
library(data.table)
library(microbenchmark)
# Set seed for reproducibility
set.seed(123)

# Generate unique ids with constant muni_cd and tipo
ids <- data.frame(
  id = 1:2000,
  muni_cd = sample(1:5, 50, replace = TRUE),
  tipo = sample(1:2, 50, replace = TRUE)
)
generate_ltrans_values <- function(n) {
  ltrans_send <- rbinom(n, 1, 0.01)
  ltrans_rec <- rbinom(n, 1, 0.01)
  ltrans_self <- rbinom(n, 1, 0.01)
  ltrans <- ltrans_send + ltrans_rec + ltrans_self
  return(data.frame(ltrans_send, ltrans_rec, ltrans_self, ltrans))
}
# Generate fake data for each id over multiple weeks
n_weeks <- 200  # Number of weeks
data <- ids %>%
  group_by(id,muni_cd,tipo) %>%
  do(data.frame(
    week = 1:n_weeks,
    generate_ltrans_values(n_weeks)
  ))

# Convert to data.table
data <- as.data.table(data)

# Print the first few rows of the data
head(data)

original_approach <- function(data) {
  data %>%
    group_by(id, muni_cd, tipo) %>%
    mutate(first_pix = ifelse(week == min(week[ltrans > 0]), 1, 0),
           first_send = ifelse(week == min(week[ltrans_send > 0]), 1, 0),
           first_rec = ifelse(week == min(week[ltrans_rec > 0]), 1, 0),
           first_self = ifelse(week == min(week[ltrans_self > 0]), 1, 0)) %>%
    ungroup() %>%
    group_by(week, muni_cd, tipo) %>%
    summarise(adoption = sum(first_pix),
              adoption_send = sum(first_send),
              adoption_rec = sum(first_rec),
              adoption_self = sum(first_self)) %>%
    ungroup() %>%
    mutate(ladoption = log1p(adoption),
           ladopt_send = log1p(adoption_send),
           ladopt_rec = log1p(adoption_rec),
           ladopt_self = log1p(adoption_self))
}
optimized_approach <- function(data) {
  data[, `:=`(
    first_pix = as.integer(week == min(week[ltrans > 0])),
    first_send = as.integer(week == min(week[ltrans_send > 0])),
    first_rec = as.integer(week == min(week[ltrans_rec > 0])),
    first_self = as.integer(week == min(week[ltrans_self > 0]))
  ), by = .(id, muni_cd, tipo)]
  
  data2 <- data[, .(
    adoption = sum(first_pix),
    adoption_send = sum(first_send),
    adoption_rec = sum(first_rec),
    adoption_self = sum(first_self)
  ), by = .(week, muni_cd, tipo)]
  
  data2[, `:=`(
    ladoption = log1p(adoption),
    ladopt_send = log1p(adoption_send),
    ladopt_rec = log1p(adoption_rec),
    ladopt_self = log1p(adoption_self)
  )]
  
  return(data2)
}
data_df <- as.data.frame(data)

# Benchmark the two approaches
benchmark <- microbenchmark(
  original = original_approach(data_df),
  optimized = optimized_approach(data),
  times = 5
)

# Print the benchmark results
print(benchmark)

data[, `:=`(
  first_pix = as.integer(week == min(week[ltrans > 0])),
  first_send = as.integer(week == min(week[ltrans_send > 0])),
  first_rec = as.integer(week == min(week[ltrans_rec > 0])),
  first_self = as.integer(week == min(week[ltrans_self > 0]))
), by = .(id, muni_cd, tipo)]

data2 <- data[, .(
  adoption = sum(first_pix),
  adoption_send = sum(first_send),
  adoption_rec = sum(first_rec),
  adoption_self = sum(first_self)
), by = .(week, muni_cd, tipo)]

data2[, `:=`(
  ladoption = log1p(adoption),
  ladopt_send = log1p(adoption_send),
  ladopt_rec = log1p(adoption_rec),
  ladopt_self = log1p(adoption_self)
)]
