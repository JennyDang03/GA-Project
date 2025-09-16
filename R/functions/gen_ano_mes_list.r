# Function to generate a list of years and months
gen_ano_mes_list <- function(year1, year2) {
  year_list <- list()
  for (year in year1:year2) {
    for (month in 1:12) {
      year_list <- c(year_list, list(year * 100 + month))
    }
  }
  return(year_list)
}