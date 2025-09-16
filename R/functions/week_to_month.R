# Function to convert week number in stata to month
week_to_month <- function(week_number) {
  year <- week_number %/% 52 + 1960
  week_n <- week_number %% 52 + 1
  first_day <- as.Date((week_n - 1) * 7, origin = paste0(year, "-01-01"))
  month <- month(first_day)
  return(month)
}