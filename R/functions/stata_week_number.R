# Function that converts a date to a Stata week number.
stata_week_number <- function(date) {
  year <- year(date)
  month <- month(date)
  days_from_jan1 <- as.numeric(date - as.Date(paste(year, "01-01", sep = "-")))
  week_number <- 1 + (days_from_jan1 %/% 7)
  week_number <- ifelse(week_number == 53, 52, week_number)
  #if (week_number == 53) { 
  #  week_number <- 52
  #}
  week_number <- week_number + (year-1960)*52 - 1
  return(week_number)
}