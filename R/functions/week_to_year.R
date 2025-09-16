#Function to convert week number in stata to year
week_to_year <- function(week_number) {
  year <- week_number %/% 52 + 1960
  return(year)
}