# Function to convert a month date number in stata to a year
time_id_to_year <- function(month_number){
  year <- month_number %/% 12 + 1960
  return(year)
}