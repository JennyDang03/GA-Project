# Function to convert a month date number in stata to a month
time_id_to_month <- function(month_number){
  month <- month_number %% 12 + 1
  return(month)
}