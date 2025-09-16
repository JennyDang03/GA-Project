# Function that returns a list of starting days of the week in a given year range
gen_week_list <- function(start_year, end_year) {
  date_list <- list()
  for (y in start_year:end_year){
    start_date <- as.Date(paste(y, "01-01", sep = "-"))
    for (i in 1:52) {
      date_list[[i+(y-start_year)*52]] <- start_date + (i - 1) * 7
    }
  }
  start_date <- as.Date(paste(end_year+1, "01-01", sep = "-"))
  date_list[[1+(end_year+1-start_year)*52]] <- start_date + (1 - 1) * 7
  return(date_list)
}
