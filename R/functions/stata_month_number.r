#Function to convert ano mes to month number in stata.
stata_month_number <- function(x) {
time_id <- ((x %/% 100) - 1960) * 12 + (x %% 100) - 1
return(time_id)
}