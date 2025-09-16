#Function to convert day to month number in stata.
day_to_stata_month <- function(date) {
    year <- as.numeric(format(date,"%Y"))
    month <- as.numeric(format(date,"%m")) #month(date)
    time_id <- (year - 1960) * 12 + month - 1
return(time_id)
}