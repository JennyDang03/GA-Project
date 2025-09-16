
prepare_data2 <- function(file){
  dat <- read_dta(file.path(dta_path,file))
  setDT(dat)
  if ("time_id" %in% names(dat)) {
    dat$time <- dat$time_id
    #dat$month <- time_id_to_month(dat$time_id)
    #dat$year <- time_id_to_year(dat$time_id)
  } else if ("week" %in% names(dat)) {
    dat$time <- dat$week
    #dat$month <- week_to_month(dat$week)
    #dat$year <- week_to_year(dat$week)
  }
  return(dat)
}
