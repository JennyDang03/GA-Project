
prepare_data <- function(file,flood_data,fe,controls){
  dat <- read_dta(file.path(dta_path,file))
  setDT(dat)
  if ("time_id" %in% names(dat)) {
    dat$time <- dat$time_id
    dat$month <- time_id_to_month(dat$time_id)
    dat$year <- time_id_to_year(dat$time_id)
  } else if ("week" %in% names(dat)) {
    dat$time <- dat$week
    dat$month <- week_to_month(dat$week)
    dat$year <- week_to_year(dat$week)
  }
  
  # Flood, FE, and Control Variables
  dat <- merge(dat, flood_data, by=c("muni_cd","time"), all=FALSE) # it deletes if no match.
  dat <- merge(dat, fe, by="muni_cd", all.x = TRUE) 
  dat <- merge(dat, controls, by=c("muni_cd","month","year"), all.x = TRUE) 
  
  # Event Study Variables
  dat[, treat := ifelse(is.na(date_flood), 0, 1)]
  dat[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
  dat[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
  dat[, after_flood := ifelse(is.na(date_flood), 0, ifelse(time >= date_flood, 1, 0))]
  
  return(dat)
}
