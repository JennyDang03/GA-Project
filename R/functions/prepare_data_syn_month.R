#prepare_data_syn_month

prepare_data_syn_month <- function(file,flood_data,fe,identifiers,value_list){
  dat <- read_dta(file.path(dta_path,file))
  setDT(dat)
  dat$time <- dat$week
  dat <- merge(dat, flood_data, by=c("muni_cd","time"), all=FALSE) # it deletes if no match.
  dat[, time := floor(week/4)]
  dat[, date_flood := floor(date_flood/4)]
  
  dat <- dat %>%
    group_by(across(c("date_flood", "muni_cd", "time", all_of(identifiers)))) %>%
    summarise(across(all_of(value_list), sum, na.rm = TRUE)) %>%
    ungroup() %>%
    mutate(across(all_of(value_list), list(l = ~ log1p(.)), .names = "l{col}"))
  
  # create variables
  setDT(dat)
  dat <- merge(dat, fe, by="muni_cd", all.x = TRUE) 
  dat <- dat %>% mutate(constant = 0, Na = NA)
  dat[, treat := ifelse(is.na(date_flood), 0, 1)]
  dat[, time_to_treat := ifelse(treat==1, time - date_flood, 0)]
  dat[, time_id_treated := ifelse(treat==0, 10000, date_flood)] # For Sun and Abraham
  dat[, after_flood := ifelse(is.na(date_flood), 0, ifelse(time >= date_flood, 1, 0))]
  
  return(dat)
}