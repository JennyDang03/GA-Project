 # Load Auxiliary data


path_main <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/"
path_main <- "//sbcdf176/PIX_Matheus$/"

path_query <- paste0(path_main, "R/DataExtraction/")
path_data <- paste0(path_main, "DadosOriginais/")
path_dta <- paste0(path_main, "Stata/dta/")
path_output <- paste0(path_main, "Output/")
log_path <- paste0(path_main, "Stata/log/")
dta_path <- paste0(path_main, "Stata/dta/")
output_path <- paste0(path_main, "Output/")
origdata_path <- paste0(path_main, "DadosOriginais/")
R_path <- paste0(path_main, "R/")

# Constants
xl <- -26
xu <- 52
xl_balanced <- -26
xu_balanced <- 52
xl_balanced_covid <- -13
xu_balanced_covid <- 13
xl_month <- -6
xu_month <- 12
xl_balanced_month <- -6
xu_balanced_month <- 12
xl_balanced_covid_month <- -3
xu_balanced_covid_month <- 3
########################################################################################

# Load estatic Fixed Effects ############################################################################
mun_fe <- read_dta(file.path(dta_path, "mun_fe.dta"))
mun_fe <- mun_fe %>%
  dplyr::select(-id_municipio)
# Load time varying Fixed Effects ############################################################################
mun_control <- read_dta(paste0(dta_path,"mun_control.dta"))
mun_control <- mun_control %>%
  dplyr::select(-id_municipio)

# Load Weekly Flood data - After Pix
flood_week_after <- read_dta(paste0(dta_path,"flood_weekly_2020_2022.dta")) 
flood_week_after <- flood_week_after %>%
  dplyr::select(muni_cd, week, date_flood) %>%
  dplyr::rename(time = week)
# Load Weekly Flood data - Before Pix
flood_week_before <- read_dta(paste0(dta_path,"flood_weekly_2018_2020.dta"))
flood_week_before <- flood_week_before %>%
  dplyr::select(muni_cd, week, date_flood) %>%
  dplyr::rename(time = week)
# Load Monthly Flood data - After Pix
flood_month_after <- read_dta(paste0(dta_path,"flood_monthly_2020_2022.dta"))  
flood_month_after <- flood_month_after %>%
  dplyr::select(muni_cd, time_id, date_flood) %>%
  dplyr::rename(time = time_id)
# Load Monthly Flood data - Before Pix
flood_month_before <- read_dta(paste0(dta_path,"flood_monthly_2018_2020.dta")) 
flood_month_before <- flood_month_before %>%
  dplyr::select(muni_cd, time_id, date_flood) %>%
  dplyr::rename(time = time_id)

# Load Weekly Flood data - After Pix
flood_week_after2023 <- read_dta(paste0(dta_path,"flood_weekly_2020_2023.dta")) # 2023
flood_week_after2023 <- flood_week_after2023 %>%
  dplyr::select(muni_cd, week, date_flood) %>%
  dplyr::rename(time = week)
# Load Weekly Flood data - Before Pix
flood_week_before2019 <- read_dta(paste0(dta_path,"flood_weekly_2019_2020.dta"))
flood_week_before2019 <- flood_week_before2019 %>%
  dplyr::select(muni_cd, week, date_flood) %>%
  dplyr::rename(time = week)
# Load Monthly Flood data - After Pix
flood_month_after2023 <- read_dta(paste0(dta_path,"flood_monthly_2020_2023.dta")) # 2023 
flood_month_after2023 <- flood_month_after2023 %>%
  dplyr::select(muni_cd, time_id, date_flood) %>%
  dplyr::rename(time = time_id)
# Load Monthly Flood data - Before Pix
flood_month_before2019 <- read_dta(paste0(dta_path,"flood_monthly_2019_2020.dta")) 
flood_month_before2019 <- flood_month_before2019 %>%
  dplyr::select(muni_cd, time_id, date_flood) %>%
  dplyr::rename(time = time_id)

# Flood covid. 
# Load Weekly Flood data - After Pix
flood_week_after_covid <- read_dta(paste0(dta_path,"flood_weekly_202011_202106.dta")) 
flood_week_after_covid <- flood_week_after_covid %>%
  dplyr::select(muni_cd, week, date_flood) %>%
  dplyr::rename(time = week)
# Load Weekly Flood data - Before Pix
flood_week_before_covid <- read_dta(paste0(dta_path,"flood_weekly_202003_202010.dta"))
flood_week_before_covid <- flood_week_before_covid %>%
  dplyr::select(muni_cd, week, date_flood) %>%
  dplyr::rename(time = week)
# Load Monthly Flood data - After Pix
flood_month_after_covid <- read_dta(paste0(dta_path,"flood_monthly_202011_202106.dta"))
flood_month_after_covid <- flood_month_after_covid %>% 
  dplyr::select(muni_cd, time_id, date_flood) %>% 
  dplyr::rename(time = time_id)
# Load Monthly Flood data - Before Pix
flood_month_before_covid <- read_dta(paste0(dta_path,"flood_monthly_202003_202010.dta")) 
flood_month_before_covid <- flood_month_before_covid %>%
  dplyr::select(muni_cd, time_id, date_flood) %>%
  dplyr::rename(time = time_id)



# BALANCE
# do balance 6 months to 12 months. something like this.
# and do for 3 months. so we can zoom in.
#Balanced Flood for 6 months.
municipios2 <- read_dta(paste0(dta_path,"municipios2.dta"))
natural_disasters_week <- read_dta(paste0(dta_path,"natural_disasters_weekly_filled_flood.dta"))
natural_disasters_week <- merge(natural_disasters_week, municipios2, by="id_municipio", all= FALSE)
natural_disasters_week <- natural_disasters_week %>% rename(time = week)
natural_disasters_month <- read_dta(paste0(dta_path,"natural_disasters_monthly_filled_flood.dta"))
natural_disasters_month <- merge(natural_disasters_month, municipios2, by="id_municipio", all= FALSE)
natural_disasters_month <- natural_disasters_month %>% rename(time = date)

balance_flood_data <- function(flood_data,natural_disasters,ll,ul) {
    flood_balanced <- natural_disasters %>% 
      rename(muni_cd = id_municipio_bcb) %>%
      filter(time >= ll & time <= ul) %>%  # 3165 =  wofd(mdy(11, 16, 2020)),  3275 = wofd(mdy(12, 31, 2022)) on stata
      mutate(flood = ifelse(number_disasters > 0, 1, 0)) %>% 
      group_by(muni_cd) %>% 
      mutate(temp = ifelse(any(flood > 0, na.rm = TRUE), min(time[flood > 0], na.rm = TRUE), NA),
             date_flood = ifelse(any(flood > 0, na.rm = TRUE), max(temp, na.rm = TRUE), NA),
             after_flood = ifelse(time >= date_flood, 1, 0)) %>% 
      dplyr::select(muni_cd, time, date_flood) %>% 
      arrange(muni_cd, time) %>% 
      ungroup()
    temp <- flood_data %>%
      dplyr::select(muni_cd, time)
    flood_balanced <- merge(flood_balanced, temp, by=c("muni_cd","time"), all=TRUE)
    flood_balanced <- flood_balanced %>% 
      group_by(muni_cd) %>% fill(date_flood, .direction = "downup") %>% ungroup()
    return(flood_balanced)
}

# 6 months before and 12 months after
flood_week_after_balanced <- balance_flood_data(flood_week_after, natural_disasters_week, 3165+26, 3275-52) # 3165 =  wofd(mdy(11, 16, 2020)),  3275 = wofd(mdy(12, 31, 2022)) on stata
flood_week_before_balanced <- balance_flood_data(flood_week_before, natural_disasters_week, 3016+26, 3165-52-1) # 3016 = wofd(mdy(1, 1, 2018)), 3165 = wofd(mdy(11, 16, 2020)) on stata
flood_month_after_balanced <- balance_flood_data(flood_month_after, natural_disasters_month, 730+6, 755-12) # 730 = ym(2020,11), 755 = ym(2022,12) on stata
flood_month_before_balanced <- balance_flood_data(flood_month_before, natural_disasters_month, 696+6, 730-12-1) # 696 = ym(2018,1), 730 = ym(2020,11) on stata

# 6 months before and 12 months after
flood_week_after_balanced2023 <- balance_flood_data(flood_week_after2023, natural_disasters_week, 3165+26, 3327-52) # 3165 =  wofd(mdy(11, 16, 2020)),  3327 = wofd(mdy(12, 31, 2023)) on stata
flood_week_before_balanced2019 <- balance_flood_data(flood_week_before2019, natural_disasters_week, 3068+26, 3165-52-1) # 3068 = wofd(mdy(1, 1, 2019)), 3165 = wofd(mdy(11, 16, 2020)) on stata
flood_month_after_balanced2023 <- balance_flood_data(flood_month_after2023, natural_disasters_month, 730+6, 767-12) # 730 = ym(2020,11), 767 = ym(2023,12) on stata
flood_month_before_balanced2019 <- balance_flood_data(flood_month_before2019, natural_disasters_month, 708+6, 730-12-1) # 708 = ym(2019,1), 730 = ym(2020,11) on stata

# Covid
flood_week_after_balanced_covid <- balance_flood_data(flood_week_after_covid, natural_disasters_week, 3165+13, 3202-13) # 3165 =  wofd(mdy(11, 16, 2020)),  3202 = wofd(mdy(07, 31, 2021)) on stata
flood_week_before_balanced_covid <- balance_flood_data(flood_week_before_covid, natural_disasters_week, 3128+13, 3165-13-1) # 3128 = wofd(mdy(03, 01, 2020)), 3165 = wofd(mdy(11, 16, 2020)) on stata
flood_month_after_balanced_covid <- balance_flood_data(flood_month_after_covid, natural_disasters_month, 730+3, 737-3) # 730 = ym(2020,11), 737 = ym(2021,06) on stata
flood_month_before_balanced_covid <- balance_flood_data(flood_month_before_covid, natural_disasters_month, 722+3, 730-3-1) # 722 = ym(2020,03), 730 = ym(2020,11) on stata

# Set as data.table
setDT(mun_fe)
setDT(mun_control)
setDT(flood_week_after)
setDT(flood_week_before)
setDT(flood_month_after)
setDT(flood_month_before)
setDT(flood_week_after_balanced)
setDT(flood_week_before_balanced)
setDT(flood_month_after_balanced)
setDT(flood_month_before_balanced)
setDT(flood_week_after2023)
setDT(flood_week_before2019)
setDT(flood_month_after2023)
setDT(flood_month_before2019)
setDT(flood_week_after_balanced2023)
setDT(flood_week_before_balanced2019)
setDT(flood_month_after_balanced2023)
setDT(flood_month_before_balanced2019)
setDT(flood_week_after_covid)
setDT(flood_week_before_covid)
setDT(flood_month_after_covid)
setDT(flood_month_before_covid)
setDT(flood_week_after_balanced_covid)
setDT(flood_week_before_balanced_covid)
setDT(flood_month_after_balanced_covid)
setDT(flood_month_before_balanced_covid)
