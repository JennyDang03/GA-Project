
ylim_function2 <- function(mod_list, dat_list_name, xlimit_l, xlimit_u){
  ylim_values <- list()
  for (i in 1:length(dat_list_name)) {
    mod <- mod_list[[i]]
    coefficients <- mod$coefficients
    standard_errors <- mod$standard_errors
    variables <- mod$variables
    filtered_coefficients <- coefficients[variables >= xlimit_l & variables <= xlimit_u]
    filtered_standard_errors <- standard_errors[variables >= xlimit_l & variables <= xlimit_u]
    
    ylim_values[[i]] <- range(filtered_coefficients - 2 * filtered_standard_errors, filtered_coefficients + 2 * filtered_standard_errors)
    # 2.576 for 99% CI, 1.96 for 95% CI, 1.645 for 90% CI  
  }
  ylim <- range(unlist(ylim_values))
  return(ylim)
}
