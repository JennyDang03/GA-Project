
ylim_function <- function(mod_list, dat_list, xlimit_l, xlimit_u){
  ylim_values <- list()
  for (i in 1:length(dat_list)) {
    mod <- mod_list[[i]]
    coeftable_mod <- summary(mod)$coeftable
    coefficients <- coeftable_mod[, "Estimate"]
    standard_errors <- coeftable_mod[, "Std. Error"]
    variables <- rownames(coeftable_mod)
    variables <- as.numeric(gsub("time_to_treat::(.+):treat", "\\1", variables))
    
    filtered_coefficients <- coefficients[variables >= xlimit_l & variables <= xlimit_u]
    filtered_standard_errors <- standard_errors[variables >= xlimit_l & variables <= xlimit_u]
    
    ylim_values[[i]] <- range(filtered_coefficients - 2 * filtered_standard_errors, filtered_coefficients + 2 * filtered_standard_errors)
    # 2.576 for 99% CI, 1.96 for 95% CI, 1.645 for 90% CI  
  }
  ylim <- range(unlist(ylim_values))
  return(ylim)
}