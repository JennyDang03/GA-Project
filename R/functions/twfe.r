
twfe <- function(graphname,y,control1,control2,fe,dat_list,dat_list_name){
  mod_list <- list()
  for (i in 1:length(dat_list)) {
    dat <- dat_list[[i]]
    dat$Y <- dat[[y]]
    dat$C1 <- dat[[control1]]
    dat$C2 <- dat[[control2]]
    dat$FE <- dat[[fe]]
    
    mod_twfe <- feols(Y ~ i(time_to_treat, treat, ref = -1) + ## Our key interaction: time ? treatment status
                        C1 + C2 |                          ## Control variables
                        muni_cd + time + FE:time,          ## FEs
                      cluster = ~muni_cd,                  ## Clustered SEs
                      lean =TRUE, mem.clean = TRUE,        ## Saves memory and RAM.
                      data = dat)
    
    #mod_twfe$fml <- NULL # this is an attempt to save memory
    saveRDS(mod_twfe, file = file.path(output_path,paste0("/coefficients/",graphname,y,dat_list_name[i],".rds")))
    mod_list[[i]] <- mod_twfe
    # I tried saving the fixest file but it is too heavy. 
    
    coeftable_mod <- summary(mod_twfe)$coeftable
    coefficients <- coeftable_mod[, "Estimate"]
    standard_errors <- coeftable_mod[, "Std. Error"]
    variables <- rownames(coeftable_mod)
    variables <- as.numeric(gsub("time_to_treat::(.+):treat", "\\1", variables))
    ci95_u <- coefficients + qnorm(0.975) * standard_errors  # Upper bound
    ci95_l <- coefficients - qnorm(0.975) * standard_errors  # Lower bound
    #Put variables, coefficients, and standard_errors together and save in RDS
    data_list <- list()
    data_list <- list(variables = variables,
                      coefficients = coefficients,
                      standard_errors = standard_errors,
                      ci95_u = ci95_u,
                      ci95_l = ci95_l)
    # Save the list as an RDS file
    saveRDS(data_list, file = file.path(output_path, paste0("/coefficients/", graphname, y, dat_list_name[i], "2.rds")))
  }
  return(mod_list)
  #return()
}
