################################################################################
#flood_flow_banco_muni.R
# Input: Base_muni_banco_flood_collapsed.dta
#        
# Output: "banco_muni_flood_",y,"_PF.png"
#         "banco_muni_flood_",y,"_PJ.png"
# y: log_valor_ratio, log_qtd_ratio


# The goal: Create a graph with 3 lines: traditional, digital, others. 
# Then we calculate changes after a flood on y
# for PJ and PF, for Before and After Pix. 

# To do: we can separate btw low deposit rate, high deposit rate
#        we need to create another dta collapsed to all banks to see the reaction of flood on all banks. Maybe put before and after in the same graph.
#        Maybe I exclude the other banks before making the graphs. Also, I like the qtd_net because it would force one or the other to go down.
################################################################################


options(download.file.method = "wininet")

#install.packages(c("data.table","fixest","haven","ggplot2"))

library(data.table) ## For some minor data wrangling
library(fixest)     ## NB: Requires version >=0.9.0
library(haven)
library(ggplot2)

rm(list = ls()) ## Clear workspace

setwd("//sbcdf176/Pix_Matheus$")

# Set file paths
log_path <- "//sbcdf176/PIX_Matheus$/Stata/log"
dta_path <- "//sbcdf176/PIX_Matheus$/Stata/dta"
output_path <- "//sbcdf176/PIX_Matheus$/Output"
origdata_path <- "//sbcdf176/PIX_Matheus$/DadosOriginais"
R_path <- "//sbcdf176/PIX_Matheus$/R"
#dta_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/dta"
#output_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output/tables"
#log_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Stata/log"

log_file <- file.path(log_path, "flood_flow_banco_muni.log")
#sink(log_file) ## redirect R output to log file

################################################################################
graph_function2 <- function(y,dat){
  dat$Y <- dat[[y]]
  
  #mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
  #                   muni_cd + week,            ## FEs
  #                 cluster = ~muni_cd,                          ## Clustered SEs
  #                 data = dat)
  
  dat_subset <- subset(dat,bank_type %in% c(1) & tipo %in% c(1))
  
  # See `?sunab`.
  mod_sa_traditional = feols(Y ~ sunab(time_id_treated, week) | ## The only thing that's changed
                               muni_cd + week,            ## FEs
                             cluster = ~muni_cd,
                             data = dat_subset)
  
  dat_subset <- dat[bank_type %in% c(2) & tipo %in% c(1)]
  
  # See `?sunab`.
  mod_sa_digital = feols(Y ~ sunab(time_id_treated, week) | ## The only thing that's changed
                           muni_cd + week,            ## FEs
                         cluster = ~muni_cd,
                         data = dat_subset)
  #dat_subset <- dat[bank_type %in% c(3) & tipo %in% c(1)]
  
  # See `?sunab`.
  #mod_sa_others = feols(Y ~ sunab(time_id_treated, week) | ## The only thing that's changed
  #                        muni_cd + week,            ## FEs
  #                      cluster = ~muni_cd,
  #                      data = dat_subset)
  
  return(list(mod_sa_traditional, mod_sa_digital))
}

graph_function3 <- function(y,dat){
  dat$Y <- dat[[y]]
  
  #mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
  #                   muni_cd + week,            ## FEs
  #                 cluster = ~muni_cd,                          ## Clustered SEs
  #                 data = dat)
  
  dat_subset <- subset(dat,bank_type %in% c(1) & tipo %in% c(2))
  
  # See `?sunab`.
  mod_sa_traditional = feols(Y ~ sunab(time_id_treated, week) | ## The only thing that's changed
                               muni_cd + week,            ## FEs
                             cluster = ~muni_cd,
                             data = dat_subset)
  
  dat_subset <- dat[bank_type %in% c(2) & tipo %in% c(2)]
  
  # See `?sunab`.
  mod_sa_digital = feols(Y ~ sunab(time_id_treated, week) | ## The only thing that's changed
                           muni_cd + week,            ## FEs
                         cluster = ~muni_cd,
                         data = dat_subset)
  #dat_subset <- dat[bank_type %in% c(3) & tipo %in% c(2)]
  
  # See `?sunab`.
  #mod_sa_others = feols(Y ~ sunab(time_id_treated, week) | ## The only thing that's changed
  #                        muni_cd + week,            ## FEs
  #                      cluster = ~muni_cd,
  #                      data = dat_subset)
  
  return(list(mod_sa_traditional, mod_sa_digital))
}

graph_function4 <- function(y,dat,xlimit_l,xlimit_u, main_title){
  png(file.path(output_path,paste0("banco_muni_flood_",y,"_PF.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function2(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Week',
        main = main_title,
        ci_level = 0.90, xlim = c(xlimit_l,xlimit_u))

  legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
         legend = c("Traditional", "Digital"), cex = 0.8)
  dev.off()
}

graph_function5 <- function(y,dat,xlimit_l,xlimit_u, main_title){
  png(file.path(output_path,paste0("banco_muni_flood_",y,"_PJ.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function3(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Week',
        main = main_title,
        ci_level = 0.90, xlim = c(xlimit_l,xlimit_u))

  legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
         legend = c("Traditional", "Digital"), cex = 0.8)
  dev.off()
}


# Whole collapse
graph_function22 <- function(y,dat){
  dat$Y <- dat[[y]]
  
  #mod_twfe = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
  #                   muni_cd + week,            ## FEs
  #                 cluster = ~muni_cd,                          ## Clustered SEs
  #                 data = dat)
  
  dat_subset <- subset(dat, tipo %in% c(1))
  
  # See `?sunab`.
  mod_sa_people = feols(Y ~ sunab(time_id_treated, week) | ## The only thing that's changed
                          muni_cd + week,            ## FEs
                        cluster = ~muni_cd,
                        data = dat_subset)
  
  dat_subset <- dat[tipo %in% c(2)]
  
  # See `?sunab`.
  mod_sa_firms = feols(Y ~ sunab(time_id_treated, week) | ## The only thing that's changed
                         muni_cd + week,            ## FEs
                       cluster = ~muni_cd,
                       data = dat_subset)
  #dat_subset <- dat[bank_type %in% c(3) & tipo %in% c(1)]
  
  # See `?sunab`.
  #mod_sa_others = feols(Y ~ sunab(time_id_treated, week) | ## The only thing that's changed
  #                        muni_cd + week,            ## FEs
  #                      cluster = ~muni_cd,
  #                      data = dat_subset)
  
  return(list(mod_sa_people, mod_sa_firms))
}

graph_function42 <- function(y,dat,xlimit_l,xlimit_u, main_title){
  png(file.path(output_path,paste0("banco_muni_flood_total",y,"_PF.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function22(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Week',
        main = main_title,
        ci_level = 0.90, xlim = c(xlimit_l,xlimit_u))
  
  legend("bottomleft", col = c(1, 2), pch = c(20, 17), 
         legend = c("People", "Firms"), cex = 0.8)
  dev.off()
}

# TWFE, with and without extra controls: id_regiao_intermediaria:Time

#No control
#pf
graph_function_pf_nocontrol <- function(y,dat){
  dat$Y <- dat[[y]]
  
  dat_subset <- subset(dat,bank_type %in% c(1) & tipo %in% c(1))
  mod_twfe_traditional = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                     muni_cd + week,            ## FEs
                   cluster = ~muni_cd,                          ## Clustered SEs
                   data = dat_subset)  
  
  dat_subset <- dat[bank_type %in% c(2) & tipo %in% c(1)]
  mod_twfe_digital = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                                 muni_cd + week,            ## FEs
                               cluster = ~muni_cd,                          ## Clustered SEs
                               data = dat_subset)  
  
  #dat_subset <- dat[bank_type %in% c(3) & tipo %in% c(1)]
  #mod_sa_others = feols(Y ~ sunab(time_id_treated, week) | ## The only thing that's changed
  #                        muni_cd + week,            ## FEs
  #                      cluster = ~muni_cd,
  #                      data = dat_subset)
  
  return(list(mod_twfe_traditional, mod_twfe_digital))
}
#pj
graph_function_pj_nocontrol <- function(y,dat){
  dat$Y <- dat[[y]]
 
  dat_subset <- subset(dat,bank_type %in% c(1) & tipo %in% c(2))
  mod_twfe_traditional = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                                 muni_cd + week,            ## FEs
                               cluster = ~muni_cd,                          ## Clustered SEs
                               data = dat_subset)  
  
  dat_subset <- dat[bank_type %in% c(2) & tipo %in% c(2)]
  mod_twfe_digital = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                             muni_cd + week,            ## FEs
                           cluster = ~muni_cd,                          ## Clustered SEs
                           data = dat_subset)  
  #dat_subset <- dat[bank_type %in% c(3) & tipo %in% c(2)]
  #mod_sa_others = feols(Y ~ sunab(time_id_treated, week) | ## The only thing that's changed
  #                        muni_cd + week,            ## FEs
  #                      cluster = ~muni_cd,
  #                      data = dat_subset)
  
  return(list(mod_twfe_traditional, mod_twfe_digital))
}
#Control
#pf
graph_function_pf_control <- function(y,dat){
  dat$Y <- dat[[y]]
  
  dat_subset <- subset(dat,bank_type %in% c(1) & tipo %in% c(1))
  mod_twfe_traditional = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                                 muni_cd + week + flood_risk:week + rural_urban:week + nome_regiao_code:week + pop2010_quart:week + capital_uf:week,            ## FEs
                               cluster = ~muni_cd,                          ## Clustered SEs
                               data = dat_subset)  
  
  dat_subset <- dat[bank_type %in% c(2) & tipo %in% c(1)]
  mod_twfe_digital = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                             muni_cd + week + flood_risk:week + rural_urban:week + nome_regiao_code:week + pop2010_quart:week + capital_uf:week,            ## FEs
                           cluster = ~muni_cd,                          ## Clustered SEs
                           data = dat_subset)  
  
  #dat_subset <- dat[bank_type %in% c(3) & tipo %in% c(1)]
  #mod_sa_others = feols(Y ~ sunab(time_id_treated, week) | ## The only thing that's changed
  #                        muni_cd + week,            ## FEs
  #                      cluster = ~muni_cd,
  #                      data = dat_subset)
  
  return(list(mod_twfe_traditional, mod_twfe_digital))
}
#pj
graph_function_pj_control <- function(y,dat){
  dat$Y <- dat[[y]]
  
  dat_subset <- subset(dat,bank_type %in% c(1) & tipo %in% c(2))
  mod_twfe_traditional = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                                 muni_cd + week + flood_risk:week + rural_urban:week + nome_regiao_code:week + pop2010_quart:week + capital_uf:week,            ## FEs
                               cluster = ~muni_cd,                          ## Clustered SEs
                               data = dat_subset)  
  
  dat_subset <- dat[bank_type %in% c(2) & tipo %in% c(2)]
  mod_twfe_digital = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                             muni_cd + week + flood_risk:week + rural_urban:week + nome_regiao_code:week + pop2010_quart:week + capital_uf:week,            ## FEs
                           cluster = ~muni_cd,                          ## Clustered SEs
                           data = dat_subset)  
  #dat_subset <- dat[bank_type %in% c(3) & tipo %in% c(2)]
  #mod_sa_others = feols(Y ~ sunab(time_id_treated, week) | ## The only thing that's changed
  #                        muni_cd + week,            ## FEs
  #                      cluster = ~muni_cd,
  #                      data = dat_subset)
  
  return(list(mod_twfe_traditional, mod_twfe_digital))
}

#pf
graph_function_pf_control1 <- function(y,dat){
  dat$Y <- dat[[y]]
  
  dat_subset <- subset(dat,bank_type %in% c(1) & tipo %in% c(1))
  mod_twfe_traditional = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                                 muni_cd + week + flood_risk:week,            ## FEs
                               cluster = ~muni_cd,                          ## Clustered SEs
                               data = dat_subset)  
  
  dat_subset <- dat[bank_type %in% c(2) & tipo %in% c(1)]
  mod_twfe_digital = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                             muni_cd + week + flood_risk:week,            ## FEs
                           cluster = ~muni_cd,                          ## Clustered SEs
                           data = dat_subset)  
  
  #dat_subset <- dat[bank_type %in% c(3) & tipo %in% c(1)]
  #mod_sa_others = feols(Y ~ sunab(time_id_treated, week) | ## The only thing that's changed
  #                        muni_cd + week,            ## FEs
  #                      cluster = ~muni_cd,
  #                      data = dat_subset)
  
  return(list(mod_twfe_traditional, mod_twfe_digital))
}
#pj
graph_function_pj_control1 <- function(y,dat){
  dat$Y <- dat[[y]]
  
  dat_subset <- subset(dat,bank_type %in% c(1) & tipo %in% c(2))
  mod_twfe_traditional = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                                 muni_cd + week + flood_risk:week,            ## FEs
                               cluster = ~muni_cd,                          ## Clustered SEs
                               data = dat_subset)  
  
  dat_subset <- dat[bank_type %in% c(2) & tipo %in% c(2)]
  mod_twfe_digital = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                             muni_cd + week + flood_risk:week,            ## FEs
                           cluster = ~muni_cd,                          ## Clustered SEs
                           data = dat_subset)  
  #dat_subset <- dat[bank_type %in% c(3) & tipo %in% c(2)]
  #mod_sa_others = feols(Y ~ sunab(time_id_treated, week) | ## The only thing that's changed
  #                        muni_cd + week,            ## FEs
  #                      cluster = ~muni_cd,
  #                      data = dat_subset)
  
  return(list(mod_twfe_traditional, mod_twfe_digital))
}


graph_function_pfandpj <- function(y,dat,xlimit_l,xlimit_u, main_title){
  #PF
  png(file.path(output_path,paste0("banco_muni_flood_",y,"_PFnocontrol.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function_pf_nocontrol(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Week',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5),
        col = c("green4", "blue4"), pch = c(20,17), cex = 0.9)
  
  legend("bottomleft", col = c("green4", "blue4"), pch = c(20,17), 
         legend = c("Traditional", "Digital"), cex = 0.8)
  dev.off()
  
  # png(file.path(output_path,paste0("banco_muni_flood_",y,"_PFcontrol.png")), width = 640*4, height = 480*4, res = 200)
  # iplot(graph_function_pf_control(y,dat), sep = 0.5, ref.line = -1,
  #       xlab = 'Week',
  #       main = main_title,
  #       ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5),
  #       col = c("green4", "blue4"), pch = c(20,17), cex = 0.9)
  # 
  # legend("bottomleft", col = c("green4", "blue4"), pch = c(20,17), 
  #        legend = c("Traditional", "Digital"), cex = 0.8)
  # dev.off()
  
  png(file.path(output_path,paste0("banco_muni_flood_",y,"_PFcontrol1.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function_pf_control1(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Week',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5),
        col = c("green4", "blue4"), pch = c(20,17), cex = 0.9)
  
  legend("bottomleft", col = c("green4", "blue4"), pch = c(20,17), 
         legend = c("Traditional", "Digital"), cex = 0.8)
  dev.off()
  #PJ
  png(file.path(output_path,paste0("banco_muni_flood_",y,"_PJnocontrol.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function_pj_nocontrol(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Week',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5),
        col = c("green4", "blue4"), pch = c(20,17), cex = 0.9)
  
  legend("bottomleft", col = c("green4", "blue4"), pch = c(20,17), 
         legend = c("Traditional", "Digital"), cex = 0.8)
  dev.off()
  
  # png(file.path(output_path,paste0("banco_muni_flood_",y,"_PJcontrol.png")), width = 640*4, height = 480*4, res = 200)
  # iplot(graph_function_pj_control(y,dat), sep = 0.5, ref.line = -1,
  #       xlab = 'Week',
  #       main = main_title,
  #       ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5),
  #       col = c("green4", "blue4"), pch = c(20,17), cex = 0.9)
  # 
  # legend("bottomleft", col = c("green4", "blue4"), pch = c(20,17), 
  #        legend = c("Traditional", "Digital"), cex = 0.8)
  # dev.off()
  
  png(file.path(output_path,paste0("banco_muni_flood_",y,"_PJcontrol1.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function_pj_control1(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Week',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5),
        col = c("green4", "blue4"), pch = c(20,17), cex = 0.9)
  
  legend("bottomleft", col = c("green4", "blue4"), pch = c(20,17), 
         legend = c("Traditional", "Digital"), cex = 0.8)
  dev.off()
  
}

# Whole collapse People+Firms

graph_function_nocontrol <- function(y,dat){
  dat$Y <- dat[[y]]

  dat_subset <- subset(dat, bank_type %in% c(1))
  mod_twfe_traditional = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                                 muni_cd + week,            ## FEs
                               cluster = ~muni_cd,                          ## Clustered SEs
                               data = dat_subset)  
  
  dat_subset <- dat[bank_type %in% c(2)]
  mod_twfe_digital = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                             muni_cd + week,            ## FEs
                           cluster = ~muni_cd,                          ## Clustered SEs
                           data = dat_subset)    
  
  #dat_subset <- dat[bank_type %in% c(3) & tipo %in% c(1)]
  #mod_sa_others = feols(Y ~ sunab(time_id_treated, week) | ## The only thing that's changed
  #                        muni_cd + week,            ## FEs
  #                      cluster = ~muni_cd,
  #                      data = dat_subset)
  
  return(list(mod_twfe_traditional, mod_twfe_digital))
}
graph_function_control <- function(y,dat){
  dat$Y <- dat[[y]]
  
  dat_subset <- subset(dat, bank_type %in% c(1))
  mod_twfe_traditional = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                                 muni_cd + week + flood_risk:week + rural_urban:week + nome_regiao_code:week + pop2010_quart:week + capital_uf:week,            ## FEs
                               cluster = ~muni_cd,                          ## Clustered SEs
                               data = dat_subset)  
  
  dat_subset <- dat[bank_type %in% c(2)]
  mod_twfe_digital = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                             muni_cd + week + flood_risk:week + rural_urban:week + nome_regiao_code:week + pop2010_quart:week + capital_uf:week,            ## FEs
                           cluster = ~muni_cd,                          ## Clustered SEs
                           data = dat_subset)    
  
  #dat_subset <- dat[bank_type %in% c(3) & tipo %in% c(1)]
  #mod_sa_others = feols(Y ~ sunab(time_id_treated, week) | ## The only thing that's changed
  #                        muni_cd + week,            ## FEs
  #                      cluster = ~muni_cd,
  #                      data = dat_subset)
  
  return(list(mod_twfe_traditional, mod_twfe_digital))
}

graph_function_control1 <- function(y,dat){
  dat$Y <- dat[[y]]
  
  dat_subset <- subset(dat, bank_type %in% c(1))
  mod_twfe_traditional = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                                 muni_cd + week + flood_risk:week,            ## FEs
                               cluster = ~muni_cd,                          ## Clustered SEs
                               data = dat_subset)  
  
  dat_subset <- dat[bank_type %in% c(2)]
  mod_twfe_digital = feols(Y ~ i(time_to_treat, treat, ref = -1) | ## Our key interaction: time ? treatment status
                             muni_cd + week + flood_risk:week,            ## FEs
                           cluster = ~muni_cd,                          ## Clustered SEs
                           data = dat_subset)    
  
  #dat_subset <- dat[bank_type %in% c(3) & tipo %in% c(1)]
  #mod_sa_others = feols(Y ~ sunab(time_id_treated, week) | ## The only thing that's changed
  #                        muni_cd + week,            ## FEs
  #                      cluster = ~muni_cd,
  #                      data = dat_subset)
  
  return(list(mod_twfe_traditional, mod_twfe_digital))
}

graph_function_collapse <- function(y,dat,xlimit_l,xlimit_u, main_title){
  png(file.path(output_path,paste0("banco_muni_flood",y,"_nocontrol.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function_nocontrol(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Week',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5))
  
  legend("bottomleft", col = c("green4", "blue4"), pch = c(1,2), 
         legend = c("Traditional", "Digital"), cex = 0.8)
  dev.off()
  
  # png(file.path(output_path,paste0("banco_muni_flood",y,"_control.png")), width = 640*4, height = 480*4, res = 200)
  # iplot(graph_function_control(y,dat), sep = 0.5, ref.line = -1,
  #       xlab = 'Week',
  #       main = main_title,
  #       ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5),
  #       col = c("green4", "blue4"), pch = c(20,17), cex = 0.9)
  # 
  # legend("bottomleft", col = c("green4", "blue4"), pch = c(20,17), 
  #        legend = c("Traditional", "Digital"), cex = 0.8)
  # dev.off()
  
  png(file.path(output_path,paste0("banco_muni_flood",y,"_control1.png")), width = 640*4, height = 480*4, res = 200)
  iplot(graph_function_control1(y,dat), sep = 0.5, ref.line = -1,
        xlab = 'Week',
        main = main_title,
        ci_level = 0.95, xlim = c(xlimit_l-0.5,xlimit_u+0.5),
        col = c("green4", "blue4"), pch = c(20,17), cex = 0.9)
  
  legend("bottomleft", col = c("green4", "blue4"), pch = c(20,17), 
         legend = c("Traditional", "Digital"), cex = 0.8)
  dev.off()
}

################################################################################
# Flood at the municipality level - Weekly Level
################################################################################

################################
# After Pix
################################


# Load every municipality and every Pix

# Load already prepared data!
dat_flood <- read_dta(file.path(dta_path,"Base_muni_banco_flood_collapsed.dta"))
mun_fe <- read_dta(file.path(dta_path, "mun_fe.dta"))
dat_flood <- merge(dat_flood, mun_fe)
# converts to data.table
setDT(dat_flood)

dat_flood[, treat := ifelse(is.na(date_flood), 0, 1)]
table(dat_flood$treat)
dat_flood[, time_to_treat := ifelse(treat==1, week - date_flood, 0)]
table(dat_flood$time_to_treat)
# Following Sun and Abraham, we give our never-treated units a fake "treatment" date far outside the relevant study period.
dat_flood[, time_id_treated := ifelse(treat==0, 10000, date_flood)]
table(dat_flood$time_id_treated)

# Set Limits
xlimit_low <- -39
xlimit_up <- 52 
xlimits <- seq(ceiling(xlimit_low*1.1),ceiling(xlimit_up*1.1),by=1)
dat_flood <- subset(dat_flood,time_to_treat %in% xlimits)
#

#Pix

graph_function_pfandpj("log_valor_totalflow",dat_flood, xlimit_low, xlimit_up, "Log Flow Value")
graph_function_pfandpj("log_qtd_totalflow",dat_flood, xlimit_low, xlimit_up, "Log Flow Transaction")

graph_function_pfandpj("valor_netflow",dat_flood, xlimit_low, xlimit_up, "NetFlow Value")
graph_function_pfandpj("qtd_netflow",dat_flood, xlimit_low, xlimit_up, "NetFlow Transaction")

##############################################################################
##############################################################################
# Do the whole collapse (People + Firms) 
##############################################################################
##############################################################################

# Load every municipality and every Pix

# Load already prepared data!
dat_flood <- read_dta(file.path(dta_path,"Base_muni_banco_flood_collapsed3.dta"))
mun_fe <- read_dta(file.path(dta_path, "mun_fe.dta"))
dat_flood <- merge(dat_flood, mun_fe)
# converts to data.table
setDT(dat_flood)

dat_flood[, treat := ifelse(is.na(date_flood), 0, 1)]
table(dat_flood$treat)
dat_flood[, time_to_treat := ifelse(treat==1, week - date_flood, 0)]
table(dat_flood$time_to_treat)
# Following Sun and Abraham, we give our never-treated units a fake "treatment" date far outside the relevant study period.
dat_flood[, time_id_treated := ifelse(treat==0, 10000, date_flood)]
table(dat_flood$time_id_treated)

# Set Limits
xlimit_low <- -39 
xlimit_up <- 52 
xlimits <- seq(ceiling(xlimit_low*1.1),ceiling(xlimit_up*1.1),by=1)
dat_flood <- subset(dat_flood,time_to_treat %in% xlimits)
#

graph_function_collapse("log_valor_totalflow",dat_flood, xlimit_low, xlimit_up, "Log Flow Value")
graph_function_collapse("log_qtd_totalflow",dat_flood, xlimit_low, xlimit_up, "Log Flow Transaction")

graph_function_collapse("valor_netflow",dat_flood, xlimit_low, xlimit_up, "NetFlow Value")
graph_function_collapse("qtd_netflow",dat_flood, xlimit_low, xlimit_up, "NetFlow Transaction")


##############################################################################
##############################################################################
# Do the whole collapse (Digital + Traditional) 

# doesnt make any sense, if we collapse by bank, we are left with people and firms
# we already know people/firms send and receive more money, no need to show it.

# We need to unite by bank_type, forget about people and firms. 
##############################################################################
##############################################################################

# Load every municipality and every Pix

# Load already prepared data!
#dat_flood <- read_dta(file.path(dta_path,"Base_muni_banco_flood_collapsed2.dta"))
#regions <- read_dta(file.path(dta_path, "regions_brazil.dta"))
#dat_flood <- merge(dat_flood, regions)
# converts to data.table
#setDT(dat_flood)

#dat_flood[, treat := ifelse(is.na(date_flood), 0, 1)]
#table(dat_flood$treat)
#dat_flood[, time_to_treat := ifelse(treat==1, week - date_flood, 0)]
#table(dat_flood$time_to_treat)
# Following Sun and Abraham, we give our never-treated units a fake "treatment" date far outside the relevant study period.
#dat_flood[, time_id_treated := ifelse(treat==0, 10000, date_flood)]
#table(dat_flood$time_id_treated)

# Set Limits
#xlimit_low <- -26 
#xlimit_up <- 52 
#xlimits <- seq(ceiling(xlimit_low*1.2),ceiling(xlimit_up*1.1),by=1)
#dat_flood <- subset(dat_flood,time_to_treat %in% xlimits)
#

#graph_function42("log_valor_totalflow",dat_flood, xlimit_low, xlimit_up, "Log Flow Value")
#graph_function42("log_qtd_totalflow",dat_flood, xlimit_low, xlimit_up, "Log Flow Transaction")

#graph_function42("valor_netflow",dat_flood, xlimit_low, xlimit_up, "NetFlow Value")
#graph_function42("qtd_netflow",dat_flood, xlimit_low, xlimit_up, "NetFlow Transaction")

