# Roda_tudo

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

################################################################################

# flood_SA_v2
# Input:  "Pix_individuo_cleaned1_sample10.dta"
#         "flood_pix_monthly_fake.dta"
# Output: "pix_flood_",y,".png"
#         "muni_pix_flood_",y,".png"
# y: 


# The goal:  

# To do: 
################################################################################

# flood_SA_muni_v1.R
# Input:  "Base_week_muni_flood.dta"
#         "Base_week_muni_flood_beforePIX.dta"
# Output: "muni_pix_flood_",y,".png"
#         "before_pix_flood_",y,".png"
# y:  log_valor_PIX_inflow, log_valor_PIX_outflow, log_valor_PIX_intra, 
#     log_qtd_PIX_inflow, log_qtd_PIX_outflow, log_qtd_PIX_intra,
#     log_n_cli_pag_pf_intra, log_n_cli_rec_pf_intra, log_n_cli_pag_pj_intra, log_n_cli_rec_pj_intra
#     log_valor_TED_intra, log_qtd_TED_intra, log_qtd_cli_TED_rec_PJ, log_qtd_cli_TED_pag_PJ
#     log_valor_boleto, log_qtd_boleto
#     log_valor_cartao_debito, log_valor_cartao_credito, log_qtd_cli_cartao_debito, log_qtd_cli_cartao_credito

# The goal: see the effect of flood on bank transactions before and after Pix.

# To do: I did the ylimits by hand. Thats not the ideal, do the deletion of some variables in the future. 

source(file.path(R_path, "flood_SA_muni_v1.R"))
################################################################################

# flood_SA_muni_self_v1.R


source(file.path(R_path, "flood_SA_muni_self_v1.R"))
################################################################################

# flood_SA_individual_v1_PJ

################################################################################

# flood_SA_individual_v1

################################################################################

# flood_SA_individual_self_v1

################################################################################

# flood_credito_muni_month


################################################################################

#flood_flow_banco_muni.R
# Input: Base_muni_banco_flood_collapsed.dta
#        
# Output: "banco_muni_flood_",y,"_PF.png"
#         "banco_muni_flood_",y,"_PJ.png"
# y: log_valor_ratio, log_qtd_ratio


# The goal: Create a graph with 3 lines: traditional, digital, others. 
# Then we calculate changes after a flood on y
# for PJ and PF. 

# To do: we can separate btw low deposit rate, high deposit rate
#        we need to create another dta collapsed to all banks to see the reaction of flood on all banks. Maybe put before and after in the same graph.
#        Maybe I exclude the other banks before making the graphs. Also, I like the qtd_net because it would force one or the other to go down.

source(file.path(R_path, "flood_flow_banco_muni.R"))
################################################################################

#flood_flow_banco_muni_self.R

# Input: Base_muni_banco_self_flood_collapsed.dta
#        
# Output: "banco_muni_self_flood_",y,"_PF.png"
#         "banco_muni_self_flood_",y,"_PJ.png"
# y: log_valor_self_ratio, log_qtd_self_ratio


# The goal: Create a graph with 3 lines: traditional, digital, others. 
# Then we calculate changes after a flood on y
# for PJ and PF, for Before and After Pix. 

# To do: we can separate btw low deposit rate, high deposit rate
#        we need to create another dta collapsed to all banks to see the reaction of flood on all banks. Maybe put before and after in the same graph.
#        Maybe I exclude the other banks before making the graphs. Also, I like the qtd_net because it would force one or the other to go down.

source(file.path(R_path, "flood_flow_banco_muni_self.R"))
################################################################################

#flood_contas_bancarias_muni.R

# Input: CCS_muni_banco_PF_flood_collapsed.dta 
#       CCS_muni_banco_PF_flood_collapsed_beforePIX.dta
# Output: "accounts_muni_flood_",y,"_PF.png"
#         "accounts_muni_flood_",y,"_PF_beforePIX.png"
# y: log_qtd

# The goal: Create a graph with 3 lines: traditional, digital, others. 
# Then we calculate changes after a flood on log quantity of bank accounts
# for PJ and PF, for Before and After Pix. 

# To do: we can separate btw low deposit rate, high deposit rate
#        we need to create another dta collapsed to all banks to see the reaction of flood on bank accounts. Maybe put before and after in the same graph.

source(file.path(R_path, "flood_contas_bancarias_muni.R"))
################################################################################

# flood_contas_bancarias_individuo.R

# Input:  
#         
# Output: 
#         
# y: 

# This code is not done.
# The goal: to use individual bank account information to run an event study on individuos
# Before and after Pix.
# We dont have information on the bank account, whether is Digital or not. 

source(file.path(R_path, "flood_contas_bancarias_individuo.R"))
################################################################################




