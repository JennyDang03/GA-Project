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

#flood_flow_banco_muni_v2.R
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

source(file.path(R_path, "flood_flow_banco_muni_v2.R"))
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

source(file.path(R_path, "flood_flow_banco_muni_self_v2.R"))
################################################################################

#flood_contas_bancarias_muni_v2.R
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

source(file.path(R_path, "flood_contas_bancarias_muni_v2.R"))
################################################################################