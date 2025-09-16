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


# Notes on all graphs: we might just exclude TWFE, then we can do more complex graphs
#                     for example: before and after pix in the same graph
#                     Pix, Boleto, TED in the same graph
#                     Value rec, value pag in the same graph ...
################################################################################
#
#
#
#
# MUNICIPAL LEVEL
#
#
#
#
################################################################################

# flood_SA_muni_v3.R
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

source(file.path(R_path, "flood_SA_muni_v3.R"))
################################################################################

# flood_SA_muni_self_v3.R
# Input:  PIX_week_muni_self_flood_sample10.dta
#         
# Output: "self_muni_pix_flood_",y,".png"
#         
# y:  valor_self_pj qtd_self_pj n_cli_self_pj log_valor_self_pj log_qtd_self_pj log_n_cli_self_pj
#     valor_self_pf qtd_self_pf n_cli_self_pf log_valor_self_pf log_qtd_self_pf log_n_cli_self_pf 

# The goal: see the effect of flood on bank transactions to themselves before and after Pix.

# To do: I did the ylimits by hand. Thats not the ideal, do the deletion of some variables in the future. 
#         Also, there is only Pix transactions here. Ideally, it would have TED and boleto so we can do before Pix analysis. 
#         Also, this is only a 10% sample, not the real thing. 

source(file.path(R_path, "flood_SA_muni_self_v3.R"))
################################################################################

# flood_credito_muni_month_v2.R
# Input: "Base_credito_muni_flood.dta"
#        "Base_credito_muni_flood_beforePIX.dta"
# Output: "credito_flood_",y,".png"
#         "before_pix_credito_flood_",y,".png"
# y: qtd_cli_total  qtd_cli_total_PF qtd_cli_total_PJ
# vol_credito_total vol_credito_total_PF vol_credito_total_PJ
# vol_emprestimo_pessoal qtd_cli_emp_pessoal
# vol_cartao qtd_cli_cartao
# + Log variations of it

# The goal: To see the changes after a flood on y before and after Pix. 

# To do: Maybe put before and after in the same graph.
#         Also, the graphs are not all done. 

source(file.path(R_path, "flood_credito_muni_month_v2.R"))
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
#flood_estban.R
# Input: "$dta\Estban_detalhado_HHI_flood.dta"
#         Estban_detalhado_HHI_flood_beforePIX
#         Estban_detalhado_flood_collapsed
#        "$dta\Estban_detalhado_flood_beforePIX_collapsed.dta"
# Output: 
#         
# y: 


# The goal: 


# To do: 


#source(file.path(R_path, "flood_estban.R"))
################################################################################
#
#
#
#
# INDIVIDUAL LEVEL
#
#
#
#
################################################################################
# flood_SA_individual_sample_v2.R
# Input: Pix_individuo_sample_flood.dta
#         
# Output: "pix_flood_sample",y,".png"
#         
# y: log_value_sent log_trans_sent log_value_rec log_trans_rec log_value_self log_trans_self 
#     after_first_pix_rec after_first_pix_sent sender receiver user

# The goal: See effects of flood on Individuals transactions to other and to themselves

# To do:  It is only PF. 

source(file.path(R_path, "flood_SA_individual_sample_v2.R"))

#Putting it here because it is untested
source(file.path(R_path, "flood_estban.R"))
################################################################################
# flood_SA_individual_v1.R
# Input:  Pix_individuo_cleaned1_sample1.dta
#         
# Output: "pix_flood_",y,".png"
#         
# y: after_first_pix_rec, after_first_pix_sent
#     receiver, sender, user
#     trans_rec, trans_sent, value_rec, value_sent
#     log_trans_rec, log_trans_sent, log_value_rec, log_value_sent


# The goal: The goal is to see the effect of flood on individuals use of Pix.

# To do: This is very badly made. We only downloaded the 5000 smallest municipalities. 
#         Then, we need to take a 1% sample. 
#         Also, there are only Pix users in this dataset. 
#         Jose tried to solve this by downloading 1% sample of CPFs in the country. 

#       ASK JOSE HOW HE DID THIS, HE ALTERED THE WAY WE DO THIS. 

#source(file.path(R_path, "flood_SA_individual_v1.R"))
################################################################################

# flood_SA_individual_v1_PJ.R
# Input:  "Pix_individuo_cleaned2_sample1.dta"
#         
# Output: "pix_flood_",y,"_PJ.png"
#         
# y: after_first_pix_rec, after_first_pix_sent
#     receiver, sender, user
#     trans_rec, trans_sent, value_rec, value_sent
#     log_trans_rec, log_trans_sent, log_value_rec, log_value_sent 


# The goal:  The goal is to see the effect of flood on Firms use of Pix.

# To do: This is very badly made. We only downloaded the 5000 smallest municipalities. 
#         Then, we need to take a 1% sample. 
#         Also, there are only Pix users in this dataset. 
#         Jose tried to solve this by downloading 1% sample of CPFs in the country. 

#       ASK JOSE HOW HE DID THIS, HE ALTERED THE WAY WE DO THIS.

#source(file.path(R_path, "flood_SA_individual_v1_PJ.R"))
################################################################################

# flood_SA_individual_self_v1.R
# Input:  Pix_individuo_PJ_self_cleaned_sample1.dta
#         Pix_individuo_PF_self_cleaned.dta
# Output: "self_pix_flood_",y,".png"
#         "self_pix_flood_",y,"_PJ.png"
# y: after_first_pix_self, user, trans_self, value_self, log_trans_self, log_value_self


# The goal:  To see the effect of flood on flow between accounts of themselves. 

# To do: Missing ted and boleto, before and after pix for them. 
#         Should I do cross sectional regressions? Like, being hit with flood (treat == 1)
#         would impact the likelyhood of me being a user at exactly time t (t== -1,-2,..., 0,1,2,...)

#source(file.path(R_path, "flood_SA_individual_self_v1.R"))
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

#source(file.path(R_path, "flood_contas_bancarias_individuo.R"))
################################################################################
#
#
#
#
# Summary
#
#
#
#
################################################################################

#INPUT

# flood_SA_muni_v1.R
# Input:  "Base_week_muni_flood.dta"
#         "Base_week_muni_flood_beforePIX.dta"
# flood_SA_muni_self_v1.R
# Input:  PIX_week_muni_self_flood_sample10.dta
# flood_credito_muni_month.R
# Input: "Base_credito_muni_flood.dta"
#        "Base_credito_muni_flood_beforePIX.dta"
#flood_flow_banco_muni.R
# Input: Base_muni_banco_flood_collapsed.dta
#flood_flow_banco_muni_self.R
# Input: Base_muni_banco_self_flood_collapsed.dta
#flood_contas_bancarias_muni.R
# Input: CCS_muni_banco_PF_flood_collapsed.dta 
#       CCS_muni_banco_PF_flood_collapsed_beforePIX.dta

# flood_SA_individual_v1.R
# Input:  Pix_individuo_cleaned1_sample1.dta
# flood_SA_individual_v1_PJ.R
# Input:  "Pix_individuo_cleaned2_sample1.dta"
# flood_SA_individual_self_v1.R
# Input:  Pix_individuo_PJ_self_cleaned_sample1.dta
#         Pix_individuo_PF_self_cleaned.dta

#TO DO:

#########
# Needs to create individual graphs for number of bank accounts
#########

# flood_SA_muni_v1.R -> Now it is v2 - have not run yet


# flood_SA_muni_self_v1.R -> Now it is v2 - have not run yet

#########
# need to work on estban
#########
