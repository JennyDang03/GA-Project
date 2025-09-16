# Roda_tudo_importa.R

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
# CartaoMuniDia.R
# Input:  './queries/CartaoDebitoMuniDia.sql'
#         './queries/CartaoCreditoMuniDia.sql'
# Output: "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/CartaoDebitoMuni_", as.character(currentYEAR), ".csv"
#         "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/CartaoCreditoMuni_", as.character(currentYEAR), ".csv"
# y:   

# The goal: 

# To do: 

source(file.path(R_path, "CartaoMuniDia.R"))
################################################################################
# TED por Muni x dia.R
# Input:  './queries/STR - muni x dia - Intramuni.sql'
#         './queries/SITRAF - muni x dia - INTRA.sql'
#         './queries/SITRAF - muni x dia - QTD_CLI PAG.sql'
#         './queries/SITRAF - muni x dia - QTD_CLI REC.sql'
#         './queries/STR - muni x dia - QTD_CLI PAG.sql'
#         './queries/STR - muni x dia - QTD_CLI REC.sql'
#         
# Output: "./results/TED_muni_dia_intra_", as.character(currentYEAR), ".csv"
#         "./results/TED_muni_dia_intra_SITRAF_", as.character(currentYEAR), ".csv"
#         "./results/TED_muni_dia_QTDCLI_PAG_SITRAF", as.character(currentYEAR), ".csv"
#         "./results/TED_muni_dia_QTDCLI_REC_SITRAF", as.character(currentYEAR), ".csv"
#         "./results/TED_muni_dia_QTDCLI_PAG_STR", as.character(currentYEAR), ".csv"
#         "./results/TED_muni_dia_QTDCLI_REC_STR", as.character(currentYEAR), ".csv"
# y:  

# The goal: 

# To do: 

source(file.path(R_path, "TED por Muni x dia.R"))
################################################################################
# BoletosMuni.R
# Input:  './queries/Boleto_PF.sql'
#         './queries/Boleto_PJ.sql'
#         './queries/Boleto_PF_Qtd_Cli_Pagador.sql'
#         './queries/Boleto_PJ_Qtd_Cli_Pagador.sql'
#         './queries/Boleto_PJ_Qtd_Cli_Recebedor.sql'
# Output: "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/Boleto_MUNI_PF_", as.character(currentYEAR*100+mes), ".csv"
#         "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/Boleto_MUNI_PJ_", as.character(currentYEAR*100+mes), ".csv"
#         "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/Boleto_MUNI_PF_QTD_CLI_", as.character(currentYEAR*100+mes), ".csv"
#         "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/Boleto_MUNI_PJ_QTD_CLI_", as.character(currentYEAR*100+mes), ".csv"
#         "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/Boleto_MUNI_PJ_QTD_CLI_REC_", as.character(currentYEAR*100+mes), ".csv"

# y:  

# The goal: 

# To do: 

source(file.path(R_path, "BoletosMuni.R"))
################################################################################
# PixMuniBanco.R
# Input:  './queries/PixMuniBancoPAG.sql'
#         './queries/PixMuniBancoREC.sql'
#         './queries/Pix_IntraMuni_Banco_PAG.sql'
#         './queries/Pix_IntraMuni_Banco_REC.sql'
#         './queries/Pix_IntraMuni_Banco_qtd_PAG.sql'
#         './queries/Pix_IntraMuni_Banco_qtd_REC.sql'
#         './queries/PixMuniBancoSELF.sql'

# Output: "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/PIX_MUNI_IF_", as.character(currentYEAR*100+mes), ".csv"
#         "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/PIX_MUNI_IF_REC_", as.character(currentYEAR*100+mes), ".csv"
#         "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/PIX_INTRA_IF_PAG", as.character(currentYEAR*100+mes), ".csv"
#         "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/PIX_INTRA_IF_REC", as.character(currentYEAR*100+mes), ".csv"
#         "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/PIX_INTRA_IF_QTD_PAG", as.character(currentYEAR*100+mes), ".csv"
#         "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/PIX_INTRA_IF_QTD_REC", as.character(currentYEAR*100+mes), ".csv"
#         "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/PIX_SELF_MUNI_IF_", as.character(currentYEAR*100+mes), ".csv"

# y:  

# The goal: 

# To do: 

source(file.path(R_path, "PixMuniBanco.R"))
################################################################################

# Credito_por_Muni.R
# Input:  './queries/Credito_Por_Muni_PF.sql'
#         './queries/Credito_Por_Muni_PJ.sql'
# Output: "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/Credito_Muni_PF_", as.character(currentYEAR), ".csv"
#         "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/Credito_Muni_PJ_", as.character(currentYEAR), ".csv"
# y:  

# The goal: 

# To do: 

source(file.path(R_path, "Credito_por_Muni.R"))
################################################################################
# CCS_Muni_IF_estoque.R
# Input:  "./queries/", CCS_Muni_IF_PF_estoque, ".sql"
#         
# Output: "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/",CCS_Muni_IF_PF_estoque, as.character(currentYEAR*100+mes), ".csv"
#         
# y:  

# The goal: 

# To do: Needs PJ

source(file.path(R_path, "CCS_Muni_IF_estoque.R"))
################################################################################
# Pix_por_Ind_Mes_Sample.R
# Input:  "./queries/", Pix_mes_ind_rec_sample, ".sql"
#         "./queries/", Pix_mes_ind_pag_sample, ".sql"
# Output: "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/", Pix_mes_ind_rec_sample,  as.character(currentYEAR*100+mes), ".csv"
#         "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/",Pix_mes_ind_pag_sample,  as.character(currentYEAR*100+mes), ".csv"",
# y:  

# The goal: 

# To do: Needs PJ

source(file.path(R_path, "Pix_por_Ind_Mes_Sample.R"))
################################################################################
# PIXMuniAgreg.R
# Input:  "./queries/", tipo[i], ".sql"
#         tipo = "PIX_INTRAMUNI", "PIX_MUNI_Outflow", "PIX_MUNI_Inflow", "PIXMuniRECAgg", "PIXMuniPAGAgg", "PIX_Muni_idade_Pag","PIX_Muni_idade_Rec", "PIX_Muni_Self"
# Output: "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/",tipo[i],  as.character(currentYEAR*100+mes), ".csv"
# y:  

# The goal: 

# To do: 

source(file.path(R_path, "PIXMuniAgreg.R"))
################################################################################
# PIXMuniAgreg.R
# Input:  './queries/PIXMuni.sql'

# Output: "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/PIX_", as.character(currentYEAR), ".csv"

# y:  

# The goal: 

# To do: 

source(file.path(R_path, "PIXMuniAgreg.R"))
################################################################################

#INPUTS:
# CartaoMuniDia.R
# Input:  './queries/CartaoDebitoMuniDia.sql'
#         './queries/CartaoCreditoMuniDia.sql'
# TED por Muni x dia.R
# Input:  './queries/STR - muni x dia - Intramuni.sql'
#         './queries/SITRAF - muni x dia - INTRA.sql'
#         './queries/SITRAF - muni x dia - QTD_CLI PAG.sql'
#         './queries/SITRAF - muni x dia - QTD_CLI REC.sql'
#         './queries/STR - muni x dia - QTD_CLI PAG.sql'
#         './queries/STR - muni x dia - QTD_CLI REC.sql'
# BoletosMuni.R
# Input:  './queries/Boleto_PF.sql'
#         './queries/Boleto_PJ.sql'
#         './queries/Boleto_PF_Qtd_Cli_Pagador.sql'
#         './queries/Boleto_PJ_Qtd_Cli_Pagador.sql'
#         './queries/Boleto_PJ_Qtd_Cli_Recebedor.sql'
# PixMuniBanco.R
# Input:  './queries/PixMuniBancoPAG.sql'
#         './queries/PixMuniBancoREC.sql'
#         './queries/Pix_IntraMuni_Banco_PAG.sql'
#         './queries/Pix_IntraMuni_Banco_REC.sql'
#         './queries/Pix_IntraMuni_Banco_qtd_PAG.sql'
#         './queries/Pix_IntraMuni_Banco_qtd_REC.sql'
#         './queries/PixMuniBancoSELF.sql'
# Credito_por_Muni.R
# Input:  './queries/Credito_Por_Muni_PF.sql'
#         './queries/Credito_Por_Muni_PJ.sql'
# CCS_Muni_IF_estoque.R
# Input:  "./queries/", CCS_Muni_IF_PF_estoque, ".sql"

# Pix_por_Ind_Mes_Sample.R
# Input:  "./queries/", Pix_mes_ind_rec_sample, ".sql"
#         "./queries/", Pix_mes_ind_pag_sample, ".sql"
# PIXMuniAggreg.R
# Input:  "./queries/", tipo[i], ".sql"
#         tipo = "PIX_INTRAMUNI", "PIX_MUNI_Outflow", "PIX_MUNI_Inflow", "PIXMuniRECAgg", "PIXMuniPAGAgg", "PIX_Muni_idade_Pag","PIX_Muni_idade_Rec", "PIX_Muni_Self"
# PIXMuniAggreg.R
# Input:  './queries/PIXMuni.sql'






# PIXMuniAggreg.R is great, they have info that feeds into the municipality aggregated stuff
# I can alter some of the sqls to make it tipo_rec tipo_pag as a type of collapse

# PIXMuniAggreg.R
# Input:  "./queries/", tipo[i], ".sql"
#         tipo = "PIX_INTRAMUNI", "PIX_MUNI_Outflow", "PIX_MUNI_Inflow", "PIXMuniRECAgg", "PIXMuniPAGAgg", "PIX_Muni_idade_Pag","PIX_Muni_idade_Rec", "PIX_Muni_Self"
# Output: "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/",tipo[i],  as.character(currentYEAR*100+mes), ".csv"


# Pix_por_Ind_Mes_Sample.R This one is great since it takes all Pix from a sample of the population
# We need to do the same for firms. But first we need to know who are they sending and receiving money from. 
# Differentiate their types at least. 
# for firms, maybe, get more information on firms-firms transactions. 

# Pix_por_Ind_Mes_Sample.R
# Input:  "./queries/", Pix_mes_ind_rec_sample, ".sql"
#         "./queries/", Pix_mes_ind_pag_sample, ".sql"
# Output: "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/", Pix_mes_ind_rec_sample,  as.character(currentYEAR*100+mes), ".csv"
#         "//sbcdf060/depep$/DEPEPCOPEF/Teradata slicer/results/",Pix_mes_ind_pag_sample,  as.character(currentYEAR*100+mes), ".csv"",
