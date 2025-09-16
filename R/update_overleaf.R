# update_overleaf.R
# Input: 
#         
# Output: 
#         
# y: 

# The goal: 

# To do:  

################################################################################

file_paths <- c(
  "muni_pix_flood/muni_pix_flood_log_qtd_sentnocontrol.png",
  "muni_pix_flood/muni_pix_flood_log_qtd_recnocontrol.png",
  "muni_pix_flood/muni_pix_flood_log_valor_sentnocontrol.png",
  "muni_pix_flood/muni_pix_flood_log_valor_recnocontrol.png",
  "muni_pix_flood/muni_pix_flood_log_qtd_PIX_inflownocontrol.png",
  "muni_pix_flood/muni_pix_flood_log_qtd_PIX_intranocontrol.png",
  "muni_pix_flood/muni_pix_flood_log_qtd_PIX_outflownocontrol.png",
  "muni_pix_flood/muni_pix_flood_log_valor_PIX_inflownocontrol.png",
  "muni_pix_flood/muni_pix_flood_log_valor_PIX_intranocontrol.png",
  "muni_pix_flood/muni_pix_flood_log_valor_PIX_outflownocontrol.png",
  "muni_pix_flood/muni_pix_flood_log_n_cli_pag_pfnocontrol.png",
  "muni_pix_flood/muni_pix_flood_log_n_cli_rec_pfnocontrol.png",
  "pix_flood_sample/pix_flood_sampleuser_PFnocontrol.png",
  "pix_flood_sample/pix_flood_samplelog_trans_sent_PFnocontrol.png",
  "pix_flood_sample/pix_flood_samplelog_trans_rec_PFnocontrol.png",
  "pix_flood_sample/pix_flood_samplelog_value_sent_PFnocontrol.png",
  "pix_flood_sample/pix_flood_samplelog_value_rec_PFnocontrol.png",
  "muni_pix_flood/muni_pix_flood_log_n_cli_pag_pjnocontrol.png",
  "muni_pix_flood/muni_pix_flood_log_n_cli_rec_pjnocontrol.png",
  "muni_pix_flood/before_pix_flood_log_qtd_cli_rec_pj_boletonocontrol.png",
  "muni_pix_flood/muni_pix_flood_log_qtd_cli_rec_pj_boletonocontrol.png",
  "muni_pix_flood/before_pix_flood_log_qtd_cli_pag_pj_boletonocontrol.png",
  "muni_pix_flood/muni_pix_flood_log_qtd_cli_pag_pj_boletonocontrol.png",
  "muni_pix_flood/before_pix_flood_log_qtd_boletonocontrol.png",
  "muni_pix_flood/muni_pix_flood_log_qtd_boletonocontrol.png",
  "muni_pix_flood/before_pix_flood_log_valor_boletonocontrol.png",
  "muni_pix_flood/muni_pix_flood_log_valor_boletonocontrol.png",
  "muni_pix_flood/before_pix_flood_log_qtd_TED_intranocontrol.png",
  "muni_pix_flood/muni_pix_flood_log_qtd_TED_intranocontrol.png",
  "muni_pix_flood/before_pix_flood_log_valor_TED_intranocontrol.png",
  "muni_pix_flood/muni_pix_flood_log_valor_TED_intranocontrol.png",
  "muni_pix_flood/before_pix_flood_log_qtd_cli_cartao_creditonocontrol.png",
  "muni_pix_flood/muni_pix_flood_log_qtd_cli_cartao_creditonocontrol.png",
  "muni_pix_flood/before_pix_flood_log_valor_cartao_creditonocontrol.png",
  "muni_pix_flood/muni_pix_flood_log_valor_cartao_creditonocontrol.png",
  "credito_flood/before_pix_credito_flood_log_qtd_cli_cartaonocontrol.png",
  "credito_flood/credito_flood_log_qtd_cli_cartaonocontrol.png",
  "credito_flood/before_pix_credito_flood_log_vol_cartaonocontrol.png",
  "credito_flood/credito_flood_log_vol_cartaonocontrol.png",
  "muni_pix_flood/before_pix_flood_log_qtd_cli_cartao_debitonocontrol.png",
  "muni_pix_flood/muni_pix_flood_log_qtd_cli_cartao_debitonocontrol.png",
  "muni_pix_flood/before_pix_flood_log_valor_cartao_debitonocontrol.png",
  "muni_pix_flood/muni_pix_flood_log_valor_cartao_debitonocontrol.png",
  "muni_pix_flood/self_muni_pix_flood_log_qtd_self_pfnocontrol.png",
  "muni_pix_flood/self_muni_pix_flood_log_valor_self_pfnocontrol.png",
  "muni_pix_flood/self_muni_pix_flood_log_n_cli_self_pfnocontrol.png",
  "muni_pix_flood/self_muni_pix_flood_log_qtd_self_pjnocontrol.png",
  "muni_pix_flood/self_muni_pix_flood_log_valor_self_pjnocontrol.png",
  "muni_pix_flood/self_muni_pix_flood_log_n_cli_self_pjnocontrol.png",
  "banco_muni_flood/banco_muni_flood_log_qtd_totalflow_PFnocontrol.png",
  "banco_muni_flood/banco_muni_flood_log_qtd_totalflow_PJnocontrol.png",
  "banco_muni_flood/banco_muni_flood_log_valor_totalflow_PFnocontrol.png",
  "banco_muni_flood/banco_muni_flood_log_valor_totalflow_PJnocontrol.png",
  "accounts_muni_flood/accounts_muni_flood_log_qtd_PFnocontrol_beforePIX.png",
  "accounts_muni_flood/accounts_muni_flood_log_qtd_PFnocontrol.png"
)

file_paths <- sub(".*/", "", file_paths)
file_paths <- sub("nocontrol", "control1", file_paths)


# Source file path
source_path <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/Output/"

# Destination folder path
destination_path <- "C:/Users/mathe/Dropbox/Aplicativos/Overleaf/Pix Flood/Output/"

# Copy the file to the destination folder
file.copy(from = paste0(source_path,file_paths), 
          to = paste0(destination_path, file_paths))

