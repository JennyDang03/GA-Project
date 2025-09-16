# Load the jsonlite package
library(jsonlite)
library(data.table)
library(haven)
library(dplyr)
library(lubridate)
library(readxl)
library(ggplot2)
library(scales)


source(paste0("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/R/","/functions/stata_week_number.R"))
source(paste0("C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/R/","/functions/stata_month_number.R"))

path_main <- "C:/Users/mathe/Dropbox/RESEARCH/pix/pix-event-study/"
#path_main <- "//sbcdf176/PIX_Matheus$/"

path_query <- paste0(path_main, "R/DataExtraction/")
path_data <- paste0(path_main, "DadosOriginais/")
path_dta <- paste0(path_main, "Stata/dta/")
path_output <- paste0(path_main, "Output/")
log_path <- paste0(path_main, "Stata/log/")
dta_path <- paste0(path_main, "Stata/dta/")
output_path <- paste0(path_main, "Output/")
origdata_path <- paste0(path_main, "DadosOriginais/")
R_path <- paste0(path_main, "R/")
public_path <- paste0(path_main, "CSV/public_data_bc/")

#"https://olinda.bcb.gov.br/olinda/servico/Pix_DadosAbertos/versao/v1/documentacao"
file_path1 <- paste0(public_path, "Estoque de Chaves Pix por Participante.json")
file_path2 <- paste0(public_path, "Transações Pix por Município.json")

#"https://olinda.bcb.gov.br/olinda/servico/MPV_DadosAbertos/versao/v1/documentacao"
file_path3 <- paste0(public_path, "Estoque e transações de cartões.json")

# Cash in circulation.
#"https://olinda.bcb.gov.br/olinda/servico/mecir_dinheiro_em_circulacao/versao/v1/odata/informacoes_diarias?$format=json&$select=Data,Quantidade,Valor,Denominacao,Especie"
file_path4 <- paste0(public_path, "Informações Diárias.json")

# Read JSON files into R
data1 <- fromJSON(file_path1)
data2 <- fromJSON(file_path2)
data3 <- fromJSON(file_path3)
data4 <- fromJSON(file_path4)

str(data1)
str(data2)
str(data3)
str(data4)

dt1 <- as.data.table(data1[[2]])
dt2 <- as.data.table(data2[[2]])
dt3 <- as.data.table(data3[[2]])
dt4 <- as.data.table(data4[[2]])

# dt2 cleaning
# transform anomes into stata time_id using stata_month_number.R, using dlypr
dt2_backup <- dt2
dt2_backup <- dt2_backup %>% arrange(AnoMes)

####

dt2 <- dt2_backup
dt2 <- dt2 %>% filter(AnoMes <= 202300)
dt2 <- dt2 %>%
  mutate(date = stata_month_number(AnoMes))
dt2 <- dt2 %>%
  rename(id_municipio = Municipio_Ibge)
dt2 <- dt2 %>%
  mutate(qtd_paid = QT_PagadorPF + QT_PagadorPJ,
         value_paid = VL_PagadorPF + VL_PagadorPJ,
         qtd_received = QT_RecebedorPF + QT_RecebedorPJ,
         value_received = VL_RecebedorPF + VL_RecebedorPJ)

mun_convert <- read_dta(paste0(path_dta, "municipios.dta"))
mun_convert <- mun_convert %>%
  select(MUN_CD, MUN_CD_CADMU, MUN_CD_IBGE, MUN_NM, MUN_NM_NAO_FORMATADO) %>%
  rename(id_municipio_bcb = MUN_CD_CADMU,
         id_municipio = MUN_CD_IBGE,
         id_municipio_receita = MUN_CD,
         muni_nm = MUN_NM,
         muni_nm_nao_formatado = MUN_NM_NAO_FORMATADO)
mun_convert <- data.table(mun_convert)
mun_convert <- mun_convert %>%
  mutate(id_municipio = as.integer(id_municipio),
         id_municipio_bcb = as.integer(id_municipio_bcb))
any(duplicated(mun_convert$id_municipio) | duplicated(mun_convert$id_municipio_bcb) | duplicated(mun_convert$id_municipio_receita))
setDT(mun_convert)
mun_convert2 <- mun_convert %>% select(id_municipio_bcb, id_municipio)
mun_convert2 <- mun_convert2 %>% arrange(id_municipio)
dt2 <- dt2 %>% arrange(id_municipio)
#merge
dt2 <- merge(dt2, mun_convert2, by = "id_municipio", all.x = TRUE)
dt2 <- dt2 %>% select(id_municipio, id_municipio_bcb, date, qtd_paid, value_paid, qtd_received, value_received)

#Delete cases in which id_municipio is NA
dt2 <- dt2 %>% filter(!is.na(id_municipio))
dt2 <- dt2 %>% filter(!is.na(id_municipio_bcb))
#Save
write_dta(dt2, paste0(path_dta, "pix_mun_public.dta"))

##################
# in dt4, I want to collapse by Data, summing valor
dt4 <- dt4[, .(Valor = sum(Valor)), by = Data]

# look at it by week, month and year. Then save it. (rename variables first)
#save it in dta format
#write_dta(dt4, paste0(public_path, "cash_in_circulation.dta"))

# I want from dt4 the last days of each year, and the corresponding Valor
dt4$Data <- as.Date(dt4$Data)
dt4$Year <- lubridate::year(dt4$Data)
end_of_year_values <- dt4 %>%
  group_by(Year) %>%
  arrange(Data) %>%
  summarise(Valor = last(Valor)) %>%
  ungroup()

# Now, do at the end of the month - create time_id
dt4$Month <- lubridate::month(dt4$Data)
end_of_month_values <- dt4 %>%
  group_by(Month) %>%
  arrange(Data) %>%
  summarise(Valor = last(Valor)) %>%
  ungroup()

# End of the week
dt4$week <- stata_week_number(dt4$Data)
end_of_week_values <- dt4 %>%
  group_by(week) %>%
  arrange(Data) %>%
  summarise(Valor = last(Valor)) %>%
  ungroup()


################################################################################


# #ESTBAN
# estban <- read_dta(paste0(dta_path, "estban_mun_basedosdados.dta"))
# #filter only month 12,  collapse by id_municipio
# estban <- estban %>% filter(mes == 12)
# estban <- estban %>% group_by(ano,id_verbete) %>% summarise(valor = sum(valor, na.rm = TRUE)) %>% ungroup()
# 
# #Pivot wide id_verbete
# estban <- estban %>% pivot_wider(names_from = id_verbete, values_from = valor)
# #Define deposit 
# estban$deposit <- estban$`420` + estban$`432`
# #Define assets and liabilities
# estban$assets <- estban$`110` + estban$`120` + estban$`130` + estban$`160` + estban$`180` + estban$`184` + estban$`190` + estban$`200`
# estban$liab <- estban$`420` + estban$`432` + estban$`430` + estban$`440` + estban$`460` + estban$`480` + estban$`490500` + estban$`610` + estban$`710` + estban$`711` + estban$`712`
# #Define caixa
# estban$caixa <- estban$`111`
# #keep only variables: ano, deposit, assets, liab, caixa
# estban <- estban %>% select(ano, deposit, assets, liab, caixa)
# 
# 



# Read public_data_summary.xlsx
#Change ted to bank wire and boleto to payment slip

public_data_summary_all <- read_excel(paste0(public_path, "public_data_summary.xlsx"))

# Limit the data for year >=2018
public_data_summary <- public_data_summary_all %>%
  filter(year >= 2018)
public_data_summary <- public_data_summary %>% filter(year <= 2022)

title_size = 24
x_size = 0.1
y_size = 20
axis_size = 18
line_size = 2
legend_key_size = 2
legend_size = 20

# Transactions PIX, boleto, debit, credit, TED, withdrawls

gg <- ggplot(public_data_summary, aes(x = year)) +
  geom_line(aes(y = trans_pix, color = "Pix", linetype = "Pix"), size = line_size) +
  geom_line(aes(y = trans_debit, color = "Debit", linetype = "Debit"), size = line_size) +
  geom_line(aes(y = trans_credit, color = "Credit", linetype = "Credit"), size = line_size) +
  geom_line(aes(y = trans_ted, color = "Wire", linetype = "Wire"), size = line_size) +
  geom_line(aes(y = trans_boleto, color = "Slip", linetype = "Slip"), size = line_size) +
  #geom_line(aes(y = trans_withdrawls, color = "Withdrawls", linetype = "Withdrawls"), size = line_size) +
  geom_vline(xintercept = 2020, linetype = "dotted", color = "black") +  # Add vertical line at x = 2020
  labs(title = "Transactions of Payment Methods", x = "Year", y = "Quantity (Millions)") +
  scale_color_manual(name = "", 
                     values = c("Pix" = "blue", "Debit" = "purple", "Credit" = "red", "Wire" = "green", 
                                "Slip" = "orange", "Withdrawls" = "grey")) +
  scale_linetype_manual(name = "", 
                        values = c("Pix" = "solid", "Debit" = "longdash", "Credit" = "dashed", "Wire" = "dotted", 
                                   "Slip" = "dotdash", "Withdrawls" = "twodash")) +
  scale_x_continuous(breaks = seq(min(public_data_summary$year), max(public_data_summary$year), by = 1)) +
  scale_y_continuous(labels = comma) +  # Add comma separator for thousands
  theme_minimal() +
  theme(panel.grid.major.x = element_blank(),  # Remove vertical grid lines
        panel.grid.major.y = element_line(color = "lightgrey"),  # Light grey horizontal grid lines
        panel.grid.minor = element_blank(),  # Remove minor grid lines
        panel.background = element_blank(),  # Remove background panel
        axis.line = element_line(color = "black"),  # Black axis lines
        plot.title = element_text(hjust = 0.5, size = title_size),  # Align title in the middle and increase size
        axis.title.x = element_text(size = x_size),  # Increase x-axis title size
        axis.title.y = element_text(size = y_size),  # Increase y-axis title size
        axis.text.x = element_text(size = axis_size),  # Increase x-axis text size
        axis.text.y = element_text(size = axis_size),  # Increase y-axis text size
        legend.position = "bottom",  # Position of the legend at the bottom
        legend.title = element_blank(),  # Remove legend title
        legend.key.size = unit(legend_key_size, "lines"),  # Increase the size of the legend keys
        legend.text = element_text(size = legend_size),  # Increase the size of the legend text
        legend.background = element_rect(fill = "transparent"))  # White background for legend

print(gg)
# Adjust margins and save the plot as PNG
ggsave(paste0(output_path,"graphs/transactions_over_years.png"), gg, width = 8, height = 6, dpi = 300, bg = "transparent")

# Value of transactions PIX, boleto, debit, credit, TED, withdrawls

gg <- ggplot(public_data_summary, aes(x = year)) +
  geom_line(aes(y = valor_pix, color = "Pix", linetype = "Pix"), size = line_size) +
  geom_line(aes(y = valor_debit, color = "Debit", linetype = "Debit"), size = line_size) +
  geom_line(aes(y = valor_credit, color = "Credit", linetype = "Credit"), size = line_size) +
  geom_line(aes(y = valor_ted, color = "Wire", linetype = "Wire"), size = line_size) +
  geom_line(aes(y = valor_boleto, color = "Slip", linetype = "Slip"), size = line_size) +
  #geom_line(aes(y = valor_withdrawls, color = "Withdrawls", linetype = "Withdrawls"), size = line_size) +
  geom_vline(xintercept = 2020, linetype = "dotted", color = "black") +  # Add vertical line at x = 2020
  labs(title = "Value of Payment Methods", x = "Year", y = "Value (Billions)") +
  scale_color_manual(name = "", 
                     values = c("Pix" = "blue", "Debit" = "purple", "Credit" = "red", "Wire" = "green", 
                                "Slip" = "orange", "Withdrawls" = "grey")) +
  scale_linetype_manual(name = "", 
                        values = c("Pix" = "solid", "Debit" = "longdash", "Credit" = "dashed", "Wire" = "dotted", 
                                   "Slip" = "dotdash", "Withdrawls" = "twodash")) +
  scale_x_continuous(breaks = seq(min(public_data_summary$year), max(public_data_summary$year), by = 1)) +
  scale_y_continuous(labels = comma) +  # Add comma separator for thousands
  theme_minimal() +
  theme(panel.grid.major.x = element_blank(),  # Remove vertical grid lines
        panel.grid.major.y = element_line(color = "lightgrey"),  # Light grey horizontal grid lines
        panel.grid.minor = element_blank(),  # Remove minor grid lines
        panel.background = element_blank(),  # Remove background panel
        axis.line = element_line(color = "black"),  # Black axis lines
        plot.title = element_text(hjust = 0.5, size = title_size),  # Align title in the middle and increase size
        axis.title.x = element_text(size = x_size),  # Increase x-axis title size
        axis.title.y = element_text(size = y_size),  # Increase y-axis title size
        axis.text.x = element_text(size = axis_size),  # Increase x-axis text size
        axis.text.y = element_text(size = axis_size),  # Increase y-axis text size
        legend.position = "bottom",  # Position of the legend at the bottom
        legend.title = element_blank(),  # Remove legend title
        legend.key.size = unit(legend_key_size, "lines"),  # Increase the size of the legend keys
        legend.text = element_text(size = legend_size),  # Increase the size of the legend text
        legend.background = element_rect(fill = "transparent"))  # White background for legend

print(gg)

# Adjust margins and save the plot as PNG
ggsave(paste0(output_path,"graphs/value_over_years.png"), gg, width = 8, height = 6, dpi = 300, bg = "transparent")


# Credit, debit and POS (qtd_debit, qtd_credit, pos_pdv)

gg <- ggplot(public_data_summary, aes(x = year)) +
  geom_line(aes(y = qtd_debit/10^6, color = "Debit", linetype = "Debit"), size = line_size) +
  geom_line(aes(y = qtd_credit/10^6, color = "Credit", linetype = "Credit"), size = line_size) +
  geom_line(aes(y = pos_pdv/10^6, color = "POS", linetype = "POS"), size = line_size) +
  geom_vline(xintercept = 2020, linetype = "dotted", color = "black") +  # Add vertical line at x = 2020
  labs(title = "Card Payment Access", x = "Year", y = "Quantity (Millions)") +
  scale_color_manual(name = "", 
                     values = c("Debit" = "purple", "Credit" = "red", "POS" = "green")) +
  scale_linetype_manual(name = "", 
                        values = c("Debit" = "longdash", "Credit" = "dashed", "POS" = "dotted")) +
  scale_x_continuous(breaks = seq(min(public_data_summary$year), max(public_data_summary$year), by = 1)) +
  scale_y_continuous(labels = comma) +  # Add comma separator for thousands
  theme_minimal() +
  theme(panel.grid.major.x = element_blank(),  # Remove vertical grid lines
        panel.grid.major.y = element_line(color = "lightgrey"),  # Light grey horizontal grid lines
        panel.grid.minor = element_blank(),  # Remove minor grid lines
        panel.background = element_blank(),  # Remove background panel
        axis.line = element_line(color = "black"),  # Black axis lines
        plot.title = element_text(hjust = 0.5, size = title_size),  # Align title in the middle and increase size
        axis.title.x = element_text(size = x_size),  # Increase x-axis title size
        axis.title.y = element_text(size = y_size),  # Increase y-axis title size
        axis.text.x = element_text(size = axis_size),  # Increase x-axis text size
        axis.text.y = element_text(size = axis_size),  # Increase y-axis text size
        legend.position = "bottom",  # Position of the legend at the bottom
        legend.title = element_blank(),  # Remove legend title
        legend.key.size = unit(legend_key_size, "lines"),  # Increase the size of the legend keys
        legend.text = element_text(size = legend_size),  # Increase the size of the legend text
        legend.background = element_rect(fill = "transparent"))  # White background for legend

print(gg)

# Adjust margins and save the plot as PNG
ggsave(paste0(output_path,"graphs/credit_debit_pos_over_years.png"), gg, width = 8, height = 6, dpi = 300, bg = "transparent")


# Bank Accounts, deposits and withdrawls (deposits, withdrawls, bank_accounts, assets, liabilities)

gg <- ggplot(public_data_summary, aes(x = year)) +
  geom_line(aes(y = deposits/10^9, color = "Deposits e+9", linetype = "Deposits e+9"), size = line_size) +
  geom_line(aes(y = valor_withdrawls, color = "Withdrawls e+9", linetype = "Withdrawls e+9"), size = line_size) +
  geom_line(aes(y = bank_accounts/10^6, color = "Bank Accounts e+6", linetype = "Bank Accounts e+6"), size = line_size) +
  #geom_line(aes(y = assets/10^10, color = "Assets e+10", linetype = "Assets e+10"), size = line_size) +
  #geom_line(aes(y = liab/10^10, color = "Liabilities e+10", linetype = "Liabilities e+10"), size = line_size) +
  geom_vline(xintercept = 2020, linetype = "dotted", color = "black") +  # Add vertical line at x = 2020
  labs(title = "Banks' Balance Sheet", x = "Year", y = "") +
  scale_color_manual(name = "", 
                     values = c("Deposits e+9" = "blue", "Withdrawls e+9" = "red", "Bank Accounts e+6" = "green", 
                                "Assets e+10" = "purple", "Liabilities e+10" = "orange")) +
  scale_linetype_manual(name = "", 
                        values = c("Deposits e+9" = "solid", "Withdrawls e+9" = "dashed", "Bank Accounts e+6" = "dotted", 
                                   "Assets e+10" = "longdash", "Liabilities e+10" = "dotdash")) +
  scale_x_continuous(breaks = seq(min(public_data_summary$year), max(public_data_summary$year), by = 1)) +
  scale_y_continuous(labels = comma) +  # Add comma separator for thousands
  theme_minimal() +
  theme(panel.grid.major.x = element_blank(),  # Remove vertical grid lines
        panel.grid.major.y = element_line(color = "lightgrey"),  # Light grey horizontal grid lines
        panel.grid.minor = element_blank(),  # Remove minor grid lines
        panel.background = element_blank(),  # Remove background panel
        axis.line = element_line(color = "black"),  # Black axis lines
        plot.title = element_text(hjust = 0.5, size = title_size),  # Align title in the middle and increase size
        axis.title.x = element_text(size = x_size),  # Increase x-axis title size
        axis.title.y = element_text(size = y_size),  # Increase y-axis title size
        axis.text.x = element_text(size = axis_size),  # Increase x-axis text size
        axis.text.y = element_text(size = axis_size),  # Increase y-axis text size
        legend.position = "bottom",  # Position of the legend at the bottom
        legend.title = element_blank(),  # Remove legend title
        legend.key.size = unit(legend_key_size, "lines"),  # Increase the size of the legend keys
        legend.text = element_text(size = legend_size),  # Increase the size of the legend text
        legend.background = element_rect(fill = "transparent")) +  # Transparent background for legend
  guides(color = guide_legend(nrow = 2, byrow = TRUE), 
         linetype = guide_legend(nrow = 2, byrow = TRUE))  # Break legend into 2 rows
print(gg)

# Adjust margins and save the plot as PNG
ggsave(paste0(output_path,"graphs/bank_accounts_deposits_withdrawls_over_years.png"), gg, width = 8, height = 6, dpi = 300, bg = "transparent")


# Bank services (trans_loan, trans_consulta, trans_financial_services, trans_other_services)

gg <- ggplot(public_data_summary, aes(x = year)) +
  geom_line(aes(y = trans_loan, color = "Loans", linetype = "Loans"), size = line_size) +
  geom_line(aes(y = trans_financial_services, color = "Other Financial Services", linetype = "Other Financial Services"), size = 1.2) +
  geom_vline(xintercept = 2020, linetype = "dotted", color = "black") +  # Add vertical line at x = 2020
  labs(title = "Bank Services", x = "Year", y = "Quantity (Millions)") +
  scale_color_manual(name = "", 
                     values = c("Loans" = "blue", "Other Financial Services" = "green")) +
  scale_linetype_manual(name = "", 
                        values = c("Loans" = "solid", "Other Financial Services" = "dotted")) +
  scale_x_continuous(breaks = seq(min(public_data_summary$year), max(public_data_summary$year), by = 1)) +
  scale_y_continuous(labels = comma) +  # Add comma separator for thousands
  theme_minimal() +
  theme(panel.grid.major.x = element_blank(),  # Remove vertical grid lines
        panel.grid.major.y = element_line(color = "lightgrey"),  # Light grey horizontal grid lines
        panel.grid.minor = element_blank(),  # Remove minor grid lines
        panel.background = element_blank(),  # Remove background panel
        axis.line = element_line(color = "black"),  # Black axis lines
        plot.title = element_text(hjust = 0.5, size = title_size),  # Align title in the middle and increase size
        axis.title.x = element_text(size = x_size),  # Increase x-axis title size
        axis.title.y = element_text(size = y_size),  # Increase y-axis title size
        axis.text.x = element_text(size = axis_size),  # Increase x-axis text size
        axis.text.y = element_text(size = axis_size),  # Increase y-axis text size
        legend.position = "bottom",  # Position of the legend at the bottom
        legend.title = element_blank(),  # Remove legend title
        legend.key.size = unit(legend_key_size, "lines"),  # Increase the size of the legend keys
        legend.text = element_text(size = legend_size),  # Increase the size of the legend text
        legend.background = element_rect(fill = "transparent"))  # White background for legend

print(gg)

# Adjust margins and save the plot as PNG
ggsave(paste0(output_path,"graphs/bank_services_over_years.png"), gg, width = 8, height = 6, dpi = 300, bg = "transparent")


# Access (internet_access, branches_access, atm_access, cellphone_access, call_center_access)

gg <- ggplot(public_data_summary, aes(x = year)) +
  geom_line(aes(y = cellphone_access, color = "Cellphone", linetype = "Cellphone"), size = line_size) +
  geom_line(aes(y = internet_access, color = "Internet", linetype = "Internet"), size = line_size) +
  geom_line(aes(y = branches_access + atm_access, color = "Branches + ATMs", linetype = "Branches + ATMs"), size = line_size) +
  geom_vline(xintercept = 2020, linetype = "dotted", color = "black") +  # Add vertical line at x = 2020
  labs(title = "Access Channel", x = "Year", y = "Quantity (Millions)") +
  scale_color_manual(name = "", 
                     values = c("Cellphone" = "blue", "Internet" = "orange", "Branches + ATMs" = "purple"),
                     breaks = c("Cellphone", "Internet", "Branches + ATMs")) +
  scale_linetype_manual(name = "", 
                        values = c("Cellphone" = "solid", "Internet" = "dotdash", "Branches + ATMs" = "dotted"),
                        breaks = c("Cellphone", "Internet", "Branches + ATMs")) +
  scale_x_continuous(breaks = seq(min(public_data_summary$year), max(public_data_summary$year), by = 1)) +
  scale_y_continuous(labels = comma) +  # Add comma separator for thousands
  theme_minimal() +
  theme(panel.grid.major.x = element_blank(),  # Remove vertical grid lines
        panel.grid.major.y = element_line(color = "lightgrey"),  # Light grey horizontal grid lines
        panel.grid.minor = element_blank(),  # Remove minor grid lines
        panel.background = element_blank(),  # Remove background panel
        axis.line = element_line(color = "black"),  # Black axis lines
        plot.title = element_text(hjust = 0.5, size = title_size),  # Align title in the middle and increase size
        axis.title.x = element_text(size = x_size),  # Increase x-axis title size
        axis.title.y = element_text(size = y_size),  # Increase y-axis title size
        axis.text.x = element_text(size = axis_size),  # Increase x-axis text size
        axis.text.y = element_text(size = axis_size),  # Increase y-axis text size
        legend.position = "bottom",  # Position of the legend at the bottom
        legend.title = element_blank(),  # Remove legend title
        legend.key.size = unit(legend_key_size, "lines"),  # Increase the size of the legend keys
        legend.text = element_text(size = legend_size),  # Increase the size of the legend text
        legend.background = element_rect(fill = "transparent")) +  # Transparent background for legend
  guides(color = guide_legend(nrow = 1, byrow = TRUE), 
         linetype = guide_legend(nrow = 1, byrow = TRUE))  # Break legend into 2 rows

print(gg)

# Adjust margins and save the plot as PNG
ggsave(paste0(output_path,"graphs/access_over_years.png"), gg, width = 8, height = 6, dpi = 300, bg = "transparent")


























# Cash transactions (cash_transactions)

public_data_summary2 <- public_data_summary_all %>% filter(year <= 2023 & year >= 2012)

gg <- ggplot(public_data_summary2, aes(x = year)) +
  geom_line(aes(y = cash_transactions, color = "Brazil", linetype = "Brazil"), size = line_size) +
  geom_vline(xintercept = 2020, linetype = "dotted", color = "black") +  # Add vertical line at x = 2020
  labs(title = "Cash Transactions", x = "", y = "Estimated % of Transactions") +
  scale_color_manual(name = "", values = c("Brazil" = "blue")) +
  scale_linetype_manual(name = "", values = c("Brazil" = "solid")) +
  scale_x_continuous(breaks = seq(min(public_data_summary2$year), max(public_data_summary2$year), by = 2)) +
  scale_y_continuous(labels = percent) +  # Format y-axis labels as percentages
  coord_cartesian(ylim = c(0.03, 0.8)) +  # Set the limits of the y-axis
  theme_minimal() +
  theme(panel.grid.major.x = element_blank(),  # Remove vertical grid lines
        panel.grid.major.y = element_line(color = "lightgrey"),  # Light grey horizontal grid lines
        panel.grid.minor = element_blank(),  # Remove minor grid lines
        panel.background = element_blank(),  # Remove background panel
        axis.line = element_line(color = "black"),  # Black axis lines
        plot.title = element_text(hjust = 0.5, size = title_size),  # Align title in the middle and increase size
        axis.title.x = element_text(size = x_size),  # Increase x-axis title size
        axis.title.y = element_text(size = y_size),  # Increase y-axis title size
        axis.text.x = element_text(size = axis_size),  # Increase x-axis text size
        axis.text.y = element_text(size = axis_size),  # Increase y-axis text size
        legend.position = "bottom",  # Position of the legend at the bottom
        legend.title = element_blank(),  # Remove legend title
        legend.key.size = unit(legend_key_size, "lines"),  # Increase the size of the legend keys
        legend.text = element_text(size = legend_size),  # Increase the size of the legend text
        legend.background = element_rect(fill = "transparent"))  # White background for legend

print(gg)



# Adjust margins and save the plot as PNG
ggsave(paste0(output_path,"graphs/cash_transactions_over_years.png"), gg, width = 8, height = 6, dpi = 300, bg = "transparent")





# money in circulation (cash_in_circulation)
public_data_summary3 <- public_data_summary_all %>% filter(year <= 2023 & year >= 2012)

gg <- ggplot(public_data_summary3, aes(x = year)) +
  geom_line(aes(y = cash_in_circulation, color = "Cash", linetype = "Cash"), size = line_size) +
  geom_vline(xintercept = 2020, linetype = "dotted", color = "black") +  # Add vertical line at x = 2020
  labs(title = "Cash in Circulation", x = "", y = "Value (Billions)") +
  scale_color_manual(name = "", values = c("Cash" = "blue")) +
  scale_linetype_manual(name = "", values = c("Cash" = "solid")) +
  scale_x_continuous(breaks = seq(min(public_data_summary3$year), max(public_data_summary3$year), by = 2)) +
  scale_y_continuous(labels = comma) +  # Add comma separator for thousands
  theme_minimal() +
  theme(panel.grid.major.x = element_blank(),  # Remove vertical grid lines
        panel.grid.major.y = element_line(color = "lightgrey"),  # Light grey horizontal grid lines
        panel.grid.minor = element_blank(),  # Remove minor grid lines
        panel.background = element_blank(),  # Remove background panel
        axis.line = element_line(color = "black"),  # Black axis lines
        plot.title = element_text(hjust = 0.5, size = title_size),  # Align title in the middle and increase size
        axis.title.x = element_text(size = x_size),  # Increase x-axis title size
        axis.title.y = element_text(size = y_size),  # Increase y-axis title size
        axis.text.x = element_text(size = axis_size),  # Increase x-axis text size
        axis.text.y = element_text(size = axis_size),  # Increase y-axis text size
        legend.position = "none",  # Position of the legend at the bottom
        legend.title = element_blank(),  # Remove legend title
        legend.key.size = unit(legend_key_size, "lines"),  # Increase the size of the legend keys
        legend.text = element_text(size = legend_size),  # Increase the size of the legend text
        legend.background = element_rect(fill = "transparent"))  # White background for legend

print(gg)

# Adjust margins and save the plot as PNG
ggsave(paste0(output_path,"graphs/money_in_circulation_over_years.png"), gg, width = 8, height = 6, dpi = 300, bg = "transparent")






#################
# Then I need to read the data on other payment methods. 

# and also get data on money in circulation. 

# Read file: paste0(path, "Instrumentos de Pagamento - Dados Estatisticos 2022.xlsx")

##############################################################################

# Read C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\CSV\comparacao entre paises\comparacao.xlsx
comparacao <- read_excel(paste0(path_main, "CSV/comparacao entre paises/comparacao.xlsx"))
#Change name fro, "United kingdom" to "UK"
comparacao <- comparacao %>% rename(`UK` = `United kingdom`)

# Y variables: "Brazil", "Australia", "Chile", "Denmark", "Mexico", "Nigeria", "India", "Singapore",  "Sweden", "United Kingdom"
gg <- ggplot(comparacao, aes(x = Years)) +
  geom_line(aes(y = Brazil, color = "Brazil", linetype = "Brazil"), size = line_size) +
  geom_line(aes(y = Australia, color = "Australia", linetype = "Australia"), size = line_size) +
  geom_line(aes(y = Chile, color = "Chile", linetype = "Chile"), size = line_size) +
  geom_line(aes(y = Denmark, color = "Denmark", linetype = "Denmark"), size = line_size) +
  geom_line(aes(y = Mexico, color = "Mexico", linetype = "Mexico"), size = line_size) +
  geom_line(aes(y = Nigeria, color = "Nigeria", linetype = "Nigeria"), size = line_size) +
  geom_line(aes(y = India, color = "India", linetype = "India"), size = line_size) +
  geom_line(aes(y = Singapore, color = "Singapore", linetype = "Singapore"), size = line_size) +
  geom_line(aes(y = Sweden, color = "Sweden", linetype = "Sweden"), size = line_size) +
  geom_line(aes(y = UK, color = "UK", linetype = "UK"), size = line_size) +
  labs(title = "Transactions per capita", x = "Years after launch", y = "") +
  scale_color_manual(name = "", 
                     values = c("Brazil" = "blue", "Australia" = "purple", "Chile" = "red", "Denmark" = "green", 
                                "Mexico" = "orange", "Nigeria" = "grey", "India" = "black", "Singapore" = "brown", 
                                "Sweden" = "pink", "UK" = "yellow"),
                     breaks = c("Brazil", "Australia", "Chile", "Denmark", "Mexico", "Nigeria", "India", "Singapore",  "Sweden", "UK")) +
  scale_linetype_manual(name = "", 
                        values = c("Brazil" = "solid", "Australia" = "longdash", "Chile" = "dashed", "Denmark" = "dotted", 
                                   "Mexico" = "dotdash", "Nigeria" = "twodash", "India" = "solid", "Singapore" = "longdash", 
                                   "Sweden" = "dashed", "UK" = "dotted"),
                        breaks = c("Brazil", "Australia", "Chile", "Denmark", "Mexico", "Nigeria", "India", "Singapore",  "Sweden", "UK")) +
  scale_x_continuous(breaks = seq(min(comparacao$Years), max(comparacao$Years))) +
  scale_y_continuous(labels = comma) +  # Add comma separator for thousands
  theme_minimal() +
  theme(panel.grid.major.x = element_blank(),  # Remove vertical grid lines
        panel.grid.major.y = element_line(color = "lightgrey"),  # Light grey horizontal grid lines
        panel.grid.minor = element_blank(),  # Remove minor grid lines
        panel.background = element_blank(),  # Remove background panel
        axis.line = element_line(color = "black"),  # Black axis lines
        plot.title = element_text(hjust = 0.5, size = title_size),  # Align title in the middle and increase size
        axis.title.x = element_text(size = y_size),  # Increase x-axis title size
        axis.title.y = element_text(size = x_size),  # Increase y-axis title size
        axis.text.x = element_text(size = axis_size),  # Increase x-axis text size
        axis.text.y = element_text(size = axis_size),  # Increase y-axis text size
        legend.position = "bottom",  # Position of the legend at the bottom
        legend.title = element_blank(),  # Remove legend title
        legend.key.size = unit(legend_key_size, "lines"),  # Increase the size of the legend keys
        legend.text = element_text(size = legend_size),  # Increase the size of the legend text
        legend.background = element_rect(fill = "transparent"))+  # Transparent background for legend
  guides(color = guide_legend(nrow = 3, byrow = TRUE), 
         linetype = guide_legend(nrow = 3, byrow = TRUE))  # Break legend into 2 rows
print(gg)

# Adjust margins and save the plot as PNG
ggsave(paste0(output_path,"graphs/comparacao.png"), gg, width = 8, height = 6, dpi = 300, bg = "transparent")



# Read C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\CSV\comparacao entre paises\cash_comparacao.xlsx
cash_comparacao <- read_excel(paste0(path_main, "CSV/comparacao entre paises/cash_comparacao.xlsx"))
cash_comparacao <- cash_comparacao %>% filter(Years >= 2018)
# Y variables: "Brazil", "Australia", "Chile", "Denmark", "Mexico", "Nigeria", "India", "Singapore",  "Sweden", "United Kingdom"
gg <- ggplot(cash_comparacao, aes(x = Years)) +
  geom_line(aes(y = Brazil, color = "Brazil", linetype = "Brazil"), size = line_size) +
  geom_line(aes(y = Australia, color = "Australia", linetype = "Australia"), size = line_size) +
  geom_line(aes(y = Chile, color = "Chile", linetype = "Chile"), size = line_size) +
  geom_line(aes(y = Denmark, color = "Denmark", linetype = "Denmark"), size = line_size) +
  geom_line(aes(y = Mexico, color = "Mexico", linetype = "Mexico"), size = line_size) +
  geom_line(aes(y = Nigeria, color = "Nigeria", linetype = "Nigeria"), size = line_size) +
  geom_line(aes(y = India, color = "India", linetype = "India"), size = line_size) +
  geom_line(aes(y = Singapore, color = "Singapore", linetype = "Singapore"), size = line_size) +
  geom_line(aes(y = Sweden, color = "Sweden", linetype = "Sweden"), size = line_size) +
  geom_line(aes(y = UK, color = "UK", linetype = "UK"), size = line_size) +
  labs(title = "Transactions per capita", x = "Years after launch", y = "") +
  scale_color_manual(name = "", 
                     values = c("Brazil" = "blue", "Australia" = "purple", "Chile" = "red", "Denmark" = "green", 
                                "Mexico" = "orange", "Nigeria" = "grey", "India" = "black", "Singapore" = "brown", 
                                "Sweden" = "pink", "UK" = "yellow"),
                     breaks = c("Brazil", "Australia", "Chile", "Denmark", "Mexico", "Nigeria", "India", "Singapore",  "Sweden", "UK")) +
  scale_linetype_manual(name = "", 
                        values = c("Brazil" = "solid", "Australia" = "longdash", "Chile" = "dashed", "Denmark" = "dotted", 
                                   "Mexico" = "dotdash", "Nigeria" = "twodash", "India" = "solid", "Singapore" = "longdash", 
                                   "Sweden" = "dashed", "UK" = "dotted"),
                        breaks = c("Brazil", "Australia", "Chile", "Denmark", "Mexico", "Nigeria", "India", "Singapore",  "Sweden", "UK")) +
  scale_x_continuous(breaks = seq(min(cash_comparacao$Years), max(cash_comparacao$Years))) +
  scale_y_continuous(labels = comma) +  # Add comma separator for thousands
  theme_minimal() +
  theme(panel.grid.major.x = element_blank(),  # Remove vertical grid lines
        panel.grid.major.y = element_line(color = "lightgrey"),  # Light grey horizontal grid lines
        panel.grid.minor = element_blank(),  # Remove minor grid lines
        panel.background = element_blank(),  # Remove background panel
        axis.line = element_line(color = "black"),  # Black axis lines
        plot.title = element_text(hjust = 0.5, size = title_size),  # Align title in the middle and increase size
        axis.title.x = element_text(size = y_size),  # Increase x-axis title size
        axis.title.y = element_text(size = x_size),  # Increase y-axis title size
        axis.text.x = element_text(size = axis_size),  # Increase x-axis text size
        axis.text.y = element_text(size = axis_size),  # Increase y-axis text size
        legend.position = "bottom",  # Position of the legend at the bottom
        legend.title = element_blank(),  # Remove legend title
        legend.key.size = unit(legend_key_size, "lines"),  # Increase the size of the legend keys
        legend.text = element_text(size = legend_size),  # Increase the size of the legend text
        legend.background = element_rect(fill = "transparent"))+  # Transparent background for legend
  guides(color = guide_legend(nrow = 3, byrow = TRUE), 
         linetype = guide_legend(nrow = 3, byrow = TRUE))  # Break legend into 2 rows
print(gg)




# Y variables: "Brazil", "Australia", "Chile", "Denmark", "Mexico", "Nigeria", "India", "Singapore",  "Sweden", "United Kingdom"
gg <- ggplot(cash_comparacao, aes(x = Years)) +
  geom_line(aes(y = Brazil, color = "Brazil", linetype = "Brazil"), size = line_size) +
  geom_line(aes(y = Colombia, color = "Colombia", linetype = "Colombia"), size = line_size) +
  geom_line(aes(y = Mexico, color = "Mexico", linetype = "Mexico"), size = line_size) +
  geom_line(aes(y = Peru, color = "Peru", linetype = "Peru"), size = line_size) +
  geom_vline(xintercept = 2020, linetype = "dotted", color = "black") +  # Add vertical line at x = 2020
  labs(title = "Cash Transactions", x = "", y = "Estimated % of Transactions") +
  scale_color_manual(name = "", 
                     values = c("Brazil" = "blue", "Argentina" = "purple", "Chile" = "red", "Colombia" = "green", 
                                "Mexico" = "orange", "Peru" = "grey", "India" = "black", "Singapore" = "brown", 
                                "Sweden" = "pink", "UK" = "yellow"),
                     breaks = c("Brazil", "Argentina", "Chile", "Colombia", "Mexico", "Peru", "India", "Singapore",  "Sweden", "UK")) +
  scale_linetype_manual(name = "", 
                        values = c("Brazil" = "solid", "Argentina" = "longdash", "Chile" = "dashed", "Colombia" = "dotted", 
                                   "Mexico" = "dotdash", "Peru" = "twodash", "India" = "solid", "Singapore" = "longdash", 
                                   "Sweden" = "dashed", "UK" = "dotted"),
                        breaks = c("Brazil", "Argentina", "Chile", "Colombia", "Mexico", "Peru", "India", "Singapore",  "Sweden", "UK")) +
  scale_x_continuous(breaks = seq(min(cash_comparacao$Years), max(cash_comparacao$Years), by = 2)) +
  scale_y_continuous(labels = percent) +  # Add comma separator for thousands
  coord_cartesian(ylim = c(0.03, 0.8)) +  # Set the limits of the y-axis
  theme_minimal() +
  theme(panel.grid.major.x = element_blank(),  # Remove vertical grid lines
        panel.grid.major.y = element_line(color = "lightgrey"),  # Light grey horizontal grid lines
        panel.grid.minor = element_blank(),  # Remove minor grid lines
        panel.background = element_blank(),  # Remove background panel
        axis.line = element_line(color = "black"),  # Black axis lines
        plot.title = element_text(hjust = 0.5, size = title_size),  # Align title in the middle and increase size
        axis.title.x = element_text(size = x_size),  # Increase x-axis title size
        axis.title.y = element_text(size = y_size),  # Increase y-axis title size
        axis.text.x = element_text(size = axis_size),  # Increase x-axis text size
        axis.text.y = element_text(size = axis_size),  # Increase y-axis text size
        legend.position = "bottom",  # Position of the legend at the bottom
        legend.title = element_blank(),  # Remove legend title
        legend.key.size = unit(legend_key_size, "lines"),  # Increase the size of the legend keys
        legend.text = element_text(size = legend_size),  # Increase the size of the legend text
        legend.background = element_rect(fill = "transparent"))+  # Transparent background for legend
  guides(color = guide_legend(nrow = 1, byrow = TRUE), 
         linetype = guide_legend(nrow = 1, byrow = TRUE))  # Break legend into 2 rows
print(gg)

# Adjust margins and save the plot as PNG
ggsave(paste0(output_path,"graphs/cash_comparacao.png"), gg, width = 8, height = 6, dpi = 300, bg = "transparent")












# Plot
gg <- ggplot(public_data_summary, aes(x = year)) +
  geom_line(aes(y = deposits / 10^9, color = "Deposits (R$ billions)", linetype = "Deposits (R$ billions)"), size = line_size) +
  geom_line(aes(y = valor_withdrawls, color = "Withdrawals (R$ billions)", linetype = "Withdrawals (R$ billions)"), size = line_size) +
  geom_line(aes(y = bank_accounts / 10^6, color = "Bank Accounts (millions)", linetype = "Bank Accounts (millions)"), size = line_size) +
  geom_vline(xintercept = 2020, linetype = "dotted", color = "black") +  # Add vertical line at x = 2020
  scale_color_manual(name = "", 
                     values = c("Deposits (R$ billions)" = "blue", 
                                "Withdrawals (R$ billions)" = "red", 
                                "Bank Accounts (millions)" = "green")) +
  scale_linetype_manual(name = "", 
                        values = c("Deposits (R$ billions)" = "solid", 
                                   "Withdrawals (R$ billions)" = "dashed", 
                                   "Bank Accounts (millions)" = "dotted")) +
  scale_x_continuous(breaks = seq(min(public_data_summary$year), max(public_data_summary$year), by = 1)) +
  scale_y_continuous(
    labels = comma, 
    name = "Value (R$ billions)", 
    sec.axis = sec_axis(~., name = "Number of Bank Accounts (millions)")
  ) +
  labs(title = "Deposits, Withdrawals, and Bank Accounts", x = "Year") +
  theme_minimal() +
  theme(panel.grid.major.x = element_blank(),  # Remove vertical grid lines
        panel.grid.major.y = element_line(color = "lightgrey"),  # Light grey horizontal grid lines
        panel.grid.minor = element_blank(),  # Remove minor grid lines
        panel.background = element_blank(),  # Remove background panel
        axis.line = element_line(color = "black"),  # Black axis lines
        plot.title = element_text(hjust = 0.5, size = title_size),  # Align title in the middle and increase size
        axis.title.x = element_text(size = x_size),  # Increase x-axis title size
        axis.title.y = element_text(size = y_size),  # Increase y-axis title size
        axis.text.x = element_text(size = axis_size),  # Increase x-axis text size
        axis.text.y = element_text(size = axis_size),  # Increase y-axis text size
        legend.position = "bottom",  # Position of the legend at the bottom
        legend.title = element_blank(),  # Remove legend title
        legend.key.size = unit(legend_key_size, "lines"),  # Increase the size of the legend keys
        legend.text = element_text(size = legend_size),  # Increase the size of the legend text
        legend.background = element_rect(fill = "transparent")) +  # Transparent background for legend
  guides(color = guide_legend(nrow = 2, byrow = TRUE), 
         linetype = guide_legend(nrow = 2, byrow = TRUE))  # Break legend into 2 rows
print(gg)

# Adjust margins and save the plot as PNG
ggsave(paste0(output_path,"images/bank_accounts_deposits_withdrawls_over_years2.png"), gg, width = 8, height = 6, dpi = 300, bg = "transparent")


# Plot
gg <- ggplot(public_data_summary, aes(x = year)) +
  geom_line(aes(y = deposits / 10^9, color = "Deposits (R$ billions)", linetype = "Deposits (R$ billions)"), size = line_size) +
  geom_line(aes(y = valor_withdrawls, color = "Withdrawals (R$ billions)", linetype = "Withdrawals (R$ billions)"), size = line_size) +
  geom_line(aes(y = cash_in_circulation * 10, color = "Cash in Circulation (R$ billions)", linetype = "Cash in Circulation (R$ billions)"), size = line_size) +
  geom_vline(xintercept = 2020, linetype = "dotted", color = "black") +  # Add vertical line at x = 2020
  scale_color_manual(name = "", 
                     values = c("Deposits (R$ billions)" = "blue", 
                                "Withdrawals (R$ billions)" = "red", 
                                "Cash in Circulation (R$ billions)" = "green")) +
  scale_linetype_manual(name = "", 
                        values = c("Deposits (R$ billions)" = "solid", 
                                   "Withdrawals (R$ billions)" = "dashed", 
                                   "Cash in Circulation (R$ billions)" = "dotted")) +
  scale_x_continuous(breaks = seq(min(public_data_summary$year), max(public_data_summary$year), by = 1)) +
  scale_y_continuous(
    labels = comma, 
    name = "Deposits and Withdrawals (R$ billions)", 
    limits = c(0, 3900),  # Set limits for the primary y-axis
    sec.axis = sec_axis(~./10, name = "Cash in Circulation (R$ billions)")  # Scale the secondary y-axis to match the range
  ) +
  labs(title = "Bank Deposits, Withdrawals, and Cash in Circulation", x = "Year") +
  theme_minimal() +
  theme(panel.grid.major.x = element_blank(),  # Remove vertical grid lines
        panel.grid.major.y = element_line(color = "lightgrey"),  # Light grey horizontal grid lines
        panel.grid.minor = element_blank(),  # Remove minor grid lines
        panel.background = element_blank(),  # Remove background panel
        axis.line = element_line(color = "black"),  # Black axis lines
        plot.title = element_text(hjust = 0.5, size = 20),  # Align title in the middle and increase size
        axis.title.x = element_text(size = x_size),  # Increase x-axis title size
        axis.title.y = element_text(size = 15),  # Increase y-axis title size
        axis.text.x = element_text(size = axis_size),  # Increase x-axis text size
        axis.text.y = element_text(size = axis_size),  # Increase y-axis text size
        legend.position = "bottom",  # Position of the legend at the bottom
        legend.title = element_blank(),  # Remove legend title
        legend.key.size = unit(legend_key_size, "lines"),  # Increase the size of the legend keys
        legend.text = element_text(size = 15),  # Increase the size of the legend text
        legend.background = element_rect(fill = "transparent")) +  # Transparent background for legend
  guides(color = guide_legend(nrow = 2, byrow = TRUE), 
         linetype = guide_legend(nrow = 2, byrow = TRUE))  # Break legend into 2 rows
print(gg)

# Adjust margins and save the plot as PNG
ggsave(paste0(output_path,"images/bank_accounts_deposits_withdrawls_over_years3.png"), gg, width = 8, height = 6, dpi = 300, bg = "transparent")


# Plot
gg <- ggplot(public_data_summary, aes(x = year)) +
  geom_line(aes(y = deposits / 10^9, color = "Deposits", linetype = "Deposits"), size = line_size) +
  geom_line(aes(y = valor_withdrawls, color = "Withdrawals", linetype = "Withdrawals"), size = line_size) +
  geom_vline(xintercept = 2020, linetype = "dotted", color = "black") +  # Add vertical line at x = 2020
  scale_color_manual(name = "", 
                     values = c("Deposits" = "blue", 
                                "Withdrawals" = "red")) +
  scale_linetype_manual(name = "", 
                        values = c("Deposits" = "solid", 
                                   "Withdrawals" = "dashed")) +
  scale_x_continuous(breaks = seq(min(public_data_summary$year), max(public_data_summary$year), by = 1)) +
  scale_y_continuous(
    labels = comma, 
    name = "Deposits and Withdrawals (R$ billions)"
    #,limits = c(0, 3500)  # Set limits for the primary y-axis
  ) +
  labs(title = "Bank Deposits and Withdrawals", x = "Year") +
  theme_minimal() +
  theme(panel.grid.major.x = element_blank(),  # Remove vertical grid lines
        panel.grid.major.y = element_line(color = "lightgrey"),  # Light grey horizontal grid lines
        panel.grid.minor = element_blank(),  # Remove minor grid lines
        panel.background = element_blank(),  # Remove background panel
        axis.line = element_line(color = "black"),  # Black axis lines
        plot.title = element_text(hjust = 0.5, size = 20),  # Align title in the middle and increase size
        axis.title.x = element_text(size = x_size),  # Increase x-axis title size
        axis.title.y = element_text(size = 15),  # Increase y-axis title size
        axis.text.x = element_text(size = axis_size),  # Increase x-axis text size
        axis.text.y = element_text(size = axis_size),  # Increase y-axis text size
        legend.position = "bottom",  # Position of the legend at the bottom
        legend.title = element_blank(),  # Remove legend title
        legend.key.size = unit(legend_key_size, "lines"),  # Increase the size of the legend keys
        legend.text = element_text(size = 15),  # Increase the size of the legend text
        legend.background = element_rect(fill = "transparent")) +  # Transparent background for legend
  guides(color = guide_legend(nrow = 1, byrow = TRUE), 
         linetype = guide_legend(nrow = 1, byrow = TRUE))  # Break legend into 2 rows
print(gg)

# Adjust margins and save the plot as PNG
ggsave(paste0(output_path,"images/bank_accounts_deposits_withdrawls_over_years4.png"), gg, width = 8, height = 6, dpi = 300, bg = "transparent")




###########################################
# Card MDR (merchant discount rate)

public_data_summary_all <- read_excel(paste0(public_path, "public_data_summary.xlsx"))
# Read file: paste0(path, "card_mdr.xlsx")
card_mdr <- read_excel(paste0(public_path, "card_mdr.xlsx"))

card_mdr$date <- as.Date(card_mdr$date)

# Create a new column for the 'YYYY.Q' format
card_mdr$quarter <- paste0(year(card_mdr$date), ".", quarter(card_mdr$date))


card_mdr_filtered <- card_mdr %>% filter(date >= as.Date("2020-01-01"), date <= as.Date("2022-12-31"))

# Create the plot
gg <- ggplot(card_mdr_filtered, aes(x = quarter)) +
  geom_line(aes(y = credit_card/100, color = "Credit Card", group = 1), size = 2) +
  geom_line(aes(y = debit_card/100, color = "Debit Card", group = 2), size = 2) +
  geom_vline(xintercept = "2020.4", linetype = "dotted", color = "black") +
  labs(x = "Quarter", y = "Fee", title = "Merchant Discount Rate") +
  scale_y_continuous(limits = c(0, NA), labels = percent) +  
  theme_minimal() +
  theme(panel.grid.major.x = element_blank(),  # Remove vertical grid lines
        panel.grid.major.y = element_line(color = "lightgrey"),  # Light grey horizontal grid lines
        panel.grid.minor = element_blank(),  # Remove minor grid lines
        panel.background = element_blank(),  # Remove background panel
        axis.line = element_line(color = "black"),  # Black axis lines
        plot.title = element_text(hjust = 0.5, size = title_size),  # Align title in the middle and increase size
        axis.title.x = element_text(size = x_size),  # Increase x-axis title size
        axis.title.y = element_text(size = y_size),  # Increase y-axis title size
        axis.text.x = element_text(size = axis_size, angle = 45, hjust = 1),  # Increase x-axis text size
        axis.text.y = element_text(size = axis_size),  # Increase y-axis text size
        legend.position = "bottom",  # Position of the legend at the bottom
        legend.title = element_blank(),  # Remove legend title
        legend.key.size = unit(legend_key_size, "lines"),  # Increase the size of the legend keys
        legend.text = element_text(size = legend_size),  # Increase the size of the legend text
        legend.background = element_rect(fill = "transparent")) +  # Transparent background for legend
  guides(color = guide_legend(nrow = 1, byrow = TRUE), 
         linetype = guide_legend(nrow = 1, byrow = TRUE))  # Break legend into 2 rows
print(gg)
# Adjust margins and save the plot as PNG
ggsave(paste0(output_path,"images/mdr_card.png"), gg, width = 8, height = 6, dpi = 300, bg = "transparent")

