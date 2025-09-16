
source(paste0(R_path,"/functions/time_id_to_month.R"))
source(paste0(R_path,"/functions/time_id_to_year.R"))


if_income <- read_dta("C:/Users/mathe/Dropbox/RESEARCH/Pix vs CBDC project/data/IFdata/IF_incomestatements.dta")
if_income <- if_income %>%
  select(institution, cnpj, financialcongname, financialcongcode, prudentialcongcode, tcb, tc, ti, netincome_j, date, dateq, quarter)


if_deposit <- read_dta("C:/Users/mathe/Dropbox/RESEARCH/Pix vs CBDC project/data/IFdata/IF_liabilities_balancesheet.dta")
if_deposit <- if_deposit %>%
  select(institution, cnpj, financialcongname, financialcongcode, prudentialcongcode, tcb, tc, ti, totaldep_a, date)

if_assets <- read_dta("C:/Users/mathe/Dropbox/RESEARCH/Pix vs CBDC project/data/IFdata/IF_assets_balancesheet.dta")
if_assets <- if_assets %>%
  select(institution, cnpj, financialcongname, financialcongcode, prudentialcongcode, tcb, tc, ti, grossloans_d1, loans_d, date)

# Merge
if_data <- merge(if_deposit, if_assets, by = c("institution", "cnpj", "financialcongname", "financialcongcode", "prudentialcongcode", "tcb", "tc", "ti", "date"))
if_data <- merge(if_data, if_income, by = c("institution", "cnpj", "financialcongname", "financialcongcode", "prudentialcongcode", "tcb", "tc", "ti", "date"))

# n4 - Payment institutions, b1 is banks
if_data <- if_data %>%
  mutate(IP = ifelse(tcb == "n4", 1, 0),
         Bank = ifelse(tcb == "b1", 1, 0))

# Collapse by conglomerate using prudentialcongcode
if_data <- if_data %>%
  group_by(prudentialcongcode, date) %>%
  summarise(IP = max(IP, na.rm = TRUE),
            Bank = max(Bank, na.rm = TRUE),
            totaldep_a = sum(totaldep_a, na.rm = TRUE),
            grossloans_d1 = sum(grossloans_d1, na.rm = TRUE),
            loans_d = sum(loans_d, na.rm = TRUE),
            netincome_j = sum(netincome_j, na.rm = TRUE),
            .groups = "drop")


# Bank == 1 and IP == 0 or Bank ==0 and IP == 1
#if_filter <- if_data %>%
#  filter((Bank == 1 & IP == 0) | (Bank == 0 & IP == 1))
if_filter <- if_data %>%
  filter((Bank == 1) | (Bank == 0 & IP == 1)) %>%
  mutate(IP = ifelse(Bank == 1, 0, 1))
  

if_filter <- if_filter %>%
  group_by(date, Bank, IP) %>%
  summarise(totaldep_a = sum(totaldep_a, na.rm = TRUE),
            grossloans_d1 = sum(grossloans_d1, na.rm = TRUE),
            loans_d = sum(loans_d, na.rm = TRUE),
            netincome_j = sum(netincome_j, na.rm = TRUE),
            .groups = "drop")

# graph the division over time of grossloans_d1 by totaldep_a, and loans_d by totaldep_a

#fix date, convert to quarter

if_filter$month <- time_id_to_month(if_filter$date)
if_filter$year <- time_id_to_year(if_filter$date)
# convert month and year to quarter date
if_filter$quarter <- as.Date(paste(if_filter$year, if_filter$month, "01", sep = "-"))


if_filter$loans_to_deposits_ratio <- if_filter$grossloans_d1 / if_filter$totaldep_a
if_filter$loans_to_deposits_ratio2 <- if_filter$loans_d / if_filter$totaldep_a


if_filter_2019 <- if_filter %>%
  filter(year >= 2019)
# Create the plot

title_size = 24
x_size = 0.1
y_size = 20
axis_size = 18
line_size = 2
legend_key_size = 2
legend_size = 20

if_filter_2019_bank <- if_filter_2019 %>%
  filter(Bank == 1) %>% select(quarter, loans_to_deposits_ratio2, netincome_j) %>%
  rename(loans_to_deposits_ratio2_bank = loans_to_deposits_ratio2, netincome_j_bank = netincome_j)
if_filter_2019_shadow <- if_filter_2019 %>%
  filter(IP == 1) %>% select(quarter, loans_to_deposits_ratio2, netincome_j) %>%
  rename(loans_to_deposits_ratio2_shadow = loans_to_deposits_ratio2, netincome_j_shadow = netincome_j)
if_filter_2019_merged <- merge(if_filter_2019_bank, if_filter_2019_shadow, by = "quarter")


gg <- ggplot(if_filter_2019_merged, aes(x = quarter)) +
  geom_line(aes(y = loans_to_deposits_ratio2_bank, color = "Banks", linetype = "Banks"), size = line_size) +
  geom_line(aes(y = loans_to_deposits_ratio2_shadow, color = "NBFIs", linetype = "NBFIs"), size = line_size) +
  geom_vline(xintercept = as.Date("2020-11-16"), linetype = "dotted", color = "black") +  # Add vertical line at x = 2020
  labs(title = "Loans to Deposits Ratio", x = "", y = "") +
  scale_color_manual(name = "", 
                     values = c("Banks" = "blue", "NBFIs" = "red")) +
  scale_linetype_manual(name = "", 
                        values = c("Banks" = "solid", "NBFIs" = "longdash")) +
  scale_y_continuous(breaks = seq(0, 1.25, by = 0.25)) +
  ylim(0,1) +
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
ggsave(paste0(output_path,"images/loan_to_deposit.png"), gg, width = 8, height = 6, dpi = 300, bg = "transparent")

gg <- ggplot(if_filter_2019_merged, aes(x = quarter)) +
  geom_line(aes(y = netincome_j_bank/1000000, color = "Banks", linetype = "Banks"), size = line_size) +
  geom_line(aes(y = netincome_j_shadow/1000000, color = "NBFIs", linetype = "NBFIs"), size = line_size) +
  geom_vline(xintercept = as.Date("2020-11-16"), linetype = "dotted", color = "black") +  # Add vertical line at x = 2020
  labs(title = "Net Income", x = "", y = "Billions") +
  scale_color_manual(name = "", 
                     values = c("Banks" = "blue", "NBFIs" = "red")) +
  scale_linetype_manual(name = "", 
                        values = c("Banks" = "solid", "NBFIs" = "longdash")) +
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
ggsave(paste0(output_path,"images/net_income.png"), gg, width = 8, height = 6, dpi = 300, bg = "transparent")




