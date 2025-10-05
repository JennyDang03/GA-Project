# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

#This code is to download COSIF data from the Central Bank

from selenium import webdriver
import time
import zipfile
import os
import shutil
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Path to the downloaded folders
downloads_folder = r"C:\Users\mathe\Dropbox\PC (3)\Downloads"

# Destination folder for extracted CSV files
destination_folder = r"C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\CSV\COSIF_deposit_rate"

# Where to save the dta
dta_folder = r"C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Stata\dta"

# Where to save the graphs
output_folder = r"C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output"


######################################################################
# Lets download COSIF Data
######################################################################

# Create a new instance of the Chrome driver
driver = webdriver.Chrome()

# Loop through the years and months
for year in range(2018, 2023):
    for month in range(1, 13):
        year_month = f"{year:04d}{month:02d}"
        url = f"https://www4.bcb.gov.br/fis/cosif/cont/balan/bancos/{year_month}BANCOS.ZIP"
        driver.execute_script("window.open('', '_blank');")
        driver.switch_to.window(driver.window_handles[-1])
        driver.get(url)
        time.sleep(1)  # Adjust this sleep time as needed

# Close the browser
time.sleep(5)
driver.quit()

######################################################################
# Lets extract the data from Zipfolders 
######################################################################


# Loop through the downloaded folders
for year in range(2018, 2023):
    for month in range(1, 13):
        year_month = f"{year:04d}{month:02d}"
        zip_folder = os.path.join(downloads_folder, f"{year_month}BANCOS.zip")
        
        if os.path.exists(zip_folder):
            # Extract the CSV file from the zip folder
            with zipfile.ZipFile(zip_folder, 'r') as zip_ref:
                # Find the CSV file in the archive
                csv_filename = f"{year_month}BANCOS.CSV"
                matching_files = [file for file in zip_ref.namelist() if csv_filename in file]
                
                if matching_files:
                    zip_ref.extract(matching_files[0], destination_folder)
                    
                    # Move the extracted CSV file to the desired destination folder
                    source_csv = os.path.join(destination_folder, matching_files[0])
                    target_csv = os.path.join(destination_folder, f"{year_month}BANCOS.CSV")
                    shutil.move(source_csv, target_csv)
               
            
print("Extraction, and move completed.")

######################################################################
# Now lets delete the zip folders
######################################################################
# Loop through the downloaded folders
for year in range(2018, 2023):
    for month in range(1, 13):
        year_month = f"{year:04d}{month:02d}"
        zip_folder = os.path.join(downloads_folder, f"{year_month}BANCOS.zip")
        # Delete the ZIP file
        os.remove(zip_folder)
print("Deletion completed.")


######################################################################
# Now lets append the csvs
######################################################################


# List to store DataFrames from CSV files
dataframes = []

# Loop through the downloaded folders
for year in range(2018, 2023):
    for month in range(1, 13):
        year_month = f"{year:04d}{month:02d}"
        csv_file = os.path.join(destination_folder, f"{year_month}BANCOS.CSV")
        
        if os.path.exists(csv_file):
            # Read CSV file skipping the first 4 rows (header)
            #try:
            df = pd.read_csv(csv_file, skiprows=3, delimiter=";", encoding='latin1', decimal=",")
            
            # Convert saldo column from string to numeric
            dataframes.append(df)
            #except UnicodeDecodeError:
            #    print(f"UnicodeDecodeError: Unable to read {csv_file}")


# Concatenate all DataFrames
combined_df = pd.concat(dataframes, ignore_index=True)

# Rename columns to lowercase and follow Stata rules
def clean_column_name(column_name):
    cleaned_name = column_name.lower()  # Convert to lowercase
    cleaned_name = cleaned_name[:32]     # Truncate to max 32 characters
    cleaned_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in cleaned_name)  # Replace non-alphanumeric characters with underscores
    if cleaned_name in ["byte", "int", "long", "float", "double", "str"]:
        cleaned_name += "_var"  # Avoid Stata reserved words
    return cleaned_name

combined_df.rename(columns=clean_column_name, inplace=True)

# Save as Stata .dta file
stata_file = os.path.join(dta_folder, "COSIF_deposit_rate.dta")
combined_df.to_stata(stata_file, write_index=False)

print("CSVs appended and saved as .dta file.")

######################################################################
# Now lets filter the information
######################################################################

stata_file = os.path.join(dta_folder, "COSIF_deposit_rate.dta")
combined_df = pd.read_stata(stata_file)
# get just the information we need
# We used the ratio between "funding expenses" (Cosif 81100008) and the total of the
#accounts: "deposits" (Cosif 41000007); "liabilities for repo operations" (Cosif 42000006); and "resources of accepted exchange, bills
#of real estate and mortgage, debentures and similar items" (Cosif
#43000005)

# Filter rows based on the specified "conta" values
selected_contas = [81100008, 41000007, 42000006, 43000005]
filtered_df = combined_df.loc[combined_df['conta'].isin(selected_contas)]


# make it wide 
# Pivot the DataFrame to create a wide format
wide_df = filtered_df.pivot(index=['_data_base','documento','cnpj','agencia','nome_instituicao','cod_congl','nome_congl','taxonomia'], columns='conta', values='saldo')
# Rename columns with a prefix
wide_df.columns = [f'conta_{col}' for col in wide_df.columns]
# Reset index to flatten it
wide_df.reset_index(inplace=True)


# merge with selic

# Load the selic DataFrame
selic_file = os.path.join(dta_folder, "selic.dta")
selic_df = pd.read_stata(selic_file)

# Convert the "date" column in selic DataFrame to integer format
def convert_to_stata_date_int(date):
    year = date.year
    month = date.month
    stata_date_int = (year - 1960) * 12 + month - 1
    return stata_date_int

selic_df['date'] = selic_df['date'].apply(convert_to_stata_date_int)

# Convert _data_base to Stata date format
def convert_to_stata_date(year_month):
    year = year_month // 100
    month = year_month % 100
    stata_date = (year - 1960) * 12 + month - 1
    return stata_date

wide_df['date'] = wide_df['_data_base'].apply(convert_to_stata_date)
wide_df['year'] = wide_df['_data_base'] // 100
wide_df['month'] = wide_df['_data_base'] % 100

# Drop the original _data_base column
wide_df.drop('_data_base', axis=1, inplace=True)

# Merge the dataframes using the 'date' column
merged_df = wide_df.merge(selic_df, on='date', how='left')

# Calculate deposit_rate and deposit_rate2
merged_df['deposit_rate'] = np.nan
merged_df['deposit_rate2'] = np.nan

# Calculate deposit_rate
merged_df['deposit_rate'] = (-merged_df['conta_81100008']/((merged_df['month']-1)%6+1)) / (merged_df['conta_41000007'] + merged_df['conta_42000006'] + merged_df['conta_43000005'])

# Calculate deposit_rate2
merged_df['deposit_rate2'] = (-merged_df['conta_81100008']/((merged_df['month']-1)%6+1)) / (merged_df['conta_41000007'].fillna(0) + merged_df['conta_42000006'].fillna(0) + merged_df['conta_43000005'].fillna(0))

# Annualize deposit_rate and deposit_rate2
merged_df['deposit_rate_annual'] = 100*((1 + merged_df['deposit_rate']) ** 12 - 1)
merged_df['deposit_rate2_annual'] = 100*((1 + merged_df['deposit_rate2']) ** 12 - 1)



### Save
# Replace infinity and -infinity with NaN
merged_df.replace([np.inf, -np.inf], np.nan, inplace=True)
# Save as Stata .dta file
stata_file = os.path.join(dta_folder, "COSIF_deposit_rate_cleaned.dta")
merged_df.to_stata(stata_file, write_index=False)

######################################################################
### Collapse
######################################################################
stata_file = os.path.join(dta_folder, "COSIF_deposit_rate_cleaned.dta")
merged_df = pd.read_stata(stata_file)

# Add top5
#0		BCO DO BRASIL S.A.
#360305		CAIXA ECONOMICA FEDERAL
#60701190		ITA� UNIBANCO S.A.
#60746948		BCO BRADESCO S.A.
#90400888		BCO SANTANDER (BRASIL) S.A.
# Create the "top5" variable
top5_cnpjs = [0, 360305, 60701190, 60746948, 90400888]
merged_df['top5'] = merged_df['cnpj'].isin(top5_cnpjs).astype(int)

merged_df['weights'] = (merged_df['conta_41000007'] +
                       merged_df['conta_42000006'] +
                       merged_df['conta_43000005'])
merged_df.loc[merged_df['deposit_rate_annual'].isna(), 'weights'] = 0
merged_df.loc[merged_df['deposit_rate_annual'].isna(), 'deposit_rate_annual'] = -99

merged_df['weights2']  = (merged_df['conta_41000007'].fillna(0) +
                         merged_df['conta_42000006'].fillna(0) +
                         merged_df['conta_43000005'].fillna(0))
merged_df.loc[merged_df['deposit_rate2_annual'].isna(), 'weights2'] = 0
merged_df.loc[merged_df['deposit_rate2_annual'].isna(), 'deposit_rate2_annual'] = -99

result1 =  merged_df.groupby(['top5', 'date']).apply(lambda x: np.average(x['deposit_rate_annual'], weights=x['weights'])).reset_index()
result1.rename(columns={0: 'deposit_rate_annual'}, inplace=True)
result2 =  merged_df.groupby(['top5', 'date']).apply(lambda x: np.average(x['deposit_rate2_annual'], weights=x['weights2'])).reset_index()
result2.rename(columns={0: 'deposit_rate2_annual'}, inplace=True)
result3 = merged_df.groupby(['top5', 'date']).agg({
    'selic': 'mean',
    'mpshock': 'mean'
}).reset_index()

# Merge result1 and result2 DataFrames
result4 = result1.merge(result2, on=['top5', 'date'], how='left')

# Merge merged_result DataFrame with grouped_df
result5 = pd.merge(result3, result4, on=['top5', 'date'])


# Save the grouped DataFrame as Stata .dta file
grouped_stata_file = os.path.join(destination_folder, "COSIF_deposit_rate_collapsed.dta")
result5.to_stata(grouped_stata_file, write_index=False)

print("Data grouped by top5 and date, averages calculated, and grouped DataFrame saved as .dta file.")


######################################################################
# LETS PLOT!!!!!
######################################################################
grouped_stata_file = os.path.join(destination_folder, "COSIF_deposit_rate_collapsed.dta")
grouped_df = pd.read_stata(grouped_stata_file)

# Create the filtered DataFrame for top5 == 0
filtered_top5_0 = grouped_df[grouped_df['top5'] == 0][['date', 'selic', 'deposit_rate_annual', 'deposit_rate2_annual']]

# Create the filtered DataFrame for top5 == 1
filtered_top5_1 = grouped_df[grouped_df['top5'] == 1][['date', 'deposit_rate_annual', 'deposit_rate2_annual']]

# Rename the columns for the top5 == 1 DataFrame
filtered_top5_1.rename(columns={'deposit_rate_annual': 'deposit_rate_annual_top5'}, inplace=True)
filtered_top5_1.rename(columns={'deposit_rate2_annual': 'deposit_rate2_annual_top5'}, inplace=True)

# Merge the three time series data
merged_time_series = pd.merge(filtered_top5_1, filtered_top5_0, on='date', how='left')

# Convert date to a numeric representation for plotting
#merged_time_series['date'] = (merged_time_series['date'] - 696) / 6  # Adjust for plotting

time_series_df = merged_time_series
# Create date labels for the x-axis
date_labels = [f'{["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"][int(month)]} {int(year)}' for month, year in zip(time_series_df['date'] % 12, time_series_df['date'].astype(int) // 12 + 1960)]

# Plotting
plt.figure(figsize=(10, 6))
plt.plot(time_series_df['date'], time_series_df['selic'], label='Selic')
plt.plot(time_series_df['date'], time_series_df['deposit_rate_annual_top5'], label='Deposit Rate Top5')
plt.plot(time_series_df['date'], time_series_df['deposit_rate_annual'], label='Deposit Rate Others')
plt.xlabel('Time')
plt.ylabel('Percent')
plt.title('Deposit Rate')
plt.legend()

# Set x-axis tick labels
subset_dates = time_series_df['date'][::6]  # Show every 6th date
subset_labels = date_labels[::6]  # Corresponding labels for the subset dates
plt.xticks(subset_dates, subset_labels, rotation=45)  # Use subset dates and labels, rotate x-axis labels for better readability

plt.grid(False)
plt.tight_layout()  # Adjust layout for better display
# Save the plot as an image file
image_file_path = os.path.join(output_folder, "deposit_rate_plot.png")
plt.savefig(image_file_path, dpi=300)  # Save the plot with higher resolution (300 dpi)
plt.show()
print(f"Plot saved as '{image_file_path}'.")



##########

# Plotting
plt.figure(figsize=(10, 6))
plt.plot(time_series_df['date'], time_series_df['selic'], label='Selic')
plt.plot(time_series_df['date'], time_series_df['deposit_rate2_annual_top5'], label='Deposit Rate Top5')
plt.plot(time_series_df['date'], time_series_df['deposit_rate2_annual'], label='Deposit Rate Others')
plt.xlabel('Time')
plt.ylabel('Percent')
plt.title('Deposit Rate')
plt.legend()

# Set x-axis tick labels
subset_dates = time_series_df['date'][::6]  # Show every 6th date
subset_labels = date_labels[::6]  # Corresponding labels for the subset dates
plt.xticks(subset_dates, subset_labels, rotation=45)  # Use subset dates and labels, rotate x-axis labels for better readability

plt.grid(False)
plt.tight_layout()  # Adjust layout for better display

plt.show()

print(f"Plot saved as '{image_file_path}'.")






