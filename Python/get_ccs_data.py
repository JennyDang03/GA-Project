# -*- coding: utf-8 -*-
"""
Created on Sun Oct 13 12:03:36 2024

@author: mathe
"""

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import time
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
# Set up Selenium driver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

file_path = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\CSV\ccs_public_data.csv'
image_path = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output\images\ccs_public_data.png'

# List of URLs for pages 1 to 19
urls = [f'https://www.bcb.gov.br/acessoinformacao/ccsestatisticas?ccsestatisticas_page={i}' for i in range(1, 20)]

# Initialize an empty list to store data
all_data = []

# Loop through each URL (page)
for url in urls:
    driver.get(url)
    
    # Wait for the table to load (up to 20 seconds)
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.TAG_NAME, "tbody"))
    )
    time.sleep(5)
    # Get the page source after the table is loaded
    page_source = driver.page_source
    
    # Parse the page with BeautifulSoup
    soup = BeautifulSoup(page_source, 'html.parser')
    
    # Find all rows in the table
    tbody = soup.find('tbody')
    rows = tbody.find_all('tr')
    
    # Extract text from each <div> inside the <td> elements for all rows
    for row in rows:
        cols = [td.find('div').text.strip() for td in row.find_all('td') if td.find('div')]
        if cols:  # Only append if there is data in the row
            all_data.append(cols)

# Close the browser
driver.quit()

# Define column names
columns = ['Date', 'Accounts', 'People', 'Firms']

# Create a DataFrame with the collected data
df = pd.DataFrame(all_data, columns=columns)

# Convert the 'Date' column to datetime format
df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y')
def clean_numeric_column(column):
    # Remove periods and convert to numeric
    column_cleaned = column.str.replace('.', '', regex=False)
    return pd.to_numeric(column_cleaned, errors='coerce')

# Clean 'Accounts' and 'People' columns by removing periods
df['Accounts'] = clean_numeric_column(df['Accounts'])
df['People'] = clean_numeric_column(df['People'])
df['Firms'] = clean_numeric_column(df['Firms'])

# Save the DataFrame to a CSV file
df.to_csv(file_path, index=False)


print("Data successfully scraped, converted, and saved to 'combined_table.csv'")

df = pd.read_csv(file_path)
df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d', errors='coerce')

df = df.sort_values('Date')
# Drop rows with invalid dates
df = df.dropna(subset=['Date'])
# Create a new column for Accounts/People ratio
df['Accounts_per_People'] = df['Accounts'] / df['People']







# Create the plot with larger text and no grid
fig, ax1 = plt.subplots(figsize=(14, 8))

# Plot 'People' on the left y-axis (adjusted to show in millions) with new color
ax1.plot(df['Date'], df['People'] / 1e6, color='#1f77b4', linewidth=2, label='People')  # Soft blue color
ax1.set_xlabel('Date', fontsize=24)
ax1.set_ylabel('Individuals with Accounts (millions)', color='#1f77b4', fontsize=24)
ax1.tick_params(axis='y', labelcolor='#1f77b4', labelsize=20)
ax1.tick_params(axis='x', labelsize=20)
ax1.set_ylim(0, 212)

# Convert the event dates for the vertical lines using mdates.date2num
covid_stimulus_date = mdates.date2num(pd.Timestamp('2020-04-01'))
pix_date = mdates.date2num(pd.Timestamp('2020-11-16'))

# Add vertical line for "Covid Stimulus" on 2020-04-01
ax1.axvline(covid_stimulus_date, color='gray', linestyle='--', linewidth=2)
ax1.text(covid_stimulus_date + 20, ax1.get_ylim()[0] + 5, 'Covid Stimulus', color='gray', rotation=90, va='bottom', fontsize=20)

# Add vertical line for "Pix" on 2020-11-16
ax1.axvline(pix_date, color='green', linestyle='--', linewidth=2)
ax1.text(pix_date + 20, ax1.get_ylim()[0] + 5, 'Pix', color='green', rotation=90, va='bottom', fontsize=20)

# Set the x-axis ticks for each year from 2018 to 2024, centered at June 1st
tick_positions = [pd.Timestamp(f'{year}-06-01') for year in range(2018, 2025)]
ax1.set_xlim(pd.Timestamp('2018-01-01'), pd.Timestamp('2024-12-31'))
ax1.set_xticks(tick_positions)
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))

# Add minor ticks (little vertical lines) at the beginning of each year (January 1st)
minor_tick_positions = [pd.Timestamp(f'{year}-01-01') for year in range(2019, 2025)]
ax1.set_xticks(minor_tick_positions, minor=True)
ax1.tick_params(axis='x', which='minor', length=5, width=1.5, color='black')  # Style of the minor ticks

# Center the x-axis ticks for the years
ax1.tick_params(axis='x', which='both', labelsize=22, pad=10)
plt.xticks(rotation=0, ha='center')  # Center years horizontally

# Creating a second y-axis to plot Accounts/People ratio with a new color
ax2 = ax1.twinx()
ax2.plot(df['Date'], df['Accounts_per_People'], color='#ff7f0e', linewidth=2, label='Accounts per Individuals')  # Soft orange color
ax2.set_ylabel('Accounts per Individuals', color='#ff7f0e', fontsize=24)
ax2.tick_params(axis='y', labelcolor='#ff7f0e', labelsize=20)
ax2.set_ylim(0, df['Accounts_per_People'].max() * 1.05)  # Start from 0 and add some padding

# Adding titles and improving the plot layout
plt.title('Financial Accessibility', fontsize=26, fontweight='bold')

# Remove gridlines
ax1.grid(False)
ax2.grid(False)

# Save the plot to the specified path
plot_save_path = r'C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output\images\people_accounts_plot.png'
#fig.patch.set_alpha(0)  # Make the background transparent
plt.savefig(plot_save_path, dpi=300)
print(f"Graph successfully saved to {plot_save_path}")

# Show the plot
plt.show()