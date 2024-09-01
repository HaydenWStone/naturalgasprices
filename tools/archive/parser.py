import pandas as pd
import requests
from io import StringIO

# URL of the CSV file
url = 'https://raw.githubusercontent.com/HaydenWStone/naturalgasprices/main/data/henry_hub_prices.csv'

# Fetch the CSV file using requests
response = requests.get(url)
response.raise_for_status()  # Ensure the request was successful

# Load the CSV data into a DataFrame
df = pd.read_csv(StringIO(response.text))

# Ensure the 'period' column is in datetime format
df['period'] = pd.to_datetime(df['period'])

# Sort the DataFrame by 'period' in ascending order (oldest date first)
df.sort_values(by='period', inplace=True)

# Add a column for the day of the week
df['day_of_week'] = df['period'].dt.day_name()

# Calculate the percentage change from the previous day, multiply by 100 to convert to percentage, and round to 3 decimal places
df['percentage_change'] = df['value'].pct_change().multiply(100).round(3)

# Add a binary flag for "UP", "DOWN", or "FLAT" based on the percentage change
df['flag'] = df['percentage_change'].apply(lambda x: 'UP' if x > 0 else ('DOWN' if x < 0 else 'FLAT'))

# Calculate the "run" column, which counts the number of successive previous days where the flag has been "UP" or "DOWN" continually
df['run'] = df.groupby((df['flag'] != df['flag'].shift()).cumsum()).cumcount() + 1

# Save the updated DataFrame to the location one directory level higher
output_file_path = '../data/henry_hub_prices.csv'
df.to_csv(output_file_path, index=False)

# Display the updated DataFrame (optional)
print(df)
