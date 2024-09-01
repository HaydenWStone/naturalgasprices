import requests
import pandas as pd
import time
from io import StringIO
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Retrieve the api key from environment variables
api_key = os.getenv('api_key')

# URL of the CSV file on GitHub
url = 'https://raw.githubusercontent.com/HaydenWStone/naturalgasprices/main/data/henry_hub_prices.csv'

# Fetch the CSV file from GitHub
response = requests.get(url)
response.raise_for_status()  # Ensure the request was successful

# Load the CSV data into a DataFrame
existing_df = pd.read_csv(StringIO(response.text))

# Ensure the 'period' column is in datetime format
existing_df['period'] = pd.to_datetime(existing_df['period'])

# Get the most recent date from the GitHub file
most_recent_date = existing_df['period'].max()
most_recent_date_str = most_recent_date.strftime('%Y-%m-%d')
print(f"Most recent date in existing data: {most_recent_date_str}")

# Base URL for the API
base_url = "https://api.eia.gov/v2/natural-gas/pri/fut/data/"
params = {
    "api_key": api_key,
    "frequency": "daily",
    "data[0]": "value",
    "facets[series][]": "RNGWHHD",
    "sort[0][column]": "period",
    "sort[0][direction]": "desc",
    "length": 5000,
    "offset": 0
}

# Add the start date to the API parameters
params["start"] = most_recent_date_str

# Function to fetch data with retry mechanism
def fetch_data(url, params):
    attempts = 0
    max_attempts = 5
    while attempts < max_attempts:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            attempts += 1
            print(f"Attempt {attempts} failed. Retrying in 5 seconds...")
            time.sleep(5)
    return None

# Fetch data
all_data = []
offset = 0

while True:
    params["offset"] = offset
    data = fetch_data(base_url, params)

    if data is None:
        print("Failed to fetch data after multiple attempts.")
        break

    # Extract data from response
    if "response" in data and "data" in data["response"]:
        entries = data["response"]["data"]
        all_data.extend(entries)

        print(f"Fetched {len(entries)} records in this batch, total records so far: {len(all_data)}")

        # If fewer entries are returned than requested, it might be the last chunk
        if len(entries) < params["length"]:
            break
    else:
        print("No more data available or response format has changed.")
        break

    # Increment offset for the next batch
    offset += params["length"]

# Convert the list of data to a DataFrame
df = pd.DataFrame(all_data)

# Keep only the 'period' and 'value' columns
df = df[['period', 'value']]

# Filter out records that are already in the existing CSV file
df['period'] = pd.to_datetime(df['period'])
df = df[df['period'] > most_recent_date]

# Append the new data to the existing DataFrame
if not df.empty:
    updated_df = pd.concat([existing_df, df], ignore_index=True)
    print(f"Appended {len(df)} new records.")
else:
    print("No new data to append.")
    updated_df = existing_df

# Convert the 'value' column to numeric, coercing errors to NaN
updated_df['value'] = pd.to_numeric(updated_df['value'], errors='coerce')

# Drop any rows where 'value' is NaN
updated_df.dropna(subset=['value'], inplace=True)

# Perform the operations on the updated data
updated_df.sort_values(by='period', inplace=True)

# Add a column for the day of the week
updated_df['day_of_week'] = updated_df['period'].dt.day_name()

# Calculate the percentage change from the previous day
updated_df['percentage_change'] = updated_df['value'].pct_change().multiply(100).round(3)

# Add a binary flag for "UP", "DOWN", or "FLAT"
updated_df['flag'] = updated_df['percentage_change'].apply(lambda x: 'UP' if x > 0 else ('DOWN' if x < 0 else 'FLAT'))

# Calculate the "run" column
updated_df['run'] = updated_df.groupby((updated_df['flag'] != updated_df['flag'].shift()).cumsum()).cumcount() + 1

# Add a new column 'reversal' with 'NO'
updated_df['reversal'] = 'NO'

# Check for reversals using shifted data
updated_df.loc[((updated_df['flag'].shift(1) == 'UP') & (updated_df['flag'] == 'DOWN')) |
       ((updated_df['flag'].shift(1) == 'DOWN') & (updated_df['flag'] == 'UP')), 'reversal'] = 'YES'

# Ensure the directory exists
output_directory = '../data'
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Save the updated DataFrame to the specified location
output_file_path = os.path.join(output_directory, 'henry_hub_prices.csv')
updated_df.to_csv(output_file_path, index=False)

# Display the updated DataFrame (optional)
print(updated_df)
