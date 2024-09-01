import requests
import pandas as pd
import time

# Your API key
api_key = "key here"

# Read the existing CSV file to get the most recent date
try:
    existing_df = pd.read_csv("henry_hub_prices.csv")
    most_recent_date = pd.to_datetime(existing_df['period']).max()
    most_recent_date_str = most_recent_date.strftime('%Y-%m-%d')
    print(f"Most recent date in existing data: {most_recent_date_str}")
except FileNotFoundError:
    existing_df = pd.DataFrame(columns=["period", "value"])
    most_recent_date_str = None
    print("No existing data found, fetching all data.")

# Base URL
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

# Add the start date to the API parameters if we have one
if most_recent_date_str:
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
if most_recent_date_str:
    df['period'] = pd.to_datetime(df['period'])
    df = df[df['period'] > most_recent_date]

# Append the new data to the existing CSV file
if not df.empty:
    df.to_csv("henry_hub_prices.csv", mode='a', header=False, index=False)
    print(f"Appended {len(df)} new records to 'henry_hub_prices.csv'.")
else:
    print("No new data to append.")

print(f"Data fetching complete. Total new records: {len(df)}")
