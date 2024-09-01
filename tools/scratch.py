import requests
import pandas as pd
import io
import os

# Fetch the CSV file
url = "https://raw.githubusercontent.com/HaydenWStone/naturalgasprices/main/data/henry_hub_prices.csv"
response = requests.get(url)
data = response.content.decode('utf-8')

# Load the data into a DataFrame
df = pd.read_csv(io.StringIO(data))

# Add a new column 'reversal' with 'NO'
df['reversal'] = 'NO'

# Check for reversals using shifted data
df.loc[((df['flag'].shift(1) == 'UP') & (df['flag'] == 'DOWN')) |
       ((df['flag'].shift(1) == 'DOWN') & (df['flag'] == 'UP')), 'reversal'] = 'YES'


# Ensure the directory exists
output_directory = '../data'
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Save the updated DataFrame to the specified location
output_file_path = os.path.join(output_directory, 'henry_hub_prices.csv')
df.to_csv(output_file_path, index=False)

# Display the updated DataFrame (optional)
print(df)

# Save the modified DataFrame to a new CSV file
df.to_csv('henry_hub_prices_test.csv', index=False)

print("The file has been saved as henry_hub_prices.csv.")
