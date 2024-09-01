import pandas as pd

# Load the CSV file
file_path = 'henry_hub_prices.csv'
df = pd.read_csv(file_path)

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

# Save the updated DataFrame to a new CSV file (optional)
df.to_csv('henry_hub_prices.csv', index=False)

# Display the updated DataFrame
print(df)
