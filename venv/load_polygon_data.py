import requests
import snowflake.connector
import json

# Polygon API endpoint and your API key
url = "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2023-11-01/2024-11-12?apiKey=Em7xrXc5QX01uQqD29xxTrVZXfrrjC6Q"

# Fetch data from Polygon API
response = requests.get(url)
if response.status_code == 200:
    data = response.json()  # Parse JSON data
else:
    print("Failed to fetch data:", response.status_code)
    exit()

# Connect to Snowflake
conn = snowflake.connector.connect(
    user="YOUR_USERNAME",
    password="YOUR_PASSWORD",
    account="YOUR_ACCOUNT_IDENTIFIER",
    warehouse="YOUR_WAREHOUSE",
    database="YOUR_DATABASE",
    schema="YOUR_SCHEMA"
)
cursor = conn.cursor()

# Insert JSON data into Snowflake
try:
    # Insert data as JSON variant into the polygon_data table
    insert_query = "INSERT INTO polygon_data(data) VALUES (PARSE_JSON(%s))"
    cursor.execute(insert_query, (json.dumps(data),))
    print("Data successfully inserted into Snowflake.")
finally:
    cursor.close()
    conn.close()
