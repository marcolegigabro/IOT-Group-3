from influxdb_client import InfluxDBClient, WriteOptions
import pandas as pd

# Connection configuration
url = "http://tek-iot-c.sandbox.tek.sdu.dk:8086/"  # Replace with your InfluxDB server URL
token = "QtrjwTZV8Yl1w"  # Replace with your authentication token
org = "cei"  # Replace with your organization
bucket = "weather"  # Replace with your bucket

# Connecting to InfluxDB
client = InfluxDBClient(url=url, token=token, org=org)

# Connection health check
try:
    health = client.health()
    print(f"Connection status: {health.status}")
except Exception as e:
    print(f"Connection error: {e}")

# Retrieving data using a Flux query
time_range_start = "-30d"  # Data from the last 30 days
time_range_stop = "now()"  # Up to the current time
window_period = "5m"  # Aggregation window of 5 minutes


with open("query.txt", "r") as f:
    queries = f.read().split('---')

L = []
for query in queries:
    query = query.strip()  # Remove extra spaces/newlines
    if not query:
        continue 

    # Replace placeholders with actual values
    query = query.replace("TIME_RANGE_START", time_range_start)
    query = query.replace("TIME_RANGE_STOP", time_range_stop)
    query = query.replace("WINDOW_PERIOD", window_period)
    
    print("Executing query:\n", query)
    df = client.query_api().query_data_frame(query)
    if type(df) != list:
        if not df.empty:
            df = df.drop(columns=['_start', '_stop', 'result', 'table'], errors='ignore')
            df = df.set_index('_time')
            L.append(df)
    else:
        df = df[0]
        df = df.drop(columns=['_start', '_stop', 'result', 'table'], errors='ignore')
        df = df.set_index('_time')
        L.append(df)

print(L)
# Closing the connection
print("Closing connection...")
client.close()

