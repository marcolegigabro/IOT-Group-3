
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

# Corrected Flux query
# Reference: https://www.youtube.com/watch?v=cMkQXLCbFQY
query = f'''
from(bucket: "sensor")
  |> range(start: {time_range_start}, stop: {time_range_stop})
  |> filter(fn: (r) => r["_measurement"] == "66a37cc69a4ead4ef37c5479")
  |> filter(fn: (r) => r["_field"] == "occupancy")
  |> aggregateWindow(every: {window_period}, fn: mean, createEmpty: false)
  |> yield(name: "mean")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
'''

# Retrieve data into a DataFrame
df = client.query_api().query_data_frame(query)
df = df[0]
df = df.drop(columns=['_start', '_stop', 'result', 'table'])  # Remove unnecessary columns
df = df.set_index('_time')  # Set the time column as the index

# Writing connection
"""
_write_client = client.write_api()
_write_client.write('test2', 
                    data_frame_measurement_name="humidity2", 
                    record=df,
                    data_frame_tag_columns=["country", "neighborhood", "station_id", 'unit']
                    )

print("✅ ")
"""
# Closing the connection
print("Closing connection...")
client.close()

