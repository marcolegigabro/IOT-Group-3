from influxdb_client import InfluxDBClient, WriteOptions
import pandas as pd
from model import make_prediction

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
    query = query.strip()
    if not query:
        continue 

    
    query = query.replace("TIME_RANGE_START", time_range_start)
    query = query.replace("TIME_RANGE_STOP", time_range_stop)
    query = query.replace("WINDOW_PERIOD", window_period)
    
    print("Executing query:\n", query)
    df = client.query_api().query_data_frame(query)
    if type(df) != list:
        if not df.empty:
            df = df.drop(columns=['_start', '_stop', 'result', 'table', 'topic', 'gateway_type', 'building', 'sensor_type', 'source', 'room', 'floor', 'gateway', 'sensor', '_measurement'], errors='ignore')
            df = df.set_index('_time')
            L.append(df)
    else:
        for dataset in df:
        
            dataset = dataset.drop(columns=['_start', '_stop', 'result', 'table', 'topic', 'gateway_type', 'building', 'sensor_type', 'source', 'room', 'floor', 'gateway', 'sensor', '_measurement'], errors='ignore')
            dataset = dataset.set_index('_time')
            L.append(dataset)

df_final = L[0]

i = 0
for df in L[1:]:
    i += 1
    df_final = pd.merge(df_final, df, on="_time", how="outer",  suffixes=(f'_{i-1}', f'_{i}'))

df_final = df_final.dropna()
df_final = df_final.drop_duplicates()

column_sets = [set(df.columns) for df in L]

# Find common and unique columns
common_columns = set.intersection(*column_sets)
unique_columns = set.union(*column_sets)

print("Common columns:", common_columns)
print("All unique columns:", unique_columns)

df_final.to_csv('./data.csv',index=True)

df_to_push = make_prediction(df_final)


_write_client = client.write_api()
_write_client.write('test2',
                    data_frame_measurement_name="prediction", 
                    record=df_to_push,
                    data_frame_tag_columns=["country", "neighborhood", "station_id", 'unit'] #TODO
                    )

print("Writen!!")

# Closing the connection
print("Closing connection...")
client.close()

