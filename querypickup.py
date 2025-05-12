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

# We can query here the data we want to use for prediction
# Corrected Flux query

with open("query.txt", "r") as f:
    queries = f.read().split('---')

L = []
for query in queries:
    query = query.strip()
    if not query:
        continue 

    # Drop in the query 
    # See InfluxDb query -> More efficient 

    query = query.replace("TIME_RANGE_START", time_range_start)
    query = query.replace("TIME_RANGE_STOP", time_range_stop)
    query = query.replace("WINDOW_PERIOD", window_period)
    
    print("Executing query:\n", query)
    df = client.query_api().query_data_frame(query)
    if type(df) != list:
        if not df.empty:
            print(dataset.columns)
            df = df.drop(columns=['result', 'table'], errors='ignore')
            L.append(df)
    else:
        for dataset in df:
            print(dataset.columns)
            dataset = dataset.drop(columns=['result', 'table'], errors='ignore')
            L.append(dataset)
print(L)
if L == []:
    print("No data found for the given time range.")
else: 
    df_final = L[0]

i = 0
for df in L[1:]:
    i += 1
    df_final = pd.merge(df_final, df, on="_time", how="outer",  suffixes=(f'_{i-1}', f'_{i}'))

df_final = df_final.dropna() # Can be done in the query itself
df_final = df_final.drop_duplicates()

column_sets = [set(df.columns) for df in L]

# Find common and unique columns
common_columns = set.intersection(*column_sets)
unique_columns = set.union(*column_sets)

print("Common columns:", common_columns)
print("All unique columns:", unique_columns)

print(df_final.columns)

df_to_push = make_prediction(df_final)


_write_client = client.write_api()
_write_client.write('test2',
                    data_frame_measurement_name="prediction", 
                    record=df_to_push,
                    data_frame_timestamp_column='_time'
                    )

print("Writen!!")

# Closing the connection
print("Closing connection...")
client.close()


# crontab -e -> x x x x x to run it every x
# Or python task executer -> wrap main in a function and call it from the task executer
