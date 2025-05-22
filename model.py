import pandas as pd 
from sklearn.ensemble import RandomForestRegressor  
import requests



def make_prediction(df, freq='5min', forecast_days=7):
    df = df.copy()
    df = df.dropna(subset=['occupancy', 'temperature'])
    df['_time'] = pd.to_datetime(df['_time'], errors='coerce')
    df = df.dropna(subset=['_time'])
    df['_time'] = df['_time'].dt.tz_convert(None)

    df['minute'] = df['_time'].dt.minute
    df['hour'] = df['_time'].dt.hour
    df['dayofweek'] = df['_time'].dt.dayofweek

    future_times = pd.date_range(
        start=df['_time'].max() + pd.Timedelta(freq),
        periods=int((pd.Timedelta(days=forecast_days) / pd.Timedelta(freq))),
        freq=freq
    )
    
    future_df = pd.DataFrame({
            '_time': future_times,
            'minute': future_times.minute,
            'hour': future_times.hour,
            'dayofweek': future_times.dayofweek
    })

    df_weather = get_weather()

    
    if df_weather is None or df_weather.empty:
        features = ['minute', 'hour', 'dayofweek']
    else:
        features = ['minute', 'hour', 'dayofweek', 'precip_rate', 'temperatureext']
        future_df = pd.merge(
            future_df, 
            df_weather.rename(columns={"time": "_time"})[['_time', 'temperature', 'precip_rate']], 
            on='_time', 
            how='left'
        )

    X = df[features]  
    y_occ = df['occupancy']
    y_temp = df['temperature']
    model_occ = RandomForestRegressor()
    model_temp = RandomForestRegressor()
    model_occ.fit(X, y_occ)
    model_temp.fit(X, y_temp)
    future_df['occupancy'] = model_occ.predict(future_df[features])
    future_df['occupancy'] = (future_df['occupancy'] >= 0.5).astype(int)
    future_df['temperature'] = model_temp.predict(future_df[features])

    return future_df[['_time', 'occupancy', 'temperature']]



def get_weather():
    # Odense lat long
    latitude = 55.366417
    longitude = 10.429611

    # parameter to get
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "temperature_2m,precipitation,relative_humidity_2m,windspeed_10m",
        "timezone": "Europe/Copenhagen"
    }

    # Request
    response = requests.get("https://api.open-meteo.com/v1/forecast", params=params)
    data = response.json()

    # DF
    df_hourly = pd.DataFrame({
        "time": pd.to_datetime(data["hourly"]["time"]),
        "temperatureext": data["hourly"]["temperature_2m"],
        "precip_rate": data["hourly"]["precipitation"],
        "humidity": data["hourly"]["relative_humidity_2m"],
        "wind_speed": data["hourly"]["windspeed_10m"]
    })


    # 5m window
    df_hourly.set_index("time", inplace=True)
    df_5min = df_hourly.resample("5min").interpolate(method="linear").reset_index()
    return df_5min