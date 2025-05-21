import pandas as pd 
from sklearn.ensemble import RandomForestRegressor  

def make_prediction(df, freq='5min', forecast_days=7):
    df = df.dropna(subset=['occupancy', 'temperature'])
    df['_time'] = pd.to_datetime(df['_time'], format='mixed', utc=True, errors='coerce')
    df = df.dropna(subset=['_time'])
    df['_time'] = df['_time'].dt.tz_convert(None)

    # Features temporelles
    df['minute'] = df['_time'].dt.minute
    df['hour'] = df['_time'].dt.hour
    df['dayofweek'] = df['_time'].dt.dayofweek

    # On prépare X et y pour les deux cibles
    features = ['minute', 'hour', 'dayofweek']
    X = df[features]
    y_occ = df['occupancy']
    y_temp = df['temperature']

    # Modèles
    model_occ = RandomForestRegressor()
    model_temp = RandomForestRegressor()
    model_occ.fit(X, y_occ)
    model_temp.fit(X, y_temp)

    # Création des timestamps pour les 7 prochains jours
    future_times = pd.date_range(start=df['_time'].max() + pd.Timedelta(freq), 
                                 periods=int((pd.Timedelta(days=forecast_days) / pd.Timedelta(freq))), 
                                 freq=freq)

    # Création des features temporelles futures
    future_df = pd.DataFrame({
        '_time': future_times,
        'minute': future_times.minute,
        'hour': future_times.hour,
        'dayofweek': future_times.dayofweek
    })

    # Prédiction
    future_df['occupancy'] = model_occ.predict(future_df[features])
    future_df['occupancy'] = (future_df['occupancy'] >= 0.5).astype(int)

    future_df['temperature'] = model_temp.predict(future_df[features])

    return future_df[['_time', 'occupancy', 'temperature']]

