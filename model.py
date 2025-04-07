import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.model_selection import train_test_split
# TO BE MODIFY CHATGPT EXAMPLE 

# 1. Load data
df = pd.read_csv("data.csv", index_col=0, parse_dates=True)
df["mean_temperature"] = df[["temperature_3", "temperature_4"]].mean(axis=1)
df_model = df[["occupancy", "mean_temperature"]].resample("15T").mean()

# 2. Feature engineering
df_model["hour"] = df_model.index.hour
df_model["minute"] = df_model.index.minute
df_model["dayofweek"] = df_model.index.dayofweek

# 3. Drop NA rows (may exist after resampling)
df_model.dropna(inplace=True)

# 4. Prepare features and target
X = df_model.drop(columns=["occupancy"])
y = df_model["occupancy"]

# 5. Train/test split
X_train, X_test, y_train, y_test = train_test_split(X, y, shuffle=False, test_size=0.1)

# 6. Train LightGBM model
model = lgb.LGBMRegressor()
model.fit(X_train, y_train)

# 7. Forecasting the next 7 days (672 points)
future_index = pd.date_range(df_model.index[-1] + pd.Timedelta(minutes=15), periods=672, freq="15T")
future_df = pd.DataFrame(index=future_index)
future_df["hour"] = future_df.index.hour
future_df["minute"] = future_df.index.minute
future_df["dayofweek"] = future_df.index.dayofweek

# Optionally, you can assume temperature stays constant or use your own temperature forecast
future_df["mean_temperature"] = df_model["mean_temperature"].iloc[-96:].mean()  # mean of last 24h

# 8. Predict
future_df["predicted_occupancy"] = model.predict(future_df)

# 9. Resulting forecast DataFrame
result_df = future_df[["predicted_occupancy"]].copy()
result_df.reset_index(inplace=True)
result_df.rename(columns={"index": "timestamp"}, inplace=True)

# 10. Preview or send to InfluxDB
print(result_df.head())
