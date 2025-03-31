from influxdb_client import InfluxDBClient, WriteOptions
import pandas as pd

# Configuration de la connexion
url = "http://tek-iot-c.sandbox.tek.sdu.dk:8086/"  # Remplace par l'URL de ton serveur InfluxDB
token = "QtrjwTZV8Yl1w"  # Remplace par ton token d'authentification
org = "cei"  # Remplace par ton organisation
bucket = "weather"  # Remplace par ton bucket

# Connexion à InfluxDB
client = InfluxDBClient(url=url, token=token, org=org)

# Vérification de la connexion
try:
    health = client.health()
    print(f"Statut de la connexion : {health.status}")
except Exception as e:
    print(f"Erreur de connexion : {e}")

# Récupération des données avec la requête Flux
time_range_start = "-30d"  
time_range_stop = "now()" 
window_period = "5m"  

# Requête Flux corrigée
query = f'''
from(bucket: "{bucket}")
  |> range(start: {time_range_start}, stop: {time_range_stop})
  |> filter(fn: (r) => r["_measurement"] == "humidity")
  |> filter(fn: (r) => r["_field"] == "humidity")
  |> filter(fn: (r) => r["country"] == "DK")
  |> aggregateWindow(every: {window_period}, fn: mean, createEmpty: false)
  |> yield(name: "mean")
'''

# Récupérer les données dans un DataFrame
df = client.query_api().query_data_frame(query)
print(df)

# Préparer les données  # Supprimer les colonnes inutiles
df.set_index('_time', inplace=True)  # Utiliser la colonne _time comme index

df_clean=df.drop(columns=['_start', '_stop', 'result','table'])# Réinitialiser l'index pour avoir _time comme colonne

# Connexion pour l'écriture
write_api = client.write_api(write_options=WriteOptions(batch_size=1))  # Write mode

# Écrire les données dans InfluxDB
write_api.write(
    bucket=bucket,
    org=org,
    record=df_clean,
    data_frame_measurement_name="humidity",  # Utiliser "humidity" comme nom de la mesure
    data_frame_tag_columns=["country", "neighborhood", "station_id",'unit'],  # Colonnes utilisées comme tags
     # Utiliser la colonne "_time" pour les timestamps
)

print("✅ Données écrites dans InfluxDB")

# Fermer la connexion
print("Fermeture de la connexion...")
client.close()
