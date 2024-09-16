import pandas as pd 
import pyarrow as py

important_colonnes = ['date', 'name']

df_parquet = pd.read_parquet('Ressources/weather.parquet',columns=important_colonnes)

df_parquet.to_parquet('Ressources/weather_filtered.parquet')

df_parquet_filtered = pd.read_parquet('Ressources/weather_filtered.parquet')

print(df_parquet_filtered)