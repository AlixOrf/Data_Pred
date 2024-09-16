import pandas as pd 
import pyarrow as py
import pyarrow.parquet as pq

def trier_weather():
    important_colonnes = ['apply_time_rl', 'fact_time','gfs_2m_dewpoint','gfs_total_clouds_cover_low', 'gfs_wind_speed','gfs_u_wind','gfs_v_wind']
    weather = pd.read_parquet('Ressources/weather.parquet',columns=important_colonnes)
    weather.to_parquet('Ressources/weather_filtered.parquet')
    weather_filtered = pd.read_parquet('Ressources_filtrer/weather_filtered.parquet')
    return weather_filtered

print(trier_weather())