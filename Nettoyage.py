import pandas as pd 
import pyarrow as py
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import numpy as np

## ----------------------------------------------------------------------------------------------------
def trier_parquetw():
    important_colonnes = ['apply_time_rl','gfs_2m_dewpoint','gfs_total_clouds_cover_low', 'gfs_wind_speed','gfs_u_wind','gfs_v_wind','fact_latitude','fact_longitude']
    weather = pd.read_parquet('Ressources/weather.parquet',columns=important_colonnes)
    weather.to_parquet('Ressources_filtrer/weather_filtered.parquet')
    weather_filtered = pd.read_parquet('Ressources_filtrer/weather_filtered.parquet')
    return weather_filtered

#print(trier_parquetw())

def transformer_parquetw():
    df = pd.read_parquet('Ressources_filtrer/weather_filtered.parquet')
    df.rename(columns={'fact_latitude': 'lat', 'fact_longitude': 'lng', 'apply_time_rl':'date'}, inplace=True)
    df.to_parquet('Ressources_filtrer/weather_filtered.parquet')
    df['date'] = pd.to_datetime(df['date'], unit='s')
    df['time'] = df['date'].dt.strftime('%H:%M:%S')
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    df.to_parquet('Ressources_filtrer/weather_filtered.parquet')

#print(transformer_parquetw())

def date_parquetw():
    races_df = pd.read_csv("Ressources_filtrer/races_filtered.csv")
    weather_df = pd.read_parquet("Ressources_filtrer/weather_filtered.parquet")
    races_dates = races_df['date'].unique()
    filtered_weather_df = weather_df[weather_df['date'].isin(races_dates)]
    filtered_weather_df.to_parquet("Ressources_filtrer/weather_filtered.parquet")

#print(date_parquetw())  

def filter_by_time():
    races_df = pd.read_csv("Ressources_filtrer/races_filtered.csv")
    weather_df = pd.read_parquet("Ressources_filtrer/weather_filtered.parquet")
    races_df['datetime'] = pd.to_datetime(races_df['date'].astype(str) + ' ' + races_df['time'], format='%Y-%m-%d %H:%M:%S')
    weather_df['datetime'] = pd.to_datetime(weather_df['date'].astype(str) + ' ' + weather_df['time'], format='%Y-%m-%d %H:%M:%S')
    filtered_weather_list = []
    for _, race in races_df.iterrows():
        race_datetime = race['datetime']
        start_time = race_datetime - timedelta(hours=2)
        end_time = race_datetime + timedelta(hours=2)
        weather_for_race = weather_df[(weather_df['datetime'] >= start_time) & (weather_df['datetime'] <= end_time)]
        filtered_weather_list.append(weather_for_race)
    final_filtered_weather_df = pd.concat(filtered_weather_list, ignore_index=True)
    final_filtered_weather_df.to_parquet("Ressources_filtrer/weather_filtered_filtered_by_time.parquet")

#filter_by_time()

def haversine(lat1, lng1, lat2, lng2):
    R = 6371.0  # Rayon de la Terre en kilomètres
    lat1, lng1, lat2, lng2 = map(np.radians, [lat1, lng1, lat2, lng2])
    dlat = lat2 - lat1
    dlng = lng2 - lng1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlng / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    return R * c 

def vectorized_haversine(lat1, lng1, lat2_series, lng2_series):
    R = 6371.0  
    lat1, lng1 = np.radians(lat1), np.radians(lng1)
    lat2_series, lng2_series = np.radians(lat2_series), np.radians(lng2_series)
    dlat = lat2_series - lat1
    dlng = lng2_series - lng1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2_series) * np.sin(dlng / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    return R * c  

def date_lieu_parquet():
    races_df = pd.read_csv("Ressources_filtrer/races_filtered.csv")
    circuits_df = pd.read_csv("Ressources_filtrer/circuits_filtered.csv")
    weather_df = pd.read_parquet("Ressources_filtrer/weather_filtered.parquet")
    filtered_weather_list = []
    for _, race in races_df.iterrows():
        race_date = race['date']
        circuit_id = race['circuitId']
        circuit = circuits_df[circuits_df['circuitId'] == circuit_id].iloc[0]
        lat_circuit = circuit['lat']
        lng_circuit = circuit['lng']
        weather_for_date = weather_df[weather_df['date'] == race_date]
        weather_for_date['distance'] = vectorized_haversine(lat_circuit, lng_circuit, weather_for_date['lat'], weather_for_date['lng'])
        filtered_weather_df = weather_for_date[weather_for_date['distance'] <= 20]
        filtered_weather_list.append(filtered_weather_df)
    final_filtered_weather_df = pd.concat(filtered_weather_list, ignore_index=True)
    final_filtered_weather_df.drop(columns=['distance'], inplace=True)
    final_filtered_weather_df.to_parquet("Ressources_filtrer/weather_filtered.parquet")

#date_lieu_parquet()
## ----------------------------------------------------------------------------------------------------

def trier_weather():
    important_colonnes = ['city_name','date','avg_wind_speed_kmh', 'peak_wind_gust_kmh','sunshine_total_min', 'avg_temp_c','min_temp_c','max_temp_c','precipitation_mm']
    weather = pd.read_parquet('Ressources/daily_weather.parquet',columns=important_colonnes)
    weather.to_parquet('Ressources_filtrer/weather_filtered.parquet')
    weather_filtered = pd.read_parquet('Ressources_filtrer/weather_filtered.parquet')
    return weather_filtered

#print(trier_weather())

def powerBI():
    print("Chargement des fichiers...")
    df_weather = pd.read_parquet('Ressources_filtrer/weather_filtered.parquet') 
    df_circuits = pd.read_csv('Ressources_filtrer/circuits_filtered.csv')
    df_races = pd.read_csv('Ressources_filtrer/races_filtered.csv')
    df_cities = pd.read_csv('Ressources/cities.csv')
    df_countries = pd.read_csv('Ressources/countries.csv')
    
    print("Fichiers chargés avec succès.")
    
    # Conversion des dates
    df_weather['date'] = pd.to_datetime(df_weather['date'])
    df_races['date'] = pd.to_datetime(df_races['date'])
    
    # Jointure des courses et des circuits
    df_races_circuits = pd.merge(df_races, df_circuits, how='left', on='circuitId')
    print("Jointure effectuée entre les courses et les circuits.")
    
    # Jointure des données météo avec les courses et circuits
    df_filtered_weather = pd.merge(df_weather, df_races_circuits[['date', 'location']],how='inner', left_on=['date', 'city_name'], right_on=['date', 'location'])
    
    print(f"Nombre d'enregistrements météo après filtrage: {len(df_filtered_weather)}")
    
    # Remplacement des valeurs '/N' par NaN
    df_filtered_weather.replace('/N', pd.NA, inplace=True)
    
    # Colonnes météo
    weather_columns = ['avg_wind_speed_kmh', 'peak_wind_gust_kmh', 'sunshine_total_min', 'avg_temp_c', 'min_temp_c', 'max_temp_c', 'precipitation_mm']
    
    # Identification des données manquantes
    missing_weather_data = df_filtered_weather[df_filtered_weather[weather_columns].isna().any(axis=1)]
    
    # Ajout des informations sur les pays via les villes
    missing_weather_with_countries = pd.merge(missing_weather_data, df_cities[['city_name', 'country']], on='city_name', how='left')
    
    # Ajout des informations sur les pays dans l'ensemble de données complet
    df_weather_with_countries = pd.merge(df_filtered_weather, df_cities[['city_name', 'country']], on='city_name', how='left')
    
    # Conversion des colonnes météo en numérique
    df_weather_with_countries[weather_columns] = df_weather_with_countries[weather_columns].apply(pd.to_numeric, errors='coerce')
    
    # Calcul des moyennes par pays
    country_weather_means = df_weather_with_countries.groupby('country')[weather_columns].mean().reset_index()
    
    # Remplissage des données manquantes avec les moyennes par pays
    missing_weather_with_averages = pd.merge(missing_weather_with_countries, country_weather_means, on='country', suffixes=('', '_country_avg'))
    
    for column in weather_columns:
        missing_weather_with_averages[column].fillna(missing_weather_with_averages[f"{column}_country_avg"], inplace=True)
    
    # Suppression des colonnes de moyennes temporaires
    missing_weather_with_averages = missing_weather_with_averages.drop(columns=[f"{col}_country_avg" for col in weather_columns])
    
    # Fusion des données corrigées avec l'ensemble original
    df_filled_weather = pd.concat([df_filtered_weather[~df_filtered_weather.index.isin(missing_weather_with_averages.index)],missing_weather_with_averages])
    
    # Sauvegarde dans un fichier CSV
    df_filled_weather.to_csv('Ressources_filtrer/weather_f1_filtered_by_city_and_date_filled.csv', index=False, na_rep='/N')
    
    print("Données sauvegardées dans 'weather_f1_filtered_by_city_and_date_filled.csv'.")
    print(df_filled_weather.head())

powerBI()

## ----------------------------------------------------------------------------------------------------

def trier_constructor_results():
    csv_colonnes = ['constructorResultsId','raceId','constructorId','points']
    constructor_r_csv = pd.read_csv('Ressources/constructor_results.csv', usecols=csv_colonnes)
    constructor_r_csv.to_csv('Ressources_filtrer/constructor_r_filtered.csv', index=False)
    constructor_r_filtered = pd.read_csv('Ressources_filtrer/constructor_r_filtered.csv')
    return constructor_r_filtered

print(trier_constructor_results())

def trier_circuits():
    csv_colonnes = ['circuitId','circuitRef','name','location','country','lat','lng','alt']
    circuits_csv = pd.read_csv('Ressources/circuits.csv', usecols=csv_colonnes)
    circuits_csv.to_csv('Ressources_filtrer/circuits_filtered.csv', index=False)
    circuits_filtered = pd.read_csv('Ressources_filtrer/circuits_filtered.csv')
    return circuits_filtered

#print(trier_circuits())

def trier_constructor_standings():
    csv_colonnes = ['constructorStandingsId','raceId','constructorId','points','position','wins']
    csv = pd.read_csv('Ressources/constructor_standings.csv', usecols=csv_colonnes)
    csv.to_csv('Ressources_filtrer/constructor_standings_filtered.csv', index=False)
    filtered = pd.read_csv('Ressources_filtrer/constructor_standings_filtered.csv')
    return filtered

#print(trier_constructor_standings())

def trier_constructors():
    csv_colonnes = ['constructorId','constructorRef','name','nationality']
    csv = pd.read_csv('Ressources/constructors.csv', usecols=csv_colonnes)
    csv.to_csv('Ressources_filtrer/constructors_filtered.csv', index=False)
    filtered = pd.read_csv('Ressources_filtrer/constructors_filtered.csv')
    return filtered

#print(trier_constructors())

def trier_driver_standings():
    csv_colonnes = ['driverStandingsId','raceId','driverId','points','position','wins']
    csv = pd.read_csv('Ressources/driver_standings.csv', usecols=csv_colonnes)
    csv.to_csv('Ressources_filtrer/driver_standings_filtered.csv', index=False)
    filtered = pd.read_csv('Ressources_filtrer/driver_standings_filtered.csv')
    return filtered

#print(trier_driver_standings())

def trier_drivers():
    csv_colonnes = ['driverId','driverRef','number','code','forename','surname','dob','nationality']
    csv = pd.read_csv('Ressources/drivers.csv', usecols=csv_colonnes)
    csv.to_csv('Ressources_filtrer/drivers_filtered.csv', index=False)
    filtered = pd.read_csv('Ressources_filtrer/drivers_filtered.csv')
    return filtered

#print(trier_drivers())

def trier_lap_times():
    csv = pd.read_csv('Ressources/lap_times.csv')
    csv.to_csv('Ressources_filtrer/lap_times_filtered.csv', index=False)
    filtered = pd.read_csv('Ressources_filtrer/lap_times_filtered.csv')
    return filtered

#print(trier_lap_times())

def trier_pit_stops():
    csv = pd.read_csv('Ressources/pit_stops.csv')
    csv.to_csv('Ressources_filtrer/pit_stops_filtered.csv', index=False)
    filtered = pd.read_csv('Ressources_filtrer/pit_stops_filtered.csv')
    return filtered

#print(trier_pit_stops())

def trier_qualifying():
    csv = pd.read_csv('Ressources/qualifying.csv')
    csv.to_csv('Ressources_filtrer/qualifying_filtered.csv', index=False)
    filtered = pd.read_csv('Ressources_filtrer/qualifying_filtered.csv')
    return filtered

#print(trier_qualifying())

def trier_races():
    csv_colonnes = ['raceId','year','round','circuitId','name','date','time','quali_date','quali_time','sprint_date','sprint_time']
    csv = pd.read_csv('Ressources/races.csv', usecols=csv_colonnes)
    csv.to_csv('Ressources_filtrer/races_filtered.csv', index=False)
    filtered = pd.read_csv('Ressources_filtrer/races_filtered.csv')
    return filtered

#print(trier_races())

def trier_results():
    csv_colonnes = ['resultId','raceId','driverId','constructorId','number','grid','position','positionOrder','points','laps','time','milliseconds','fastestLap','rank','fastestLapTime','fastestLapSpeed','statusId']
    csv = pd.read_csv('Ressources/results.csv', usecols=csv_colonnes)
    csv.to_csv('Ressources_filtrer/results_filtered.csv', index=False)
    filtered = pd.read_csv('Ressources_filtrer/results_filtered.csv')
    return filtered

#print(trier_results())

def trier_sprint_results():
    csv_colonnes = ['resultId','raceId','driverId','constructorId','number','grid','position','positionOrder','points','laps','time','milliseconds','fastestLap','fastestLapTime','statusId']
    csv = pd.read_csv('Ressources/sprint_results.csv', usecols=csv_colonnes)
    csv.to_csv('Ressources_filtrer/sprint_results_filtered.csv', index=False)
    filtered = pd.read_csv('Ressources_filtrer/sprint_results_filtered.csv')
    return filtered

#print(trier_sprint_results())

def trier_status():
    csv = pd.read_csv('Ressources/status.csv')
    csv.to_csv('Ressources_filtrer/status_filtered.csv', index=False)
    filtered = pd.read_csv('Ressources_filtrer/status_filtered.csv')
    return filtered

#print(trier_status())