import pandas as pd 
import pyarrow as py
import pyarrow.parquet as pq
from datetime import datetime

def trier_parquet():
    important_colonnes = ['apply_time_rl','gfs_2m_dewpoint','gfs_total_clouds_cover_low', 'gfs_wind_speed','gfs_u_wind','gfs_v_wind','fact_latitude','fact_longitude']
    weather = pd.read_parquet('Ressources/weather.parquet',columns=important_colonnes)
    weather.to_parquet('Ressources_filtrer/weather_filtered.parquet')
    weather_filtered = pd.read_parquet('Ressources_filtrer/weather_filtered.parquet')
    return weather_filtered

#print(trier_parquet())

def transformer_parquet():
    df = pd.read_parquet('Ressources_filtrer/weather_filtered.parquet')
    df.rename(columns={'fact_latitude': 'lat', 'fact_longitude': 'lng', 'apply_time_rl':'date'}, inplace=True)
    df.to_parquet('Ressources_filtrer/weather_filtered.parquet')
    df['date'] = pd.to_datetime(df['date'], unit='s')
    df['time'] = df['date'].dt.strftime('%H:%M:%S')
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    df.to_parquet('Ressources_filtrer/weather_filtered.parquet')

#print(transformer_parquet())

def trier_constructor_results():
    csv_colonnes = ['constructorResultsId','raceId','constructorId','points']
    constructor_r_csv = pd.read_csv('Ressources/constructor_results.csv', usecols=csv_colonnes)
    constructor_r_csv.to_csv('Ressources_filtrer/constructor_r_filtered.csv', index=False)
    constructor_r_filtered = pd.read_csv('Ressources_filtrer/constructor_r_filtered.csv')
    return constructor_r_filtered

#print(trier_constructor_results())

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