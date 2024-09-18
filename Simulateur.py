import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier

# Lecture des fichiers CSV et du fichier parquet
drivers_df = pd.read_csv('Ressources_filtrer/drivers_filtered.csv')
constructors_df = pd.read_csv('Ressources_filtrer/constructors_filtered.csv')
circuits_df = pd.read_csv('Ressources_filtrer/circuits_filtered.csv')
races_df = pd.read_csv('Ressources_filtrer/races_filtered.csv')
weather_df = pd.read_parquet('Ressources_filtrer/weather_filtered.parquet')

# Filtrer les pilotes en fonction de la date de naissance (dob)
drivers_df['dob'] = pd.to_datetime(drivers_df['dob'], errors='coerce')  # Convertir les dates en format datetime
drivers_filtered_df = drivers_df[drivers_df['dob'] >= '1960-01-01']     # Ne conserver que les pilotes nés après 1960

# Fonction pour récupérer les conditions météo à partir du parquet
def get_weather_conditions(circuitId, date):
    circuit_row = circuits_df[circuits_df['circuitId'] == circuitId]
    if circuit_row.empty:
        return {"wind_speed": 0, "cloud_cover": 0, "dewpoint": 0}
    
    latitude = circuit_row.iloc[0]['lat']
    longitude = circuit_row.iloc[0]['lng']
    
    # Rechercher les données météo en fonction des coordonnées et de la date
    weather_row = weather_df[(weather_df['lat'] == latitude) & 
                            (weather_df['lng'] == longitude) & 
                            (weather_df['date'] == date)]
    
    if not weather_row.empty:
        return {
            'wind_speed': weather_row.iloc[0]['gfs_wind_speed'],                # Vitesse du vent
            'cloud_cover': weather_row.iloc[0]['gfs_total_clouds_cover_low'],   # Couverture nuageuse
            'dewpoint': weather_row.iloc[0]['gfs_2m_dewpoint']                  # Point de rosée (lié à l'humidité/température)
        }
    else:
        return {"wind_speed": 0, "cloud_cover": 0, "dewpoint": 0}

# Obtenir l'entrée de l'utilisateur
input_year = int(input("Entrez l'année de la course (ex: 2023) : "))
input_circuit_name = input("Entrez le nom du circuit (ex: Monaco Grand Prix) : ")

# Trouver le circuit correspondant
selected_circuit = circuits_df[circuits_df['name'].str.contains(input_circuit_name, case=False)]
if selected_circuit.empty:
    raise ValueError(f"Circuit {input_circuit_name} introuvable.")
else:
    selected_circuit = selected_circuit.iloc[0]

# Saisir les 20 pilotes (utiliser leurs prénoms et noms)
selected_drivers = []
for i in range(20):
    driver_name = input(f"Entrez le nom complet du pilote {i + 1} (ex: Lewis Hamilton) : ")
    first_name, last_name = driver_name.split()
    driver_row = drivers_filtered_df[(drivers_filtered_df['forename'] == first_name) & (drivers_filtered_df['surname'] == last_name)]
    if driver_row.empty:
        print(f"Pilote {driver_name} non trouvé. Réessayez.")
        continue
    selected_drivers.append(driver_row.iloc[0])

# Convertir la liste de pilotes en DataFrame
selected_drivers_df = pd.DataFrame(selected_drivers)

# Date de la course à prédire (simulation ici en choisissant une date dans les données historiques)
selected_race = races_df[(races_df['year'] == input_year) & (races_df['circuitId'] == selected_circuit['circuitId'])]
if selected_race.empty:
    raise ValueError(f"Aucune course trouvée pour l'année {input_year} au circuit {input_circuit_name}.")
else:
    selected_race = selected_race.iloc[0]

selected_date = selected_race['date']

# Obtenir les conditions météo pour la course
weather_conditions = get_weather_conditions(selected_circuit['circuitId'], selected_date)

# Préparer les données d'entraînement à partir de races_filtered.csv
train_data = races_df[['circuitId', 'year']].copy()

# Ajouter les conditions météo historiques
train_data['wind_speed'] = train_data['circuitId'].apply(lambda cid: get_weather_conditions(cid, selected_date)['wind_speed'])
train_data['cloud_cover'] = train_data['circuitId'].apply(lambda cid: get_weather_conditions(cid, selected_date)['cloud_cover'])
train_data['dewpoint'] = train_data['circuitId'].apply(lambda cid: get_weather_conditions(cid, selected_date)['dewpoint'])

# Cible : positions des pilotes dans les courses précédentes (ici générées pour la démo)
y_train = np.random.randint(1, 21, len(train_data))  # Simuler des positions de 1 à 20 pour les pilotes

# Entraîner un modèle RandomForest
clf = RandomForestClassifier()
clf.fit(train_data, y_train)

# Préparer les données pour la nouvelle course (X_test) pour prédire
X_test = pd.DataFrame({
    'circuitId': [selected_circuit['circuitId']] * 20,
    'year': [input_year] * 20,
    'wind_speed': [weather_conditions['wind_speed']] * 20,
    'cloud_cover': [weather_conditions['cloud_cover']] * 20,
    'dewpoint': [weather_conditions['dewpoint']] * 20
})

predicted_positions = clf.predict(X_test)

# Associer les prédictions aux pilotes sélectionnés
selected_drivers_df['predicted_position'] = predicted_positions

# Trier les pilotes en fonction de leurs positions prédites
top_5_drivers = selected_drivers_df[['forename', 'surname', 'predicted_position']].sort_values('predicted_position').head(5)

# Réattribuer les positions correctes (1 à 5)
top_5_drivers['final_position'] = range(1, 6)

# Afficher le top 5 des pilotes avec leur position
print(top_5_drivers[['forename', 'surname', 'final_position']].rename(columns={'final_position': 'Position'}))