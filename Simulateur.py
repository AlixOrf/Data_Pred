import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor

# Lecture des fichiers CSV
drivers_df = pd.read_csv('Ressources_filtrer/drivers_filtered.csv')
constructors_df = pd.read_csv('Ressources_filtrer/constructors_filtered.csv')
circuits_df = pd.read_csv('Ressources_filtrer/circuits_filtered.csv')
results_df = pd.read_csv('Ressources_filtrer/results_filtered.csv')

# Convertir la colonne 'dob' (date de naissance) en format datetime
drivers_df['dob'] = pd.to_datetime(drivers_df['dob'], errors='coerce')

# Filtrer les pilotes nés après 1er janvier 1960
drivers_filtered_df = drivers_df[drivers_df['dob'] >= '1960-01-01']

# Fonction pour simuler le temps au tour en fonction de la météo et des performances
def simulate_lap_time(driver_performance, car_performance, weather_conditions):
    base_time = 90  # Base time for a lap in seconds
    time = base_time / (driver_performance + car_performance)
    
    # Ajustement selon les conditions météo
    if weather_conditions['rain']:
        time *= 1.15  # Temps plus long en cas de pluie
    if weather_conditions['wind']:
        time *= 1.05  # Légère augmentation avec le vent
    if weather_conditions['sunshine'] < 50:  # S'il y a moins de soleil
        time *= 1.02

    return time

# Fonction pour calculer le "win rate" (ratio de victoires sur l'ensemble des courses)
def calculate_win_rate(driver_id):
    driver_results = results_df[results_df['driverId'] == driver_id]
    total_races = len(driver_results)
    if total_races == 0:
        return 0  # Si pas de données, win rate = 0
    wins = len(driver_results[driver_results['positionOrder'] == 1])
    return wins / total_races

# Obtenir l'entrée de l'utilisateur
input_circuit_name = input("Entrez le nom du circuit (ex: Monaco Grand Prix) : ")
weather_conditions = {
    'rain': input("Est-ce qu'il pleut ? (oui/non) : ").lower() == 'oui',
    'wind': input("Y a-t-il du vent ? (oui/non) : ").lower() == 'oui',
    'sunshine': int(input("Quelle est la quantité d'ensoleillement en pourcentage ? (0-100) : "))
}

# Trouver le circuit correspondant
selected_circuit = circuits_df[circuits_df['name'].str.contains(input_circuit_name, case=False)]
if selected_circuit.empty:
    raise ValueError(f"Circuit {input_circuit_name} introuvable.")
else:
    selected_circuit = selected_circuit.iloc[0]

# Saisir les 20 pilotes
selected_drivers = []
for i in range(20):
    driver_name = input(f"Entrez le nom complet du pilote {i + 1} (ex: Lewis Hamilton) : ")
    first_name, last_name = driver_name.split()
    driver_row = drivers_df[(drivers_df['forename'] == first_name) & (drivers_df['surname'] == last_name)]
    if driver_row.empty:
        print(f"Pilote {driver_name} non trouvé. Réessayez.")
        continue
    selected_drivers.append(driver_row.iloc[0])

# Convertir la liste de pilotes en DataFrame
selected_drivers_df = pd.DataFrame(selected_drivers)

# Associer chaque pilote à son constructeur à partir de results_filtered.csv
# On choisit la première ligne correspondant à chaque pilote (limite à une course par pilote)
selected_drivers_df = selected_drivers_df.merge(results_df[['driverId', 'constructorId']].drop_duplicates(subset='driverId'), on='driverId')

# Ajouter les informations sur les constructeurs à partir de constructors_filtered.csv
selected_drivers_df = selected_drivers_df.merge(constructors_df[['constructorId', 'name']], on='constructorId', how='left')

# Calculer le win rate et simuler les temps au tour pour chaque pilote
selected_drivers_df['win_rate'] = selected_drivers_df['driverId'].apply(calculate_win_rate)

# Simulation des temps au tour
selected_drivers_df['lap_time'] = selected_drivers_df.apply(
    lambda row: simulate_lap_time(row['win_rate'], np.random.uniform(0.8, 1.2), weather_conditions),
    axis=1
)

# Trier les pilotes en fonction de leur temps au tour
selected_drivers_df = selected_drivers_df.sort_values('lap_time')

# Réattribuer les positions de 1 à 20
selected_drivers_df['final_position'] = range(1, 21)

# Afficher le classement final des pilotes avec leur temps
print(selected_drivers_df[['forename', 'surname', 'name', 'final_position', 'lap_time']].rename(columns={
    'name': 'Constructor', 'final_position': 'Position', 'lap_time': 'Time (seconds)'}))