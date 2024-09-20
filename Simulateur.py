from flask import Flask, render_template, request, url_for
import pandas as pd
import numpy as np

app = Flask(__name__)

# Lecture des fichiers CSV
drivers_df = pd.read_csv('Ressources_filtrer/drivers_filtered.csv')
constructors_df = pd.read_csv('Ressources_filtrer/constructors_filtered.csv')
results_df = pd.read_csv('Ressources_filtrer/results_filtered.csv')
circuits_df = pd.read_csv('Ressources_filtrer/circuits_filtered.csv')  # Réintégration du fichier circuits.csv

# Fonction pour calculer le "win rate" (ratio de victoires sur l'ensemble des courses)
def calculate_win_rate(driver_id):
    driver_results = results_df[results_df['driverId'] == driver_id]
    total_races = len(driver_results)
    if total_races == 0:
        return 0  # Si pas de données, win rate = 0
    wins = len(driver_results[driver_results['positionOrder'] == 1])
    return wins / total_races

# Fonction pour simuler le temps au tour en fonction de la météo et des performances
def simulate_lap_time(driver_performance, car_performance, weather_conditions, circuit_difficulty):
    base_time = 90 * circuit_difficulty  # Base time for a lap in seconds adjusted by circuit difficulty
    time = base_time / (driver_performance + car_performance)
    
    # Ajustement selon les conditions météo
    if weather_conditions['rain']:
        time *= 1.96  # Temps plus long en cas de pluie
    if weather_conditions['wind']:
        time *= 1.02  # Légère augmentation avec le vent
    if weather_conditions['sunshine'] < 50:  # S'il y a moins de soleil
        time *= 1.00

    return time

# Page d'accueil avec le formulaire
@app.route('/')
def index():
    return render_template('index.html')

# Route pour traiter les données saisies
@app.route('/simulate', methods=['POST'])
def simulate():
    # Récupérer les données du formulaire
    circuit_name = request.form['circuit']
    rain = request.form['rain'] == 'oui'
    wind = request.form['wind'] == 'oui'
    sunshine = int(request.form['sunshine'])

    # Chercher le circuit dans circuits_filtered.csv
    selected_circuit = circuits_df[circuits_df['name'].str.contains(circuit_name, case=False)]
    if selected_circuit.empty:
        return "Circuit introuvable"
    else:
        selected_circuit = selected_circuit.iloc[0]
        circuit_difficulty = 1.0  # Par exemple, on peut ajouter une difficulté spécifique par circuit

    # Pilotes
    driver_names = []
    for i in range(1, 21):
        driver_name = request.form.get(f'driver{i}')
        driver_names.append(driver_name)

    # Créer le DataFrame pour les pilotes
    selected_drivers = []
    for driver_name in driver_names:
        first_name, last_name = driver_name.split()
        driver_row = drivers_df[(drivers_df['forename'] == first_name) & (drivers_df['surname'] == last_name)]
        if not driver_row.empty:
            selected_drivers.append(driver_row.iloc[0])

    selected_drivers_df = pd.DataFrame(selected_drivers)

    # Associer chaque pilote à son constructeur et calculer le win rate
    selected_drivers_df = selected_drivers_df.merge(results_df[['driverId', 'constructorId']].drop_duplicates(subset='driverId'), on='driverId')
    selected_drivers_df = selected_drivers_df.merge(constructors_df[['constructorId', 'name']], on='constructorId', how='left')
    selected_drivers_df['win_rate'] = selected_drivers_df['driverId'].apply(calculate_win_rate)

    # Simuler les temps au tour
    weather_conditions = {'rain': rain, 'wind': wind, 'sunshine': sunshine}
    selected_drivers_df['lap_time'] = selected_drivers_df.apply(
        lambda row: simulate_lap_time(row['win_rate'], np.random.uniform(0.8, 1.2), weather_conditions, circuit_difficulty),
        axis=1
    )

    # Trier les pilotes en fonction de leur temps au tour
    selected_drivers_df = selected_drivers_df.sort_values('lap_time')
    selected_drivers_df['position'] = range(1, 21)

    # Passer les résultats au template HTML pour affichage
    return render_template('results.html', drivers=selected_drivers_df[['forename', 'surname', 'name', 'position', 'lap_time']].to_dict(orient='records'))

if __name__ == '__main__':
    app.run(debug=True)