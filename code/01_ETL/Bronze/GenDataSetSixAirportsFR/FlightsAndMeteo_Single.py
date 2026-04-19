import pandas as pd
from datetime import datetime
import glob
import os

# Chargement des fichiers

# Recherche automatique du fichier SingleFlightData_*.csv le plus récent
single_flight_files = glob.glob(os.path.join("OutputSingleFlight", "SingleFlightData_*.csv"))
if not single_flight_files:
    raise FileNotFoundError("Aucun fichier SingleFlightData_*.csv trouvé dans OutputSingleFlight")
flights_path = max(single_flight_files, key=os.path.getmtime)

meteo_path = "OutputDataMeteo/meteo_aeroports_Single.csv"
output_path = "FlightsAndMeteo_Single.csv"

# Lecture des données
flights = pd.read_csv(flights_path, sep=",", dtype=str)
meteo = pd.read_csv(meteo_path, sep=";", dtype=str)



# Diagnostic sur le format source
print("Aperçu des valeurs de scheduled_departure avant conversion :")
print(flights["scheduled_departure"].head(10))

# Conversion des dates avec gestion UTC
for col in ["scheduled_departure", "scheduled_arrival"]:
    flights[col + "_dt"] = pd.to_datetime(flights[col], errors="coerce", utc=True)

meteo["time_dt"] = pd.to_datetime(meteo["time"], errors="coerce")

# Harmonisation des codes aéroports pour la jointure
# On convertit les codes 'NICE' en 'NCE' dans flights si besoin
flights['airport_origin'] = flights['airport_origin'].replace({'NICE': 'NCE'})
flights['airport_destination'] = flights['airport_destination'].replace({'NICE': 'NCE'})
flights["scheduled_departure_dt"] = pd.to_datetime(flights["scheduled_departure"], errors="coerce")
flights["scheduled_arrival_dt"] = pd.to_datetime(flights["scheduled_arrival"], errors="coerce")

# Harmonisation des codes aéroports dans la météo aussi
meteo['icao'] = meteo['icao'].replace({'NICE': 'NCE'})

# Fusion météo départ
flights["merge_hour_dep"] = flights["scheduled_departure_dt"].dt.floor("H")
meteo["merge_hour"] = meteo["time_dt"].dt.floor("H")
meteo_dep = meteo.copy()
meteo_dep = meteo_dep.drop_duplicates(subset=["icao", "merge_hour"])
meteo_dep = meteo_dep.add_suffix("_dep")
merged = pd.merge(
    flights,
    meteo_dep,
    left_on=["airport_origin", "merge_hour_dep"],
    right_on=["icao_dep", "merge_hour_dep"],
    how="left"
)

# Fusion météo arrivée
merged["merge_hour_arr"] = merged["scheduled_arrival_dt"].dt.floor("H")
meteo_arr = meteo.copy()
meteo_arr = meteo_arr.drop_duplicates(subset=["icao", "merge_hour"])
meteo_arr = meteo_arr.add_suffix("_arr")
merged = pd.merge(
    merged,
    meteo_arr,
    left_on=["airport_destination", "merge_hour_arr"],
    right_on=["icao_arr", "merge_hour_arr"],
    how="left"
)

# Nettoyage colonnes techniques
merged = merged.drop(columns=[
    "scheduled_departure_dt", "scheduled_arrival_dt", "merge_hour_dep", "merge_hour_arr", "icao_dep", "merge_hour_dep", "time_dt_dep", "icao_arr", "merge_hour_arr", "time_dt_arr"
], errors="ignore")

# Sauvegarde
merged.to_csv(output_path, index=False, encoding="utf-8-sig", sep=",")
print(f"Fichier généré : {output_path} (séparateur virgule)")
