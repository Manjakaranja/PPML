import pandas as pd
import os

# Chemins des fichiers d'entrée
flights_meteo_path = "FlightsAndMeteo.csv"
congestion_ww_path = "OutputFlights/congestion_WW.csv"
output_path = "FlightsAndMeteoWWCount.csv"

# Chargement des données
flights = pd.read_csv(flights_meteo_path)
cong = pd.read_csv(congestion_ww_path, skipinitialspace=True)
cong.columns = [c.strip() for c in cong.columns]
cong['airport'] = cong['airport'].str.strip()
cong['flight_date'] = cong['flight_date'].astype(str).str.strip()






# Création de la colonne NBVols_Depart_Airport_Origin (par défaut NaN)
flights['NBVols_Depart_Airport_Origin'] = None
# Création de la colonne NBVols_Arrivee_Airport_Origin (par défaut NaN)
flights['NBVols_Arrivee_Airport_Origin'] = None
# Création de la colonne NBVols_Depart_Airport_Destination (par défaut NaN)
flights['NBVols_Depart_Airport_Destination'] = None
# Création de la colonne NBVols_Arrivee_Airport_Destination (par défaut NaN)
flights['NBVols_Arrivee_Airport_Destination'] = None




# Remplir NBVols_Depart_Airport_Origin pour toutes les lignes
for idx, row in flights.iterrows():
    date = str(row['flight_date']).strip()
    airport = str(row['airport_origin']).strip()
    match = cong[(cong['flight_date'] == date) & (cong['airport'] == airport)]
    if not match.empty:
        flights.at[idx, 'NBVols_Depart_Airport_Origin'] = match.iloc[0]['nombre_departs']            

# Sauvegarde du résultat
flights.to_csv(output_path, index=False, encoding="utf-8-sig")
print(f"Fichier {output_path} généré avec la colonne NBVols_Depart_Airport_Origin pour tous les aéroports.")



# Remplir NBVols_Arrivee_Airport_Origin pour toutes les lignes
for idx, row in flights.iterrows():
    date = str(row['flight_date']).strip()
    airport = str(row['airport_origin']).strip()
    match = cong[(cong['flight_date'] == date) & (cong['airport'] == airport)]
    if not match.empty:
        flights.at[idx, 'NBVols_Arrivee_Airport_Origin'] = match.iloc[0]['nombre_arrivees']

# Sauvegarde du résultat
flights.to_csv(output_path, index=False, encoding="utf-8-sig")
print(f"Fichier {output_path} généré avec la colonne NBVols_Arrivee_Airport_Origin pour tous les aéroports.")





# Remplir NBVols_Depart_Airport_Destination pour toutes les lignes
for idx, row in flights.iterrows():
    date = str(row['flight_date']).strip()
    airport = str(row['airport_destination']).strip()
    match = cong[(cong['flight_date'] == date) & (cong['airport'] == airport)]
    if not match.empty:
        flights.at[idx, 'NBVols_Depart_Airport_Destination'] = match.iloc[0]['nombre_departs']            

# Sauvegarde du résultat
flights.to_csv(output_path, index=False, encoding="utf-8-sig")
print(f"Fichier {output_path} généré avec la colonne NBVols_Depart_Airport_Destination pour tous les aéroports.")



# Remplir NBVols_Arrivee_Airport_Destination pour toutes les lignes
for idx, row in flights.iterrows():
    date = str(row['flight_date']).strip()
    airport = str(row['airport_destination']).strip()
    match = cong[(cong['flight_date'] == date) & (cong['airport'] == airport)]
    if not match.empty:
        flights.at[idx, 'NBVols_Arrivee_Airport_Destination'] = match.iloc[0]['nombre_arrivees']

# Sauvegarde du résultat
flights.to_csv(output_path, index=False, encoding="utf-8-sig")
print(f"Fichier {output_path} généré avec la colonne NBVols_Arrivee_Airport_Destination pour tous les aéroports.")




