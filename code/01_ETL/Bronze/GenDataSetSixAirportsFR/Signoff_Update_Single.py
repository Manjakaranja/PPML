from typing_extensions import final

import pandas as pd
from datetime import datetime
import sys


# Fichiers d'entrée
flights_path = "FlightsAndMeteoAndJFVacancesAndGreves_Single.csv"
cong_path = "OutputSingleFlight/vols_journaliers_single.csv"

 
# Ajout de la date dans le nom du fichier de sortie
date_gen = datetime.now().strftime("%Y%m%d_%H%M%S")
output_path = f"SignoffFlightsDataset_Single_{date_gen}.csv"


# Lecture du seuil de retard en minutes (par défaut 15)
# Entrer Seuil  Delai
delay_threshold = 15
if len(sys.argv) > 1:
    try:
        delay_threshold = int(sys.argv[1])
    except Exception:
        print("Seuil de retard non valide, utilisation de 15 min par défaut.")
        delay_threshold = 15
else:
    val = input("Entrer Seuil  Delai (minutes, entier, défaut=15) : ").strip()
    if val:
        try:
            delay_threshold = int(val)
        except Exception:
            print("Entrée non valide, utilisation de 15 min par défaut.")



# Chargement des données
flights = pd.read_csv(flights_path, encoding="utf-8-sig")
cong = pd.read_csv(cong_path, encoding="utf-8-sig")

# Nettoyage des noms de colonnes (strip)
flights.columns = [c.strip() for c in flights.columns]
cong.columns = [c.strip() for c in cong.columns]


# Normalisation des dates et codes aéroports (strip + upper)
flights['flight_date'] = flights['flight_date'].astype(str).str.strip()
flights['airport_origin'] = flights['airport_origin'].astype(str).str.strip().str.upper()
flights['airport_destination'] = flights['airport_destination'].astype(str).str.strip().str.upper()
cong['flight_date'] = cong['flight_date'].astype(str).str.strip()
cong['airport'] = cong['airport'].astype(str).str.strip().str.upper()

# Préparation des colonnes à ajouter
source_cols = ['nombre_departures', 'nombre_arrivals', 'somme_nombre_departs_arrivees','congestion']
dest_cols = ['nombre_departures', 'nombre_arrivals', 'somme_nombre_departs_arrivees','congestion']

# Pour chaque ligne, on va chercher les valeurs pour l'aéroport d'origine et de destination
source_data = []
dest_data = []

for idx, row in flights.iterrows():
    # Source
    src = cong[(cong['flight_date'] == row['flight_date']) & (cong['airport'] == row['airport_origin'])]
    if not src.empty:
        src_vals = src.iloc[0][source_cols].tolist()
    else:
        print(f"Aucune correspondance source pour {row['flight_date']} / {row['airport_origin']} (ligne {idx+2})")
        src_vals = [None]*len(source_cols)
    source_data.append(src_vals)
    # Destination
    dst = cong[(cong['flight_date'] == row['flight_date']) & (cong['airport'] == row['airport_destination'])]
    if not dst.empty:
        dst_vals = dst.iloc[0][dest_cols].tolist()
    else:
        print(f"Aucune correspondance destination pour {row['flight_date']} / {row['airport_destination']} (ligne {idx+2})")
        dst_vals = [None]*len(dest_cols)
    dest_data.append(dst_vals)


# Ajout des colonnes au DataFrame
for i, col in enumerate(source_cols):
    vals = [vals[i] for vals in source_data]
    # Conversion stricte en int pour congestion_source
    if col == "congestion":
        def to_int(v):
            try:
                return int(float(v)) if v is not None and v != '' else v
            except Exception:
                return v
        vals = [to_int(v) for v in vals]
    flights[f"{col}_source"] = vals

for i, col in enumerate(dest_cols):
    vals = [vals[i] for vals in dest_data]
    # Conversion stricte en int pour congestion_destination
    if col == "congestion":
        def to_int(v):
            try:
                return int(float(v)) if v is not None and v != '' else v
            except Exception:
                return v
        vals = [to_int(v) for v in vals]
    flights[f"{col}_destination"] = vals


# Remplissage ligne à ligne de arrival_delay_min si vide ou Missing Value avec departure_delay_min
arr_delay_col = 'arrival_delay_min'
dep_delay_col = 'departure_delay_min'
if arr_delay_col in flights.columns and dep_delay_col in flights.columns:
    def fill_arrival_with_departure(row):
        v = row[arr_delay_col]
        if pd.isna(v) or v == '' or str(v).lower() in ('nan', 'none', 'missing value'):
            return row[dep_delay_col]
        return v
    flights[arr_delay_col] = flights.apply(fill_arrival_with_departure, axis=1)
    
    


# Création stricte de la colonne 'retard arrivée' (1 si arrival_delay_min > seuil, sinon 0, valeurs forcées à 1 ou 0)
if arr_delay_col in flights.columns:
    arr_delay_num = pd.to_numeric(flights[arr_delay_col], errors='coerce')

    flights['retard arrivée'] = arr_delay_num.apply(lambda v: 1 if pd.notna(v) and float(v) > delay_threshold else 0).astype(int)
else:
    flights['retard arrivée'] = 0



# Génération d'un seul fichier de sortie : flights_clean.csv
col_status = 'status'
if col_status in flights.columns:
    nb_unknown = (flights[col_status].astype(str).str.strip().str.lower() == 'unknown').sum()
    nb_total = flights.shape[0]
    percent_unknown = (nb_unknown / nb_total) * 100 if nb_total > 0 else 0
    print(f"% de cas où '{col_status}' = 'Unknown' : {percent_unknown:.2f}% ({nb_unknown}/{nb_total})")

    # Suppression des lignes où status == 'Unknown'
    flights_clean = flights[flights[col_status].astype(str).str.strip().str.lower() != 'unknown'].copy()
    print(f"Lignes restantes après suppression des 'Unknown' : {len(flights_clean)}/{nb_total}")
else:
    flights_clean = flights.copy()


# Ajout de la date en première ligne
# Format humain pour la date de génération
##-- try:
##--     date_gen_human = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
##-- except Exception:
##--     date_gen_human = date_gen
##-- date_row_clean = pd.DataFrame([{col: "" for col in flights_clean.columns}])
##-- date_row_clean.iloc[0, 0] = "DATE_GENERATION"
##-- date_row_clean.iloc[0, 1] = date_gen_human
##-- flights_clean_with_date = pd.concat([date_row_clean, flights_clean], ignore_index=True)


flights_clean_with_date = flights.copy()


# Sauvegarde unique
output_clean = output_path.replace('.csv', '_CLEAN.csv')
flights_clean_with_date.to_csv(output_clean, index=False, encoding="utf-8-sig")
print(f"Fichier nettoyé généré : {output_clean}")

# Génération d'un fichier ColList.csv avec la liste des colonnes du fichier final
col_list_path = output_path.replace('.csv', '_ColList.csv') 
col_list = pd.DataFrame({'columns': flights_clean.columns})
col_list.to_csv(col_list_path, index=False, encoding="utf-8-sig")
print(f"Fichier de liste des colonnes généré : {col_list_path}")

