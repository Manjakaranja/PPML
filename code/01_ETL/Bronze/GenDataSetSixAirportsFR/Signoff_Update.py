
import pandas as pd
from datetime import datetime
import sys


# Fichiers d'entrée
flights_path = "FlightsAndMeteoAndJFVacancesAndGreves.csv"
cong_path = "OutputFlights/congestion_WW_updated.csv"

# Ajout de la date dans le nom du fichier de sortie
date_gen = datetime.now().strftime("%Y%m%d_%H%M%S")
output_path = f"SignofFlightsDataset_{date_gen}.csv"

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
source_cols = ['nombre_departs', 'nombre_arrivees', 'somme_depart_arrivee', 'congestion']
dest_cols = ['nombre_departs', 'nombre_arrivees', 'somme_depart_arrivee', 'congestion']

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


# Conversion robuste en int pour congestion_source et congestion_destination
for col in ["congestion_source", "congestion_destination"]:
    if col in flights.columns:
        flights[col] = pd.to_numeric(flights[col], errors='coerce')
        # Si vous voulez remplacer les NaN par 0, décommentez la ligne suivante :
        flights[col] = flights[col].fillna(0)
        flights[col] = flights[col].apply(lambda v: int(v) if pd.notna(v) else v)

# Ajout de la date de génération dans le contenu du CSV (colonne spéciale en première ligne)
date_gen_human = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
date_row = pd.DataFrame([{col: "" for col in flights.columns}])
date_row.iloc[0, 0] = "DATE_GENERATION"
date_row.iloc[0, 1] = date_gen_human

# Forcer la dernière colonne à 0 ou 1 (logique)
last_col = flights.columns[-1]
flights[last_col] = pd.to_numeric(flights[last_col], errors='coerce').apply(lambda v: 1 if pd.notna(v) and v != 0 else 0).astype(int)

flights_with_date = pd.concat([date_row, flights], ignore_index=True)


flights_with_date.to_csv(output_path, index=False, encoding="utf-8-sig")
print(f"Fichier généré : {output_path} (généré le {date_gen_human})")

# Affichage du pourcentage de 1 dans la dernière colonne (hors ligne d'en-tête spéciale)
last_col = flights.columns[-1]
nb_ones = flights[last_col].sum()
nb_total = len(flights)
percent_ones = (nb_ones / nb_total) * 100 if nb_total > 0 else 0

print(f"% de 1 dans la colonne '{last_col}' : {percent_ones:.2f}% ({nb_ones}/{nb_total})")

# Affichage du pourcentage de 1 dans la colonne 'congestion_destination'
col = 'congestion_destination'
if col in flights.columns:
    col_vals = pd.to_numeric(flights[col], errors='coerce')
    nb_ones = (col_vals == 1).sum()
    nb_total = col_vals.notna().sum()
    percent_ones = (nb_ones / nb_total) * 100 if nb_total > 0 else 0
    print(f"% de 1 dans la colonne '{col}' : {percent_ones:.2f}% ({nb_ones}/{nb_total})")


# Affichage du pourcentage de 1 dans la colonne 'congestion_destination'
col = 'congestion_source'
if col in flights.columns:
    col_vals = pd.to_numeric(flights[col], errors='coerce')
    nb_ones = (col_vals == 1).sum()
    nb_total = col_vals.notna().sum()
    percent_ones = (nb_ones / nb_total) * 100 if nb_total > 0 else 0
    print(f"% de 1 dans la colonne '{col}' : {percent_ones:.2f}% ({nb_ones}/{nb_total})")

# Affichage du pourcentage de cas où congestion_destination et congestion_source sont toutes les deux à 1
col_dest = 'congestion_destination'
col_src = 'congestion_source'
if col_dest in flights.columns and col_src in flights.columns:
    vals_dest = pd.to_numeric(flights[col_dest], errors='coerce')
    vals_src = pd.to_numeric(flights[col_src], errors='coerce')
    both_ones = ((vals_dest == 1) & (vals_src == 1)).sum()
    nb_total = flights.shape[0]
    percent_both_ones = (both_ones / nb_total) * 100 if nb_total > 0 else 0
    print(f"% de cas où '{col_dest}' et '{col_src}' = 1 : {percent_both_ones:.2f}% ({both_ones}/{nb_total})")
    


 # Affichage du pourcentage de cas où congestion_source est a 1 quand retard_arrivée est à 1    
col_src = 'congestion_source'
col_retard = 'retard arrivée'
if col_src in flights.columns and col_retard in flights.columns:
    vals_src = pd.to_numeric(flights[col_src], errors='coerce')
    vals_retard = pd.to_numeric(flights[col_retard], errors='coerce')
    src_when_retard = ((vals_src == 1) & (vals_retard == 1)).sum()
    nb_total = (vals_retard == 1).sum()
    percent_src_when_retard = (src_when_retard / nb_total) * 100 if nb_total > 0 else 0
    print(f"% de cas où '{col_src}' = 1 quand '{col_retard}' = 1 : {percent_src_when_retard:.2f}% ({src_when_retard}/{nb_total})")

 # Affichage du pourcentage de cas où congestion_destination est a 1 quand retard_arrivée est à 1    
col_dest = 'congestion_destination'
col_retard = 'retard arrivée'
if col_dest in flights.columns and col_retard in flights.columns:
    vals_dest = pd.to_numeric(flights[col_dest], errors='coerce')
    vals_retard = pd.to_numeric(flights[col_retard], errors='coerce')
    dest_when_retard = ((vals_dest == 1) & (vals_retard == 1)).sum()
    nb_total = (vals_retard == 1).sum()
    percent_dest_when_retard = (dest_when_retard / nb_total) * 100 if nb_total > 0 else 0
    print(f"% de cas où '{col_dest}' = 1 quand '{col_retard}' = 1 : {percent_dest_when_retard:.2f}% ({dest_when_retard}/{nb_total})")


# Affichage du pourcentage de cas où la colonne 'status' est égale à 'Unknown'
col_status = 'status'
if col_status in flights.columns:
    nb_unknown = (flights[col_status].astype(str).str.strip().str.lower() == 'unknown').sum()
    nb_total = flights.shape[0]
    percent_unknown = (nb_unknown / nb_total) * 100 if nb_total > 0 else 0
    print(f"% de cas où '{col_status}' = 'Unknown' : {percent_unknown:.2f}% ({nb_unknown}/{nb_total})")

    # Suppression des lignes où status == 'Unknown'
    flights_clean = flights[flights[col_status].astype(str).str.strip().str.lower() != 'unknown'].copy()
    print(f"Lignes restantes après suppression des 'Unknown' : {len(flights_clean)}/{nb_total}")
    # Sauvegarde du CSV nettoyé
    output_clean = output_path.replace('.csv', '_CLEAN.csv')
    flights_clean_with_date = pd.concat([date_row, flights_clean], ignore_index=True)
    flights_clean_with_date.to_csv(output_clean, index=False, encoding="utf-8-sig")
    print(f"Fichier nettoyé généré : {output_clean}")
