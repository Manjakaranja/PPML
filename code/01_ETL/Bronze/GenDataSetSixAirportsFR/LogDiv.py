import json
import pandas as pd
from datetime import datetime
import sys

# Lecture du fichier JSON
with open("flight_selection.json", "r", encoding="utf-8") as f:
    data = json.load(f)


# Extraction des champs et split de la date
selected_date = data.get("selected_date", "")
date_part = ""
time_part = ""
if selected_date:
    parts = selected_date.split()
    if len(parts) == 2:
        date_part, time_part = parts
    elif len(parts) == 1:
        date_part = parts[0]

fields = [
    ("flight_number", data.get("flight_number", "")),
    ("date", date_part),
    ("hour", time_part),
    ("departure_airport", data.get("departure_airport", "")),
    ("arrival_airport", data.get("arrival_airport", "")),
]

print("Champs extraits du JSON :")
for name, value in fields:
    print(f"{name}: {value}")           
 
  
            
# Extraction des infos du fichier CSV
import glob
csv_files = glob.glob("SignoffFlightsDataset_Single*_CLEAN.csv")

# Ordre demandé : numéro de vol, date, heure, aéroport de départ, aéroport d'arrivée
csv_valeurs = ["", "", "", "", ""]
if csv_files:
    df_csv = pd.read_csv(csv_files[0], encoding="utf-8-sig")
    if not df_csv.empty:
        row = df_csv.iloc[0]
        # flight_number
        csv_valeurs[0] = str(row.get("flight_number", "")).strip()
        # date
        csv_valeurs[1] = str(row.get("flight_date", "")).strip()
        # heure de départ (on prend scheduled_departure ou time_dep)
        heure = str(row.get("scheduled_departure", "")).strip()
        if not heure or heure.lower() == 'nan':
            heure = str(row.get("time_dep", "")).strip()
        # Extraire uniquement l'heure si format YYYY-MM-DD HH:MM... sinon garder tel quel
        if " " in heure:
            heure = heure.split(" ")[1].split(":")[0] + ":" + heure.split(" ")[1].split(":")[1]
        elif "T" in heure:
            heure = heure.split("T")[1][:5]
        csv_valeurs[2] = heure
        # aéroport de départ
        csv_valeurs[3] = str(row.get("airport_origin", "")).strip()
        # aéroport d'arrivée
        csv_valeurs[4] = str(row.get("airport_destination", "")).strip()


# Création de la colonne MATCH
def compare_value(idx, json_val, csv_val):
    # 0: flight_number, 1: date, 2: heure, 3: dep, 4: arr
    if not json_val or not csv_val:
        return "DIFF"
    if idx == 2:  # heure
        # Format attendu : HH:MM
        try:
            from datetime import datetime, timedelta
            t1 = datetime.strptime(json_val[:5], "%H:%M")
            t2 = datetime.strptime(csv_val[:5], "%H:%M")
            diff = abs((t1 - t2).total_seconds())
            return "OK" if diff < 60 else "DIFF"
        except Exception:
            return "DIFF"
    else:
        return "OK" if str(json_val).strip() == str(csv_val).strip() else "DIFF"

match_col = [compare_value(i, fields[i][1], csv_valeurs[i]) for i in range(5)]


# Création de la colonne LOG selon la règle demandée
log_col = ["" for _ in range(5)]
if match_col[0] == "DIFF" and all(m == "OK" for m in match_col[1:]):
    log_col[0] = "CODE_SHARING"
    for i in range(1, 5):
        log_col[i] = "OK"
elif any(m == "DIFF" for m in match_col):
    log_col = ["ERR"] * 5

# Création du DataFrame (5 colonnes, 5 lignes)
df = pd.DataFrame({
    "JSON Param": [f[0] for f in fields],
    "JSON Val": [f[1] for f in fields],
    "API Val": csv_valeurs,
    "MATCH": match_col,
    "LOG": log_col
})

# Suppression du fichier de sortie s'il existe
import os
log_path = "flight_selection_log.csv"
if os.path.exists(log_path):
    os.remove(log_path)

# Sauvegarde dans un fichier log
df.to_csv(log_path, index=False, encoding="utf-8-sig")
print(f"Log généré : {log_path}")

# Si le fichier de sortie SignoffFlightsDataset_Single*_CLEAN.csv contient plus d'une ligne , ne garder que celle qui a la meme date et une heure de depart 
# à plus ou moins 1h que celle du JSON  et ecraser le fichier   
# Si aucune ligne ne correspond, effcaer toutes les lignes du fichier de sortie (on garde le fichier mais il est vide)


 
 
 
 