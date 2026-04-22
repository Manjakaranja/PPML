


import json
import pandas as pd
from datetime import datetime
import sys
import os

# --- Extraction de l'heure du JSON AVANT le traitement CSV ---
json_heure = ""
try:
	with open("flight_selection.json", "r", encoding="utf-8") as f:
		data = json.load(f)
		selected_date = data.get("selected_date", "")
		if selected_date:
			parts = selected_date.split()
			if len(parts) == 2:
				json_heure = parts[1]
			elif len(parts) == 1:
				json_heure = ""
	print(f"[INFO] Heure_JSON : {json_heure}")
except Exception as e:
	print(f"[DEBUG] Erreur lecture JSON : {e}")

# --- Fonction de parsing d'heure ---
def parse_hhmm(h):
	for fmt in ("%H:%M:%S", "%H:%M"):
		try:
			return datetime.strptime(h.strip(), fmt)
		except Exception:
			continue
	return None

# --- Affichage de tous les horaires de la colonne scheduled_departure du fichier demandé ---




import glob

# Recherche du dernier fichier CSV correspondant au motif
csv_pattern = "SignoffFlightsDataset_Single_*_CLEAN.csv"
csv_files = glob.glob(csv_pattern)
if csv_files:
	csv_files.sort(key=os.path.getmtime, reverse=True)
	csv_extract_path = csv_files[0]
	print(f"[INFO] Fichier sélectionné : {csv_extract_path}")
else:
	csv_extract_path = None

if csv_extract_path and os.path.exists(csv_extract_path):
	try:
		df_extract = pd.read_csv(csv_extract_path, encoding="utf-8-sig")
		df_extract.columns = [col.strip() for col in df_extract.columns]
		if len(df_extract.columns) >= 5:
			print("[INFO] Extraction depuis la 5ème colonne :")
			horaires = df_extract.iloc[:, 4].tolist()
			lignes_a_supprimer = set()
			for idx, val in enumerate(horaires):
				val_clean = str(val).split('+')[0].strip()
				parts = val_clean.split()
				if len(parts) == 2:
					date, heure = parts
				else:
					date, heure = val_clean, ""
				t_csv = parse_hhmm(heure)
				t_json = parse_hhmm(json_heure)
				diff = None
				if t_csv is not None and t_json is not None:
					try:
						diff = abs((t_csv - t_json).total_seconds()) / 3600
					except Exception:
						diff = None
				if diff is not None and diff > 1.0:
					lignes_a_supprimer.add(idx)
				diff_str = f"{diff:.2f}h" if diff is not None else "N/A"
				print(f"Ligne {idx+1} | Date : {date} | Heure : {heure} | Différence avec JSON : {diff_str}")
			if lignes_a_supprimer:
				print(f"Suppression des lignes : {[i+1 for i in sorted(lignes_a_supprimer)]}")
				df_extract = df_extract.drop(list(lignes_a_supprimer)).reset_index(drop=True)
				df_extract.to_csv(csv_extract_path, index=False, encoding="utf-8-sig")
				print("Fichier CSV mis à jour après suppression.")
			else:
				print("Aucune ligne à supprimer.")
		else:
			print("[INFO] Le fichier ne contient pas au moins 5 colonnes.")
	except Exception as e:
		print(f"[ERREUR] Lecture du CSV pour extraction horaire : {e}")
else:
	print(f"[INFO] Fichier non trouvé : {csv_extract_path}")


# Extraire l'heure du JSON
json_heure = ""
try:
	with open("flight_selection.json", "r", encoding="utf-8") as f:
		data = json.load(f)
		selected_date = data.get("selected_date", "")
		if selected_date:
			parts = selected_date.split()
			if len(parts) == 2:
				json_heure = parts[1]
			elif len(parts) == 1:
				json_heure = ""
	print(f"[INFO] Heure_JSON : {json_heure}")
except Exception as e:
	print(f"[DEBUG] Erreur lecture JSON : {e}")
 
 