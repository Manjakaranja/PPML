from tkinter.ttk import Label

import pandas as pd
import glob
import os

# Fichiers d'entrée
flights_path = "FlightsAndMeteo_Single.csv"
# Recherche automatique du fichier le plus récent contenant 'single_calendrier' dans le nom
jf_files = glob.glob(os.path.join("OutputJFVacances", "*single_calendrier*"))
if not jf_files:
    raise FileNotFoundError("Aucun fichier correspondant à '*single_calendrier*' trouvé dans OutputJFVacances")
jf_path = max(jf_files, key=os.path.getmtime)
output_path = "FlightsAndMeteoAndJFVacances_Single.csv"

# Chargement des données
flights = pd.read_csv(flights_path, encoding="utf-8-sig")
jf = pd.read_csv(jf_path, sep=';', encoding="utf-8-sig")
print(f"Diagnostic : {flights_path} contient {len(flights)} lignes.")
print(f"Diagnostic : {jf_path} contient {len(jf)} lignes.")
print(f"Nombre de lignes dans flights avant merge : {len(flights)}")
print(f"Nombre de lignes dans jf avant merge : {len(jf)}")
        

# Normalisation des dates pour le merge
flights['flight_date'] = flights['flight_date'].astype(str).str.strip()
jf['date'] = jf['date'].astype(str).str.strip()

# Fusion sur la date du vol
merged = pd.merge(flights, jf, left_on='flight_date', right_on='date', how='left')
print(f"Nombre de lignes après merge : {len(merged)}")

# On enlève la colonne 'date' dupliquée
if 'date' in merged.columns:
    merged = merged.drop(columns=['date'])

# Sauvegarde
merged.to_csv(output_path, index=False, encoding="utf-8-sig")
print(f"Fichier généré : {output_path}")

# Ajoute une colonne binaire indiquant si le vol est pendant les vacances d'été
merged['is_summer_vacation'] = (merged['Label des Vacances'] == "Vacances d'été").astype(int)
# Optionnel : pour garder le label texte à la place du booléen, décommente la ligne suivante
# merged['is_summer_vacation'] = merged['Label des Vacances'].apply(lambda x: "Vacances d'été" if x == "Vacances d'été" else "")

# Sauvegarde du fichier final avec la colonne
merged.to_csv(output_path, index=False, encoding="utf-8-sig")
print(f"Fichier final avec colonne 'is_summer_vacation' généré : {output_path}")
