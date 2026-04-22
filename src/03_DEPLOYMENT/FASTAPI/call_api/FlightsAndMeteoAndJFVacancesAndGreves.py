# from tkinter.ttk import Label
import pandas as pd

# Fichiers d'entrée
flights_path = "FlightsAndMeteoAndJFVacances.csv"
greves_path = "OutputDataGreves/greves_aeroports_210_derniers_jours.csv"
output_path = "FlightsAndMeteoAndJFVacancesAndGreves.csv"


# Chargement des données
flights = pd.read_csv(flights_path, encoding="utf-8-sig")
greves = pd.read_csv(greves_path, sep=';', encoding="utf-8-sig")
print(f"Diagnostic : {flights_path} contient {len(flights)} lignes.")
print(f"Diagnostic : {greves_path} contient {len(greves)} lignes.")
print(f"Nombre de lignes dans flights avant merge : {len(flights)}")
print(f"Nombre de lignes dans greves avant merge : {len(greves)}")

# Normalisation des dates pour le merge
flights['flight_date'] = flights['flight_date'].astype(str).str.strip()
greves['date'] = greves['date'].astype(str).str.strip()

# Fusion sur la date du vol
merged = pd.merge(flights, greves, left_on='flight_date', right_on='date', how='left')
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
