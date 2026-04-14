import pandas as pd
import sys

if len(sys.argv) < 2:
    print("Usage: python extract_row_FULL_to_kv_csv.py <numero_ligne>")
    sys.exit(1)
try:
    line_num = int(sys.argv[1])
    if line_num < 1:
        raise ValueError
except ValueError:
    print("Le numéro de ligne doit être un entier >= 1.")
    sys.exit(1)

source_path = "FlightsAndMeteoAndJFVacancesAndGreves.csv"
output_path = f"FlightsAndMeteoAndJFVacancesAndGreves_row{line_num}_kv.csv"

# Lire le header
with open(source_path, encoding="utf-8-sig") as f:
    header = f.readline().strip().split(",")

# Charger la ligne demandée (index = line_num - 1)
df = pd.read_csv(source_path, skiprows=line_num-1, nrows=1, header=None, encoding="utf-8-sig")
if df.empty:
    print(f"La ligne {line_num} n'existe pas dans le fichier.")
    sys.exit(1)
row = df.iloc[0].tolist()

# Créer le DataFrame clé/valeur
kv_df = pd.DataFrame({
    "Nom de colonne": header,
    "valeur": row
})

# Sauvegarder le résultat
kv_df.to_csv(output_path, index=False, encoding="utf-8-sig")
print(f"Fichier généré : {output_path}")
