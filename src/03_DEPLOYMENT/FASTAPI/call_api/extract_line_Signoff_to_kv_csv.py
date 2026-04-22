
import pandas as pd
import sys
import glob
import os
from datetime import datetime

# Trouver le dernier fichier SignofFlightsDataset_future_*.csv
files = glob.glob("SignofFlightsDataset_future_*.csv")
if not files:
    print("Aucun fichier SignofFlightsDataset_future_*.csv trouvé dans le dossier courant.")
    sys.exit(1)
input_path = max(files, key=os.path.getctime)
print(f"Fichier source utilisé : {input_path}")

# Fichier de sortie daté et numéroté
row_number_1idx = int(sys.argv[1]) if len(sys.argv) > 1 else row_number + 1
date_gen = datetime.now().strftime("%Y%m%d_%H%M%S")
output_path = f"SignoffLine_7days_LineNumber{row_number_1idx}_{date_gen}.csv"

# Numéro de la ligne à extraire (0-indexé, donc 99 pour la 100e ligne)
if len(sys.argv) > 1:
     try:
         row_number = int(sys.argv[1]) - 1
         if row_number < 0:
             raise ValueError
     except ValueError:
         print("Veuillez entrer un numéro de ligne valide (entier >= 1) en argument.")
         sys.exit(1)
else:
     while True:
         row_number = input("Entrez le numéro de la ligne à extraire (1 pour la première ligne) : ")
         try:
             row_number = int(row_number) - 1  # Convertir en index 0
             if row_number < 0:
                 raise ValueError
             break
         except ValueError:
             print("Veuillez entrer un numéro de ligne valide (entier >= 1).")

df = pd.read_csv(input_path, encoding="utf-8-sig")

if row_number >= len(df):
    raise ValueError(f"Le fichier ne contient que {len(df)} lignes.")

row = df.iloc[row_number]

# Création du DataFrame résultat
result = pd.DataFrame({
    'colonne': row.index,
    'valeur': row.values
})


# Ajout de la date de génération et du NbLineWWW
date_gen_human = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
row_number_1idx = row_number + 1
nbline_label = f"LineNumber_{row_number_1idx}"

# Place DATE_GENERATION et LineNumber_ en tout début du fichier
result_with_date = pd.concat([
    pd.DataFrame({
        'colonne': ['DATE_GENERATION', nbline_label],
        'valeur': [date_gen_human, row_number_1idx]
    }),
    result
], ignore_index=True)

result_with_date.to_csv(output_path, index=False, encoding="utf-8-sig")
print(f"Ligne extraite et sauvegardée dans {output_path} (générée le {date_gen_human}, NbLine={row_number_1idx})")
