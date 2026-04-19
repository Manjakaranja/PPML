import pandas as pd
import os
from datetime import datetime
import glob

def merge_all_meteo_files_for_day(directory, day_str, output_path):
    # Cherche tous les fichiers du dossier qui contiennent 'meteo_' et la date
    files = [f for f in os.listdir(directory) if f.startswith('meteo_') and f.endswith('.csv')]
    if not files:
        print(f"Aucun fichier météo trouvé pour {day_str} dans {directory}")
        return
    dfs = []
    for f in files:
        df = pd.read_csv(os.path.join(directory, f), sep=';', dtype=str)
        # On ne touche pas à la colonne 'icao' du fichier source
        dfs.append(df)
    merged = pd.concat(dfs, ignore_index=True)
    # Harmonisation finale : remplacer NICE par NCE dans la colonne icao
    merged['icao'] = merged['icao'].replace({'NICE': 'NCE'})
    merged.to_csv(output_path, index=False, encoding='utf-8-sig', sep=';')
    print(f"Fichier généré : {output_path} (séparateur point-virgule)")


if __name__ == "__main__":
    # Utilise la date du jour ou une date passée en argument
    import sys
    if len(sys.argv) > 1:
        day_str = sys.argv[1]
    else:
        day_str = datetime.today().strftime('%Y-%m-%d')
    directory = "OutputDataMeteo"
    output_path = os.path.join(directory, "meteo_aeroports_Single.csv")
    merge_all_meteo_files_for_day(directory, day_str, output_path)

# Recherche des deux derniers fichiers CSV contenant 'vols_journaliers_' dans OutputSingleFlight
vols_files = glob.glob(os.path.join("OutputSingleFlight", "vols_journaliers_*.csv"))
if len(vols_files) < 2:
    raise FileNotFoundError("Moins de deux fichiers 'vols_journaliers_*.csv' trouvés dans OutputSingleFlight")
# Trie par date de modification décroissante
vols_files_sorted = sorted(vols_files, key=os.path.getmtime, reverse=True)[:2]
# Concaténation
dfs = [pd.read_csv(f) for f in vols_files_sorted]
df_concat = pd.concat(dfs, ignore_index=True)
output_single = os.path.join("OutputSingleFlight", "vols_journaliers_single.csv")
df_concat.to_csv(output_single, index=False, encoding="utf-8-sig")
print(f"Fichier concaténé généré : {output_single}")


