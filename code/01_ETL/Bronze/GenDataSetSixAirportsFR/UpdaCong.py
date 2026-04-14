import pandas as pd
import os
import glob
import sys

# Dossier contenant les fichiers de congestion
cong_dir = 'OutputFlights'
abs_cong_dir = os.path.abspath(cong_dir)
print(f"Chemin absolu du dossier OutputFlights : {abs_cong_dir}")

# Suppression des anciens fichiers *_corrige.csv et *_updated.csv
for pattern in ['*_corrige.csv', '*WW*_updated.csv']:
    for oldfile in glob.glob(os.path.join(cong_dir, pattern)):
        try:
            os.remove(oldfile)
            print(f"Fichier supprimé : {oldfile}")
        except Exception as e:
            print(f"Erreur suppression {oldfile} : {e}")

print("Fichiers trouvés dans OutputFlights :", os.listdir(cong_dir))
print("Fichiers CSV détectés :", [f for f in os.listdir(cong_dir) if f.endswith('.csv')])

fichiers_corriges = []
# Seuil de décision pour la congestion (par défaut 20)
if len(sys.argv) > 1:
    try:
        seuil_congestion = float(sys.argv[1])
    except Exception:
        print("Seuil de congestion invalide, utilisation de 20.")
        seuil_congestion = 20
else:
    seuil_congestion = 20
print(f"Seuil de décision pour la congestion : {seuil_congestion}")

# Cherche tous les fichiers contenant 'Congestion_ww' ou 'congestion_ww' dans le nom
for fname in os.listdir(cong_dir):
    print(f"Analyse du fichier : {fname}")
    # Ne traiter que les fichiers du type 'congestion_ww*.csv' (insensible à la casse)
    lower = fname.lower()
    if lower.startswith('congestion_ww'):
        fpath = os.path.join(cong_dir, fname)
        print(f"Chemin complet du fichier traité : {os.path.abspath(fpath)}")
        try:
            df = pd.read_csv(fpath)
        except Exception as e:
            print(f"Erreur lecture {fname} : {e}")
            continue
        # Colonnes à utiliser (détection automatique pour la colonne aéroport)
        col_depart = 'nombre_departs'
        col_arrivee = 'nombre_arrivees'
        col_aeroport = None
        for possible in ['aeroport', 'airport']:
            if possible in df.columns:
                col_aeroport = possible
                break
        print(f"Colonnes du fichier {fname} : {list(df.columns)}")
        if col_depart in df.columns and col_arrivee in df.columns and col_aeroport:
            # Supprimer les 3 premières lignes et la dernière ligne
            df = df.iloc[3:-1].reset_index(drop=True)
            df['somme_depart_arrivee'] = df[col_depart] + df[col_arrivee]
            # Moyenne par aéroport sur tous les jours (après suppression)
            moyennes = df.groupby(col_aeroport)['somme_depart_arrivee'].transform('mean')
            # Pourcentage d'écart par rapport à la moyenne, arrondi à 2 décimales
            df['pourcentage_ecart_moyenne'] = (100 * (df['somme_depart_arrivee'] - moyennes) / moyennes.replace(0, float('nan'))).round(2)
            # Ajoute la colonne 'congestion' : 1 si > 20, sinon 0
            df['congestion'] = (df['pourcentage_ecart_moyenne'] > seuil_congestion).astype(int)
            # Supprime les anciennes colonnes si elles existent
            for col in ['moyenne_depart_arrivee', 'ecart_somme_moyenne']:
                if col in df.columns:
                    df = df.drop(columns=[col])
            base, ext = os.path.splitext(fname)
            new_fname = base + '_updated' + ext
            new_fpath = os.path.join(cong_dir, new_fname)
            print(f"Chemin de sortie : {os.path.abspath(new_fpath)}")
            try:
                df.to_csv(new_fpath, index=False, encoding='utf-8-sig')
                print(f"Colonnes ajoutées et fichier renommé : {new_fname}")
                fichiers_corriges.append(new_fname)
            except Exception as e:
                print(f"Erreur écriture {new_fname} : {e}")
        else:
            print(f"Colonnes nécessaires non trouvées dans {fname}, fichier ignoré.")

if fichiers_corriges:
    print("\nFichiers corrigés générés dans OutputFlights :")
    for f in fichiers_corriges:
        print(f" - {f}")
else:
    print("\nAucun fichier corrigé n'a été généré.")
