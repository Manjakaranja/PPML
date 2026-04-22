import os
import glob
import sys

import pandas as pd


def get_congestion_dir() -> str:
    """
    Nouveau mode :
    - utilise le dossier de requête courant pour les fichiers vols / congestion

    Fallback ancien mode :
    - OutputSingleFlight
    """
    return os.getenv("REQUEST_OUTPUT_SINGLE", "OutputSingleFlight")


cong_dir = get_congestion_dir()
abs_cong_dir = os.path.abspath(cong_dir)
print(f"Chemin absolu du dossier congestion : {abs_cong_dir}")

os.makedirs(cong_dir, exist_ok=True)

# Suppression des anciens fichiers générés dans le dossier courant de la requête
for pattern in ["*_corrige.csv", "*WW*_updated.csv", "*ww*_updated.csv"]:
    for oldfile in glob.glob(os.path.join(cong_dir, pattern)):
        try:
            os.remove(oldfile)
            print(f"Fichier supprimé : {oldfile}")
        except Exception as e:
            print(f"Erreur suppression {oldfile} : {e}")

print("Fichiers trouvés dans le dossier congestion :", os.listdir(cong_dir))
print(
    "Fichiers CSV détectés :",
    [f for f in os.listdir(cong_dir) if f.endswith(".csv")]
)

fichiers_corriges = []

# Seuil de décision pour la congestion
if len(sys.argv) > 1:
    try:
        seuil_congestion = float(sys.argv[1])
    except Exception:
        print("Seuil de congestion invalide, utilisation de 20.")
        seuil_congestion = 20
else:
    seuil_congestion = 20

print(f"Seuil de décision pour la congestion : {seuil_congestion}")

# Cherche tous les fichiers contenant 'congestion_ww' au début du nom
for fname in os.listdir(cong_dir):
    print(f"Analyse du fichier : {fname}")
    lower = fname.lower()

    if lower.startswith("congestion_ww") and lower.endswith(".csv"):
        fpath = os.path.join(cong_dir, fname)
        print(f"Chemin complet du fichier traité : {os.path.abspath(fpath)}")

        try:
            df = pd.read_csv(fpath)
        except Exception as e:
            print(f"Erreur lecture {fname} : {e}")
            continue

        col_depart = "nombre_departs"
        col_arrivee = "nombre_arrivees"
        col_aeroport = None

        for possible in ["aeroport", "airport"]:
            if possible in df.columns:
                col_aeroport = possible
                break

        print(f"Colonnes du fichier {fname} : {list(df.columns)}")

        if col_depart in df.columns and col_arrivee in df.columns and col_aeroport:
            df = df.iloc[3:-1].reset_index(drop=True)

            df["somme_depart_arrivee"] = (
                pd.to_numeric(df[col_depart], errors="coerce").fillna(0)
                + pd.to_numeric(df[col_arrivee], errors="coerce").fillna(0)
            )

            moyennes = df.groupby(col_aeroport)["somme_depart_arrivee"].transform("mean")

            df["pourcentage_ecart_moyenne"] = (
                100
                * (df["somme_depart_arrivee"] - moyennes)
                / moyennes.replace(0, float("nan"))
            ).round(2)

            df["congestion"] = (
                df["pourcentage_ecart_moyenne"] > seuil_congestion
            ).astype(int)

            for col in ["moyenne_depart_arrivee", "ecart_somme_moyenne"]:
                if col in df.columns:
                    df = df.drop(columns=[col])

            base, ext = os.path.splitext(fname)
            new_fname = base + "_updated" + ext
            new_fpath = os.path.join(cong_dir, new_fname)
            print(f"Chemin de sortie : {os.path.abspath(new_fpath)}")

            try:
                df.to_csv(new_fpath, index=False, encoding="utf-8-sig")
                print(f"Colonnes ajoutées et fichier renommé : {new_fname}")
                fichiers_corriges.append(new_fname)
            except Exception as e:
                print(f"Erreur écriture {new_fname} : {e}")
        else:
            print(f"Colonnes nécessaires non trouvées dans {fname}, fichier ignoré.")

if fichiers_corriges:
    print("\nFichiers corrigés générés dans le dossier congestion :")
    for f in fichiers_corriges:
        print(f" - {f}")
else:
    print("\nAucun fichier corrigé n'a été généré.")