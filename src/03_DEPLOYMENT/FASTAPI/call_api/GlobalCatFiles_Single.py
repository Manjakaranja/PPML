import os
import glob
import sys
from datetime import datetime

import pandas as pd


def get_output_single_dir() -> str:
    return os.getenv("REQUEST_OUTPUT_SINGLE", "OutputSingleFlight")


def get_output_meteo_dir() -> str:
    return os.getenv("REQUEST_OUTPUT_METEO", "OutputDataMeteo")


def merge_all_meteo_files_for_day(directory: str, day_str: str, output_path: str):
    """
    Fusionne tous les fichiers météo du dossier courant de la requête.
    """
    files = [
        f for f in os.listdir(directory)
        if f.startswith("meteo_")
        and f.endswith(".csv")
        and f != "meteo_aeroports_Single.csv"
    ]

    if not files:
        print(f"Aucun fichier météo trouvé pour {day_str} dans {directory}")
        return

    dfs = []
    for f in files:
        file_path = os.path.join(directory, f)
        df = pd.read_csv(file_path, sep=";", dtype=str)
        dfs.append(df)

    merged = pd.concat(dfs, ignore_index=True)

    if "icao" in merged.columns:
        merged["icao"] = merged["icao"].replace({"NICE": "NCE"})

    merged.to_csv(output_path, index=False, encoding="utf-8-sig", sep=";")
    print(f"Fichier généré : {output_path} (séparateur point-virgule)")


def concat_vols_journaliers(output_single_dir: str):
    """
    Concatène les deux fichiers vols_journaliers produits pour source + destination.
    """
    vols_files = glob.glob(os.path.join(output_single_dir, "vols_journaliers_*.csv"))

    # On exclut l'éventuel fichier de sortie déjà concaténé
    vols_files = [
        f for f in vols_files
        if not os.path.basename(f).startswith("vols_journaliers_single")
    ]

    if len(vols_files) < 2:
        raise FileNotFoundError(
            f"Moins de deux fichiers 'vols_journaliers_*.csv' trouvés dans {output_single_dir}"
        )

    vols_files_sorted = sorted(vols_files, key=os.path.getmtime, reverse=True)[:2]

    dfs = [pd.read_csv(f) for f in vols_files_sorted]
    df_concat = pd.concat(dfs, ignore_index=True)

    output_single = os.path.join(output_single_dir, "vols_journaliers_single.csv")
    df_concat.to_csv(output_single, index=False, encoding="utf-8-sig")
    print(f"Fichier concaténé généré : {output_single}")


def main():
    if len(sys.argv) > 1:
        day_str = sys.argv[1]
    else:
        day_str = datetime.today().strftime("%Y-%m-%d")

    output_meteo_dir = get_output_meteo_dir()
    output_single_dir = get_output_single_dir()

    os.makedirs(output_meteo_dir, exist_ok=True)
    os.makedirs(output_single_dir, exist_ok=True)

    meteo_output_path = os.path.join(output_meteo_dir, "meteo_aeroports_Single.csv")

    merge_all_meteo_files_for_day(
        directory=output_meteo_dir,
        day_str=day_str,
        output_path=meteo_output_path,
    )

    concat_vols_journaliers(output_single_dir)


if __name__ == "__main__":
    main()