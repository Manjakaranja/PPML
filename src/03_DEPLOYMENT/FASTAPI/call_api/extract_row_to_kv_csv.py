import os
import re
import sys
from pathlib import Path

import pandas as pd


def get_request_dir() -> Path:
    return Path(os.getenv("REQUEST_DIR", "."))


def get_output_meteo_dir() -> Path:
    return Path(os.getenv("REQUEST_OUTPUT_METEO", "OutputDataMeteo"))


def build_source_path() -> Path:
    """
    Nouveau mode:
    - cherche d'abord dans REQUEST_DIR / FlightsAndMeteo_Single.csv

    Fallback ancien mode:
    - FlightsAndMeteo.csv à la racine
    """
    request_dir = get_request_dir()

    candidates = [
        request_dir / "FlightsAndMeteo_Single.csv",
        request_dir / "FlightsAndMeteo.csv",
        Path("FlightsAndMeteo_Single.csv"),
        Path("FlightsAndMeteo.csv"),
    ]

    for path in candidates:
        if path.exists():
            return path

    raise FileNotFoundError(
        "Aucun fichier source trouvé parmi : "
        + ", ".join(str(p) for p in candidates)
    )


def build_meteo_path() -> Path:
    """
    Nouveau mode:
    - REQUEST_OUTPUT_METEO / meteo_aeroports_Single.csv

    Fallback ancien mode:
    - OutputDataMeteo/meteo_aeroports_210_derniers_jours.csv
    - OutputDataMeteo/meteo_aeroports_Single.csv
    """
    output_meteo_dir = get_output_meteo_dir()

    candidates = [
        output_meteo_dir / "meteo_aeroports_Single.csv",
        output_meteo_dir / "meteo_aeroports_210_derniers_jours.csv",
        Path("OutputDataMeteo") / "meteo_aeroports_Single.csv",
        Path("OutputDataMeteo") / "meteo_aeroports_210_derniers_jours.csv",
    ]

    for path in candidates:
        if path.exists():
            return path

    raise FileNotFoundError(
        "Aucun fichier météo trouvé parmi : "
        + ", ".join(str(p) for p in candidates)
    )


def build_output_path(line_num: int) -> Path:
    """
    Sauvegarde dans REQUEST_DIR si fourni.
    """
    request_dir = get_request_dir()
    request_dir.mkdir(parents=True, exist_ok=True)
    return request_dir / f"FlightsAndMeteo_row{line_num}_kv.csv"


def extract_hour_int(val):
    m = re.search(r"(\d{2}):(\d{2})", str(val))
    return int(m.group(1)) if m else 0


def extract_hour(val):
    if not val:
        return ""
    m = re.search(r"(\d{2}:\d{2}(?::\d{2})?)", str(val))
    return m.group(1) if m else ""


def main():
    if len(sys.argv) < 2:
        print("Usage: python extract_row_to_kv_csv.py <numero_ligne>")
        sys.exit(1)

    try:
        line_num = int(sys.argv[1])
        if line_num < 1:
            raise ValueError
    except ValueError:
        print("Le numéro de ligne doit être un entier >= 1.")
        sys.exit(1)

    source_path = build_source_path()
    meteo_path = build_meteo_path()
    output_path = build_output_path(line_num)

    print(f"[INFO] source_path = {source_path}")
    print(f"[INFO] meteo_path  = {meteo_path}")
    print(f"[INFO] output_path = {output_path}")

    with open(source_path, encoding="utf-8-sig") as f:
        header = f.readline().strip().split(",")

    df = pd.read_csv(
        source_path,
        skiprows=line_num - 1,
        nrows=1,
        header=None,
        encoding="utf-8-sig",
    )

    if df.empty:
        print(f"La ligne {line_num} n'existe pas dans le fichier.")
        sys.exit(1)

    row = df.iloc[0].tolist()

    try:
        idx_actual_departure = header.index("actual_departure")
        idx_actual_arrival = header.index("actual_arrival")
        idx_origin = header.index("airport_origin")
        idx_dest = header.index("airport_destination")
    except ValueError:
        print("actual_departure, actual_arrival, airport_origin ou airport_destination introuvable dans le header.")
        sys.exit(1)

    actual_departure = str(row[idx_actual_departure])[:10]
    actual_arrival = str(row[idx_actual_arrival])[:10]
    airport_origin = str(row[idx_origin]).strip()
    airport_dest = str(row[idx_dest]).strip()

    try:
        meteo_df = pd.read_csv(meteo_path, sep=";", encoding="utf-8-sig")
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier météo: {e}")
        sys.exit(1)

    meteo_df.columns = [c.strip() for c in meteo_df.columns]
    if "icao" in meteo_df.columns:
        meteo_df["icao"] = meteo_df["icao"].astype(str).str.strip()
    if "time" in meteo_df.columns:
        meteo_df["time"] = meteo_df["time"].astype(str).str[:10]

    verif_col = []
    meteo_line_idx = []

    try:
        idx_time_dep = header.index("time_dep")
        time_dep_val = row[idx_time_dep]
    except ValueError:
        time_dep_val = ""

    try:
        idx_time_arr = header.index("time_arr")
        time_arr_val = row[idx_time_arr]
    except ValueError:
        time_arr_val = ""

    hour_offset_dep = extract_hour_int(time_dep_val)
    hour_offset_arr = extract_hour_int(time_arr_val)

    for col, val in zip(header, row):
        if col.endswith("_dep"):
            meteo_col = col.replace("_dep", "")
            filt = (meteo_df["time"] == actual_departure) & (meteo_df["icao"] == airport_origin)
            subset = meteo_df.loc[filt]

            if meteo_col in meteo_df.columns and not subset.empty:
                idx = hour_offset_dep if hour_offset_dep < len(subset) else 0
                val_meteo = subset.iloc[idx][meteo_col] if len(subset) > idx else ""
                verif_col.append(str(val_meteo))
                meteo_line_idx.append(str(subset.index[idx] + 2) if len(subset) > idx else "")
            else:
                verif_col.append("")
                meteo_line_idx.append("")

        elif col.endswith("_arr"):
            meteo_col = col.replace("_arr", "")
            filt = (meteo_df["time"] == actual_arrival) & (meteo_df["icao"] == airport_dest)
            subset = meteo_df.loc[filt]

            if meteo_col in meteo_df.columns and not subset.empty:
                idx = hour_offset_arr if hour_offset_arr < len(subset) else 0
                val_meteo = subset.iloc[idx][meteo_col] if len(subset) > idx else ""
                verif_col.append(str(val_meteo))
                meteo_line_idx.append(str(subset.index[idx] + 2) if len(subset) > idx else "")
            else:
                verif_col.append("")
                meteo_line_idx.append("")
        else:
            verif_col.append("")
            meteo_line_idx.append("")

    kv_df = pd.DataFrame({
        "colonne": header,
        "valeur": row,
        "verif": verif_col
    })

    if any(meteo_line_idx):
        dep_idx = None
        arr_idx = None

        for col, idx in zip(header, meteo_line_idx):
            if col.endswith("_dep") and idx and dep_idx is None:
                dep_idx = idx
            if col.endswith("_arr") and idx and arr_idx is None:
                arr_idx = idx
            if dep_idx and arr_idx:
                break

        idxs = []
        if dep_idx:
            idxs.append(dep_idx)
        if arr_idx:
            idxs.append(arr_idx)

        kv_df = pd.concat([
            kv_df,
            pd.DataFrame({
                "colonne": ["meteo_row_index"],
                "valeur": [""],
                "verif": [";".join(idxs)]
            })
        ], ignore_index=True)

    time_dep_hour = extract_hour(time_dep_val)
    time_arr_hour = extract_hour(time_arr_val)

    kv_df = pd.concat([
        kv_df,
        pd.DataFrame({
            "colonne": ["X_meteo_row_index"],
            "valeur": [""],
            "verif": [
                kv_df[kv_df["colonne"] == "meteo_row_index"]["verif"].iloc[0]
            ] if "meteo_row_index" in kv_df["colonne"].values else [""]
        }) if "meteo_row_index" in kv_df["colonne"].values else pd.DataFrame(),
        pd.DataFrame({
            "colonne": ["X_time_dep"],
            "valeur": [""],
            "verif": [time_dep_hour]
        }),
        pd.DataFrame({
            "colonne": ["X_time_arr"],
            "valeur": [""],
            "verif": [time_arr_hour]
        })
    ], ignore_index=True)

    kv_df = kv_df[kv_df["colonne"] != "meteo_row_index"]

    kv_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"Fichier généré : {output_path}")


if __name__ == "__main__":
    main()