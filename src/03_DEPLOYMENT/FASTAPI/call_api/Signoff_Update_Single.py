import os
import sys
from pathlib import Path
from datetime import datetime

import pandas as pd


def get_request_dir() -> Path:
    return Path(os.getenv("REQUEST_DIR", "."))


def get_output_single_dir() -> Path:
    return Path(os.getenv("REQUEST_OUTPUT_SINGLE", "OutputSingleFlight"))


def build_flights_path() -> Path:
    request_dir = get_request_dir()
    candidates = [
        request_dir / "FlightsAndMeteoAndJFVacancesAndGreves_Single.csv",
        Path("FlightsAndMeteoAndJFVacancesAndGreves_Single.csv"),
    ]

    for path in candidates:
        if path.exists():
            return path

    raise FileNotFoundError(
        "Fichier FlightsAndMeteoAndJFVacancesAndGreves_Single.csv introuvable"
    )


def build_cong_path() -> Path:
    output_single_dir = get_output_single_dir()
    candidates = [
        output_single_dir / "vols_journaliers_single.csv",
        Path("OutputSingleFlight") / "vols_journaliers_single.csv",
    ]

    for path in candidates:
        if path.exists():
            return path

    raise FileNotFoundError("Fichier vols_journaliers_single.csv introuvable")


def build_output_paths() -> tuple[Path, Path]:
    request_dir = get_request_dir()
    request_id = os.getenv("REQUEST_ID", datetime.now().strftime("%Y%m%d_%H%M%S"))

    output_clean = request_dir / f"SignoffFlightsDataset_Single_{request_id}_CLEAN.csv"
    col_list_path = request_dir / f"SignoffFlightsDataset_Single_{request_id}_ColList.csv"

    return output_clean, col_list_path


def get_delay_threshold() -> int:
    delay_threshold = 15

    if len(sys.argv) > 1:
        try:
            delay_threshold = int(sys.argv[1])
        except Exception:
            print("Seuil de retard non valide, utilisation de 15 min par défaut.")
            delay_threshold = 15

    return delay_threshold


def main():
    flights_path = build_flights_path()
    cong_path = build_cong_path()
    output_clean, col_list_path = build_output_paths()
    delay_threshold = get_delay_threshold()

    print(f"[INFO] flights_path   = {flights_path}")
    print(f"[INFO] cong_path      = {cong_path}")
    print(f"[INFO] output_clean   = {output_clean}")
    print(f"[INFO] col_list_path  = {col_list_path}")
    print(f"[INFO] delay_threshold = {delay_threshold}")

    flights = pd.read_csv(flights_path, encoding="utf-8-sig")
    cong = pd.read_csv(cong_path, encoding="utf-8-sig")

    flights.columns = [c.strip() for c in flights.columns]
    cong.columns = [c.strip() for c in cong.columns]

    flights["flight_date"] = flights["flight_date"].astype(str).str.strip()
    flights["airport_origin"] = flights["airport_origin"].astype(str).str.strip().str.upper()
    flights["airport_destination"] = flights["airport_destination"].astype(str).str.strip().str.upper()

    cong["flight_date"] = cong["flight_date"].astype(str).str.strip()
    cong["airport"] = cong["airport"].astype(str).str.strip().str.upper()

    source_cols = [
        "nombre_departures",
        "nombre_arrivals",
        "somme_nombre_departs_arrivees",
        "congestion",
    ]
    dest_cols = [
        "nombre_departures",
        "nombre_arrivals",
        "somme_nombre_departs_arrivees",
        "congestion",
    ]

    source_data = []
    dest_data = []

    for idx, row in flights.iterrows():
        src = cong[
            (cong["flight_date"] == row["flight_date"])
            & (cong["airport"] == row["airport_origin"])
        ]
        if not src.empty:
            src_vals = src.iloc[0][source_cols].tolist()
        else:
            print(f"Aucune correspondance source pour {row['flight_date']} / {row['airport_origin']} (ligne {idx+2})")
            src_vals = [None] * len(source_cols)
        source_data.append(src_vals)

        dst = cong[
            (cong["flight_date"] == row["flight_date"])
            & (cong["airport"] == row["airport_destination"])
        ]
        if not dst.empty:
            dst_vals = dst.iloc[0][dest_cols].tolist()
        else:
            print(f"Aucune correspondance destination pour {row['flight_date']} / {row['airport_destination']} (ligne {idx+2})")
            dst_vals = [None] * len(dest_cols)
        dest_data.append(dst_vals)

    for i, col in enumerate(source_cols):
        vals = [vals[i] for vals in source_data]
        flights[f"{col}_source"] = vals

    for i, col in enumerate(dest_cols):
        vals = [vals[i] for vals in dest_data]
        flights[f"{col}_destination"] = vals

    arr_delay_col = "arrival_delay_min"
    dep_delay_col = "departure_delay_min"

    if arr_delay_col in flights.columns and dep_delay_col in flights.columns:
        def fill_arrival_with_departure(row):
            v = row[arr_delay_col]
            if pd.isna(v) or v == "" or str(v).lower() in ("nan", "none", "missing value"):
                return row[dep_delay_col]
            return v

        flights[arr_delay_col] = flights.apply(fill_arrival_with_departure, axis=1)

    if arr_delay_col in flights.columns:
        arr_delay_num = pd.to_numeric(flights[arr_delay_col], errors="coerce")
        flights["retard arrivée"] = arr_delay_num.apply(
            lambda v: 1 if pd.notna(v) and float(v) > delay_threshold else 0
        ).astype(int)
    else:
        flights["retard arrivée"] = 0

    col_status = "status"
    if col_status in flights.columns:
        nb_unknown = (flights[col_status].astype(str).str.strip().str.lower() == "unknown").sum()
        nb_total = flights.shape[0]
        percent_unknown = (nb_unknown / nb_total) * 100 if nb_total > 0 else 0
        print(f"% de cas où '{col_status}' = 'Unknown' : {percent_unknown:.2f}% ({nb_unknown}/{nb_total})")

        flights_clean = flights[
            flights[col_status].astype(str).str.strip().str.lower() != "unknown"
        ].copy()
        print(f"Lignes restantes après suppression des 'Unknown' : {len(flights_clean)}/{nb_total}")
    else:
        flights_clean = flights.copy()

    try:
        date_gen_human = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        date_gen_human = datetime.now().strftime("%Y%m%d_%H%M%S")

    date_row_clean = pd.DataFrame([{col: "" for col in flights_clean.columns}])
    if len(date_row_clean.columns) >= 2:
        date_row_clean.iloc[0, 0] = "DATE_GENERATION"
        date_row_clean.iloc[0, 1] = date_gen_human

    flights_clean_with_date = pd.concat([date_row_clean, flights_clean], ignore_index=True)

    output_clean.parent.mkdir(parents=True, exist_ok=True)
    flights_clean_with_date.to_csv(output_clean, index=False, encoding="utf-8-sig")
    print(f"Fichier nettoyé généré : {output_clean}")

    col_list = pd.DataFrame({"columns": flights_clean.columns})
    col_list.to_csv(col_list_path, index=False, encoding="utf-8-sig")
    print(f"Fichier de liste des colonnes généré : {col_list_path}")


if __name__ == "__main__":
    main()