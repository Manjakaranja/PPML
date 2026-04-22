import glob
import os
from pathlib import Path

import pandas as pd


def get_request_dir() -> Path:
    return Path(os.getenv("REQUEST_DIR", "."))


def get_output_greves_dir() -> Path:
    return Path(os.getenv("REQUEST_OUTPUT_GREVES", "OutputDataGreves"))


def build_flights_path() -> Path:
    request_dir = get_request_dir()
    candidates = [
        request_dir / "FlightsAndMeteoAndJFVacances_Single.csv",
        Path("FlightsAndMeteoAndJFVacances_Single.csv"),
    ]

    for path in candidates:
        if path.exists():
            return path

    raise FileNotFoundError(
        "Aucun fichier FlightsAndMeteoAndJFVacances_Single.csv trouvé."
    )


def build_greves_path() -> Path:
    output_greves_dir = get_output_greves_dir()
    output_greves_dir.mkdir(parents=True, exist_ok=True)

    csv_files = glob.glob(str(output_greves_dir / "*greve_aeroports*.csv"))
    if not csv_files:
        raise FileNotFoundError(
            f"Aucun fichier correspondant à '*greve_aeroports*.csv' trouvé dans {output_greves_dir}"
        )

    latest_csv = max(csv_files, key=os.path.getmtime)
    greves_path = output_greves_dir / "greve_aeroports_Single.csv"

    if os.path.abspath(latest_csv) != os.path.abspath(greves_path):
        os.replace(latest_csv, greves_path)

    return greves_path


def build_output_path() -> Path:
    request_dir = get_request_dir()
    request_dir.mkdir(parents=True, exist_ok=True)
    return request_dir / "FlightsAndMeteoAndJFVacancesAndGreves_Single.csv"


def main():
    flights_path = build_flights_path()
    greves_path = build_greves_path()
    output_path = build_output_path()

    flights = pd.read_csv(flights_path, encoding="utf-8-sig")
    greves = pd.read_csv(greves_path, sep=";", encoding="utf-8-sig")

    print(f"Diagnostic : {flights_path} contient {len(flights)} lignes.")
    print(f"Diagnostic : {greves_path} contient {len(greves)} lignes.")
    print(f"Nombre de lignes dans flights avant merge : {len(flights)}")
    print(f"Nombre de lignes dans greves avant merge : {len(greves)}")

    flights["flight_date"] = flights["flight_date"].astype(str).str.strip()
    greves["date"] = greves["date"].astype(str).str.strip()

    merged = pd.merge(
        flights,
        greves,
        left_on="flight_date",
        right_on="date",
        how="left"
    )
    print(f"Nombre de lignes après merge : {len(merged)}")

    if "date" in merged.columns:
        merged = merged.drop(columns=["date"])

    merged["is_summer_vacation"] = (
        merged["Label des Vacances"] == "Vacances d'été"
    ).astype(int)

    cols_to_drop = merged.columns[:4]
    merged = merged.drop(columns=cols_to_drop)

    merged.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"Fichier final avec colonne 'is_summer_vacation' généré : {output_path}")


if __name__ == "__main__":
    main()