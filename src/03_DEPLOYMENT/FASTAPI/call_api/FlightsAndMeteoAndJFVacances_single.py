import pandas as pd
import glob
import os
from pathlib import Path


def get_request_dir() -> Path:
    return Path(os.getenv("REQUEST_DIR", "."))


def get_output_jf_dir() -> Path:
    return Path(os.getenv("REQUEST_OUTPUT_JF", "OutputJFVacances"))


def build_flights_path() -> Path:
    request_dir = get_request_dir()
    candidates = [
        request_dir / "FlightsAndMeteo_Single.csv",
        Path("FlightsAndMeteo_Single.csv"),
    ]

    for path in candidates:
        if path.exists():
            return path

    raise FileNotFoundError(
        "Aucun fichier FlightsAndMeteo_Single.csv trouvé."
    )


def build_jf_path() -> Path:
    output_jf_dir = get_output_jf_dir()
    candidates = glob.glob(str(output_jf_dir / "*single_calendrier*"))

    if not candidates:
        candidates = glob.glob(os.path.join("OutputJFVacances", "*single_calendrier*"))

    if not candidates:
        raise FileNotFoundError(
            "Aucun fichier correspondant à '*single_calendrier*' trouvé dans le dossier JF/Vacances"
        )

    return Path(max(candidates, key=os.path.getmtime))


def build_output_path() -> Path:
    request_dir = get_request_dir()
    request_dir.mkdir(parents=True, exist_ok=True)
    return request_dir / "FlightsAndMeteoAndJFVacances_Single.csv"


def main():
    flights_path = build_flights_path()
    jf_path = build_jf_path()
    output_path = build_output_path()

    flights = pd.read_csv(flights_path, encoding="utf-8-sig")
    jf = pd.read_csv(jf_path, sep=";", encoding="utf-8-sig")

    print(f"Diagnostic : {flights_path} contient {len(flights)} lignes.")
    print(f"Diagnostic : {jf_path} contient {len(jf)} lignes.")
    print(f"Nombre de lignes dans flights avant merge : {len(flights)}")
    print(f"Nombre de lignes dans jf avant merge : {len(jf)}")

    flights["flight_date"] = flights["flight_date"].astype(str).str.strip()
    jf["date"] = jf["date"].astype(str).str.strip()

    merged = pd.merge(flights, jf, left_on="flight_date", right_on="date", how="left")
    print(f"Nombre de lignes après merge : {len(merged)}")

    if "date" in merged.columns:
        merged = merged.drop(columns=["date"])

    merged["is_summer_vacation"] = (
        merged["Label des Vacances"] == "Vacances d'été"
    ).astype(int)

    merged.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"Fichier final avec colonne 'is_summer_vacation' généré : {output_path}")


if __name__ == "__main__":
    main()