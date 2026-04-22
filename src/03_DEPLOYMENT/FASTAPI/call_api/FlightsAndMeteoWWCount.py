import os
from pathlib import Path

import pandas as pd


def get_request_dir() -> Path:
    return Path(os.getenv("REQUEST_DIR", "."))


def build_flights_meteo_path() -> Path:
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
        "Aucun fichier FlightsAndMeteo trouvé parmi : "
        + ", ".join(str(p) for p in candidates)
    )


def build_congestion_path() -> Path:
    request_dir = get_request_dir()
    output_single_dir = Path(os.getenv("REQUEST_OUTPUT_SINGLE", "OutputSingleFlight"))

    candidates = [
        request_dir / "vols_journaliers_single.csv",
        output_single_dir / "vols_journaliers_single.csv",
        Path("OutputSingleFlight") / "vols_journaliers_single.csv",
        Path("OutputFlights") / "congestion_WW.csv",
    ]

    for path in candidates:
        if path.exists():
            return path

    raise FileNotFoundError(
        "Aucun fichier de congestion trouvé parmi : "
        + ", ".join(str(p) for p in candidates)
    )


def build_output_path() -> Path:
    request_dir = get_request_dir()
    request_dir.mkdir(parents=True, exist_ok=True)
    return request_dir / "FlightsAndMeteoWWCount.csv"


def main():
    flights_meteo_path = build_flights_meteo_path()
    congestion_path = build_congestion_path()
    output_path = build_output_path()

    flights = pd.read_csv(flights_meteo_path)
    cong = pd.read_csv(congestion_path, skipinitialspace=True)
    cong.columns = [c.strip() for c in cong.columns]

    if "airport" in cong.columns:
        cong["airport"] = cong["airport"].astype(str).str.strip()
    if "flight_date" in cong.columns:
        cong["flight_date"] = cong["flight_date"].astype(str).str.strip()

    flights["NBVols_Depart_Airport_Origin"] = None
    flights["NBVols_Arrivee_Airport_Origin"] = None
    flights["NBVols_Depart_Airport_Destination"] = None
    flights["NBVols_Arrivee_Airport_Destination"] = None

    for idx, row in flights.iterrows():
        date = str(row["flight_date"]).strip()
        airport = str(row["airport_origin"]).strip()
        match = cong[(cong["flight_date"] == date) & (cong["airport"] == airport)]
        if not match.empty:
            flights.at[idx, "NBVols_Depart_Airport_Origin"] = match.iloc[0]["nombre_departures"]
            flights.at[idx, "NBVols_Arrivee_Airport_Origin"] = match.iloc[0]["nombre_arrivals"]

    for idx, row in flights.iterrows():
        date = str(row["flight_date"]).strip()
        airport = str(row["airport_destination"]).strip()
        match = cong[(cong["flight_date"] == date) & (cong["airport"] == airport)]
        if not match.empty:
            flights.at[idx, "NBVols_Depart_Airport_Destination"] = match.iloc[0]["nombre_departures"]
            flights.at[idx, "NBVols_Arrivee_Airport_Destination"] = match.iloc[0]["nombre_arrivals"]

    flights.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"Fichier {output_path} généré.")


if __name__ == "__main__":
    main()