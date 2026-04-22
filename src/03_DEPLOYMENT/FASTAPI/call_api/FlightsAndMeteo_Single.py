import glob
import os

import pandas as pd


def get_output_single_dir() -> str:
    return os.getenv("REQUEST_OUTPUT_SINGLE", "OutputSingleFlight")


def get_output_meteo_dir() -> str:
    return os.getenv("REQUEST_OUTPUT_METEO", "OutputDataMeteo")


def get_request_dir() -> str:
    return os.getenv("REQUEST_DIR", ".")


def main():
    output_single_dir = get_output_single_dir()
    output_meteo_dir = get_output_meteo_dir()
    request_dir = get_request_dir()

    single_flight_files = glob.glob(
        os.path.join(output_single_dir, "SingleFlightData_*.csv")
    )
    single_flight_files = [
        f for f in single_flight_files
        if "RespAPI" not in os.path.basename(f) and "ColList" not in os.path.basename(f)
    ]

    if not single_flight_files:
        raise FileNotFoundError(
            f"Aucun fichier SingleFlightData_*.csv trouvé dans {output_single_dir}"
        )

    flights_path = max(single_flight_files, key=os.path.getmtime)
    meteo_path = os.path.join(output_meteo_dir, "meteo_aeroports_Single.csv")
    output_path = os.path.join(request_dir, "FlightsAndMeteo_Single.csv")

    if not os.path.exists(meteo_path):
        raise FileNotFoundError(f"Fichier météo introuvable : {meteo_path}")

    flights = pd.read_csv(flights_path, sep=",", dtype=str)
    meteo = pd.read_csv(meteo_path, sep=";", dtype=str)

    print("Aperçu des valeurs de scheduled_departure avant conversion :")
    print(flights["scheduled_departure"].head(10))


    meteo["time_dt"] = pd.to_datetime(meteo["time"], errors="coerce")

    flights["airport_origin"] = flights["airport_origin"].replace({"NICE": "NCE"})
    flights["airport_destination"] = flights["airport_destination"].replace({"NICE": "NCE"})
    flights["scheduled_departure_dt"] = pd.to_datetime(
        flights["scheduled_departure"], errors="coerce"
    )
    flights["scheduled_arrival_dt"] = pd.to_datetime(
        flights["scheduled_arrival"], errors="coerce"
    )

    meteo["icao"] = meteo["icao"].replace({"NICE": "NCE"})

    flights["merge_hour_dep"] = flights["scheduled_departure_dt"].dt.floor("h")
    meteo["merge_hour"] = meteo["time_dt"].dt.floor("h")

    meteo_dep = meteo.copy()
    meteo_dep = meteo_dep.drop_duplicates(subset=["icao", "merge_hour"])
    meteo_dep = meteo_dep.add_suffix("_dep")

    merged = pd.merge(
        flights,
        meteo_dep,
        left_on=["airport_origin", "merge_hour_dep"],
        right_on=["icao_dep", "merge_hour_dep"],
        how="left",
    )

    merged["merge_hour_arr"] = merged["scheduled_arrival_dt"].dt.floor("h")

    meteo_arr = meteo.copy()
    meteo_arr = meteo_arr.drop_duplicates(subset=["icao", "merge_hour"])
    meteo_arr = meteo_arr.add_suffix("_arr")

    merged = pd.merge(
        merged,
        meteo_arr,
        left_on=["airport_destination", "merge_hour_arr"],
        right_on=["icao_arr", "merge_hour_arr"],
        how="left",
    )

    merged = merged.drop(
        columns=[
            "scheduled_departure_dt",
            "scheduled_arrival_dt",
            "merge_hour_dep",
            "merge_hour_arr",
            "icao_dep",
            "time_dt_dep",
            "icao_arr",
            "time_dt_arr",
        ],
        errors="ignore",
    )

    merged.to_csv(output_path, index=False, encoding="utf-8-sig", sep=",")
    print(f"Fichier généré : {output_path} (séparateur virgule)")


if __name__ == "__main__":
    main()