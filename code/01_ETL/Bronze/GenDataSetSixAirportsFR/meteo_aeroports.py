#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

"""
Génère un CSV météo horaire sur les 210 derniers jours pour :
CDG, ORLY, NICE, TOULOUSE, LYON, MARSEILLE

Source principale : Open-Meteo Historical Weather API
- pas de clé API
- données horaires historiques
- timezone=UTC pour avoir une base uniforme

Colonnes générées :
time
icao
relative_humidity_2m
dew_point
icing_conditions
rain
freezing_rain
snow
thunderstorms
pressure_msl
wind_shear
wind_gusts_10m
wind_speed_10m
wind_direction_10m
precipitation
has_precipitation
fog
humidity
temperature_2m
cloud_cover
cloud_base
visibility

Remarques importantes :
- Open-Meteo fournit directement la plupart des variables historiques.
- Certaines colonnes demandées ne sont pas disponibles telles quelles dans l'API historique
  et sont donc dérivées :
    * humidity = relative_humidity_2m
    * fog, freezing_rain, thunderstorms = dérivés de weather_code (codes WMO)
    * icing_conditions = heuristique opérationnelle
    * wind_shear = proxy de cisaillement entre 10m et 100m
- visibility n'est pas disponible dans l'API historique Open-Meteo : la colonne est laissée vide.
  Si tu veux, on pourra la compléter avec une 2e source dédiée.

Installation :
    pip install requests pandas

Exécution :
    python meteo_aeroports.py

Sortie :
    meteo_aeroports_210_derniers_jours.csv
"""


import math
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
import os


AIRPORTS = {
    "CDG": {"name": "Paris Charles de Gaulle", "lat": 49.0097, "lon": 2.5479},
    "ORLY": {"name": "Paris Orly", "lat": 48.7262, "lon": 2.3652},
    "NICE": {"name": "Nice Côte d'Azur", "lat": 43.6584, "lon": 7.2159},
    "TOULOUSE": {"name": "Toulouse-Blagnac", "lat": 43.6293, "lon": 1.3630},
    "LYON": {"name": "Lyon Saint-Exupéry", "lat": 45.7256, "lon": 5.0811},
    "MARSEILLE": {"name": "Marseille Provence", "lat": 43.4393, "lon": 5.2214},
}

OUTPUT_CSV = "meteo_aeroports_210_derniers_jours.csv"
BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "rain",
    "snowfall",
    "pressure_msl",
    "wind_gusts_10m",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_speed_100m",
    "wind_direction_100m",
    "precipitation",
    "cloud_cover",
    "cloud_base",
    "weather_code",
]

FOG_CODES = {45, 48}
FREEZING_RAIN_CODES = {56, 57, 66, 67}
THUNDERSTORM_CODES = {95, 96, 97, 98, 99}


def build_dates() -> tuple[str, str]:
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=209)
    return start_date.isoformat(), end_date.isoformat()


def wind_to_uv(speed_kmh, direction_deg):
    if pd.isna(speed_kmh) or pd.isna(direction_deg):
        return None, None
    rad = math.radians(direction_deg)
    u = -speed_kmh * math.sin(rad)
    v = -speed_kmh * math.cos(rad)
    return u, v


def compute_wind_shear(row):
    u10, v10 = wind_to_uv(row.get("wind_speed_10m"), row.get("wind_direction_10m"))
    u100, v100 = wind_to_uv(row.get("wind_speed_100m"), row.get("wind_direction_100m"))
    if any(v is None for v in [u10, v10, u100, v100]):
        return None
    return round(math.sqrt((u100 - u10) ** 2 + (v100 - v10) ** 2), 2)


def compute_icing_conditions(row):
    t = row.get("temperature_2m")
    rh = row.get("relative_humidity_2m")
    precip = row.get("precipitation")
    dew = row.get("dew_point_2m")
    weather_code = row.get("weather_code")

    if pd.isna(t):
        return "Non"

    cond_temp = t <= 2.0
    cond_humidity = (not pd.isna(rh) and rh >= 90)
    cond_precip = (not pd.isna(precip) and precip > 0)
    cond_dew_close = (not pd.isna(dew) and abs(t - dew) <= 1.0)
    cond_fog_or_freezing = bool((not pd.isna(weather_code)) and weather_code in FOG_CODES.union(FREEZING_RAIN_CODES))

    return "Oui" if cond_temp and (cond_humidity or cond_precip or cond_dew_close or cond_fog_or_freezing) else "Non"


def fetch_airport_weather(icao, lat, lon, start_date, end_date):
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(HOURLY_VARS),
        "timezone": "UTC",
        "wind_speed_unit": "kmh",
        "precipitation_unit": "mm",
        "temperature_unit": "celsius",
    }

    resp = requests.get(BASE_URL, params=params, timeout=120)
    resp.raise_for_status()
    data = resp.json()

    if "hourly" not in data:
        raise ValueError(f"Aucune donnée horaire renvoyée pour {icao}: {data}")

    df = pd.DataFrame(data["hourly"])
    df = df.rename(columns={"dew_point_2m": "dew_point", "snowfall": "snow"})

    df["icao"] = icao
    df["humidity"] = df["relative_humidity_2m"]
    df["has_precipitation"] = df["precipitation"].fillna(0).gt(0).map({True: "Oui", False: "Non"})
    df["fog"] = df["weather_code"].isin(FOG_CODES).map({True: "Oui", False: "Non"})
    df["freezing_rain"] = df["weather_code"].isin(FREEZING_RAIN_CODES).map({True: "Oui", False: "Non"})
    df["thunderstorms"] = df["weather_code"].isin(THUNDERSTORM_CODES).map({True: "Oui", False: "Non"})
    df["icing_conditions"] = df.apply(compute_icing_conditions, axis=1)
    df["wind_shear"] = df.apply(compute_wind_shear, axis=1)

    df["visibility"] = pd.NA

    final_columns = [
        "time",
        "icao",
        "relative_humidity_2m",
        "dew_point",
        "icing_conditions",
        "rain",
        "freezing_rain",
        "snow",
        "thunderstorms",
        "pressure_msl",
        "wind_shear",
        "wind_gusts_10m",
        "wind_speed_10m",
        "wind_direction_10m",
        "precipitation",
        "has_precipitation",
        "fog",
        "humidity",
        "temperature_2m",
        "cloud_cover",
        "cloud_base",
        "visibility",
    ]

    for col in final_columns:
        if col not in df.columns:
            df[col] = pd.NA

    return df[final_columns]


def fetch_airport_weather_forecast(icao, lat, lon, start_date, end_date):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(HOURLY_VARS),
        "timezone": "UTC",
        "wind_speed_unit": "kmh",
        "precipitation_unit": "mm",
        "temperature_unit": "celsius",
    }
    resp = requests.get(url, params=params, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    if "hourly" not in data:
        raise ValueError(f"Aucune donnée horaire renvoyée pour {icao}: {data}")
    df = pd.DataFrame(data["hourly"])
    df = df.rename(columns={"dew_point_2m": "dew_point", "snowfall": "snow"})
    df["icao"] = icao
    df["humidity"] = df["relative_humidity_2m"]
    df["has_precipitation"] = df["precipitation"].fillna(0).gt(0).map({True: "Oui", False: "Non"})
    df["fog"] = df["weather_code"].isin(FOG_CODES).map({True: "Oui", False: "Non"})
    df["freezing_rain"] = df["weather_code"].isin(FREEZING_RAIN_CODES).map({True: "Oui", False: "Non"})
    df["thunderstorms"] = df["weather_code"].isin(THUNDERSTORM_CODES).map({True: "Oui", False: "Non"})
    df["icing_conditions"] = df.apply(compute_icing_conditions, axis=1)
    df["wind_shear"] = df.apply(compute_wind_shear, axis=1)
    df["visibility"] = pd.NA
    final_columns = [
        "time", "icao", "relative_humidity_2m", "dew_point", "icing_conditions", "rain", "freezing_rain", "snow", "thunderstorms", "pressure_msl", "wind_shear", "wind_gusts_10m", "wind_speed_10m", "wind_direction_10m", "precipitation", "has_precipitation", "fog", "humidity", "temperature_2m", "cloud_cover", "cloud_base", "visibility"
    ]
    for col in final_columns:
        if col not in df.columns:
            df[col] = pd.NA
    return df[final_columns]


def main():
    import os
    # Crée le dossier OutputDataMeteo s'il n'existe pas
    output_dir = "OutputDataMeteo"
    os.makedirs(output_dir, exist_ok=True)
    # Supprime tous les fichiers du dossier OutputDataMeteo
    for f in os.listdir(output_dir):
        file_path = os.path.join(output_dir, f)
        if os.path.isfile(file_path):
            os.remove(file_path)

    start_date, end_date = build_dates()

    icao_map = {
        "MARSEILLE": "MRS",
        "ORLY": "ORY",
        "NICE": "NCE",
        "TOULOUSE": "TLS",
        "LYON": "LYS",
    }
    all_dfs = []
    for icao, info in AIRPORTS.items():
        print(f"Récupération {icao} ({info['name']})...")
        df = fetch_airport_weather(
            icao=icao,
            lat=info["lat"],
            lon=info["lon"],
            start_date=start_date,
            end_date=end_date,
        )
        # Remplacement immédiat du code ICAO par IATA dans la colonne icao
        df["icao"] = df["icao"].replace(icao_map)
        all_dfs.append(df)

    final_df = pd.concat(all_dfs, ignore_index=True)

    final_df["time"] = pd.to_datetime(final_df["time"], utc=True, errors="coerce")
    final_df = final_df.sort_values(["icao", "time"]).reset_index(drop=True)
    output_hist = os.path.join(output_dir, "meteo_aeroports_210_derniers_jours.csv")
    final_df.to_csv(output_hist, sep=";", index=False, encoding="utf-8-sig")
    print(f"[OK] Fichier généré : {output_hist}")
    print(f"[OK] Nombre total de lignes : {len(final_df)}")


    # Génération du fichier prévisions 7 jours
    forecast_start = (datetime.now(timezone.utc) + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    forecast_start_str = forecast_start.strftime("%Y-%m-%d")
    forecast_end_future = forecast_start + timedelta(days=7)
    forecast_end_future_str = forecast_end_future.strftime("%Y-%m-%d")
    all_forecast_dfs_future = []
    for icao, info in AIRPORTS.items():
        print(f"Prévisions {icao} ({info['name']}) 7 jours...")
        df = fetch_airport_weather_forecast(
            icao=icao,
            lat=info["lat"],
            lon=info["lon"],
            start_date=forecast_start_str,
            end_date=forecast_end_future_str,
        )
        df["icao"] = df["icao"].replace(icao_map)
        all_forecast_dfs_future.append(df)

    forecast_df_future = pd.concat(all_forecast_dfs_future, ignore_index=True)
    forecast_df_future["time"] = pd.to_datetime(forecast_df_future["time"], utc=True, errors="coerce")
    forecast_df_future = forecast_df_future.sort_values(["icao", "time"]).reset_index(drop=True)
    output_forecast_future = os.path.join(output_dir, "meteo_aeroports_future.csv")
    forecast_df_future.to_csv(output_forecast_future, sep=";", index=False, encoding="utf-8-sig")
    print(f"[OK] Fichier généré : {output_forecast_future}")
    print(f"[OK] Nombre total de lignes (prévisions 7 jours) : {len(forecast_df_future)}")


if __name__ == "__main__":
    main()
