# src/fetcher.py
import os
import pandas as pd
import requests
from datetime import datetime, timedelta
from typing import Optional, List
import json
import boto3
import time

API_HOST = "https://aerodatabox.p.rapidapi.com"
HEADERS = {
    "X-RapidAPI-Key": os.getenv("X_RAPIDAPI_KEY"),
    "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com",
}

# Paramètres fixes pour l'API Aerodatabox
API_PARAMS = {
    "withLeg": "true",
    "direction": "Both",
    "withCancelled": "true",
    "withCodeshared": "true",
    "withCargo": "true",
    "withPrivate": "false",
}

AEROPORTS = {"LFPG": "CDG", "LFPO": "ORY", "LFMN": "NCE", "LFLL": "LYS", "LFML": "MRS"}


# src/fetcher.py
def extraire_vols(donnees_json, icao):
    """Parsing adapté à la structure de l'API"""
    if not donnees_json:
        return pd.DataFrame()

    # on va juste créer une colonne calculée pour recupérer directement le retard de chaque avion
    rows = []
    # l'API AeroDataBox renvoie deux listes séparées dans le JSON : "departures" et "arrivals"
    for mov_type in ["departures", "arrivals"]:
        for flight in donnees_json.get(mov_type, []):
            departure = flight.get("departure", {})
            arrival = flight.get("arrival", {})

            scheduledTime = departure.get("scheduledTime", {}).get("utc")
            revisedTime = departure.get("revisedTime", {}).get("utc")
            runwayTime = departure.get("runwayTime", {}).get("utc")

            delay_min = None
            if scheduledTime and revisedTime:
                try:
                    s = datetime.fromisoformat(scheduledTime.replace("Z", "+00:00"))
                    r = datetime.fromisoformat(revisedTime.replace("Z", "+00:00"))
                    delay_min = round((r - s).total_seconds() / 60, 1)
                except:
                    pass

            row = {
                # ajout de l'icao pour pouvoir faire le lien avec les autres tables en base de données
                "icao": icao,
                "type": "departure" if mov_type == "departures" else "arrival",
                "flight_number": flight.get("number"),
                "status": flight.get("status"),
                "airline": flight.get("airline", {}).get("name"),
                "scheduled_utc": scheduledTime,
                "revised_utc": revisedTime,
                "runway_utc": runwayTime,
                # Ajout de la colonne calculée simple
                "delay_minutes": delay_min,
                # chaque liste dans l'API a le meme nom de colonne pour depart
                "terminal_dep": departure.get("terminal"),
                "terminal_arr": arrival.get("terminal"),
                "destination_icao": arrival.get("airport", {}).get("icao")
                if mov_type == "departures"
                else None,
            }
            rows.append(row)

    return pd.DataFrame(rows)


def fetch_real_flights_from_aerodatabox(
    date_str: str,
    icao: Optional[str] = None,
) -> pd.DataFrame:
    """
    Récupère les vols réels depuis Aerodatabox et les parse correctement.
    """
    if not os.getenv("X_RAPIDAPI_KEY"):
        raise ValueError(" X_RAPIDAPI_KEY non définie")

    all_rows = []
    current_date = datetime.strptime(date_str, "%Y-%m-%d")

    start_time = current_date.replace(hour=0, minute=0, second=0)
    end_time = current_date.replace(hour=23, minute=59, second=59)

    airports_to_fetch = [icao] if icao else list(AEROPORTS.keys())

    print(
        f" Récupération vols réels du {date_str} pour {len(airports_to_fetch)} aéroport(s)"
    )

    for apt in airports_to_fetch:
        current_start = start_time
        tranche = 1

        while current_start < end_time:
            current_end = min(current_start + timedelta(hours=12), end_time)

            start_str = current_start.strftime("%Y-%m-%dT%H:%M")
            end_str = current_end.strftime("%Y-%m-%dT%H:%M")

            url = f"{API_HOST}/flights/airports/icao/{apt}/{start_str}/{end_str}?"
            url += "withLeg=true&direction=Both&withCancelled=true&withCodeshared=true&withCargo=false&withPrivate=false&withLocation=false"

            print(f"   📡 Tranche {tranche} | {apt} | {start_str} → {end_str}")

            try:
                response = requests.get(url, headers=HEADERS, timeout=45)
                print(f"   Status Code : {response.status_code}")

                if response.status_code == 200:
                    data = response.json()

                    # Utilisation de ta fonction de parsing existante
                    df_tranche = extraire_vols(data, apt)

                    if not df_tranche.empty:
                        print(f"    {len(df_tranche)} vols parsés dans cette tranche")
                        all_rows.append(df_tranche)
                    else:
                        print("   → Aucun vol dans cette tranche")

                else:
                    print(f"    Erreur {response.status_code} : {response.text[:300]}")

            except Exception as e:
                print(f"    Exception : {type(e).__name__} - {e}")

            current_start = current_end
            tranche += 1
            time.sleep(1)

    if all_rows:
        df_final = pd.concat(all_rows, ignore_index=True)
    else:
        df_final = pd.DataFrame()

    print(f" TOTAL : {len(df_final)} vols réels récupérés pour le {date_str}\n")
    return df_final


def fetch_all_movements_historical(
    date_str: str, icao_list: Optional[List[str]] = None
) -> dict:
    """
    Récupère tous les vols historiques pour les aéroports demandés
    et sauvegarde directement un seul fichier JSON brut sur S3.
    """
    if icao_list is None:
        icao_list = list(AEROPORTS.keys())

    full_data = {
        "date": date_str,
        "retrieved_at": datetime.now().isoformat(),
        "airports": {},
        "metadata": {"total_airports": len(icao_list), "total_flights": 0},
    }

    print(
        f" Début récupération historique pour le {date_str} ({len(icao_list)} aéroports)"
    )

    for icao in icao_list:
        print(f"    Traitement aéroport : {icao} ({AEROPORTS.get(icao, icao)})")

        airport_data = {"departures": [], "arrivals": []}

        current_date = datetime.strptime(date_str, "%Y-%m-%d")
        start_time = current_date.replace(hour=0, minute=0, second=0)
        end_time = current_date.replace(hour=23, minute=59, second=59)

        current_start = start_time
        tranche = 1

        while current_start < end_time:
            current_end = min(current_start + timedelta(hours=12), end_time)

            start_str = current_start.strftime("%Y-%m-%dT%H:%M")
            end_str = current_end.strftime("%Y-%m-%dT%H:%M")

            url = f"{API_HOST}/flights/airports/icao/{icao}/{start_str}/{end_str}?"
            url += "withLeg=true&direction=Both&withCancelled=true&withCodeshared=true&withCargo=false&withPrivate=false&withLocation=false"

            print(f"      Tranche {tranche}: {start_str} → {end_str}")

            try:
                response = requests.get(url, headers=HEADERS, timeout=40)

                if response.status_code == 200:
                    data = response.json()
                    departures = data.get("departures", [])
                    arrivals = data.get("arrivals", [])

                    airport_data["departures"].extend(departures)
                    airport_data["arrivals"].extend(arrivals)

                    print(
                        f"          {len(departures)} départs + {len(arrivals)} arrivées"
                    )
                else:
                    print(
                        f"          Status {response.status_code} - {response.text[:200]}"
                    )

            except Exception as e:
                print(f"          Exception : {e}")

            current_start = current_end
            tranche += 1
            time.sleep(1)

        full_data["airports"][icao] = airport_data
        full_data["metadata"]["total_flights"] += len(airport_data["departures"]) + len(
            airport_data["arrivals"]
        )

    # ====================== SAUVEGARDE SUR S3 ======================
    try:
        output_key = f"raw/movements_historical_{date_str}.json"
        s3_full_path = f"s3://ppml2026/{output_key}"

        # Conversion en JSON string
        json_data = json.dumps(full_data, ensure_ascii=False, indent=2)

        # Sauvegarde via boto3 (plus fiable pour gros JSON)
        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION", "eu-north-1"),
        )

        s3.put_object(
            Bucket="ppml2026",
            Key=output_key,
            Body=json_data,
            ContentType="application/json",
        )

        print(f" Fichier historique sauvegardé avec succès sur S3 : {output_key}")
        print(f"   Total vols : {full_data['metadata']['total_flights']}")

    except Exception as e:
        print(f" Erreur lors de la sauvegarde sur S3 : {e}")

    return full_data


def load_historical_json_to_dataframe(s3_key: str) -> pd.DataFrame:
    """
    Charge un fichier JSON brut movements_historical depuis S3
    et le transforme en DataFrame plat (une ligne par vol).
    """
    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION", "eu-north-1"),
        )

        # Téléchargement du fichier JSON depuis S3
        full_key = f"raw/{s3_key}"
        response = s3.get_object(Bucket="ppml2026", Key=full_key)
        json_data = json.loads(response["Body"].read().decode("utf-8"))

        rows = []

        # Parcours de la structure : date → airport → departures/arrivals
        for date, airports_data in json_data.get("airports", {}).items():
            for icao, airport_data in airports_data.items():
                # Départs
                for flight in airport_data.get("departures", []):
                    row = parse_flight(flight, icao, date, "departure")
                    if row:
                        rows.append(row)
                # Arrivées
                for flight in airport_data.get("arrivals", []):
                    row = parse_flight(flight, icao, date, "arrival")
                    if row:
                        rows.append(row)

        df = pd.DataFrame(rows)
        print(f" Chargé {len(df)} vols depuis {s3_key}")
        return df

    except Exception as e:
        print(f" Erreur lors du chargement du JSON : {e}")
        return pd.DataFrame()


def parse_flight(flight: dict, icao: str, date: str, mov_type: str) -> dict:
    """
    Parse un vol complet (departure ou arrival) en récupérant un maximum de champs utiles.
    """
    departure = flight.get("departure", {})
    arrival = flight.get("arrival", {})
    aircraft = flight.get("aircraft", {})
    airline = flight.get("airline", {})

    # Calcul du retard (si possible)
    delay_minutes = None
    scheduled_utc = None
    revised_utc = None

    if mov_type == "departure":
        scheduled = departure.get("scheduledTime", {})
        revised = departure.get("revisedTime", {})
        scheduled_utc = scheduled.get("utc")
        revised_utc = revised.get("utc")
    else:  # arrival
        scheduled = arrival.get("scheduledTime", {})
        revised = arrival.get("revisedTime", {})
        scheduled_utc = scheduled.get("utc")
        revised_utc = revised.get("utc")

    if scheduled_utc and revised_utc:
        try:
            s = datetime.fromisoformat(scheduled_utc.replace("Z", "+00:00"))
            r = datetime.fromisoformat(revised_utc.replace("Z", "+00:00"))
            delay_minutes = round((r - s).total_seconds() / 60, 1)
        except:
            pass

    return {
        "date": date,
        "icao": icao,
        "type": mov_type,
        # Informations vol
        "flight_number": flight.get("number"),
        "call_sign": flight.get("callSign"),
        "status": flight.get("status"),
        "codeshare_status": flight.get("codeshareStatus"),
        "is_cargo": flight.get("isCargo", False),
        # Horaires
        "scheduled_utc": scheduled_utc,
        "revised_utc": revised_utc,
        "runway_utc": departure.get("runwayTime", {}).get("utc")
        if mov_type == "departure"
        else arrival.get("runwayTime", {}).get("utc"),
        # Delay
        "delay_minutes": delay_minutes,
        # Terminaux et portes
        "terminal_dep": departure.get("terminal"),
        "terminal_arr": arrival.get("terminal"),
        "gate_dep": departure.get("gate"),
        "baggage_belt": arrival.get("baggageBelt"),
        # Aéroport de destination / origine
        "destination_icao": arrival.get("airport", {}).get("icao")
        if mov_type == "departure"
        else None,
        "destination_iata": arrival.get("airport", {}).get("iata")
        if mov_type == "departure"
        else None,
        "destination_name": arrival.get("airport", {}).get("name")
        if mov_type == "departure"
        else None,
        # Compagnie
        "airline_name": airline.get("name"),
        "airline_iata": airline.get("iata"),
        "airline_icao": airline.get("icao"),
        # Avion
        "aircraft_model": aircraft.get("model"),
        "aircraft_reg": aircraft.get("reg"),
        "aircraft_mode_s": aircraft.get("modeS"),
        # Qualité des données
        "quality_dep": departure.get("quality"),
        "quality_arr": arrival.get("quality"),
    }


def fetch_and_save_meteo_raw(
    coord_aeroport: dict,
    start_date: datetime,
    end_date: datetime,
    is_future: bool = False,
    output_key: str = None,
) -> str:
    """
    Récupère la météo via Open-Meteo et sauvegarde le JSON brut sur S3.
    Retourne la clé S3 du fichier sauvegardé.
    """
    all_data = {}

    for airport_code, (lat, lon) in coord_aeroport.items():
        print(f" Récupération météo pour {airport_code} ({lat}, {lon})")

        url = (
            "https://api.open-meteo.com/v1/forecast"
            if is_future
            else "https://archive-api.open-meteo.com/v1/archive"
        )

        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,wind_gusts_10m,pressure_msl,precipitation,cloud_cover",
            "timezone": "Europe/Paris",
        }

        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            all_data[airport_code] = data
            print(
                f"   → {len(data.get('hourly', {}).get('time', []))} heures récupérées"
            )
        except Exception as e:
            print(f"    Erreur pour {airport_code}: {e}")

        time.sleep(1.1)  # Rate limit respectueux

    # Sauvegarde JSON brut sur S3
    if not output_key:
        date_str = (
            start_date.strftime("%Y-%m-%d") + "_to_" + end_date.strftime("%Y-%m-%d")
        )
        prefix = "meteo_future" if is_future else "meteo_historical"
        output_key = f"raw/{prefix}_{date_str}.json"

    s3_full_path = f"s3://ppml2026/{output_key}"

    # Sauvegarde via boto3 (plus fiable que to_json)
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        # region_name=os.getenv("AWS_DEFAULT_REGION", "eu-north-1")
    )
    s3.put_object(
        Bucket="ppml2026",
        Key=f"{output_key}",
        Body=json.dumps(all_data, default=str),
        ContentType="application/json",
    )

    print(f" Météo brute sauvegardée → {output_key}")
    return output_key


def load_meteo_json_to_dataframe(s3_key: str) -> pd.DataFrame:
    """
    Charge le JSON météo brut depuis S3 et retourne un DataFrame plat
    (une ligne par heure et par aéroport).
    """
    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION", "eu-north-1"),
        )

        # Construction du Key complet
        full_key = f"raw/{s3_key}"

        # Téléchargement depuis S3
        response = s3.get_object(Bucket="ppml2026", Key=full_key)
        content = response["Body"].read().decode("utf-8")
        data = json.loads(content)

        print(f" JSON météo chargé depuis S3 : {full_key}")
        print(f"   Nombre d'aéroports : {len(data)}")

        rows = []
        for icao, meteo_json in data.items():
            if "hourly" in meteo_json:
                hourly = meteo_json["hourly"]
                df_hourly = pd.DataFrame(hourly)
                df_hourly["time"] = pd.to_datetime(df_hourly["time"])
                df_hourly["icao"] = icao
                rows.append(df_hourly)

        if not rows:
            print(" Aucune donnée 'hourly' trouvée dans le JSON météo")
            return pd.DataFrame()

        df_meteo = pd.concat(rows, ignore_index=True)

        print(
            f" Transformation réussie : {df_meteo.shape[0]:,} lignes | "
            f"{df_meteo['icao'].nunique()} aéroports"
        )

        return df_meteo

    except Exception as e:
        print(f" Erreur chargement météo JSON {s3_key} : {e}")
        raise
