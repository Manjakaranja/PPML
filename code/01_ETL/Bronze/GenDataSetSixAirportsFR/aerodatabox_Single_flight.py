#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import sys
import time
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()


# ============================================================
# OUTIL : découpage de période en tranches de 12h
# ============================================================

def daterange_12h(start_date: datetime, end_date: datetime):
    current = start_date
    while current < end_date:
        next_ = min(current + timedelta(hours=12), end_date)
        yield current, next_
        current = next_


# ============================================================
# CONFIGURATION
# ============================================================

API_BASE_URL = "https://prod.api.market/api/v1/aedbx/aerodatabox"

PARIS_AIRPORTS = {"CDG", "ORY"}
REGIONAL_AIRPORTS = {"LYS", "NCE", "MRS", "TLS"}
ALL_AIRPORTS = sorted(PARIS_AIRPORTS | REGIONAL_AIRPORTS)

REQUEST_TIMEOUT = 60
REQUEST_SLEEP_SECONDS = 0.4
DAYS_BACK = 1

MAX_RETRIES = 5
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
BACKOFF_FACTOR = 2.0
INITIAL_BACKOFF_SECONDS = 1.5


# ============================================================
# OUTILS
# ============================================================

def env_or_fail(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variable d'environnement manquante : {name}")
    return value


def build_headers() -> Dict[str, str]:
    api_key = env_or_fail("API_KEY")
    return {
        "Accept": "application/json",
        "x-magicapi-key": api_key,
        "x-api-market-key": api_key,
    }


def get_date_range(days_back: int = 5) -> Tuple[str, str]:
    end_date = date.today()
    start_date = end_date - timedelta(days=days_back - 1)
    return start_date.isoformat(), end_date.isoformat()


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def compute_delay_and_advance_minutes(
    reference_value: Optional[str],
    scheduled_value: Optional[str],
) -> Tuple[Optional[int], Optional[int]]:
    reference_dt = parse_dt(reference_value)
    scheduled_dt = parse_dt(scheduled_value)

    if not reference_dt or not scheduled_dt:
        return None, None

    diff_min = int((reference_dt - scheduled_dt).total_seconds() / 60)

    if diff_min >= 0:
        return diff_min, 0

    return 0, abs(diff_min)


def get_local_time(block: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(block, dict):
        return None
    return block.get("local")


def parse_retry_after(response: requests.Response) -> Optional[float]:
    retry_after = response.headers.get("Retry-After")
    if not retry_after:
        return None
    try:
        return float(retry_after)
    except ValueError:
        return None


def normalize_status(status: Optional[str]) -> str:
    return (status or "").strip().lower()


def choose_actual_or_fallback(
    actual_value: Optional[str],
    revised_value: Optional[str],
    status: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    if actual_value:
        return actual_value, "actualTime"

    s = normalize_status(status)
    if s in {"arrived", "departed", "landed"} and revised_value:
        return revised_value, "revisedTime"

    return None, None


def is_target_route(origin: Optional[str], destination: Optional[str]) -> bool:
    if not origin or not destination:
        return False

    return (
        (origin in PARIS_AIRPORTS and destination in REGIONAL_AIRPORTS)
        or
        (origin in REGIONAL_AIRPORTS and destination in PARIS_AIRPORTS)
    )


def is_domestic_france_route(origin: Optional[str], destination: Optional[str]) -> bool:
    if not origin or not destination:
        return False

    return origin in ALL_AIRPORTS and destination in ALL_AIRPORTS


def is_any_route_within_scope(origin: Optional[str], destination: Optional[str]) -> bool:
    return bool(origin in ALL_AIRPORTS or destination in ALL_AIRPORTS)


def is_route_of_interest(origin: Optional[str], destination: Optional[str]) -> bool:
    return is_target_route(origin, destination)


# ============================================================
# API AERODATABOX
# ============================================================

def request_with_retries(
    method: str,
    url: str,
    *,
    headers: Dict[str, str],
    params: Optional[Dict[str, Any]] = None,
    timeout: int = REQUEST_TIMEOUT,
    max_retries: int = MAX_RETRIES,
) -> requests.Response:
    last_exception: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                timeout=timeout,
            )

            if response.status_code in RETRY_STATUS_CODES:
                if attempt == max_retries:
                    return response

                retry_after_seconds = parse_retry_after(response)
                if retry_after_seconds is None:
                    retry_after_seconds = INITIAL_BACKOFF_SECONDS * (BACKOFF_FACTOR ** (attempt - 1))

                print(
                    f"   ! HTTP {response.status_code} sur {url} - tentative {attempt}/{max_retries}, "
                    f"nouvel essai dans {retry_after_seconds:.1f}s",
                    file=sys.stderr,
                )
                time.sleep(retry_after_seconds)
                continue

            return response

        except (requests.Timeout, requests.ConnectionError) as exc:
            last_exception = exc

            if attempt == max_retries:
                raise RuntimeError(f"Erreur réseau après {max_retries} tentatives : {exc}") from exc

            sleep_seconds = INITIAL_BACKOFF_SECONDS * (BACKOFF_FACTOR ** (attempt - 1))
            print(
                f"   ! Erreur réseau ({exc.__class__.__name__}) - tentative {attempt}/{max_retries}, "
                f"nouvel essai dans {sleep_seconds:.1f}s",
                file=sys.stderr,
            )
            time.sleep(sleep_seconds)

    if last_exception:
        raise RuntimeError(f"Échec requête : {last_exception}") from last_exception

    raise RuntimeError("Échec requête sans détail disponible.")


def fetch_airport_fids(iata_code: str, from_local: str, to_local: str) -> Dict[str, Any]:
    url = f"{API_BASE_URL}/flights/airports/Iata/{iata_code}/{from_local}/{to_local}"

    params = {
        "withLeg": "true",
        "withCancelled": "true",
        "withCodeshared": "true",
        "withCargo": "true",
        "withPrivate": "true",
    }

    response = request_with_retries(
        "GET",
        url,
        headers=build_headers(),
        params=params,
        timeout=REQUEST_TIMEOUT,
        max_retries=MAX_RETRIES,
    )

    if response.status_code == 204:
        return {"departures": [], "arrivals": []}

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise RuntimeError(
            f"Erreur API pour {iata_code} [{response.status_code}] : {response.text[:500]}"
        ) from exc

    try:
        data = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Réponse JSON invalide pour {iata_code}") from exc

    if not isinstance(data, dict):
        raise RuntimeError(f"Réponse inattendue pour {iata_code}: {type(data)}")

    return data


# ============================================================
# EXTRACTION DES DONNÉES - VERSION GENERALE
# ============================================================

def extract_departure_record_all(dep: Dict[str, Any], queried_airport: str) -> Optional[Dict[str, Any]]:
    departure = dep.get("departure") or {}
    arrival = dep.get("arrival") or {}
    airline = dep.get("airline") or {}

    status = dep.get("status")

    origin = queried_airport
    destination_airport = arrival.get("airport") or {}
    destination = destination_airport.get("iata")

    scheduled_departure = get_local_time(departure.get("scheduledTime"))
    revised_departure = get_local_time(departure.get("revisedTime"))
    raw_actual_departure = get_local_time(departure.get("actualTime"))

    scheduled_arrival = get_local_time(arrival.get("scheduledTime"))
    revised_arrival = get_local_time(arrival.get("revisedTime"))
    raw_actual_arrival = get_local_time(arrival.get("actualTime"))

    estimated_departure = revised_departure or scheduled_departure
    estimated_arrival = revised_arrival or scheduled_arrival

    actual_departure, actual_source_departure = choose_actual_or_fallback(
        raw_actual_departure, revised_departure, status
    )
    actual_arrival, actual_source_arrival = choose_actual_or_fallback(
        raw_actual_arrival, revised_arrival, status
    )

    departure_delay_min, departure_advance_min = compute_delay_and_advance_minutes(
        actual_departure, scheduled_departure
    )
    arrival_delay_min, arrival_advance_min = compute_delay_and_advance_minutes(
        actual_arrival, scheduled_arrival
    )

    movement_date = scheduled_departure[:10] if scheduled_departure else None

    # S'assure que le champ codeshares est bien extrait dans extract_departure_record_all
    # et que flight_number_principal/codeshare sont toujours présents
    return {
        "flight_date": scheduled_departure[:10] if scheduled_departure else (
            scheduled_arrival[:10] if scheduled_arrival else None
        ),
        "movement_date": movement_date,
        "flight_number": dep.get("number"),
        "airline": airline.get("name"),
        "airport_origin": origin,
        "airport_destination": destination,
        "terminal_departure": departure.get("terminal"),
        "terminal_arrival": arrival.get("terminal"),
        "scheduled_departure": scheduled_departure,
        "scheduled_arrival": scheduled_arrival,
        "estimated_departure": estimated_departure,
        "estimated_arrival": estimated_arrival,
        "actual_departure": actual_departure,
        "actual_arrival": actual_arrival,
        "actual_source_departure": actual_source_departure,
        "actual_source_arrival": actual_source_arrival,
        "departure_delay_min": departure_delay_min,
        "departure_advance_min": departure_advance_min,
        "arrival_delay_min": arrival_delay_min,
        "arrival_advance_min": arrival_advance_min,
        "status": status,
        "movement_type": "departure",
        "codeshares": dep.get("codeshares", []),
    }


def extract_arrival_record_all(arr: Dict[str, Any], queried_airport: str) -> Optional[Dict[str, Any]]:
    departure = arr.get("departure") or {}
    arrival = arr.get("arrival") or {}
    airline = arr.get("airline") or {}

    status = arr.get("status")

    departure_airport = departure.get("airport") or {}
    origin = departure_airport.get("iata")
    destination = queried_airport

    scheduled_departure = get_local_time(departure.get("scheduledTime"))
    revised_departure = get_local_time(departure.get("revisedTime"))
    raw_actual_departure = get_local_time(departure.get("actualTime"))

    scheduled_arrival = get_local_time(arrival.get("scheduledTime"))
    revised_arrival = get_local_time(arrival.get("revisedTime"))
    raw_actual_arrival = get_local_time(arrival.get("actualTime"))

    estimated_departure = revised_departure or scheduled_departure
    estimated_arrival = revised_arrival or scheduled_arrival

    actual_departure, actual_source_departure = choose_actual_or_fallback(
        raw_actual_departure, revised_departure, status
    )
    actual_arrival, actual_source_arrival = choose_actual_or_fallback(
        raw_actual_arrival, revised_arrival, status
    )

    departure_delay_min, departure_advance_min = compute_delay_and_advance_minutes(
        actual_departure, scheduled_departure
    )
    arrival_delay_min, arrival_advance_min = compute_delay_and_advance_minutes(
        actual_arrival, scheduled_arrival
    )

    movement_date = scheduled_arrival[:10] if scheduled_arrival else None

    return {
        "flight_date": scheduled_departure[:10] if scheduled_departure else (
            scheduled_arrival[:10] if scheduled_arrival else None
        ),
        "movement_date": movement_date,
        "flight_number": arr.get("number"),
        "airline": airline.get("name"),
        "airport_origin": origin,
        "airport_destination": destination,
        "terminal_departure": departure.get("terminal"),
        "terminal_arrival": arrival.get("terminal"),
        "scheduled_departure": scheduled_departure,
        "scheduled_arrival": scheduled_arrival,
        "estimated_departure": estimated_departure,
        "estimated_arrival": estimated_arrival,
        "actual_departure": actual_departure,
        "actual_arrival": actual_arrival,
        "actual_source_departure": actual_source_departure,
        "actual_source_arrival": actual_source_arrival,
        "departure_delay_min": departure_delay_min,
        "departure_advance_min": departure_advance_min,
        "arrival_delay_min": arrival_delay_min,
        "arrival_advance_min": arrival_advance_min,
        "status": status,
        "movement_type": "arrival",
    }


# ============================================================
# EXTRACTION DES DONNÉES - CSV PRINCIPAL
# ============================================================

def extract_departure_record(dep: Dict[str, Any], queried_airport: str) -> Optional[Dict[str, Any]]:
    row = extract_departure_record_all(dep, queried_airport)
    if not row:
        return None

    if not is_route_of_interest(row.get("airport_origin"), row.get("airport_destination")):
        return None

    return row


def extract_arrival_record(arr: Dict[str, Any], queried_airport: str) -> Optional[Dict[str, Any]]:
    row = extract_arrival_record_all(arr, queried_airport)
    if not row:
        return None

    if not is_route_of_interest(row.get("airport_origin"), row.get("airport_destination")):
        return None

    return row


# ============================================================
# FUSION DES DÉPARTS / ARRIVÉES
# ============================================================

def merge_values(left: Any, right: Any) -> Any:
    return left if left not in (None, "", []) else right


def make_merge_key(row: Dict[str, Any]) -> Tuple[Any, ...]:
    return (
        row.get("flight_number"),
        row.get("airline"),
        row.get("flight_date"),
        row.get("scheduled_departure"),
        row.get("scheduled_arrival"),
    )


def merge_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[Tuple[Any, ...], Dict[str, Any]] = {}

    for row in rows:
        key = make_merge_key(row)

        if key not in merged:
            merged[key] = dict(row)
            continue

        current = merged[key]

        for col in [
            "flight_date",
            "movement_date",
            "flight_number",
            "airline",
            "airport_origin",
            "airport_destination",
            "terminal_departure",
            "terminal_arrival",
            "scheduled_departure",
            "scheduled_arrival",
            "estimated_departure",
            "estimated_arrival",
            "actual_departure",
            "actual_arrival",
            "actual_source_departure",
            "actual_source_arrival",
            "departure_delay_min",
            "departure_advance_min",
            "arrival_delay_min",
            "arrival_advance_min",
            "status",
            "movement_type",
        ]:
            current[col] = merge_values(current.get(col), row.get(col))

    return list(merged.values())


# ============================================================
# CSV
# ============================================================

def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    expected_columns = [
        "flight_date",
        "movement_date",
        "flight_number",
        "airline",
        "airport_origin",
        "airport_destination",
        "terminal_departure",
        "terminal_arrival",
        "scheduled_departure",
        "scheduled_arrival",
        "estimated_departure",
        "estimated_arrival",
        "actual_departure",
        "actual_arrival",
        "actual_source_departure",
        "actual_source_arrival",
        "departure_delay_min",
        "departure_advance_min",
        "arrival_delay_min",
        "arrival_advance_min",
        "status",
        "movement_type",
    ]

    for col in expected_columns:
        if col not in df.columns:
            df[col] = None

    return df[expected_columns]




# ============================================================
# MAIN
# ============================================================

def main() -> int:
    generation_stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    output_dir = "OutputSingleFlight"
    os.makedirs(output_dir, exist_ok=True)

    # Remove old JSON files with 'SingleFlightData' and a generation stamp in their names
    for fname in os.listdir(output_dir):
        if fname.startswith("SingleFlightData") and fname.endswith(".json"):
            try:
                os.remove(os.path.join(output_dir, fname))
                print(f"[INFO] Fichier supprimé : {fname}")
            except Exception as e:
                print(f"[ALERTE] Impossible de supprimer {fname} : {e}", file=sys.stderr)
    # Génération d'un fichier JSON avec la réponse brute de l'API
    raw_json_output_file = f"SingleFlightDataRespAPI_{generation_stamp}.json"
    raw_json_output_path = os.path.join(output_dir, raw_json_output_file)
    try:
        with open(raw_json_output_path, "w", encoding="utf-8-sig") as jf:
            import json as _json
            _json.dump(data, jf, ensure_ascii=False, indent=2)
        print(f"Fichier JSON brut généré : {raw_json_output_path}")
    except Exception as e:
        print(f"[ALERTE] Impossible de générer le fichier JSON brut : {e}", file=sys.stderr)




    generation_stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    output_dir = "OutputSingleFlight"
    os.makedirs(output_dir, exist_ok=True)

    # Rien ne doit précéder ces lignes !

    print(f"Répertoire courant : {os.getcwd()}")
    # Génération d'un fichier JSON de sortie avec tous les vols extraits
    json_output_file = f"SingleFlightData_{generation_stamp}.json"
    json_output_path = os.path.join(output_dir, json_output_file)
    try:
        with open(json_output_path, "w", encoding="utf-8-sig") as jf:
            import json as _json
            _json.dump(records, jf, ensure_ascii=False, indent=2)
        print(f"Fichier JSON généré : {json_output_path}")
    except Exception as e:
        print(f"[ALERTE] Impossible de générer le fichier JSON : {e}", file=sys.stderr)




    # === Paramètres du vol ciblé depuis la ligne de commande ===
    if len(sys.argv) < 2:
        print("Usage: python aerodatabox_single_flights.py <flight_number> [flight_date] [departure_airport] [arrival_airport]", file=sys.stderr)
        print("Exemple: python aerodatabox_single_flights.py 'SQ 1894' 2026-04-16 CDG LYS", file=sys.stderr)
        return 1
    flight_number = sys.argv[1]
    flight_date = sys.argv[2] if len(sys.argv) > 2 else "2026-04-16"
    departure_airport = sys.argv[3] if len(sys.argv) > 3 else "CDG"
    arrival_airport = sys.argv[4] if len(sys.argv) > 4 else "LYS"


    # Construction de l'URL pour un vol spécifique
    url = f"{API_BASE_URL}/flights/number/{flight_number.replace(' ', '')}/{flight_date}"
    print(f"[DEBUG] Appel API unique : {url}")

    # --- Bloc robuste pour garantir la génération des fichiers de sortie ---
    data = []
    records = []
    api_error = False
    try:
        response = request_with_retries(
            "GET",
            url,
            headers=build_headers(),
            timeout=REQUEST_TIMEOUT,
            max_retries=MAX_RETRIES,
        )
        response.raise_for_status()
        try:
            data = response.json()
        except Exception as json_exc:
            print(f"Erreur lors du décodage JSON : {json_exc}", file=sys.stderr)
            print(f"[DEBUG] Code HTTP : {response.status_code}")
            print(f"[DEBUG] Longueur de la réponse brute : {len(response.text)}")
            print(f"[DEBUG] Réponse brute : {response.text}")
            api_error = True
            # Écriture du code d'erreur dans API_Single_ERR.log
            with open("API_Single_ERR.log", "a", encoding="utf-8") as ferr:
                ferr.write(f"{datetime.now().isoformat()} | HTTP {response.status_code} | Erreur JSON: {json_exc}\n")
            data = []
            return 0
        print(f"[DEBUG] Réponse API : {str(data)[:500]}")
    except Exception as exc:
        print(f"Erreur lors de l'appel API : {exc}", file=sys.stderr)
        # Récupération du code d'erreur si possible
        err_code = None
        err_text = None
        if hasattr(exc, 'response') and exc.response is not None:
            err_code = getattr(exc.response, 'status_code', None)
            err_text = getattr(exc.response, 'text', None)
            print(f"[DEBUG] Réponse brute : {err_text}")
        api_error = True
        # Écriture du code d'erreur dans API_Single_ERR.log
        with open("API_Single_ERR.log", "a", encoding="utf-8") as ferr:
            ferr.write(f"{datetime.now().isoformat()} | HTTP {err_code} | Exception: {exc}\n")
        data = []
        return 0

    # data est une liste de vols, on extrait les infos pour chaque vol
    if isinstance(data, list) and data:
        for flight in data:
            rec = extract_departure_record_all(flight, departure_airport)
            if rec:
                records.append(rec)
    else:
        print("Aucun vol trouvé pour ce critère.")

    # Si pas de records, on crée un DataFrame vide avec les bonnes colonnes

    df = pd.DataFrame(records)
    df = reorder_columns(df)


    # Ajout des colonnes explicites dans le DataFrame avant export
    df["flight_number_principal"] = df["flight_number"]
    # codeshares peut être absent ou non list-like, on force une chaîne jointe par ;
    def codeshares_str(val):
        if pd.isna(val):
            return ""
        if isinstance(val, list):
            return ";".join(str(x) for x in val)
        return str(val)
    df["flight_number_codeshare"] = df["codeshares"].apply(codeshares_str) if "codeshares" in df.columns else ""



    # === Calcul du total mondial des vols pour l'aéroport de départ ===
    def build_congestion_dataframe_filtered(rows, airport_code):
        records = []
        for row in rows:
            origin = row.get("airport_origin")
            destination = row.get("airport_destination")
            movement_type = row.get("movement_type")
            movement_date = row.get("movement_date")
            if not movement_date or movement_type not in {"departure", "arrival"}:
                continue
            airport = origin if movement_type == "departure" else destination
            if not airport:
                continue
            records.append({
                "flight_date": movement_date,
                "airport": airport,
                "movement_type": movement_type,
            })
        if not records:
            return 0, 0
        temp_df = pd.DataFrame(records)
        grouped = (
            temp_df.groupby(["airport", "movement_type"]).size().unstack(fill_value=0).reset_index()
        )
        arrivals = grouped.loc[grouped["airport"] == departure_airport_code, "arrival"].sum() if "arrival" in grouped.columns else 0
        departures = grouped.loc[grouped["airport"] == departure_airport_code, "departure"].sum() if "departure" in grouped.columns else 0
        return arrivals, departures

    if not df.empty:
        departure_airport_code = df["airport_origin"].iloc[0]
        arrivals, departures = build_congestion_dataframe_filtered(records, departure_airport_code)
    else:
        arrivals, departures = 0, 0

    df["total_global_arrivals_at_airport"] = arrivals
    df["total_global_departures_from_airport"] = departures

    # Place uniquement les colonnes globales en tête du CSV
    cols = [
        "flight_number_principal",
        "flight_number_codeshare",
        "total_global_arrivals_at_airport",
        "total_global_departures_from_airport"
    ] + [c for c in df.columns if c not in ("flight_number_principal", "flight_number_codeshare", "total_global_arrivals_at_airport", "total_global_departures_from_airport")]
    df = df[cols]

    # Affichage des valeurs extraites avant filtrage strict
    print("[DEBUG] Aperçu des valeurs extraites avant filtrage strict :")
    if not df.empty:
        print(df[["flight_number", "flight_date", "airport_origin", "airport_destination"]])
    else:
        print("[DEBUG] DataFrame vide après extraction.")

    # Ajout d'un affichage des codeshares pour diagnostic
    if not df.empty and "codeshares" in df.columns:
        print("[DEBUG] Aperçu des codeshares :")
        print(df[["flight_number", "codeshares"]])

    # Nouveau filtrage : accepte si flight_number == cible OU cible dans codeshares
    def match_flight(row):
        num = str(row.get("flight_number", "")).replace(" ", "").upper()
        codeshares = row.get("codeshares", [])
        if isinstance(codeshares, str):
            codeshares = [codeshares]
        codeshares = [str(cs).replace(" ", "").upper() for cs in codeshares]
        cible = flight_number.replace(" ", "").upper()
        print(f"[DEBUG] codeshares bruts pour ce vol (flight_number={num}) : {codeshares}")
        return num == cible or cible in codeshares

        

    # Suppression du filtrage strict : on conserve toutes les valeurs extraites

    # Stockage de la réponse brute de l'API :
    # Ce bloc doit être exécuté même si le DataFrame filtré est vide
    import json

    # Ajout de la date et l'heure de génération dans les noms de fichiers
    generation_stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")

    # Export a true CSV with all fields from the raw API response (flattened)
    resp_output_dir = "OutputSingleFlight"
    os.makedirs(resp_output_dir, exist_ok=True)
    resp_output_file = f"SingleFlightDataRespAPI_{generation_stamp}.csv"
    resp_output_path = os.path.join(resp_output_dir, resp_output_file)
    # Always create the CSV, even if empty, with columns matching the API structure
    try:
        if isinstance(data, list) and data:
            df_raw = pd.json_normalize(data)
        else:
            # Try to infer columns from a typical API response structure
            # If you have a sample structure, you can define it here
            df_raw = pd.DataFrame()
        df_raw.to_csv(resp_output_path, index=False, encoding="utf-8-sig")
        print(f"[DEBUG] Fichier CSV brut (toutes les colonnes) créé : {resp_output_path}")
    except Exception as e:
        print(f"[ALERTE] Erreur lors de la création du CSV brut : {e}", file=sys.stderr)

    output_dir = "OutputSingleFlight"
    os.makedirs(output_dir, exist_ok=True)
    # Suppression de tous les fichiers CSV existants dans le dossier de sortie
    for fname in os.listdir(output_dir):
        if fname.lower().endswith(".csv"):
            try:
                os.remove(os.path.join(output_dir, fname))
            except Exception as e:
                print(f"[ALERTE] Impossible de supprimer {fname} : {e}", file=sys.stderr)
    output_file = f"SingleFlightData_{generation_stamp}.csv"
    output_path = os.path.join(output_dir, output_file)


    try:
        if os.path.exists(output_path):
            os.remove(output_path)
        # Si le DataFrame est vide, ajouter une ligne vide avec toutes les colonnes attendues
        if df.empty:
            empty_row = {col: "" for col in df.columns}
            df = pd.DataFrame([empty_row])
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        # Génération du fichier de colonnes
        col_list_file = os.path.join(output_dir, f"SingleFlightColList_{generation_stamp}.csv")
        with open(col_list_file, "w", encoding="utf-8-sig") as fcols:
            for col in df.columns:
                fcols.write(f"{col}\n")
        print(f"Fichier des colonnes généré : {col_list_file}")
    except PermissionError:
        print(
            f"Erreur : impossible d'écraser {output_path} car le fichier est ouvert dans Excel ou un autre programme.",
            file=sys.stderr,
        )
        return 0

    print(f"CSV généré : {output_path}")
    print(f"Nombre de lignes : {len(df)}")
    if df.empty or (len(df) == 1 and all(str(v) == '' for v in df.iloc[0].values)):
        print("Aucun vol trouvé après filtrage strict. (ligne vide ajoutée)")
    return 0



if __name__ == "__main__":
    raise SystemExit(main())