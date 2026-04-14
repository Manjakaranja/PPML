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
DAYS_BACK = 209

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


def build_congestion_dataframe_filtered(
    rows: List[Dict[str, Any]],
    route_filter: Callable[[Optional[str], Optional[str]], bool],
) -> pd.DataFrame:
    records: List[Dict[str, Any]] = []

    for row in rows:
        origin = row.get("airport_origin")
        destination = row.get("airport_destination")

        if not route_filter(origin, destination):
            continue

        movement_date = row.get("movement_date")
        movement_type = row.get("movement_type")

        if not movement_date or movement_type not in {"departure", "arrival"}:
            continue

        airport = row.get("airport_origin") if movement_type == "departure" else row.get("airport_destination")

        if not airport:
            continue

        records.append({
            "flight_date": movement_date,
            "airport": airport,
            "movement_type": movement_type,
        })

    if not records:
        return pd.DataFrame(columns=[
            "flight_date",
            "airport",
            "nombre_departs",
            "nombre_arrivees",
        ])

    temp_df = pd.DataFrame(records)

    grouped = (
        temp_df.groupby(["flight_date", "airport", "movement_type"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )

    if "departure" not in grouped.columns:
        grouped["departure"] = 0
    if "arrival" not in grouped.columns:
        grouped["arrival"] = 0

    grouped = grouped.rename(columns={
        "departure": "nombre_departs",
        "arrival": "nombre_arrivees",
    })

    grouped = grouped[
        ["flight_date", "airport", "nombre_departs", "nombre_arrivees"]
    ]

    grouped = grouped.sort_values(
        by=["flight_date", "airport"],
        ascending=[True, True]
    ).reset_index(drop=True)

    return grouped


# ============================================================
# MAIN
# ============================================================

def main() -> int:
    print(f"Répertoire courant : {os.getcwd()}")

    try:
        env_or_fail("API_KEY")
    except Exception as exc:
        print(f"Erreur configuration : {exc}", file=sys.stderr)
        return 1

    from_local, to_local = get_date_range(DAYS_BACK)
    from_dt = datetime.fromisoformat(from_local)
    to_dt = datetime.fromisoformat(to_local) + timedelta(days=1)

    print(f"Période demandée : {from_local} -> {to_local}")
    print("Récupération des vols AeroDataBox...")

    raw_rows: List[Dict[str, Any]] = []
    all_movements: List[Dict[str, Any]] = []
    tls_logs: List[Dict[str, Any]] = []

    for airport_code in ALL_AIRPORTS:
        print(f" - Interrogation {airport_code}")

        for start, end in daterange_12h(from_dt, to_dt):
            start_str = start.strftime("%Y-%m-%dT%H:%M")
            end_str = end.strftime("%Y-%m-%dT%H:%M")

            try:
                payload = fetch_airport_fids(airport_code, start_str, end_str)
            except Exception as exc:
                print(f"   ! Erreur pour {airport_code} ({start_str} -> {end_str}) : {exc}", file=sys.stderr)
                if airport_code == "TLS":
                    tls_logs.append({
                        "airport": airport_code,
                        "start": start_str,
                        "end": end_str,
                        "departures": "ERROR",
                        "arrivals": "ERROR",
                        "error": str(exc),
                    })
                continue

            departures = payload.get("departures") or []
            arrivals = payload.get("arrivals") or []

            print(f"     > {len(departures)} départs, {len(arrivals)} arrivées bruts pour {airport_code} ({start_str} -> {end_str})")

            if airport_code == "TLS":
                tls_logs.append({
                    "airport": airport_code,
                    "start": start_str,
                    "end": end_str,
                    "departures": len(departures),
                    "arrivals": len(arrivals),
                    "error": "",
                })

            for dep in departures:
                row_all = extract_departure_record_all(dep, airport_code)
                if row_all:
                    all_movements.append(row_all)

                    if is_target_route(row_all.get("airport_origin"), row_all.get("airport_destination")):
                        raw_rows.append(dict(row_all))

            for arr in arrivals:
                row_all = extract_arrival_record_all(arr, airport_code)
                if row_all:
                    all_movements.append(row_all)

                    if is_target_route(row_all.get("airport_origin"), row_all.get("airport_destination")):
                        raw_rows.append(dict(row_all))

            time.sleep(REQUEST_SLEEP_SECONDS)

    tls_log_columns = ["airport", "start", "end", "departures", "arrivals", "error"]
    tls_log_path = os.path.join(os.getcwd(), "tls_interrogations.csv")
    pd.DataFrame(tls_logs, columns=tls_log_columns).to_csv(
        tls_log_path,
        index=False,
        encoding="utf-8-sig"
    )
    print(f"[INFO] Fichier tls_interrogations.csv généré à : {tls_log_path}")

    if not raw_rows:
        print("Aucun vol brut trouvé pour le CSV principal.")
        return 0

    merged_rows = merge_rows(raw_rows)

    df = pd.DataFrame(merged_rows)
    df = reorder_columns(df)

    df = df[
        (
            df["airport_origin"].isin(PARIS_AIRPORTS)
            & df["airport_destination"].isin(REGIONAL_AIRPORTS)
        )
        |
        (
            df["airport_origin"].isin(REGIONAL_AIRPORTS)
            & df["airport_destination"].isin(PARIS_AIRPORTS)
        )
    ]

    df = df.drop_duplicates(
        subset=[
            "flight_number",
            "flight_date",
            "airport_origin",
            "airport_destination",
            "scheduled_departure",
            "scheduled_arrival",
        ]
    )

    if df.empty:
        print("Aucun vol trouvé après filtrage final Paris ↔ régions.")
        print("[ALERTE] Aucun fichier de vol n'a été généré car aucune donnée filtrée n'a été trouvée.")
        return 0

    df = df.sort_values(
        by=[
            "flight_date",
            "flight_number",
            "airport_origin",
            "airport_destination",
            "scheduled_departure",
            "scheduled_arrival",
        ],
        ascending=True,
        na_position="last",
    )

    output_dir = "OutputFlights"
    os.makedirs(output_dir, exist_ok=True)

    output_file = "flights_paris_regional.csv"
    output_path = os.path.join(output_dir, output_file)

    try:
        if os.path.exists(output_path):
            os.remove(output_path)
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
    except PermissionError:
        print(
            f"Erreur : impossible d'écraser {output_path} car le fichier est ouvert dans Excel ou un autre programme.",
            file=sys.stderr,
        )
        return 1

    print(f"CSV généré : {output_path}")
    print(f"Nombre de lignes : {len(df)}")

    congestion_cible_df = build_congestion_dataframe_filtered(
        all_movements,
        is_target_route
    )

    congestion_france_df = build_congestion_dataframe_filtered(
        all_movements,
        is_domestic_france_route
    )

    congestion_ww_df = build_congestion_dataframe_filtered(
        all_movements,
        is_any_route_within_scope
    )

    congestion_cible_path = os.path.join(output_dir, "congestion_cible.csv")
    congestion_france_path = os.path.join(output_dir, "congestion_France.csv")
    congestion_ww_path = os.path.join(output_dir, "congestion_WW.csv")

    try:
        if os.path.exists(congestion_cible_path):
            os.remove(congestion_cible_path)
        congestion_cible_df.to_csv(congestion_cible_path, index=False, encoding="utf-8-sig")
    except PermissionError:
        print(
            f"Erreur : impossible d'écraser {congestion_cible_path} car le fichier est ouvert.",
            file=sys.stderr,
        )
        return 1

    try:
        if os.path.exists(congestion_france_path):
            os.remove(congestion_france_path)
        congestion_france_df.to_csv(congestion_france_path, index=False, encoding="utf-8-sig")
    except PermissionError:
        print(
            f"Erreur : impossible d'écraser {congestion_france_path} car le fichier est ouvert.",
            file=sys.stderr,
        )
        return 1

    try:
        if os.path.exists(congestion_ww_path):
            os.remove(congestion_ww_path)
        congestion_ww_df.to_csv(congestion_ww_path, index=False, encoding="utf-8-sig")
    except PermissionError:
        print(
            f"Erreur : impossible d'écraser {congestion_ww_path} car le fichier est ouvert.",
            file=sys.stderr,
        )
        return 1

    print(f"CSV congestion cible généré : {congestion_cible_path}")
    print(f"Nombre de lignes congestion cible : {len(congestion_cible_df)}")

    print(f"CSV congestion France généré : {congestion_france_path}")
    print(f"Nombre de lignes congestion France : {len(congestion_france_df)}")

    print(f"CSV congestion WW généré : {congestion_ww_path}")
    print(f"Nombre de lignes congestion WW : {len(congestion_ww_df)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())