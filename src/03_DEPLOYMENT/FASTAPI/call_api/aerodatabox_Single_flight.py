#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()


# ============================================================
# CONFIGURATION
# ============================================================

API_BASE_URL = "https://prod.api.market/api/v1/aedbx/aerodatabox"

REQUEST_TIMEOUT = 60
MAX_RETRIES = 5
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
BACKOFF_FACTOR = 2.0
INITIAL_BACKOFF_SECONDS = 1.5

TIME_TOLERANCE_MINUTES = 30


# ============================================================
# OUTILS REQUÊTE / FICHIERS
# ============================================================

def get_output_dir() -> str:
    return os.getenv("REQUEST_OUTPUT_SINGLE", "OutputSingleFlight")


def get_request_dir() -> Path:
    request_dir = os.getenv("REQUEST_DIR")
    if request_dir:
        return Path(request_dir)
    return Path(get_output_dir())


def get_request_id() -> str:
    return os.getenv("REQUEST_ID", "unknown_request")


def get_status_file_path() -> Path:
    return get_request_dir() / "flight_request_status.json"


def get_log_file_path() -> Path:
    return get_request_dir() / "API_Single_ERR.log"


def append_error_log(message: str) -> None:
    log_path = get_log_file_path()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {message}\n")


def write_request_status(
    *,
    status: str,
    user_message: str,
    error_code: Optional[str] = None,
    warning_message: Optional[str] = None,
    matched_flight_number: Optional[str] = None,
    matched_scheduled_departure: Optional[str] = None,
    matched_departure_airport: Optional[str] = None,
    matched_arrival_airport: Optional[str] = None,
    requested_flight_number: Optional[str] = None,
    requested_flight_date: Optional[str] = None,
    requested_departure_airport: Optional[str] = None,
    requested_arrival_airport: Optional[str] = None,
) -> None:
    payload = {
        "request_id": get_request_id(),
        "status": status,
        "error_code": error_code,
        "user_message": user_message,
        "warning_message": warning_message,
        "matched_flight_number": matched_flight_number,
        "matched_scheduled_departure": matched_scheduled_departure,
        "matched_departure_airport": matched_departure_airport,
        "matched_arrival_airport": matched_arrival_airport,
        "requested_flight_number": requested_flight_number,
        "requested_flight_date": requested_flight_date,
        "requested_departure_airport": requested_departure_airport,
        "requested_arrival_airport": requested_arrival_airport,
        "generated_at": datetime.now().isoformat(),
    }

    status_path = get_status_file_path()
    status_path.parent.mkdir(parents=True, exist_ok=True)

    with open(status_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


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


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


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


def normalize_flight_number(value: Optional[str]) -> str:
    if not value:
        return ""
    return str(value).replace(" ", "").upper().strip()


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


def minutes_diff_between_iso_and_hour(iso_dt: Optional[str], requested_hour: str) -> Optional[int]:
    if not iso_dt or not requested_hour:
        return None

    try:
        dt = datetime.fromisoformat(str(iso_dt).replace("Z", "+00:00"))
    except Exception:
        return None

    try:
        req_h, req_m = requested_hour.split(":")
        req_hour = int(req_h)
        req_min = int(req_m)
    except Exception:
        return None

    flight_minutes = dt.hour * 60 + dt.minute
    requested_minutes = req_hour * 60 + req_min
    return abs(flight_minutes - requested_minutes)


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
                    f"HTTP {response.status_code} - tentative {attempt}/{max_retries}, retry dans {retry_after_seconds:.1f}s",
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
                f"Erreur réseau ({exc.__class__.__name__}) - tentative {attempt}/{max_retries}, retry dans {sleep_seconds:.1f}s",
                file=sys.stderr,
            )
            time.sleep(sleep_seconds)

    if last_exception:
        raise RuntimeError(f"Échec requête : {last_exception}") from last_exception

    raise RuntimeError("Échec requête sans détail disponible.")


# ============================================================
# EXTRACTION
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

    codeshares = dep.get("codeshares", [])
    if not isinstance(codeshares, list):
        codeshares = [codeshares] if codeshares else []

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
        "codeshares": codeshares,
    }


# ============================================================
# FILTRAGE MÉTIER
# ============================================================

def flight_matches_requested_number(row: Dict[str, Any], requested_flight_number: str) -> bool:
    requested = normalize_flight_number(requested_flight_number)
    principal = normalize_flight_number(row.get("flight_number"))

    if principal == requested:
        return True

    codeshares = row.get("codeshares", [])
    return any(normalize_flight_number(cs) == requested for cs in codeshares)


def is_codeshare_match(row: Dict[str, Any], requested_flight_number: str) -> bool:
    requested = normalize_flight_number(requested_flight_number)
    principal = normalize_flight_number(row.get("flight_number"))
    if principal == requested:
        return False
    return any(normalize_flight_number(cs) == requested for cs in row.get("codeshares", []))


def filter_best_matching_records(
    records: List[Dict[str, Any]],
    requested_flight_number: str,
    requested_departure_airport: str,
    requested_arrival_airport: str,
    requested_hour: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    debug_info = {
        "initial_count": len(records),
        "after_flight_number_count": 0,
        "after_departure_airport_count": 0,
        "after_arrival_airport_count": 0,
        "after_time_count": 0,
        "best_time_diff_minutes": None,
        "has_codeshare": False,
    }

    if not records:
        return [], debug_info

    filtered = [r for r in records if flight_matches_requested_number(r, requested_flight_number)]
    debug_info["after_flight_number_count"] = len(filtered)

    if not filtered:
        return [], debug_info

    filtered_dep = [
        r for r in filtered
        if str(r.get("airport_origin", "")).upper().strip() == requested_departure_airport.upper().strip()
    ]
    if filtered_dep:
        filtered = filtered_dep
    debug_info["after_departure_airport_count"] = len(filtered)

    if requested_arrival_airport:
        filtered_arr = [
            r for r in filtered
            if str(r.get("airport_destination", "")).upper().strip() == requested_arrival_airport.upper().strip()
        ]
        if filtered_arr:
            filtered = filtered_arr
    debug_info["after_arrival_airport_count"] = len(filtered)

    if any(is_codeshare_match(r, requested_flight_number) for r in filtered):
        debug_info["has_codeshare"] = True

    if requested_hour:
        valid_diffs = []
        for row in filtered:
            diff = minutes_diff_between_iso_and_hour(row.get("scheduled_departure"), requested_hour)
            if diff is not None:
                valid_diffs.append((row, diff))

        if valid_diffs:
            best_diff = min(diff for _, diff in valid_diffs)
            debug_info["best_time_diff_minutes"] = best_diff

            if best_diff <= TIME_TOLERANCE_MINUTES:
                filtered = [row for row, diff in valid_diffs if diff == best_diff]
            else:
                filtered = []

    debug_info["after_time_count"] = len(filtered)
    return filtered, debug_info


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


def codeshares_str(val):
    if pd.isna(val):
        return ""
    if isinstance(val, list):
        return ";".join(str(x) for x in val)
    return str(val)


# ============================================================
# MAIN
# ============================================================

def main() -> int:
    output_dir = get_output_dir()
    os.makedirs(output_dir, exist_ok=True)
    get_request_dir().mkdir(parents=True, exist_ok=True)

    if len(sys.argv) < 2:
        append_error_log("Arguments insuffisants pour aerodatabox_Single_flight.py")
        write_request_status(
            status="error_flight_not_found",
            error_code="MISSING_ARGUMENTS",
            user_message="Vol introuvable. Veuillez vérifier le numéro de vol, la date, l’horaire et l’aéroport de départ.",
        )
        return 1

    flight_number = sys.argv[1]
    flight_date_raw = sys.argv[2] if len(sys.argv) > 2 else "2026-04-16"
    departure_airport = sys.argv[3] if len(sys.argv) > 3 else "CDG"
    arrival_airport = sys.argv[4] if len(sys.argv) > 4 else ""

    requested_hour = ""
    flight_date = flight_date_raw

    if "T" in flight_date_raw or " " in flight_date_raw:
        try:
            dt = datetime.fromisoformat(flight_date_raw.replace(" ", "T"))
            requested_hour = dt.strftime("%H:%M")
            flight_date = dt.strftime("%Y-%m-%d")
        except Exception:
            parts = flight_date_raw.replace("T", " ").split()
            if len(parts) >= 2:
                flight_date = parts[0]
                requested_hour = parts[1][:5]

    generation_stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    url = f"{API_BASE_URL}/flights/number/{flight_number.replace(' ', '')}/{flight_date}"

    print(f"[DEBUG] Appel API unique : {url}")
    print("DEBUG_NEW_AERODATABOX_VERSION_RUNNING")

    try:
        response = request_with_retries(
            "GET",
            url,
            headers=build_headers(),
            timeout=REQUEST_TIMEOUT,
            max_retries=MAX_RETRIES,
        )

        if response.status_code in {400, 404}:
            msg = f"HTTP {response.status_code} - vol non trouvé ou informations invalides pour {flight_number} le {flight_date}"
            append_error_log(msg)
            write_request_status(
                status="error_flight_not_found",
                error_code="FLIGHT_NOT_FOUND",
                user_message="Vol introuvable. Veuillez vérifier le numéro de vol, la date, l’horaire et l’aéroport de départ.",
                requested_flight_number=flight_number,
                requested_flight_date=flight_date_raw,
                requested_departure_airport=departure_airport,
                requested_arrival_airport=arrival_airport,
            )
            return 1

        if response.status_code in {429, 500, 502, 503, 504}:
            msg = f"HTTP {response.status_code} - service indisponible pour {flight_number} le {flight_date}"
            append_error_log(msg)
            write_request_status(
                status="error_api_unavailable",
                error_code="API_TEMPORARILY_UNAVAILABLE",
                user_message="Le service de recherche de vol est momentanément indisponible. Merci de réessayer dans quelques instants.",
                requested_flight_number=flight_number,
                requested_flight_date=flight_date_raw,
                requested_departure_airport=departure_airport,
                requested_arrival_airport=arrival_airport,
            )
            return 1

        response.raise_for_status()
#=====
        response_text = response.text.strip()

        try:
            data = response.json()
        except Exception as json_exc:
            append_error_log(f"Erreur décodage JSON Aerodatabox : {json_exc}")

            if not response_text:
                append_error_log("Réponse API vide : interprétée comme vol introuvable.")
                write_request_status(
                    status="error_flight_not_found",
                    error_code="EMPTY_RESPONSE_FLIGHT_NOT_FOUND",
                    user_message="Vol introuvable. Veuillez vérifier le numéro de vol, la date, l’horaire et l’aéroport de départ.",
                    requested_flight_number=flight_number,
                    requested_flight_date=flight_date_raw,
                    requested_departure_airport=departure_airport,
                    requested_arrival_airport=arrival_airport,
                )
                return 1

            if response_text == "[]":
                append_error_log("Réponse API [] : interprétée comme vol introuvable.")
                write_request_status(
                    status="error_flight_not_found",
                    error_code="EMPTY_LIST_FLIGHT_NOT_FOUND",
                    user_message="Vol introuvable. Veuillez vérifier le numéro de vol, la date, l’horaire et l’aéroport de départ.",
                    requested_flight_number=flight_number,
                    requested_flight_date=flight_date_raw,
                    requested_departure_airport=departure_airport,
                    requested_arrival_airport=arrival_airport,
                )
                return 1

            if response_text.startswith("<"):
                append_error_log("Réponse HTML/non JSON détectée : interprétée comme indisponibilité du service.")
                write_request_status(
                    status="error_api_unavailable",
                    error_code="NON_JSON_HTML_RESPONSE",
                    user_message="Le service de recherche de vol est momentanément indisponible. Merci de réessayer dans quelques instants.",
                    requested_flight_number=flight_number,
                    requested_flight_date=flight_date_raw,
                    requested_departure_airport=departure_airport,
                    requested_arrival_airport=arrival_airport,
                )
                return 1

            append_error_log(f"Réponse brute non JSON : {response_text[:500]}")
            write_request_status(
                status="error_flight_not_found",
                error_code="NON_JSON_FLIGHT_NOT_FOUND",
                user_message="Vol introuvable. Veuillez vérifier le numéro de vol, la date, l’horaire et l’aéroport de départ.",
                requested_flight_number=flight_number,
                requested_flight_date=flight_date_raw,
                requested_departure_airport=departure_airport,
                requested_arrival_airport=arrival_airport,
            )
            return 1
#=====
    except (requests.Timeout, requests.ConnectionError) as exc:
        append_error_log(f"Erreur réseau lors de l'appel API : {exc}")
        write_request_status(
            status="error_api_unavailable",
            error_code="NETWORK_ERROR",
            user_message="Le service de recherche de vol est momentanément indisponible. Merci de réessayer dans quelques instants.",
            requested_flight_number=flight_number,
            requested_flight_date=flight_date_raw,
            requested_departure_airport=departure_airport,
            requested_arrival_airport=arrival_airport,
        )
        return 1

    except Exception as exc:
        append_error_log(f"Erreur lors de l'appel API : {exc}")
        if hasattr(exc, "response") and exc.response is not None:
            append_error_log(f"Réponse brute API : {exc.response.text}")

        write_request_status(
            status="error_api_unavailable",
            error_code="API_CALL_FAILED",
            user_message="Le service de recherche de vol est momentanément indisponible. Merci de réessayer dans quelques instants.",
            requested_flight_number=flight_number,
            requested_flight_date=flight_date_raw,
            requested_departure_airport=departure_airport,
            requested_arrival_airport=arrival_airport,
        )
        return 1

    raw_json_output_path = os.path.join(output_dir, f"SingleFlightDataRespAPI_{generation_stamp}.json")
    with open(raw_json_output_path, "w", encoding="utf-8-sig") as jf:
        json.dump(data, jf, ensure_ascii=False, indent=2)

    records: List[Dict[str, Any]] = []

    if isinstance(data, list) and data:
        for flight in data:
            rec = extract_departure_record_all(flight, departure_airport)
            if rec:
                records.append(rec)

    if not records:
        append_error_log(
            f"Aucun vol exploitable trouvé après extraction pour {flight_number} / {flight_date} / {departure_airport}"
        )
        write_request_status(
            status="error_flight_not_found",
            error_code="NO_RECORDS_AFTER_EXTRACTION",
            user_message="Vol introuvable. Veuillez vérifier le numéro de vol, la date, l’horaire et l’aéroport de départ.",
            requested_flight_number=flight_number,
            requested_flight_date=flight_date_raw,
            requested_departure_airport=departure_airport,
            requested_arrival_airport=arrival_airport,
        )
        return 1

    filtered_records, debug_info = filter_best_matching_records(
        records=records,
        requested_flight_number=flight_number,
        requested_departure_airport=departure_airport,
        requested_arrival_airport=arrival_airport,
        requested_hour=requested_hour,
    )

    if not filtered_records:
        if debug_info.get("after_flight_number_count", 0) > 0 and requested_hour:
            append_error_log(
                f"Aucun vol trouvé avec horaire compatible. requested_hour={requested_hour}, best_diff={debug_info.get('best_time_diff_minutes')}"
            )
            write_request_status(
                status="error_time_mismatch",
                error_code="TIME_MISMATCH",
                user_message="Plusieurs vols ont été trouvés, mais aucun ne correspond précisément à l’horaire renseigné. Veuillez vérifier l’heure saisie.",
                requested_flight_number=flight_number,
                requested_flight_date=flight_date_raw,
                requested_departure_airport=departure_airport,
                requested_arrival_airport=arrival_airport,
            )
            return 1

        append_error_log(
            f"Aucun vol trouvé après filtrage métier. debug_info={json.dumps(debug_info, ensure_ascii=False)}"
        )
        write_request_status(
            status="error_flight_not_found",
            error_code="NO_MATCH_AFTER_FILTERING",
            user_message="Vol introuvable. Veuillez vérifier le numéro de vol, la date, l’horaire et l’aéroport de départ.",
            requested_flight_number=flight_number,
            requested_flight_date=flight_date_raw,
            requested_departure_airport=departure_airport,
            requested_arrival_airport=arrival_airport,
        )
        return 1

    selected_record = filtered_records[0]

    warning_message = None
    status = "success"

    if is_codeshare_match(selected_record, flight_number):
        status = "success_with_warning_codeshare"
        warning_message = (
            f"Le vol saisi correspond à un vol en partage de code. "
            f"Les données ont été retrouvées sous la référence {selected_record.get('flight_number')}."
        )

    write_request_status(
        status=status,
        error_code=None,
        user_message="Vol trouvé avec succès.",
        warning_message=warning_message,
        matched_flight_number=selected_record.get("flight_number"),
        matched_scheduled_departure=selected_record.get("scheduled_departure"),
        matched_departure_airport=selected_record.get("airport_origin"),
        matched_arrival_airport=selected_record.get("airport_destination"),
        requested_flight_number=flight_number,
        requested_flight_date=flight_date_raw,
        requested_departure_airport=departure_airport,
        requested_arrival_airport=arrival_airport,
    )

    json_output_path = os.path.join(output_dir, f"SingleFlightData_{generation_stamp}.json")
    with open(json_output_path, "w", encoding="utf-8-sig") as jf:
        json.dump(filtered_records, jf, ensure_ascii=False, indent=2)

    df = pd.DataFrame(filtered_records)
    df = reorder_columns(df)

    df["flight_number_principal"] = df["flight_number"]
    df["flight_number_codeshare"] = df["codeshares"].apply(codeshares_str) if "codeshares" in df.columns else ""

    def build_congestion_dataframe_filtered(rows, airport_code):
        records_local = []
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
            records_local.append({
                "flight_date": movement_date,
                "airport": airport,
                "movement_type": movement_type,
            })

        if not records_local:
            return 0, 0

        temp_df = pd.DataFrame(records_local)
        grouped = temp_df.groupby(["airport", "movement_type"]).size().unstack(fill_value=0).reset_index()
        arrivals = grouped.loc[grouped["airport"] == airport_code, "arrival"].sum() if "arrival" in grouped.columns else 0
        departures = grouped.loc[grouped["airport"] == airport_code, "departure"].sum() if "departure" in grouped.columns else 0
        return arrivals, departures

    departure_airport_code = df["airport_origin"].iloc[0] if not df.empty else departure_airport
    arrivals, departures = build_congestion_dataframe_filtered(filtered_records, departure_airport_code)

    df["total_global_arrivals_at_airport"] = arrivals
    df["total_global_departures_from_airport"] = departures

    cols = [
        "flight_number_principal",
        "flight_number_codeshare",
        "total_global_arrivals_at_airport",
        "total_global_departures_from_airport",
    ] + [
        c for c in df.columns
        if c not in (
            "flight_number_principal",
            "flight_number_codeshare",
            "total_global_arrivals_at_airport",
            "total_global_departures_from_airport",
        )
    ]
    df = df[cols]

    resp_output_path = os.path.join(output_dir, f"SingleFlightDataRespAPI_{generation_stamp}.csv")
    df_raw = pd.json_normalize(data) if isinstance(data, list) and data else pd.DataFrame()
    df_raw.to_csv(resp_output_path, index=False, encoding="utf-8-sig")

    output_path = os.path.join(output_dir, f"SingleFlightData_{generation_stamp}.csv")
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    col_list_file = os.path.join(output_dir, f"SingleFlightColList_{generation_stamp}.csv")
    with open(col_list_file, "w", encoding="utf-8-sig") as fcols:
        for col in df.columns:
            fcols.write(f"{col}\n")

    print(f"CSV généré : {output_path}")
    print(f"Nombre de lignes : {len(df)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())