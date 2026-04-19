#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

API_BASE_URL = "https://prod.api.market/api/v1/aedbx/aerodatabox"
REQUEST_TIMEOUT = 60
MAX_RETRIES = 5
BACKOFF_FACTOR = 2.0
INITIAL_BACKOFF_SECONDS = 1.5

CDG_AIRPORT = "CDG"

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

def daterange_12h(start_date: datetime, end_date: datetime):
    current = start_date
    while current < end_date:
        next_ = min(current + timedelta(hours=12), end_date)
        yield current, next_
        current = next_

def fetch_airport_fids(iata_code: str, from_local: str, to_local: str) -> Dict[str, Any]:
    url = f"{API_BASE_URL}/flights/airports/Iata/{iata_code}/{from_local}/{to_local}"
    params = {
        "withLeg": "true",
        "withCancelled": "true",
        "withCodeshared": "true",
        "withCargo": "true",
        "withPrivate": "true",
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=build_headers(), params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            if attempt == MAX_RETRIES:
                raise
            wait = INITIAL_BACKOFF_SECONDS * (BACKOFF_FACTOR ** (attempt - 1))
            print(f"Retry {attempt}/{MAX_RETRIES} after error: {exc}", file=sys.stderr)
            import time
            time.sleep(wait)
    return {"departures": [], "arrivals": []}

def main():
    if len(sys.argv) < 3:
        print("Usage: python vols_journaliers_1DayDateAirport.py YYYY-MM-DD AIRPORT_IATA", file=sys.stderr)
        sys.exit(1)
    try:
        target_date = datetime.strptime(sys.argv[1], "%Y-%m-%d")
    except ValueError:
        print("Format de date invalide. Utilisez YYYY-MM-DD.", file=sys.stderr)
        sys.exit(1)
    airport_code = sys.argv[2].upper()
    from_dt = datetime(target_date.year, target_date.month, target_date.day)
    to_dt = from_dt + timedelta(days=1)
    results = []
    print(f"Traitement {airport_code} pour la date {from_dt.date()}")
    day_counts = {}
    for start, end in daterange_12h(from_dt, to_dt):
        start_str = start.strftime("%Y-%m-%dT%H:%M")
        end_str = end.strftime("%Y-%m-%dT%H:%M")
        try:
            payload = fetch_airport_fids(airport_code, start_str, end_str)
        except Exception as exc:
            print(f"Erreur pour {airport_code} ({start_str} -> {end_str}) : {exc}", file=sys.stderr)
            continue
        for dep in payload.get("departures", []):
            sched = dep.get("departure", {}).get("scheduledTime", {})
            date_str = sched.get("local")
            if date_str:
                day = date_str[:10]
                day_counts.setdefault(day, {"departures": 0, "arrivals": 0})
                day_counts[day]["departures"] += 1
        for arr in payload.get("arrivals", []):
            sched = arr.get("arrival", {}).get("scheduledTime", {})
            date_str = sched.get("local")
            if date_str:
                day = date_str[:10]
                day_counts.setdefault(day, {"departures": 0, "arrivals": 0})
                day_counts[day]["arrivals"] += 1
    for day, counts in sorted(day_counts.items()):
        results.append({
            "airport": airport_code,
            "date": day,
            "nombre_departures": counts["departures"],
            "nombre_arrivals": counts["arrivals"]
        })
    df = pd.DataFrame(results)
    # Renommer la colonne 'date' en 'flight_date' et la placer en première position si elle existe
    if 'date' in df.columns:
        df = df.rename(columns={'date': 'flight_date'})
        cols = df.columns.tolist()
        cols.insert(0, cols.pop(cols.index('flight_date')))
        df = df[cols]
    # Ajout de la colonne somme_nombre_departs_arrivees
    if 'nombre_departures' in df.columns and 'nombre_arrivals' in df.columns:
        df['somme_nombre_departs_arrivees'] = df['nombre_departures'].fillna(0).astype(int) + df['nombre_arrivals'].fillna(0).astype(int)
    # Ajout de la colonne 'congestion' avec la valeur 'unknown'
    df['congestion'] = 'unknown'
    output_dir = "OutputSingleFlight"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"vols_journaliers_{airport_code}_{from_dt.date()}.csv")
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"CSV généré : {output_file}")



if __name__ == "__main__":
    main()
