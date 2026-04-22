#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import html
import os
import re
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime


NB_DAYS = 365
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
REQUEST_TIMEOUT = 30
SLEEP_BETWEEN_REQUESTS = 1.2

AIRPORTS = {
    "LYON": {
        "queries": [
            'grève aéroport Lyon Saint-Exupéry',
            'greve aeroport Lyon Saint Exupery',
            'grève LYS aéroport',
            'strike Lyon airport France',
        ],
        "aliases": [
            "lyon", "saint-exupéry", "saint exupery", "lys",
            "aéroport lyon", "aeroport lyon", "lyon airport"
        ],
    },
    "TOULOUSE": {
        "queries": [
            'grève aéroport Toulouse-Blagnac',
            'greve aeroport Toulouse Blagnac',
            'grève TLS aéroport',
            'strike Toulouse airport France',
        ],
        "aliases": [
            "toulouse", "blagnac", "tls",
            "aéroport toulouse", "aeroport toulouse", "toulouse airport"
        ],
    },
    "NICE": {
        "queries": [
            "grève aéroport Nice Côte d'Azur",
            "greve aeroport Nice Cote d Azur",
            'grève NCE aéroport',
            'strike Nice airport France',
        ],
        "aliases": [
            "nice", "côte d'azur", "cote d azur", "nce",
            "aéroport nice", "aeroport nice", "nice airport"
        ],
    },
    "MARSEILLE": {
        "queries": [
            'grève aéroport Marseille-Provence',
            'greve aeroport Marseille Provence',
            'grève MRS aéroport',
            'strike Marseille airport France',
        ],
        "aliases": [
            "marseille", "provence", "mrs",
            "aéroport marseille", "aeroport marseille", "marseille airport"
        ],
    },
    "CDG": {
        "queries": [
            'grève aéroport Charles-de-Gaulle',
            'greve aeroport Roissy Charles de Gaulle',
            'grève CDG aéroport',
            'strike Paris Charles de Gaulle airport',
        ],
        "aliases": [
            "charles-de-gaulle", "charles de gaulle", "roissy", "cdg",
            "paris-charles-de-gaulle", "paris charles de gaulle"
        ],
    },
    "ORLY": {
        "queries": [
            'grève aéroport Paris-Orly',
            'greve aeroport Paris Orly',
            'grève ORY aéroport',
            'strike Paris Orly airport',
        ],
        "aliases": [
            "orly", "ory", "paris-orly", "paris orly"
        ],
    },
}

STRIKE_KEYWORDS = [
    "grève", "greve", "strike", "walkout", "arrêt de travail", "arret de travail",
    "mouvement social", "social movement", "débrayage", "debrayage",
]

RSS_ENDPOINT = "https://news.google.com/rss/search?q={query}&hl=fr&gl=FR&ceid=FR:fr"


def get_output_dir() -> str:
    return os.getenv("REQUEST_OUTPUT_GREVES", "OutputDataGreves")


def normalize_text(text: str) -> str:
    text = html.unescape(text or "")
    text = text.lower()
    replacements = {
        "é": "e", "è": "e", "ê": "e", "ë": "e",
        "à": "a", "â": "a", "ä": "a",
        "î": "i", "ï": "i",
        "ô": "o", "ö": "o",
        "ù": "u", "û": "u", "ü": "u",
        "ç": "c",
        "œ": "oe",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def google_news_rss_url(query: str) -> str:
    q = f"{query} when:{NB_DAYS}d"
    return RSS_ENDPOINT.format(query=urllib.parse.quote(q))


def http_get(url: str) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/rss+xml, application/xml, text/xml;q=0.9, */*;q=0.8",
        },
    )
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
        return resp.read()


def parse_rss_items(xml_bytes: bytes) -> list[dict]:
    items = []
    root = ET.fromstring(xml_bytes)
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub_date = (item.findtext("pubDate") or "").strip()
        description = (item.findtext("description") or "").strip()
        source_el = item.find("source")
        source = source_el.text.strip() if source_el is not None and source_el.text else ""

        dt = None
        if pub_date:
            try:
                dt = parsedate_to_datetime(pub_date)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
            except Exception:
                dt = None

        items.append(
            {
                "title": title,
                "link": link,
                "pub_date": dt,
                "description": description,
                "source": source,
            }
        )
    return items


def is_relevant_for_airport(item: dict, airport_code: str) -> bool:
    haystack = " ".join(
        [
            item.get("title", ""),
            item.get("description", ""),
            item.get("link", ""),
            item.get("source", ""),
        ]
    )
    text = normalize_text(haystack)

    has_strike_kw = any(normalize_text(k) in text for k in STRIKE_KEYWORDS)
    has_airport_kw = any(
        normalize_text(alias) in text
        for alias in AIRPORTS[airport_code]["aliases"]
    )
    return has_strike_kw and has_airport_kw


def fetch_airport_articles(airport_code: str) -> list[dict]:
    seen = set()
    articles = []

    for query in AIRPORTS[airport_code]["queries"]:
        url = google_news_rss_url(query)
        try:
            xml_bytes = http_get(url)
            items = parse_rss_items(xml_bytes)
        except Exception as exc:
            print(f"[WARN] Échec pour {airport_code} / query={query!r}: {exc}", file=sys.stderr)
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            continue

        for item in items:
            if not item.get("pub_date"):
                continue
            if not is_relevant_for_airport(item, airport_code):
                continue

            dedup_key = (
                normalize_text(item.get("title", "")),
                item.get("link", ""),
                item["pub_date"].date().isoformat(),
            )
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            articles.append(item)

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    return articles


def build_rows() -> list[dict]:
    today = datetime.now(timezone.utc).date()
    start_date = today - timedelta(days=NB_DAYS - 1)

    index = defaultdict(lambda: defaultdict(list))

    for airport_code in AIRPORTS:
        print(f"[INFO] Recherche des articles pour {airport_code}...", file=sys.stderr)
        articles = fetch_airport_articles(airport_code)

        for art in articles:
            art_date = art["pub_date"].astimezone(timezone.utc).date()
            if art_date < start_date or art_date > today:
                continue

            title = art.get("title", "").strip()
            if title and title not in index[art_date][airport_code]:
                index[art_date][airport_code].append(title)

    rows = []
    current = start_date
    while current <= today:
        row = {"date": current.isoformat()}
        for airport_code in AIRPORTS:
            titles = index[current].get(airport_code, [])
            row[f"GREVE_{airport_code}"] = "Oui" if titles else "Non"
            row[f"LABEL_{airport_code}"] = " | ".join(titles)
        rows.append(row)
        current += timedelta(days=1)

    return rows


def write_csv(rows: list[dict], path: str) -> None:
    fieldnames = ["date"]
    for airport_code in AIRPORTS:
        fieldnames.append(f"GREVE_{airport_code}")
        fieldnames.append(f"LABEL_{airport_code}")

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)


def main():
    output_dir = get_output_dir()
    os.makedirs(output_dir, exist_ok=True)

    if len(sys.argv) < 2:
        print("Usage: python greves_aeroports_Single.py YYYY-MM-DD", file=sys.stderr)
        sys.exit(1)

    try:
        target_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    except ValueError:
        print("Format de date invalide. Utilisez YYYY-MM-DD.", file=sys.stderr)
        sys.exit(1)

    future_rows = []
    row = {"date": target_date.isoformat()}
    for airport_code in AIRPORTS:
        row[f"GREVE_{airport_code}"] = "Non"
        row[f"LABEL_{airport_code}"] = ""
    future_rows.append(row)

    output_path = os.path.join(output_dir, f"greve_aeroports_{target_date}.csv")
    write_csv(future_rows, output_path)
    print(f"[OK] CSV généré : {output_path}")


if __name__ == "__main__":
    main()