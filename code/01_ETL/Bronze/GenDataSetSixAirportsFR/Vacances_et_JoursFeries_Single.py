#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
from datetime import date, timedelta, datetime
import os

YEAR = 2025
OUTPUT_FILE = "histo_calendrier_jferies_et_vacances.csv"


def daterange(start_date: date, end_date: date):
    """Iterate from start_date to end_date included."""
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def easter_sunday(year: int) -> date:
    """
    Calcul de la date de Pâques (algorithme de Meeus/Jones/Butcher).
    Utile pour calculer les jours fériés mobiles en France.
    """
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def get_public_holidays_2025(year: int) -> dict:
    """
    Jours fériés nationaux France métropolitaine.
    """
    easter = easter_sunday(year)

    holidays = {
        date(year, 1, 1): "Jour de l'an",
        easter + timedelta(days=1): "Lundi de Pâques",
        date(year, 5, 1): "Fête du Travail",
        date(year, 5, 8): "Victoire 1945",
        easter + timedelta(days=39): "Ascension",
        easter + timedelta(days=50): "Lundi de Pentecôte",
        date(year, 7, 14): "Fête nationale",
        date(year, 8, 15): "Assomption",
        date(year, 11, 1): "Toussaint",
        date(year, 11, 11): "Armistice 1918",
        date(year, 12, 25): "Noël",
    }
    return holidays


def build_school_holidays_2025() -> dict:
    """
    Vacances scolaires 2025 pour les villes demandées :
      - PARIS -> zone C
      - TOULOUSE -> zone C
      - NICE -> zone B
      - MARSEILLE -> zone B
      - LYON -> zone A

    Règle officielle :
    - départ en vacances après la classe du jour indiqué
    - reprise le matin du jour indiqué

    Pour un export journalier simple, on marque comme "vacances"
    les jours entiers compris entre :
      début = lendemain de "fin des cours"
      fin   = veille de "reprise"

    Les 30 et 31 mai 2025 sont aussi marqués comme jours sans classe
    pour toutes les villes.
    """

    city_periods = {
        "PARIS": [
            ("Vacances de Noël", date(2024, 12, 21), date(2025, 1, 5)),
            ("Vacances d'hiver (février)", date(2025, 2, 22), date(2025, 3, 9)),
            ("Vacances de printemps", date(2025, 4, 19), date(2025, 5, 4)),
            ("Pont de l'Ascension", date(2025, 5, 30), date(2025, 5, 31)),
            ("Vacances d'été", date(2025, 7, 6), date(2025, 8, 31)),
            ("Vacances de Noël", date(2025, 12, 20), date(2026, 1, 4)),
            ("Vacances d'hiver (février)", date(2026, 2, 21), date(2026, 3, 8)),
            ("Vacances de printemps", date(2026, 4, 18), date(2026, 5, 3)),
            ("Pont de l'Ascension", date(2026, 5, 15), date(2026, 5, 17)),
        ],
        "TOULOUSE": [
            ("Vacances de Noël", date(2024, 12, 21), date(2025, 1, 5)),
            ("Vacances d'hiver (février)", date(2025, 2, 22), date(2025, 3, 9)),
            ("Vacances de printemps", date(2025, 4, 19), date(2025, 5, 4)),
            ("Pont de l'Ascension", date(2025, 5, 30), date(2025, 5, 31)),
            ("Vacances d'été", date(2025, 7, 6), date(2025, 8, 31)),
            ("Vacances de Noël", date(2025, 12, 20), date(2026, 1, 4)),
            ("Vacances d'hiver (février)", date(2026, 2, 21), date(2026, 3, 8)),
            ("Vacances de printemps", date(2026, 4, 18), date(2026, 5, 3)),
            ("Pont de l'Ascension", date(2026, 5, 15), date(2026, 5, 17)),
        ],
        "NICE": [
            ("Vacances de Noël", date(2024, 12, 21), date(2025, 1, 5)),
            ("Vacances d'hiver (février)", date(2025, 2, 15), date(2025, 3, 2)),
            ("Vacances de printemps", date(2025, 4, 12), date(2025, 4, 27)),
            ("Pont de l'Ascension", date(2025, 5, 30), date(2025, 5, 31)),
            ("Vacances d'été", date(2025, 7, 6), date(2025, 8, 31)),
            ("Vacances de Noël", date(2025, 12, 20), date(2026, 1, 4)),
            ("Vacances d'hiver (février)", date(2026, 2, 14), date(2026, 3, 1)),
            ("Vacances de printemps", date(2026, 4, 11), date(2026, 4, 26)),
            ("Pont de l'Ascension", date(2026, 5, 15), date(2026, 5, 17)),
        ],
        "MARSEILLE": [
            ("Vacances de Noël", date(2024, 12, 21), date(2025, 1, 5)),
            ("Vacances d'hiver (février)", date(2025, 2, 15), date(2025, 3, 2)),
            ("Vacances de printemps", date(2025, 4, 12), date(2025, 4, 27)),
            ("Pont de l'Ascension", date(2025, 5, 30), date(2025, 5, 31)),
            ("Vacances d'été", date(2025, 7, 6), date(2025, 8, 31)),
            ("Vacances de Noël", date(2025, 12, 20), date(2026, 1, 4)),
            ("Vacances d'hiver (février)", date(2026, 2, 14), date(2026, 3, 1)),
            ("Vacances de printemps", date(2026, 4, 11), date(2026, 4, 26)),
            ("Pont de l'Ascension", date(2026, 5, 15), date(2026, 5, 17)),
        ],
        "LYON": [
            ("Vacances de Noël", date(2024, 12, 21), date(2025, 1, 5)),
            ("Vacances d'hiver (février)", date(2025, 2, 8), date(2025, 2, 23)),
            ("Vacances de printemps", date(2025, 4, 5), date(2025, 4, 21)),
            ("Pont de l'Ascension", date(2025, 5, 30), date(2025, 5, 31)),
            ("Vacances d'été", date(2025, 7, 6), date(2025, 8, 31)),
            ("Vacances de Noël", date(2025, 12, 20), date(2026, 1, 4)),
            ("Vacances d'hiver (février)", date(2026, 2, 7), date(2026, 2, 22)),
            ("Vacances de printemps", date(2026, 4, 4), date(2026, 4, 20)),
            ("Pont de l'Ascension", date(2026, 5, 15), date(2026, 5, 17)),
        ],
    }

    school_holidays = {}

    for city, periods in city_periods.items():
        for label, start_dt, end_dt in periods:
            for d in daterange(start_dt, end_dt):
                school_holidays.setdefault(d, {})
                school_holidays[d][city] = label

    return school_holidays


def main():
    # Crée le dossier OutputJFVacances s'il n'existe pas
    output_dir = "OutputJFVacances"
    os.makedirs(output_dir, exist_ok=True)
    # Supprime tous les fichiers du dossier OutputJFVacances
    for f in os.listdir(output_dir):
        file_path = os.path.join(output_dir, f)
        if os.path.isfile(file_path):
            os.remove(file_path)

    public_holidays = get_public_holidays_2025(YEAR)
    school_holidays = build_school_holidays_2025()

    cities = ["PARIS", "NICE", "TOULOUSE", "MARSEILLE", "LYON"]

    city_col_map = {
        "PARIS": "Vacances PRS",
        "NICE": "Vacances NCE",
        "TOULOUSE": "Vacances TLS",
        "MARSEILLE": "Vacances MRS",
        "LYON": "Vacances LYS"
    }

    fieldnames = [
        "date",
        "Vacances Scolaires",
        "Label des Vacances",
        "Jour férié",
        "Label Jour Ferié",
        "Week End",
        "Vacances PRS",
        "Vacances NCE",
        "Vacances TLS",
        "Vacances MRS",
        "Vacances LYS",
    ]

    # Ajout : lecture de la date en argument
    import sys
    if len(sys.argv) < 2:
        print("Usage: python Vacances_et_JoursFeries_Single.py YYYY-MM-DD", file=sys.stderr)
        return
    try:
        target_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    except Exception:
        print("Date invalide. Format attendu: YYYY-MM-DD", file=sys.stderr)
        return

    output_hist = os.path.join(output_dir, f"single_calendrier_jferies_et_vacances_{target_date}.csv")
    with open(output_hist, mode="w", newline="", encoding="utf-8-sig") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        d = target_date
        city_values = {}
        vacation_cities = []
        vacation_labels = []
        for city in cities:
            in_vacation = d in school_holidays and city in school_holidays[d]
            city_values[city_col_map[city]] = 1 if in_vacation else 0
            if in_vacation:
                vacation_cities.append(city)
                vacation_labels.append(school_holidays[d][city])
        label_vacances = ""
        if vacation_labels:
            label_vacances = ", ".join(sorted(set(vacation_labels)))
        is_public_holiday = d in public_holidays
        holiday_label = public_holidays[d] if is_public_holiday else ""
        is_weekend = 1 if d.weekday() >= 5 else 0
        row = {
            "date": d.isoformat(),
            "Vacances Scolaires": ", ".join(vacation_cities),
            "Label des Vacances": label_vacances,
            "Jour férié": 1 if is_public_holiday else 0,
            "Label Jour Ferié": holiday_label,
            "Week End": is_weekend,
        }
        for city in cities:
            row[city_col_map[city]] = city_values[city_col_map[city]]
        writer.writerow(row)
    print(f"Fichier généré : {output_hist}")

    # Suppression de la génération du fichier "future" (non pertinente pour la version single-date)


if __name__ == "__main__":
    main()