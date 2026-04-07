import pandas as pd
from datetime import datetime
import holidays


def enrich_aircraft_features(
    df: pd.DataFrame, model_column: str = "aircraft_model"
) -> pd.DataFrame:
    """
    Enrichit avec aircraft_family + num_seats (plus précis) + autres features.
    """
    df = df.copy()

    # 1. Nettoyage famille
    def clean_family(family):
        if pd.isna(family) or str(family).strip().lower() in ["unknown", "null", ""]:
            return "Other"
        f = str(family).upper().strip()

        if any(x in f for x in ["A320", "A319", "A321", "A318"]):
            if "NEO" in f or "SHARKLETS" in f:
                return "Airbus A320 NEO"
            return "Airbus A320 Family"
        elif "A350" in f:
            return "Airbus A350"
        elif "A330" in f:
            return "Airbus A330"
        elif "A380" in f:
            return "Airbus A380"
        elif "A220" in f or "CS300" in f:
            return "Airbus A220"
        elif "B737" in f or "737" in f:
            if "MAX" in f:
                return "Boeing 737 MAX"
            return "Boeing 737 Family"
        elif "B777" in f or "777" in f:
            return "Boeing 777"
        elif "B787" in f or "787" in f:
            return "Boeing 787"
        elif any(x in f for x in ["E190", "E195", "E170", "E175", "ERJ", "EMB"]):
            return "Embraer E-Jet"
        elif "ATR 72" in f:
            return "ATR 72"
        elif "ATR 42" in f:
            return "ATR 42"
        elif "CRJ" in f:
            return "Bombardier CRJ"
        elif "Q400" in f or "DHC-8" in f:
            return "Bombardier Dash 8 Q400"
        elif "B767" in f:
            return "Boeing 767"
        elif "B757" in f:
            return "Boeing 757"
        elif "B747" in f:
            return "Boeing 747"
        else:
            return "Other"

    df["aircraft_family"] = df[model_column].apply(clean_family)

    # 2. Nombre de places plus précis (basé sur configurations courantes)
    seats_mapping = {
        "Airbus A320 Family": 165,  # moyenne réelle A319/A320/A321
        "Airbus A320 NEO": 180,
        "Airbus A220": 135,
        "Boeing 737 Family": 155,
        "Boeing 737 MAX": 178,
        "Boeing 777": 355,
        "Boeing 787": 295,
        "Airbus A350": 325,
        "Airbus A330": 275,
        "Airbus A380": 525,
        "Embraer E-Jet": 100,
        "ATR 72": 68,
        "ATR 42": 48,
        "Bombardier CRJ": 85,
        "Bombardier Dash 8 Q400": 78,
        "Boeing 767": 260,
        "Boeing 757": 200,
        "Boeing 747": 410,
        "Other": 150,
    }

    df["num_seats"] = df["aircraft_family"].map(seats_mapping).fillna(150).astype(int)

    # 3. Autres features
    df["is_widebody"] = df["aircraft_family"].isin(
        [
            "Airbus A330",
            "Airbus A350",
            "Airbus A380",
            "Boeing 777",
            "Boeing 787",
            "Boeing 767",
            "Boeing 747",
        ]
    )

    df["is_narrowbody"] = df["aircraft_family"].isin(
        [
            "Airbus A320 Family",
            "Airbus A220",
            "Boeing 737 Family",
            "Boeing 737 MAX",
            "Embraer E-Jet",
        ]
    )

    df["is_regional"] = df["aircraft_family"].isin(
        [
            "ATR 72",
            "ATR 42",
            "Bombardier CRJ",
            "Bombardier Dash 8 Q400",
            "Embraer E-Jet",
        ]
    )

    # Taille catégorie
    def get_size_category(family):
        if family in [
            "Airbus A380",
            "Boeing 777",
            "Boeing 787",
            "Airbus A350",
            "Boeing 747",
        ]:
            return "Very Large"
        elif family in ["Airbus A330", "Boeing 767"]:
            return "Large"
        elif family in [
            "Airbus A320 Family",
            "Boeing 737 Family",
            "Boeing 737 MAX",
            "Airbus A220",
        ]:
            return "Medium"
        else:
            return "Small"

    df["aircraft_size_category"] = df["aircraft_family"].apply(get_size_category)

    df["is_freighter"] = False  # On pourra l'améliorer plus tard si tu as l'info

    print(f" Features aircraft enrichies avec num_seats")
    print(f"   - aircraft_family : {df['aircraft_family'].nunique()} catégories")
    print(f"   - num_seats       : ajouté (moyenne réaliste par famille)")

    return df


def add_holiday_features(
    df: pd.DataFrame, date_column: str = "scheduled_utc"
) -> pd.DataFrame:
    """
    Ajoute les features jours fériés + weekend sans nécessiter la colonne day_of_week.
    """
    df = df.copy()

    # Conversion en datetime si ce n'est pas déjà le cas
    if not pd.api.types.is_datetime64_any_dtype(df[date_column]):
        df[date_column] = pd.to_datetime(df[date_column], errors="coerce")

    # Extraire la date (sans heure)
    df["flight_date"] = df[date_column].dt.date

    # Charger les jours fériés français (2024 à 2028)
    fr_holidays = holidays.FR(years=range(2024, 2029))

    # Features jours fériés
    df["is_holiday"] = df["flight_date"].apply(lambda x: 1 if x in fr_holidays else 0)

    df["is_holiday_eve"] = df["flight_date"].apply(
        lambda x: 1 if (x + pd.Timedelta(days=1)) in fr_holidays else 0
    )

    df["is_holiday_next"] = df["flight_date"].apply(
        lambda x: 1 if (x - pd.Timedelta(days=1)) in fr_holidays else 0
    )

    # Feature weekend (samedi=5, dimanche=6)
    df["is_weekend"] = df[date_column].dt.dayofweek.isin([5, 6]).astype(int)

    # Feature combinée (weekend OU jour férié)
    df["is_weekend_or_holiday"] = (
        (df["is_weekend"] == 1) | (df["is_holiday"] == 1)
    ).astype(int)

    # Nom du jour férié (utile pour analyse)
    df["holiday_name"] = df["flight_date"].apply(lambda x: fr_holidays.get(x, None))

    print(" Features jours fériés ajoutées :")
    print(f"   Jours fériés       : {df['is_holiday'].sum():,} vols")
    print(f"   Veilles fériées    : {df['is_holiday_eve'].sum():,} vols")
    print(f"   Lendemains fériés  : {df['is_holiday_next'].sum():,} vols")
    print(f"   Week-ends          : {df['is_weekend'].sum():,} vols")
    print(f"   Weekend ou férié   : {df['is_weekend_or_holiday'].sum():,} vols")

    # Nettoyage de la colonne temporaire
    df = df.drop(columns=["flight_date"], errors="ignore")

    return df
