import pandas as pd
import numpy as np


def clean_flight_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoyage des données pour la prédiction uniquement.
    - Garde scheduled_utc pour l'affichage dans Streamlit
    - Supprime les colonnes inutiles/leakage
    """
    df = df.copy()
    initial_shape = len(df)

    print(f"🧹 Réception de {initial_shape:,} vols pour prédiction...")

    # Colonnes à supprimer (leakage + inutiles pour le modèle)
    cols_to_drop = [
        # "flight_number",
        "airport_name",
        "revised_utc",
        "runway_utc",
        "meteo_hour_start",
        "scheduled_hour_bin",
        "meteo_hour_bin_used",
        "flight_date",
        "status",
        "mouvement_id",  # On peut le supprimer ici car on le récupère avant le nettoyage
    ]

    df = df.drop(columns=[col for col in cols_to_drop if col in df.columns])

    # Filtre critique : on doit avoir scheduled_utc
    if "scheduled_utc" in df.columns:
        before = len(df)
        df = df[df["scheduled_utc"].notna()].copy()
        removed = before - len(df)
        if removed > 0:
            print(f" → {removed:,} vols supprimés car scheduled_utc manquant")

    # ====================== NETTOYAGE COMMUN ======================
    df["terminal_dep"] = df["terminal_dep"].fillna("Unknown")
    df["terminal_arr"] = df["terminal_arr"].fillna("Unknown")

    # Correction destination_icao pour les arrivals
    if all(col in df.columns for col in ["type", "icao", "destination_icao"]):
        mask_arrival = (df["type"] == "arrival") & (df["destination_icao"].isna())
        df.loc[mask_arrival, "destination_icao"] = df.loc[mask_arrival, "icao"]

    df["destination_icao"] = df["destination_icao"].fillna("UNKNOWN")

    # Remplissage intelligent des NaN météo par aéroport
    meteo_cols = [
        "temperature_2m",
        "relative_humidity_2m",
        "wind_speed_10m",
        "wind_gusts_10m",
        "pressure_msl",
        "precipitation",
        "cloud_cover",
    ]
    for col in meteo_cols:
        if col in df.columns:
            df[col] = df.groupby("icao")[col].transform(lambda x: x.fillna(x.median()))
            df[col] = df[col].fillna(
                df[col].median() if not pd.isna(df[col].median()) else 0
            )

    # Features temporelles
    df["scheduled_hour"] = df["scheduled_hour"].fillna(12.0)
    df["day_of_week"] = df["day_of_week"].fillna(0.0)

    df["is_weekend"] = df["day_of_week"].isin([0, 6]).astype(int)
    df["hour_sin"] = np.sin(2 * np.pi * df["scheduled_hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["scheduled_hour"] / 24)

    df["day_of_week"] = df["day_of_week"].astype(int)
    df["scheduled_hour"] = df["scheduled_hour"].astype(int)

    print(
        f"✅ Nettoyage terminé | Shape : {df.shape} | NaN max = {df.isnull().mean().max():.3f}%"
    )

    return df
