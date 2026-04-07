# src/comparison.py

import pandas as pd
from datetime import datetime


def match_predictions_with_reals(
    df_pred: pd.DataFrame, df_real: pd.DataFrame
) -> pd.DataFrame:
    """
    Matching robuste entre prédictions et vols réels.
    """
    print("Colonnes df_pred :", df_pred.columns.tolist())
    print("Colonnes df_real :", df_real.columns.tolist())

    if df_pred.empty or df_real.empty:
        print("Un des DataFrames est vide")
        return pd.DataFrame()

    pred = df_pred.copy()
    real = df_real.copy()

    # Normalisation des noms de colonnes (au cas où)
    pred.rename(columns=lambda x: x.lower().strip(), inplace=True)
    real.rename(columns=lambda x: x.lower().strip(), inplace=True)

    # Clés possibles pour le matching
    common_keys = ["icao", "flight_number", "scheduled_utc"]

    # Vérification des colonnes disponibles
    for key in common_keys:
        if key not in pred.columns:
            print(f"Colonne manquante dans df_pred : {key}")
        if key not in real.columns:
            print(f"Colonne manquante dans df_real : {key}")

    # Conversion scheduled_utc en datetime pour faciliter le matching
    for df in [pred, real]:
        if "scheduled_utc" in df.columns:
            df["scheduled_utc"] = pd.to_datetime(df["scheduled_utc"], errors="coerce")

    # Matching avec tolérance sur l'heure
    # On crée une clé basée sur icao + flight_number + date + heure tronquée
    for df in [pred, real]:
        df["match_key"] = (
            df["icao"].astype(str).str.upper()
            + "_"
            + df.get("flight_number", pd.Series([""] * len(df))).astype(str)
            + "_"
            + df["scheduled_utc"].dt.strftime("%Y-%m-%d %H")  # tronqué à l'heure
        )

    # Merge
    df_merged = pd.merge(
        pred, real, on="match_key", how="inner", suffixes=("_pred", "_real")
    )

    if df_merged.empty:
        print("Aucun matching trouvé avec la clé actuelle")
        return pd.DataFrame()

    # Calcul des erreurs
    df_merged["error_minutes"] = df_merged["predicted_delay_minutes"] - df_merged.get(
        "delay_minutes", 0
    )
    df_merged["absolute_error"] = abs(df_merged["error_minutes"])

    # Colonnes finales
    final_cols = [
        "icao_pred",
        "flight_number_pred",
        "scheduled_utc_pred",
        "predicted_delay_minutes",
        "delay_minutes",
        "error_minutes",
        "absolute_error",
        "airline_pred",
        "type_pred",
    ]

    result = df_merged[[col for col in final_cols if col in df_merged.columns]].copy()

    result.rename(
        columns={
            "icao_pred": "icao",
            "flight_number_pred": "flight_number",
            "scheduled_utc_pred": "scheduled_utc",
            "airline_pred": "airline",
            "type_pred": "type",
        },
        inplace=True,
    )

    print(f" Matching réussi : {len(result)} vols appariés")

    return result


def calculate_metrics(df_comparison: pd.DataFrame) -> dict:
    """
    Calcule les métriques d'évaluation à partir du DataFrame de comparaison.
    """
    if df_comparison.empty:
        return {}

    metrics = {
        "mae": round(df_comparison["absolute_error"].mean(), 2),
        "rmse": round((df_comparison["error_minutes"] ** 2).mean() ** 0.5, 2),
        "median_error": round(df_comparison["absolute_error"].median(), 2),
        "mean_error": round(df_comparison["error_minutes"].mean(), 2),
        "nombre_vols": len(df_comparison),
        "pourcentage_dans_15min": round(
            (df_comparison["absolute_error"] <= 15).mean() * 100, 1
        ),
        "pourcentage_dans_30min": round(
            (df_comparison["absolute_error"] <= 30).mean() * 100, 1
        ),
        "pourcentage_dans_60min": round(
            (df_comparison["absolute_error"] <= 60).mean() * 100, 1
        ),
        "max_erreur": round(df_comparison["absolute_error"].max(), 1),
        "vols_en_avance": (df_comparison["error_minutes"] < 0).sum(),
        "vols_en_retard": (df_comparison["error_minutes"] > 0).sum(),
    }

    return metrics
