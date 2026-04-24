"""
transformation.py
Projet PPML - FlyOnTime
------------------------
Transformations du dataset single flight issu de extraction.py.

Objectif ici :
- nettoyer
- réparer les vols si nécessaire
- garder une structure cohérente avec df_train_final
- NE PAS casser la nomenclature notebook XGBoost
- sauvegarder le parquet final dans /data
"""

from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.preprocessing import OrdinalEncoder

from preprocessing.extraction import extract_single_flight_dataset_from_s3


# =========================================================
# CONFIG
# =========================================================
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


# =========================================================
# DIAGNOSTICS
# =========================================================
def check_missing(df: pd.DataFrame, name: str) -> None:
    print(f"\n🔍 ANALYSE : {name}")
    stats = (df.isna().mean() * 100).sort_values(ascending=False)
    stats_filtered = stats[stats > 0]
    if not stats_filtered.empty:
        print(stats_filtered.head(30))
    else:
        print("✅ Propre comme un sou neuf !")


def scan_total_vides(df: pd.DataFrame, name: str) -> None:
    print(f"\n🕵️ SCAN COMPLET : {name}")
    nans = df.isna().sum().sum()
    vides = (df == "").sum().sum()
    placeholders = ["none", "null", "unknown", "missing", "nan", "undefined"]
    text_vides = (
        df.apply(lambda x: x.astype(str).str.lower().isin(placeholders)).sum().sum()
    )
    infinites = np.isinf(df.select_dtypes(include=np.number)).sum().sum()

    print(f"  - NaNs classiques      : {nans}")
    print(f"  - Textes vides ('')    : {vides}")
    print(f"  - Mots-clés 'vides'    : {text_vides}")
    print(f"  - Valeurs infinies     : {infinites}")


# =========================================================
# RÉPARATION / HARMONISATION
# =========================================================
def reparer_vols_si_necessaire(df: pd.DataFrame) -> pd.DataFrame:
    """
    Si les lignes départ/arrivée sont encore séparées,
    on fusionne en une seule observation.
    Si le fichier est déjà clean / fusionné, on ne casse rien.
    """
    df = df.copy()

    required_cols = {
        "flight_number",
        "flight_date",
        "airport_origin",
        "airport_destination",
        "movement_type",
    }

    if not required_cols.issubset(df.columns):
        print("ℹ️ Pas de réparation nécessaire : colonnes de fusion absentes.")
        return df

    dates_cols = ["flight_date", "scheduled_departure", "scheduled_arrival"]
    for col in dates_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    cles = ["flight_number", "flight_date", "airport_origin", "airport_destination"]
    masque = df.duplicated(subset=cles, keep=False)

    vols_solo = df[~masque].copy()
    vols_a_reparer = df[masque].copy()

    if vols_a_reparer.empty:
        print("ℹ️ Aucun vol dupliqué à réparer.")
        return df

    dep = vols_a_reparer[vols_a_reparer["movement_type"] == "departure"].sort_values(
        "scheduled_departure"
    )
    arr = vols_a_reparer[vols_a_reparer["movement_type"] == "arrival"].sort_values(
        "scheduled_arrival"
    )

    dep["n_vol"] = dep.groupby(cles).cumcount()
    arr["n_vol"] = arr.groupby(cles).cumcount()

    df_reconstruit = pd.merge(
        dep, arr, on=cles + ["n_vol"], suffixes=("_dep", "_arr")
    )

    df_final = pd.concat([vols_solo, df_reconstruit], ignore_index=True)
    print(f"✅ Réparation vols effectuée : {df.shape} -> {df_final.shape}")
    return df_final


def harmonize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Important :
    on NE renomme PAS scheduled_departure_dep / scheduled_arrival_arr
    pour rester fidèle aux notebooks XGBoost.
    """
    df = df.copy()
    rename_map = {}

    if "movement_date_dep" in df.columns and "movement_date" not in df.columns:
        rename_map["movement_date_dep"] = "movement_date"

    if "status_dep" in df.columns and "status" not in df.columns:
        rename_map["status_dep"] = "status"

    if rename_map:
        print(f"✅ Rename colonnes : {rename_map}")
        df = df.rename(columns=rename_map)

    return df


# =========================================================
# NETTOYAGE
# =========================================================
def purger_et_voir(df: pd.DataFrame, nom_dataset: str, seuil: float = 0.6) -> pd.DataFrame:
    print(f"\n--- 🗑️ NETTOYAGE : {nom_dataset} ---")
    colonnes_avant = set(df.columns)
    cles = ["flight_number", "flight_date", "airport_origin", "airport_destination"]
    mots_cles_proteges = ["delay", "retard", "target"]

    cols_a_garder = [
        c
        for c in df.columns
        if c.endswith(("_dep", "_arr"))
        or c in cles
        or any(mot in c.lower() for mot in mots_cles_proteges)
        or c in ["scheduled_departure", "scheduled_arrival", "movement_date", "status"]
    ]

    df_filtre = df[cols_a_garder].copy()

    taux_nan = df_filtre.isnull().mean()
    cols_trop_vides = [
        c
        for c in taux_nan[taux_nan > seuil].index
        if not any(mot in c.lower() for mot in mots_cles_proteges)
    ]

    df_final = df_filtre.drop(columns=cols_trop_vides)

    fantomes = colonnes_avant - set(cols_a_garder)
    print(f"👻 {len(fantomes)} colonnes supprimées.")
    print(f"🏜️ {len(cols_trop_vides)} colonnes > {seuil * 100:.0f}% vides supprimées.")
    print(
        f"🛡️ Colonnes retard préservées : "
        f"{[c for c in df_final.columns if any(mot in c.lower() for mot in mots_cles_proteges)]}"
    )

    return df_final.ffill().bfill()


# =========================================================
# ENCODAGE OPTIONNEL
# =========================================================
def encoder_categories(df: pd.DataFrame, encoder: OrdinalEncoder | None = None):
    df = df.copy()
    cat_cols = df.select_dtypes(include=["object", "category"]).columns.tolist()

    if not cat_cols:
        return df, encoder

    if encoder is None:
        encoder = OrdinalEncoder(
            handle_unknown="use_encoded_value",
            unknown_value=-1
        )
        df[cat_cols] = encoder.fit_transform(df[cat_cols].astype(str))
    else:
        df[cat_cols] = encoder.transform(df[cat_cols].astype(str))

    return df, encoder


# =========================================================
# SAVE
# =========================================================
def build_output_name(request_id: str) -> str:
    return f"single_flight_model_input_{request_id}.parquet"


def save_transformed_parquet(df: pd.DataFrame, request_id: str) -> Path:
    output_name = build_output_name(request_id)
    output_path = DATA_DIR / output_name
    df.to_parquet(output_path, index=False)
    print(f"✅ Parquet sauvegardé : {output_path}")
    return output_path


# =========================================================
# PIPELINE PRINCIPAL
# =========================================================
def transform_single_flight_dataset(
    request_id: str,
    run_date: str,
    bucket_name: str | None = None,
    encode_categories: bool = False,
    encoder: OrdinalEncoder | None = None,
    save_output: bool = True,
):
    """
    Pipeline complet de transformation.
    Une requête = un user input = un request_id.
    """
    df = extract_single_flight_dataset_from_s3(
        request_id=request_id,
        run_date=run_date,
        bucket_name=bucket_name,
    )

    check_missing(df, "RAW_SINGLE")
    scan_total_vides(df, "RAW_SINGLE")

    df = reparer_vols_si_necessaire(df)
    df = harmonize_columns(df)
    df = purger_et_voir(df, "SINGLE_FLIGHT")

    if encode_categories:
        df, encoder = encoder_categories(df, encoder=encoder)

    output_path = None
    if save_output:
        output_path = save_transformed_parquet(df, request_id=request_id)

    print(f"\n✅ Dataset final transformé : {df.shape}")
    return df, encoder, output_path


if __name__ == "__main__":
    request_id = "requete_AF1234_20260421_183500"
    run_date = "2026-04-21"

    df_final, _, output_path = transform_single_flight_dataset(
        request_id=request_id,
        run_date=run_date,
        encode_categories=False,
        save_output=True,
    )
    print(df_final.head())