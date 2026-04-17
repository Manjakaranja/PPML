"""
transform.py
Projet PPML - FlyOnTime
------------------------
Nettoyage des données (NaN, colonnes inutiles), feature engineering
sur les colonnes datetime, encodage des catégories et définition
de la variable cible (retard > 15 min).
"""

import numpy as np
import pandas as pd
from sklearn.preprocessing import OrdinalEncoder


# ---------------------------------------------------------------------------
# 1. DIAGNOSTICS
# ---------------------------------------------------------------------------

def check_missing(df: pd.DataFrame, name: str) -> None:
    """Affiche le taux de valeurs manquantes par colonne (colonnes > 0% seulement)."""
    print(f"\n🔍 ANALYSE : {name}")
    stats = (df.isna().mean() * 100).sort_values(ascending=False)
    stats_filtered = stats[stats > 0]
    if not stats_filtered.empty:
        print(stats_filtered.head(30))
    else:
        print("✅ Propre comme un sou neuf !")


def scan_total_vides(df: pd.DataFrame, name: str) -> None:
    """Diagnostic complet : NaN, chaînes vides, placeholders textuels, infinis."""
    print(f"\n🕵️  SCAN COMPLET : {name}")
    nans = df.isna().sum().sum()
    vides = (df == "").sum().sum()
    placeholders = ["none", "null", "unknown", "missing", "nan", "undefined"]
    text_vides = (
        df.apply(lambda x: x.astype(str).str.lower().isin(placeholders)).sum().sum()
    )
    infinites = np.isinf(df.select_dtypes(include=np.number)).sum().sum()

    print(f"  - NaNs classiques      : {nans}")
    print(f"  - Textes vides ('' )   : {vides}")
    print(f"  - Mots-clés 'vides'    : {text_vides}")
    print(f"  - Valeurs infinies     : {infinites}")


# ---------------------------------------------------------------------------
# 2. NETTOYAGE DES COLONNES
# ---------------------------------------------------------------------------

def purger_et_voir(df: pd.DataFrame, nom_dataset: str, seuil: float = 0.6) -> pd.DataFrame:
    """
    Garde uniquement les colonnes avec suffixe _dep/_arr, les clés métier
    et les colonnes liées aux retards. Supprime celles > `seuil` de NaN.
    Applique un ffill/bfill sur les valeurs restantes.
    """
    print(f"\n--- 🗑️  NETTOYAGE : {nom_dataset} ---")
    colonnes_avant = set(df.columns)
    cles = ["flight_number", "flight_date", "airport_origin", "airport_destination"]
    mots_cles_proteges = ["delay", "retard", "target"]

    cols_a_garder = [
        c
        for c in df.columns
        if c.endswith(("_dep", "_arr"))
        or c in cles
        or any(mot in c.lower() for mot in mots_cles_proteges)
    ]

    df_filtre = df[cols_a_garder].copy()

    taux_nan = df_filtre.isnull().mean()
    cols_trop_vides = [
        c
        for c in taux_nan[taux_nan > seuil].index
        if not any(mot in c.lower() for mot in mots_cles_proteges)
    ]

    df_final = df_filtre.drop(columns=cols_trop_vides)

    fantômes = colonnes_avant - set(cols_a_garder)
    print(f"👻 {len(fantômes)} colonnes sans suffixe supprimées.")
    print(f"🏜️  {len(cols_trop_vides)} colonnes > {seuil * 100:.0f}% vides supprimées.")
    print(
        f"🛡️  Colonnes de retard préservées : "
        f"{[c for c in df_final.columns if any(mot in c.lower() for mot in mots_cles_proteges)]}"
    )

    return df_final.ffill().bfill()


# ---------------------------------------------------------------------------
# 3. FEATURE ENGINEERING
# ---------------------------------------------------------------------------

DATETIME_COLS = ["flight_date", "scheduled_departure_dep", "scheduled_arrival_arr"]


def extract_date_features(df: pd.DataFrame, datetime_cols: list = None) -> pd.DataFrame:
    """
    Extrait mois, jour de la semaine et heure depuis les colonnes datetime,
    puis supprime les colonnes d'origine.
    """
    if datetime_cols is None:
        datetime_cols = DATETIME_COLS

    df = df.copy()
    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df[f"{col}_month"] = df[col].dt.month
            df[f"{col}_weekday"] = df[col].dt.dayofweek
            df[f"{col}_hour"] = df[col].dt.hour
            df = df.drop(columns=[col])
    return df


def encoder_categories(df: pd.DataFrame, encoder: OrdinalEncoder = None):
    """
    Encode les colonnes catégorielles avec un OrdinalEncoder.
    Retourne (df_encodé, encoder) pour pouvoir réutiliser le même encoder
    sur les données de prédiction.
    """
    cat_cols = df.select_dtypes(include=["object", "category"]).columns.tolist()

    if encoder is None:
        encoder = OrdinalEncoder(
            handle_unknown="use_encoded_value", unknown_value=-1
        )
        df[cat_cols] = encoder.fit_transform(df[cat_cols].astype(str))
    else:
        df[cat_cols] = encoder.transform(df[cat_cols].astype(str))

    return df, encoder


# ---------------------------------------------------------------------------
# 4. DÉFINITION DE LA CIBLE
# ---------------------------------------------------------------------------

def creer_cible(df: pd.DataFrame, seuil_minutes: int = 15) -> pd.DataFrame:
    """
    Crée la colonne binaire 'retard_arrivee' : 1 si retard > seuil_minutes.
    Cherche automatiquement la colonne de retard disponible.
    """
    colonnes_dispo = df.columns.tolist()
    nom_retard = next(
        (c for c in colonnes_dispo if "arrival_delay_min" in c), None
    )

    if nom_retard is None:
        raise ValueError("❌ Aucune colonne de retard trouvée pour créer la cible.")

    df["retard_arrivee"] = (df[nom_retard] > seuil_minutes).astype(int)
    df["target_retard"] = df["retard_arrivee"]

    print(f"✅ Cible créée depuis '{nom_retard}' (seuil : {seuil_minutes} min).")
    print(f"Répartition :\n{df['retard_arrivee'].value_counts()}")
    return df


# ---------------------------------------------------------------------------
# 5. PIPELINE PRINCIPAL
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Ce bloc suppose que extraction.py a déjà produit df_train_final et df_predict_final.
    # En pratique, importer ces variables depuis extraction.py ou les charger depuis un fichier intermédiaire.
    from extraction import reparer_vols, charger_data_s3, bilan_sante

    df_train = charger_data_s3("datasets/SignofFlightsDataset_20260416_233018_CLEAN.csv")
    df_predict = charger_data_s3(
        "datasets/SignofFlightsDataset_future_20260416_233150_CLEAN.csv"
    )

    df_train_final = reparer_vols(df_train)
    df_predict_final = reparer_vols(df_predict)

    # Diagnostics
    check_missing(df_train_final, "TRAIN")
    check_missing(df_predict_final, "PREDICT")

    # Nettoyage
    df_train_final = purger_et_voir(df_train_final, "TRAIN")
    df_predict_final = purger_et_voir(df_predict_final, "PREDICT")

    scan_total_vides(df_train_final, "TRAIN")
    scan_total_vides(df_predict_final, "PREDICT")

    # Vérification cohérence des colonnes
    diff = set(df_train_final.columns) ^ set(df_predict_final.columns)
    print(
        f"\n✅ Colonnes identiques : {not diff}"
        if not diff
        else f"\n⚠️  Différences : {diff}"
    )

    # Cible
    df_train_final = creer_cible(df_train_final)
