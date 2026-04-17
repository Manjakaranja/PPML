"""
extraction.py
Projet PPML - FlyOnTime
------------------------
Chargement des données depuis AWS S3 et réparation des vols dupliqués
(fusion des lignes départ/arrivée en une ligne unique par vol).
"""

import os
import pandas as pd
import boto3


# ---------------------------------------------------------------------------
# 1. CHARGEMENT DEPUIS S3
# ---------------------------------------------------------------------------

def charger_data_s3(chemin_fichier: str) -> pd.DataFrame:
    """Récupère un fichier CSV ou Parquet depuis le bucket S3 de l'équipe."""
    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID_EQUIPE"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY_EQUIPE"),
        region_name="eu-north-1",
    )

    nom_bucket = os.getenv("BUCKET_EQUIPE")
    url_complete = f"s3://{nom_bucket}/{chemin_fichier}"

    print(f"Récupération du fichier : {chemin_fichier}...")

    if chemin_fichier.endswith(".csv"):
        return pd.read_csv(url_complete)
    else:
        return pd.read_parquet(url_complete)


# ---------------------------------------------------------------------------
# 2. RÉPARATION DES VOLS (fusion départ + arrivée)
# ---------------------------------------------------------------------------

def reparer_vols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Certains vols sont représentés par deux lignes (une départ, une arrivée).
    Cette fonction les fusionne en une seule observation complète.
    """
    # Suppression de la ligne technique et conversion des dates
    df = df.iloc[1:].reset_index(drop=True)

    dates_cols = ["flight_date", "scheduled_departure", "scheduled_arrival"]
    for col in dates_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    cles = ["flight_number", "flight_date", "airport_origin", "airport_destination"]
    masque = df.duplicated(subset=cles, keep=False)

    vols_solo = df[~masque].copy()
    vols_a_reparer = df[masque].copy()

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

    return pd.concat([vols_solo, df_reconstruit], ignore_index=True)


# ---------------------------------------------------------------------------
# 3. VÉRIFICATIONS
# ---------------------------------------------------------------------------

def verifier_doublons(df: pd.DataFrame, nom: str) -> None:
    """Affiche un bilan des doublons sur flight_number + flight_date."""
    doublons = df[df.duplicated(subset=["flight_number", "flight_date"], keep=False)]
    print(f"⚠️  Doublons dans {nom} : {len(doublons)}")


def bilan_sante(df: pd.DataFrame, nom: str) -> None:
    """Vérifie qu'il ne reste plus de vrais doublons après réparation."""
    colonnes_cles = ["flight_number", "flight_date", "scheduled_departure"]
    nb = df.duplicated(subset=colonnes_cles).sum()
    print(f"Bilan de santé {nom} : {nb} vrais doublons.")
    if nb == 0:
        print("✅ Nickel ! Tout est propre, chaque vol est désormais une ligne unique.")
    else:
        print("⚠️  Attention, il reste encore des rescapés à checker.")


# ---------------------------------------------------------------------------
# 4. PIPELINE PRINCIPAL
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Chargement
    df_train = charger_data_s3(
        "datasets/SignofFlightsDataset_20260416_233018_CLEAN.csv"
    )
    df_predict = charger_data_s3(
        "datasets/SignofFlightsDataset_future_20260416_233150_CLEAN.csv"
    )

    print(f"Format Train   : {df_train.shape}")
    print(f"Format Predict : {df_predict.shape}")

    # Vérification initiale
    verifier_doublons(df_train, "TRAIN")
    verifier_doublons(df_predict, "PREDICT")

    # Réparation
    df_train_final = reparer_vols(df_train)
    df_predict_final = reparer_vols(df_predict)

    print(f"✅ TRAIN   : {len(df_train_final)} vols prêts.")
    print(f"✅ PREDICT : {len(df_predict_final)} vols prêts.")

    # Bilan de santé
    bilan_sante(df_train_final, "TRAIN")
    bilan_sante(df_predict_final, "PREDICT")
