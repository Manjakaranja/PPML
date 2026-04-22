from __future__ import annotations

import os
from io import BytesIO
from pathlib import Path
from typing import Optional

import boto3
import pandas as pd


# CONFIG

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_BUCKET = os.getenv("S3_BUCKET_NAME", "ppml2026")
DEFAULT_PREFIX = os.getenv("S3_PREFIX", "raw")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "eu-north-1")


# HELPERS S3

def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=AWS_REGION,
    )


def build_request_prefix(run_date: str, request_id: str) -> str:
    """
    Exemple :
    raw/2026-04-21/requete_AF1234_20260421_183500/
    """
    return f"{DEFAULT_PREFIX}/{run_date}/{request_id}/"


def find_request_parquet_in_s3(
    request_id: str,
    run_date: str,
    bucket_name: Optional[str] = None,
) -> str:
    """
    Trouve le parquet final correspondant à une requête user.
    Retourne la clé S3 complète.
    """
    bucket_name = bucket_name or DEFAULT_BUCKET
    s3 = get_s3_client()

    prefix = build_request_prefix(run_date=run_date, request_id=request_id)
    print(f"Recherche S3 dans : s3://{bucket_name}/{prefix}")

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if "Contents" not in response:
        raise FileNotFoundError(
            f"Aucun objet trouvé pour la requête : s3://{bucket_name}/{prefix}"
        )

    candidates = []
    for obj in response["Contents"]:
        key = obj["Key"]
        filename = key.split("/")[-1]

        if (
            filename.startswith("SignoffFlightsDataset_Single_")
            and filename.endswith(".parquet")
        ):
            candidates.append(key)

    if not candidates:
        raise FileNotFoundError(
            f"Aucun parquet final trouvé dans s3://{bucket_name}/{prefix}"
        )

    if len(candidates) > 1:
        print(f"Plusieurs parquets trouvés, on prend le dernier trié : {candidates}")

    selected_key = sorted(candidates)[-1]
    print(f"Parquet trouvé : s3://{bucket_name}/{selected_key}")
    return selected_key


def charger_data_s3_parquet(
    request_id: str,
    run_date: str,
    bucket_name: Optional[str] = None,
) -> pd.DataFrame:
    """
    Charge le parquet final S3 correspondant à la requête user.
    """
    bucket_name = bucket_name or DEFAULT_BUCKET
    s3 = get_s3_client()

    s3_key = find_request_parquet_in_s3(
        request_id=request_id,
        run_date=run_date,
        bucket_name=bucket_name,
    )

    print(f"Chargement du parquet : s3://{bucket_name}/{s3_key}")
    obj = s3.get_object(Bucket=bucket_name, Key=s3_key)
    content = obj["Body"].read()

    df = pd.read_parquet(BytesIO(content))

    if df.empty:
        raise ValueError(f"Le parquet chargé est vide : s3://{bucket_name}/{s3_key}")

    print(f"Dataset S3 chargé : s3://{bucket_name}/{s3_key}")
    print(f"Format : {df.shape}")
    return df


# VÉRIFICATIONS

def verifier_colonnes_minimales(df: pd.DataFrame) -> None:
    colonnes_attendues = [
        "flight_number",
        "flight_date",
        "airport_origin",
        "airport_destination",
    ]

    manquantes = [c for c in colonnes_attendues if c not in df.columns]
    if manquantes:
        raise ValueError(f"Colonnes minimales manquantes : {manquantes}")

    print(f"Colonnes minimales présentes : {colonnes_attendues}")


def verifier_doublons(df: pd.DataFrame, nom: str = "DATA") -> None:
    doublons = df[df.duplicated(subset=["flight_number", "flight_date"], keep=False)]
    print(f"Doublons dans {nom} : {len(doublons)}")


def bilan_sante(df: pd.DataFrame, nom: str = "DATA") -> None:
    nb_lignes = len(df)
    nb_colonnes = len(df.columns)
    nb_na = int(df.isna().sum().sum())

    print(f"\n🩺 Bilan de santé - {nom}")
    print(f"  - lignes   : {nb_lignes}")
    print(f"  - colonnes : {nb_colonnes}")
    print(f"  - NaN total: {nb_na}")


def normaliser_flight_number(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "flight_number" in df.columns:
        df["flight_number"] = (
            df["flight_number"]
            .astype(str)
            .str.replace(" ", "", regex=False)
            .str.upper()
            .str.strip()
        )
    return df


# PIPELINE EXTRACTION

def extract_single_flight_dataset_from_s3(
    request_id: str,
    run_date: str,
    bucket_name: Optional[str] = None,
) -> pd.DataFrame:
    """
    Extraction pure depuis S3 pour UNE requête utilisateur.
    Une requête = un user input = un request_id.
    """
    df = charger_data_s3_parquet(
        request_id=request_id,
        run_date=run_date,
        bucket_name=bucket_name,
    )

    df = normaliser_flight_number(df)

    verifier_colonnes_minimales(df)
    verifier_doublons(df, "SINGLE_LOOKUP_S3")
    bilan_sante(df, "SINGLE_LOOKUP_S3")

    return df


if __name__ == "__main__":
    # Exemple de test manuel
    df = extract_single_flight_dataset_from_s3(
        request_id="requete_AF1234_20260421_183500",
        run_date="2026-04-21",
    )
    print("\nAperçu :")
    print(df.head())