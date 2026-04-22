"""
load.py
Projet PPML - FlyOnTime
-----------------------
Chargement du parquet transformé local vers S3.

Rôle :
- retrouver le bon parquet local produit par transformation.py
- l'uploader dans S3 avec un chemin propre à la requête
- retourner le chemin local et la clé S3 finale
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import boto3
import pandas as pd


# =========================================================
# CONFIG
# =========================================================
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "eu-north-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "ppml2026")

# Préfixe du parquet transformé
# raw = signoff brut
# processed = parquet transformé prêt pour l'inférence
TRANSFORMED_S3_PREFIX = os.getenv("TRANSFORMED_S3_PREFIX", "processed")


# =========================================================
# HELPERS
# =========================================================
def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=AWS_REGION,
    )


def build_local_output_name(request_id: str) -> str:
    return f"single_flight_model_input_{request_id}.parquet"


def build_local_output_path(request_id: str) -> Path:
    return DATA_DIR / build_local_output_name(request_id)


def build_s3_key(request_id: str, run_date: str) -> str:
    filename = build_local_output_name(request_id)
    return f"{TRANSFORMED_S3_PREFIX}/{run_date}/{request_id}/{filename}"


# =========================================================
# LECTURE LOCALE
# =========================================================
def load_local_transformed_parquet(request_id: str) -> pd.DataFrame:
    local_path = build_local_output_path(request_id)

    if not local_path.exists():
        raise FileNotFoundError(f"Parquet local introuvable : {local_path}")

    df = pd.read_parquet(local_path)

    if df.empty:
        raise ValueError(f"Le parquet local est vide : {local_path}")

    print(f"✅ Parquet local chargé : {local_path}")
    print(f"Format : {df.shape}")
    return df


# =========================================================
# UPLOAD S3
# =========================================================
def upload_local_parquet_to_s3(
    request_id: str,
    run_date: str,
    bucket_name: Optional[str] = None,
) -> dict:
    bucket_name = bucket_name or S3_BUCKET_NAME
    local_path = build_local_output_path(request_id)

    if not local_path.exists():
        raise FileNotFoundError(f"Parquet local introuvable : {local_path}")

    s3_key = build_s3_key(request_id=request_id, run_date=run_date)
    s3 = get_s3_client()

    print("📤 Upload du parquet local vers S3")
    print(f"   Local : {local_path}")
    print(f"   S3    : s3://{bucket_name}/{s3_key}")

    s3.upload_file(str(local_path), bucket_name, s3_key)

    print("✅ Upload S3 terminé avec succès")

    return {
        "status": "success",
        "request_id": request_id,
        "run_date": run_date,
        "local_path": str(local_path),
        "bucket_name": bucket_name,
        "s3_key": s3_key,
        "s3_uri": f"s3://{bucket_name}/{s3_key}",
    }


# =========================================================
# PIPELINE PRINCIPAL
# =========================================================
def load_single_flight_model_input_to_s3(
    request_id: str,
    run_date: str,
    bucket_name: Optional[str] = None,
) -> dict:
    """
    Charge le parquet transformé local correspondant à une requête user
    et l'envoie vers S3.
    """
    _ = load_local_transformed_parquet(request_id=request_id)

    result = upload_local_parquet_to_s3(
        request_id=request_id,
        run_date=run_date,
        bucket_name=bucket_name,
    )

    return result


if __name__ == "__main__":
    raise SystemExit(
        "load.py doit être appelé avec un request_id et un run_date depuis le pipeline applicatif."
    )