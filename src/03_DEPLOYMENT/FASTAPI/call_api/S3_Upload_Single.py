import os
from pathlib import Path

import boto3
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
REQUEST_DIR = Path(os.environ["REQUEST_DIR"])
REQUEST_ID = os.environ["REQUEST_ID"]
RUN_DATE = os.environ["RUN_DATE"]  # format YYYY-MM-DD

BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "ppml2026")
S3_PREFIX = os.getenv("S3_PREFIX", "raw")

PARQUET_PATH = REQUEST_DIR / f"SignoffFlightsDataset_Single_{REQUEST_ID}.parquet"

if not PARQUET_PATH.exists():
    raise FileNotFoundError(f"Fichier parquet introuvable : {PARQUET_PATH}")

session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_DEFAULT_REGION", "eu-north-1"),
)

s3 = session.client("s3")

s3_key = f"{S3_PREFIX}/{RUN_DATE}/{REQUEST_ID}/{PARQUET_PATH.name}"

print(f"Upload de {PARQUET_PATH} vers s3://{BUCKET_NAME}/{s3_key}")
s3.upload_file(str(PARQUET_PATH), BUCKET_NAME, s3_key)
print("Upload S3 terminé avec succès.")