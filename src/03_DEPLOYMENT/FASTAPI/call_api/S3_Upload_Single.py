import os
from pathlib import Path

import boto3
from dotenv import load_dotenv

load_dotenv()

REQUEST_DIR = Path(os.environ["REQUEST_DIR"])
REQUEST_ID = os.environ["REQUEST_ID"]
RUN_DATE = os.environ["RUN_DATE"]  # YYYY-MM-DD
UPLOAD_MODE = os.getenv("UPLOAD_MODE", "all")  # all | logs_only

BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "ppml2026")
RAW_PREFIX = os.getenv("S3_PREFIX", "raw")

PARQUET_PATH = REQUEST_DIR / f"SignoffFlightsDataset_Single_{REQUEST_ID}.parquet"
STATUS_PATH = REQUEST_DIR / "flight_request_status.json"
ERROR_LOG_PATH = REQUEST_DIR / "API_Single_ERR.log"

session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_DEFAULT_REGION", "eu-north-1"),
)

s3 = session.client("s3")


def upload_if_exists(local_path: Path, s3_key: str):
    if not local_path.exists():
        print(f"[INFO] Fichier absent, upload ignoré : {local_path}")
        return False

    print(f"Upload de {local_path} vers s3://{BUCKET_NAME}/{s3_key}")
    s3.upload_file(str(local_path), BUCKET_NAME, s3_key)
    print("Upload S3 terminé avec succès.")
    return True


def main():
    uploaded_any = False

    if UPLOAD_MODE != "logs_only":
        parquet_key = f"{RAW_PREFIX}/{RUN_DATE}/{REQUEST_ID}/{PARQUET_PATH.name}"
        uploaded_any = upload_if_exists(PARQUET_PATH, parquet_key) or uploaded_any

    status_key = f"{RAW_PREFIX}/{RUN_DATE}/{REQUEST_ID}/{STATUS_PATH.name}"
    log_key = f"{RAW_PREFIX}/{RUN_DATE}/{REQUEST_ID}/{ERROR_LOG_PATH.name}"

    uploaded_any = upload_if_exists(STATUS_PATH, status_key) or uploaded_any
    uploaded_any = upload_if_exists(ERROR_LOG_PATH, log_key) or uploaded_any

    if not uploaded_any:
        print("[WARN] Aucun fichier uploadé vers S3.")
    else:
        print("[INFO] Upload S3 single terminé.")


if __name__ == "__main__":
    main()