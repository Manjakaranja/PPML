import glob

! pip install boto3 python-dotenv pandas
import boto3
import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()  
   
session = boto3.Session(
    aws_access_key_id=          os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=      os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name="eu-north-1"
)

s3 = session.client("s3")

import shutil


def upload_latest_clean_files():
    # Cherche les deux derniers fichiers *CLEAN.csv dans le dossier courant
    clean_files = sorted(glob.glob("*CLEAN.csv"), key=os.path.getmtime, reverse=True)[:2]
    if not clean_files:
        print("Aucun fichier *CLEAN.csv trouvé pour upload.")
        return
    for file in clean_files:
        s3_key = os.path.basename(file)
        print(f"Uploading {file} to S3 bucket {bucket} as {s3_key} ...")
        s3.upload_file(file, bucket, s3_key)
    print(f"Upload terminé pour : {clean_files}")


# Upload des deux derniers fichiers *CLEAN.csv
upload_latest_clean_files()