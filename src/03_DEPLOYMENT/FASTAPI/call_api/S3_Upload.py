#!pip install boto3 python-dotenv pandas
import boto3
import os
import pandas as pd
import shutil
import glob

from pathlib import Path
from dotenv import load_dotenv

load_dotenv()  
   
session = boto3.Session(
    aws_access_key_id=          os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=      os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name="eu-north-1"
)

s3 = session.client("s3")

# Paramètres S3
bucket_name = 'ppml2026'  
s3_folder = 'Datasets/'           


# Recherche des fichiers à uploader (Large et future)
signoff_files = glob.glob('SignoffFlightsDataset_Large_*.csv') + glob.glob('SignoffFlightsDataset_future_*.csv')

for file_path in signoff_files:
    file_name = os.path.basename(file_path)
    s3_key = os.path.join(s3_folder, file_name)
    print(f"Upload de {file_path} vers s3://{bucket_name}/{s3_key}")
    s3.upload_file(file_path, bucket_name, s3_key)
    print(f"Fichier {file_name} uploadé avec succès.")