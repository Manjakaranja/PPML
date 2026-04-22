from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field
from typing import Optional
import logging
import os
import sys
import subprocess
import re
from pathlib import Path
from datetime import datetime

import pandas as pd
import mlflow
import mlflow.pyfunc
import mlflow.sklearn
from pandas.api.types import is_datetime64_any_dtype

from preprocessing.transformation import transform_single_flight_dataset
from preprocessing.load import load_single_flight_model_input_to_s3


# LOGGING

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# APP

app = FastAPI(
    title="FlyOnTime FastAPI",
    description="API for flight delay prediction",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# CONFIG

BASE_DIR = Path(__file__).resolve().parent

MLFLOW_TRACKING_URI = os.getenv(
    "MLFLOW_TRACKING_URI",
    "https://ppml2026-ppml-mlflow.hf.space"
)

CLASSIFIER_MODEL_URI = "models:/RandomForest_Classifier_registered@challenger"
REGRESSOR_MODEL_URI = "models:/RandomForest_Regressor_registered@challenger"

REGRESSOR_LOADER = "pyfunc"

TEST_DATA_PATH = BASE_DIR / "data" / "df_1_train_cleaned.parquet"
TEST_ROW_INDEX = 0

FLIGHT_LOOKUP_DIR = BASE_DIR / "flight_lookup"
GLOBAL_RUN_SINGLE_FLIGHT_PATH = FLIGHT_LOOKUP_DIR / "GlobalRunSingleFlight.py"

ENABLE_S3_UPLOAD = os.getenv("ENABLE_S3_UPLOAD", "0")


# AWS / HF SECRETS MAPPING

aws_key = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY")
aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_DEFAULT_REGION", "eu-north-1")

if aws_key:
    os.environ["AWS_ACCESS_KEY_ID"] = aws_key
if aws_secret:
    os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret
if aws_region:
    os.environ["AWS_DEFAULT_REGION"] = aws_region

logger.info("AWS_ACCESS_KEY_ID present: %s", bool(os.getenv("AWS_ACCESS_KEY_ID")))
logger.info("AWS_SECRET_ACCESS_KEY present: %s", bool(os.getenv("AWS_SECRET_ACCESS_KEY")))
logger.info("AWS_DEFAULT_REGION present: %s", bool(os.getenv("AWS_DEFAULT_REGION")))
logger.info("MLFLOW_TRACKING_URI present: %s", bool(os.getenv("MLFLOW_TRACKING_URI")))

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)


# GLOBAL OBJECTS

classifier_model = None
regressor_model = None
df_reference = None


# REQUEST / RESPONSE

class PredictionRequest(BaseModel):
    flight_number: str = Field(..., example="AF1234")
    date: str = Field(..., example="2026-04-20T14:30:00")
    departure_airport: str = Field(..., example="CDG - Paris Charles de Gaulle")
    arrival_airport: str = Field(..., example="CDG - Paris Charles de Gaulle")


class PredictionResponse(BaseModel):
    status: str
    flight_number: str
    date: str
    departure_airport: str
    arrival_airport: Optional[str] = None
    delay_probability: Optional[float] = None
    predicted_arrival_delay_minutes: Optional[float] = None
    is_delayed: Optional[bool] = None
    message: Optional[str] = None


# HELPERS

def normalize_departure_airport(user_value: str) -> str:
    if not user_value:
        return user_value

    value = user_value.strip()
    if " - " in value:
        return value.split(" - ")[0].strip()

    return value


def normalize_flight_number(value: str) -> str:
    if not value:
        return ""
    return str(value).replace(" ", "").upper().strip()


def extract_request_id_from_stdout(stdout: str) -> str:
    match = re.search(r"REQUEST_ID:\s*(\S+)", stdout)
    if not match:
        raise ValueError("Impossible de récupérer REQUEST_ID depuis le stdout du pipeline")
    return match.group(1).strip()


def load_reference_dataframe() -> pd.DataFrame:
    logger.info("Loading reference dataframe from: %s", TEST_DATA_PATH)

    if not TEST_DATA_PATH.exists():
        raise FileNotFoundError(f"Reference dataset not found at path: {TEST_DATA_PATH}")

    if TEST_DATA_PATH.suffix == ".parquet":
        df = pd.read_parquet(TEST_DATA_PATH)
    elif TEST_DATA_PATH.suffix == ".csv":
        df = pd.read_csv(TEST_DATA_PATH)
    else:
        raise ValueError("TEST_DATA_PATH must point to a .parquet or .csv file")

    if df.empty:
        raise ValueError("Reference dataframe is empty")

    logger.info("Reference dataframe loaded with shape: %s", df.shape)
    return df


def get_reference_row(df: pd.DataFrame, row_index: int) -> pd.DataFrame:
    if row_index < 0 or row_index >= len(df):
        raise IndexError(
            f"TEST_ROW_INDEX={row_index} is out of bounds for dataframe of length {len(df)}"
        )
    return df.iloc[[row_index]].copy()


def add_datetime_features_safe(df: pd.DataFrame, datetime_cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    bad_datetime_cols = []

    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

            if not is_datetime64_any_dtype(df[col]):
                bad_datetime_cols.append(col)

    usable_datetime_cols = [
        col for col in datetime_cols
        if col in df.columns and col not in bad_datetime_cols
    ]

    if "flight_date" in usable_datetime_cols:
        df["flight_month"] = df["flight_date"].dt.month
        df["flight_day"] = df["flight_date"].dt.day
        df["flight_dayofweek"] = df["flight_date"].dt.dayofweek

    if "scheduled_departure" in usable_datetime_cols:
        df["sched_dep_hour"] = df["scheduled_departure"].dt.hour
        df["sched_dep_minute"] = df["scheduled_departure"].dt.minute

    if "scheduled_arrival" in usable_datetime_cols:
        df["sched_arr_hour"] = df["scheduled_arrival"].dt.hour
        df["sched_arr_minute"] = df["scheduled_arrival"].dt.minute

    logger.info("Colonnes datetime problématiques droppées : %s", bad_datetime_cols)

    df = df.drop(columns=datetime_cols, errors="ignore")
    return df


def harmonize_columns_for_model_input(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    rename_map = {}

    if "scheduled_departure_dep" in df.columns and "scheduled_departure" not in df.columns:
        rename_map["scheduled_departure_dep"] = "scheduled_departure"

    if "scheduled_arrival_arr" in df.columns and "scheduled_arrival" not in df.columns:
        rename_map["scheduled_arrival_arr"] = "scheduled_arrival"

    if "movement_date_dep" in df.columns and "movement_date" not in df.columns:
        rename_map["movement_date_dep"] = "movement_date"

    if "status_dep" in df.columns and "status" not in df.columns:
        rename_map["status_dep"] = "status"

    if rename_map:
        logger.info("Rename columns applied: %s", rename_map)
        df = df.rename(columns=rename_map)

    return df


def select_single_flight_row(
    df: pd.DataFrame,
    flight_number: str,
    departure_airport: str
) -> pd.DataFrame:
    df = df.copy()

    if "flight_number" in df.columns:
        mask_flight = (
            df["flight_number"].astype(str).apply(normalize_flight_number)
            == normalize_flight_number(flight_number)
        )
        if mask_flight.any():
            df = df[mask_flight].copy()

    if "airport_origin" in df.columns:
        mask_airport = (
            df["airport_origin"].astype(str).str.upper().str.strip()
            == departure_airport.upper()
        )
        if mask_airport.any():
            df = df[mask_airport].copy()

    if df.empty:
        raise ValueError("Aucune ligne correspondant au vol demandé après ETL")

    logger.info("Rows remaining after ETL filtering: %s", len(df))
    return df.iloc[[0]].copy()


def prepare_features_from_real_single_row(
    df_real_single: pd.DataFrame,
    df_reference: pd.DataFrame,
    flight_number: str,
    date: str,
    departure_airport: str
) -> pd.DataFrame:
    X = get_reference_row(df_reference, TEST_ROW_INDEX)
    df_real_single = harmonize_columns_for_model_input(df_real_single)

    common_cols = [c for c in df_real_single.columns if c in X.columns]
    for col in common_cols:
        X[col] = df_real_single.iloc[0][col]

    if "flight_number" in X.columns:
        X["flight_number"] = flight_number

    if "airport_origin" in X.columns:
        X["airport_origin"] = departure_airport

    if "flight_date" in X.columns:
        X["flight_date"] = date

    X = X.drop(columns=[
        "retard arrivée",
        "arrival_delay_min",
        "status",
    ], errors="ignore")

    datetime_cols = [
        "flight_date",
        "movement_date",
        "scheduled_departure",
        "scheduled_arrival",
        "time_dep",
        "time_arr",
    ]

    X = add_datetime_features_safe(X, datetime_cols)

    logger.info("Prepared features shape: %s", X.shape)
    logger.info("Prepared features columns: %s", X.columns.tolist())
    return X


# MODELS

def load_models():
    global classifier_model, regressor_model, df_reference

    if df_reference is None:
        df_reference = load_reference_dataframe()

    if classifier_model is None:
        logger.info("Loading classifier model from MLflow: %s", CLASSIFIER_MODEL_URI)
        classifier_model = mlflow.sklearn.load_model(CLASSIFIER_MODEL_URI)
        logger.info("Classifier loaded successfully")

    if regressor_model is None:
        logger.info("Loading regressor model from MLflow: %s", REGRESSOR_MODEL_URI)

        if REGRESSOR_LOADER == "sklearn":
            regressor_model = mlflow.sklearn.load_model(REGRESSOR_MODEL_URI)
        else:
            regressor_model = mlflow.pyfunc.load_model(REGRESSOR_MODEL_URI)

        logger.info("Regressor loaded successfully")


# FLIGHT LOOKUP + ETL

def run_single_flight_lookup_pipeline(
    flight_number: str,
    date: str,
    departure_airport: str
) -> dict:
    if not GLOBAL_RUN_SINGLE_FLIGHT_PATH.exists():
        raise FileNotFoundError(
            f"GlobalRunSingleFlight.py introuvable à : {GLOBAL_RUN_SINGLE_FLIGHT_PATH}"
        )

    cmd = [
        sys.executable,
        str(GLOBAL_RUN_SINGLE_FLIGHT_PATH),
        flight_number,
        date,
        departure_airport,
    ]

    env = os.environ.copy()
    env["ENABLE_S3_UPLOAD"] = ENABLE_S3_UPLOAD

    logger.info("Running flight_lookup pipeline: %s", " ".join(cmd))

    completed = subprocess.run(
        cmd,
        cwd=str(FLIGHT_LOOKUP_DIR),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    logger.info("flight_lookup stdout:\n%s", completed.stdout)
    if completed.stderr:
        logger.warning("flight_lookup stderr:\n%s", completed.stderr)

    if completed.returncode != 0:
        raise RuntimeError(
            f"GlobalRunSingleFlight failed with code {completed.returncode}: "
            f"{completed.stderr or completed.stdout}"
        )

    request_id = extract_request_id_from_stdout(completed.stdout)
    run_date = datetime.now().strftime("%Y-%m-%d")

    logger.info("Recovered request_id=%s run_date=%s", request_id, run_date)

    return {
        "request_id": request_id,
        "run_date": run_date,
        "stdout": completed.stdout,
    }


def run_etl_pipeline(
    request_id: str,
    run_date: str
) -> tuple[pd.DataFrame, str]:
    logger.info(
        "Running ETL pipeline for request_id=%s run_date=%s",
        request_id,
        run_date,
    )

    df_single, _, output_path = transform_single_flight_dataset(
        request_id=request_id,
        run_date=run_date,
        encode_categories=False,
        save_output=True,
    )

    logger.info("Local transformed parquet path: %s", output_path)

    upload_result = load_single_flight_model_input_to_s3(
        request_id=request_id,
        run_date=run_date,
    )

    logger.info("Processed parquet uploaded to S3: %s", upload_result["s3_uri"])

    if df_single.empty:
        raise ValueError("Le parquet ETL final est vide")

    return df_single, upload_result["s3_uri"]


# PREDICTION

def run_prediction(
    flight_number: str,
    date: str,
    departure_airport: str
) -> dict:
    if classifier_model is None or regressor_model is None or df_reference is None:
        raise RuntimeError("Models or reference dataframe not loaded")

    departure_airport_clean = normalize_departure_airport(departure_airport)
    flight_number_clean = normalize_flight_number(flight_number)

    logger.info(
        "Running REAL prediction for flight=%s date=%s airport=%s",
        flight_number_clean,
        date,
        departure_airport_clean
    )

    lookup_result = run_single_flight_lookup_pipeline(
        flight_number=flight_number_clean,
        date=date,
        departure_airport=departure_airport_clean,
    )

    request_id = lookup_result["request_id"]
    run_date = lookup_result["run_date"]

    df_single_etl, processed_s3_uri = run_etl_pipeline(
        request_id=request_id,
        run_date=run_date,
    )

    df_real_single = select_single_flight_row(
        df=df_single_etl,
        flight_number=flight_number_clean,
        departure_airport=departure_airport_clean,
    )

    returned_arrival_airport = None

    if "airport_destination" in df_real_single.columns:
        returned_arrival_airport = str(df_real_single.iloc[0]["airport_destination"]).strip()
    elif "arrival_airport" in df_real_single.columns:
        returned_arrival_airport = str(df_real_single.iloc[0]["arrival_airport"]).strip()

    X_prepared = prepare_features_from_real_single_row(
        df_real_single=df_real_single,
        df_reference=df_reference,
        flight_number=flight_number_clean,
        date=date,
        departure_airport=departure_airport_clean,
    )

    clf_pred = classifier_model.predict(X_prepared)
    clf_pred_value = int(clf_pred[0])

    if hasattr(classifier_model, "predict_proba"):
        clf_proba = classifier_model.predict_proba(X_prepared)
        clf_proba_value = float(clf_proba[0][1])
    else:
        clf_proba_value = float(clf_pred_value)

    reg_pred = regressor_model.predict(X_prepared)
    reg_pred_value = float(reg_pred[0])
    reg_pred_value = max(0.0, reg_pred_value)

    return {
        "status": "success",
        "flight_number": flight_number_clean,
        "date": date,
        "departure_airport": departure_airport_clean,
        "arrival_airport": returned_arrival_airport,
        "delay_probability": clf_proba_value,
        "predicted_arrival_delay_minutes": reg_pred_value,
        "is_delayed": bool(clf_pred_value),
        "message": (
            f"Prediction generated successfully "
            f"(request_id={request_id}, processed={processed_s3_uri})"
        )
    }


# STARTUP

@app.on_event("startup")
def startup_event():
    try:
        load_models()
        logger.info("Startup completed successfully")
    except Exception:
        logger.exception("Startup failed")
        raise


# ENDPOINTS

@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "service": "flyontime-fastapi"
    }


@app.get("/")
def root():
    return RedirectResponse(url="/docs")


@app.get("/debug-models")
def debug_models():
    return {
        "tracking_uri": MLFLOW_TRACKING_URI,
        "classifier_model_uri": CLASSIFIER_MODEL_URI,
        "regressor_model_uri": REGRESSOR_MODEL_URI,
        "test_data_path": str(TEST_DATA_PATH),
        "test_row_index": TEST_ROW_INDEX,
        "reference_loaded": df_reference is not None,
        "n_rows": 0 if df_reference is None else int(len(df_reference)),
        "n_cols": 0 if df_reference is None else int(df_reference.shape[1]),
        "regressor_loader": REGRESSOR_LOADER,
        "flight_lookup_dir": str(FLIGHT_LOOKUP_DIR),
        "global_run_single_flight_exists": GLOBAL_RUN_SINGLE_FLIGHT_PATH.exists(),
    }


@app.post("/predict", response_model=PredictionResponse)
def predict(payload: PredictionRequest):
    try:
        logger.info("Received prediction request: %s", payload.model_dump())

        result = run_prediction(
            flight_number=payload.flight_number,
            date=payload.date,
            departure_airport=payload.departure_airport
        )

        return PredictionResponse(**result)

    except ValueError as e:
        logger.warning("Validation/business error: %s", str(e))
        raise HTTPException(status_code=400, detail=str(e))

    except FileNotFoundError as e:
        logger.error("Model or file not found: %s", str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Model or resource not found: {str(e)}"
        )

    except Exception as e:
        logger.exception("Unexpected error during prediction")
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected server error: {str(e)}"
        )