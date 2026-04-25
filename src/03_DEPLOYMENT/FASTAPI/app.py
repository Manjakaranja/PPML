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
import json
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
import mlflow
import mlflow.xgboost
from pandas.api.types import is_datetime64_any_dtype
from sklearn.preprocessing import OrdinalEncoder

from preprocessing.transformation import transform_single_flight_dataset
from preprocessing.load import load_single_flight_model_input_to_s3


# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =========================================================
# APP
# =========================================================
app = FastAPI(
    title="FlyOnTime FastAPI",
    description="API for flight delay prediction",
    version="3.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================================================
# CONFIG
# =========================================================
BASE_DIR = Path(__file__).resolve().parent

MLFLOW_TRACKING_URI = os.getenv(
    "MLFLOW_TRACKING_URI",
    "https://ppml2026-ppml-mlflow.hf.space"
)

CLASSIFIER_MODEL_URI = "models:/XGBoost_Classifier_registered@challenger"
REGRESSOR_MODEL_URI = "models:/XGBoost_Regressor_registered@challenger"

TEST_DATA_PATH = BASE_DIR / "data" / "df_train_final.parquet"
TEST_ROW_INDEX = 0

FLIGHT_LOOKUP_DIR = BASE_DIR / "flight_lookup"
GLOBAL_RUN_SINGLE_FLIGHT_PATH = FLIGHT_LOOKUP_DIR / "GlobalRunSingleFlight.py"
OUTPUT_ROOT = FLIGHT_LOOKUP_DIR / "output"

ENABLE_S3_UPLOAD = os.getenv("ENABLE_S3_UPLOAD", "0")


# =========================================================
# AWS / HF SECRETS MAPPING
# =========================================================
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


# =========================================================
# CONSTANTES PREPROCESSING XGBOOST
# =========================================================
COLS_A_VIRER_CLASSIFIER = [
    "departure_delay_min",
    "arrival_delay_min",
    "time_dep",
    "time_arr",
]

COLS_A_VIRER_REGRESSOR = [
    "departure_delay_min",
    "arrival_delay_min",
    "time_dep",
    "time_arr",
]

DATETIME_COLS_NOTEBOOK = [
    "flight_date",
    "scheduled_departure_dep",
    "scheduled_arrival_arr",
]


# =========================================================
# GLOBAL OBJECTS
# =========================================================
classifier_model = None
regressor_model = None
df_reference = None

clf_preprocessor = None
reg_preprocessor = None


# =========================================================
# REQUEST / RESPONSE
# =========================================================
class PredictionRequest(BaseModel):
    flight_number: str = Field(..., example="AF1234")
    date: str = Field(..., example="2026-04-20T14:30:00")
    departure_airport: str = Field(..., example="CDG - Paris Charles de Gaulle")
    arrival_airport: Optional[str] = Field(None, example="NCE - Nice Côte d'Azur")


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
    warning_message: Optional[str] = None


# =========================================================
# HELPERS GÉNÉRAUX
# =========================================================
def normalize_departure_airport(user_value: Optional[str]) -> Optional[str]:
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


def get_request_dir(request_id: str) -> Path:
    return OUTPUT_ROOT / request_id


def get_request_status_path(request_id: str) -> Path:
    return get_request_dir(request_id) / "flight_request_status.json"


def get_request_error_log_path(request_id: str) -> Path:
    return get_request_dir(request_id) / "API_Single_ERR.log"


def read_request_status(request_id: str) -> dict:
    status_path = get_request_status_path(request_id)
    if not status_path.exists():
        return {}

    with open(status_path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_user_friendly_pipeline_error(request_id: str, fallback_message: str) -> str:
    status_payload = read_request_status(request_id)
    if status_payload and status_payload.get("user_message"):
        return status_payload["user_message"]

    log_path = get_request_error_log_path(request_id)
    if log_path.exists():
        return fallback_message

    return fallback_message


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
        raise IndexError(f"TEST_ROW_INDEX={row_index} is out of bounds for dataframe of length {len(df)}")
    return df.iloc[[row_index]].copy()


def align_single_row_columns(df_single: pd.DataFrame) -> pd.DataFrame:
    """
    Sécurise les noms pour rester cohérent avec le training notebook.
    On garde scheduled_departure_dep / scheduled_arrival_arr.
    """
    df_single = df_single.copy()

    if "scheduled_departure_dep" not in df_single.columns and "scheduled_departure" in df_single.columns:
        df_single["scheduled_departure_dep"] = df_single["scheduled_departure"]

    if "scheduled_arrival_arr" not in df_single.columns and "scheduled_arrival" in df_single.columns:
        df_single["scheduled_arrival_arr"] = df_single["scheduled_arrival"]

    if "movement_date" not in df_single.columns and "movement_date_dep" in df_single.columns:
        df_single["movement_date"] = df_single["movement_date_dep"]

    if "status" not in df_single.columns and "status_dep" in df_single.columns:
        df_single["status"] = df_single["status_dep"]

    return df_single


def datetime_clean_like_notebook(df: pd.DataFrame, datetime_cols: list[str]) -> pd.DataFrame:
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

    if "scheduled_departure_dep" in usable_datetime_cols:
        df["sched_dep_hour"] = df["scheduled_departure_dep"].dt.hour
        df["sched_dep_minute"] = df["scheduled_departure_dep"].dt.minute

    if "scheduled_arrival_arr" in usable_datetime_cols:
        df["sched_arr_hour"] = df["scheduled_arrival_arr"].dt.hour
        df["sched_arr_minute"] = df["scheduled_arrival_arr"].dt.minute

    logger.info("Colonnes datetime problématiques droppées : %s", bad_datetime_cols)

    df = df.drop(columns=datetime_cols, errors="ignore")
    return df


def build_training_frame_for_classifier(df_ref: pd.DataFrame) -> pd.DataFrame:
    X = df_ref.drop(
        columns=COLS_A_VIRER_CLASSIFIER + ["retard_arrivee"],
        errors="ignore",
    ).copy()
    X = datetime_clean_like_notebook(X, DATETIME_COLS_NOTEBOOK)
    return X


def build_training_frame_for_regressor(df_ref: pd.DataFrame) -> pd.DataFrame:
    X = df_ref.drop(
        columns=COLS_A_VIRER_REGRESSOR + ["arrival_delay_min", "retard_arrivee"],
        errors="ignore",
    ).copy()
    X = datetime_clean_like_notebook(X, DATETIME_COLS_NOTEBOOK)
    return X


def fit_preprocessor_from_training(
    X_train_ref: pd.DataFrame,
    model,
    task_name: str,
) -> dict:
    X_train_ref = X_train_ref.copy()

    cat_cols = X_train_ref.select_dtypes(include=["object", "category"]).columns.tolist()

    encoder = None
    if cat_cols:
        encoder = OrdinalEncoder(
            handle_unknown="use_encoded_value",
            unknown_value=-1,
        )
        X_train_ref[cat_cols] = encoder.fit_transform(X_train_ref[cat_cols].astype(str))

    num_cols = X_train_ref.select_dtypes(include=[np.number]).columns.tolist()
    medians = X_train_ref[num_cols].median() if num_cols else pd.Series(dtype=float)

    if num_cols:
        X_train_ref[num_cols] = X_train_ref[num_cols].fillna(medians)

    expected_cols = None
    if hasattr(model, "feature_names_in_"):
        expected_cols = list(model.feature_names_in_)
    else:
        expected_cols = list(X_train_ref.columns)

    logger.info("[%s] cat_cols: %s", task_name, cat_cols)
    logger.info("[%s] num_cols count: %s", task_name, len(num_cols))
    logger.info("[%s] expected_cols count: %s", task_name, len(expected_cols))

    return {
        "cat_cols": cat_cols,
        "num_cols": num_cols,
        "encoder": encoder,
        "medians": medians,
        "expected_cols": expected_cols,
    }


def prepare_single_row_base(
    df_real_single: pd.DataFrame,
    df_reference: pd.DataFrame,
    flight_number: str,
    date: str,
    departure_airport: str,
) -> pd.DataFrame:
    X = get_reference_row(df_reference, TEST_ROW_INDEX)
    df_real_single = align_single_row_columns(df_real_single)

    common_cols = [c for c in df_real_single.columns if c in X.columns]
    for col in common_cols:
        X[col] = df_real_single.iloc[0][col]

    if "flight_number" in X.columns:
        X["flight_number"] = flight_number

    if "airport_origin" in X.columns:
        X["airport_origin"] = departure_airport

    if "flight_date" in X.columns:
        X["flight_date"] = date

    return X


def apply_preprocessor_to_single_row(
    X_single: pd.DataFrame,
    task: str,
    preprocessor: dict,
) -> pd.DataFrame:
    X_single = X_single.copy()

    if task == "classifier":
        X_single = X_single.drop(
            columns=COLS_A_VIRER_CLASSIFIER + ["retard_arrivee"],
            errors="ignore",
        )
    elif task == "regressor":
        X_single = X_single.drop(
            columns=COLS_A_VIRER_REGRESSOR + ["arrival_delay_min", "retard_arrivee"],
            errors="ignore",
        )
    else:
        raise ValueError(f"Task inconnue: {task}")

    X_single = datetime_clean_like_notebook(X_single, DATETIME_COLS_NOTEBOOK)

    cat_cols = preprocessor["cat_cols"]
    encoder = preprocessor["encoder"]
    num_cols = preprocessor["num_cols"]
    medians = preprocessor["medians"]
    expected_cols = preprocessor["expected_cols"]

    for col in cat_cols:
        if col not in X_single.columns:
            X_single[col] = ""

    if cat_cols and encoder is not None:
        X_single[cat_cols] = encoder.transform(X_single[cat_cols].astype(str))

    for col in expected_cols:
        if col not in X_single.columns:
            X_single[col] = np.nan

    if num_cols:
        missing_num_cols = [c for c in num_cols if c not in X_single.columns]
        for col in missing_num_cols:
            X_single[col] = np.nan

        fill_cols = [c for c in num_cols if c in X_single.columns and c in medians.index]
        if fill_cols:
            X_single[fill_cols] = X_single[fill_cols].fillna(medians[fill_cols])

    X_single = X_single[expected_cols].copy()

    logger.info("[%s] Prepared shape: %s", task, X_single.shape)
    logger.info("[%s] Prepared columns: %s", task, X_single.columns.tolist())

    return X_single


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


# =========================================================
# MODELS + PREPROCESSORS
# =========================================================
def load_models():
    global classifier_model, regressor_model, df_reference, clf_preprocessor, reg_preprocessor

    if df_reference is None:
        df_reference = load_reference_dataframe()

    if classifier_model is None:
        logger.info("Loading classifier model from MLflow: %s", CLASSIFIER_MODEL_URI)
        classifier_model = mlflow.xgboost.load_model(CLASSIFIER_MODEL_URI)
        logger.info("Classifier loaded successfully")

    if regressor_model is None:
        logger.info("Loading regressor model from MLflow: %s", REGRESSOR_MODEL_URI)
        regressor_model = mlflow.xgboost.load_model(REGRESSOR_MODEL_URI)
        logger.info("Regressor loaded successfully")

    if clf_preprocessor is None:
        X_clf_train_ref = build_training_frame_for_classifier(df_reference)
        clf_preprocessor = fit_preprocessor_from_training(
            X_train_ref=X_clf_train_ref,
            model=classifier_model,
            task_name="classifier",
        )

    if reg_preprocessor is None:
        X_reg_train_ref = build_training_frame_for_regressor(df_reference)
        reg_preprocessor = fit_preprocessor_from_training(
            X_train_ref=X_reg_train_ref,
            model=regressor_model,
            task_name="regressor",
        )


# =========================================================
# FLIGHT LOOKUP + ETL
# =========================================================
def run_single_flight_lookup_pipeline(
    flight_number: str,
    date: str,
    departure_airport: str,
    arrival_airport: Optional[str] = None,
) -> dict:
    if not GLOBAL_RUN_SINGLE_FLIGHT_PATH.exists():
        raise FileNotFoundError(f"GlobalRunSingleFlight.py introuvable à : {GLOBAL_RUN_SINGLE_FLIGHT_PATH}")

    cmd = [
        sys.executable,
        str(GLOBAL_RUN_SINGLE_FLIGHT_PATH),
        flight_number,
        date,
        departure_airport,
    ]

    if arrival_airport:
        cmd.append(arrival_airport)

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

    request_id = extract_request_id_from_stdout(completed.stdout)

    if completed.returncode != 0:
        friendly_message = build_user_friendly_pipeline_error(
            request_id=request_id,
            fallback_message="Vol introuvable. Veuillez vérifier le numéro de vol, la date, l’horaire et l’aéroport de départ."
        )
        raise ValueError(friendly_message)

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
    logger.info("Running ETL pipeline for request_id=%s run_date=%s", request_id, run_date)

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


# =========================================================
# PREDICTION
# =========================================================
def run_prediction(
    flight_number: str,
    date: str,
    departure_airport: str,
    arrival_airport: Optional[str] = None,
) -> dict:
    if (
        classifier_model is None
        or regressor_model is None
        or df_reference is None
        or clf_preprocessor is None
        or reg_preprocessor is None
    ):
        raise RuntimeError("Models, preprocessors or reference dataframe not loaded")

    departure_airport_clean = normalize_departure_airport(departure_airport)
    arrival_airport_clean = normalize_departure_airport(arrival_airport) if arrival_airport else None
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
        arrival_airport=arrival_airport_clean,
    )

    request_id = lookup_result["request_id"]
    run_date = lookup_result["run_date"]

    status_payload = read_request_status(request_id)

    df_single_etl, processed_s3_uri = run_etl_pipeline(
        request_id=request_id,
        run_date=run_date,
    )

    matched_flight_number = status_payload.get("matched_flight_number") or flight_number_clean

    df_real_single = select_single_flight_row(
        df=df_single_etl,
        flight_number=matched_flight_number,
        departure_airport=departure_airport_clean,
    )

    returned_arrival_airport = None
    if "airport_destination" in df_real_single.columns:
        returned_arrival_airport = str(df_real_single.iloc[0]["airport_destination"]).strip()
    elif "arrival_airport" in df_real_single.columns:
        returned_arrival_airport = str(df_real_single.iloc[0]["arrival_airport"]).strip()

    X_base = prepare_single_row_base(
        df_real_single=df_real_single,
        df_reference=df_reference,
        flight_number=matched_flight_number,
        date=date,
        departure_airport=departure_airport_clean,
    )

    X_clf = apply_preprocessor_to_single_row(
        X_single=X_base,
        task="classifier",
        preprocessor=clf_preprocessor,
    )

    X_reg = apply_preprocessor_to_single_row(
        X_single=X_base,
        task="regressor",
        preprocessor=reg_preprocessor,
    )

    clf_pred = classifier_model.predict(X_clf)
    clf_pred_value = int(clf_pred[0])

    if hasattr(classifier_model, "predict_proba"):
        clf_proba = classifier_model.predict_proba(X_clf)
        clf_proba_value = float(clf_proba[0][1])
    else:
        clf_proba_value = float(clf_pred_value)

    reg_pred = regressor_model.predict(X_reg)
    reg_pred_value = float(reg_pred[0])
    reg_pred_value = max(0.0, reg_pred_value)

    final_message = "Prédiction générée avec succès."
    if status_payload.get("warning_message"):
        final_message = status_payload.get("user_message") or final_message

    return {
        "status": "success",
        "flight_number": matched_flight_number,
        "date": date,
        "departure_airport": departure_airport_clean,
        "arrival_airport": returned_arrival_airport,
        "delay_probability": clf_proba_value,
        "predicted_arrival_delay_minutes": reg_pred_value,
        "is_delayed": bool(clf_pred_value),
        "message": f"{final_message} (request_id={request_id}, processed={processed_s3_uri})",
        "warning_message": status_payload.get("warning_message"),
    }


# =========================================================
# STARTUP
# =========================================================
@app.on_event("startup")
def startup_event():
    try:
        load_models()
        logger.info("Startup completed successfully")
    except Exception:
        logger.exception("Startup failed")
        raise


# =========================================================
# ENDPOINTS
# =========================================================
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
        "flight_lookup_dir": str(FLIGHT_LOOKUP_DIR),
        "global_run_single_flight_exists": GLOBAL_RUN_SINGLE_FLIGHT_PATH.exists(),
        "clf_preprocessor_ready": clf_preprocessor is not None,
        "reg_preprocessor_ready": reg_preprocessor is not None,
    }


@app.post("/predict", response_model=PredictionResponse)
def predict(payload: PredictionRequest):
    try:
        logger.info("Received prediction request: %s", payload.model_dump())

        result = run_prediction(
            flight_number=payload.flight_number,
            date=payload.date,
            departure_airport=payload.departure_airport,
            arrival_airport=payload.arrival_airport,
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

    except Exception:
        logger.exception("Unexpected error during prediction")
        raise HTTPException(
            status_code=500,
            detail="Une erreur technique est survenue pendant la prédiction. Merci de réessayer."
        )