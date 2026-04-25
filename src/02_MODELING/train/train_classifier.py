#!/usr/bin/env python3
"""
Train the FlyOnTime XGBoost classifier.

This script is the clean Python version of the XGBoost classifier notebook.
It keeps the notebook modeling logic, but replaces notebook-style loading with
an explicit S3 loading step.

Workflow:
1. Load the historical CLEAN training CSV from S3:
   s3://ppml2026/datasets/SignofFlightsDataset_20260416_233018_CLEAN.csv
2. Apply the same cleaning / feature selection logic used in the notebook.
3. Create the binary target `retard_arrivee` from `arrival_delay_min`.
4. Save the cleaned training dataframe locally as `data/df_train_final.parquet`.
   This parquet is then reused by the XGBoost regressor training script.
5. Train an XGBoost classifier with RandomizedSearchCV.
6. Log metrics, artifacts and registered model to MLflow.
7. Set the registered model alias to `challenger`.

Example:
    python train_xgboost_classifier.py \
        --tracking-uri https://ppml2026-ppml-mlflow.hf.space

Required environment variables for S3:
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_DEFAULT_REGION  # optional, defaults to eu-north-1
"""

from __future__ import annotations

import argparse
import json
import os
import warnings
from io import BytesIO
from pathlib import Path
from typing import Any

import boto3
import joblib
import mlflow
import mlflow.xgboost
import numpy as np
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import RandomizedSearchCV, StratifiedKFold, cross_validate, train_test_split
from sklearn.preprocessing import OrdinalEncoder
from xgboost import XGBClassifier

warnings.filterwarnings("ignore")

# Same notebook conventions
DEFAULT_S3_BUCKET = "ppml2026"
DEFAULT_S3_KEY = "datasets/SignofFlightsDataset_20260416_233018_CLEAN.csv"
DEFAULT_OUTPUT_PARQUET = "data/df_train_final.parquet"

DEFAULT_DATETIME_COLS = ["flight_date", "scheduled_departure_dep", "scheduled_arrival_arr"]
COLS_A_VIRER = ["departure_delay_min", "arrival_delay_min", "time_dep", "time_arr"]
DEDUP_KEYS = ["flight_number", "flight_date", "airport_origin", "airport_destination"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train FlyOnTime XGBoost classifier from the historical S3 CSV")

    parser.add_argument("--s3-bucket", default=os.getenv("S3_BUCKET_NAME", DEFAULT_S3_BUCKET))
    parser.add_argument("--s3-key", default=os.getenv("TRAIN_CLASSIFIER_S3_KEY", DEFAULT_S3_KEY))
    parser.add_argument("--s3-region", default=os.getenv("AWS_DEFAULT_REGION", "eu-north-1"))
    parser.add_argument("--output-parquet", default=os.getenv("DF_TRAIN_FINAL_PATH", DEFAULT_OUTPUT_PARQUET))

    parser.add_argument("--tracking-uri", default=os.getenv("MLFLOW_TRACKING_URI", "https://ppml2026-ppml-mlflow.hf.space"))
    parser.add_argument("--experiment-name", default=os.getenv("MLFLOW_EXPERIMENT_NAME", "FlyOnTime_Classifier"))
    parser.add_argument("--run-name", default=os.getenv("MLFLOW_RUN_NAME", "XGBoost_Classifier"))
    parser.add_argument("--registered-name", default=os.getenv("MLFLOW_REGISTERED_NAME", "XGBoost_Classifier_registered"))
    parser.add_argument("--alias", default=os.getenv("MLFLOW_MODEL_ALIAS", "challenger"))
    parser.add_argument("--developer", default=os.getenv("MODEL_DEVELOPER", "Ludo"))

    parser.add_argument("--test-size", type=float, default=0.2)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--n-iter", type=int, default=20)
    parser.add_argument("--cv-splits", type=int, default=5)
    parser.add_argument("--delay-threshold-min", type=int, default=15)
    parser.add_argument("--artifacts-dir", default="artifacts_classifier")
    return parser.parse_args()


def get_s3_client(region_name: str):
    aws_key = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")

    return boto3.client(
        "s3",
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        region_name=region_name,
    )


def load_training_csv_from_s3(bucket_name: str, s3_key: str, region_name: str) -> pd.DataFrame:
    """Load the historical clean CSV from S3, following the boto3 approach used in extraction.py."""
    s3 = get_s3_client(region_name)
    print(f"Loading classifier training dataset from s3://{bucket_name}/{s3_key}")

    obj = s3.get_object(Bucket=bucket_name, Key=s3_key)
    df = pd.read_csv(BytesIO(obj["Body"].read()))

    if df.empty:
        raise ValueError(f"Training dataset is empty: s3://{bucket_name}/{s3_key}")

    print(f"Dataset loaded from S3 with shape: {df.shape}")
    return df


def purge_et_voir(df: pd.DataFrame, nom_dataset: str, seuil: float = 0.6) -> pd.DataFrame:
    """
    Same cleaning logic as the classifier notebook:
    - keep key columns;
    - keep columns ending with _dep or _arr;
    - keep delay / retard / target columns;
    - remove columns with too many missing values, except protected target-like columns;
    - forward/backward fill remaining missing values.
    """
    print(f"\n--- NETTOYAGE : {nom_dataset} ---")
    colonnes_avant = set(df.columns)
    cles = ["flight_number", "flight_date", "airport_origin", "airport_destination"]
    mots_cles_proteges = ["delay", "retard", "target"]

    cols_a_garder = [
        c for c in df.columns
        if c.endswith(("_dep", "_arr"))
        or c in cles
        or any(mot in c.lower() for mot in mots_cles_proteges)
    ]

    df_filtre = df[cols_a_garder].copy()

    taux_nan = df_filtre.isnull().mean()
    cols_trop_vides = [
        c for c in taux_nan[taux_nan > seuil].index
        if not any(mot in c.lower() for mot in mots_cles_proteges)
    ]

    df_final = df_filtre.drop(columns=cols_trop_vides)
    fantomes = colonnes_avant - set(cols_a_garder)

    print(f"{len(fantomes)} colonnes sans suffixe supprimées.")
    print(f"{len(cols_trop_vides)} colonnes > {seuil * 100:.0f}% vides supprimées.")
    print(
        "Colonnes de retard préservées :",
        [c for c in df_final.columns if any(mot in c.lower() for mot in mots_cles_proteges)],
    )

    return df_final.ffill().bfill()


def build_df_train_final(df_train: pd.DataFrame, delay_threshold_min: int) -> pd.DataFrame:
    """Rebuild `df_train_final` exactly as the modeling notebooks expect it."""
    before = len(df_train)
    df_train_final = df_train.drop_duplicates(subset=DEDUP_KEYS, keep=False).copy()
    print(f"Duplicates removed: {before - len(df_train_final)}")
    print(f"TRAIN: {len(df_train_final)} flights ready before column purge.")

    df_train_final = purge_et_voir(df_train_final, "TRAIN")

    if "arrival_delay_min" not in df_train_final.columns:
        raise KeyError("Column `arrival_delay_min` is required to create `retard_arrivee`.")

    df_train_final["retard_arrivee"] = (df_train_final["arrival_delay_min"] > delay_threshold_min).astype(int)

    # Old accented target from previous experiments is not used anymore.
    df_train_final = df_train_final.drop(columns=["retard arrivée"], errors="ignore")

    print("Target `retard_arrivee` created successfully.")
    print(df_train_final["retard_arrivee"].value_counts(dropna=False))
    print(f"df_train_final shape: {df_train_final.shape}")
    return df_train_final


def save_df_train_final(df_train_final: pd.DataFrame, output_parquet: str) -> Path:
    output_path = Path(output_parquet)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_train_final.to_parquet(output_path, index=False)
    print(f"df_train_final saved locally: {output_path}")
    return output_path


def datetime_clean(df: pd.DataFrame, datetime_cols: list[str] | None = None) -> pd.DataFrame:
    datetime_cols = datetime_cols or DEFAULT_DATETIME_COLS
    df = df.copy()
    bad_datetime_cols: list[str] = []

    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            if not is_datetime64_any_dtype(df[col]):
                bad_datetime_cols.append(col)

    usable_datetime_cols = [col for col in datetime_cols if col in df.columns and col not in bad_datetime_cols]

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

    print("Colonnes datetime problématiques droppées :", bad_datetime_cols)
    return df.drop(columns=datetime_cols, errors="ignore")


def prepare_xy(df_train_final: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    df_train_final = df_train_final[df_train_final["retard_arrivee"].notnull()].copy()

    y = df_train_final["retard_arrivee"].astype(int)
    X = df_train_final.drop(columns=COLS_A_VIRER + ["retard_arrivee"], errors="ignore").copy()
    X = datetime_clean(X, DEFAULT_DATETIME_COLS)
    return X, y


def fit_preprocess_train_test(X_train: pd.DataFrame, X_test: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    X_train = X_train.copy()
    X_test = X_test.copy()

    cat_cols = X_train.select_dtypes(include=["object", "category"]).columns.tolist()
    encoder = OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)

    if cat_cols:
        X_train[cat_cols] = encoder.fit_transform(X_train[cat_cols].astype(str))
        X_test[cat_cols] = encoder.transform(X_test[cat_cols].astype(str))

    num_cols = X_train.select_dtypes(include=np.number).columns.tolist()
    medians = X_train[num_cols].median()
    X_train[num_cols] = X_train[num_cols].fillna(medians)
    X_test[num_cols] = X_test[num_cols].fillna(medians)

    X_test = X_test.reindex(columns=X_train.columns, fill_value=0)

    preprocessing = {
        "cat_cols": cat_cols,
        "num_cols": num_cols,
        "feature_columns": X_train.columns.tolist(),
        "encoder": encoder,
        "medians": medians,
    }
    return X_train, X_test, preprocessing


def save_preprocessing_artifacts(preprocessing: dict[str, Any], artifacts_dir: str, df_train_final_path: Path) -> Path:
    path = Path(artifacts_dir)
    path.mkdir(parents=True, exist_ok=True)

    joblib.dump(preprocessing["encoder"], path / "ordinal_encoder.joblib")
    preprocessing["medians"].to_json(path / "train_medians.json", indent=2)

    metadata = {
        "cat_cols": preprocessing["cat_cols"],
        "num_cols": preprocessing["num_cols"],
        "feature_columns": preprocessing["feature_columns"],
        "datetime_cols": DEFAULT_DATETIME_COLS,
        "drop_cols": COLS_A_VIRER,
        "target": "retard_arrivee",
        "df_train_final_local_path": str(df_train_final_path),
    }
    with open(path / "preprocessing_metadata.json", "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

    return path


def log_xgboost_model(best_model: XGBClassifier, model_name: str, registered_name: str, input_example: pd.DataFrame):
    try:
        return mlflow.xgboost.log_model(
            xgb_model=best_model,
            name=model_name,
            registered_model_name=registered_name,
            input_example=input_example,
            model_format="json",
        )
    except TypeError:
        return mlflow.xgboost.log_model(
            xgb_model=best_model,
            artifact_path=model_name,
            registered_model_name=registered_name,
            input_example=input_example,
            model_format="json",
        )


def set_alias_if_possible(registered_name: str, alias: str, model_info: Any) -> None:
    version = getattr(model_info, "registered_model_version", None)
    if version is None:
        print("Model alias not set: registered model version unavailable from MLflow response.")
        return
    client = mlflow.tracking.MlflowClient()
    client.set_registered_model_alias(registered_name, alias, str(version))


def main() -> None:
    args = parse_args()

    mlflow.set_tracking_uri(args.tracking_uri)
    mlflow.set_experiment(args.experiment_name)
    while mlflow.active_run():
        mlflow.end_run()

    source_uri = f"s3://{args.s3_bucket}/{args.s3_key}"
    df_train_raw = load_training_csv_from_s3(args.s3_bucket, args.s3_key, args.s3_region)
    df_train_final = build_df_train_final(df_train_raw, delay_threshold_min=args.delay_threshold_min)
    df_train_final_path = save_df_train_final(df_train_final, args.output_parquet)

    X, y = prepare_xy(df_train_final)

    X_train_raw, X_test_raw, y_train, y_test = train_test_split(
        X,
        y,
        test_size=args.test_size,
        random_state=args.random_state,
        stratify=y,
    )

    X_train, X_test, preprocessing = fit_preprocess_train_test(X_train_raw, X_test_raw)
    artifacts_path = save_preprocessing_artifacts(preprocessing, args.artifacts_dir, df_train_final_path)

    cv = StratifiedKFold(n_splits=args.cv_splits, shuffle=True, random_state=args.random_state)
    param_dist = {
        "n_estimators": [200, 300, 400, 500],
        "learning_rate": [0.03, 0.05, 0.08, 0.1],
        "max_depth": [3, 4, 5, 6],
        "min_child_weight": [1, 3, 5, 7],
        "subsample": [0.7, 0.8, 0.9, 1.0],
        "colsample_bytree": [0.7, 0.8, 0.9, 1.0],
        "gamma": [0, 0.1, 0.3, 0.5],
        "reg_alpha": [0, 0.1, 1],
        "reg_lambda": [1, 2, 5],
    }

    base_model = XGBClassifier(random_state=args.random_state, eval_metric="logloss", n_jobs=-1)
    random_search = RandomizedSearchCV(
        estimator=base_model,
        param_distributions=param_dist,
        n_iter=args.n_iter,
        scoring="f1",
        cv=cv,
        verbose=2,
        random_state=args.random_state,
        n_jobs=-1,
        refit=True,
    )

    model_name = f"{args.run_name}_{args.developer}"

    with mlflow.start_run(run_name=args.run_name):
        mlflow.set_tag("developer", args.developer)
        mlflow.set_tag("model_full_name", model_name)
        mlflow.set_tag("target", "retard_arrivee")
        mlflow.set_tag("model_type", "classification")
        mlflow.set_tag("optimization_metric", "f1")
        mlflow.log_param("source_dataset_uri", source_uri)
        mlflow.log_param("df_train_final_local_path", str(df_train_final_path))
        mlflow.log_param("delay_threshold_min", args.delay_threshold_min)
        mlflow.log_param("n_features", X_train.shape[1])
        mlflow.log_param("n_train_rows", X_train.shape[0])
        mlflow.log_param("n_test_rows", X_test.shape[0])

        print("Searching best XGBoost classifier hyperparameters...")
        random_search.fit(X_train, y_train)
        best_model = random_search.best_estimator_

        mlflow.log_params(random_search.best_params_)
        mlflow.log_metric("best_cv_f1", float(random_search.best_score_))

        scoring = {
            "accuracy": "accuracy",
            "precision": "precision",
            "recall": "recall",
            "f1": "f1",
            "roc_auc": "roc_auc",
        }
        cv_results = cross_validate(best_model, X_train, y_train, cv=cv, scoring=scoring, n_jobs=-1)
        for metric in scoring:
            mlflow.log_metric(f"cv_{metric}_mean", float(cv_results[f"test_{metric}"].mean()))
            mlflow.log_metric(f"cv_{metric}_std", float(cv_results[f"test_{metric}"].std()))

        y_pred_train = best_model.predict(X_train)
        y_proba_train = best_model.predict_proba(X_train)[:, 1]
        y_pred_test = best_model.predict(X_test)
        y_proba_test = best_model.predict_proba(X_test)[:, 1]

        metrics = {
            "train_accuracy": accuracy_score(y_train, y_pred_train),
            "train_precision": precision_score(y_train, y_pred_train, zero_division=0),
            "train_recall": recall_score(y_train, y_pred_train, zero_division=0),
            "train_f1": f1_score(y_train, y_pred_train, zero_division=0),
            "train_roc_auc": roc_auc_score(y_train, y_proba_train),
            "test_accuracy": accuracy_score(y_test, y_pred_test),
            "test_precision": precision_score(y_test, y_pred_test, zero_division=0),
            "test_recall": recall_score(y_test, y_pred_test, zero_division=0),
            "test_f1": f1_score(y_test, y_pred_test, zero_division=0),
            "test_roc_auc": roc_auc_score(y_test, y_proba_test),
        }
        mlflow.log_metrics({k: float(v) for k, v in metrics.items()})

        with open(artifacts_path / "classification_report.json", "w", encoding="utf-8") as f:
            json.dump(classification_report(y_test, y_pred_test, zero_division=0, output_dict=True), f, ensure_ascii=False, indent=2)
        with open(artifacts_path / "confusion_matrix.json", "w", encoding="utf-8") as f:
            json.dump(confusion_matrix(y_test, y_pred_test).tolist(), f, ensure_ascii=False, indent=2)

        # Log preprocessing artifacts and the generated df_train_final parquet.
        mlflow.log_artifacts(str(artifacts_path), artifact_path="preprocessing_and_evaluation")
        mlflow.log_artifact(str(df_train_final_path), artifact_path="prepared_training_data")

        model_info = log_xgboost_model(best_model, model_name, args.registered_name, X_test.head(5))
        set_alias_if_possible(args.registered_name, args.alias, model_info)

        print("\n===== BEST PARAMS =====")
        print(random_search.best_params_)
        print("\n===== FINAL METRICS =====")
        for key, value in metrics.items():
            print(f"{key}: {value:.4f}")
        print(f"\ndf_train_final saved for regressor: {df_train_final_path}")
        print(f"Model registered: {args.registered_name} @{args.alias}")


if __name__ == "__main__":
    main()
