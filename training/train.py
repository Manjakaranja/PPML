# train.py - Version finale propre avec MLproject

import os
import yaml
import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn
import argparse
from pathlib import Path
from dotenv import load_dotenv
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from category_encoders import TargetEncoder
from sklearn.pipeline import Pipeline
import xgboost as xgb
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import boto3
import logging

load_dotenv()


# ====================== ARGUMENTS (pour MLproject) ======================
def parse_args():
    parser = argparse.ArgumentParser(description="Entraînement PPML Flight Delay")
    parser.add_argument("--n-estimators", type=int, default=1500)
    parser.add_argument("--learning-rate", type=float, default=0.04)
    parser.add_argument("--max-depth", type=int, default=9)
    parser.add_argument("--test-size", type=float, default=0.2)
    return parser.parse_args()


# ====================== CONFIG ======================
def load_config():
    config_path = Path("config/config.yaml")
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_historical_data_from_s3():
    bucket_name = (os.getenv("BUCKET"),)
    try:
        session = boto3.Session(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION", "eu-north-1"),
        )
        bucket = os.getenv("BUCKET")
        s3_key = "datasets/vue_historical.parquet"
        df = pd.read_parquet(f"s3://{bucket}/{s3_key}")
        logging.info(f"données historiques chargées pour le train")
        return df
    except Exception as e:
        logging.info(f" Erreur chargement depuis S3 : {e}")
        logging.info(
            "Vérifiez que le bucket S3 est accessible et que les credentials sont corrects."
        )
        return pd.DataFrame()


# ====================== NETTOYAGE ======================
def clean_flight_data(
    df: pd.DataFrame,
    is_training: bool = True,
    delay_lower: int = -30,
    delay_upper: int = 300,
) -> pd.DataFrame:
    df = df.copy()
    initial_shape = len(df)

    cols_to_drop = [
        "flight_number",
        "airport_name",
        "scheduled_utc",
        "revised_utc",
        "runway_utc",
        "meteo_hour_start",
        "scheduled_hour_bin",
        "meteo_hour_bin_used",
        "flight_date",
        "status",
    ]
    if is_training:
        cols_to_drop.append("mouvement_id")

    df = df.drop(columns=[col for col in cols_to_drop if col in df.columns])

    if is_training:
        df = df[df["delay_minutes"].notna()].copy()
        df = df[
            (df["delay_minutes"] >= delay_lower) & (df["delay_minutes"] <= delay_upper)
        ].copy()
    else:
        # Mode Future
        print(f" Réception de {initial_shape} vols pour prédiction...")

        if "scheduled_utc" in df.columns:
            before = len(df)
            df = df[df["scheduled_utc"].notna()].copy()
            removed = before - len(df)
            if removed > 0:
                print(f"   → {removed:,} vols supprimés car scheduled_utc is NaT/NULL")

        if "delay_minutes" in df.columns:
            before = len(df)
            df = df[df["delay_minutes"].isna()].copy()
            removed = before - len(df)
            if removed > 0:
                print(f"   → {removed:,} vols supprimés car delay_minutes était rempli")

    # Nettoyage standard
    df["terminal_dep"] = df["terminal_dep"].fillna("Unknown")
    df["terminal_arr"] = df["terminal_arr"].fillna("Unknown")

    if all(col in df.columns for col in ["type", "icao", "destination_icao"]):
        mask_arrival = (df["type"] == "arrival") & (df["destination_icao"].isna())
        df.loc[mask_arrival, "destination_icao"] = df.loc[mask_arrival, "icao"]

    df["destination_icao"] = df["destination_icao"].fillna("UNKNOWN")

    # Remplissage météo
    meteo_cols = [
        "temperature_2m",
        "relative_humidity_2m",
        "wind_speed_10m",
        "wind_gusts_10m",
        "pressure_msl",
        "precipitation",
        "cloud_cover",
    ]
    for col in meteo_cols:
        if col in df.columns:
            df[col] = df.groupby("icao")[col].transform(lambda x: x.fillna(x.median()))
            df[col] = df[col].fillna(
                df[col].median() if not pd.isna(df[col].median()) else 0
            )

    # Features temporelles
    df["scheduled_hour"] = df["scheduled_hour"].fillna(12.0)
    df["day_of_week"] = df["day_of_week"].fillna(0.0)

    df["is_weekend"] = df["day_of_week"].isin([0, 6]).astype(int)
    df["hour_sin"] = np.sin(2 * np.pi * df["scheduled_hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["scheduled_hour"] / 24)

    df["day_of_week"] = df["day_of_week"].astype(int)
    df["scheduled_hour"] = df["scheduled_hour"].astype(int)

    final_shape = len(df)
    if not is_training and final_shape < initial_shape:
        print(
            f"Future : {initial_shape - final_shape:,} vols supprimés au total pendant le nettoyage"
        )

    print(f" Nettoyage terminé | Shape : {df.shape} | Training = {is_training}")
    return df


# ====================== PREPROCESSING ======================
def prepare_data(df_clean: pd.DataFrame, test_size=0.2, random_state=42):
    y = df_clean["delay_minutes"].copy()
    X = df_clean.drop(columns=["delay_minutes"]).copy()

    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=test_size, random_state=random_state, shuffle=True
    )

    print(f" Préparation terminée | X_train: {X_train.shape} | X_val: {X_val.shape}")
    return X_train, X_val, y_train, y_val


def create_preprocessor():
    high_card_cols = ["airline", "destination_icao"]
    low_card_cols = ["icao", "type", "terminal_dep", "terminal_arr", "sky_condition"]
    num_cols = [
        "temperature_2m",
        "relative_humidity_2m",
        "wind_speed_10m",
        "wind_gusts_10m",
        "pressure_msl",
        "precipitation",
        "cloud_cover",
        "scheduled_hour",
        "day_of_week",
        "has_precipitation",
        "is_weekend",
        "hour_sin",
        "hour_cos",
    ]

    preprocessor = ColumnTransformer(
        transformers=[
            (
                "target_enc",
                TargetEncoder(cols=high_card_cols, smoothing=10, min_samples_leaf=20),
                high_card_cols,
            ),
            (
                "onehot",
                OneHotEncoder(
                    sparse_output=False, handle_unknown="ignore", drop="if_binary"
                ),
                low_card_cols,
            ),
            ("num", "passthrough", num_cols),
        ],
        remainder="drop",
    )
    return preprocessor


def create_full_pipeline(preprocessor, model):
    return Pipeline([("preprocessor", preprocessor), ("model", model)])


# ====================== MODELING ======================
def train_xgboost(
    X_train, y_train, X_val, y_val, n_estimators=1500, learning_rate=0.04, max_depth=9
):
    print(" Entraînement du modèle XGBoost...")
    model = xgb.XGBRegressor(
        n_estimators=n_estimators,
        learning_rate=learning_rate,
        max_depth=max_depth,
        min_child_weight=4,
        subsample=0.85,
        colsample_bytree=0.75,
        gamma=0.1,
        random_state=42,
        tree_method="hist",
        eval_metric="mae",
        early_stopping_rounds=80,
        verbosity=0,
    )

    model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)
    print(f" Entraînement terminé - Meilleur itération : {model.best_iteration}")
    return model


def evaluate_model(model, X_val, y_val):
    y_pred = model.predict(X_val)
    mae = mean_absolute_error(y_val, y_pred)
    rmse = np.sqrt(mean_squared_error(y_val, y_pred))
    r2 = r2_score(y_val, y_pred)

    print(f"\n Évaluation :")
    print(f"   MAE  : {mae:.2f} minutes")
    print(f"   RMSE : {rmse:.2f} minutes")
    print(f"   R²   : {r2:.4f}")
    print(f"   Erreur médiane : {np.median(np.abs(y_val - y_pred)):.2f} minutes")

    return mae, rmse, r2


def prepare_and_encode(X_train, X_val, y_train, y_val):
    preprocessor = create_preprocessor()

    X_train_encoded = preprocessor.fit_transform(X_train, y_train)
    X_val_encoded = preprocessor.transform(X_val)

    onehot_names = preprocessor.named_transformers_["onehot"].get_feature_names_out()
    feature_names = (
        ["airline", "destination_icao"]
        + list(onehot_names)
        + [
            "temperature_2m",
            "relative_humidity_2m",
            "wind_speed_10m",
            "wind_gusts_10m",
            "pressure_msl",
            "precipitation",
            "cloud_cover",
            "scheduled_hour",
            "day_of_week",
            "has_precipitation",
            "is_weekend",
            "hour_sin",
            "hour_cos",
        ]
    )

    X_train_encoded = pd.DataFrame(
        X_train_encoded, columns=feature_names, index=X_train.index
    )
    X_val_encoded = pd.DataFrame(
        X_val_encoded, columns=feature_names, index=X_val.index
    )

    print(f" Encodage terminé → {X_train_encoded.shape[1]} features")
    return X_train_encoded, X_val_encoded, preprocessor


# ====================== MAIN ======================
def main():
    args = parse_args()
    config = load_config()

    print("=" * 90)
    print(" PPML - Entraînement via MLproject")
    print("=" * 90)

    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI"))
    mlflow.set_experiment(config["mlflow"]["experiment_name"])

    with mlflow.start_run(run_name=config["mlflow"]["run_name_prefix"]) as run:
        # Log des hyperparamètres
        mlflow.log_param("n_estimators", args.n_estimators)
        mlflow.log_param("learning_rate", args.learning_rate)
        mlflow.log_param("max_depth", args.max_depth)
        mlflow.log_param("test_size", args.test_size)

        # # Chargement + Nettoyage
        print("\n Chargement du dataset historique...")
        df_historical = load_historical_data_from_s3()

        print("\n Nettoyage...")
        df_clean = clean_flight_data(df_historical, is_training=True)

        # Préparation
        print("\n Préparation des données...")
        X_train, X_val, y_train, y_val = prepare_data(
            df_clean, test_size=args.test_size
        )

        # Encodage
        print("\n Encodage des features...")
        X_train_encoded, X_val_encoded, preprocessor = prepare_and_encode(
            X_train, X_val, y_train, y_val
        )

        # Entraînement
        print("\n Entraînement du modèle...")
        model = train_xgboost(
            X_train_encoded,
            y_train,
            X_val_encoded,
            y_val,
            n_estimators=args.n_estimators,
            learning_rate=args.learning_rate,
            max_depth=args.max_depth,
        )

        full_pipeline = create_full_pipeline(preprocessor, model)

        mae, rmse, r2 = evaluate_model(model, X_val_encoded, y_val)

        # Logging
        mlflow.log_metric("mae", round(mae, 4))
        mlflow.log_metric("rmse", round(rmse, 4))
        mlflow.log_metric("r2", round(r2, 4))

        # Enregistrement
        print("\n Enregistrement du pipeline complet...")
        mlflow.sklearn.log_model(
            sk_model=full_pipeline,
            artifact_path="model",
            registered_model_name=config["mlflow"]["model_name"],
        )

        # Alias champion
        try:
            client = mlflow.MlflowClient()
            versions = client.search_model_versions(
                f"name='{config['mlflow']['model_name']}'"
            )
            latest_version = max(int(v.version) for v in versions)
            client.set_registered_model_alias(
                config["mlflow"]["model_name"], "champion", latest_version
            )
            print(f" Alias 'champion' ajouté sur la version {latest_version}")
        except Exception as e:
            print(f" Impossible d'ajouter l'alias champion : {e}")

        print(f"\n Entraînement terminé avec succès !")
        print(f"   Pipeline disponible → {config['mlflow']['model_name']}@champion")
        print(f"   Run ID → {run.info.run_id}")


if __name__ == "__main__":
    main()
