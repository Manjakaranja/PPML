"""
load.py
Projet PPML - FlyOnTime
------------------------
Configuration de la connexion au serveur MLflow distant,
initialisation de l'expérience et helpers pour le Model Registry.
"""

import mlflow
from mlflow.tracking import MlflowClient


# ---------------------------------------------------------------------------
# 1. CONFIGURATION
# ---------------------------------------------------------------------------

MLFLOW_REMOTE_URI = "https://stoneray-ppml-mlflow.hf.space/"

PRENOM = "Ludo"
TYPE_MODEL = "XGBoost"  # "XGBoost" ou "RandomForest"

EXPERIMENT_NAME = "FlyOnTime_classif"
RUN_NAME = f"{TYPE_MODEL}_classifier"
MODEL_NAME_TAG = f"{TYPE_MODEL}_classifier_{PRENOM}"
REGISTERED_NAME = "FlyOnTime_classif"
ALIAS_NAME = "challenger"


# ---------------------------------------------------------------------------
# 2. INITIALISATION
# ---------------------------------------------------------------------------

def init_mlflow() -> None:
    """Connecte MLflow au serveur distant et initialise l'expérience."""
    mlflow.set_tracking_uri(MLFLOW_REMOTE_URI)

    # Clore tout run actif résiduel
    while mlflow.active_run():
        mlflow.end_run()

    mlflow.set_experiment(EXPERIMENT_NAME)

    print(f"✅ Connecté à : {MLFLOW_REMOTE_URI}")
    print(f"🚀 Run : {RUN_NAME} | Tag : {MODEL_NAME_TAG}")


# ---------------------------------------------------------------------------
# 3. MODEL REGISTRY
# ---------------------------------------------------------------------------

def enregistrer_modele(model_info) -> None:
    """
    Enregistre le modèle dans le MLflow Model Registry
    et lui attribue l'alias configuré.
    """
    client = MlflowClient()
    client.set_registered_model_alias(
        REGISTERED_NAME,
        ALIAS_NAME,
        str(model_info.registered_model_version),
    )
    print(f"✅ Modèle enregistré : {REGISTERED_NAME} @{ALIAS_NAME}")


# ---------------------------------------------------------------------------
# 4. PIPELINE PRINCIPAL
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    init_mlflow()
