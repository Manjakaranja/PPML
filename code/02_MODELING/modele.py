"""
modele.py
Projet PPML - FlyOnTime
------------------------
Entraînement du classifieur XGBoost, logging MLflow,
prédictions sur les données futures et visualisations.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import mlflow
import mlflow.xgboost
from dotenv import load_dotenv
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from xgboost import XGBClassifier

from extraction import charger_data_s3, reparer_vols
from transform import (
    purger_et_voir,
    creer_cible,
    extract_date_features,
    encoder_categories,
    DATETIME_COLS,
)
from load import (
    ALIAS_NAME,
    EXPERIMENT_NAME,
    MODEL_NAME_TAG,
    PRENOM,
    REGISTERED_NAME,
    RUN_NAME,
    TYPE_MODEL,
    enregistrer_modele,
    init_mlflow,
)

load_dotenv()


# ---------------------------------------------------------------------------
# 1. PRÉPARATION DES FEATURES
# ---------------------------------------------------------------------------

# Colonnes à exclure pour éviter la fuite de données
COLS_LEAKAGE = ["delay", "retard", "target", "time_dep", "time_arr"]


def preparer_features(df: pd.DataFrame):
    """
    Extrait X et y depuis le DataFrame d'entraînement :
    - Supprime les colonnes à risque de data leakage
    - Extrait les features datetime
    - Encode les catégories
    Retourne (X_processed, y, encoder, cols_a_virer).
    """
    colonnes_dispo = df.columns.tolist()

    # Variable cible
    nom_retard = next(
        (c for c in colonnes_dispo if "arrival_delay_min" in c), None
    )
    if nom_retard is None:
        raise ValueError("❌ Aucune colonne de retard trouvée pour créer la cible.")

    y = (df[nom_retard] > 15).astype(int)

    # Suppression des colonnes leakage
    cols_a_virer = [
        c for c in colonnes_dispo
        if any(x in c.lower() for x in COLS_LEAKAGE)
    ]
    X = df.drop(columns=cols_a_virer)

    # Feature engineering datetime
    X_processed = extract_date_features(X, DATETIME_COLS)

    # Encodage
    X_processed, encoder = encoder_categories(X_processed)

    return X_processed, y, encoder, cols_a_virer


# ---------------------------------------------------------------------------
# 2. ENTRAÎNEMENT
# ---------------------------------------------------------------------------

def entrainer_xgboost(X_train, y_train) -> XGBClassifier:
    """Instancie et entraîne le classifieur XGBoost."""
    model = XGBClassifier(
        n_estimators=300,
        learning_rate=0.05,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        eval_metric="logloss",
    )
    print(f"🚀 Entraînement {TYPE_MODEL} en cours...")
    model.fit(X_train, y_train)
    return model


# ---------------------------------------------------------------------------
# 3. ÉVALUATION
# ---------------------------------------------------------------------------

def evaluer_modele(model: XGBClassifier, X_test, y_test) -> dict:
    """Calcule et retourne les métriques de classification."""
    y_pred = model.predict(X_test)
    y_probs = model.predict_proba(X_test)[:, 1]

    metrics = {
        "test_accuracy": accuracy_score(y_test, y_pred),
        "test_precision": precision_score(y_test, y_pred),
        "test_recall": recall_score(y_test, y_pred),
        "test_f1": f1_score(y_test, y_pred),
        "test_auc": roc_auc_score(y_test, y_probs),
    }

    print("\n📊 RÉSULTATS DU MODÈLE :")
    for name, value in metrics.items():
        print(f"🔹 {name.replace('test_', '').capitalize()}: {value:.4f}")

    return metrics


# ---------------------------------------------------------------------------
# 4. VISUALISATION DES IMPORTANCES
# ---------------------------------------------------------------------------

def plot_feature_importance(model: XGBClassifier, X_train: pd.DataFrame) -> None:
    """Affiche les 20 variables les plus importantes du modèle."""
    importances = model.feature_importances_
    labels = model.get_booster().feature_names or X_train.columns

    if len(labels) != len(importances):
        min_len = min(len(labels), len(importances))
        feature_df = pd.DataFrame(
            {"Feature": list(labels)[:min_len], "Importance": importances[:min_len]}
        )
    else:
        feature_df = pd.DataFrame({"Feature": list(labels), "Importance": importances})

    feature_df = feature_df.sort_values(by="Importance", ascending=False).head(20)

    plt.figure(figsize=(10, 6), facecolor="#1E1E1E")
    ax = sns.barplot(x="Importance", y="Feature", data=feature_df, palette="viridis")
    ax.set_facecolor("#1E1E1E")
    ax.set_title(
        f"🚀 Top 20 des variables — Modèle {TYPE_MODEL}",
        color="white",
        fontsize=14,
    )
    ax.set_xlabel("Importance relative", color="white")
    ax.set_ylabel("Variables", color="white")
    plt.xticks(color="white")
    plt.yticks(color="white")
    sns.despine(left=True, bottom=True)
    plt.tight_layout()
    plt.show()


# ---------------------------------------------------------------------------
# 5. PRÉDICTION SUR DONNÉES FUTURES
# ---------------------------------------------------------------------------

def predire_futur(
    model: XGBClassifier,
    df_predict_final: pd.DataFrame,
    encoder,
    cols_a_virer: list,
) -> pd.DataFrame:
    """
    Applique le modèle entraîné sur les données de prédiction (avril 2026).
    Retourne un DataFrame enrichi avec 'prediction_retard', 'Confiance' et 'Prédiction'.
    """
    X_predict = df_predict_final.drop(
        columns=[c for c in cols_a_virer if c in df_predict_final.columns]
    )
    X_predict_processed = extract_date_features(X_predict, DATETIME_COLS)
    X_predict_processed, _ = encoder_categories(X_predict_processed, encoder)

    predictions = model.predict(X_predict_processed)
    probabilities = model.predict_proba(X_predict_processed)[:, 1]

    df_resultats = df_predict_final.copy()
    df_resultats["prediction_retard"] = predictions
    df_resultats["Confiance"] = (probabilities * 100).round(1)
    df_resultats["Prédiction"] = np.where(predictions == 1, "⚠️ RETARD", "✅ À L'HEURE")

    print("✅ Prédictions générées avec succès.")
    return df_resultats


# ---------------------------------------------------------------------------
# 6. PIPELINE PRINCIPAL
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # -- Extraction --
    df_train = charger_data_s3("datasets/SignofFlightsDataset_20260416_233018_CLEAN.csv")
    df_predict = charger_data_s3(
        "datasets/SignofFlightsDataset_future_20260416_233150_CLEAN.csv"
    )
    df_train_final = reparer_vols(df_train)
    df_predict_final = reparer_vols(df_predict)

    # -- Transform --
    df_train_final = purger_et_voir(df_train_final, "TRAIN")
    df_predict_final = purger_et_voir(df_predict_final, "PREDICT")
    df_train_final = creer_cible(df_train_final)

    # -- Features --
    X_processed, y, encoder, cols_a_virer = preparer_features(df_train_final)
    X_train, X_test, y_train, y_test = train_test_split(
        X_processed, y, test_size=0.2, random_state=42, stratify=y
    )

    # -- MLflow --
    init_mlflow()
    mlflow.set_experiment(EXPERIMENT_NAME)

    with mlflow.start_run(run_name=f"{RUN_NAME}_{PRENOM}"):
        mlflow.xgboost.autolog()
        mlflow.set_tag("developer", PRENOM)
        mlflow.set_tag("model_full_name", MODEL_NAME_TAG)

        # Entraînement
        model = entrainer_xgboost(X_train, y_train)

        # Métriques
        metrics = evaluer_modele(model, X_test, y_test)
        mlflow.log_metrics(metrics)

        # Enregistrement dans le registry
        model_info = mlflow.xgboost.log_model(
            model,
            artifact_path="model",
            registered_model_name=REGISTERED_NAME,
        )
        enregistrer_modele(model_info)

    # -- Visualisation importances --
    plot_feature_importance(model, X_train)

    # -- Prédictions futures --
    df_resultats = predire_futur(model, df_predict_final, encoder, cols_a_virer)
    print(df_resultats.head())
