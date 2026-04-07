# api/main.py

import os
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
import mlflow.pyfunc
import pandas as pd
from datetime import datetime, timedelta
import uvicorn
import warnings
from src.comparison import match_predictions_with_reals, calculate_metrics
from src.fetcher import (
    fetch_real_flights_from_aerodatabox,
    fetch_all_movements_historical,
    fetch_and_save_meteo_raw,
    load_historical_json_to_dataframe,
    load_meteo_json_to_dataframe,
)
from src.features import enrich_aircraft_features, add_holiday_features
from api.schemas import PredictionRequest, PredictionResponse, SinglePrediction
from src.data_cleaning import clean_flight_data
import numpy as np
import boto3
import json

load_dotenv()

app = FastAPI(title="PPML Flight Delay Prediction API")

# Chargement du pipeline depuis MLflow
mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI"))
MODEL_URI = "models:/ppml-delay-pipeline@champion"

try:
    pipeline = mlflow.pyfunc.load_model(MODEL_URI)
    print(f" Pipeline chargé avec succès depuis {MODEL_URI}")
except Exception as e:
    print(f" Erreur lors du chargement du pipeline : {e}")
    pipeline = None


@app.get("/")
async def root():
    return {"message": "PPML Flight Delay Prediction API is running"}


@app.post("/fetch_and_save_aeroports")
async def fetch_and_save_aeroports(request: dict):
    """
    Construit le dataset des mouvements aeroports.
    Sauvegarde un seul fichier JSON sur S3.
    """
    date_debut = request.get("date_debut")  # Format YYYY-MM-DD
    date_fin = request.get("date_fin")  # Format YYYY-MM-DD
    icao = request.get("icao")  # Optionnel
    force = request.get("force", False)

    if not date_debut or not date_fin:
        raise HTTPException(
            status_code=400,
            detail="Les paramètres date_debut et date_fin sont obligatoires (YYYY-MM-DD)",
        )

    try:
        print(
            f" Construction du dataset d'entraînement du {date_debut} au {date_fin}..."
        )

        # Conversion en objets date
        start_date = datetime.strptime(date_debut, "%Y-%m-%d")
        end_date = datetime.strptime(date_fin, "%Y-%m-%d")

        current_date = start_date
        all_data = {
            "period": f"{date_debut}_to_{date_fin}",
            "retrieved_at": datetime.now().isoformat(),
            "airports": {},
            "metadata": {
                "total_days": (end_date - start_date).days + 1,
                "total_flights": 0,
            },
        }

        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            print(f"    Traitement du jour : {date_str}")

            # Récupération pour ce jour
            daily_data = fetch_all_movements_historical(
                date_str, [icao] if icao else None
            )

            # Fusion dans le gros dictionnaire
            all_data["airports"][date_str] = daily_data.get("airports", {})

            current_date += timedelta(days=1)

        # Mise à jour des métadonnées
        total_flights = sum(
            len(airport_data.get("departures", []))
            + len(airport_data.get("arrivals", []))
            for day_data in all_data["airports"].values()
            for airport_data in day_data.values()
        )
        all_data["metadata"]["total_flights"] = total_flights

        # ====================== SAUVEGARDE SUR S3 ======================
        output_key = f"raw/movements_historical_{date_debut}_to_{date_fin}.json"
        s3_full_path = f"s3://ppml2026/{output_key}"

        # Sauvegarde via boto3 (pour gros JSON)
        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            # region_name=os.getenv("AWS_DEFAULT_REGION", "eu-north-1"),
        )

        json_data = json.dumps(all_data, ensure_ascii=False, indent=2)

        s3.put_object(
            Bucket="ppml2026",
            Key=output_key,
            Body=json_data,
            ContentType="application/json",
        )

        print(f" Dataset d'entraînement sauvegardé sur S3 : {output_key}")
        print(f"   Période : {date_debut} → {date_fin}")
        print(f"   Total vols : {total_flights}")

        return {
            "status": "success",
            "message": "Dataset d'entraînement brut créé avec succès",
            "period": f"{date_debut}_to_{date_fin}",
            "total_flights": total_flights,
            "cle_fichier": output_key,
        }

    except Exception as e:
        print(f" Erreur lors de la construction du dataset d'entraînement : {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/fetch_and_save_meteo")
async def fetch_and_save_meteo(request: dict):
    """
    Récupère la météo et sauvegarde le JSON brut sur S3.
    """
    start_date_str = request.get("start_date")  # "2026-04-01"
    end_date_str = request.get("end_date")  # "2026-04-06"
    is_future = request.get("is_future", False)
    output_key = request.get("output_key")  # optionnel

    if not start_date_str or not end_date_str:
        raise HTTPException(
            status_code=400,
            detail="start_date et end_date sont obligatoires (YYYY-MM-DD)",
        )

    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

        # Coordonnées des 5 aéroports
        coord_aeroport = {
            "LFPG": (49.0097, 2.5479),  # CDG
            "LFPO": (48.7253, 2.3594),  # ORY
            "LFMN": (43.6653, 7.2150),  # NCE
            "LFLL": (45.7264, 5.0908),  # LYS
            "LFML": (43.4393, 5.2214),  # MRS
        }

        print(
            f" Lancement récupération météo {'future' if is_future else 'historical'} "
            f"du {start_date_str} au {end_date_str}"
        )

        saved_key = fetch_and_save_meteo_raw(
            coord_aeroport=coord_aeroport,
            start_date=start_date,
            end_date=end_date,
            is_future=is_future,
            output_key=output_key,
        )

        return {
            "status": "success",
            "message": "Météo brute récupérée et sauvegardée avec succès",
            "s3_key": saved_key,
            "periode": f"{start_date_str} → {end_date_str}",
            "is_future": is_future,
            "aeroports": len(coord_aeroport),
        }

    except Exception as e:
        print(f" Erreur globale : {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/construit_dataset_train")
async def build_training_dataset(request: dict):
    """
    Construit le dataset complet d'entraînement :
    - Chargement des mouvements
    - Enrichissement aircraft + features temporelles (jours fériés)
    - Ajout de la météo (-3h)
    - Sauvegarde du dataset final

      exemple input:
      {
        "movements_s3_key": "movements_historical_2025-10-05_to_2026-04-06.json",
        "meteo_s3_key": "meteo_historical_2025-10-04_to_2026-04-06.json"
      }
    """
    movements_s3_key = request.get("movements_s3_key")
    meteo_s3_key = request.get("meteo_s3_key")
    output_name = request.get("output_name")

    if not movements_s3_key:
        raise HTTPException(status_code=400, detail="movements_s3_key est obligatoire")
    if not meteo_s3_key:
        raise HTTPException(
            status_code=400, detail="meteo_s3_key est maintenant obligatoire"
        )

    try:
        print(" Construction du dataset complet (mouvements + aircraft + météo)")

        # 1. Chargement des mouvements
        df_mov = load_historical_json_to_dataframe(movements_s3_key)
        if df_mov.empty:
            raise HTTPException(
                status_code=404, detail="Aucune donnée de mouvements trouvée"
            )

        # 2. Enrichissement aircraft + holidays
        df_enriched = enrich_aircraft_features(df_mov, model_column="aircraft_model")
        df_enriched = add_holiday_features(df_enriched, date_column="scheduled_utc")

        print(f" Base enrichie | Shape: {df_enriched.shape}")

        # 3. Chargement et ajout de la météo
        print(f" Chargement et fusion de la météo : {meteo_s3_key}")
        df_meteo = load_meteo_json_to_dataframe(meteo_s3_key)

        # === NORMALISATION DES DATES ===
        df_enriched["scheduled_utc"] = pd.to_datetime(
            df_enriched["scheduled_utc"], errors="coerce"
        ).dt.tz_localize(None)

        df_meteo["time"] = pd.to_datetime(
            df_meteo["time"], errors="coerce"
        ).dt.tz_localize(None)

        df_meteo["meteo_time"] = df_meteo["time"] - pd.Timedelta(hours=3)

        # Tri obligatoire pour merge_asof
        df_enriched = df_enriched.sort_values("scheduled_utc").reset_index(drop=True)
        df_meteo = df_meteo.sort_values("meteo_time").reset_index(drop=True)

        # Merge asof
        df_final = pd.merge_asof(
            df_enriched,
            df_meteo,
            left_on="scheduled_utc",
            right_on="meteo_time",
            by="icao",
            direction="nearest",
            tolerance=pd.Timedelta(hours=6),
        )

        # Nettoyage
        df_final = df_final.drop(columns=["meteo_time", "time"], errors="ignore")

        print(f" Fusion météo terminée | Shape final: {df_final.shape}")

        # Sauvegarde
        if output_name:
            output_key = f"train/{output_name}.parquet"
        else:
            date_part = movements_s3_key.split("historical_")[-1].replace(".json", "")
            output_key = f"train/full_training_{date_part}.parquet"

        s3_full_path = f"s3://ppml2026/prod/{output_key}"

        df_final.to_parquet(
            s3_full_path,
            storage_options={
                "key": os.getenv("AWS_ACCESS_KEY_ID"),
                "secret": os.getenv("AWS_SECRET_ACCESS_KEY"),
                "anon": False,
            },
        )

        print(f" Dataset complet sauvegardé → {output_key} | Shape: {df_final.shape}")

        return {
            "status": "success",
            "message": "Dataset d'entraînement complet (mouvements + aircraft + météo) créé avec succès",
            "shape": df_final.shape,
            "cle_fichier_final": output_key,
            "total_vols": len(df_final),
        }

    except Exception as e:
        print(f" Erreur lors de la construction du dataset : {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/build_prediction_dataset")
async def build_prediction_dataset(request: dict):
    """
    Construit le dataset complet pour la prédiction (futur) :
    - Récupère les mouvements futurs (post-dates d'entraînement)
    - Récupère la météo future (avec is_future=True)
    - Enrichit avec aircraft + features temporelles
    - Ajoute la météo (-3h)
    - Sauvegarde le dataset final prêt pour l'inférence

    Exemple d'input :
    {
        "date_debut": "2026-04-08",
        "date_fin": "2026-04-15",
        "icao": null,                    // optionnel : code aéroport spécifique
        "output_name": "prediction_avril2026"  // optionnel
    }
    """
    date_debut = request.get("date_debut")  # YYYY-MM-DD
    date_fin = request.get("date_fin")  # YYYY-MM-DD
    icao = request.get("icao")  # optionnel
    output_name = request.get("output_name")

    if not date_debut or not date_fin:
        raise HTTPException(
            status_code=400,
            detail="date_debut et date_fin sont obligatoires (format YYYY-MM-DD)",
        )

    try:
        print(f" Construction du dataset de PRÉDICTION du {date_debut} au {date_fin}")

        # ====================== 1. RÉCUPÉRATION MOUVEMENTS FUTURS ======================
        print("📡 Récupération des mouvements aéroportuaires futurs...")

        movements_response = await fetch_and_save_aeroports(
            {  # on réutilise la logique existante
                "date_debut": date_debut,
                "date_fin": date_fin,
                "icao": icao,
                "force": True,  # on force pour les données futures
            }
        )

        movements_s3_key = movements_response.get("cle_fichier")
        print(f" Mouvements futurs sauvegardés → {movements_s3_key}")

        # ====================== 2. RÉCUPÉRATION MÉTÉO FUTURE ======================
        # Date début météo = 1 jour avant la date début des mouvements
        meteo_start = (
            datetime.strptime(date_debut, "%Y-%m-%d") - timedelta(days=1)
        ).strftime("%Y-%m-%d")

        print(f" Récupération de la météo future du {meteo_start} au {date_fin}...")

        meteo_response = await fetch_and_save_meteo(
            {  # on réutilise l'endpoint existant
                "start_date": meteo_start,
                "end_date": date_fin,
                "is_future": True,
                "output_key": None,  # on laisse la fonction générer le nom
            }
        )

        meteo_s3_key = meteo_response.get("s3_key")
        print(f" Météo future sauvegardée → {meteo_s3_key}")

        # ====================== 3. CONSTRUCTION DU DATASET FINAL ======================
        print(
            " Construction du dataset final de prédiction (enrichissement + fusion météo)..."
        )

        # Chargement des mouvements
        df_mov = load_historical_json_to_dataframe(movements_s3_key)
        if df_mov.empty:
            raise HTTPException(
                status_code=404, detail="Aucune donnée de mouvements future trouvée"
            )

        # Enrichissement aircraft + holidays
        df_enriched = enrich_aircraft_features(df_mov, model_column="aircraft_model")
        df_enriched = add_holiday_features(df_enriched, date_column="scheduled_utc")

        print(f" Base enrichie | Shape: {df_enriched.shape}")

        # Chargement et fusion météo
        df_meteo = load_meteo_json_to_dataframe(meteo_s3_key)

        # Normalisation des dates
        df_enriched["scheduled_utc"] = pd.to_datetime(
            df_enriched["scheduled_utc"], errors="coerce"
        ).dt.tz_localize(None)

        df_meteo["time"] = pd.to_datetime(
            df_meteo["time"], errors="coerce"
        ).dt.tz_localize(None)

        df_meteo["meteo_time"] = df_meteo["time"] - pd.Timedelta(hours=3)

        # Tri pour merge_asof
        df_enriched = df_enriched.sort_values("scheduled_utc").reset_index(drop=True)
        df_meteo = df_meteo.sort_values("meteo_time").reset_index(drop=True)

        # Merge asof
        df_final = pd.merge_asof(
            df_enriched,
            df_meteo,
            left_on="scheduled_utc",
            right_on="meteo_time",
            by="icao",
            direction="nearest",
            tolerance=pd.Timedelta(hours=6),
        )

        # Nettoyage
        df_final = df_final.drop(columns=["meteo_time", "time"], errors="ignore")

        print(f" Fusion météo terminée | Shape final: {df_final.shape}")

        # ====================== 4. SAUVEGARDE ======================
        if output_name:
            output_key = f"predict/{output_name}.parquet"
        else:
            output_key = f"predict/prediction_{date_debut}_to_{date_fin}.parquet"

        s3_full_path = f"s3://ppml2026/prod/{output_key}"

        df_final.to_parquet(
            s3_full_path,
            storage_options={
                "key": os.getenv("AWS_ACCESS_KEY_ID"),
                "secret": os.getenv("AWS_SECRET_ACCESS_KEY"),
                "anon": False,
            },
        )

        print(
            f" Dataset de prédiction sauvegardé → {output_key} | Shape: {df_final.shape}"
        )

        return {
            "status": "success",
            "message": "Dataset de prédiction complet créé avec succès",
            "period": f"{date_debut} → {date_fin}",
            "shape": df_final.shape,
            "cle_fichier_final": output_key,
            "total_vols": len(df_final),
            "movements_s3_key": movements_s3_key,
            "meteo_s3_key": meteo_s3_key,
        }

    except Exception as e:
        print(f" Erreur lors de la construction du dataset de prédiction : {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/predire_et_sauvegarder")
async def predire_et_sauvegarder(request: PredictionRequest):
    """
    Endpoint qui fait les prédictions ET les sauvegarde sur S3.
    Utilisé par Streamlit quand l'utilisateur veut garder une trace.
    """
    if pipeline is None:
        raise HTTPException(status_code=503, detail="Pipeline non disponible")

    if not request.records or len(request.records) == 0:
        raise HTTPException(
            status_code=400, detail="Aucune donnée à prédire (records vide)"
        )

    try:
        # 1. Conversion en DataFrame
        df_to_predict = pd.DataFrame(request.records)
        print(
            f" Réception de {len(df_to_predict)} vols pour prédiction et sauvegarde..."
        )

        # 2. Nettoyage
        df_clean = clean_flight_data(df_to_predict)
        if len(df_clean) == 0:
            raise HTTPException(
                status_code=400, detail="Aucun vol valide après nettoyage"
            )

        # 3. Prédiction
        X = df_clean.drop(columns=["delay_minutes"], errors="ignore")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            predictions = pipeline.predict(X)

        predictions_list = [float(p) for p in predictions]

        # 4. Construction des résultats + ajout de scheduled_utc
        results = []
        for idx, row in df_clean.iterrows():
            pred_value = predictions_list[idx] if idx < len(predictions_list) else 0.0

            results.append(
                SinglePrediction(
                    mouvement_id=int(row.get("mouvement_id", idx)),
                    icao=row.get("icao", "Unknown"),
                    type=row.get("type", "Unknown"),
                    airline=row.get("airline", "Unknown"),
                    scheduled_hour=int(row.get("scheduled_hour", 0)),
                    scheduled_utc=str(row.get("scheduled_utc"))
                    if row.get("scheduled_utc") is not None
                    else None,
                    flight_number=str(row.get("flight_number")),
                    terminal_dep=str(row.get("terminal_dep", "Unknown")),
                    terminal_arr=str(row.get("terminal_arr", "Unknown")),
                    predicted_delay_minutes=pred_value,
                    prediction_timestamp=datetime.now(),
                )
            )

        # 5. Sauvegarde sur S3
        timestamp = datetime.now().strftime("%Y-%m-%d")
        output_key = f"predictions/predictions_{timestamp}.parquet"
        s3_full_path = f"s3://ppml2026/prod/{output_key}"

        # Création du DataFrame à sauvegarder
        df_results = pd.DataFrame([r.dict() for r in results])

        # Sauvegarde sur S3
        df_results.to_parquet(
            s3_full_path,
            storage_options={
                "key": os.getenv("AWS_ACCESS_KEY_ID"),
                "secret": os.getenv("AWS_SECRET_ACCESS_KEY"),
            },
        )

        print(f" Prédictions sauvegardées sur S3 → {output_key}")

        # 6. Réponse vers Streamlit
        return PredictionResponse(
            status="success",
            message=f"{len(results)} prédictions générées et sauvegardées sur S3",
            predictions=results,
            total_predictions=len(results),
        )

    except Exception as e:
        print(f" Erreur dans /predire_et_sauvegarder : {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/importer_vols_reels")
async def importer_vols_reels(request: dict):
    """
    Récupère les vols réels depuis l'API Aerodatabox et les sauvegarde sur S3.
    """
    date_str = request.get("date")  # Format attendu : YYYY-MM-DD
    icao = request.get("icao")  # Optionnel
    force = request.get("force", False)

    if not date_str:
        raise HTTPException(
            status_code=400, detail="Le paramètre 'date' (YYYY-MM-DD) est obligatoire"
        )

    try:
        print(
            f" Début import vols réels pour la date : {date_str} | icao={icao or 'tous'}"
        )

        # Appel à la fonction de récupération (synchrone)
        df_reels = fetch_real_flights_from_aerodatabox(date_str, icao)

        if df_reels.empty:
            raise HTTPException(
                status_code=404, detail=f"Aucun vol réel trouvé pour la date {date_str}"
            )

        # Sauvegarde sur S3
        output_key = f"vols_recuperes/vols_reels_{date_str}.parquet"
        s3_full_path = f"s3://ppml2026/prod/{output_key}"

        df_reels.to_parquet(
            s3_full_path,
            storage_options={
                "key": os.getenv("AWS_ACCESS_KEY_ID"),
                "secret": os.getenv("AWS_SECRET_ACCESS_KEY"),
                "anon": False,
            },
        )

        print(f" {len(df_reels)} vols réels sauvegardés sur S3 → {output_key}")

        return {
            "status": "success",
            "message": f"{len(df_reels)} vols réels importés avec succès",
            "date": date_str,
            "icao": icao,
            "nombre_vols": len(df_reels),
            "cle_fichier": output_key,
        }

    except Exception as e:
        print(f" Erreur lors de l'import vols réels : {e}")
        raise HTTPException(
            status_code=500, detail=f"Erreur lors de l'import : {str(e)}"
        )


@app.post("/comparer_predictions_reel")
async def comparer_predictions_reel(request: dict):
    """
    Compare les prédictions avec les vols réels pour une date donnée.
    Effectue le matching et calcule les métriques.
    """
    date_str = request.get("date")  # Format : YYYY-MM-DD
    icao = request.get("icao")  # Optionnel

    if not date_str:
        raise HTTPException(
            status_code=400, detail="Le paramètre 'date' (YYYY-MM-DD) est obligatoire"
        )

    try:
        # Chemins sur S3
        pred_key = f"prod/predictions/predictions_{date_str}.parquet"
        real_key = f"prod/vols_recuperes/vols_reels_{date_str}.parquet"

        pred_path = f"s3://ppml2026/{pred_key}"
        real_path = f"s3://ppml2026/{real_key}"

        print(f" Comparaison pour la date {date_str}...")

        # Chargement des fichiers depuis S3
        df_pred = pd.read_parquet(
            pred_path,
            storage_options={
                "key": os.getenv("AWS_ACCESS_KEY_ID"),
                "secret": os.getenv("AWS_SECRET_ACCESS_KEY"),
            },
        )

        df_real = pd.read_parquet(
            real_path,
            storage_options={
                "key": os.getenv("AWS_ACCESS_KEY_ID"),
                "secret": os.getenv("AWS_SECRET_ACCESS_KEY"),
            },
        )

        if df_pred.empty or df_real.empty:
            raise HTTPException(
                status_code=404, detail=f"Données manquantes pour la date {date_str}"
            )

        # Matching sur icao + flight_number + scheduled_utc
        df_comparison = match_predictions_with_reals(df_pred, df_real)

        if df_comparison.empty:
            raise HTTPException(status_code=404, detail="Aucun vol n'a pu être apparié")

        metrics = calculate_metrics(df_comparison)

        # Sauvegarde de la comparaison sur S3
        comp_key = f"prod/comparaisons/comparaison_{date_str}.parquet"
        comp_path = f"s3://ppml2026/{comp_key}"

        df_comparison.to_parquet(
            comp_path,
            storage_options={
                "key": os.getenv("AWS_ACCESS_KEY_ID"),
                "secret": os.getenv("AWS_SECRET_ACCESS_KEY"),
            },
        )

        print(f" Comparaison sauvegardée : {comp_key}")
        # Conversion : tout passer en Python natif
        metrics_clean = {
            k: float(v) if isinstance(v, (np.integer, np.floating)) else v
            for k, v in metrics.items()
        }

        return {
            "status": "success",
            "message": f"Comparaison effectuée avec {len(df_comparison)} vols appariés",
            "date": date_str,
            "nombre_vols_apparies": int(len(df_comparison)),
            "metriques": metrics_clean,
            "cle_fichier_comparaison": comp_key,
        }

    except Exception as e:
        print(f" Erreur lors de la comparaison : {e}")
        raise HTTPException(
            status_code=500, detail=f"Erreur lors de la comparaison : {str(e)}"
        )


# MAIN
if __name__ == "__main__":
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)
