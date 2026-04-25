---
title: PPML FASTAPI
emoji: ✈️
colorFrom: blue
colorTo: red
sdk: docker
pinned: false
---

# FlyOnTime FastAPI Space

This Hugging Face Space hosts the backend API for FlyOnTime.

It receives requests from the Streamlit frontend, launches the single-flight lookup and ETL pipeline, prepares inference features, loads the promoted XGBoost models from MLflow and returns the final prediction response.

## Main files

```text
FASTAPI/
├── app.py
├── Dockerfile
├── requirements.txt
├── call_api/
└── preprocessing/
```
### Why does FastAPI contain a `preprocessing/` folder?

The FastAPI service includes its own `preprocessing/` module because the deployed API must be self-contained on Hugging Face Spaces.

Although similar logic exists in `src/01_ETL`, the deployment version is used specifically for online inference:

- retrieving the single-flight dataset generated for one user request;
- applying the same feature preparation logic as during training;
- aligning live API data with the model input schema;
- uploading processed inference files to S3;
- keeping the FastAPI Space independent from the offline ETL folder.

This avoids broken imports during deployment and makes the API easier to run as a standalone service.

## Main endpoints

| Endpoint | Role |
|---|---|
| `GET /` | Redirects to `/docs`. |
| `GET /health` | Health check endpoint. |
| `GET /debug-models` | Debug endpoint for model / schema loading. |
| `POST /predict` | Main prediction endpoint used by Streamlit. |

## Models loaded

```python
CLASSIFIER_MODEL_URI = "models:/XGBoost_Classifier_registered@challenger"
REGRESSOR_MODEL_URI = "models:/XGBoost_Regressor_registered@challenger"
```

## Required variables and secrets

| Name | Type | Purpose |
|---|---|---|
| `PORT` | Variable | Hugging Face runtime port. |
| `ENABLE_S3_UPLOAD` | Variable | Enables request output upload to S3. |
| `S3_BUCKET_NAME` | Variable | Main S3 bucket. |
| `S3_PREFIX` | Variable | Raw output prefix. |
| `MLFLOW_TRACKING_URI` | Secret | Remote MLflow Space URL. |
| `AWS_ACCESS_KEY_ID` | Secret | AWS access key. |
| `AWS_SECRET_ACCESS_KEY` | Secret | AWS secret key. |
| `AWS_DEFAULT_REGION` | Secret | AWS region. |
| `API_KEY` | Secret | External API key for flight data. |
| `API_KEY_NAME` | Secret | API provider identifier. |

## Run locally

```bash
uvicorn app:app --host 0.0.0.0 --port 7860
```
