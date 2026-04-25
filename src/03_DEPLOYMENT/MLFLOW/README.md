---
title: PPML Mlflow
emoji: ✈️
colorFrom: blue
colorTo: gray
sdk: docker
pinned: false
---

# FlyOnTime MLflow Space

This Hugging Face Space hosts the MLflow tracking server and model registry used by FlyOnTime.

The modeling notebooks and training scripts log experiments to this server. FastAPI then loads the promoted model versions from the registry.

## Main files

```text
MLFLOW/
├── Dockerfile
├── requirements.txt
└── README.md
```

## Runtime command

```bash
mlflow server -p $PORT \
  --host 0.0.0.0 \
  --backend-store-uri $BACKEND_STORE_URI \
  --default-artifact-root $ARTIFACT_STORE_URI \
  --x-frame-options NONE \
  --allowed-hosts '*' \
  --cors-allowed-origins '*'
```

## Required variables and secrets

| Name | Type | Purpose |
|---|---|---|
| `PORT` | Variable | Hugging Face runtime port. |
| `BACKEND_STORE_URI` | Secret | Database URI for MLflow metadata. |
| `ARTIFACT_STORE_URI` | Secret | Artifact root, usually S3. |
| `AWS_ACCESS_KEY_ID` | Secret | AWS access key for artifacts. |
| `AWS_SECRET_ACCESS_KEY` | Secret | AWS secret key for S3. |
| `AWS_ACCESS_KEY` | Secret | Legacy alternative key name. |
| `AWS_SECRET_ACCESS_KEY_ID` | Secret | Legacy alternative secret name. |

Preferred clean AWS names are `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
