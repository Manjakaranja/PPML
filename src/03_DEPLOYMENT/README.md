# 03_DEPLOYMENT — FlyOnTime production deployment

This folder contains the deployment layer of the FlyOnTime project.

The goal of this part is to turn the modeling work into a real user-facing application. Instead of keeping the project as notebooks only, the team deployed three independent services on Hugging Face Spaces:

```text
03_DEPLOYMENT/
├── FASTAPI/       # Backend API, orchestration and inference service
├── MLFLOW/        # Remote experiment tracking server and model registry
└── STREAMLIT/     # User-facing web application
```

Each service has its own Docker image, dependencies, runtime and environment variables. This separation makes the architecture easier to understand, maintain, debug and present.

---

## 1. Global architecture

```text
User
 ↓
Streamlit Space
 ↓ JSON request
FastAPI Space
 ↓
Single-flight lookup + API calls
 ↓
ETL / feature preparation
 ↓
MLflow Space / model registry
 ↓
XGBoost classifier + XGBoost regressor
 ↓ JSON response
Streamlit result display
```

The frontend does not run machine learning models directly. It only collects and validates the user input. The backend handles orchestration, external API calls, feature preparation, model loading and prediction. MLflow centralizes the model versions and lets FastAPI load the promoted models by registry alias.

---

# 2. FastAPI Space

## Role

FastAPI is the backend orchestration service.

It receives the request from Streamlit, launches the single-flight pipeline, prepares the features, loads the promoted XGBoost models from MLflow and returns the prediction response.

Current folder structure:

```text
FASTAPI/
├── app.py
├── Dockerfile
├── requirements.txt
├── call_api/
├── preprocessing/
└── README.md
```

Main responsibilities:

- expose the prediction API;
- normalize user inputs such as flight number and airport names;
- launch the on-demand flight lookup pipeline;
- read and transform the single-flight data;
- upload raw and processed files to S3 when enabled;
- align inference features with the training schema;
- load XGBoost models from MLflow;
- return user-readable predictions and error messages.

---

## FastAPI application

The main application is:

```text
FASTAPI/app.py
```

The API is configured as:

```python
app = FastAPI(
    title="FlyOnTime FastAPI",
    description="API for flight delay prediction",
    version="3.0.0"
)
```

The backend currently uses XGBoost models registered in MLflow:

```python
CLASSIFIER_MODEL_URI = "models:/XGBoost_Classifier_registered@challenger"
REGRESSOR_MODEL_URI = "models:/XGBoost_Regressor_registered@challenger"
```

This means FastAPI does not need model files manually copied into the API folder. It loads the promoted model versions from the MLflow registry.

---

## FastAPI endpoints

### `GET /`

Redirects to the interactive FastAPI documentation:

```text
/docs
```

### `GET /health`

Simple health check endpoint.

Expected response:

```json
{
  "status": "ok",
  "service": "flyontime-fastapi"
}
```

### `GET /debug-models`

Debug endpoint used during development to verify:

- MLflow tracking URI;
- classifier model URI;
- regressor model URI;
- reference dataset loading;
- feature preprocessor status;
- single-flight lookup script availability.

This endpoint is useful for debugging but should be disabled or protected in a real production environment.

### `POST /predict`

Main prediction endpoint used by Streamlit.

Example request:

```json
{
  "flight_number": "AF7300",
  "date": "2026-04-22T06:45:00",
  "departure_airport": "CDG - Paris Charles de Gaulle",
  "arrival_airport": "NCE - Nice Côte d'Azur"
}
```

Example response:

```json
{
  "status": "success",
  "flight_number": "AF7300",
  "date": "2026-04-22T06:45:00",
  "departure_airport": "CDG",
  "arrival_airport": "NCE",
  "delay_probability": 0.73,
  "predicted_arrival_delay_minutes": 18.4,
  "is_delayed": true,
  "message": "Prédiction générée avec succès.",
  "warning_message": null
}
```

---

## FastAPI internal workflow

When `/predict` is called, FastAPI executes the following workflow:

```text
1. Receive the Streamlit payload
2. Normalize flight number and airport values
3. Launch GlobalRunSingleFlight.py
4. Recover request_id from the pipeline stdout
5. Read flight_request_status.json if generated
6. Run the single-flight transformation pipeline
7. Upload the processed parquet to S3
8. Select the matching single-flight row
9. Build a base row from the reference training dataframe
10. Apply classifier-specific preprocessing
11. Apply regressor-specific preprocessing
12. Load the XGBoost classifier and regressor from MLflow
13. Run classifier prediction
14. Run regressor prediction
15. Return the final JSON response to Streamlit
```

---

## Feature preparation for inference

FastAPI uses a reference dataframe:

```text
data/df_train_final.parquet
```

This file is important because it preserves the training schema after feature engineering. During inference, the backend starts from a reference row, replaces the values available from the real single-flight ETL output, then applies notebook-compatible preprocessing.

The production preprocessing includes:

- preserving the training feature schema;
- aligning column names such as scheduled departure / arrival columns;
- extracting datetime features;
- encoding categorical variables with `OrdinalEncoder`;
- imputing numerical missing values with training medians;
- creating one prepared input for the classifier;
- creating one prepared input for the regressor.

This is critical because the XGBoost models expect the same feature order and feature names used during training.

---

## FastAPI Docker deployment

FastAPI is deployed on Hugging Face Spaces with Docker.

The Dockerfile:

- starts from `python:3.11-slim`;
- installs system packages such as `curl` and `nano`;
- creates the Hugging Face `user` account;
- copies the FastAPI project into `/home/user/app`;
- installs dependencies from `requirements.txt`;
- exposes port `7860`;
- starts the app with `uvicorn`.

Runtime command:

```bash
uvicorn app:app --host 0.0.0.0 --port ${PORT:-7860}
```

---

## FastAPI dependencies

The main dependencies are declared in:

```text
FASTAPI/requirements.txt
```

They include:

- `fastapi`
- `uvicorn[standard]`
- `pandas`
- `pyarrow`
- `mlflow`
- `scikit-learn`
- `requests`
- `boto3`
- `python-dotenv`

These dependencies support API serving, parquet reading, MLflow model loading, S3 communication and preprocessing.

---

## FastAPI environment variables and secrets

FastAPI uses both public variables and private secrets from the Hugging Face Space settings.

### Public variables

| Variable | Purpose |
|---|---|
| `PORT` | Runtime port used by Hugging Face Spaces. Usually `7860`. |
| `BUCKET_EQUIPE` | Team bucket reference used in the project configuration. |
| `ENABLE_S3_UPLOAD` | Enables or disables S3 upload for request outputs and logs. Use `1` to enable. |
| `S3_BUCKET_NAME` | Main S3 bucket name, for example `ppml2026`. |
| `S3_PREFIX` | Prefix used for raw pipeline outputs, for example `raw`. |

### Private secrets

| Secret | Purpose |
|---|---|
| `MLFLOW_TRACKING_URI` | URL of the deployed MLflow Space used to load registered models. |
| `BACKEND_STORE_URI` | Remote backend store URI, kept when MLflow-related config is needed. |
| `ARTIFACT_STORE_URI` | S3 artifact storage URI used by the MLflow setup. |
| `AWS_ACCESS_KEY_ID` | AWS access key used to read/write S3 objects. |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key used to access S3. |
| `AWS_DEFAULT_REGION` | AWS region of the S3 bucket. |
| `API_KEY` | External API key used for flight data calls. |
| `API_KEY_NAME` | API provider identifier / key name. |

The code also supports a fallback from `AWS_ACCESS_KEY` to `AWS_ACCESS_KEY_ID`, but the preferred clean naming is:

```text
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_DEFAULT_REGION
```

---

# 3. MLflow Space

## Role

MLflow is the remote experiment tracking server and model registry.

It connects the modeling phase to deployment:

```text
Training scripts / notebooks
 ↓
MLflow experiments
 ↓
Registered models
 ↓
Model alias: challenger
 ↓
FastAPI loads the promoted models
```

MLflow stores:

- experiment runs;
- model parameters;
- metrics;
- artifacts;
- registered model versions;
- model aliases.

In this project, the promoted models are loaded by FastAPI through the `challenger` alias.

---

## MLflow folder structure

```text
MLFLOW/
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## MLflow Docker deployment

MLflow is deployed as a dedicated Hugging Face Docker Space.

The Dockerfile:

- starts from `continuumio/miniconda3`;
- installs system dependencies and AWS CLI;
- creates the Hugging Face `user` account;
- installs Python dependencies from `requirements.txt`;
- starts an MLflow server.

Runtime command:

```bash
mlflow server -p $PORT \
  --host 0.0.0.0 \
  --backend-store-uri $BACKEND_STORE_URI \
  --default-artifact-root $ARTIFACT_STORE_URI \
  --x-frame-options NONE \
  --allowed-hosts '*' \
  --cors-allowed-origins '*'
```

The backend store contains MLflow metadata. The artifact store contains model files and other run artifacts.

---

## MLflow dependencies

The MLflow Space dependencies include:

- `mlflow==3.5.0`
- `scikit-learn==1.6.1`
- `pandas`
- `numpy`
- `psycopg2-binary`
- `boto3`
- `matplotlib`
- `seaborn`
- `plotly`

`psycopg2-binary` is used for a SQL backend store, while `boto3` allows MLflow to access the S3 artifact store.

---

## MLflow environment variables and secrets

### Public variables

| Variable | Purpose |
|---|---|
| `PORT` | Runtime port used by the Hugging Face Space. |

### Private secrets

| Secret | Purpose |
|---|---|
| `BACKEND_STORE_URI` | Remote database URI used to store experiments, runs, metrics, parameters and registry metadata. |
| `ARTIFACT_STORE_URI` | Default artifact root, usually an S3 URI. |
| `AWS_ACCESS_KEY_ID` | AWS access key used to access the artifact store. |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key used to access S3. |
| `AWS_ACCESS_KEY` | Legacy / alternative access key name visible in the Space configuration. |
| `AWS_SECRET_ACCESS_KEY_ID` | Legacy / alternative secret name visible in the Space configuration. |

For a cleaner production configuration, the preferred AWS names are `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. Legacy names should only remain if the current Docker image or older scripts still depend on them.

---

# 4. Streamlit Space

## Role

Streamlit is the public-facing web application.

It allows a passenger to:

- enter a flight number;
- choose the flight date and departure time;
- select departure and arrival airports;
- submit the request;
- view the delay prediction returned by FastAPI.

Streamlit does not run MLflow or XGBoost directly. It communicates with the FastAPI Space through an HTTP POST request.

---

## Streamlit folder structure

```text
STREAMLIT/
├── app.py
├── Dockerfile
├── requirements.txt
├── anims/
└── README.md
```

The `anims/` folder contains Lottie JSON animations used in the interface.

---

## Streamlit application behavior

The main application is:

```text
STREAMLIT/app.py
```

Current FastAPI target:

```python
API_BASE = "https://ppml2026-ppml-fastapi.hf.space"
API_URL = f"{API_BASE}/predict"
```

The app provides:

- an aviation-themed user interface;
- Lottie animations for the sidebar and loading state;
- a flight search form;
- client-side validation before calling FastAPI;
- a loading animation while the backend runs the prediction;
- metric cards for delay and risk level;
- an expandable section with full flight details.

---

## Streamlit input validation

Before calling FastAPI, Streamlit validates:

- the flight number format, for example `AF1234` or `BA 456`;
- the date and time selected by the user;
- the departure airport;
- the arrival airport;
- the rule that departure and arrival airports must be different.

This avoids unnecessary backend calls and improves the user experience.

---

## Payload sent to FastAPI

Streamlit sends a JSON payload to FastAPI:

```json
{
  "flight_number": "AF7300",
  "date": "2026-04-22T06:45:00",
  "departure_airport": "CDG - Paris Charles de Gaulle",
  "arrival_airport": "NCE - Nice Côte d'Azur"
}
```

The backend then normalizes the airport labels and uses only the code needed by the pipeline.

---

## Streamlit result display

After receiving the FastAPI response, Streamlit displays:

- estimated arrival delay in minutes;
- delay probability;
- risk level;
- interpreted result message;
- complete flight details inside an expandable section.

Risk interpretation example:

| Delay estimate | Risk level | Interpretation |
|---:|---|---|
| `< 30 min` | Low | Flight appears likely to be on time or lightly delayed. |
| `30–59 min` | Medium | Some delay risk exists. |
| `>= 60 min` | High | The passenger should anticipate a significant delay. |

The current UI also contains a visual section for probable delay causes. At MVP stage, these causes are still mocked / illustrative. A future improvement would be to connect this section to SHAP explanations or feature contributions from the backend.

---

## Streamlit error handling

Streamlit catches API call failures and displays a user-facing error message.

Typical cases:

```text
Flight not found. Please verify your details.
A technical error has occurred. Please try again later.
The service is taking too long to respond. Please try again shortly.
Unable to reach the prediction service right now. Please try again later.
```

The goal is to avoid exposing raw backend errors to the final user.

---

## Streamlit Docker deployment

Streamlit is deployed on Hugging Face Spaces with Docker.

The Dockerfile:

- starts from `continuumio/miniconda3`;
- installs basic system packages;
- creates the Hugging Face `user` account;
- installs dependencies from `requirements.txt`;
- copies the app files into `/home/user/app`;
- exposes port `7860`;
- starts Streamlit.

Runtime command:

```bash
streamlit run app.py --server.port=7860
```

---

## Streamlit dependencies

The Streamlit dependencies are:

- `streamlit`
- `pandas`
- `plotly`
- `streamlit_lottie`

These cover the web UI, input handling, visual components and animations.

---

## Streamlit environment variables and secrets

The current `app.py` hardcodes the FastAPI endpoint:

```python
API_BASE = "https://ppml2026-ppml-fastapi.hf.space"
```

For a cleaner production setup, this should ideally be moved to a Hugging Face variable:

| Variable | Purpose |
|---|---|
| `API_BASE` | Base URL of the deployed FastAPI Space. |
| `API_URL` | Full prediction endpoint. Usually `${API_BASE}/predict`. |
| `PORT` | Runtime port used by Hugging Face Spaces, usually `7860`. |

Streamlit should not contain AWS credentials, MLflow credentials or external API keys. Those belong to FastAPI and MLflow only.

---

# 5. End-to-end test

A typical manual test is:

```text
1. Verify that the MLflow Space is running.
2. Verify that the promoted XGBoost models exist with the challenger alias.
3. Start or restart the FastAPI Space.
4. Open FastAPI /docs.
5. Test GET /health.
6. Test GET /debug-models.
7. Test POST /predict with a known flight payload.
8. Open the Streamlit Space.
9. Submit the same flight from the web form.
10. Verify that Streamlit displays the FastAPI prediction.
```

---

# 6. Production-oriented recommendations

For a more production-ready architecture:

- keep all secrets in Hugging Face Spaces secrets;
- never commit `.env` files;
- keep `API_KEY`, AWS credentials and database URLs private;
- move the Streamlit FastAPI URL to an environment variable;
- protect or disable `/debug-models` in production;
- keep `/health` available for monitoring;
- store request logs in S3 using the generated `request_id`;
- keep using MLflow aliases for model promotion;
- add schema validation tests between ETL output and model input;
- add automated tests for the `/predict` endpoint;
- replace mocked delay causes in Streamlit with SHAP-based explanations when available.

---

# 7. Why this deployment matters

This deployment is not just a notebook export.

It demonstrates a complete machine learning application workflow:

- frontend validation;
- backend API orchestration;
- external flight data integration;
- S3 storage;
- feature engineering for inference;
- model registry usage;
- XGBoost model serving;
- user-facing error handling;
- independent services deployed on Hugging Face Spaces.

This makes FlyOnTime closer to a real-world machine learning product than a simple classroom notebook.
