# 03_DEPLOYMENT — FastAPI, MLflow and Streamlit

This folder contains the deployment layer of the FlyOnTime project.

It turns the data science work into a usable application.

The deployment stack contains three main services, each deployed as a separate **Hugging Face Space**:

```text
03_DEPLOYMENT/
├── FASTAPI/       # Backend API, orchestration and inference — Hugging Face Space
├── MLFLOW/        # Model registry and experiment tracking — Hugging Face Space
└── STREAMLIT/     # User-facing web application — Hugging Face Space
```

This separation is intentional. Streamlit, FastAPI and MLflow each have a different responsibility, so deploying them as three independent Spaces makes the project easier to maintain, debug and explain.

---

## Global deployment architecture

```text
User
 ↓
Streamlit Hugging Face Space
 ↓
FastAPI Hugging Face Space
 ↓
Single-flight lookup pipeline
 ↓
ETL / feature preparation
 ↓
MLflow Hugging Face Space / model registry
 ↓
Classifier + Regressor
 ↓
FastAPI response
 ↓
Streamlit result display
```

---

# 1. FASTAPI

## Deployment target

FastAPI is deployed as a dedicated **Hugging Face Space**.

Its role in production is to expose the backend API used by the Streamlit Space. It receives prediction requests, launches the flight lookup / ETL workflow, loads the promoted MLflow models and returns the final JSON response.

Secrets such as AWS credentials, API keys and the MLflow tracking URI are configured as Hugging Face Space secrets, not committed to GitHub.

## Role

FastAPI is the backend of the application.

It is responsible for:

- receiving the user request from Streamlit;
- validating request fields;
- normalizing flight number and airport values;
- launching the single-flight lookup pipeline;
- running the ETL transformation;
- loading the MLflow models;
- preparing model-compatible features;
- running predictions;
- returning a clean JSON response to Streamlit.

FastAPI is the orchestrator between the user interface, the data pipeline and the machine learning models.

---

## FASTAPI folder structure

```text
FASTAPI/
├── app.py
├── Dockerfile
├── requirements.txt
├── call_api/
├── preprocessing/
└── README.md
```

The important file is:

```text
app.py
```

---

## Main endpoints

### `GET /`

Redirects to the interactive API documentation.

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

Debug endpoint used to verify:

- MLflow tracking URI;
- classifier model URI;
- regressor model URI;
- reference dataset status;
- model loader status;
- flight lookup script existence.

### `POST /predict`

Main prediction endpoint.

It receives the flight request and returns the delay prediction.

---

## Prediction request schema

Example payload:

```json
{
  "flight_number": "AF7300",
  "date": "2026-04-22T06:45:00",
  "departure_airport": "CDG",
  "arrival_airport": "NCE"
}
```

Fields:

| Field | Type | Description |
|---|---|---|
| `flight_number` | string | Flight number entered by the user |
| `date` | string | Date and time selected by the user |
| `departure_airport` | string | Departure airport code or label |
| `arrival_airport` | string, optional | Arrival airport code or label |

---

## Prediction response schema

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

Fields:

| Field | Description |
|---|---|
| `status` | Request status |
| `delay_probability` | Probability of arrival delay |
| `predicted_arrival_delay_minutes` | Estimated arrival delay in minutes |
| `is_delayed` | Boolean classification output |
| `message` | User-readable message |
| `warning_message` | Optional warning, for example codeshare |

---

## FastAPI internal workflow

When `/predict` is called, FastAPI performs the following steps:

```text
1. Receive Streamlit payload
2. Normalize flight number and airport values
3. Run GlobalRunSingleFlight.py
4. Recover request_id from pipeline stdout
5. Read flight_request_status.json
6. Run transformation pipeline
7. Upload processed parquet to S3
8. Select the correct flight row
9. Align features with the reference training schema
10. Load classifier and regressor from MLflow
11. Run classifier prediction
12. Run regressor prediction
13. Return response to Streamlit
```

---

## Model loading

FastAPI loads the models from MLflow using registered model URIs.

Example:

```python
CLASSIFIER_MODEL_URI = "models:/RandomForest_Classifier_registered@challenger"
REGRESSOR_MODEL_URI = "models:/RandomForest_Regressor_registered@challenger"
```

In the final XGBoost direction, the same principle applies with XGBoost registered models and the `challenger` alias.

This allows the backend to use the model currently promoted in MLflow without manually copying model files into the API container.

---

## Feature preparation

The backend prepares features by:

- starting from a reference dataframe;
- replacing matching columns with the real single-flight ETL data;
- harmonizing column names;
- extracting datetime features;
- removing unavailable target columns;
- keeping the schema aligned with the training data.

This is critical because model inference will fail if training and production columns are not aligned.

---

## Error handling

FastAPI translates technical pipeline failures into human-readable messages.

Examples:

```text
Vol introuvable. Veuillez vérifier le numéro de vol, la date, l’horaire et l’aéroport de départ.
Le service de recherche de vol est momentanément indisponible. Merci de réessayer dans quelques instants.
Une erreur technique est survenue pendant la prédiction. Merci de réessayer.
```

This avoids exposing raw Python or API errors to the final user.

---

## Run FastAPI locally

```bash
cd src/03_DEPLOYMENT/FASTAPI
uvicorn app:app --host 0.0.0.0 --port 7860
```

Then open:

```text
http://localhost:7860/docs
```

---

## Hugging Face variables and secrets — FastAPI Space

The FastAPI Space uses both **public variables** and **private secrets** configured from the Hugging Face Space settings panel.

Public variables used in the project:

| Variable | Purpose |
|---|---|
| `PORT` | Runtime port used by the Hugging Face Space container |
| `BUCKET_EQUIPE` | Team bucket reference used by the project configuration |
| `ENABLE_S3_UPLOAD` | Enables upload of request outputs and logs to S3 when set to `1` |
| `S3_BUCKET_NAME` | Main S3 bucket name, for example `ppml2026` |
| `S3_PREFIX` | S3 prefix used for raw pipeline outputs, for example `raw` |

Private secrets used in the project:

| Secret | Purpose |
|---|---|
| `MLFLOW_TRACKING_URI` | URL of the deployed MLflow Hugging Face Space used by FastAPI to load registered models |
| `BACKEND_STORE_URI` | Database URI used by MLflow metadata / tracking configuration when needed |
| `ARTIFACT_STORE_URI` | S3 artifact storage URI used by the MLflow setup |
| `AWS_ACCESS_KEY_ID` | AWS access key used by FastAPI to read/write S3 objects |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key used by FastAPI to access S3 |
| `AWS_DEFAULT_REGION` | AWS region of the S3 bucket |
| `API_KEY` | External API key used to call the flight data provider |
| `API_KEY_NAME` | API key name / provider identifier used by the API integration |

These values are not hardcoded in the repository. They are injected at runtime by Hugging Face Spaces.
---

# 2. MLFLOW

## Deployment target

MLflow is deployed as a dedicated **Hugging Face Space**.

This Space acts as the remote experiment tracking server and model registry used by both the modeling notebooks and the FastAPI backend.

The notebooks log experiments to this MLflow Space, and FastAPI later loads the promoted model versions from the same tracking server.

## Role

MLflow is used for model tracking and model registry.

It stores:

- experiments;
- model parameters;
- model metrics;
- model artifacts;
- registered models;
- model aliases.

In this project, MLflow is the bridge between modeling and deployment.

```text
Notebook training
        ↓
MLflow experiment tracking
        ↓
Registered model
        ↓
FastAPI model loading
```

---

## MLFLOW folder structure

```text
MLFLOW/
├── Dockerfile
├── requirements.txt
└── README.md
```

The MLflow service is deployed as a Hugging Face Space. In this project, this is what allows the team to avoid storing model binaries directly inside the FastAPI or Streamlit applications.

---

## Why MLflow is useful here

Without MLflow, FastAPI would need to load local model files manually.

With MLflow:

- models are versioned;
- metrics are preserved;
- experiments can be compared;
- the API can load a model by registry alias;
- model promotion becomes easier.

Typical lifecycle:

```text
Experiment run → Register model → Assign alias → FastAPI loads alias
```

Example alias:

```text
challenger
```

---

## Hugging Face variables and secrets — MLflow Space

The MLflow server is also deployed as a dedicated Hugging Face Space. Its variables and secrets are separated from FastAPI because MLflow has its own runtime and storage configuration.

Public variable used in the MLflow Space:

| Variable | Purpose |
|---|---|
| `PORT` | Runtime port used by the Hugging Face Space container |

Private secrets used in the MLflow Space:

| Secret | Purpose |
|---|---|
| `BACKEND_STORE_URI` | Remote database URI used by MLflow to store experiments, runs, metrics, parameters and model registry metadata |
| `ARTIFACT_STORE_URI` | S3 location used to store MLflow artifacts and model files |
| `AWS_ACCESS_KEY_ID` | AWS access key used by MLflow to access the S3 artifact store |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key used by MLflow to access S3 |
| `AWS_ACCESS_KEY` | Legacy or alternative AWS access key name kept for compatibility with some scripts/configurations |
| `AWS_SECRET_ACCESS_KEY_ID` | Legacy or alternative secret name visible in the current Space configuration |

For a cleaner production setup, the preferred AWS variable names are `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. Extra legacy names should only be kept if one of the existing scripts or Docker commands still depends on them.
In the project architecture:

- the MLflow server runs on Hugging Face Spaces;
- the backend store can be a remote SQL database;
- model artifacts can be stored in S3;
- FastAPI connects to the MLflow Space through `MLFLOW_TRACKING_URI`;
- notebooks also use the same `MLFLOW_TRACKING_URI` to log and register models.

---

# 3. STREAMLIT

## Deployment target

Streamlit is deployed as a dedicated **Hugging Face Space**.

This Space is the public-facing application. It does not run the models directly. Instead, it validates the user input, sends the request to the FastAPI Space, waits for the backend response, and displays the result in a user-friendly way.

## Role

Streamlit is the user-facing web application.

It allows a passenger to:

- enter a flight number;
- choose a flight date and time;
- choose departure and arrival airports;
- submit the request;
- view the prediction returned by FastAPI.

The app is designed with an aviation-themed UI and visual consistency with the project presentation.

---

## STREAMLIT folder structure

```text
STREAMLIT/
├── app.py
├── requirements.txt
├── Dockerfile
├── anims/
│   ├── plane.json
│   └── loader_plane.json
└── README.md
```

---

## User input validation

Before calling FastAPI, Streamlit validates the form inputs.

Validation includes:

- flight number format;
- date and time input through dedicated widgets;
- departure airport selection;
- arrival airport selection;
- check that arrival airport is different from departure airport.

This avoids unnecessary backend calls and improves user experience.

---

## Payload sent to FastAPI

Streamlit sends a JSON payload like:

```json
{
  "flight_number": "AF7300",
  "date": "2026-04-22T06:45:00",
  "departure_airport": "CDG",
  "arrival_airport": "NCE"
}
```

The backend expects the same fields in the `POST /predict` endpoint.

---

## Result display

Streamlit displays:

- predicted arrival delay in minutes;
- delay probability;
- risk level;
- short interpretation text;
- optional warning messages;
- backend error messages in user-friendly wording.

Risk levels can be displayed visually:

| Risk level | Interpretation |
|---|---|
| Low | Flight appears likely to be on time |
| Medium | Some delay risk exists |
| High | Delay risk is significant |

---

## Error messages shown to users

Streamlit should display backend errors in a clear form, for example:

```text
Flight not found. Please verify your details.
A technical error has occurred. Please try again later.
The service is taking too long to respond. Please try again shortly.
Unable to reach the prediction service right now. Please try again later.
```

The purpose is to keep the UI clean and understandable even when the API or external flight data service fails.

---

## Lottie animations

The Streamlit UI uses Lottie animations.

A Lottie animation is a lightweight animation stored as a JSON file.

In this project:

```text
anims/plane.json
anims/loader_plane.json
```

These animations support the aviation theme and make the app feel more polished.

---

## Hugging Face variables and secrets — Streamlit Space

The Streamlit Space is lighter than FastAPI and MLflow. It mainly needs to know where the FastAPI backend is deployed.

Typical variables:

| Variable | Purpose |
|---|---|
| `API_URL` or `API_BASE` | URL of the deployed FastAPI Hugging Face Space prediction endpoint |
| `PORT` | Runtime port used by the Hugging Face Space container, if required by the Space configuration |

Streamlit should not contain AWS credentials or MLflow credentials. It should only communicate with FastAPI. This keeps the frontend simple and safer.

---

## Run Streamlit locally

```bash
cd src/03_DEPLOYMENT/STREAMLIT
streamlit run app.py
```

Make sure the FastAPI backend URL is correctly configured in `app.py`. In production, this URL points to the deployed FastAPI Hugging Face Space.

---

# Deployment notes

## Hugging Face Spaces

The three services are deployed on Hugging Face Spaces:

- one Space for **FastAPI**: backend orchestration and inference API;
- one Space for **Streamlit**: user-facing web application;
- one Space for **MLflow**: experiment tracking and model registry.

Each service has its own runtime and secrets. This is closer to a production architecture than putting everything into one monolithic app.

Secrets should be configured in Hugging Face, not committed to GitHub. In this project, each Space has its own environment variables: FastAPI has API/S3/MLflow access, MLflow has database/S3 artifact access, and Streamlit mainly needs the FastAPI endpoint URL.

---

## Production-oriented recommendations

For a cleaner production setup:

- keep all secrets in Hugging Face Spaces secrets;
- never commit `.env` files;
- use `/health` for monitoring FastAPI;
- keep `/debug-models` private or disable it in production;
- add timeout handling in Streamlit;
- add schema validation tests between ETL and model inputs;
- track every user request with a unique `request_id`;
- store logs in S3 for auditability;
- use MLflow aliases to control model promotion.

---

## End-to-end test

A typical manual test:

1. Start MLflow or make sure the remote MLflow server is available.
2. Start FastAPI.
3. Open `/docs`.
4. Test `/health`.
5. Test `/predict` with a known flight payload.
6. Start Streamlit.
7. Submit the same flight from the UI.
8. Verify that Streamlit receives and displays the FastAPI response.

---

## Why this deployment design matters

The deployment is not a simple notebook export.  
It is an application pipeline with:

- frontend validation;
- backend orchestration;
- external API dependency handling;
- cloud storage;
- model registry;
- inference schema alignment;
- user-facing error handling.

This makes the project closer to a real-world machine learning product.
