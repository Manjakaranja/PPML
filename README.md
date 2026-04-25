# FlyOnTime — Real-time Flight Delay Prediction

FlyOnTime is an end-to-end data science and MLOps project built for the PPML 2026 program.  
The goal is to help a passenger anticipate the risk of arrival delay for a given flight, directly from a simple web interface.

The project combines:

- historical flight data,
- weather and contextual data,
- feature engineering,
- machine learning models,
- model tracking and registry,
- cloud storage,
- an API backend,
- and a Streamlit web application.

The final user journey is simple:

```text
User input in Streamlit
        ↓
FastAPI backend
        ↓
Flight lookup + external API calls
        ↓
ETL / feature preparation
        ↓
MLflow model loading
        ↓
Classifier + regressor inference
        ↓
Prediction displayed to the user
```

---

## Business objective

Flight delays are uncertain, difficult to interpret, and often communicated too late for passengers to reorganize their trip.

FlyOnTime answers two practical questions:

1. **Is my flight likely to be delayed at arrival?**
2. **If yes, how many minutes of delay should I expect?**

To answer these questions, we use two complementary models:

- a **classification model** to estimate the risk of delay;
- a **regression model** to estimate the arrival delay in minutes.

---

## Repository structure

```text
PPML/
├── architecture/              # Project architecture diagrams
├── data_/                     # Data documentation / data-related assets
├── organisation/              # Project organization documents
├── presentation/              # Presentation material
├── src/
│   ├── 01_ETL/                # Extraction, transformation, loading
│   ├── 02_MODELING/           # Notebooks, model experiments, training logic
│   └── 03_DEPLOYMENT/         # FastAPI, MLflow, Streamlit deployment
├── requirements.txt
└── README.md
```

---

## Pipeline overview

### 1. ETL

The ETL layer collects and prepares the data used by the project.

It supports three main data generation scenarios:

| Scenario | Purpose | Output |
|---|---|---|
| Historical data | Build the model training dataset | Clean training dataset |
| Future data | Prepare upcoming flight data | Future inference dataset |
| Single flight on demand | Prepare one user-requested flight | One-row inference dataset |

Data is enriched with information from multiple sources: flight APIs, weather data, holidays, airport-related data and contextual information.

The main outputs are clean datasets with a consistent structure, designed to be reusable by the modeling and inference pipelines.

---

### 2. Modeling

The modeling layer contains both the experiments and the production-oriented training scripts.

The project first used **Random Forest models** as a Minimum Viable Product.  
This MVP was useful because Random Forest is robust, easy to understand, and quick to validate for a first working pipeline.

The final modeling direction moved to **XGBoost**, mainly because:

- it reduced user waiting time during inference;
- it was faster and more efficient for the deployed application;
- it gave stronger and more reliable metrics;
- it was better suited for the final real-time prediction workflow.

The final modeling strategy uses:

- **XGBoost Classifier** for delay risk detection;
- **XGBoost Regressor** for delay duration estimation.

The XGBoost logic was first developed in notebooks, then moved into clean scripts for reproducible training:

```text
src/02_MODELING/training_scripts/
├── train_xgboost_classifier.py
└── train_xgboost_regressor.py
```

The classifier script loads the official cleaned historical dataset from S3:

```text
s3://ppml2026/datasets/SignofFlightsDataset_20260416_233018_CLEAN.csv
```

It then saves the prepared training dataframe locally as:

```text
data/df_train_final.parquet
```

The regressor script reuses this local parquet so both models stay aligned on the same final feature base.

Key metrics used in the project:

| Model | Main objective | Main metric |
|---|---|---|
| XGBoost Classifier | Predict delayed / not delayed | F1 score around 0.86 in cross-validation, test F1 around 0.87 |
| XGBoost Regressor | Predict delay duration in minutes | MAE around 6.98 minutes |

---

### 3. Deployment

The deployment layer is composed of three services, all deployed on **Hugging Face Spaces**:

```text
03_DEPLOYMENT/
├── FASTAPI/       # Backend API and inference orchestration, deployed as a HF Space
├── MLFLOW/        # Model tracking / registry server, deployed as a HF Space
└── STREAMLIT/     # User-facing web application, deployed as a HF Space
```

Using one Hugging Face Space per service keeps the architecture modular: the UI, backend API and model registry can be updated, restarted and debugged independently.

Each Space uses its own Hugging Face variables and secrets. FastAPI contains the external API, S3 and MLflow connection settings; MLflow contains the backend database and artifact store settings; Streamlit only needs the FastAPI endpoint.
The deployment workflow is:

1. Streamlit collects the user input.
2. Streamlit sends a JSON payload to FastAPI.
3. FastAPI validates the request.
4. FastAPI runs the flight lookup and single-flight ETL pipeline.
5. The prepared data is aligned with the model schema.
6. Models are loaded from MLflow.
7. The classifier predicts the delay risk.
8. The regressor predicts the delay duration.
9. FastAPI returns a structured response.
10. Streamlit displays the result in a user-friendly interface.

---

## How to use the project

### 1. Clone the repository

```bash
git clone https://github.com/Manjakaranja/PPML.git
cd PPML
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

Some deployment folders also contain their own `requirements.txt` files, especially FastAPI, Streamlit and MLflow.

---

## Recommended execution order

### Step 1 — Run the ETL layer

Go to:

```bash
cd src/01_ETL
```

Typical order:

```bash
python 01_Extraction.py
python 02_Transformation.py
python 03_Load.py
```

The exact execution can depend on whether the goal is historical data generation, future data generation, or single-flight inference.

---

### Step 2 — Understand the modeling notebooks

Go to:

```bash
cd src/02_MODELING/notebook
```

Recommended reading order:

1. `randomforest_classifier.ipynb`
2. `randomforest_regressor.ipynb`
3. `xgboost_classifier.ipynb`
4. `xgboost_regressor.ipynb`

The Random Forest notebooks document the MVP phase.  
The XGBoost notebooks represent the final modeling direction.

### Step 3 — Run the clean XGBoost training scripts

Go to:

```bash
cd src/02_MODELING
```

Run the classifier first:

```bash
python training_scripts/train_xgboost_classifier.py
```

This script loads the training CSV from S3, trains the classifier, logs the model to MLflow, and creates:

```text
data/df_train_final.parquet
```

Then run the regressor:

```bash
python training_scripts/train_xgboost_regressor.py
```

The regressor reads the local `data/df_train_final.parquet` generated by the classifier script.

---

### Step 4 — Register models with MLflow

MLflow is also deployed on its own **Hugging Face Space**. The selected models are logged to this remote tracking server with:

- parameters,
- metrics,
- artifacts,
- model signatures when available,
- and registered model names.

The production API loads models from MLflow using a registered model alias, typically `challenger`.

---

### Step 5 — Start or deploy FastAPI

Go to:

```bash
cd src/03_DEPLOYMENT/FASTAPI
```

Run locally:

```bash
uvicorn app:app --host 0.0.0.0 --port 7860
```

In the deployed project, FastAPI is hosted on its own **Hugging Face Space**. This Space exposes the `/predict` endpoint used by Streamlit and stores credentials as Hugging Face secrets.

Useful endpoints:

```text
GET  /
GET  /health
GET  /debug-models
POST /predict
```

---

### Step 6 — Start or deploy Streamlit

Go to:

```bash
cd src/03_DEPLOYMENT/STREAMLIT
```

Run locally:

```bash
streamlit run app.py
```

In the deployed project, Streamlit is hosted on its own **Hugging Face Space**. The app sends user inputs to the FastAPI Space and displays the prediction result.

---

## Main environment variables

The project uses environment variables for credentials and deployment configuration.

Typical variables:

```bash
API_KEY=...
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=eu-north-1
S3_BUCKET_NAME=ppml2026
S3_PREFIX=raw
TRANSFORMED_S3_PREFIX=processed
MLFLOW_TRACKING_URI=...
ENABLE_S3_UPLOAD=1
```

These variables should never be committed to GitHub.  
In deployment, they are provided as Hugging Face Spaces secrets.

---

## Example prediction payload

The Streamlit app sends a payload similar to:

```json
{
  "flight_number": "AF7300",
  "date": "2026-04-22T06:45:00",
  "departure_airport": "CDG",
  "arrival_airport": "NCE"
}
```

FastAPI returns a structured response containing:

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
  "message": "Prediction generated successfully.",
  "warning_message": null
}
```

---

## Why this project matters

FlyOnTime is not only a notebook project.  
It demonstrates the full path from raw data to a deployed machine learning application:

- data collection,
- data cleaning,
- feature engineering,
- model training,
- model comparison,
- model versioning,
- API serving,
- user interface,
- cloud storage,
- and error handling.

The project was designed as a realistic end-to-end machine learning workflow, with a clear separation between data engineering, modeling and deployment.

---

## Documentation

Detailed README files are available in:

```text
src/01_ETL/README.md
src/02_MODELING/README.md
src/03_DEPLOYMENT/README.md
```

Each README explains the purpose of the folder, how the code is used, and how it fits into the full FlyOnTime pipeline.
